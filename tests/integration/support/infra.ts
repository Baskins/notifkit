import { afterAll, beforeAll, inject } from "vitest";
import { randomBytes } from "node:crypto";
import postgres from "postgres";
import { Redis } from "ioredis";
import { drizzle } from "drizzle-orm/postgres-js";
import type { Db, Sql } from "@/db/index.js";
import * as schema from "@/db/schema.js";
import { registerCustomCommands } from "@/redis/index.js";
import { TEMPLATE_DB, withDatabase } from "./global-setup.js";

export interface Infra {
  /** Connection string for this file's private database. */
  dbUrl: string;
  /** Connection string for this file's private Redis logical database. */
  redisUrl: string;
  sql: Sql;
  db: Db;
  /** A plain client with the Lua commands registered. */
  redis: Redis;
  /** Empties every table and the Redis database. */
  reset(): Promise<void>;
}

/**
 * Registers hooks that give the calling file a real, empty Postgres database
 * (migrated) and a real, empty Redis database. The returned object is filled
 * in by `beforeAll`, so read its fields inside hooks and tests, not at import.
 */
export function useInfra(): Infra {
  const infra = {} as Infra;
  let dbName = "";
  let adminUrl = "";

  beforeAll(async () => {
    const infraError = inject("infraError");
    if (infraError) {
      throw new Error(
        `Integration tests need Docker for Postgres and Redis, and it is not available: ${infraError}`,
      );
    }
    adminUrl = inject("pgAdminUrl");
    dbName = `t_${process.env.VITEST_POOL_ID ?? "0"}_${randomBytes(6).toString("hex")}`;
    await withAdmin(adminUrl, async (admin) => {
      // Two files cloning the template at the same moment can collide on its
      // lock; a short retry absorbs that.
      for (let attempt = 0; ; attempt++) {
        try {
          await admin.unsafe(`CREATE DATABASE ${dbName} TEMPLATE ${TEMPLATE_DB}`);
          break;
        } catch (err: any) {
          if (attempt >= 20 || !String(err?.message).includes("being accessed")) throw err;
          await new Promise((r) => setTimeout(r, 100 + Math.random() * 200));
        }
      }
    });

    infra.dbUrl = withDatabase(adminUrl, dbName);
    // The production pool's 10s statement timeout exists to shed load; on a
    // test machine running the chaos suites alongside, a trivial query can wait
    // that long for CPU. The code under test still gets its own pools.
    const sql = postgres(infra.dbUrl, {
      max: 5,
      onnotice: () => {},
      connection: { statement_timeout: 60_000 as any },
    });
    const db = drizzle(sql, { schema });
    infra.sql = sql;
    infra.db = db;

    const index = (Number(process.env.VITEST_POOL_ID ?? "1") % 127) + 1;
    infra.redisUrl = `${inject("redisBaseUrl")}/${index}`;
    infra.redis = new Redis(infra.redisUrl, { maxRetriesPerRequest: null });
    registerCustomCommands(infra.redis);
    await infra.redis.flushdb();

    infra.reset = async () => {
      const tables = await sql<{ tablename: string }[]>`
        SELECT tablename FROM pg_tables WHERE schemaname = 'public'`;
      if (tables.length > 0) {
        await sql.unsafe(
          `TRUNCATE ${tables.map((t) => `"${t.tablename}"`).join(", ")} RESTART IDENTITY CASCADE`,
        );
      }
      await infra.redis.flushdb();
    };
  }, 120_000);

  afterAll(async () => {
    infra.redis?.disconnect();
    await infra.sql?.end({ timeout: 5 }).catch(() => {});
    if (dbName) {
      await withAdmin(adminUrl, (admin) =>
        admin.unsafe(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`),
      ).catch(() => {});
    }
  }, 60_000);

  return infra;
}

async function withAdmin<T>(url: string, fn: (admin: postgres.Sql) => Promise<T>): Promise<T> {
  const admin = postgres(url, { max: 1, onnotice: () => {} });
  try {
    return await fn(admin);
  } finally {
    await admin.end({ timeout: 5 });
  }
}

/** Polls `condition` until it holds, failing with `what` on timeout. */
export async function waitFor(
  what: string,
  condition: () => Promise<boolean> | boolean,
  timeoutMs = 20_000,
  intervalMs = 50,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastErr: unknown;
  while (Date.now() < deadline) {
    try {
      if (await condition()) return;
    } catch (err) {
      lastErr = err;
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(
    `timed out after ${timeoutMs}ms waiting for: ${what}` +
      (lastErr ? ` (last error: ${(lastErr as Error).message})` : ""),
  );
}

/** Waits `ms`, for asserting that something does *not* happen. */
export function settle(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
