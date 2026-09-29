import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { RedisContainer, type StartedRedisContainer } from "@testcontainers/redis";
import postgres from "postgres";
import type { GlobalSetupContext } from "vitest/node";
import { createDatabase, runMigrations } from "../../../src/db/index.js";

/**
 * One Postgres and one Redis for the whole run.
 *
 * Every integration test file gets its own database, cloned from a migrated
 * template (see `useInfra`), and its own Redis logical database, so files run
 * in parallel without seeing each other's rows or keys. Starting a container
 * per file cost several seconds each and was the main reason real-infra tests
 * were rationed.
 */

export const TEMPLATE_DB = "notifkit_template";

declare module "vitest" {
  export interface ProvidedContext {
    pgAdminUrl: string;
    redisBaseUrl: string;
    /** Why the containers could not start, or "" when they did. */
    infraError: string;
  }
}

let pg: StartedPostgreSqlContainer | undefined;
let redis: StartedRedisContainer | undefined;

export default async function setup({ provide }: GlobalSetupContext) {
  try {
    await startContainers(provide);
  } catch (err) {
    // Without Docker the unit tests can still run. Each integration file then
    // fails on its own with this reason, rather than every file in the run
    // failing here — and rather than integration tests being skipped quietly.
    if (process.env.CI) throw err;
    provide("pgAdminUrl", "");
    provide("redisBaseUrl", "");
    provide("infraError", err instanceof Error ? err.message : String(err));
  }

  return async () => {
    await Promise.all([pg?.stop(), redis?.stop()]);
  };
}

async function startContainers(provide: GlobalSetupContext["provide"]) {
  [pg, redis] = await Promise.all([
    new PostgreSqlContainer("postgres:16-alpine").start(),
    new RedisContainer("redis:7-alpine")
      .withCommand(["redis-server", "--databases", "128"])
      .start(),
  ]);

  const adminUrl = pg!.getConnectionUri();
  const admin = postgres(adminUrl, { max: 1, onnotice: () => {} });
  await admin.unsafe(`CREATE DATABASE ${TEMPLATE_DB}`);
  await admin.end();

  const templateUrl = withDatabase(adminUrl, TEMPLATE_DB);
  const { db, sql } = createDatabase({ url: templateUrl, maxConnections: 1 });
  await runMigrations(db);
  await sql.end();

  provide("pgAdminUrl", adminUrl);
  provide("redisBaseUrl", redis!.getConnectionUrl());
  provide("infraError", "");
}

export function withDatabase(url: string, database: string): string {
  const parsed = new URL(url);
  parsed.pathname = `/${database}`;
  return parsed.toString();
}
