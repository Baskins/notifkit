import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { RedisContainer, type StartedRedisContainer } from "@testcontainers/redis";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { NotifkitServer } from "../../src/server.js";
import { createDatabase, runMigrations } from "../../src/db/index.js";
import { STREAMS, buildStreamEvent } from "../../src/contracts/index.js";
import type { Transport, DeliveryResult } from "../../src/transport/index.js";

/**
 * Redis loses everything mid-run: a FLUSHALL, a failover to an empty replica,
 * or a restart without persistence. Postgres survives.
 *
 * What can come back: the pipeline itself (streams and consumer groups are
 * recreated on demand) and anything whose state lives in Postgres — scheduled
 * sends keep their payload and send time in `scheduled_payloads`.
 *
 * What cannot: messages sitting in a stream at the moment of the wipe that no
 * stage has written to Postgres yet. Those are gone, so none of these tests
 * wipe while messages are in flight.
 */

class RecordingTransport implements Transport {
  readonly channel = "email";
  readonly taskIds: string[] = [];
  async send(task: { taskId: string }): Promise<DeliveryResult> {
    this.taskIds.push(task.taskId);
    return { success: true };
  }
}

async function waitFor(
  what: string,
  condition: () => Promise<boolean> | boolean,
  timeoutMs: number,
) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await condition()) return;
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`timed out after ${timeoutMs}ms waiting for: ${what}`);
}

describe("Redis data loss", () => {
  let pg: StartedPostgreSqlContainer;
  let redisContainer: StartedRedisContainer;
  let redis: Redis;
  let sql: any;
  let server: NotifkitServer;
  const transport = new RecordingTransport();
  const projectId = randomUUID();

  async function send(count: number, extra: Record<string, unknown> = {}) {
    for (let i = 0; i < count; i++) {
      const event = buildStreamEvent(
        "notification.requested",
        {
          projectId,
          target: { type: "user", userId: "wipe-user" },
          channels: ["email"],
          templateId: "welcome",
          // Critical skips the per-user throttle, which is not what is under test.
          priority: "critical",
          ...extra,
        } as any,
        "test",
        `trace-${randomUUID()}`,
      );
      const full = { ...event, id: randomUUID(), timestamp: new Date().toISOString() };
      await redis.xadd(STREAMS.INBOUND_NORMAL, "*", "data", JSON.stringify(full));
    }
  }

  async function scheduledZsetSize(): Promise<number> {
    let total = 0;
    for (let i = 0; i < 16; i++) total += await redis.zcard(`notif:scheduled:zset:${i}`);
    return total;
  }

  beforeAll(async () => {
    [pg, redisContainer] = await Promise.all([
      new PostgreSqlContainer("postgres:15-alpine").start(),
      new RedisContainer("redis:7-alpine").start(),
    ]);

    const database = createDatabase({ url: pg.getConnectionUri() });
    sql = database.sql;
    await runMigrations(database.db);

    const userId = randomUUID();
    await sql`INSERT INTO projects (id, name) VALUES (${projectId}, 'Wipe')`;
    await sql`INSERT INTO users (id, project_id, external_id) VALUES (${userId}, ${projectId}, 'wipe-user')`;
    await sql`INSERT INTO user_contacts (id, user_id, channel, target, is_primary) VALUES (${randomUUID()}, ${userId}, 'email', 'wipe@example.com', true)`;
    await sql`INSERT INTO templates (project_id, id, channel, topics, content) VALUES (${projectId}, 'welcome', 'email', '{}', '{"subject": "Hi", "text": "Hello"}')`;

    redis = new Redis(redisContainer.getConnectionUrl(), { maxRetriesPerRequest: null });

    server = new NotifkitServer({
      services: ["enricher", "engine", "scheduler", "delivery"],
      redisUrl: redisContainer.getConnectionUrl(),
      databaseUrl: pg.getConnectionUri(),
      logLevel: "warn",
      autoMigrate: false,
      providers: [transport],
    });
    await server.start();

    // Baseline: the pipeline works before anything is wiped.
    await send(5);
    await waitFor("baseline deliveries", () => transport.taskIds.length === 5, 30_000);
  }, 180_000);

  afterAll(async () => {
    await server?.stop().catch(() => {});
    await sql?.end().catch(() => {});
    redis?.disconnect();
    await Promise.all([pg?.stop(), redisContainer?.stop()]);
  }, 60_000);

  it("keeps delivering notifications sent after the wipe", async () => {
    const before = transport.taskIds.length;
    await redis.flushall();

    await send(10);
    await waitFor("10 post-wipe deliveries", () => transport.taskIds.length >= before + 10, 45_000);

    // Give stragglers a moment, then check nothing went out twice.
    await new Promise((r) => setTimeout(r, 2000));
    expect(transport.taskIds.length).toBe(before + 10);
    expect(new Set(transport.taskIds).size).toBe(transport.taskIds.length);
  }, 90_000);

  it("still delivers a notification that was scheduled before the wipe", async () => {
    const before = transport.taskIds.length;
    const sendAt = new Date(Date.now() + 8_000);
    await send(1, { scheduledAt: sendAt.toISOString() });

    // Wait until it is fully parked: payload in Postgres, task in the ZSET.
    await waitFor(
      "scheduled payload stored",
      async () => (await sql`SELECT count(*)::int AS n FROM scheduled_payloads`)[0].n === 1,
      30_000,
    );
    await waitFor(
      "scheduled task queued in Redis",
      async () => (await scheduledZsetSize()) === 1,
      30_000,
    );

    await redis.flushall();

    // Postgres still has the payload and its send time, so this is recoverable.
    await waitFor(
      "scheduled notification delivered after the wipe",
      () => transport.taskIds.length === before + 1,
      45_000,
    );
    expect(Date.now()).toBeGreaterThanOrEqual(sendAt.getTime());
  }, 120_000);
});
