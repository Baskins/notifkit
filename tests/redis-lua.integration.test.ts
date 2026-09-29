import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { RedisContainer, type StartedRedisContainer } from "@testcontainers/redis";
import { RedisClient } from "@/redis/index.js";
import { UserThrottle, ApiRateLimiter } from "@/rate-limiter/index.js";

/**
 * The Lua scripts run against a real Redis. Their whole reason to exist is
 * atomicity under concurrent callers, which a mock cannot exercise, so most of
 * these fire many calls at once and check the total that got through.
 */

let container: StartedRedisContainer;
let client: RedisClient;
let redis: RedisClient["native"];

beforeAll(async () => {
  container = await new RedisContainer("redis:7-alpine").start();
  client = new RedisClient({ url: container.getConnectionUrl() });
  redis = client.native;
}, 120_000);

afterAll(async () => {
  await client?.disconnect();
  await container?.stop();
});

beforeEach(async () => {
  await redis.flushall();
});

const HOUR = 3600_000;

describe("throttleUser (per-user sliding window)", () => {
  it("lets exactly `limit` distinct messages through and denies the next", async () => {
    const throttle = new UserThrottle({ redis, maxPerHour: 3 });
    const results = [];
    for (let i = 0; i < 5; i++) {
      results.push(await throttle.check("p1", "u1", "normal", { messageId: `m${i}` }));
    }
    expect(results.map((r) => r.allowed)).toEqual([true, true, true, false, false]);
  });

  it("never over-admits when many messages for one user race", async () => {
    const throttle = new UserThrottle({ redis, maxPerHour: 10 });
    const results = await Promise.all(
      Array.from({ length: 60 }, (_, i) =>
        throttle.check("p1", "u1", "normal", { messageId: `m${i}` }),
      ),
    );
    expect(results.filter((r) => r.allowed)).toHaveLength(10);
  });

  it("re-admits a replayed message without spending another slot", async () => {
    const throttle = new UserThrottle({ redis, maxPerHour: 2 });
    await throttle.check("p1", "u1", "normal", { messageId: "a" });
    await throttle.check("p1", "u1", "normal", { messageId: "b" });

    // At the limit: a crash replay of "a" must still go out, a new message not.
    expect((await throttle.check("p1", "u1", "normal", { messageId: "a" })).allowed).toBe(true);
    expect((await throttle.check("p1", "u1", "normal", { messageId: "c" })).allowed).toBe(false);
  });

  it("frees slots once earlier sends fall out of the window", async () => {
    const key = "throttle:p1:user:u1";
    const t0 = Date.now();
    for (const m of ["a", "b"]) {
      await redis.throttleUser(key, t0 - HOUR, 2, t0, m, 7200);
    }
    expect(await redis.throttleUser(key, t0 - HOUR, 2, t0, "c", 7200)).toBe(3); // denied

    const later = t0 + HOUR + 1;
    expect(await redis.throttleUser(key, later - HOUR, 2, later, "c", 7200)).toBe(1);
  });

  it("does not count sends scheduled for next week against a send right now", async () => {
    const throttle = new UserThrottle({ redis, maxPerHour: 2 });
    const nextWeek = new Date(Date.now() + 7 * 24 * HOUR).toISOString();
    await throttle.check("p1", "u1", "normal", { messageId: "later-1", scheduledAt: nextWeek });
    await throttle.check("p1", "u1", "normal", { messageId: "later-2", scheduledAt: nextWeek });

    // Nothing has been sent in the last hour, so this must go out.
    expect((await throttle.check("p1", "u1", "normal", { messageId: "now" })).allowed).toBe(true);
  });

  it("does not count a send from this morning against one scheduled for tomorrow", async () => {
    const throttle = new UserThrottle({ redis, maxPerHour: 1 });
    await throttle.check("p1", "u1", "normal", { messageId: "now" });

    const tomorrow = new Date(Date.now() + 24 * HOUR).toISOString();
    const result = await throttle.check("p1", "u1", "normal", {
      messageId: "tomorrow",
      scheduledAt: tomorrow,
    });
    expect(result.allowed).toBe(true);
  });

  it("does not forget today's sends when a future-dated send is checked", async () => {
    const throttle = new UserThrottle({ redis, maxPerHour: 2 });
    await throttle.check("p1", "u1", "normal", { messageId: "a1" });
    await throttle.check("p1", "u1", "normal", { messageId: "a2" });

    const tomorrow = new Date(Date.now() + 24 * HOUR).toISOString();
    await throttle.check("p1", "u1", "normal", { messageId: "b", scheduledAt: tomorrow });

    // a1 and a2 went out moments ago, so a third immediate send is over the limit.
    expect((await throttle.check("p1", "u1", "normal", { messageId: "c" })).allowed).toBe(false);
  });

  it("does not cut short the life of a key still holding next week's send", async () => {
    const throttle = new UserThrottle({ redis, maxPerHour: 5 });
    const nextWeek = new Date(Date.now() + 7 * 24 * HOUR).toISOString();
    await throttle.check("p1", "u1", "normal", { messageId: "later", scheduledAt: nextWeek });
    await throttle.check("p1", "u1", "normal", { messageId: "now" });

    // If the key expired in an hour, next week's send would stop counting.
    expect(await redis.ttl("throttle:p1:user:u1")).toBeGreaterThan(6 * 24 * 3600);
  });

  it("keeps each project's count separate for the same user id", async () => {
    const throttle = new UserThrottle({ redis, maxPerHour: 1 });
    expect((await throttle.check("p1", "u1", "normal", { messageId: "a" })).allowed).toBe(true);
    expect((await throttle.check("p2", "u1", "normal", { messageId: "a" })).allowed).toBe(true);
  });

  it("gives the key a TTL, so idle users do not accumulate forever", async () => {
    const throttle = new UserThrottle({ redis, maxPerHour: 5 });
    await throttle.check("p1", "u1", "normal", { messageId: "a" });
    const ttl = await redis.ttl("throttle:p1:user:u1");
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(3600);
  });
});

describe("leaseApiRateLimit (sliding window token leasing)", () => {
  const cur = "{rl:p1}:2";
  const prev = "{rl:p1}:1";
  const window = 60_000;
  const startOfBucket2 = 2 * window;

  it("never grants more than the limit in total, however requests interleave", async () => {
    const grants = await Promise.all(
      Array.from({ length: 100 }, () =>
        redis.leaseApiRateLimit(cur, prev, startOfBucket2 + 10, window, 100, 7),
      ),
    );
    expect(grants.reduce((a, b) => a + b, 0)).toBe(100);
    expect(grants.every((g) => g >= 0 && g <= 7)).toBe(true);
  });

  it("counts the previous window in proportion to how much of it still overlaps", async () => {
    await redis.set(prev, 100);
    // Halfway through the current bucket, half the previous count still applies.
    const granted = await redis.leaseApiRateLimit(
      cur,
      prev,
      startOfBucket2 + window / 2,
      window,
      100,
      1000,
    );
    expect(granted).toBe(50);
  });

  it("grants nothing for a zero limit", async () => {
    expect(await redis.leaseApiRateLimit(cur, prev, startOfBucket2, window, 0, 5)).toBe(0);
  });

  it("expires the bucket so counters do not live forever", async () => {
    await redis.leaseApiRateLimit(cur, prev, startOfBucket2, window, 100, 5);
    expect(await redis.ttl(cur)).toBeGreaterThan(0);
  });
});

describe("ApiRateLimiter across several processes", () => {
  it("admits at most the limit in total when the budget is shared by many instances", async () => {
    // An hour-long window keeps the run inside one bucket.
    const limiters = Array.from(
      { length: 5 },
      () => new ApiRateLimiter({ redis, windowMs: HOUR, keyPrefix: "rl-test" }),
    );
    const limit = 60;
    const decisions: boolean[] = [];
    for (let round = 0; round < 40; round++) {
      decisions.push(...(await Promise.all(limiters.map((l) => l.check("proj", limit)))));
    }
    const admitted = decisions.filter(Boolean).length;
    expect(admitted).toBeLessThanOrEqual(limit);
    // Leasing strands some tokens in each instance, but most of the budget
    // must still be usable or the limiter is effectively a lower limit.
    expect(admitted).toBeGreaterThanOrEqual(limit * 0.5);
  });
});

describe("schedulerPoll (claim due tasks with a visibility timeout)", () => {
  const key = "notif:scheduled:zset:0";

  it("returns only due tasks, oldest first, up to the limit", async () => {
    const now = 1_000_000;
    await redis.zadd(key, now - 30, "a", now - 20, "b", now - 10, "c", now + 10, "future");

    expect(await redis.schedulerPoll(key, now, 2, 60_000)).toEqual(["a", "b"]);
    expect(await redis.schedulerPoll(key, now, 10, 60_000)).toEqual(["c"]);
  });

  it("hides a claimed task until its visibility timeout, then hands it out again", async () => {
    const now = 1_000_000;
    await redis.zadd(key, now - 1, "t");

    expect(await redis.schedulerPoll(key, now, 10, 60_000)).toEqual(["t"]);
    expect(await redis.schedulerPoll(key, now + 59_999, 10, 60_000)).toEqual([]);
    // A poller that crashed before removing it must not lose the task.
    expect(await redis.schedulerPoll(key, now + 60_000, 10, 60_000)).toEqual(["t"]);
  });

  it("never gives the same task to two pollers racing on one shard", async () => {
    const now = 1_000_000;
    const tasks = Array.from({ length: 50 }, (_, i) => `task-${i}`);
    await redis.zadd(key, ...tasks.flatMap((t, i) => [now - 100 + i, t]));

    const claims = await Promise.all(
      Array.from({ length: 20 }, () => redis.schedulerPoll(key, now, 5, 60_000)),
    );
    const claimed = claims.flat();
    expect(new Set(claimed).size).toBe(claimed.length);
    expect(claimed.sort()).toEqual([...tasks].sort());
  });
});

describe("releaseLock / renewLock (owner-checked lock)", () => {
  const key = "lock:test";

  it("does not let a non-owner release or extend the lock", async () => {
    await redis.set(key, "owner-a", "EX", 30);

    expect(await redis.releaseLock(key, "owner-b")).toBe(0);
    expect(await redis.renewLock(key, "owner-b", 300)).toBe(0);
    expect(await redis.get(key)).toBe("owner-a");
    expect(await redis.ttl(key)).toBeLessThanOrEqual(30);
  });

  it("lets the owner extend and then release it", async () => {
    await redis.set(key, "owner-a", "EX", 30);

    expect(await redis.renewLock(key, "owner-a", 300)).toBe(1);
    expect(await redis.ttl(key)).toBeGreaterThan(30);
    expect(await redis.releaseLock(key, "owner-a")).toBe(1);
    expect(await redis.exists(key)).toBe(0);
  });

  it("does not delete a lock that expired and was taken over by someone else", async () => {
    await redis.set(key, "owner-a", "PX", 50);
    await new Promise((r) => setTimeout(r, 100));
    await redis.set(key, "owner-b", "EX", 30, "NX");

    // owner-a finishes late and tries to release what it thinks is its lock.
    expect(await redis.releaseLock(key, "owner-a")).toBe(0);
    expect(await redis.get(key)).toBe("owner-b");
  });
});
