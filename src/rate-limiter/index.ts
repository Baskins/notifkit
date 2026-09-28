import type { Redis } from "@/index.js";
import { LRUCache } from "@/shared/index.js";
import { LUA_USER_THROTTLE, LUA_LEASE_API_RATE_LIMIT } from "@/redis/index.js";
import type { Logger } from "@/logger/index.js";

export { LUA_USER_THROTTLE, LUA_LEASE_API_RATE_LIMIT };

import { randomUUID } from "crypto";

export interface ThrottleResult {
  allowed: boolean;
  count: number;
  limit: number;
}

// ─── Per-project overrides ──────────────────────────────────────────────────

/**
 * Throttle overrides stored on the project row. `null` on either field means
 * "no override" — fall back to the process-wide default.
 */
export interface ProjectThrottleSettings {
  throttleLimit: number | null;
  throttleWindowHours: number | null;
}

const NO_OVERRIDES: ProjectThrottleSettings = {
  throttleLimit: null,
  throttleWindowHours: null,
};

export interface ProjectSettingsCacheOptions {
  maxSize?: number;
  ttlMs?: number;
}

/**
 * Caches per-project throttle overrides for the engine.
 *
 * The throttle check runs once per notification, so an uncached lookup here
 * would put a Postgres round trip on the hot path. Projects with no overrides
 * are cached as well — the common case must not cost a query per message.
 *
 * The TTL bounds staleness on its own; `invalidate()` exists so a settings
 * change published over pub/sub applies immediately rather than at expiry.
 */
export class ProjectSettingsCache {
  private readonly cache: LRUCache<string, ProjectThrottleSettings>;
  /**
   * Lookups already on the wire, keyed by project.
   *
   * The cache only fills once a query has come back, so without this a cold
   * project at the start of a campaign puts one query per in-flight message on
   * Postgres before the first answer lands — exactly when the database is
   * busiest. Followers wait on the leader's promise instead.
   */
  private readonly inFlight = new Map<string, Promise<ProjectThrottleSettings>>();

  constructor(
    private readonly load: (projectId: string) => Promise<ProjectThrottleSettings | null>,
    { maxSize = 1000, ttlMs = 60_000 }: ProjectSettingsCacheOptions = {},
  ) {
    this.cache = new LRUCache<string, ProjectThrottleSettings>(maxSize, ttlMs);
  }

  /**
   * Throws whatever the loader throws. The caller decides whether a settings
   * lookup failure should drop the message or fall back to defaults.
   */
  async get(projectId: string): Promise<ProjectThrottleSettings> {
    const cached = this.cache.get(projectId);
    if (cached) return cached;

    const existing = this.inFlight.get(projectId);
    if (existing) return existing;

    // Assigned before any callback below can run, since `load` cannot settle
    // within this synchronous block.
    let pending!: Promise<ProjectThrottleSettings>;
    pending = this.load(projectId)
      .then((settings) => {
        const resolved = settings ?? NO_OVERRIDES;
        // Only cache while this is still the current lookup: an invalidate()
        // that landed while the query was on the wire means the answer in hand
        // already describes the old settings.
        if (this.inFlight.get(projectId) === pending) {
          this.cache.set(projectId, resolved);
        }
        return resolved;
      })
      .finally(() => {
        // Cleared on failure too, so one bad lookup does not pin every later
        // caller to the same rejection.
        if (this.inFlight.get(projectId) === pending) {
          this.inFlight.delete(projectId);
        }
      });

    this.inFlight.set(projectId, pending);
    return pending;
  }

  invalidate(projectId: string): void {
    this.cache.delete(projectId);
    // A lookup that started before the change would write a stale value on
    // arrival; dropping it here sends the next caller back to the database.
    this.inFlight.delete(projectId);
  }

  clear(): void {
    this.cache.clear();
    this.inFlight.clear();
  }
}

// ─── UserThrottle ──────────────────────────────────────────────────────────
// True sliding window counter using Redis ZSET: max N sends per window per user.

export interface UserThrottleOptions {
  redis: Redis;
  maxPerHour?: number;
  windowHours?: number;
}

export interface ThrottleCheckOptions {
  /** Per-project cap for this window. `0` blocks every non-critical send. */
  limit?: number | null;
  /** Per-project window length in hours. */
  windowHours?: number | null;
  /** Future send time. The window is evaluated at that instant, not at now. */
  scheduledAt?: string;
  /**
   * Stable id of the message being counted. A retry of the same message is
   * then counted once, not once per attempt. Random when omitted.
   */
  messageId?: string;
}

/** Reject stored values that would make the window meaningless. */
function positiveOrNull(value: number | null | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : null;
}

/** A limit of 0 is a legitimate kill switch, so zero is allowed here. */
function nonNegativeOrNull(value: number | null | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}

export class UserThrottle {
  private readonly redis: Redis;
  private readonly maxPerHour: number;
  private readonly windowHours: number;

  constructor({ redis, maxPerHour = 3, windowHours = 1 }: UserThrottleOptions) {
    this.redis = redis;
    this.maxPerHour = maxPerHour;
    this.windowHours = windowHours;
  }

  /**
   * @param projectId Tenant that owns `userId`. User ids are caller-supplied
   *   external ids, so they collide across tenants and MUST be namespaced —
   *   otherwise one tenant's traffic throttles another's.
   * @param options Per-project overrides. Values that are absent, null, or
   *   nonsensical fall back to this instance's defaults.
   */
  async check(
    projectId: string,
    userId: string,
    priority?: string,
    options: ThrottleCheckOptions = {},
  ): Promise<ThrottleResult> {
    const limit = nonNegativeOrNull(options.limit) ?? this.maxPerHour;
    const windowHours = positiveOrNull(options.windowHours) ?? this.windowHours;

    if (priority === "critical") {
      return { allowed: true, count: 0, limit };
    }

    const windowMs = windowHours * 3600_000;
    const key = `throttle:${projectId}:user:${userId}`;
    const targetTime = options.scheduledAt ? new Date(options.scheduledAt).getTime() : Date.now();
    const windowStart = targetTime - windowMs;
    const memberId = options.messageId ?? randomUUID();

    // The key must outlive the window it is counting. For a future-dated send
    // that means surviving until targetTime plus one more window, so a task
    // scheduled for next week still counts against the right bucket.
    const windowSeconds = Math.ceil(windowMs / 1000);
    const ttlSeconds = Math.max(
      windowSeconds,
      Math.ceil((targetTime - Date.now()) / 1000) + windowSeconds,
    );

    const count =
      typeof this.redis.throttleUser === "function"
        ? await this.redis.throttleUser(key, windowStart, limit, targetTime, memberId, ttlSeconds)
        : ((await this.redis.eval(
            LUA_USER_THROTTLE,
            1,
            key,
            windowStart,
            limit,
            targetTime,
            memberId,
            ttlSeconds,
          )) as number);

    return { allowed: count <= limit, count, limit };
  }
}

// ─── ApiRateLimiter (In-Memory Token Leasing) ──────────────────────────────
// Leases token chunks from Redis sliding-window counters into local memory.
// Eliminates per-request Redis network roundtrips on API ingress.

export interface ApiRateLimiterOptions {
  redis: Redis;
  windowMs?: number;
  maxBatchSize?: number;
  logger?: Logger;
  /** Redis key namespace. Distinct limiters must not share one. */
  keyPrefix?: string;
}

/**
 * Computes dynamic token lease batch size based on RPM.
 * Targets ~5s of limit capacity so Redis is contacted at most ~12 times/min
 * regardless of instantaneous request rate. Bounded between 1 and maxBatchSize.
 */
export function calculateTokenBatchSize(limitRpm: number, maxBatchSize = 500): number {
  if (limitRpm <= 0) return 0;
  if (limitRpm <= 10) return 1;
  if (limitRpm <= 60) return 5;
  const perSecond = limitRpm / 60;
  // 5 s window = perSecond * 5; at least 10 to absorb jitter.
  return Math.min(Math.max(Math.ceil(perSecond * 5), 10), maxBatchSize);
}

interface ProjectTokenLeaseState {
  bucketIndex: number;
  tokens: number;
  batchSize: number;
  inFlightLease: Promise<number> | null;
}

export class ApiRateLimiter {
  private readonly redis: Redis;
  private readonly windowMs: number;
  private readonly maxBatchSize: number;
  private readonly logger?: Logger;
  private readonly keyPrefix: string;
  private readonly leases = new Map<string, ProjectTokenLeaseState>();

  constructor({
    redis,
    windowMs = 60_000,
    maxBatchSize = 500,
    logger,
    keyPrefix = "rate-limit:api:req",
  }: ApiRateLimiterOptions) {
    this.redis = redis;
    this.windowMs = windowMs;
    this.maxBatchSize = maxBatchSize;
    this.logger = logger;
    this.keyPrefix = keyPrefix;
  }

  /**
   * Evaluates if a request for `projectId` is allowed within its `limitRpm`.
   * Fast-path uses local in-memory token lease (0ms Redis network latency).
   * Refills in the background before tokens are depleted.
   */
  async check(projectId: string, limitRpm: number): Promise<boolean> {
    if (limitRpm <= 0) return false;

    const nowMs = Date.now();
    const currentBucket = Math.floor(nowMs / this.windowMs);

    let state = this.leases.get(projectId);
    if (!state) {
      state = {
        bucketIndex: currentBucket,
        tokens: 0,
        batchSize: calculateTokenBatchSize(limitRpm, this.maxBatchSize),
        inFlightLease: null,
      };
      this.leases.set(projectId, state);
      this.maybePrune(currentBucket);
    }

    // Rollover to new bucket window when minute boundary elapses.
    // Kick off a prefill immediately so the first request of the new window
    // doesn't have to wait — it will land before tokens hit 0 again.
    if (state.bucketIndex !== currentBucket) {
      state.bucketIndex = currentBucket;
      state.tokens = 0;
      state.batchSize = calculateTokenBatchSize(limitRpm, this.maxBatchSize);
      state.inFlightLease = null;
      // Fire the first lease for the new window now, don't await it here.
      this.refill(projectId, state, limitRpm, nowMs, currentBucket).catch(() => {});
    }

    // 1. Fast Path: Local token available in memory (0ms network latency)
    if (state.tokens > 0) {
      state.tokens--;

      // Background refill when tokens drop below 25% of batch size
      const refillThreshold = Math.max(1, Math.floor(state.batchSize * 0.25));
      if (state.tokens <= refillThreshold && !state.inFlightLease) {
        this.refill(projectId, state, limitRpm, nowMs, currentBucket).catch(() => {});
      }

      return true;
    }

    // 2. Slow Path: tokens are depleted, block on the in-flight lease or start one.
    // Multiple concurrent requests racing here all join the *same* promise so
    // only one Redis round trip happens.  After it resolves each waiter tries to
    // claim exactly one token — if they lose the race they're rate-limited.
    if (!state.inFlightLease) {
      this.refill(projectId, state, limitRpm, nowMs, currentBucket).catch(() => {});
    }

    if (state.inFlightLease) {
      await state.inFlightLease;
    }

    // Only claim a token if the lease came back for the same bucket we waited on.
    if (state.bucketIndex === currentBucket && state.tokens > 0) {
      state.tokens--;
      return true;
    }

    return false;
  }

  private async refill(
    projectId: string,
    state: ProjectTokenLeaseState,
    limitRpm: number,
    nowMs: number,
    bucketIndex: number,
  ): Promise<number> {
    if (state.inFlightLease) {
      return state.inFlightLease;
    }

    const prevBucket = bucketIndex - 1;
    const rlTag = `{${this.keyPrefix}:${projectId || "global"}}`;
    const currentKey = `${rlTag}:${bucketIndex}`;
    const prevKey = `${rlTag}:${prevBucket}`;
    const requested = state.batchSize;
    let inFlightRef: Promise<number> | null = null;
    const executeLease = async (): Promise<number> => {
      try {
        let granted: number;
        if (typeof this.redis.leaseApiRateLimit === "function") {
          granted = await this.redis.leaseApiRateLimit(
            currentKey,
            prevKey,
            nowMs,
            this.windowMs,
            limitRpm,
            requested,
          );
        } else {
          granted = (await this.redis.eval(
            LUA_LEASE_API_RATE_LIMIT,
            2,
            currentKey,
            prevKey,
            nowMs,
            this.windowMs,
            limitRpm,
            requested,
          )) as number;
        }

        // On invalid return or negative, fail-open
        if (typeof granted !== "number" || granted < 0) {
          granted = requested;
        }

        if (state.bucketIndex === bucketIndex) {
          state.tokens += granted;
        }
        return granted;
      } catch (err) {
        this.logger?.warn(
          { err, projectId },
          "project rate limit lease failed — allowing request (fail-open)",
        );
        // Fail-open: credit requested tokens locally to prevent tight retry loops during Redis hiccups
        if (state.bucketIndex === bucketIndex) {
          state.tokens += requested;
        }
        return requested;
      } finally {
        if (state.inFlightLease === inFlightRef) {
          state.inFlightLease = null;
        }
      }
    };

    const promise = executeLease();
    inFlightRef = promise;
    state.inFlightLease = promise;
    return promise;
  }

  private maybePrune(currentBucket: number): void {
    if (this.leases.size > 2000) {
      for (const [id, s] of this.leases) {
        if (currentBucket - s.bucketIndex > 1) {
          this.leases.delete(id);
        }
      }
    }
  }

  /** Reset all local cached leases */
  clear(): void {
    this.leases.clear();
  }

  /** Invalidate local lease for a specific project */
  invalidate(projectId: string): void {
    this.leases.delete(projectId);
  }
}
