import type { Redis } from "ioredis";
import { LUA_THROTTLE_PROVIDER as LUA_THROTTLE } from "@/redis/index.js";
import { ApiRateLimiter } from "@/rate-limiter/index.js";

export { LUA_THROTTLE };

export interface ThrottleResult {
  allowed: boolean;
  retryAfterMs: number;
}

export async function throttleProvider(
  redis: Redis,
  channel: string,
  config: { limit: number; windowSeconds: number },
  logger: any,
): Promise<ThrottleResult> {
  const key = `rate-limit:provider:${channel}`;
  const now = Date.now();
  const zmember = `${now}:${Math.random()}`;

  const result =
    typeof redis.throttleProvider === "function"
      ? await redis.throttleProvider(
          key,
          now.toString(),
          config.windowSeconds.toString(),
          config.limit.toString(),
          zmember,
        )
      : ((await redis.eval(
          LUA_THROTTLE,
          1,
          key,
          now.toString(),
          config.windowSeconds.toString(),
          config.limit.toString(),
          zmember,
        )) as [number, number]);

  const allowed = result[0] === 1;
  const oldestTimestamp = result[1];

  let retryAfterMs = 0;
  if (!allowed) {
    retryAfterMs = Math.max(0, oldestTimestamp + config.windowSeconds * 1000 - now);
    logger.warn(
      { channel, limit: config.limit, windowSeconds: config.windowSeconds, retryAfterMs },
      "Provider rate limit hit — task must be rescheduled",
    );
  }

  return { allowed, retryAfterMs };
}

/**
 * Provider rate limit enforced through locally leased tokens.
 *
 * `throttleProvider` makes one Redis round trip per message against a single
 * sorted set per channel, so every delivery process in the cluster serialises
 * on one hot key and the set grows by one member per send. Here each process
 * leases a chunk of the window's budget and spends it in memory, contacting
 * Redis only when its chunk runs low. The window is a sliding estimate over
 * two fixed buckets rather than an exact log, the same trade the API ingress
 * limiter already makes.
 */
export class ProviderThrottle {
  private readonly limiters = new Map<number, ApiRateLimiter>();

  constructor(
    private readonly redis: Redis,
    private readonly logger?: any,
  ) {}

  async check(
    channel: string,
    config: { limit: number; windowSeconds: number },
  ): Promise<ThrottleResult> {
    const windowMs = Math.max(1, Math.round(config.windowSeconds * 1000));
    let limiter = this.limiters.get(windowMs);
    if (!limiter) {
      limiter = new ApiRateLimiter({
        redis: this.redis,
        windowMs,
        keyPrefix: "rate-limit:provider",
        logger: this.logger,
      });
      this.limiters.set(windowMs, limiter);
    }

    if (await limiter.check(channel, config.limit)) {
      return { allowed: true, retryAfterMs: 0 };
    }

    const retryAfterMs = windowMs - (Date.now() % windowMs);
    this.logger?.warn(
      { channel, limit: config.limit, windowSeconds: config.windowSeconds, retryAfterMs },
      "Provider rate limit hit — task must be rescheduled",
    );
    return { allowed: false, retryAfterMs };
  }
}
