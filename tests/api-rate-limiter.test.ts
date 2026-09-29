import { describe, it, expect, vi, beforeEach } from "vitest";
import { ApiRateLimiter, calculateTokenBatchSize } from "@/rate-limiter/index.js";

describe("ApiRateLimiter (In-Memory Token Leasing)", () => {
  describe("calculateTokenBatchSize", () => {
    it("returns 0 for non-positive limits", () => {
      expect(calculateTokenBatchSize(0)).toBe(0);
      expect(calculateTokenBatchSize(-10)).toBe(0);
    });

    it("returns 1 for low limit (<= 10 RPM)", () => {
      expect(calculateTokenBatchSize(5)).toBe(1);
      expect(calculateTokenBatchSize(10)).toBe(1);
    });

    it("returns 5 for medium-low limit (<= 60 RPM)", () => {
      expect(calculateTokenBatchSize(30)).toBe(5);
      expect(calculateTokenBatchSize(60)).toBe(5);
    });

    it("returns proportional batch size for higher RPMs", () => {
      // 600 RPM -> 10 req/s * 5 = 50
      expect(calculateTokenBatchSize(600)).toBe(50);
      // 1200 RPM -> 20 req/s * 5 = 100
      expect(calculateTokenBatchSize(1200)).toBe(100);
      // 6000 RPM -> 100 req/s * 5 = 500
      expect(calculateTokenBatchSize(6000)).toBe(500);
      // 60000 RPM -> 1000 req/s * 5 = 500 (maxBatchSize ceiling)
      expect(calculateTokenBatchSize(60000)).toBe(500);
    });

    it("respects maxBatchSize ceiling", () => {
      expect(calculateTokenBatchSize(10_000_000, 100)).toBe(100);
      expect(calculateTokenBatchSize(10_000_000, 500)).toBe(500);
    });
  });

  describe("In-Memory Token Leasing & Redis Interaction", () => {
    let mockRedis: any;
    let mockLogger: any;

    beforeEach(() => {
      mockRedis = {
        leaseApiRateLimit: vi.fn(),
      };
      mockLogger = {
        warn: vi.fn(),
        info: vi.fn(),
        debug: vi.fn(),
      };
    });

    it("satisfies subsequent requests from memory with 0 Redis calls once leased", async () => {
      // Grant 100 tokens on first lease request (1200 RPM -> batch size 100)
      mockRedis.leaseApiRateLimit.mockResolvedValue(100);

      const limiter = new ApiRateLimiter({
        redis: mockRedis,
        windowMs: 60_000,
        logger: mockLogger,
      });

      // Request 1: hits Redis to lease 100 tokens
      const res1 = await limiter.check("proj_1", 1200);
      expect(res1).toBe(true);
      expect(mockRedis.leaseApiRateLimit).toHaveBeenCalledTimes(1);

      // Requests 2, 3, 4, 5, 6, 7: served entirely from local memory (0ms network latency)
      // Batch size is 100, refill threshold is floor(100 * 0.25) = 25.
      for (let i = 0; i < 6; i++) {
        const allowed = await limiter.check("proj_1", 1200);
        expect(allowed).toBe(true);
      }

      // Redis was NOT called again for all 6 local requests
      expect(mockRedis.leaseApiRateLimit).toHaveBeenCalledTimes(1);
    });

    it("triggers background pre-fetch when tokens drop to refill threshold", async () => {
      // First call grants 5 tokens, second call grants 5 more
      mockRedis.leaseApiRateLimit.mockResolvedValue(5);

      const limiter = new ApiRateLimiter({
        redis: mockRedis,
        windowMs: 60_000,
        maxBatchSize: 5,
      });

      // Request 1: initial lease (5 tokens -> 4 remaining)
      await limiter.check("proj_1", 600);
      expect(mockRedis.leaseApiRateLimit).toHaveBeenCalledTimes(1);

      // Requests 2 and 3 (3 remaining, then 2 remaining)
      await limiter.check("proj_1", 600);
      await limiter.check("proj_1", 600);
      expect(mockRedis.leaseApiRateLimit).toHaveBeenCalledTimes(1);

      // Request 4: decrements to 1 (<= refillThreshold of 1) -> triggers background refill
      await limiter.check("proj_1", 600);
      expect(mockRedis.leaseApiRateLimit).toHaveBeenCalledTimes(2);
    });

    it("coalesces concurrent requests when local lease is empty", async () => {
      let resolveRedis: (val: number) => void;
      mockRedis.leaseApiRateLimit.mockImplementation(
        () =>
          new Promise<number>((resolve) => {
            resolveRedis = resolve;
          }),
      );

      const limiter = new ApiRateLimiter({
        redis: mockRedis,
        windowMs: 60_000,
      });

      // Fire 5 concurrent requests while token bucket is empty
      const p1 = limiter.check("proj_1", 600);
      const p2 = limiter.check("proj_1", 600);
      const p3 = limiter.check("proj_1", 600);
      const p4 = limiter.check("proj_1", 600);
      const p5 = limiter.check("proj_1", 600);

      // Only ONE lease request was dispatched to Redis
      expect(mockRedis.leaseApiRateLimit).toHaveBeenCalledTimes(1);

      // Resolve lease grant of 5 tokens
      resolveRedis!(5);

      const results = await Promise.all([p1, p2, p3, p4, p5]);
      expect(results).toEqual([true, true, true, true, true]);
      expect(mockRedis.leaseApiRateLimit).toHaveBeenCalledTimes(1);
    });

    it("rejects with false when Redis indicates limit is reached (0 tokens granted)", async () => {
      mockRedis.leaseApiRateLimit.mockResolvedValue(0);

      const limiter = new ApiRateLimiter({
        redis: mockRedis,
        windowMs: 60_000,
      });

      const allowed = await limiter.check("proj_exhausted", 600);
      expect(allowed).toBe(false);
      expect(mockRedis.leaseApiRateLimit).toHaveBeenCalledTimes(1);
    });

    it("discards expired lease and requests new bucket tokens on minute rollover", async () => {
      mockRedis.leaseApiRateLimit.mockResolvedValue(10);

      const limiter = new ApiRateLimiter({
        redis: mockRedis,
        windowMs: 60_000,
      });

      const originalNow = Date.now;
      try {
        let currentTime = 1_000_000;
        vi.spyOn(Date, "now").mockImplementation(() => currentTime);

        // In window 16 (1,000,000 / 60,000 = 16)
        await limiter.check("proj_1", 600);
        expect(mockRedis.leaseApiRateLimit).toHaveBeenCalledTimes(1);

        // Advance time to next minute (window 17: 1,060,000)
        currentTime = 1_060_001;

        await limiter.check("proj_1", 600);
        // A new lease for window 17 must be requested
        expect(mockRedis.leaseApiRateLimit).toHaveBeenCalledTimes(2);
      } finally {
        Date.now = originalNow;
      }
    });

    it("fails open when Redis errors to prevent dropping production traffic", async () => {
      mockRedis.leaseApiRateLimit.mockRejectedValue(new Error("Redis connection timed out"));

      const limiter = new ApiRateLimiter({
        redis: mockRedis,
        windowMs: 60_000,
        logger: mockLogger,
      });

      const allowed = await limiter.check("proj_1", 600);
      expect(allowed).toBe(true);
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.objectContaining({ projectId: "proj_1" }),
        expect.stringContaining("fail-open"),
      );
    });

    it("invalidates local lease when invalidate() or clear() is called", async () => {
      mockRedis.leaseApiRateLimit.mockResolvedValue(10);

      const limiter = new ApiRateLimiter({
        redis: mockRedis,
        windowMs: 60_000,
      });

      await limiter.check("proj_1", 600);
      expect(mockRedis.leaseApiRateLimit).toHaveBeenCalledTimes(1);

      limiter.invalidate("proj_1");

      // Next check must re-lease from Redis
      await limiter.check("proj_1", 600);
      expect(mockRedis.leaseApiRateLimit).toHaveBeenCalledTimes(2);

      limiter.clear();
      await limiter.check("proj_1", 600);
      expect(mockRedis.leaseApiRateLimit).toHaveBeenCalledTimes(3);
    });
  });
});
