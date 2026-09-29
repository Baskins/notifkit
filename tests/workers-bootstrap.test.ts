import { describe, it, expect, vi } from "vitest";

vi.mock("@/queue/index.js", () => {
  const record = (kind: string) =>
    vi.fn(function (this: any, options: any) {
      this.kind = kind;
      this.options = options;
    });
  return {
    StreamConsumer: record("consumer"),
    PendingMessageScanner: record("scanner"),
    StreamProducer: record("producer"),
  };
});

import {
  createPriorityProducers,
  createStreamConsumer,
  shutdownWorker,
  uniqueConsumerId,
} from "@/workers/bootstrap.js";
import { CONSUMER_GROUPS, OUTBOUND_STREAMS, STREAMS } from "@/contracts/streams.js";

const logger: any = { info: vi.fn() };

describe("createPriorityProducers", () => {
  it("maps each priority lane to its stage's stream", () => {
    const producers = createPriorityProducers({} as any, logger, "ENRICHED") as any;

    expect(producers.critical.options.stream).toBe(STREAMS.ENRICHED_CRITICAL);
    expect(producers.normal.options.stream).toBe(STREAMS.ENRICHED_NORMAL);
    expect(producers.low.options.stream).toBe(STREAMS.ENRICHED_LOW);
  });
});

describe("createStreamConsumer", () => {
  it("gives the consumer and its pending scanner the same stream, group and id", () => {
    const redis: any = { native: {} };
    const { consumer, pendingScanner } = createStreamConsumer(
      { redis, logger },
      {
        stream: OUTBOUND_STREAMS,
        group: CONSUMER_GROUPS.DELIVERY,
        consumer: "delivery-1",
        batchSize: 8,
        bufferAcks: true,
      },
    ) as any;

    for (const built of [consumer, pendingScanner]) {
      expect(built.options).toMatchObject({
        redis: redis.native,
        stream: [...OUTBOUND_STREAMS],
        group: CONSUMER_GROUPS.DELIVERY,
        consumer: "delivery-1",
      });
    }
    expect(consumer.options).toMatchObject({
      dlqStream: STREAMS.DEAD_LETTER,
      batchSize: 8,
      bufferAcks: true,
    });
  });
});

describe("uniqueConsumerId", () => {
  it("differs between two consumers in the same process", () => {
    expect(uniqueConsumerId("engine")).toMatch(/^engine-/);
    expect(uniqueConsumerId("engine")).not.toBe(uniqueConsumerId("engine"));
  });
});

describe("shutdownWorker", () => {
  it("stops timers and subscribers before draining, and closes connections last", async () => {
    const order: string[] = [];
    let fired = false;
    const timer = setTimeout(() => (fired = true), 0);

    await shutdownWorker("test", {
      logger,
      timers: [timer, null],
      subscriber: { disconnect: () => order.push("subscriber") },
      worker: { stop: async () => void order.push("worker") } as any,
      sql: { end: async () => void order.push("sql") } as any,
      redis: { disconnect: async () => void order.push("redis") } as any,
    });
    await new Promise((r) => setTimeout(r, 5));

    expect(fired).toBe(false);
    expect(order).toEqual(["subscriber", "worker", "sql", "redis"]);
  });

  it("tolerates a service that never started", async () => {
    await expect(shutdownWorker("test", {})).resolves.toBeUndefined();
  });
});
