import { describe, it, expect, vi, beforeEach } from "vitest";
import type * as Bootstrap from "@/workers/bootstrap.js";

/**
 * Every replica of a worker must join its consumer group under its own name.
 * In containers each replica is PID 1, so a name built from the PID alone makes
 * every replica the same group member. Starting a worker twice in one process
 * reproduces that exactly: same PID, two starts.
 *
 * Startup is cut off right after the consumer is named, so nothing connects.
 */

const seen = vi.hoisted(() => [] as string[]);

vi.mock("@/workers/bootstrap.js", async (importOriginal) => {
  const actual = await importOriginal<typeof Bootstrap>();
  const logger = { info: () => {}, warn: () => {}, error: () => {}, debug: () => {} };
  return {
    ...actual,
    createWorkerRuntime: () => ({ logger, redis: { native: {} }, sql: {}, db: {} }),
    createStreamConsumer: (_runtime: unknown, options: { consumer: string }) => {
      seen.push(options.consumer);
      throw new Error("stop after naming the consumer");
    },
  };
});

import { startAiWorker } from "@/services/ai/main.js";
import { startEventWorker } from "@/services/events/main.js";
import { startSchedulerWorker } from "@/services/scheduler/main.js";
import { startWorkflowWorker } from "@/services/workflow/main.js";

beforeEach(() => {
  seen.length = 0;
});

describe.each([
  ["ai", startAiWorker],
  ["events", startEventWorker],
  ["scheduler", startSchedulerWorker],
  ["workflow", startWorkflowWorker],
] as const)("%s worker", (name, start) => {
  it("joins its consumer group under a name no other replica shares", async () => {
    await expect(start()).rejects.toThrow("stop after naming the consumer");
    await expect(start()).rejects.toThrow("stop after naming the consumer");

    expect(seen).toHaveLength(2);
    expect(seen[0]).not.toBe(seen[1]);
    for (const id of seen) expect(id.startsWith(`${name}-`)).toBe(true);
  });
});
