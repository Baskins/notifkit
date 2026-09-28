import { describe, it, expect, vi } from "vitest";
import { insertMessageLogs, upsertOutboxProviderIds } from "@/db/bulk.js";

const log = (taskId: string) => ({
  projectId: "123e4567-e89b-12d3-a456-426614174000",
  taskId,
  providerMessageId: null,
  channel: "email",
  attempt: 1,
  kind: "attempt",
  status: "delivered",
  templateId: null,
  workflowInstanceId: null,
  campaignId: null,
});

describe("bulk writes", () => {
  it("sends one prepared statement with a column array per field", async () => {
    const unsafe = vi.fn().mockResolvedValue([]);
    await insertMessageLogs({ $client: { unsafe } }, [log("t1"), log("t2")]);

    expect(unsafe).toHaveBeenCalledTimes(1);
    const [query, params, options] = unsafe.mock.calls[0]!;
    expect(query).toContain("unnest(");
    expect(query).toContain("ON CONFLICT DO NOTHING");
    expect(params).toHaveLength(11);
    expect(params[10]).toEqual([null, null]);
    expect(params[1]).toEqual(["t1", "t2"]);
    expect(options).toEqual({ prepare: true });
  });

  it("falls back to Drizzle for handles without a postgres.js client", async () => {
    const onConflictDoNothing = vi.fn().mockResolvedValue(undefined);
    const db = {
      insert: vi.fn().mockReturnValue({ values: vi.fn(() => ({ onConflictDoNothing })) }),
    };

    await insertMessageLogs(db, [log("t1")]);

    expect(db.insert).toHaveBeenCalled();
    expect(onConflictDoNothing).toHaveBeenCalled();
  });

  it("does nothing for an empty batch", async () => {
    const unsafe = vi.fn();
    await insertMessageLogs({ $client: { unsafe } }, []);
    await upsertOutboxProviderIds({ $client: { unsafe } }, []);
    expect(unsafe).not.toHaveBeenCalled();
  });

  it("keeps only the last update per outbox row, which ON CONFLICT DO UPDATE requires", async () => {
    const unsafe = vi.fn().mockResolvedValue([]);
    await upsertOutboxProviderIds({ $client: { unsafe } }, [
      { taskId: "t1", channel: "email", destination: "a@x", providerMessageId: null },
      { taskId: "t1", channel: "email", destination: "a@x", providerMessageId: "p1" },
      { taskId: "t2", channel: "email", destination: "b@x", providerMessageId: "p2" },
    ]);

    const params = unsafe.mock.calls[0]![1];
    expect(params[0]).toEqual(["t1", "t2"]);
    expect(params[3]).toEqual(["p1", "p2"]);
  });
});
