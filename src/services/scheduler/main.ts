import { randomUUID } from "node:crypto";
import { loadEnv, readBaseConfig } from "@/index.js";
import type { Logger } from "@/index.js";
import type { RedisClient, Redis } from "@/index.js";
import { type StreamConsumer, type PendingMessageScanner, type StreamMessage } from "@/index.js";
import { BaseWorker } from "@/index.js";
import {
  STREAMS,
  CONSUMER_GROUPS,
  registry,
  buildStreamEvent,
  type NotificationScheduledPayload,
} from "@/index.js";
import { getPriorityBucket, type WorkerOptions } from "@/shared/index.js";
import { scheduledPayloads } from "@/db/schema.js";
import { inArray } from "drizzle-orm";
import { startHealthReporter } from "@/workers/index.js";
import {
  createPriorityProducers,
  createStreamConsumer,
  createWorkerRuntime,
  shutdownWorker,
} from "@/workers/bootstrap.js";
// ─── Bootstrap ─────────────────────────────────────────────────────────────

loadEnv();
const config = readBaseConfig();

let logger: Logger;
let redis: RedisClient;
let sql: any;
let db: any;

let consumer: StreamConsumer;
let pendingScanner: PendingMessageScanner;
let worker: BaseWorker;
let healthInterval: NodeJS.Timeout | null = null;
let pollTimeout: ReturnType<typeof setTimeout> | null = null;
let isPolling = false;

export interface SchedulerWorkerOptions extends WorkerOptions {
  registry: any;
  redis: Redis;
}

export class SchedulerWorker extends BaseWorker {
  private readonly registry: any;
  private readonly redisCli: Redis;

  constructor(options: SchedulerWorkerOptions) {
    super(options);
    this.registry = options.registry;
    this.redisCli = options.redis;
  }
  async process(message: StreamMessage): Promise<void> {
    const { event } = message;

    const payloadResult = this.registry.safeParsePayload("notification.scheduled", event.payload);
    if (!payloadResult.success) {
      this.logger.warn(
        { messageId: message.id, issues: payloadResult.error.issues },
        "invalid notification.scheduled payload — skipping",
      );
      return;
    }

    const scheduled = payloadResult.data as NotificationScheduledPayload;
    const scheduledAt = new Date(scheduled.scheduledAt).getTime();

    const taskData = JSON.stringify({
      taskId: scheduled.taskId,
      enrichedEventId: scheduled.enrichedEventId,
      traceId: event.metadata.traceId,
    });

    const shard = parseInt(scheduled.taskId.slice(-1), 16) || 0;
    const zsetKey = `notif:scheduled:zset:${shard}`;
    await this.redisCli.zadd(zsetKey, scheduledAt, taskData);
    this.logger.debug(
      { taskId: scheduled.taskId, scheduledAt: scheduled.scheduledAt, shard },
      "scheduled task queued in ZSET",
    );
  }
}

export async function executeSchedulerPoll(
  redis: Redis,
  outboundProducers: any,
  logger: any,
  db: any,
): Promise<boolean> {
  const lockKey = "notif:lock:scheduler:poll";
  const lockOwner = randomUUID();
  let acquired = false;
  try {
    const lockAcquired = await redis.set(lockKey, lockOwner, "EX", 30, "NX");
    if (lockAcquired !== "OK") {
      return false; // Another instance is currently polling
    }
    acquired = true;

    const now = Date.now();
    const visibilityTimeout = 60000; // 60 seconds
    const perShardLimit = 100;
    const pipeline = redis.pipeline();

    for (let i = 0; i < 16; i++) {
      const shardKey = `notif:scheduled:zset:${i}`;
      pipeline.schedulerPoll(shardKey, now, perShardLimit, visibilityTimeout);
    }

    const results = await pipeline.exec();
    const perShard = results?.map((res: any) => (res[0] ? [] : ((res[1] ?? []) as string[]))) ?? [];
    const tasks = perShard.flat();
    // A shard that came back full may still have due tasks behind it, which is
    // what tells the caller to poll again rather than wait for the next tick.
    const anyShardFull = perShard.some((shard) => shard.length >= perShardLimit);

    if (tasks.length === 0) return false;

    const parsedTasks = tasks
      .map((t) => {
        try {
          return { taskStr: t, ...JSON.parse(t) };
        } catch (err) {
          logger.error({ taskStr: t, err }, "failed to parse scheduled task JSON");
          return null;
        }
      })
      .filter(Boolean) as any[];
    if (parsedTasks.length === 0) return false;

    const taskIds = parsedTasks.map((t) => t.taskId);
    const dbPayloads = await db
      .select()
      .from(scheduledPayloads)
      .where(inArray(scheduledPayloads.taskId, taskIds));
    const payloadMap = new Map(dbPayloads.map((row: any) => [row.taskId, row.payload]));

    const batchedEvents: Record<"critical" | "normal" | "low", Omit<any, "id" | "timestamp">[]> = {
      critical: [],
      normal: [],
      low: [],
    };

    const cleanupPipeline = redis.pipeline();
    const dbCleanupIds: string[] = [];

    for (let i = 0; i < parsedTasks.length; i++) {
      const { taskStr, taskId, traceId } = parsedTasks[i];
      const payload = payloadMap.get(taskId);

      if (!payload) {
        logger.warn({ taskId }, "scheduled payload not found in Postgres — skipping release");
        const shard = parseInt(taskId.slice(-1), 16) || 0;
        cleanupPipeline.zrem(`notif:scheduled:zset:${shard}`, taskStr);
        continue;
      }

      const dispatchPayload = payload as any;
      const p = getPriorityBucket(dispatchPayload.priority);

      batchedEvents[p].push(
        buildStreamEvent("notification.dispatched", dispatchPayload, "scheduler", traceId),
      );

      dbCleanupIds.push(taskId);
      const shard = parseInt(taskId.slice(-1), 16) || 0;
      cleanupPipeline.zrem(`notif:scheduled:zset:${shard}`, taskStr);
    }

    // Every bucket, not a hardcoded subset: a task binned into one that is not
    // published here has already been added to the cleanup list, so it would be
    // deleted from Redis and Postgres without ever being sent.
    for (const p of Object.keys(batchedEvents) as (keyof typeof batchedEvents)[]) {
      if (batchedEvents[p].length > 0) {
        const producer = outboundProducers[p] ?? outboundProducers["normal"]!;
        await producer!.publishBatch(batchedEvents[p]);
      }
    }

    if (dbCleanupIds.length > 0) {
      try {
        await db.delete(scheduledPayloads).where(inArray(scheduledPayloads.taskId, dbCleanupIds));
      } catch (err) {
        logger.error(
          { err },
          "failed to delete scheduled payloads from Postgres, continuing with Redis cleanup",
        );
      }
    }
    await cleanupPipeline.exec();

    logger.info({ count: parsedTasks.length }, "scheduled tasks released to outbound");

    // "Poll again immediately" — the caller loops while this is true. The old
    // test was `tasks.length === 100` against a total drawn from 16 shards, so
    // a real backlog reported "nothing more" and waited for the next tick.
    return anyShardFull;
  } catch (err) {
    logger.error({ err }, "error in scheduler polling loop");
    return false;
  } finally {
    if (acquired) {
      await redis.releaseLock(lockKey, lockOwner).catch(() => {});
    }
  }
}

export async function startSchedulerWorker() {
  ({ logger, redis, sql, db } = createWorkerRuntime(config, "scheduler"));
  ({ consumer, pendingScanner } = createStreamConsumer(
    { redis, logger },
    {
      stream: STREAMS.SCHEDULED,
      group: CONSUMER_GROUPS.SCHEDULER,
      consumer: `scheduler-${process.pid}`,
      batchSize: config.WORKER_CONCURRENCY,
    },
  ));

  const outboundProducers = createPriorityProducers(redis.native, logger, "OUTBOUND");

  // ─── Stage 4a: Scheduled notification processor ─────────────────────────────
  //
  // Reads from SCHEDULED stream, parses, and queues tasks in a Redis ZSET.
  // A background polling interval releases tasks when they are due.

  worker = new SchedulerWorker({
    consumer,
    pendingScanner,
    logger,
    concurrency: config.WORKER_CONCURRENCY,
    registry,
    redis: redis.native,
  });

  // ─── Polling Loop ─────────────────────────────────────────────────────────
  isPolling = true;
  const pollLoop = async (): Promise<void> => {
    if (!isPolling) return;
    try {
      const hasMore = await executeSchedulerPoll(redis.native, outboundProducers, logger, db);
      if (isPolling) {
        pollTimeout = setTimeout(() => void pollLoop(), hasMore ? 0 : 5000);
      }
    } catch (err) {
      logger.error({ err }, "scheduler poll loop error");
      if (isPolling) {
        pollTimeout = setTimeout(() => void pollLoop(), 5000);
      }
    }
  };
  void pollLoop();

  // ─── Health check interval ──────────────────────────────────────────────────

  healthInterval = startHealthReporter("scheduler", worker, redis, logger);

  logger.info({ env: config.NODE_ENV }, "scheduler starting");
  await worker.start();
}

// ─── Shutdown ──────────────────────────────────────────────────────────────

export async function stopSchedulerWorker(): Promise<void> {
  isPolling = false;
  await shutdownWorker("scheduler", {
    logger,
    timers: [healthInterval, pollTimeout],
    worker,
    sql,
    redis,
  });
  healthInterval = null;
  pollTimeout = null;
}
