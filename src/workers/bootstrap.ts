import type { Redis } from "ioredis";
import { STREAMS, type ConsumerGroup, type StreamName } from "@/contracts/streams.js";
import { createDatabase, type Db, type Sql } from "@/db/index.js";
import { createLogger, type LogLevel, type Logger } from "@/logger/index.js";
import { PendingMessageScanner, StreamConsumer, StreamProducer } from "@/queue/index.js";
import { RedisClient } from "@/redis/index.js";
import type { BaseWorker } from "./index.js";

// Shared start/stop plumbing for the pipeline services under src/services.
// Each service still owns its worker and anything specific to it (polling
// loops, pub/sub subscribers); this only covers the parts they all repeat.

export interface WorkerRuntime {
  logger: Logger;
  redis: RedisClient;
  sql: Sql;
  db: Db;
}

/** Logger, shared Redis connection and Postgres pool for one service. */
export function createWorkerRuntime(
  config: { LOG_LEVEL: LogLevel; REDIS_URL: string; DATABASE_URL: string },
  name: string,
  loggerName = name,
): WorkerRuntime {
  const logger = createLogger({ name: loggerName, level: config.LOG_LEVEL });
  const redis = RedisClient.shared({ url: config.REDIS_URL, name, logger });
  const { sql, db } = createDatabase({
    url: config.DATABASE_URL,
    applicationName: name,
    logger,
  });
  return { logger, redis, sql, db };
}

/**
 * A consumer id unique to this process, so replicas sharing a host or PID
 * namespace never read as the same group member.
 */
export function uniqueConsumerId(name: string): string {
  const host = process.env.HOSTNAME || process.pid;
  return `${name}-${host}-${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * A group consumer and the scanner that reclaims its stalled messages. The two
 * must agree on stream, group and consumer id, so they are built together.
 */
export function createStreamConsumer(
  runtime: { redis: RedisClient; logger: Logger },
  options: {
    stream: StreamName | readonly StreamName[];
    group: ConsumerGroup;
    consumer: string;
    batchSize: number;
    bufferAcks?: boolean;
    coalesceMs?: number;
  },
): { consumer: StreamConsumer; pendingScanner: PendingMessageScanner } {
  const { redis, logger } = runtime;
  const stream = (Array.isArray(options.stream) ? [...options.stream] : options.stream) as
    StreamName | StreamName[];
  const consumer = new StreamConsumer({
    redis: redis.native,
    stream,
    group: options.group,
    consumer: options.consumer,
    dlqStream: STREAMS.DEAD_LETTER,
    batchSize: options.batchSize,
    bufferAcks: options.bufferAcks,
    coalesceMs: options.coalesceMs,
    logger,
  });
  const pendingScanner = new PendingMessageScanner({
    redis: redis.native,
    stream,
    group: options.group,
    consumer: options.consumer,
    logger,
  });
  return { consumer, pendingScanner };
}

export type PriorityProducers = Record<"critical" | "normal" | "low", StreamProducer>;

/** One producer per priority lane of a pipeline stage. */
export function createPriorityProducers(
  redis: Redis,
  logger: Logger,
  stage: "INBOUND" | "ENRICHED" | "OUTBOUND",
): PriorityProducers {
  const producer = (stream: StreamName) => new StreamProducer({ redis, stream, logger });
  return {
    critical: producer(STREAMS[`${stage}_CRITICAL`]),
    normal: producer(STREAMS[`${stage}_NORMAL`]),
    low: producer(STREAMS[`${stage}_LOW`]),
  };
}

/**
 * Stops a service in dependency order: timers and subscribers first so nothing
 * new starts, then the worker drains, then the connections it was using close.
 */
export async function shutdownWorker(
  label: string,
  resources: {
    logger?: Logger;
    timers?: (NodeJS.Timeout | null | undefined)[];
    subscriber?: { disconnect(): void } | null;
    worker?: BaseWorker;
    sql?: Sql;
    redis?: RedisClient;
  },
): Promise<void> {
  const { logger, timers = [], subscriber, worker, sql, redis } = resources;
  logger?.info("shutdown initiated");
  // clearTimeout also cancels intervals in Node.
  for (const timer of timers) if (timer) clearTimeout(timer);
  subscriber?.disconnect();
  if (worker) await worker.stop();
  if (sql) await sql.end();
  if (redis) await redis.disconnect();
  logger?.info(`${label} stopped`);
}
