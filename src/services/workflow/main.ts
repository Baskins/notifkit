import { randomUUID } from "node:crypto";
import { loadEnv, readBaseConfig } from "@/index.js";
import type { Logger } from "@/index.js";
import type { RedisClient, Redis } from "@/index.js";
import {
  type StreamConsumer,
  type PendingMessageScanner,
  StreamProducer,
  type StreamMessage,
} from "@/index.js";
import { BaseWorker } from "@/index.js";
import { STREAMS, CONSUMER_GROUPS, buildStreamEvent } from "@/index.js";
import {
  workflowInstances,
  workflowSteps,
  workflowWaiters,
  workflowDefinitions,
} from "@/db/schema.js";
import { eq, and } from "drizzle-orm";
import {
  workflowRegistry,
  SuspendExecutionError,
  buildStepNotifyPayload,
  type WorkflowContext,
  type WorkflowStepContext,
} from "@/workflows/index.js";
import { type WorkerOptions } from "@/shared/index.js";
import { startHealthReporter } from "@/workers/index.js";
import {
  createStreamConsumer,
  createWorkerRuntime,
  shutdownWorker,
  uniqueConsumerId,
} from "@/workers/bootstrap.js";

/** How long a workflow instance lock is held before it self-expires. */
const WORKFLOW_LOCK_TTL_SECONDS = 60;
/** Renew the lock well inside its TTL so long-running handlers keep it. */
const WORKFLOW_LOCK_RENEW_MS = (WORKFLOW_LOCK_TTL_SECONDS / 3) * 1000;
/** How long a claimed workflow timer stays invisible to other pollers. */
const WORKFLOW_TIMER_VISIBILITY_MS = 60_000;
/** Wake-ups for sleeping instances: waits, event timeouts, deferred resumes. */
export const WORKFLOW_TIMERS_KEY = "notif:workflow:timers";
/** How soon a resume that found its instance busy is tried again. */
const WORKFLOW_RESUME_RETRY_MS = 2_000;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

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
let pollInterval: NodeJS.Timeout | null = null;
let reaperInterval: NodeJS.Timeout | null = null;

let notificationProducer: StreamProducer;
let workflowProducer: StreamProducer;

export interface WorkflowWorkerOptions extends WorkerOptions {
  redis: Redis;
  db: any;
  workflowProducer: any;
  notificationProducer: any;
}

export class WorkflowWorker extends BaseWorker {
  private readonly redisCli: Redis;
  private readonly dbConn: any;
  private readonly workflowProducer: any;
  private readonly notificationProducer: any;

  private eventBuffer: {
    producer: any;
    event: any;
    resolve: (result?: { messageId: string; notificationId: string }) => void;
    reject: (err: any) => void;
  }[] = [];
  private flushTimer: NodeJS.Timeout | null = null;

  constructor(options: WorkflowWorkerOptions) {
    super(options);
    this.redisCli = options.redis;
    this.dbConn = options.db;
    this.workflowProducer = options.workflowProducer;
    this.notificationProducer = options.notificationProducer;

    this.flushTimer = setInterval(() => void this.flushWorkerBuffers(), 100);
  }

  override async stop(): Promise<void> {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    await this.flushWorkerBuffers();
    await super.stop();
  }

  private async flushWorkerBuffers(): Promise<void> {
    if (this.eventBuffer.length === 0) return;

    const events = this.eventBuffer;
    this.eventBuffer = [];

    try {
      const byProducer = new Map<any, typeof events>();
      for (const item of events) {
        if (!byProducer.has(item.producer)) byProducer.set(item.producer, []);
        byProducer.get(item.producer)!.push(item);
      }

      for (const [producer, batch] of byProducer) {
        const { messageIds, eventIds } = await producer.publishBatch(batch.map((b) => b.event));
        for (let i = 0; i < batch.length; i++) {
          const mId = messageIds[i];
          const eId = eventIds[i];
          if (mId && eId) batch[i]!.resolve({ messageId: mId, notificationId: eId });
        }
      }
    } catch (err: any) {
      this.logger.error({ err }, "failed to flush workflow worker buffer");
      for (const e of events) e.reject(err);
    }
  }

  /**
   * Re-delivers a resume through the timer ZSET a few seconds from now. The
   * poll loop publishes it once the instance is pending again, and drops it if
   * the instance has finished. The member carries no timestamp, so repeated
   * deferrals of one instance collapse into a single entry.
   */
  private async deferResume(payload: any): Promise<void> {
    await this.redisCli.zadd(
      WORKFLOW_TIMERS_KEY,
      Date.now() + WORKFLOW_RESUME_RETRY_MS,
      JSON.stringify({
        instanceId: payload.instanceId,
        name: payload.name,
        projectId: payload.projectId,
        input: payload.input ?? {},
      }),
    );
  }

  async process(message: StreamMessage): Promise<void> {
    const { event } = message;
    const publishPromises: Promise<void>[] = [];

    if (event.type !== "workflow.triggered" && event.type !== "workflow.resumed") {
      return;
    }

    const payload = event.payload as any;
    const name = payload.name;
    const instanceId = payload.instanceId;

    if (!name || !instanceId || !payload.projectId) {
      this.logger.warn("Missing name, instanceId, or projectId in workflow event");
      return;
    }

    let handler = workflowRegistry.get(name);
    if (!handler) {
      // Fallback: check dynamic JSON workflows
      const defRows = await this.dbConn
        .select()
        .from(workflowDefinitions)
        .where(
          and(
            eq(workflowDefinitions.projectId, payload.projectId),
            eq(workflowDefinitions.name, name),
          ),
        )
        .limit(1);

      if (defRows.length === 0) {
        this.logger.warn(
          { name, projectId: payload.projectId },
          "No handler or dynamic definition found for workflow",
        );
        return;
      }

      const def = defRows[0];
      handler = async ({ step }) => {
        const steps = def.steps as any[];
        for (const stepDef of steps) {
          if (stepDef.action === "notify") {
            // A payload target wins; naming none inherits the instance user.
            await step.notify(stepDef.payload);
          } else if (stepDef.action === "wait") {
            await step.wait(stepDef.duration);
          } else if (stepDef.action === "waitForEvent") {
            await step.waitForEvent(stepDef.event, stepDef.options);
          } else {
            this.logger.warn(
              { action: (stepDef as any)?.action, name },
              "unknown workflow step action",
            );
          }
        }
      };
    }

    const lockKey = `lock:workflow:${instanceId}`;
    const lockToken = randomUUID();
    const acquired = await this.redisCli.set(
      lockKey,
      lockToken,
      "EX",
      WORKFLOW_LOCK_TTL_SECONDS,
      "NX",
    );
    if (!acquired) {
      // Another process is mid-run on this instance — typically still
      // persisting the very suspension this resume answers, when the awaited
      // event arrived quickly. Acking here dropped the resume, and with the
      // waiter already consumed nothing would ever wake the instance again.
      this.logger.info({ instanceId }, "Workflow is locked by another process, deferring resume");
      await this.deferResume(payload);
      return;
    }

    // Keep the lock alive while the handler runs; without this a handler that
    // outlives the TTL lets a second resume execute the same steps in parallel.
    const renewTimer = setInterval(() => {
      void this.redisCli
        .renewLock(lockKey, lockToken, WORKFLOW_LOCK_TTL_SECONDS)
        .catch((err: unknown) => {
          this.logger.warn({ err, instanceId }, "failed to renew workflow lock");
        });
    }, WORKFLOW_LOCK_RENEW_MS);

    try {
      let instance = (
        await this.dbConn
          .select()
          .from(workflowInstances)
          .where(eq(workflowInstances.id, instanceId))
          .limit(1)
      )[0];
      if (!instance) {
        const rows = await this.dbConn
          .insert(workflowInstances)
          .values({
            id: instanceId,
            projectId: payload.projectId,
            name: name,
            status: "pending",
            input: payload.input || {},
          })
          .returning();
        instance = rows[0]!;
      }

      if (instance.status === "running") {
        // Its last holder died mid-run (the reaper will set it back to
        // pending) or has not finished suspending. Either way this resume
        // is still owed.
        await this.deferResume(payload);
        return;
      }
      if (instance.status !== "pending") {
        this.logger.info({ instanceId }, "Workflow is not pending, skipping");
        return;
      }

      await this.dbConn
        .update(workflowInstances)
        .set({ status: "running" })
        .where(eq(workflowInstances.id, instanceId));

      // Load existing steps
      const existingSteps = await this.dbConn
        .select()
        .from(workflowSteps)
        .where(eq(workflowSteps.instanceId, instanceId));
      const stepOutputMap = new Map<string, any>();
      for (const s of existingSteps) {
        stepOutputMap.set(s.stepIndex, s.output);
      }

      let currentStepIndex = 0;

      const stepProxy: WorkflowStepContext = {
        notify: async (args) => {
          const stepId = String(currentStepIndex++);
          if (stepOutputMap.has(stepId)) return stepOutputMap.get(stepId);

          // The step payload is the same shape as notify(), but the wire event
          // is not — translate rather than spread.
          const requested = buildStepNotifyPayload(
            args,
            instance!.input,
            payload.projectId,
            `wf-${instanceId}-${stepId}`,
            // The contract wants a uuid, which the API always issues; anything
            // else would fail validation and drop the notification.
            UUID_RE.test(instanceId) ? instanceId : undefined,
          );

          const result = await new Promise<{ messageId: string; notificationId: string }>(
            (resolve, reject) => {
              this.eventBuffer.push({
                producer: this.notificationProducer,
                event: buildStreamEvent(
                  "notification.requested",
                  requested as unknown as Record<string, unknown>,
                  "workflow",
                  event.metadata.traceId,
                ),
                resolve: resolve as any,
                reject,
              });
            },
          );

          const output = {
            success: true,
            messageId: result.messageId,
            notificationId: result.notificationId,
          };

          await this.dbConn.insert(workflowSteps).values({
            instanceId: instance!.id,
            projectId: payload.projectId,
            stepIndex: stepId,
            action: "notify",
            output,
          });

          return output;
        },
        wait: async (duration) => {
          const stepId = String(currentStepIndex++);
          if (stepOutputMap.has(stepId)) {
            // A resume that arrives early — a duplicate trigger, a replayed
            // message — must not cut the sleep short. The timer is still set.
            const until = Number(stepOutputMap.get(stepId)?.scheduledAt);
            if (Number.isFinite(until) && until > Date.now()) {
              throw new SuspendExecutionError("wait", { duration });
            }
            return;
          }

          // Simple duration parse (e.g. '2h' -> ms)
          let ms = 0;
          if (duration.endsWith("d")) ms = parseInt(duration) * 24 * 60 * 60 * 1000;
          else if (duration.endsWith("h")) ms = parseInt(duration) * 60 * 60 * 1000;
          else if (duration.endsWith("m")) ms = parseInt(duration) * 60 * 1000;
          else if (duration.endsWith("s")) ms = parseInt(duration) * 1000;
          else throw new Error(`Invalid wait duration: ${duration}`);

          const resumeAt = Date.now() + ms;

          // Persist the wake-up signal before committing the suspended state.
          // If the process dies after this point the source event is retried;
          // if it dies after the database write, the timer is already durable.
          await this.redisCli.zadd(
            WORKFLOW_TIMERS_KEY,
            resumeAt,
            JSON.stringify({
              instanceId: instance!.id,
              name: instance!.name,
              projectId: payload.projectId,
              input: instance!.input,
            }),
          );

          await this.dbConn.insert(workflowSteps).values({
            instanceId: instance!.id,
            projectId: payload.projectId,
            stepIndex: stepId,
            action: "wait",
            output: { scheduledAt: resumeAt },
          });

          throw new SuspendExecutionError("wait", { duration });
        },
        waitForEvent: async (eventName, options) => {
          const stepId = String(currentStepIndex++);
          if (stepOutputMap.has(stepId)) {
            const out = stepOutputMap.get(stepId);
            // Still waiting: neither the event nor the timeout has filled the
            // step in, so this resume is spurious.
            if (out === null || out === undefined) {
              throw new SuspendExecutionError("waitForEvent", { eventName });
            }
            if (typeof out === "object" && (out as any).timedOut === true) {
              return null;
            }
            return out;
          }

          options = options || {};
          options.timeout = options.timeout || "24h";
          options.match = options.match || {};

          let ms = 0;
          if (options.timeout.endsWith("d")) ms = parseInt(options.timeout) * 24 * 60 * 60 * 1000;
          else if (options.timeout.endsWith("h")) ms = parseInt(options.timeout) * 60 * 60 * 1000;
          else if (options.timeout.endsWith("m")) ms = parseInt(options.timeout) * 60 * 1000;
          else if (options.timeout.endsWith("s")) ms = parseInt(options.timeout) * 1000;
          else throw new Error(`Invalid waitForEvent timeout: ${options.timeout}`);

          const resumeAt = Date.now() + ms;

          await this.redisCli.zadd(
            WORKFLOW_TIMERS_KEY,
            resumeAt,
            JSON.stringify({
              instanceId: instance!.id,
              name: instance!.name,
              projectId: payload.projectId,
              input: instance!.input,
              isEventTimeout: true,
              eventName,
              stepId,
            }),
          );

          // The pending step before the waiter: an event that matches the
          // waiter the moment it exists must find a step to record itself in.
          await this.dbConn.insert(workflowSteps).values({
            instanceId: instance!.id,
            projectId: payload.projectId,
            stepIndex: stepId,
            action: "waitForEvent",
            output: null, // this will be updated by event worker or timeout
          });

          await this.dbConn.insert(workflowWaiters).values({
            instanceId: instance!.id,
            projectId: payload.projectId,
            eventName: eventName,
            matchCriteria: options.match,
            expiresAt: new Date(resumeAt),
          });

          throw new SuspendExecutionError("waitForEvent", { eventName });
        },
        run: async (stepName, fn) => {
          const stepId = String(currentStepIndex++);
          if (stepOutputMap.has(stepId)) return stepOutputMap.get(stepId);

          const result = await fn();
          await this.dbConn.insert(workflowSteps).values({
            instanceId: instance!.id,
            projectId: payload.projectId,
            stepIndex: stepId,
            action: "run",
            output: result,
          });
          return result;
        },
      };

      const ctx: WorkflowContext = {
        step: stepProxy,
        event: (instance!.input as any) || { user: { id: "unknown" } },
      };

      try {
        await handler(ctx);
        // If we reach here, workflow completed
        await this.dbConn
          .update(workflowInstances)
          .set({ status: "completed" })
          .where(eq(workflowInstances.id, instance!.id));
        this.logger.info({ instanceId: instance!.id }, "Workflow completed successfully");
      } catch (err: any) {
        if (err instanceof SuspendExecutionError || err.name === "SuspendExecutionError") {
          await this.dbConn
            .update(workflowInstances)
            .set({ status: "pending" })
            .where(eq(workflowInstances.id, instance!.id));
          this.logger.info({ instanceId: instance!.id, reason: err.reason }, "Workflow suspended");
        } else {
          this.logger.error({ err, instanceId: instance!.id }, "Workflow failed");
          await this.dbConn
            .update(workflowInstances)
            .set({ status: "failed" })
            .where(eq(workflowInstances.id, instance!.id));
        }
      }
    } finally {
      clearInterval(renewTimer);
      // Compare-and-delete: never release a lock a later process re-acquired.
      await this.redisCli.releaseLock(lockKey, lockToken);
    }

    await Promise.all(publishPromises);
  }
}

export function __injectForTests(r: any, d: any, wp: any, np: any) {
  redis = r;
  db = d;
  workflowProducer = wp;
  notificationProducer = np;
}

export async function startWorkflowWorker() {
  ({ logger, redis, sql, db } = createWorkerRuntime(config, "workflow", "workflow-worker"));
  notificationProducer = new StreamProducer({
    redis: redis.native,
    stream: STREAMS.INBOUND_NORMAL,
    logger,
  });
  workflowProducer = new StreamProducer({
    redis: redis.native,
    stream: STREAMS.WORKFLOW_INBOUND,
    logger,
  });
  ({ consumer, pendingScanner } = createStreamConsumer(
    { redis, logger },
    {
      stream: STREAMS.WORKFLOW_INBOUND,
      group: CONSUMER_GROUPS.WORKFLOW,
      consumer: uniqueConsumerId("workflow"),
      batchSize: config.WORKER_CONCURRENCY,
    },
  ));

  worker = new WorkflowWorker({
    consumer,
    pendingScanner,
    logger,
    concurrency: config.WORKER_CONCURRENCY,
    redis: redis.native,
    db,
    workflowProducer,
    notificationProducer,
  });

  // Polling loop for timers
  pollInterval = setInterval(() => {
    void (async () => {
      try {
        const now = Date.now();
        const tasks = await redis.native.schedulerPoll(
          WORKFLOW_TIMERS_KEY,
          now,
          100,
          WORKFLOW_TIMER_VISIBILITY_MS,
        );

        for (const taskStr of tasks) {
          const task = JSON.parse(taskStr);

          // Check if workflow is already completed/failed
          const inst = (
            await db
              .select()
              .from(workflowInstances)
              .where(eq(workflowInstances.id, task.instanceId))
              .limit(1)
          )[0];
          if (
            !inst ||
            inst.status === "completed" ||
            inst.status === "failed" ||
            inst.status === "canceled"
          ) {
            await redis.native.zrem(WORKFLOW_TIMERS_KEY, taskStr);
            continue;
          }
          // A timer can be observed while the worker is still persisting the
          // corresponding suspension. Leave the claimed member in place; its
          // visibility timeout will make it eligible once the instance becomes
          // pending instead of losing the wake-up signal.
          if (inst.status === "running") continue;

          if (task.isEventTimeout) {
            // It's a timeout for waitForEvent. Clean up only this step's waiter
            const deleted = await db
              .delete(workflowWaiters)
              .where(
                and(
                  eq(workflowWaiters.instanceId, task.instanceId),
                  eq(workflowWaiters.eventName, task.eventName),
                ),
              )
              .returning();

            // If the waiter was already deleted (by EventWorker when event arrived before timeout),
            // this timer is stale. Clean up from Redis and skip re-resuming the workflow.
            if (deleted.length === 0) {
              await redis.native.zrem(WORKFLOW_TIMERS_KEY, taskStr);
              continue;
            }

            // Record timedOut state on the pending waitForEvent step
            const steps = await db
              .select()
              .from(workflowSteps)
              .where(
                and(
                  eq(workflowSteps.instanceId, task.instanceId),
                  eq(workflowSteps.action, "waitForEvent"),
                ),
              );
            const pendingStep = steps.find((s: any) => s.output === null);
            if (pendingStep) {
              await db
                .update(workflowSteps)
                .set({ output: { timedOut: true } })
                .where(eq(workflowSteps.id, pendingStep.id));
            }
          }

          await workflowProducer.publish(
            buildStreamEvent("workflow.resumed", task, "scheduler", undefined),
          );
          await redis.native.zrem(WORKFLOW_TIMERS_KEY, taskStr);
          logger.info({ instanceId: task.instanceId }, "Workflow resumed from timer");
        }
      } catch (err) {
        logger.error({ err }, "error in workflow polling loop");
      }
    })();
  }, 5000);

  reaperInterval = setInterval(() => {
    void (async () => {
      try {
        const runningInstances = await db
          .select({ id: workflowInstances.id })
          .from(workflowInstances)
          .where(eq(workflowInstances.status, "running"));
        for (const inst of runningInstances) {
          const lockKey = `lock:workflow:${inst.id}`;
          const hasLock = await redis.native.exists(lockKey);
          if (!hasLock) {
            await db
              .update(workflowInstances)
              .set({ status: "pending" })
              .where(eq(workflowInstances.id, inst.id));
            logger.info({ instanceId: inst.id }, "Reaped stuck workflow instance (lock expired)");
          }
        }
      } catch (err) {
        logger.error({ err }, "error in stuck workflow reaper");
      }
    })();
  }, 60000);

  healthInterval = startHealthReporter("workflow", worker, redis, logger);

  logger.info("workflow worker starting");
  await worker.start();
}

export async function stopWorkflowWorker(): Promise<void> {
  await shutdownWorker("workflow worker", {
    logger,
    timers: [healthInterval, pollInterval, reaperInterval],
    worker,
    sql,
    redis,
  });
  healthInterval = null;
  pollInterval = null;
  reaperInterval = null;
}
