import { z } from "zod";
import { NotificationChannelSchema, NotificationPrioritySchema } from "@/contracts/common.js";
import { RecipientProfileSchema } from "./notification-enriched.js";

export const NotificationAiPendingPayloadSchema = z.object({
  projectId: z.string().uuid(),
  enrichedEventId: z.string().uuid(),
  recipientId: z.string().min(1),
  channel: NotificationChannelSchema,
  priority: NotificationPrioritySchema,
  templateId: z.string().min(1).optional(),
  templateVariables: z.record(z.string(), z.unknown()),
  recipient: RecipientProfileSchema,
  aiPrompts: z.record(z.string(), z.string()),
  scheduledAt: z.string().datetime().optional(),
  fallbackChain: z.array(NotificationChannelSchema).optional(),
  /**
   * The task id the engine assigned. Stable across replays, so a re-run of
   * this event dispatches the same task — which delivery's lease then sends
   * once — rather than a fresh one.
   */
  taskId: z.string().min(1).optional(),
  /** The contact address the engine resolved and cleared against suppressions. */
  destination: z.string().min(1).optional(),
  /** Headers for the provider, e.g. one-click unsubscribe. */
  deliveryHeaders: z.record(z.string(), z.string()).optional(),
  campaignId: z.string().min(1).max(128).optional(),
  workflowInstanceId: z.string().uuid().optional(),
});
export type NotificationAiPendingPayload = z.infer<typeof NotificationAiPendingPayloadSchema>;
