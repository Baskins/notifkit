-- Both indexes are strict prefixes of others on the same table:
--   message_logs_project_idx (project_id)  <- every composite leading with project_id
--   task_idx (task_id)                     <- task_channel_attempt_uidx (task_id, ...)
-- message_logs takes two rows per delivered notification, so each index is
-- paid for on the hottest insert path in the system.
DROP INDEX IF EXISTS "message_logs_project_idx";--> statement-breakpoint
DROP INDEX IF EXISTS "task_idx";
