-- WorkflowRepository.cancelInstance writes 'canceled', which the enum never
-- had, so every cancel failed with invalid_text_representation.
ALTER TYPE "public"."workflow_status" ADD VALUE IF NOT EXISTS 'canceled';
