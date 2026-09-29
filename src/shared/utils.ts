export function getPriorityBucket(priority: string | undefined): "critical" | "normal" | "low" {
  const p = priority || "normal";
  return p === "critical" ? "critical" : p === "low" ? "low" : "normal";
}

/**
 * Canonical form of a destination, for suppression lookups.
 *
 * Both the writer (the provider webhook) and the reader (the engine's
 * pre-dispatch gate) must agree on this, or an unsubscribe recorded as
 * `Bob@Example.com` will not match a send addressed to `bob@example.com` and
 * the person keeps receiving mail. Case folding is safe for email domains and
 * for the local part in every mailbox provider in practice; phone numbers and
 * push tokens are case-sensitive and are only trimmed.
 */
export function normaliseTarget(target: string): string {
  const trimmed = target.trim();
  return trimmed.includes("@") ? trimmed.toLowerCase() : trimmed;
}
