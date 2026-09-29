// The package entry point first, as in a real install. The services import
// through this barrel while some modules also import each other directly;
// vitest's module runner snapshots `export *` at evaluation time, so loading a
// leaf module first (the API loads the queue directly) leaves the barrel
// missing exports. Node's own ESM loader binds them lazily and is unaffected.
import "@/index.js";
import { createServer } from "node:net";
import { NotifkitServer, type NotifkitOptions } from "@/server.js";
import type { DeliveryResult, Transport } from "@/transport/index.js";
import type { NotificationChannel, NotificationDispatchedPayload } from "@/contracts/index.js";
import type { Infra } from "./infra.js";
import { waitFor } from "./infra.js";

export const ADMIN_KEY = "integration-admin-key-0123456789";

type Behaviour = (task: NotificationDispatchedPayload) => Promise<DeliveryResult> | DeliveryResult;

/**
 * A provider that records what it was asked to send. By default every send
 * succeeds; `behaviour` can fail, time out or reject specific destinations.
 */
export class RecordingTransport implements Transport {
  readonly sent: NotificationDispatchedPayload[] = [];
  behaviour?: Behaviour;

  constructor(
    readonly channel: NotificationChannel,
    options: { behaviour?: Behaviour; limits?: { limit: number; windowSeconds: number } } = {},
  ) {
    this.behaviour = options.behaviour;
    if (options.limits) (this as any).limits = options.limits;
  }

  async send(task: NotificationDispatchedPayload): Promise<DeliveryResult> {
    const { signal: _signal, ...copy } = task as any;
    const result = this.behaviour
      ? await this.behaviour(task)
      : { success: true, providerMessageId: `pm-${task.taskId}` };
    if (result.success) this.sent.push(structuredClone(copy));
    return result;
  }

  for(projectId: string): NotificationDispatchedPayload[] {
    return this.sent.filter((t) => t.projectId === projectId);
  }
}

export async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.once("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const port = (srv.address() as any).port as number;
      srv.close(() => resolve(port));
    });
  });
}

export interface Api {
  (
    method: string,
    path: string,
    body?: unknown,
    headers?: Record<string, string>,
  ): Promise<{
    status: number;
    body: any;
    headers: Headers;
  }>;
}

export function apiClient(
  baseUrl: string,
  token?: string,
  defaultHeaders: Record<string, string> = {},
): Api {
  return async (method, path, body, headers = {}) => {
    const res = await fetch(baseUrl + path, {
      method,
      headers: {
        ...(token ? { authorization: `Bearer ${token}` } : {}),
        ...(body !== undefined ? { "content-type": "application/json" } : {}),
        ...defaultHeaders,
        ...headers,
      },
      body: body === undefined ? undefined : typeof body === "string" ? body : JSON.stringify(body),
    });
    const text = await res.text();
    let parsed: any = text;
    try {
      parsed = text ? JSON.parse(text) : undefined;
    } catch {
      // not JSON — keep the text
    }
    return { status: res.status, body: parsed, headers: res.headers };
  };
}

export interface Running {
  server: NotifkitServer;
  baseUrl: string;
  admin: Api;
  /** Creates a project through the API and returns a client authenticated as it. */
  project(name?: string): Promise<{ id: string; apiKey: string; api: Api }>;
  stop(): Promise<void>;
}

/**
 * Starts a real NotifkitServer — API and workers in this process — against the
 * file's private Postgres and Redis. Environment overrides are applied before
 * start, since the services read their configuration once at boot.
 */
export async function startNotifkit(
  infra: Infra,
  options: Partial<NotifkitOptions> & { env?: Record<string, string> } = {},
): Promise<Running> {
  const port = await freePort();
  Object.assign(process.env, {
    ADMIN_API_KEY: ADMIN_KEY,
    HOST: "127.0.0.1",
    LOG_FLUSH_INTERVAL_MS: "50",
    ...options.env,
  });

  const { env: _env, ...serverOptions } = options;
  const server = new NotifkitServer({
    services: ["api", "enricher", "engine", "scheduler", "delivery", "events"],
    redisUrl: infra.redisUrl,
    databaseUrl: infra.dbUrl,
    logLevel: (process.env.TEST_LOG_LEVEL as any) ?? "silent",
    autoMigrate: false,
    port,
    ...serverOptions,
  });
  await server.start();

  const baseUrl = `http://127.0.0.1:${port}`;
  const services = serverOptions.services ?? ["api"];
  if (services.includes("api") || services.includes("all")) {
    await waitFor("api listening", async () => (await fetch(`${baseUrl}/live`)).ok, 10_000);
  }
  const admin = apiClient(baseUrl, ADMIN_KEY);

  return {
    server,
    baseUrl,
    admin,
    async project(name = "test") {
      const res = await admin("POST", "/v1/projects", { name });
      if (res.status !== 201)
        throw new Error(`create project failed: ${res.status} ${JSON.stringify(res.body)}`);
      return { id: res.body.id, apiKey: res.body.apiKey, api: apiClient(baseUrl, res.body.apiKey) };
    },
    async stop() {
      await server.stop();
    },
  };
}
