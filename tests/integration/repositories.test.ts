import { describe, it, expect, beforeEach } from "vitest";
import { randomUUID } from "node:crypto";
import {
  UserRepository,
  PreferenceRepository,
  ContactRepository,
  TemplateRepository,
  ProjectRepository,
  WorkflowRepository,
  SegmentRepository,
  AdminUserRepository,
} from "@/repositories/index.js";
import { useInfra } from "./support/infra.js";

const infra = useInfra();

let P: string; // project under test
let OTHER: string; // a second tenant, to prove isolation

async function newProject(name = "p") {
  const id = randomUUID();
  await infra.sql`INSERT INTO projects (id, name) VALUES (${id}, ${name})`;
  return id;
}

beforeEach(async () => {
  await infra.reset();
  P = await newProject("main");
  OTHER = await newProject("other");
});

function user(userId: string, extra: Record<string, unknown> = {}) {
  return { userId, segments: [], preferences: {}, ...extra } as any;
}

// ─── UserRepository ─────────────────────────────────────────────────────────

describe("UserRepository", () => {
  it("upsertFull stores the profile, segments, preferences and quiet hours", async () => {
    const repo = new UserRepository(infra.db);
    await repo.upsertFull(P, {
      userId: "u1",
      email: "a@x.com",
      language: "fr",
      timezone: "Europe/Paris",
      segments: ["vip", "beta"],
      preferences: {
        topics: { marketing: false, news: true },
        channels: { sms: false },
        quietHours: [{ start: "22:00", end: "07:00" }],
      },
    });

    const rec = await new UserRepository(infra.db).findRecordById(P, "u1");
    expect(rec).toMatchObject({
      userId: "u1",
      email: "a@x.com",
      language: "fr",
      timezone: "Europe/Paris",
      preferences: {
        topics: { marketing: false, news: true },
        channels: { sms: false },
        quietHours: [{ start: "22:00", end: "07:00" }],
      },
    });
    expect(rec!.segments.sort()).toEqual(["beta", "vip"]);
    expect(await repo.findById(P, "u1")).toEqual({
      userId: "u1",
      email: "a@x.com",
      language: "fr",
      timezone: "Europe/Paris",
    });
  });

  it("upsertFull on an existing user updates it and replaces quiet hours", async () => {
    const repo = new UserRepository(infra.db);
    await repo.upsertFull(
      P,
      user("u1", {
        email: "old@x.com",
        preferences: { quietHours: [{ start: "01:00", end: "02:00" }] },
      }),
    );
    await repo.upsertFull(
      P,
      user("u1", { email: "new@x.com", preferences: { topics: { a: false }, quietHours: [] } }),
    );

    const rec = await new UserRepository(infra.db).findRecordById(P, "u1");
    expect(rec!.email).toBe("new@x.com");
    expect(rec!.preferences.topics).toEqual({ a: false });
    expect(rec!.preferences.quietHours).toBeUndefined();
    const { n } = (
      await infra.sql`SELECT count(*)::int AS n FROM users WHERE project_id = ${P}`
    )[0]!;
    expect(n).toBe(1);
  });

  it("keeps the same external id separate per project", async () => {
    const repo = new UserRepository(infra.db);
    await repo.upsertFull(P, user("same", { email: "p@x.com" }));
    await repo.upsertFull(OTHER, user("same", { email: "o@x.com" }));
    expect((await repo.findById(P, "same"))!.email).toBe("p@x.com");
    expect((await repo.findById(OTHER, "same"))!.email).toBe("o@x.com");
    expect(await repo.findById(P, "nobody")).toBeNull();
    expect(await repo.findRecordById(P, "nobody")).toBeNull();
  });

  it("findRecordsByIds returns the users that exist and serves repeats from cache", async () => {
    const repo = new UserRepository(infra.db);
    await repo.upsertManyFull(P, [user("a"), user("b"), user("c")]);

    const first = await repo.findRecordsByIds(P, ["a", "b", "missing"]);
    expect(first.map((r) => r.userId).sort()).toEqual(["a", "b"]);

    // Once cached, a row deleted behind the repository's back is still served.
    await infra.sql`DELETE FROM users WHERE external_id = 'a'`;
    const second = await repo.findRecordsByIds(P, ["a", "c"]);
    expect(second.map((r) => r.userId).sort()).toEqual(["a", "c"]);
    expect(await repo.findRecordsByIds(P, [])).toEqual([]);
  });

  it("writes through the repository invalidate its cache", async () => {
    const repo = new UserRepository(infra.db);
    await repo.upsertFull(P, user("u", { email: "one@x.com" }));
    expect((await repo.findRecordById(P, "u"))!.email).toBe("one@x.com");

    await repo.updatePartial(P, "u", { email: "two@x.com" });
    expect((await repo.findRecordById(P, "u"))!.email).toBe("two@x.com");

    await repo.upsertFull(P, user("u", { email: "three@x.com" }));
    expect((await repo.findRecordById(P, "u"))!.email).toBe("three@x.com");

    await repo.delete(P, "u");
    expect(await repo.findRecordById(P, "u")).toBeNull();
  });

  it("upsertManyFull inserts many users, de-duplicating repeated rows", async () => {
    const repo = new UserRepository(infra.db);
    await repo.upsertManyFull(P, [
      user("a", { segments: ["s", "s"], preferences: { topics: { t: true } } }),
      user("a", { segments: ["s"], preferences: { topics: { t: false } } }),
      user("b", {
        preferences: { channels: { email: false }, quietHours: [{ start: "10:00", end: "11:00" }] },
      }),
    ]);

    const [a, b] = await Promise.all([repo.findRecordById(P, "a"), repo.findRecordById(P, "b")]);
    expect(a!.segments).toEqual(["s"]);
    expect(a!.preferences.topics).toEqual({ t: false });
    expect(b!.preferences.channels).toEqual({ email: false });
    expect(b!.preferences.quietHours).toEqual([{ start: "10:00", end: "11:00" }]);
    await repo.upsertManyFull(P, []);
  });

  it("upsertManyFull replaces quiet hours only for users that supplied them", async () => {
    const repo = new UserRepository(infra.db);
    await repo.upsertManyFull(P, [
      user("a", { preferences: { quietHours: [{ start: "01:00", end: "02:00" }] } }),
      user("b", { preferences: { quietHours: [{ start: "03:00", end: "04:00" }] } }),
    ]);
    await repo.upsertManyFull(P, [user("a", { preferences: { quietHours: [] } }), user("b")]);

    const fresh = new UserRepository(infra.db);
    expect((await fresh.findRecordById(P, "a"))!.preferences.quietHours).toBeUndefined();
    expect((await fresh.findRecordById(P, "b"))!.preferences.quietHours).toEqual([
      { start: "03:00", end: "04:00" },
    ]);
  });

  it("updatePartial changes only what it is given, and replaces segments", async () => {
    const repo = new UserRepository(infra.db);
    await repo.upsertFull(
      P,
      user("u", { email: "e@x.com", language: "en", timezone: "UTC", segments: ["a", "b"] }),
    );

    expect(
      await repo.updatePartial(P, "u", {
        language: "de",
        segments: ["c"],
        preferences: {
          topics: { x: false },
          channels: { push: false },
          quietHours: [{ start: "05:00", end: "06:00" }],
        },
      }),
    ).toBe(true);
    const rec = await new UserRepository(infra.db).findRecordById(P, "u");
    expect(rec).toMatchObject({
      email: "e@x.com",
      language: "de",
      timezone: "UTC",
      segments: ["c"],
      preferences: {
        topics: { x: false },
        channels: { push: false },
        quietHours: [{ start: "05:00", end: "06:00" }],
      },
    });

    expect(await repo.updatePartial(P, "missing", { language: "x" })).toBe(false);
    expect(await repo.updatePartial(OTHER, "u", { language: "x" })).toBe(false);
  });

  it("updatePartial can clear an email with null", async () => {
    const repo = new UserRepository(infra.db);
    await repo.upsertFull(P, user("u", { email: "e@x.com" }));
    await repo.updatePartial(P, "u", { email: null });
    expect((await repo.findById(P, "u"))!.email ?? null).toBeNull();
  });

  it("delete removes the user and everything hanging off it, in its own project only", async () => {
    const repo = new UserRepository(infra.db);
    const contacts = new ContactRepository(infra.db);
    await repo.upsertFull(P, user("u", { segments: ["s"] }));
    await repo.upsertFull(OTHER, user("u"));
    await contacts.upsert(P, "u", "email", "u@x.com", { topics: { t: false } });

    expect(await repo.delete(P, "u")).toBe(true);
    expect(await repo.delete(P, "u")).toBe(false);
    expect(await repo.findById(OTHER, "u")).not.toBeNull();
    const { n } = (await infra.sql`SELECT count(*)::int AS n FROM user_contacts`)[0]!;
    expect(n).toBe(0);
  });

  describe("list", () => {
    it("filters by search, segment, language, timezone and channel", async () => {
      const repo = new UserRepository(infra.db);
      await repo.upsertFull(
        P,
        user("alice", {
          email: "alice@corp.com",
          language: "en",
          timezone: "UTC",
          segments: ["vip"],
        }),
      );
      await repo.upsertFull(
        P,
        user("bob", { email: "bob@home.net", language: "fr", timezone: "Europe/Paris" }),
      );
      await new ContactRepository(infra.db).upsert(P, "bob", "sms", "+100");
      await repo.upsertFull(OTHER, user("alice-other"));

      const ids = async (f: any) =>
        (await repo.list(P, 50, undefined, f)).users.map((u) => u.userId).sort();
      expect(await ids(undefined)).toEqual(["alice", "bob"]);
      expect(await ids({ search: "CORP" })).toEqual(["alice"]);
      expect(await ids({ search: "bo" })).toEqual(["bob"]);
      expect(await ids({ segment: "vip" })).toEqual(["alice"]);
      expect(await ids({ language: "fr" })).toEqual(["bob"]);
      expect(await ids({ timezone: "UTC" })).toEqual(["alice"]);
      expect(await ids({ channel: "sms" })).toEqual(["bob"]);
    });

    it("pages through every user, including users imported in one batch", async () => {
      const repo = new UserRepository(infra.db);
      // A bulk import writes every row in one transaction, so they all share
      // one created_at.
      await repo.upsertManyFull(
        P,
        Array.from({ length: 25 }, (_, i) => user(`bulk-${i}`)),
      );

      const seen: string[] = [];
      let cursor: string | undefined;
      for (let page = 0; page < 10; page++) {
        const { users, nextCursor } = await repo.list(P, 10, cursor);
        seen.push(...users.map((u) => u.userId));
        if (!nextCursor) break;
        cursor = nextCursor;
      }
      expect(seen).toHaveLength(25);
      expect(new Set(seen).size).toBe(25);
    });
  });

  it("findUsersBySegment and findUsersByTopic stay inside the project", async () => {
    const repo = new UserRepository(infra.db);
    await repo.upsertFull(
      P,
      user("a", { segments: ["vip"], preferences: { topics: { news: true } } }),
    );
    await repo.upsertFull(
      P,
      user("b", { segments: ["vip"], preferences: { topics: { news: false } } }),
    );
    await repo.upsertFull(
      OTHER,
      user("c", { segments: ["vip"], preferences: { topics: { news: true } } }),
    );

    expect((await repo.findUsersBySegment(P, "vip")).sort()).toEqual(["a", "b"]);
    expect(await repo.findUsersBySegment(P, "none")).toEqual([]);
    expect(await repo.findUsersByTopic(P, "news")).toEqual(["a"]);
  });
});

// ─── PreferenceRepository ───────────────────────────────────────────────────

describe("PreferenceRepository", () => {
  it("reports topic opt-ins, defaulting to opted in", async () => {
    await new UserRepository(infra.db).upsertFull(
      P,
      user("u", { preferences: { topics: { promo: false, news: true } } }),
    );
    const prefs = new PreferenceRepository(infra.db);

    expect(await prefs.isOptedIn(P, "u", "promo")).toBe(false);
    expect(await prefs.isOptedIn(P, "u", "news")).toBe(true);
    expect(await prefs.isOptedIn(P, "u", "unknown")).toBe(true);
    expect(
      (await prefs.findByUserId(P, "u")).sort((a, b) => a.eventType.localeCompare(b.eventType)),
    ).toEqual([
      { userId: "u", eventType: "news", optedIn: true },
      { userId: "u", eventType: "promo", optedIn: false },
    ]);
    expect(await prefs.findByUserId(P, "nobody")).toEqual([]);
    expect(await prefs.findByUserId(OTHER, "u")).toEqual([]);
  });
});

// ─── ContactRepository ──────────────────────────────────────────────────────

describe("ContactRepository", () => {
  beforeEach(async () => {
    const users = new UserRepository(infra.db);
    await users.upsertManyFull(P, [user("u"), user("v")]);
    await users.upsertFull(OTHER, user("u"));
  });

  it("upsert stores a contact with its topic preferences", async () => {
    const repo = new ContactRepository(infra.db);
    await repo.upsert(P, "u", "email", "u@x.com", { topics: { promo: false } });
    await repo.upsert(P, "u", "email", "u@x.com", { topics: { promo: true, news: false } });

    const contacts = await repo.findByUserId(P, "u");
    expect(contacts).toHaveLength(1);
    expect(contacts[0]).toMatchObject({
      userId: "u",
      channel: "email",
      target: "u@x.com",
      active: true,
      preferences: { topics: { promo: true, news: false } },
    });
    expect(await repo.findByUserId(P, "v")).toEqual([]);
    expect(await repo.findByUserId(P, "nobody")).toEqual([]);
  });

  it("upsert for an unknown user is a no-op", async () => {
    const repo = new ContactRepository(infra.db);
    await repo.upsert(P, "ghost", "sms", "+1");
    const { n } = (await infra.sql`SELECT count(*)::int AS n FROM user_contacts`)[0]!;
    expect(n).toBe(0);
  });

  it("findActiveByUserIds returns only enabled contacts, grouped by user", async () => {
    const repo = new ContactRepository(infra.db);
    await repo.upsert(P, "u", "email", "u@x.com");
    await repo.upsert(P, "u", "sms", "+1");
    await repo.upsert(P, "v", "push", "tok");
    await repo.upsert(OTHER, "u", "email", "other@x.com");
    await repo.deactivate(P, "u", "sms", "+1");

    const map = await new ContactRepository(infra.db).findActiveByUserIds(P, ["u", "v", "nobody"]);
    expect(map.get("u")!.map((c) => c.target)).toEqual(["u@x.com"]);
    expect(map.get("v")!.map((c) => c.channel)).toEqual(["push"]);
    expect(map.has("nobody")).toBe(false);
    expect((await repo.findActiveByUserIds(P, [])).size).toBe(0);
  });

  it("carries each contact's topic opt-outs, so a per-address unsubscribe can be honoured", async () => {
    const repo = new ContactRepository(infra.db);
    await repo.upsert(P, "u", "email", "work@x.com", { topics: { promo: false } });
    const [contact] = (await repo.findActiveByUserIds(P, ["u"])).get("u")!;
    expect(contact!.preferences.topics).toEqual({ promo: false });
  });

  it("sees writes made through the repository immediately", async () => {
    const repo = new ContactRepository(infra.db);
    await repo.upsert(P, "u", "email", "a@x.com");
    expect((await repo.findActiveByUserIds(P, ["u"])).get("u")).toHaveLength(1);

    await repo.upsert(P, "u", "sms", "+2");
    expect((await repo.findActiveByUserIds(P, ["u"])).get("u")).toHaveLength(2);

    await repo.deactivate(P, "u", "sms", "+2");
    expect((await repo.findActiveByUserIds(P, ["u"])).get("u")).toHaveLength(1);

    await repo.delete(P, "u", "email", "a@x.com");
    expect((await repo.findActiveByUserIds(P, ["u"])).has("u")).toBe(false);

    await repo.upsertMany(P, [{ userId: "u", channel: "email", target: "b@x.com" }]);
    expect((await repo.findActiveByUserIds(P, ["u"])).get("u")).toHaveLength(1);
  });

  it("re-adding a deactivated contact turns it back on", async () => {
    const repo = new ContactRepository(infra.db);
    await repo.upsert(P, "u", "push", "tok");
    expect(await repo.deactivate(P, "u", "push", "tok")).toBe(true);
    expect((await repo.findByUserId(P, "u"))[0]!.active).toBe(false);
    await repo.upsert(P, "u", "push", "tok");
    expect((await repo.findByUserId(P, "u"))[0]!.active).toBe(true);
  });

  it("deactivate and delete report whether anything matched", async () => {
    const repo = new ContactRepository(infra.db);
    await repo.upsert(P, "u", "email", "u@x.com");
    expect(await repo.deactivate(P, "u", "email", "nope@x.com")).toBe(false);
    expect(await repo.deactivate(P, "ghost", "email", "u@x.com")).toBe(false);
    expect(await repo.delete(OTHER, "u", "email", "u@x.com")).toBe(false);
    expect(await repo.delete(P, "ghost", "email", "u@x.com")).toBe(false);
    expect(await repo.delete(P, "u", "email", "u@x.com")).toBe(true);
    expect(await repo.findByUserId(P, "u")).toEqual([]);
  });

  it("upsertMany adds contacts for known users with their topics, skipping unknown users", async () => {
    const repo = new ContactRepository(infra.db);
    await repo.upsertMany(P, [
      {
        userId: "u",
        channel: "email",
        target: "u@x.com",
        preferences: { topics: { promo: false } },
      },
      { userId: "v", channel: "sms", target: "+9" },
      { userId: "ghost", channel: "sms", target: "+0" },
    ]);
    await repo.upsertMany(P, []);

    expect((await repo.findByUserId(P, "u"))[0]!.preferences.topics).toEqual({ promo: false });
    expect((await repo.findByUserId(P, "v"))[0]!.target).toBe("+9");
    const { n } = (await infra.sql`SELECT count(*)::int AS n FROM user_contacts`)[0]!;
    expect(n).toBe(2);
    await repo.upsertMany(P, [{ userId: "ghost", channel: "sms", target: "+0" }]);
  });
});

// ─── TemplateRepository ─────────────────────────────────────────────────────

describe("TemplateRepository", () => {
  it("upserts, reads, lists and deletes templates per project", async () => {
    const repo = new TemplateRepository(infra.db);
    expect(await repo.upsertMany(P, [])).toBe(0);
    expect(
      await repo.upsertMany(P, [
        {
          id: "welcome",
          channel: "email",
          topics: ["onboarding"],
          content: { subject: "Hi" },
          aiPrompts: { intro: "say hi" },
        },
        { id: "otp", channel: "sms", content: { text: "{{code}}" } },
      ]),
    ).toBe(2);
    await repo.upsertMany(OTHER, [{ id: "welcome", channel: "push", content: {} }]);

    expect(await repo.findById(P, "welcome")).toEqual({
      id: "welcome",
      channel: "email",
      topics: ["onboarding"],
      content: { subject: "Hi" },
      aiPrompts: { intro: "say hi" },
    });
    expect((await repo.findById(P, "otp"))!.topics).toEqual([]);
    expect(await repo.findById(P, "nope")).toBeNull();
    expect((await repo.list(P)).map((t) => t.id).sort()).toEqual(["otp", "welcome"]);

    await repo.upsertMany(P, [
      { id: "welcome", channel: "email", topics: [], content: { subject: "Hello" } },
    ]);
    const updated = await repo.findById(P, "welcome");
    expect(updated!.content).toEqual({ subject: "Hello" });
    expect(updated!.aiPrompts).toBeNull();

    expect(await repo.delete(P, "welcome")).toBe(true);
    expect(await repo.delete(P, "welcome")).toBe(false);
    expect(await repo.findById(OTHER, "welcome")).not.toBeNull();
  });
});

// ─── ProjectRepository ──────────────────────────────────────────────────────

describe("ProjectRepository", () => {
  it("lists projects newest first", async () => {
    const newest = await newProject("newest");
    const list = await new ProjectRepository(infra.db).list();
    expect(list[0]!.id).toBe(newest);
    expect(list.map((p) => p.id)).toEqual(expect.arrayContaining([P, OTHER]));
  });

  it("reads and updates throttle and rate-limit settings", async () => {
    const repo = new ProjectRepository(infra.db);
    expect(await repo.findThrottleSettings(P)).toEqual({
      throttleLimit: null,
      throttleWindowHours: null,
    });
    expect(
      await repo.updateSettings(P, { throttleLimit: 5, throttleWindowHours: 2, rateLimitRpm: 100 }),
    ).toBe(true);
    expect(await repo.findThrottleSettings(P)).toEqual({
      throttleLimit: 5,
      throttleWindowHours: 2,
    });
    expect(await repo.updateSettings(P, { throttleLimit: null })).toBe(true);
    expect((await repo.findThrottleSettings(P))!.throttleLimit).toBeNull();
    expect(await repo.findThrottleSettings(randomUUID())).toBeNull();
    expect(await repo.updateSettings(randomUUID(), { throttleLimit: 1 })).toBe(false);
  });

  it("creates, lists and deletes API keys within a project", async () => {
    const repo = new ProjectRepository(infra.db);
    const a = await repo.createApiKey(P, "hash-a");
    const b = await repo.createApiKey(P, "hash-b", "read_only");
    const keys = await repo.listApiKeys(P);
    expect(keys.map((k) => k.id).sort()).toEqual([a.id, b.id].sort());
    expect(keys.find((k) => k.id === b.id)!.role).toBe("read_only");
    expect(keys[0]).not.toHaveProperty("keyHash");

    expect(await repo.deleteApiKey(OTHER, a.id)).toBe(false);
    expect(await repo.deleteApiKey(P, a.id)).toBe(true);
    expect(await repo.listApiKeys(P)).toHaveLength(1);
    await expect(repo.createApiKey(OTHER, "hash-b")).rejects.toThrow();
  });

  it("delete removes the project and all of its data, leaving other projects alone", async () => {
    const users = new UserRepository(infra.db);
    const contacts = new ContactRepository(infra.db);
    const repo = new ProjectRepository(infra.db);
    for (const pid of [P, OTHER]) {
      await users.upsertFull(
        pid,
        user("u", {
          segments: ["s"],
          preferences: {
            topics: { t: false },
            channels: { sms: false },
            quietHours: [{ start: "01:00", end: "02:00" }],
          },
        }),
      );
      await contacts.upsert(pid, "u", "email", "u@x.com", { topics: { t: false } });
      await new TemplateRepository(infra.db).upsertMany(pid, [
        { id: "t", channel: "email", content: {} },
      ]);
      await repo.createApiKey(pid, `hash-${pid}`);
      await infra.sql`INSERT INTO suppressions (project_id, channel, target, reason) VALUES (${pid}, 'email', 'u@x.com', 'manual')`;
      await infra.sql`INSERT INTO message_logs (project_id, task_id, channel, status) VALUES (${pid}, ${randomUUID()}, 'email', 'delivered')`;
      await infra.sql`INSERT INTO workflow_definitions (project_id, name, steps) VALUES (${pid}, 'wf', '[]')`;
      const [inst] =
        await infra.sql`INSERT INTO workflow_instances (project_id, name) VALUES (${pid}, 'wf') RETURNING id`;
      await infra.sql`INSERT INTO workflow_steps (project_id, instance_id, step_index, action) VALUES (${pid}, ${inst!.id}, '0', 'run')`;
    }

    expect(await repo.delete(P)).toBe(true);
    expect(await repo.delete(P)).toBe(false);

    for (const table of [
      "users",
      "templates",
      "project_api_keys",
      "suppressions",
      "message_logs",
      "workflow_definitions",
      "workflow_instances",
      "workflow_steps",
    ]) {
      const { mine } = (
        await infra.sql.unsafe(`SELECT count(*)::int AS mine FROM ${table} WHERE project_id = $1`, [
          P,
        ])
      )[0]!;
      const { theirs } = (
        await infra.sql.unsafe(
          `SELECT count(*)::int AS theirs FROM ${table} WHERE project_id = $1`,
          [OTHER],
        )
      )[0]!;
      expect({ table, mine }).toEqual({ table, mine: 0 });
      expect({ table, theirs }).toEqual({ table, theirs: 1 });
    }
    const { n } = (await infra.sql`SELECT count(*)::int AS n FROM user_contacts`)[0]!;
    expect(n).toBe(1);
  });
});

// ─── WorkflowRepository ─────────────────────────────────────────────────────

describe("WorkflowRepository", () => {
  async function instance(pid: string, status = "pending") {
    const [row] = await infra.sql`
      INSERT INTO workflow_instances (project_id, name, status) VALUES (${pid}, 'wf', ${status}) RETURNING id`;
    return row!.id as string;
  }

  it("lists a project's definitions", async () => {
    await infra.sql`INSERT INTO workflow_definitions (project_id, name, steps) VALUES (${P}, 'a', '[]'), (${OTHER}, 'b', '[]')`;
    const defs = await new WorkflowRepository(infra.db).listDefinitions(P);
    expect(defs.map((d) => d.name)).toEqual(["a"]);
  });

  it("returns an instance with its steps and waiters, only to its own project", async () => {
    const id = await instance(P);
    await infra.sql`INSERT INTO workflow_steps (project_id, instance_id, step_index, action, output) VALUES (${P}, ${id}, '0', 'notify', '{"ok":true}')`;
    await infra.sql`INSERT INTO workflow_waiters (project_id, instance_id, event_name, match_criteria, expires_at) VALUES (${P}, ${id}, 'paid', '{}', now() + interval '1 hour')`;

    const repo = new WorkflowRepository(infra.db);
    const got = await repo.getInstance(P, id);
    expect(got.id).toBe(id);
    expect(got.steps.map((s: any) => s.action)).toEqual(["notify"]);
    expect(got.waiters.map((w: any) => w.eventName)).toEqual(["paid"]);
    expect(await repo.getInstance(OTHER, id)).toBeNull();
    expect(await repo.getInstance(P, randomUUID())).toBeNull();
  });

  it("cancels a pending or running instance and drops its waiters", async () => {
    const repo = new WorkflowRepository(infra.db);
    const pending = await instance(P, "pending");
    const running = await instance(P, "running");
    const done = await instance(P, "completed");
    await infra.sql`INSERT INTO workflow_waiters (project_id, instance_id, event_name, match_criteria, expires_at) VALUES (${P}, ${pending}, 'e', '{}', now() + interval '1 hour')`;

    expect(await repo.cancelInstance(P, pending)).toBe(true);
    expect(await repo.cancelInstance(P, running)).toBe(true);
    expect(await repo.cancelInstance(P, done)).toBe(false);
    expect(await repo.cancelInstance(OTHER, pending)).toBe(false);

    const { status } = (
      await infra.sql`SELECT status FROM workflow_instances WHERE id = ${pending}`
    )[0]!;
    expect(status).toBe("canceled");
    const { n } = (await infra.sql`SELECT count(*)::int AS n FROM workflow_waiters`)[0]!;
    expect(n).toBe(0);
  });
});

// ─── SegmentRepository ──────────────────────────────────────────────────────

describe("SegmentRepository", () => {
  it("lists a project's distinct segments", async () => {
    const users = new UserRepository(infra.db);
    await users.upsertManyFull(P, [
      user("a", { segments: ["vip", "beta"] }),
      user("b", { segments: ["vip"] }),
    ]);
    await users.upsertFull(OTHER, user("c", { segments: ["secret"] }));
    expect((await new SegmentRepository(infra.db).listSegments(P)).sort()).toEqual(["beta", "vip"]);
  });
});

// ─── AdminUserRepository ────────────────────────────────────────────────────

describe("AdminUserRepository", () => {
  it("creates admins with a normalised email, finds them by email or username, and counts them", async () => {
    const repo = new AdminUserRepository(infra.db);
    expect(await repo.count()).toBe(0);
    const admin = await repo.create({
      email: "  Boss@Example.COM ",
      username: " boss ",
      passwordHash: "h1",
    });
    expect(admin.email).toBe("boss@example.com");
    expect(admin.username).toBe("boss");
    expect(admin.role).toBe("admin");

    expect((await repo.findByEmailOrUsername("BOSS@example.com"))!.id).toBe(admin.id);
    expect((await repo.findByEmailOrUsername(" boss "))!.id).toBe(admin.id);
    expect(await repo.findByEmailOrUsername("nobody")).toBeNull();
    expect((await repo.findById(admin.id))!.email).toBe("boss@example.com");
    expect(await repo.findById(randomUUID())).toBeNull();
    expect(await repo.count()).toBe(1);

    await expect(repo.create({ email: "boss@example.com", passwordHash: "x" })).rejects.toThrow();
  });

  it("updates a password and never lists hashes", async () => {
    const repo = new AdminUserRepository(infra.db);
    const a = await repo.create({ email: "a@x.com", passwordHash: "old" });
    await repo.create({ email: "b@x.com", passwordHash: "hb", role: "viewer" });

    expect(await repo.updatePassword(a.id, "new")).toBe(true);
    expect((await repo.findById(a.id))!.passwordHash).toBe("new");
    expect(await repo.updatePassword(randomUUID(), "x")).toBe(false);

    const list = await repo.list();
    expect(list.map((u) => u.email)).toEqual(["a@x.com", "b@x.com"]);
    expect(list.every((u) => !("passwordHash" in u))).toBe(true);
    expect(list[1]!.role).toBe("viewer");
  });
});
