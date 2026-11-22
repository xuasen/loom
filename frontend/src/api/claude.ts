import { apiFetch, getAuthToken, getBaseUrl, tryRefreshToken } from "./client";

/**
 * Claude sessions API.
 *
 * A session is one ongoing Claude Code conversation on a private worker (like a
 * terminal running `claude`): each message is a turn and context carries over.
 * The worker numbers every step (seq) and stores it, so a session keeps going
 * after the user signs out and can be replayed. Reading a session = fetch
 * history (`/events?after=0`), then follow the SSE stream from the last seq.
 */

export type ClaudeSessionStatus = "IDLE" | "RUNNING" | "WAITING_APPROVAL" | "CLOSED" | "FAILED";
/** Claude Code permission modes (Shift+Tab in the CLI) + Loom's "unattended". */
export type ClaudeSessionMode = "default" | "acceptEdits" | "plan" | "auto" | "unattended";

export const MODE_LABELS: Record<ClaudeSessionMode, string> = {
  default: "Ask before acting",
  acceptEdits: "Accept edits",
  plan: "Plan mode",
  auto: "Auto mode",
  unattended: "Unattended (allowlist)",
};
/** The order Shift+Tab cycles through, like the CLI. */
export const MODE_CYCLE: ClaudeSessionMode[] = ["default", "acceptEdits", "plan", "auto"];

/** "us.anthropic.claude-sonnet-5-5" -> "Sonnet 5.5"; "…claude-opus-5" -> "Opus 5". */
export function modelLabel(id: string, labels?: Record<string, string>): string {
  const known = labels?.[id];
  if (known) return known;
  const m = id.match(/claude-([a-z]+)-(\d+)(?:-(\d{1,2}))?(?![\d])/);
  if (!m) return id;
  const family = `${m[1]![0]!.toUpperCase()}${m[1]!.slice(1)}`;
  return m[3] ? `${family} ${m[2]}.${m[3]}` : `${family} ${m[2]}`;
}

export const CLOSED_STATUSES: ReadonlySet<ClaudeSessionStatus> = new Set(["CLOSED", "FAILED"]);

export interface ClaudeSession {
  session_id: string;
  title: string;
  status: ClaudeSessionStatus;
  mode: ClaudeSessionMode;
  submitted_by: string;
  worker_id: string;
  created_at: string;
  updated_at?: string | null;
  last_seq: number;
  pending_count: number;
  turns: number;
  cost_usd?: number | null;
  cwd?: string | null;
  model?: string | null;
  pending_approvals?: string[];
  schedule_id?: string | null;
  /** "" = ungrouped; "/" separates nested folders. */
  folder?: string;
}

export type ClaudeEventType =
  | "user_message"
  | "text"
  | "tool_use"
  | "tool_result"
  | "approval_request"
  | "approval_decision"
  | "status"
  | "turn_end"
  | "mode"
  | "model"
  | "error";

export interface ClaudeEvent {
  seq: number;
  ts: string;
  type: ClaudeEventType;
  text?: string;
  partial?: boolean;
  by?: string;
  tool?: string;
  input?: unknown;
  output?: string;
  is_error?: boolean;
  approval_id?: string;
  decision?: "allow" | "deny";
  decided_by?: string;
  status?: ClaudeSessionStatus;
  message?: string;
  cost_usd?: number;
  mode?: ClaudeSessionMode;
  model?: string;
  attachments?: { name: string; size: number }[];
  /** user_message sent while a turn ran and passed to Claude mid-turn. */
  steer?: boolean;
  /** user_message sent by a schedule (its name). */
  scheduled?: string;
}

export interface ClaudeWorker {
  worker_id: string;
  online: boolean;
  last_heartbeat?: string | null;
  model?: string | null;
  projects?: string[];
  default_cwd?: string | null;
}

export interface ClaudeConfig {
  enabled: boolean;
  workers: ClaudeWorker[];
  models: string[];
  model_labels?: Record<string, string>;
  default_model?: string;
  modes: ClaudeSessionMode[];
  /** VAPID public key of the worker that sends notifications (null = push unavailable). */
  vapid_public_key?: string | null;
}

// ---- Web Push --------------------------------------------------------------

export function pushSupported(): boolean {
  return "serviceWorker" in navigator && "PushManager" in window && "Notification" in window;
}

function b64urlToBytes(b64: string): Uint8Array {
  const pad = "=".repeat((4 - (b64.length % 4)) % 4);
  const raw = atob((b64 + pad).replace(/-/g, "+").replace(/_/g, "/"));
  return Uint8Array.from(raw, (c) => c.charCodeAt(0));
}

async function swRegistration(): Promise<ServiceWorkerRegistration> {
  const existing = await navigator.serviceWorker.getRegistration("/");
  return existing ?? navigator.serviceWorker.register("/sw.js", { scope: "/" });
}

/** Current subscription of this browser, if it was made with this key. */
export async function currentPushSubscription(vapidKey: string): Promise<PushSubscription | null> {
  if (!pushSupported()) return null;
  const reg = await navigator.serviceWorker.getRegistration("/");
  const sub = await reg?.pushManager.getSubscription();
  if (!sub) return null;
  const key = sub.options.applicationServerKey;
  if (key && btoa(String.fromCharCode(...new Uint8Array(key))) !== btoa(String.fromCharCode(...b64urlToBytes(vapidKey)))) {
    return null; // made with another worker key: treat as not subscribed
  }
  return sub;
}

export async function enablePush(vapidKey: string): Promise<void> {
  const permission = await Notification.requestPermission();
  if (permission !== "granted") throw new Error("Notifications were not allowed in this browser");
  const reg = await swRegistration();
  await navigator.serviceWorker.ready;
  let sub = await reg.pushManager.getSubscription();
  if (sub && !(await currentPushSubscription(vapidKey))) {
    await sub.unsubscribe();
    sub = null;
  }
  sub ??= await reg.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: b64urlToBytes(vapidKey) });
  const json = sub.toJSON() as { endpoint: string; keys: { p256dh: string; auth: string } };
  await apiFetch("/api/claude/push/subscriptions", {
    method: "POST",
    body: JSON.stringify({ endpoint: json.endpoint, keys: json.keys, user_agent: navigator.userAgent.slice(0, 300) }),
  });
}

export async function disablePush(): Promise<void> {
  const reg = await navigator.serviceWorker.getRegistration("/");
  const sub = await reg?.pushManager.getSubscription();
  if (!sub) return;
  await apiFetch<void>("/api/claude/push/unsubscribe", { method: "POST", body: JSON.stringify({ endpoint: sub.endpoint }) });
  await sub.unsubscribe();
}

// ---- Schedules ---------------------------------------------------------------

export interface ClaudeSchedule {
  schedule_id: string;
  name: string;
  cron: string;
  tz: string;
  message: string;
  target: "session" | "new";
  session_id?: string | null;
  cwd?: string | null;
  model?: string | null;
  mode: ClaudeSessionMode;
  enabled: boolean;
  next_run?: string | null;
  last_run?: string | null;
  last_session_id?: string | null;
  last_error?: string | null;
  runs: number;
}

export interface NewSchedule {
  name: string;
  cron: string;
  tz: string;
  message: string;
  target: "session" | "new";
  session_id?: string;
  mode: ClaudeSessionMode;
  model?: string;
  cwd?: string;
}

export function listClaudeSchedules(): Promise<{ schedules: ClaudeSchedule[] }> {
  return apiFetch("/api/claude/schedules");
}

export function previewClaudeSchedule(cron: string, tz: string): Promise<{ next_runs: string[] }> {
  return apiFetch("/api/claude/schedules/preview", { method: "POST", body: JSON.stringify({ cron, tz }) });
}

export function createClaudeSchedule(body: NewSchedule): Promise<ClaudeSchedule> {
  return apiFetch("/api/claude/schedules", { method: "POST", body: JSON.stringify(body) });
}

export function updateClaudeSchedule(
  id: string,
  body: Partial<Pick<ClaudeSchedule, "enabled" | "name" | "cron" | "tz" | "message" | "mode">>,
): Promise<ClaudeSchedule> {
  return apiFetch(`/api/claude/schedules/${encodeURIComponent(id)}`, { method: "PATCH", body: JSON.stringify(body) });
}

export function runClaudeScheduleNow(id: string): Promise<{ queued: boolean }> {
  return apiFetch(`/api/claude/schedules/${encodeURIComponent(id)}/run`, { method: "POST" });
}

export function deleteClaudeSchedule(id: string): Promise<void> {
  return apiFetch<void>(`/api/claude/schedules/${encodeURIComponent(id)}`, { method: "DELETE" });
}

export interface SessionStateFrame {
  status: ClaudeSessionStatus;
  pending_count: number;
}

const path = (id: string, suffix = "") => `/api/claude/sessions/${encodeURIComponent(id)}${suffix}`;

export function getClaudeConfig(): Promise<ClaudeConfig> {
  return apiFetch<ClaudeConfig>("/api/claude/config");
}

export function listClaudeSessions(): Promise<{ sessions: ClaudeSession[] }> {
  return apiFetch<{ sessions: ClaudeSession[] }>("/api/claude/sessions");
}

export function getClaudeSession(id: string): Promise<ClaudeSession> {
  return apiFetch<ClaudeSession>(path(id));
}

export function createClaudeSession(body: {
  message: string;
  mode: ClaudeSessionMode;
  worker_id?: string;
  cwd?: string;
  model?: string;
  attachments?: string[];
  folder?: string;
}): Promise<ClaudeSession> {
  return apiFetch<ClaudeSession>("/api/claude/sessions", { method: "POST", body: JSON.stringify(body) });
}

export function sendClaudeMessage(id: string, message: string, attachments: string[] = []): Promise<ClaudeSession> {
  return apiFetch<ClaudeSession>(path(id, "/messages"), {
    method: "POST",
    body: JSON.stringify({ message, attachments }),
  });
}

export function getClaudeEvents(
  id: string,
  after: number,
): Promise<{ events: ClaudeEvent[]; status: ClaudeSessionStatus; pending_count: number }> {
  return apiFetch(path(id, `/events?after=${after}`));
}

export function decideClaudeApproval(id: string, approvalId: string, decision: "allow" | "deny"): Promise<void> {
  return apiFetch<void>(path(id, `/approvals/${encodeURIComponent(approvalId)}`), {
    method: "POST",
    body: JSON.stringify({ decision }),
  });
}

export interface UploadedFile {
  file_id: string;
  name: string;
  size: number;
  content_type: string;
}

/** Upload one attachment (any type) through the authenticated backend route. */
export async function uploadClaudeFile(file: File): Promise<UploadedFile> {
  const send = (token: string | null) => {
    const form = new FormData();
    form.append("file", file, file.name);
    return fetch(`${getBaseUrl()}/api/claude/files`, {
      method: "POST",
      body: form,
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
  };
  let response = await send(getAuthToken());
  if (response.status === 401) {
    const refreshed = await tryRefreshToken();
    if (refreshed) response = await send(refreshed);
  }
  if (!response.ok) {
    let detail = `HTTP ${response.status}`;
    try {
      const body = (await response.json()) as { detail?: unknown };
      if (typeof body.detail === "string") detail = body.detail;
    } catch {
      /* keep default */
    }
    throw new Error(detail);
  }
  return response.json() as Promise<UploadedFile>;
}

export function updateClaudeSettings(
  id: string,
  settings: { mode?: ClaudeSessionMode; model?: string },
): Promise<ClaudeSession> {
  return apiFetch<ClaudeSession>(path(id, "/settings"), { method: "POST", body: JSON.stringify(settings) });
}

export function interruptClaudeSession(id: string): Promise<void> {
  return apiFetch<void>(path(id, "/interrupt"), { method: "POST" });
}

export function setClaudeSessionFolder(id: string, folder: string): Promise<ClaudeSession> {
  return apiFetch<ClaudeSession>(path(id, "/folder"), { method: "POST", body: JSON.stringify({ folder }) });
}

export function deleteClaudeSession(id: string): Promise<void> {
  return apiFetch<void>(path(id, "/delete"), { method: "POST" });
}

export function closeClaudeSession(id: string): Promise<void> {
  return apiFetch<void>(path(id, "/close"), { method: "POST" });
}

/**
 * Follow the live stream (SSE over fetch so the bearer token stays in the
 * Authorization header, not the URL). Resolves when the server ends the stream.
 */
export async function streamClaudeEvents(
  id: string,
  after: number,
  onEvent: (event: ClaudeEvent) => void,
  onState: (state: SessionStateFrame) => void,
  signal: AbortSignal,
): Promise<void> {
  const url = `${getBaseUrl()}${path(id, `/stream?after=${after}`)}`;
  const open = (token: string | null) =>
    fetch(url, {
      headers: { Accept: "text/event-stream", ...(token ? { Authorization: `Bearer ${token}` } : {}) },
      signal,
    });

  let response = await open(getAuthToken());
  if (response.status === 401) {
    const refreshed = await tryRefreshToken();
    if (refreshed) response = await open(refreshed);
  }
  if (!response.ok || !response.body) throw new Error(`stream HTTP ${response.status}`);

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return;
    buffer += decoder.decode(value, { stream: true });
    let boundary = buffer.indexOf("\n\n");
    while (boundary !== -1) {
      const block = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      let eventName = "message";
      const dataLines: string[] = [];
      for (const line of block.split("\n")) {
        if (line.startsWith("event:")) eventName = line.slice(6).trim();
        else if (line.startsWith("data:")) dataLines.push(line.slice(5).trimStart());
      }
      if (dataLines.length > 0) {
        try {
          const payload = JSON.parse(dataLines.join("\n")) as unknown;
          if (eventName === "session") onState(payload as SessionStateFrame);
          else onEvent(payload as ClaudeEvent);
        } catch {
          // ignore a malformed frame; the next history fetch fills any gap
        }
      }
      boundary = buffer.indexOf("\n\n");
    }
  }
}
