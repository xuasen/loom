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
export type ClaudeSessionMode = "interactive" | "unattended";

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
}

export interface ClaudeWorker {
  worker_id: string;
  online: boolean;
  last_heartbeat?: string | null;
  model?: string | null;
}

export interface ClaudeConfig {
  enabled: boolean;
  workers: ClaudeWorker[];
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
}): Promise<ClaudeSession> {
  return apiFetch<ClaudeSession>("/api/claude/sessions", { method: "POST", body: JSON.stringify(body) });
}

export function sendClaudeMessage(id: string, message: string): Promise<ClaudeSession> {
  return apiFetch<ClaudeSession>(path(id, "/messages"), { method: "POST", body: JSON.stringify({ message }) });
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

export function interruptClaudeSession(id: string): Promise<void> {
  return apiFetch<void>(path(id, "/interrupt"), { method: "POST" });
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
