import { apiFetch, getAuthToken, getBaseUrl, tryRefreshToken } from "./client";

/**
 * Claude task runner API.
 *
 * Tasks run on a worker host that only makes outbound connections. The backend
 * signs a submitted task and hands it to the worker; the worker streams every
 * step back as a numbered event (seq) that is also stored, so a task keeps
 * running after the user signs out and its full history can be replayed.
 *
 * Reading a task = fetch history (`/events?after=0`), then follow the live
 * stream from the last seq; events are de-duplicated by seq.
 */

export type ClaudeTaskStatus =
  | "QUEUED"
  | "RUNNING"
  | "WAITING_APPROVAL"
  | "SUCCEEDED"
  | "FAILED"
  | "CANCELLED";

export type ClaudeTaskMode = "interactive" | "unattended";

export const TERMINAL_STATUSES: ReadonlySet<ClaudeTaskStatus> = new Set([
  "SUCCEEDED",
  "FAILED",
  "CANCELLED",
]);

export interface ClaudeTask {
  task_id: string;
  prompt: string;
  status: ClaudeTaskStatus;
  mode: ClaudeTaskMode;
  submitted_by: string;
  worker_id: string;
  created_at: string;
  started_at?: string | null;
  finished_at?: string | null;
  last_seq: number;
  result_summary?: string | null;
  parent_task_id?: string | null;
}

export type ClaudeEventType =
  | "text"
  | "tool_use"
  | "tool_result"
  | "approval_request"
  | "approval_decision"
  | "status"
  | "error"
  | "result";

export interface ClaudeEvent {
  seq: number;
  ts: string;
  type: ClaudeEventType;
  text?: string;
  tool?: string;
  input?: unknown;
  output?: string;
  is_error?: boolean;
  approval_id?: string;
  decision?: "allow" | "deny";
  decided_by?: string;
  status?: ClaudeTaskStatus;
  message?: string;
}

export interface ClaudeWorker {
  worker_id: string;
  online: boolean;
  last_heartbeat?: string | null;
}

export interface ClaudeConfig {
  enabled: boolean;
  workers: ClaudeWorker[];
}

export function getClaudeConfig(): Promise<ClaudeConfig> {
  return apiFetch<ClaudeConfig>("/api/claude/config");
}

export function listClaudeTasks(): Promise<{ tasks: ClaudeTask[] }> {
  return apiFetch<{ tasks: ClaudeTask[] }>("/api/claude/tasks");
}

export function getClaudeTask(taskId: string): Promise<ClaudeTask> {
  return apiFetch<ClaudeTask>(`/api/claude/tasks/${encodeURIComponent(taskId)}`);
}

export function createClaudeTask(body: {
  prompt: string;
  mode: ClaudeTaskMode;
  worker_id?: string;
  parent_task_id?: string;
}): Promise<ClaudeTask> {
  return apiFetch<ClaudeTask>("/api/claude/tasks", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function getClaudeEvents(
  taskId: string,
  after: number,
): Promise<{ events: ClaudeEvent[]; task_status: ClaudeTaskStatus }> {
  return apiFetch(`/api/claude/tasks/${encodeURIComponent(taskId)}/events?after=${after}`);
}

export function decideClaudeApproval(
  taskId: string,
  approvalId: string,
  decision: "allow" | "deny",
): Promise<void> {
  return apiFetch<void>(
    `/api/claude/tasks/${encodeURIComponent(taskId)}/approvals/${encodeURIComponent(approvalId)}`,
    { method: "POST", body: JSON.stringify({ decision }) },
  );
}

export function cancelClaudeTask(taskId: string): Promise<void> {
  return apiFetch<void>(`/api/claude/tasks/${encodeURIComponent(taskId)}/cancel`, { method: "POST" });
}

/**
 * Follow the live event stream (Server-Sent Events over fetch, so the bearer
 * token travels in the Authorization header rather than the URL).
 * Resolves when the server ends the stream; rejects on network/HTTP errors.
 */
export async function streamClaudeEvents(
  taskId: string,
  after: number,
  onEvent: (event: ClaudeEvent) => void,
  signal: AbortSignal,
): Promise<void> {
  const url = `${getBaseUrl()}/api/claude/tasks/${encodeURIComponent(taskId)}/stream?after=${after}`;
  const open = (token: string | null) =>
    fetch(url, {
      headers: {
        Accept: "text/event-stream",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      signal,
    });

  let response = await open(getAuthToken());
  if (response.status === 401) {
    const refreshed = await tryRefreshToken();
    if (refreshed) response = await open(refreshed);
  }
  if (!response.ok || !response.body) {
    throw new Error(`stream HTTP ${response.status}`);
  }

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
      const data = block
        .split("\n")
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice(5).trimStart())
        .join("\n");
      if (data) {
        try {
          onEvent(JSON.parse(data) as ClaudeEvent);
        } catch {
          // ignore malformed frame; history fetch will fill any gap
        }
      }
      boundary = buffer.indexOf("\n\n");
    }
  }
}
