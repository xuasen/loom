import { useCallback, useEffect, useRef, useState } from "react";
import { AlertTriangle, Ban, Check, Loader2, Play, RefreshCw, SquareTerminal, X } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Textarea } from "@/components/ui/textarea";
import { ApiError } from "@/api/client";
import {
  TERMINAL_STATUSES,
  cancelClaudeTask,
  createClaudeTask,
  decideClaudeApproval,
  getClaudeConfig,
  getClaudeEvents,
  getClaudeTask,
  listClaudeTasks,
  streamClaudeEvents,
  type ClaudeConfig,
  type ClaudeEvent,
  type ClaudeTask,
  type ClaudeTaskMode,
  type ClaudeTaskStatus,
} from "@/api/claude";

const POLL_INTERVAL_MS = 2000;

function statusVariant(status: ClaudeTaskStatus): "success" | "warning" | "destructive" | "neutral" | "secondary" {
  switch (status) {
    case "SUCCEEDED":
      return "success";
    case "WAITING_APPROVAL":
      return "warning";
    case "FAILED":
      return "destructive";
    case "CANCELLED":
      return "neutral";
    default:
      return "secondary";
  }
}

function shortJson(value: unknown, max = 400): string {
  let text: string;
  try {
    text = typeof value === "string" ? value : JSON.stringify(value);
  } catch {
    text = String(value);
  }
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

/** History first, then live stream; events are merged and de-duplicated by seq. */
function useTaskEvents(taskId: string | null) {
  const [events, setEvents] = useState<ClaudeEvent[]>([]);
  const [status, setStatus] = useState<ClaudeTaskStatus | null>(null);
  const [live, setLive] = useState(false);
  const lastSeqRef = useRef(0);

  const merge = useCallback((incoming: ClaudeEvent[]) => {
    const fresh = incoming.filter((e) => e.seq > lastSeqRef.current).sort((a, b) => a.seq - b.seq);
    if (fresh.length === 0) return;
    const last = fresh[fresh.length - 1];
    if (last) lastSeqRef.current = last.seq;
    setEvents((prev) => [...prev, ...fresh]);
    for (const e of fresh) {
      if (e.type === "status" && e.status) setStatus(e.status);
    }
  }, []);

  useEffect(() => {
    setEvents([]);
    setStatus(null);
    setLive(false);
    lastSeqRef.current = 0;
    if (!taskId) return;

    const controller = new AbortController();
    let pollTimer: ReturnType<typeof setTimeout> | null = null;
    let stopped = false;

    const poll = async () => {
      if (stopped) return;
      try {
        const page = await getClaudeEvents(taskId, lastSeqRef.current);
        merge(page.events);
        setStatus(page.task_status);
        if (TERMINAL_STATUSES.has(page.task_status)) {
          setLive(false);
          return;
        }
      } catch {
        // transient; keep polling
      }
      pollTimer = setTimeout(poll, POLL_INTERVAL_MS);
    };

    (async () => {
      try {
        const history = await getClaudeEvents(taskId, 0);
        if (stopped) return;
        merge(history.events);
        setStatus(history.task_status);
        if (TERMINAL_STATUSES.has(history.task_status)) return;
        setLive(true);
        await streamClaudeEvents(taskId, lastSeqRef.current, (e) => merge([e]), controller.signal);
        // Stream ended (task finished or server closed it): reconcile once.
        await poll();
      } catch {
        if (stopped || controller.signal.aborted) return;
        setLive(true);
        void poll(); // streaming unavailable: fall back to incremental polling
      }
    })();

    return () => {
      stopped = true;
      controller.abort();
      if (pollTimer) clearTimeout(pollTimer);
    };
  }, [taskId, merge]);

  return { events, status, live };
}

function EventRow({
  event,
  canDecide,
  onDecide,
}: {
  event: ClaudeEvent;
  canDecide: boolean;
  onDecide: (approvalId: string, decision: "allow" | "deny") => void;
}) {
  switch (event.type) {
    case "text":
      return <div className="whitespace-pre-wrap text-sm leading-relaxed">{event.text}</div>;
    case "tool_use":
      return (
        <div className="font-mono text-xs text-muted-foreground">
          <span className="text-foreground">▶ {event.tool}</span> {shortJson(event.input)}
        </div>
      );
    case "tool_result":
      return (
        <details className="font-mono text-xs">
          <summary className={event.is_error ? "text-destructive cursor-pointer" : "text-muted-foreground cursor-pointer"}>
            {event.is_error ? "✗ tool error" : "✓ tool result"} ({(event.output ?? "").length} chars)
          </summary>
          <pre className="mt-1 max-h-64 overflow-auto whitespace-pre-wrap rounded border bg-muted/40 p-2">{event.output}</pre>
        </details>
      );
    case "approval_request":
      return (
        <div className="rounded-md border border-warning/50 bg-warning-bg p-3 text-sm space-y-2">
          <div className="flex items-center gap-2 font-medium">
            <AlertTriangle className="h-4 w-4 text-warning" /> Approval needed: {event.tool}
          </div>
          <pre className="max-h-48 overflow-auto whitespace-pre-wrap font-mono text-xs">{shortJson(event.input, 2000)}</pre>
          {canDecide && event.approval_id && (
            <div className="flex gap-2">
              <Button size="sm" onClick={() => onDecide(event.approval_id!, "allow")}>
                <Check className="h-3.5 w-3.5" /> Allow
              </Button>
              <Button size="sm" variant="outline" onClick={() => onDecide(event.approval_id!, "deny")}>
                <X className="h-3.5 w-3.5" /> Deny
              </Button>
            </div>
          )}
        </div>
      );
    case "approval_decision":
      return (
        <div className="text-xs text-muted-foreground">
          {event.decision === "allow" ? "✓ allowed" : "✗ denied"}
          {event.decided_by ? ` by ${event.decided_by}` : ""}
        </div>
      );
    case "status":
      return (
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          {event.status && <Badge variant={statusVariant(event.status)}>{event.status}</Badge>}
          {event.message}
        </div>
      );
    case "error":
      return <div className="whitespace-pre-wrap text-sm text-destructive">{event.message ?? event.text}</div>;
    case "result":
      return (
        <div className="rounded-md border bg-muted/30 p-3 whitespace-pre-wrap text-sm">
          <div className="mb-1 text-xs font-medium text-muted-foreground">Result</div>
          {event.text ?? event.message}
        </div>
      );
    default:
      return null;
  }
}

export function ClaudeTerminalPage() {
  const [config, setConfig] = useState<ClaudeConfig | null>(null);
  const [configError, setConfigError] = useState<string | null>(null);
  const [tasks, setTasks] = useState<ClaudeTask[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selectedTask, setSelectedTask] = useState<ClaudeTask | null>(null);
  const [prompt, setPrompt] = useState("");
  const [mode, setMode] = useState<ClaudeTaskMode>("interactive");
  const [submitting, setSubmitting] = useState(false);
  const [followUp, setFollowUp] = useState("");
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const stickToBottomRef = useRef(true);

  const { events, status, live } = useTaskEvents(selectedId);
  const effectiveStatus: ClaudeTaskStatus | null = status ?? selectedTask?.status ?? null;
  const isFinished = effectiveStatus !== null && TERMINAL_STATUSES.has(effectiveStatus);
  const decidedIds = new Set(events.filter((e) => e.type === "approval_decision").map((e) => e.approval_id));

  const refreshTasks = useCallback(async () => {
    try {
      const res = await listClaudeTasks();
      setTasks(res.tasks);
    } catch (e) {
      toast.error(`Could not load tasks: ${(e as Error).message}`);
    }
  }, []);

  useEffect(() => {
    getClaudeConfig()
      .then((cfg) => {
        setConfig(cfg);
        if (cfg.enabled) void refreshTasks();
      })
      .catch((e: unknown) => {
        setConfigError(
          e instanceof ApiError && e.status === 404
            ? "The Claude worker backend is not deployed yet."
            : (e as Error).message,
        );
      });
  }, [refreshTasks]);

  useEffect(() => {
    if (!selectedId) {
      setSelectedTask(null);
      return;
    }
    getClaudeTask(selectedId).then(setSelectedTask).catch(() => setSelectedTask(null));
  }, [selectedId]);

  // Keep the list's status badges current while something is still running.
  useEffect(() => {
    if (!tasks.some((t) => !TERMINAL_STATUSES.has(t.status))) return;
    const id = setInterval(() => void refreshTasks(), 10000);
    return () => clearInterval(id);
  }, [tasks, refreshTasks]);

  useEffect(() => {
    const el = scrollRef.current;
    if (el && stickToBottomRef.current) el.scrollTop = el.scrollHeight;
  }, [events]);

  const submit = async (text: string, parentTaskId?: string) => {
    const trimmed = text.trim();
    if (!trimmed) return;
    setSubmitting(true);
    try {
      const task = await createClaudeTask({ prompt: trimmed, mode, parent_task_id: parentTaskId });
      setTasks((prev) => [task, ...prev.filter((t) => t.task_id !== task.task_id)]);
      setSelectedId(task.task_id);
      setPrompt("");
      setFollowUp("");
    } catch (e) {
      toast.error(`Could not submit task: ${(e as Error).message}`);
    } finally {
      setSubmitting(false);
    }
  };

  const decide = async (approvalId: string, decision: "allow" | "deny") => {
    if (!selectedId) return;
    try {
      await decideClaudeApproval(selectedId, approvalId, decision);
    } catch (e) {
      toast.error(`Could not send decision: ${(e as Error).message}`);
    }
  };

  const cancel = async () => {
    if (!selectedId) return;
    try {
      await cancelClaudeTask(selectedId);
      toast.success("Cancel requested");
    } catch (e) {
      toast.error(`Could not cancel: ${(e as Error).message}`);
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between flex-wrap gap-3">
        <div>
          <h2 className="text-2xl font-semibold flex items-center gap-2">
            <SquareTerminal className="h-6 w-6" /> Claude Terminal
          </h2>
          <p className="text-sm text-muted-foreground">
            Send tasks to Claude Code on a private worker. Tasks keep running after you sign out; every step is
            recorded and can be replayed.
          </p>
        </div>
        {config?.enabled && (
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            {config.workers.map((w) => (
              <Badge key={w.worker_id} variant={w.online ? "success" : "neutral"}>
                {w.worker_id} {w.online ? "online" : "offline"}
              </Badge>
            ))}
          </div>
        )}
      </div>

      {configError && (
        <Card>
          <CardContent className="py-6 text-sm text-muted-foreground flex items-center gap-2">
            <AlertTriangle className="h-4 w-4 text-warning" /> {configError}
          </CardContent>
        </Card>
      )}
      {!configError && !config && (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading…
        </div>
      )}
      {config && !config.enabled && (
        <Card>
          <CardContent className="py-6 text-sm text-muted-foreground">No Claude worker is configured.</CardContent>
        </Card>
      )}

      {config?.enabled && (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-[320px_1fr]">
          <div className="space-y-4">
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm">New task</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2">
                <Textarea
                  value={prompt}
                  onChange={(e) => setPrompt(e.target.value)}
                  placeholder="Describe what Claude Code should do…"
                  rows={5}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) void submit(prompt);
                  }}
                />
                <div className="flex items-center gap-3 text-xs">
                  <label className="flex items-center gap-1">
                    <input type="radio" checked={mode === "interactive"} onChange={() => setMode("interactive")} />
                    Ask before risky steps
                  </label>
                  <label className="flex items-center gap-1">
                    <input type="radio" checked={mode === "unattended"} onChange={() => setMode("unattended")} />
                    Unattended (allowlist only)
                  </label>
                </div>
                <Button className="w-full" disabled={submitting || !prompt.trim()} onClick={() => void submit(prompt)}>
                  {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />} Run
                </Button>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-2 flex flex-row items-center justify-between">
                <CardTitle className="text-sm">Tasks</CardTitle>
                <Button size="icon-xs" variant="ghost" onClick={() => void refreshTasks()} title="Refresh">
                  <RefreshCw />
                </Button>
              </CardHeader>
              <CardContent className="p-2 space-y-1 max-h-[55vh] overflow-y-auto">
                {tasks.length === 0 && <div className="px-2 py-4 text-xs text-muted-foreground">No tasks yet.</div>}
                {tasks.map((t) => (
                  <button
                    key={t.task_id}
                    type="button"
                    onClick={() => setSelectedId(t.task_id)}
                    className={`w-full rounded-md px-2 py-2 text-left text-xs transition-colors ${
                      t.task_id === selectedId ? "bg-accent" : "hover:bg-accent/50"
                    }`}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="truncate font-medium">{t.prompt}</span>
                      <Badge variant={statusVariant(t.status)}>{t.status}</Badge>
                    </div>
                    <div className="mt-0.5 text-muted-foreground">
                      {new Date(t.created_at).toLocaleString()} · {t.submitted_by}
                    </div>
                  </button>
                ))}
              </CardContent>
            </Card>
          </div>

          <Card className="min-h-[60vh] flex flex-col">
            {!selectedId ? (
              <CardContent className="flex-1 flex items-center justify-center text-sm text-muted-foreground">
                Select a task or run a new one.
              </CardContent>
            ) : (
              <>
                <CardHeader className="pb-2 flex flex-row items-center justify-between gap-2">
                  <div className="min-w-0">
                    <CardTitle className="text-sm truncate">{selectedTask?.prompt ?? selectedId}</CardTitle>
                    <div className="mt-1 flex items-center gap-2 text-xs text-muted-foreground">
                      {effectiveStatus && <Badge variant={statusVariant(effectiveStatus)}>{effectiveStatus}</Badge>}
                      {live && !isFinished && (
                        <span className="flex items-center gap-1">
                          <span className="h-2 w-2 rounded-full bg-success animate-pulse" /> live
                        </span>
                      )}
                      <span className="font-mono">{selectedId}</span>
                    </div>
                  </div>
                  {!isFinished && (
                    <Button size="sm" variant="outline" onClick={() => void cancel()}>
                      <Ban className="h-3.5 w-3.5" /> Cancel
                    </Button>
                  )}
                </CardHeader>
                <CardContent className="flex-1 flex flex-col gap-3 min-h-0">
                  <div
                    ref={scrollRef}
                    onScroll={(e) => {
                      const el = e.currentTarget;
                      stickToBottomRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
                    }}
                    className="flex-1 min-h-0 max-h-[60vh] overflow-y-auto space-y-3 rounded-md border bg-background p-3"
                  >
                    {events.length === 0 && (
                      <div className="text-xs text-muted-foreground flex items-center gap-2">
                        <Loader2 className="h-3.5 w-3.5 animate-spin" /> Waiting for the worker…
                      </div>
                    )}
                    {events.map((e) => (
                      <EventRow
                        key={e.seq}
                        event={e}
                        canDecide={
                          effectiveStatus === "WAITING_APPROVAL" && !!e.approval_id && !decidedIds.has(e.approval_id)
                        }
                        onDecide={(id, d) => void decide(id, d)}
                      />
                    ))}
                  </div>
                  {isFinished && (
                    <div className="flex gap-2">
                      <Textarea
                        value={followUp}
                        onChange={(e) => setFollowUp(e.target.value)}
                        placeholder="Continue this conversation…"
                        rows={2}
                        className="flex-1"
                      />
                      <Button
                        disabled={submitting || !followUp.trim()}
                        onClick={() => void submit(followUp, selectedId)}
                      >
                        <Play className="h-4 w-4" /> Send
                      </Button>
                    </div>
                  )}
                </CardContent>
              </>
            )}
          </Card>
        </div>
      )}
    </div>
  );
}
