import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  AlertTriangle, Check, Loader2, Plus, RefreshCw, Send, Square, SquareTerminal, X, XCircle,
} from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Textarea } from "@/components/ui/textarea";
import { ApiError } from "@/api/client";
import {
  CLOSED_STATUSES,
  closeClaudeSession,
  createClaudeSession,
  decideClaudeApproval,
  getClaudeConfig,
  getClaudeEvents,
  getClaudeSession,
  interruptClaudeSession,
  listClaudeSessions,
  sendClaudeMessage,
  streamClaudeEvents,
  type ClaudeConfig,
  type ClaudeEvent,
  type ClaudeSession,
  type ClaudeSessionMode,
  type ClaudeSessionStatus,
} from "@/api/claude";

const POLL_INTERVAL_MS = 2000;
const RECONNECT_DELAY_MS = 600;

function statusVariant(status: ClaudeSessionStatus): "success" | "warning" | "destructive" | "neutral" | "secondary" {
  switch (status) {
    case "IDLE":
      return "success";
    case "WAITING_APPROVAL":
      return "warning";
    case "FAILED":
      return "destructive";
    case "CLOSED":
      return "neutral";
    default:
      return "secondary";
  }
}

const STATUS_LABEL: Record<ClaudeSessionStatus, string> = {
  IDLE: "ready",
  RUNNING: "working",
  WAITING_APPROVAL: "needs approval",
  CLOSED: "ended",
  FAILED: "failed",
};

function shortJson(value: unknown, max = 400): string {
  let text: string;
  try {
    text = typeof value === "string" ? value : JSON.stringify(value);
  } catch {
    text = String(value);
  }
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** History first, then the live stream (reconnecting); events de-duplicated by seq. */
function useSessionEvents(sessionId: string | null) {
  const [events, setEvents] = useState<ClaudeEvent[]>([]);
  const [status, setStatus] = useState<ClaudeSessionStatus | null>(null);
  const [pending, setPending] = useState(0);
  const [connected, setConnected] = useState(false);
  const lastSeqRef = useRef(0);

  const merge = useCallback((incoming: ClaudeEvent[]) => {
    const fresh = incoming.filter((e) => e.seq > lastSeqRef.current).sort((a, b) => a.seq - b.seq);
    if (fresh.length === 0) return;
    const last = fresh[fresh.length - 1];
    if (last) lastSeqRef.current = last.seq;
    setEvents((prev) => [...prev, ...fresh]);
    for (const e of fresh) if (e.type === "status" && e.status) setStatus(e.status);
  }, []);

  useEffect(() => {
    setEvents([]);
    setStatus(null);
    setPending(0);
    setConnected(false);
    lastSeqRef.current = 0;
    if (!sessionId) return;

    const controller = new AbortController();
    const { signal } = controller;

    const catchUp = async (): Promise<ClaudeSessionStatus | null> => {
      const page = await getClaudeEvents(sessionId, lastSeqRef.current);
      merge(page.events);
      setStatus(page.status);
      setPending(page.pending_count);
      return page.status;
    };

    (async () => {
      let st: ClaudeSessionStatus | null = null;
      try {
        st = await catchUp();
      } catch {
        /* retried below */
      }
      while (!signal.aborted) {
        if (st && CLOSED_STATUSES.has(st)) {
          setConnected(false);
          return;
        }
        try {
          setConnected(true);
          await streamClaudeEvents(
            sessionId,
            lastSeqRef.current,
            (e) => merge([e]),
            (s) => {
              st = s.status;
              setStatus(s.status);
              setPending(s.pending_count);
            },
            signal,
          );
          await sleep(RECONNECT_DELAY_MS); // server ends streams periodically; resume from last seq
        } catch {
          if (signal.aborted) return;
          setConnected(false);
          await sleep(POLL_INTERVAL_MS);
          try {
            st = await catchUp();
          } catch {
            /* keep retrying */
          }
        }
      }
    })();

    return () => controller.abort();
  }, [sessionId, merge]);

  return { events, status, pending, connected };
}

type Block =
  | { kind: "user"; key: number; text: string; by?: string }
  | { kind: "assistant"; key: number; text: string }
  | { kind: "event"; key: number; event: ClaudeEvent };

/** Merge consecutive streamed text chunks into one assistant block. */
function toBlocks(events: ClaudeEvent[]): Block[] {
  const blocks: Block[] = [];
  for (const e of events) {
    if (e.type === "text") {
      const prev = blocks[blocks.length - 1];
      if (prev && prev.kind === "assistant") prev.text += e.text ?? "";
      else blocks.push({ kind: "assistant", key: e.seq, text: e.text ?? "" });
    } else if (e.type === "user_message") {
      blocks.push({ kind: "user", key: e.seq, text: e.text ?? "", by: e.by });
    } else if (e.type === "status" && !e.message) {
      // plain state changes are shown in the header, not the transcript
    } else {
      blocks.push({ kind: "event", key: e.seq, event: e });
    }
  }
  return blocks;
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
    case "tool_use":
      return (
        <div className="font-mono text-xs text-muted-foreground">
          <span className="text-foreground">● {event.tool}</span> {shortJson(event.input)}
        </div>
      );
    case "tool_result":
      return (
        <details className="font-mono text-xs pl-3">
          <summary className={event.is_error ? "text-destructive cursor-pointer" : "text-muted-foreground cursor-pointer"}>
            {event.is_error ? "⎿ error" : "⎿ result"} ({(event.output ?? "").length} chars)
          </summary>
          <pre className="mt-1 max-h-64 overflow-auto whitespace-pre-wrap rounded border bg-muted/40 p-2">{event.output}</pre>
        </details>
      );
    case "approval_request":
      return (
        <div className="rounded-md border border-warning/50 bg-warning-bg p-3 text-sm space-y-2">
          <div className="flex items-center gap-2 font-medium">
            <AlertTriangle className="h-4 w-4 text-warning" /> Allow {event.tool}?
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
      return <div className="text-xs text-muted-foreground">{event.message}</div>;
    case "turn_end":
      return (
        <div className="text-[10px] text-muted-foreground/70">
          {event.message === "interrupted" ? "interrupted" : event.is_error ? "turn ended with an error" : "done"}
          {typeof event.cost_usd === "number" ? ` · $${event.cost_usd.toFixed(4)}` : ""}
        </div>
      );
    case "error":
      return <div className="whitespace-pre-wrap text-sm text-destructive">{event.message ?? event.text}</div>;
    default:
      return null;
  }
}

export function ClaudeTerminalPage() {
  const [config, setConfig] = useState<ClaudeConfig | null>(null);
  const [configError, setConfigError] = useState<string | null>(null);
  const [sessions, setSessions] = useState<ClaudeSession[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selected, setSelected] = useState<ClaudeSession | null>(null);
  const [input, setInput] = useState("");
  const [mode, setMode] = useState<ClaudeSessionMode>("interactive");
  const [sending, setSending] = useState(false);
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const stickRef = useRef(true);

  const { events, status, pending, connected } = useSessionEvents(selectedId);
  const blocks = useMemo(() => toBlocks(events), [events]);
  const effectiveStatus: ClaudeSessionStatus | null = status ?? selected?.status ?? null;
  const isClosed = effectiveStatus !== null && CLOSED_STATUSES.has(effectiveStatus);
  const isBusy = effectiveStatus === "RUNNING" || effectiveStatus === "WAITING_APPROVAL";
  const decided = useMemo(
    () => new Set(events.filter((e) => e.type === "approval_decision").map((e) => e.approval_id)),
    [events],
  );

  const refreshSessions = useCallback(async () => {
    try {
      setSessions((await listClaudeSessions()).sessions);
    } catch (e) {
      toast.error(`Could not load sessions: ${(e as Error).message}`);
    }
  }, []);

  useEffect(() => {
    getClaudeConfig()
      .then((cfg) => {
        setConfig(cfg);
        if (cfg.enabled) void refreshSessions();
      })
      .catch((e: unknown) => {
        setConfigError(
          e instanceof ApiError && e.status === 404
            ? "The Claude worker backend is not configured."
            : (e as Error).message,
        );
      });
  }, [refreshSessions]);

  useEffect(() => {
    if (!selectedId) {
      setSelected(null);
      return;
    }
    getClaudeSession(selectedId).then(setSelected).catch(() => setSelected(null));
  }, [selectedId]);

  // Keep list badges fresh while any session is active.
  useEffect(() => {
    if (!sessions.some((s) => s.status === "RUNNING" || s.status === "WAITING_APPROVAL")) return;
    const id = setInterval(() => void refreshSessions(), 10000);
    return () => clearInterval(id);
  }, [sessions, refreshSessions]);

  useEffect(() => {
    if (status && selectedId) {
      setSessions((prev) => prev.map((s) => (s.session_id === selectedId ? { ...s, status } : s)));
    }
  }, [status, selectedId]);

  useEffect(() => {
    const el = scrollRef.current;
    if (el && stickRef.current) el.scrollTop = el.scrollHeight;
  }, [blocks]);

  const send = async () => {
    const text = input.trim();
    if (!text || sending) return;
    setSending(true);
    try {
      if (!selectedId) {
        const s = await createClaudeSession({ message: text, mode });
        setSessions((prev) => [s, ...prev]);
        setSelectedId(s.session_id);
      } else {
        await sendClaudeMessage(selectedId, text);
      }
      setInput("");
      stickRef.current = true;
    } catch (e) {
      toast.error(`Could not send: ${(e as Error).message}`);
    } finally {
      setSending(false);
    }
  };

  const interrupt = useCallback(async () => {
    if (!selectedId) return;
    try {
      await interruptClaudeSession(selectedId);
    } catch (e) {
      toast.error(`Could not interrupt: ${(e as Error).message}`);
    }
  }, [selectedId]);

  const endSession = async () => {
    if (!selectedId) return;
    try {
      await closeClaudeSession(selectedId);
      void refreshSessions();
    } catch (e) {
      toast.error(`Could not end session: ${(e as Error).message}`);
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

  // Esc interrupts the running turn, like in the terminal.
  useEffect(() => {
    if (!isBusy) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") void interrupt();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isBusy, interrupt]);

  const worker = config?.workers[0];

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between flex-wrap gap-3">
        <div>
          <h2 className="text-2xl font-semibold flex items-center gap-2">
            <SquareTerminal className="h-6 w-6" /> Claude Terminal
          </h2>
          <p className="text-sm text-muted-foreground">
            Conversations with Claude Code on a private worker. A session keeps its context across messages,
            keeps working after you sign out, and can be replayed.
          </p>
        </div>
        {config?.enabled && worker && (
          <Badge variant={worker.online ? "success" : "neutral"}>
            {worker.worker_id} {worker.online ? "online" : "offline"}
          </Badge>
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

      {config?.enabled && (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-[280px_1fr]">
          <Card className="h-fit">
            <CardHeader className="pb-2 flex flex-row items-center justify-between">
              <CardTitle className="text-sm">Sessions</CardTitle>
              <div className="flex gap-1">
                <Button size="icon-xs" variant="ghost" onClick={() => void refreshSessions()} title="Refresh">
                  <RefreshCw />
                </Button>
                <Button size="icon-xs" variant="ghost" onClick={() => setSelectedId(null)} title="New session">
                  <Plus />
                </Button>
              </div>
            </CardHeader>
            <CardContent className="p-2 space-y-1 max-h-[70vh] overflow-y-auto">
              {sessions.length === 0 && <div className="px-2 py-4 text-xs text-muted-foreground">No sessions yet.</div>}
              {sessions.map((s) => (
                <button
                  key={s.session_id}
                  type="button"
                  onClick={() => setSelectedId(s.session_id)}
                  className={`w-full rounded-md px-2 py-2 text-left text-xs transition-colors ${
                    s.session_id === selectedId ? "bg-accent" : "hover:bg-accent/50"
                  }`}
                >
                  <div className="flex items-center justify-between gap-2">
                    <span className="truncate font-medium">{s.title}</span>
                    <Badge variant={statusVariant(s.status)}>{STATUS_LABEL[s.status]}</Badge>
                  </div>
                  <div className="mt-0.5 text-muted-foreground">
                    {new Date(s.updated_at ?? s.created_at).toLocaleString()} · {s.turns} turns
                  </div>
                </button>
              ))}
            </CardContent>
          </Card>

          <Card className="flex flex-col min-h-[70vh]">
            <CardHeader className="pb-2 flex flex-row items-center justify-between gap-2">
              <div className="min-w-0">
                <CardTitle className="text-sm truncate">
                  {selectedId ? selected?.title ?? selectedId : "New session"}
                </CardTitle>
                {selectedId && (
                  <div className="mt-1 flex items-center gap-2 text-xs text-muted-foreground">
                    {effectiveStatus && (
                      <Badge variant={statusVariant(effectiveStatus)}>{STATUS_LABEL[effectiveStatus]}</Badge>
                    )}
                    {pending > 0 && isBusy && <span>{pending - 1 > 0 ? `${pending - 1} queued` : ""}</span>}
                    {connected && !isClosed && (
                      <span className="flex items-center gap-1">
                        <span className="h-2 w-2 rounded-full bg-success animate-pulse" /> live
                      </span>
                    )}
                    {selected && <span>{selected.mode}</span>}
                  </div>
                )}
              </div>
              {selectedId && !isClosed && (
                <div className="flex gap-2">
                  {isBusy && (
                    <Button size="sm" variant="outline" onClick={() => void interrupt()} title="Esc">
                      <Square className="h-3.5 w-3.5" /> Interrupt
                    </Button>
                  )}
                  <Button size="sm" variant="ghost" onClick={() => void endSession()}>
                    <XCircle className="h-3.5 w-3.5" /> End session
                  </Button>
                </div>
              )}
            </CardHeader>

            <CardContent className="flex-1 flex flex-col gap-3 min-h-0">
              <div
                ref={scrollRef}
                onScroll={(e) => {
                  const el = e.currentTarget;
                  stickRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
                }}
                className="flex-1 min-h-[45vh] max-h-[62vh] overflow-y-auto space-y-3 rounded-md border bg-background p-3"
              >
                {!selectedId && (
                  <div className="text-sm text-muted-foreground space-y-3">
                    <p>Start a conversation. Claude Code runs in the worker's work directory.</p>
                    <div className="flex items-center gap-4 text-xs">
                      <label className="flex items-center gap-1">
                        <input type="radio" checked={mode === "interactive"} onChange={() => setMode("interactive")} />
                        Ask me before edits and commands
                      </label>
                      <label className="flex items-center gap-1">
                        <input type="radio" checked={mode === "unattended"} onChange={() => setMode("unattended")} />
                        Unattended (allowlist only)
                      </label>
                    </div>
                  </div>
                )}
                {selectedId && blocks.length === 0 && (
                  <div className="text-xs text-muted-foreground flex items-center gap-2">
                    <Loader2 className="h-3.5 w-3.5 animate-spin" /> Waiting for the worker…
                  </div>
                )}
                {blocks.map((b) =>
                  b.kind === "user" ? (
                    <div key={b.key} className="flex justify-end">
                      <div className="max-w-[85%] rounded-lg bg-accent px-3 py-2 text-sm whitespace-pre-wrap">{b.text}</div>
                    </div>
                  ) : b.kind === "assistant" ? (
                    <div key={b.key} className="whitespace-pre-wrap text-sm leading-relaxed">{b.text}</div>
                  ) : (
                    <EventRow
                      key={b.key}
                      event={b.event}
                      canDecide={
                        effectiveStatus === "WAITING_APPROVAL" &&
                        !!b.event.approval_id &&
                        !decided.has(b.event.approval_id)
                      }
                      onDecide={(id, d) => void decide(id, d)}
                    />
                  ),
                )}
                {isBusy && (
                  <div className="text-xs text-muted-foreground flex items-center gap-2">
                    <Loader2 className="h-3.5 w-3.5 animate-spin" /> working… (Esc to interrupt)
                  </div>
                )}
              </div>

              {!isClosed ? (
                <div className="flex gap-2 items-end">
                  <Textarea
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    placeholder={
                      isBusy ? "Type the next message (it runs after the current turn)…" : "Message Claude Code…"
                    }
                    rows={2}
                    className="flex-1 font-mono text-sm"
                    onKeyDown={(e) => {
                      if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
                        e.preventDefault();
                        void send();
                      }
                    }}
                  />
                  <Button disabled={sending || !input.trim()} onClick={() => void send()}>
                    {sending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
                  </Button>
                </div>
              ) : (
                <div className="text-xs text-muted-foreground">
                  This session has ended. Start a new one with <Plus className="inline h-3 w-3" />.
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}
