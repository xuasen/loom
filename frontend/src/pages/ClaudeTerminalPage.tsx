import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  AlertTriangle, ArrowDown, Bell, BellOff, Check, ChevronLeft, ChevronRight, Clock, CornerDownRight, FileText,
  Folder, Loader2, Paperclip, Plus, RefreshCw, Send, Shield, Sparkles, Square, SquareTerminal, X, XCircle,
} from "lucide-react";
import { ClaudeSchedulesPanel } from "@/components/ClaudeSchedulesPanel";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { MarkdownBlock } from "@/components/MarkdownRenderer";
import { ApiError } from "@/api/client";
import {
  CLOSED_STATUSES,
  MODE_CYCLE,
  MODE_LABELS,
  closeClaudeSession,
  createClaudeSession,
  currentPushSubscription,
  decideClaudeApproval,
  disablePush,
  enablePush,
  getClaudeConfig,
  getClaudeEvents,
  getClaudeSession,
  interruptClaudeSession,
  listClaudeSessions,
  modelLabel,
  pushSupported,
  sendClaudeMessage,
  streamClaudeEvents,
  updateClaudeSettings,
  uploadClaudeFile,
  type ClaudeConfig,
  type ClaudeEvent,
  type ClaudeSession,
  type ClaudeSessionMode,
  type ClaudeSessionStatus,
} from "@/api/claude";

const POLL_INTERVAL_MS = 2000;
const RECONNECT_DELAY_MS = 600;

const STATUS_LABEL: Record<ClaudeSessionStatus, string> = {
  IDLE: "Ready",
  RUNNING: "Working",
  WAITING_APPROVAL: "Needs approval",
  CLOSED: "Ended",
  FAILED: "Failed",
};

const STATUS_DOT: Record<ClaudeSessionStatus, string> = {
  IDLE: "bg-success",
  RUNNING: "bg-primary animate-pulse",
  WAITING_APPROVAL: "bg-warning animate-pulse",
  CLOSED: "bg-muted-foreground/40",
  FAILED: "bg-destructive",
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function shortJson(value: unknown, max = 400): string {
  let text: string;
  try {
    text = typeof value === "string" ? value : JSON.stringify(value);
  } catch {
    text = String(value);
  }
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

function humanSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function relativeTime(iso?: string | null): string {
  if (!iso) return "";
  const diff = (Date.now() - new Date(iso).getTime()) / 1000;
  if (diff < 60) return "just now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return new Date(iso).toLocaleDateString();
}

function shortPath(p?: string | null): string {
  if (!p) return "";
  const parts = p.split("/").filter(Boolean);
  return parts.length <= 2 ? p : `…/${parts.slice(-2).join("/")}`;
}

function useIsTouch(): boolean {
  const [touch, setTouch] = useState(false);
  useEffect(() => {
    const mq = window.matchMedia("(pointer: coarse)");
    setTouch(mq.matches);
    const on = (e: MediaQueryListEvent) => setTouch(e.matches);
    mq.addEventListener("change", on);
    return () => mq.removeEventListener("change", on);
  }, []);
  return touch;
}

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
          await sleep(RECONNECT_DELAY_MS);
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
  | {
      kind: "user";
      key: number;
      text: string;
      attachments: { name: string; size: number }[];
      steer?: boolean;
      scheduled?: string;
    }
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
      blocks.push({
        kind: "user", key: e.seq, text: e.text ?? "", attachments: e.attachments ?? [],
        steer: e.steer, scheduled: e.scheduled,
      });
    } else if (e.type === "status" && !e.message) {
      // plain state changes live in the header, not the transcript
    } else {
      blocks.push({ kind: "event", key: e.seq, event: e });
    }
  }
  return blocks;
}

/** Approvals still waiting: requested, not decided, and no later turn end. */
function openApprovalIds(events: ClaudeEvent[]): Set<string> {
  const open = new Set<string>();
  for (const e of events) {
    if (e.type === "approval_request" && e.approval_id) open.add(e.approval_id);
    else if (e.type === "approval_decision" && e.approval_id) open.delete(e.approval_id);
    else if (e.type === "turn_end") open.clear();
  }
  return open;
}

function ToolRow({ event }: { event: ClaudeEvent }) {
  return (
    <div className="flex items-start gap-2 font-mono text-xs text-muted-foreground min-w-0">
      <span className="mt-1 h-1.5 w-1.5 shrink-0 rounded-full bg-primary/70" />
      <span className="min-w-0 break-words">
        <span className="text-foreground font-medium">{event.tool}</span>{" "}
        <span className="opacity-80">{shortJson(event.input, 240)}</span>
      </span>
    </div>
  );
}

function EventRow({
  event,
  open,
  onDecide,
  labels,
}: {
  event: ClaudeEvent;
  open: boolean;
  onDecide: (approvalId: string, decision: "allow" | "deny") => void;
  labels?: Record<string, string>;
}) {
  switch (event.type) {
    case "tool_use":
      return <ToolRow event={event} />;
    case "tool_result":
      return (
        <details className="group pl-3.5 font-mono text-xs">
          <summary
            className={`flex cursor-pointer list-none items-center gap-1 py-1 ${
              event.is_error ? "text-destructive" : "text-muted-foreground"
            }`}
          >
            <ChevronRight className="h-3 w-3 transition-transform group-open:rotate-90" />
            {event.is_error ? "Error" : "Output"} · {(event.output ?? "").length.toLocaleString()} chars
          </summary>
          <pre className="mt-1 max-h-72 overflow-auto whitespace-pre-wrap break-words rounded-md border bg-muted/40 p-2.5">
            {event.output}
          </pre>
        </details>
      );
    case "approval_request":
      return (
        <div
          className={`rounded-xl border p-3 text-sm space-y-2.5 ${
            open ? "border-warning/60 bg-warning-bg" : "border-border bg-muted/30 opacity-80"
          }`}
        >
          <div className="flex items-center gap-2 font-medium">
            <Shield className="h-4 w-4 text-warning shrink-0" />
            <span>
              Allow <span className="font-mono">{event.tool}</span>?
            </span>
          </div>
          <pre className="max-h-48 overflow-auto whitespace-pre-wrap break-words rounded-md bg-background/60 p-2 font-mono text-xs">
            {shortJson(event.input, 3000)}
          </pre>
          {open && event.approval_id && (
            <div className="grid grid-cols-2 gap-2">
              <Button className="h-11" onClick={() => onDecide(event.approval_id!, "allow")}>
                <Check className="h-4 w-4" /> Allow
              </Button>
              <Button className="h-11" variant="outline" onClick={() => onDecide(event.approval_id!, "deny")}>
                <X className="h-4 w-4" /> Deny
              </Button>
            </div>
          )}
        </div>
      );
    case "approval_decision":
      return (
        <div className={`text-xs ${event.decision === "allow" ? "text-success" : "text-muted-foreground"}`}>
          {event.decision === "allow" ? "✓ Allowed" : "✗ Denied"}
          {event.decided_by ? ` · ${event.decided_by}` : ""}
        </div>
      );
    case "mode":
      return (
        <div className="text-center text-[11px] text-muted-foreground">
          — {event.mode ? MODE_LABELS[event.mode] : event.message} —
        </div>
      );
    case "model":
      return (
        <div className="text-center text-[11px] text-muted-foreground">
          — {event.model ? modelLabel(event.model, labels) : event.message} —
        </div>
      );
    case "status":
      return <div className="text-xs text-muted-foreground">{event.message}</div>;
    case "turn_end":
      return (
        <div className="text-[10px] text-muted-foreground/70">
          {event.message === "interrupted" ? "Interrupted" : event.is_error ? "Ended with an error" : "Done"}
          {typeof event.cost_usd === "number" ? ` · $${event.cost_usd.toFixed(4)}` : ""}
        </div>
      );
    case "error":
      return (
        <div className="flex items-start gap-2 rounded-lg bg-destructive/10 px-3 py-2 text-sm text-destructive">
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
          <span className="whitespace-pre-wrap break-words">{event.message ?? event.text}</span>
        </div>
      );
    default:
      return null;
  }
}

/** A chip that looks like a pill but is a native <select> (native pickers on phones). */
function PillSelect<T extends string>({
  icon,
  value,
  options,
  onChange,
  label,
  disabled,
}: {
  icon: React.ReactNode;
  value: T;
  options: { value: T; label: string }[];
  onChange: (v: T) => void;
  label: string;
  disabled?: boolean;
}) {
  const current = options.find((o) => o.value === value)?.label ?? value;
  return (
    <label
      className={`relative inline-flex h-8 max-w-[11rem] shrink-0 items-center gap-1.5 rounded-full border bg-background px-3 text-xs text-muted-foreground transition-colors ${
        disabled ? "opacity-50" : "hover:bg-accent hover:text-foreground"
      }`}
      title={label}
    >
      {icon}
      <span className="truncate">{current}</span>
      <select
        aria-label={label}
        value={value}
        disabled={disabled}
        onChange={(e) => onChange(e.target.value as T)}
        className="absolute inset-0 cursor-pointer opacity-0"
      >
        {options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    </label>
  );
}

interface PendingFile {
  localId: string;
  file: File;
  fileId?: string;
  error?: string;
}

export function ClaudeTerminalPage() {
  const isTouch = useIsTouch();
  const [config, setConfig] = useState<ClaudeConfig | null>(null);
  const [configError, setConfigError] = useState<string | null>(null);
  const [sessions, setSessions] = useState<ClaudeSession[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selected, setSelected] = useState<ClaudeSession | null>(null);
  // Mobile master/detail: show the conversation pane instead of the list.
  const [showConversation, setShowConversation] = useState(false);

  const [input, setInput] = useState("");
  const [sending, setSending] = useState(false);
  const [files, setFiles] = useState<PendingFile[]>([]);
  const [dragOver, setDragOver] = useState(false);
  const [showSchedules, setShowSchedules] = useState(false);
  const [pushOn, setPushOn] = useState(false);
  const [pushBusy, setPushBusy] = useState(false);

  // Settings for a new session, or the live values of the selected one.
  const [mode, setMode] = useState<ClaudeSessionMode>("default");
  const [model, setModel] = useState<string>("");
  const [cwd, setCwd] = useState<string>("");

  const scrollRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLTextAreaElement | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const stickRef = useRef(true);
  const [showJump, setShowJump] = useState(false);

  const { events, status, pending, connected } = useSessionEvents(selectedId);
  const blocks = useMemo(() => toBlocks(events), [events]);
  const openApprovals = useMemo(() => openApprovalIds(events), [events]);
  const effectiveStatus: ClaudeSessionStatus | null = status ?? selected?.status ?? null;
  const isClosed = effectiveStatus !== null && CLOSED_STATUSES.has(effectiveStatus);
  const isBusy = effectiveStatus === "RUNNING" || effectiveStatus === "WAITING_APPROVAL";
  const worker = config?.workers[0];
  const models = config?.models ?? [];
  const uploading = files.some((f) => !f.fileId && !f.error);

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
        const initial = cfg.default_model ?? cfg.models[0];
        if (initial) setModel((m) => m || initial);
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

  // Deep link (?session=<id>), e.g. from a notification.
  useEffect(() => {
    const id = new URLSearchParams(window.location.search).get("session");
    if (id && /^[a-f0-9]{8,32}$/.test(id)) {
      setSelectedId(id);
      setShowConversation(true);
    }
  }, []);

  // Keep the URL on the open session, so a notification for it can be skipped.
  useEffect(() => {
    const url = new URL(window.location.href);
    if (selectedId) url.searchParams.set("session", selectedId);
    else url.searchParams.delete("session");
    window.history.replaceState(window.history.state, "", url);
  }, [selectedId]);

  useEffect(() => {
    const key = config?.vapid_public_key;
    if (!key || !pushSupported() || Notification.permission !== "granted") return;
    currentPushSubscription(key)
      .then((s) => setPushOn(!!s))
      .catch(() => setPushOn(false));
  }, [config?.vapid_public_key]);

  const togglePush = async () => {
    const key = config?.vapid_public_key;
    if (!key) return;
    setPushBusy(true);
    try {
      if (pushOn) {
        await disablePush();
        setPushOn(false);
        toast.success("Notifications off for this device");
      } else {
        await enablePush(key);
        setPushOn(true);
        toast.success("Notifications on: approvals and finished turns will alert this device");
      }
    } catch (e) {
      toast.error((e as Error).message);
    } finally {
      setPushBusy(false);
    }
  };

  // Load the selected session and adopt its live mode/model in the pickers.
  useEffect(() => {
    if (!selectedId) {
      setSelected(null);
      return;
    }
    getClaudeSession(selectedId)
      .then((s) => {
        setSelected(s);
        if (s.mode && (MODE_LABELS as Record<string, string>)[s.mode]) setMode(s.mode);
        if (s.model) setModel(s.model);
      })
      .catch(() => setSelected(null));
  }, [selectedId]);

  // Follow mode/model changes announced in the stream.
  useEffect(() => {
    const last = [...events].reverse();
    const m = last.find((e) => e.type === "mode" && e.mode);
    const md = last.find((e) => e.type === "model" && e.model);
    if (m?.mode) setMode(m.mode);
    if (md?.model) setModel(md.model);
  }, [events]);

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

  // Auto-grow the composer up to ~8 lines.
  useEffect(() => {
    const el = inputRef.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${Math.min(el.scrollHeight, 200)}px`;
  }, [input]);

  const openSession = (id: string | null) => {
    setSelectedId(id);
    setShowConversation(true);
    stickRef.current = true;
    setShowJump(false);
    if (!id) {
      setMode("default");
      setCwd("");
      const initial = config?.default_model ?? models[0];
      if (initial) setModel(initial);
    }
  };

  const addFiles = useCallback((list: FileList | File[]) => {
    const incoming = Array.from(list).slice(0, 20);
    if (incoming.length === 0) return;
    const entries: PendingFile[] = incoming.map((file) => ({ localId: crypto.randomUUID(), file }));
    setFiles((prev) => [...prev, ...entries]);
    for (const entry of entries) {
      uploadClaudeFile(entry.file)
        .then((r) => setFiles((prev) => prev.map((f) => (f.localId === entry.localId ? { ...f, fileId: r.file_id } : f))))
        .catch((e: unknown) =>
          setFiles((prev) =>
            prev.map((f) => (f.localId === entry.localId ? { ...f, error: (e as Error).message } : f)),
          ),
        );
    }
  }, []);

  const send = async () => {
    const text = input.trim();
    if (!text || sending || uploading) return;
    const attachments = files.filter((f) => f.fileId).map((f) => f.fileId!);
    setSending(true);
    try {
      if (!selectedId) {
        const s = await createClaudeSession({
          message: text, mode, model: model || undefined, cwd: cwd || undefined, attachments,
        });
        setSessions((prev) => [s, ...prev]);
        setSelectedId(s.session_id);
      } else {
        await sendClaudeMessage(selectedId, text, attachments);
      }
      setInput("");
      setFiles([]);
      stickRef.current = true;
    } catch (e) {
      toast.error(`Could not send: ${(e as Error).message}`);
    } finally {
      setSending(false);
    }
  };

  const changeMode = async (next: ClaudeSessionMode) => {
    setMode(next);
    if (!selectedId || isClosed) return;
    try {
      await updateClaudeSettings(selectedId, { mode: next });
    } catch (e) {
      toast.error(`Could not switch mode: ${(e as Error).message}`);
    }
  };

  const changeModel = async (next: string) => {
    setModel(next);
    if (!selectedId || isClosed) return;
    try {
      await updateClaudeSettings(selectedId, { model: next });
    } catch (e) {
      toast.error(`Could not switch model: ${(e as Error).message}`);
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
      toast.error((e as Error).message);
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

  const modeOptions = (config?.modes ?? MODE_CYCLE).map((m) => ({ value: m, label: MODE_LABELS[m] }));
  const modelOptions = models.map((m) => ({ value: m, label: modelLabel(m, config?.model_labels) }));
  const projectOptions = [
    { value: "", label: shortPath(worker?.default_cwd) || "Default work dir" },
    ...(worker?.projects ?? [])
      .filter((p) => p !== worker?.default_cwd)
      .map((p) => ({ value: p, label: shortPath(p) })),
  ];

  if (configError) {
    return (
      <div className="p-4 md:p-0">
        <div className="flex items-center gap-2 rounded-xl border p-6 text-sm text-muted-foreground">
          <AlertTriangle className="h-4 w-4 text-warning" /> {configError}
        </div>
      </div>
    );
  }
  if (!config) {
    return (
      <div className="flex items-center gap-2 p-4 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" /> Loading…
      </div>
    );
  }

  const sessionList = (
    <aside
      className={`${showConversation ? "hidden" : "flex"} md:flex min-h-0 w-full flex-col border-r bg-card md:w-72 md:rounded-l-xl`}
    >
      <div className="flex h-14 shrink-0 items-center justify-between gap-2 border-b px-4">
        <div className="flex items-center gap-2 min-w-0">
          <SquareTerminal className="h-5 w-5 shrink-0" />
          <span className="font-semibold truncate">Claude Terminal</span>
        </div>
        <div className="flex items-center gap-1">
          {config.vapid_public_key && pushSupported() && (
            <Button
              size="icon-sm"
              variant="ghost"
              onClick={() => void togglePush()}
              disabled={pushBusy}
              aria-label={pushOn ? "Turn notifications off" : "Turn notifications on"}
              title={pushOn ? "Notifications on (tap to turn off)" : "Notify me about approvals and finished turns"}
            >
              {pushOn ? <Bell className="h-4 w-4 text-primary" /> : <BellOff className="h-4 w-4" />}
            </Button>
          )}
          <Button size="icon-sm" variant="ghost" onClick={() => setShowSchedules(true)} aria-label="Schedules" title="Schedules">
            <Clock className="h-4 w-4" />
          </Button>
          <Button size="icon-sm" variant="ghost" onClick={() => void refreshSessions()} aria-label="Refresh">
            <RefreshCw className="h-4 w-4" />
          </Button>
          <Button size="icon-sm" onClick={() => openSession(null)} aria-label="New session">
            <Plus className="h-4 w-4" />
          </Button>
        </div>
      </div>
      {worker && (
        <div className="flex items-center gap-2 border-b px-4 py-2 text-xs text-muted-foreground">
          <span className={`h-2 w-2 rounded-full ${worker.online ? "bg-success" : "bg-muted-foreground/40"}`} />
          {worker.worker_id} · {worker.online ? "online" : "offline"}
        </div>
      )}
      <div className="flex-1 overflow-y-auto overscroll-contain p-2">
        {sessions.length === 0 && (
          <div className="px-3 py-10 text-center text-sm text-muted-foreground">
            No sessions yet.
            <div className="mt-3">
              <Button size="sm" onClick={() => openSession(null)}>
                <Plus className="h-4 w-4" /> New session
              </Button>
            </div>
          </div>
        )}
        {sessions.map((s) => (
          <button
            key={s.session_id}
            type="button"
            onClick={() => openSession(s.session_id)}
            className={`w-full rounded-lg px-3 py-3 text-left transition-colors md:py-2.5 ${
              s.session_id === selectedId ? "bg-accent" : "hover:bg-accent/50 active:bg-accent"
            }`}
          >
            <div className="flex items-center gap-2">
              <span className={`h-2 w-2 shrink-0 rounded-full ${STATUS_DOT[s.status]}`} />
              <span className="min-w-0 flex-1 truncate text-sm font-medium">{s.title || "Untitled"}</span>
              <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground md:hidden" />
            </div>
            <div className="mt-0.5 pl-4 text-xs text-muted-foreground truncate">
              {STATUS_LABEL[s.status]} · {relativeTime(s.updated_at ?? s.created_at)} · {s.turns} turns
              {s.cwd ? ` · ${shortPath(s.cwd)}` : ""}
            </div>
          </button>
        ))}
      </div>
    </aside>
  );

  const header = (
    <div className="flex h-14 shrink-0 items-center gap-2 border-b px-2 md:px-4">
      <Button
        size="icon-sm"
        variant="ghost"
        className="md:hidden"
        onClick={() => setShowConversation(false)}
        aria-label="Back to sessions"
      >
        <ChevronLeft className="h-5 w-5" />
      </Button>
      <div className="min-w-0 flex-1">
        <div className="truncate text-sm font-semibold">
          {selectedId ? selected?.title || "Session" : "New session"}
        </div>
        {selectedId && effectiveStatus && (
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <span className={`h-1.5 w-1.5 rounded-full ${STATUS_DOT[effectiveStatus]}`} />
            <span>{STATUS_LABEL[effectiveStatus]}</span>
            {isBusy && pending > 1 && <span>· {pending - 1} waiting</span>}
            {connected && !isClosed && <span className="hidden sm:inline">· live</span>}
            {selected?.cwd && <span className="hidden truncate font-mono sm:inline">· {shortPath(selected.cwd)}</span>}
          </div>
        )}
      </div>
      {selectedId && !isClosed && (
        <Button size="sm" variant="ghost" onClick={() => void endSession()} className="text-muted-foreground">
          <XCircle className="h-4 w-4" />
          <span className="hidden sm:inline">End</span>
        </Button>
      )}
    </div>
  );

  const transcript = (
    <div className="relative min-h-0 flex-1">
      <div
        ref={scrollRef}
        onScroll={(e) => {
          const el = e.currentTarget;
          const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 48;
          stickRef.current = atBottom;
          setShowJump(!atBottom);
        }}
        className="h-full overflow-y-auto overscroll-contain px-3 py-4 md:px-6"
      >
        <div className="mx-auto max-w-3xl space-y-4">
          {!selectedId && (
            <div className="mx-auto max-w-md py-8 text-center">
              <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-2xl bg-accent">
                <Sparkles className="h-6 w-6" />
              </div>
              <h3 className="text-lg font-semibold">Start a Claude Code session</h3>
              <p className="mt-1 text-sm text-muted-foreground">
                Pick a project — it runs like <code className="font-mono">cd &lt;project&gt; &amp;&amp; claude</code>, with
                that project's CLAUDE.md, skills, MCP servers and memory.
              </p>
              <div className="mt-5 flex justify-center">
                <PillSelect
                  icon={<Folder className="h-3.5 w-3.5 shrink-0" />}
                  value={cwd}
                  options={projectOptions}
                  onChange={setCwd}
                  label="Project"
                />
              </div>
            </div>
          )}
          {selectedId && blocks.length === 0 && (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" /> Waiting for the worker…
            </div>
          )}
          {blocks.map((b) =>
            b.kind === "user" ? (
              <div key={b.key} className="flex justify-end">
                <div className="max-w-[88%] rounded-2xl rounded-br-md bg-primary px-3.5 py-2.5 text-sm text-primary-foreground md:max-w-[75%]">
                  {(b.steer || b.scheduled) && (
                    <div className="mb-1 flex items-center gap-1 text-[11px] opacity-75">
                      {b.steer ? (
                        <>
                          <CornerDownRight className="h-3 w-3" /> 插话（执行中送达）
                        </>
                      ) : (
                        <>
                          <Clock className="h-3 w-3" /> 定时：{b.scheduled}
                        </>
                      )}
                    </div>
                  )}
                  <div className="whitespace-pre-wrap break-words">{b.text}</div>
                  {b.attachments.length > 0 && (
                    <div className="mt-2 flex flex-wrap gap-1.5">
                      {b.attachments.map((a, i) => (
                        <span
                          key={i}
                          className="inline-flex max-w-full items-center gap-1 rounded-md bg-primary-foreground/15 px-2 py-0.5 text-xs"
                        >
                          <FileText className="h-3 w-3 shrink-0" />
                          <span className="truncate">{a.name}</span>
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            ) : b.kind === "assistant" ? (
              <MarkdownBlock key={b.key} text={b.text} className="min-w-0 break-words text-sm leading-relaxed" />
            ) : (
              <EventRow
                key={b.key}
                event={b.event}
                open={!!b.event.approval_id && openApprovals.has(b.event.approval_id) && !isClosed}
                onDecide={(id, d) => void decide(id, d)}
                labels={config.model_labels}
              />
            ),
          )}
          {isBusy && (
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <span className="flex gap-1">
                <span className="h-1.5 w-1.5 animate-bounce rounded-full bg-muted-foreground [animation-delay:-0.3s]" />
                <span className="h-1.5 w-1.5 animate-bounce rounded-full bg-muted-foreground [animation-delay:-0.15s]" />
                <span className="h-1.5 w-1.5 animate-bounce rounded-full bg-muted-foreground" />
              </span>
              {effectiveStatus === "WAITING_APPROVAL" ? "Waiting for your approval" : "Working"}
              {!isTouch && <span className="opacity-70">· Esc to interrupt</span>}
            </div>
          )}
        </div>
      </div>
      {showJump && (
        <button
          type="button"
          onClick={() => {
            const el = scrollRef.current;
            if (el) el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
          }}
          className="absolute bottom-3 left-1/2 flex h-9 -translate-x-1/2 items-center gap-1 rounded-full border bg-background px-3 text-xs shadow-md"
        >
          <ArrowDown className="h-3.5 w-3.5" /> Latest
        </button>
      )}
    </div>
  );

  const composer = isClosed ? (
    <div className="shrink-0 border-t px-4 py-3 pb-[max(0.75rem,env(safe-area-inset-bottom))] text-center text-sm text-muted-foreground">
      This session has ended.{" "}
      <button type="button" className="font-medium text-foreground underline" onClick={() => openSession(null)}>
        Start a new one
      </button>
    </div>
  ) : (
    <div
      className={`shrink-0 border-t bg-background px-2 pt-2 pb-[max(0.5rem,env(safe-area-inset-bottom))] md:px-4 md:pb-3 ${
        dragOver ? "bg-accent/40" : ""
      }`}
      onDragOver={(e) => {
        e.preventDefault();
        setDragOver(true);
      }}
      onDragLeave={() => setDragOver(false)}
      onDrop={(e) => {
        e.preventDefault();
        setDragOver(false);
        if (e.dataTransfer.files.length) addFiles(e.dataTransfer.files);
      }}
    >
      <div className="mx-auto max-w-3xl">
        {files.length > 0 && (
          <div className="mb-2 flex gap-1.5 overflow-x-auto pb-1">
            {files.map((f) => (
              <span
                key={f.localId}
                className={`inline-flex h-8 max-w-[14rem] shrink-0 items-center gap-1.5 rounded-lg border px-2 text-xs ${
                  f.error ? "border-destructive/50 text-destructive" : ""
                }`}
                title={f.error ?? f.file.name}
              >
                {f.error ? (
                  <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
                ) : f.fileId ? (
                  <FileText className="h-3.5 w-3.5 shrink-0" />
                ) : (
                  <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin" />
                )}
                <span className="truncate">{f.file.name}</span>
                <span className="shrink-0 text-muted-foreground">{humanSize(f.file.size)}</span>
                <button
                  type="button"
                  aria-label={`Remove ${f.file.name}`}
                  className="-mr-1 flex h-6 w-6 shrink-0 items-center justify-center rounded hover:bg-accent"
                  onClick={() => setFiles((prev) => prev.filter((x) => x.localId !== f.localId))}
                >
                  <X className="h-3 w-3" />
                </button>
              </span>
            ))}
          </div>
        )}

        <div className="rounded-2xl border bg-card shadow-sm focus-within:ring-2 focus-within:ring-ring/40">
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onPaste={(e) => {
              if (e.clipboardData.files.length) {
                e.preventDefault();
                addFiles(e.clipboardData.files);
              }
            }}
            onKeyDown={(e) => {
              if (e.key === "Tab" && e.shiftKey) {
                // Shift+Tab cycles the permission mode, like the CLI.
                e.preventDefault();
                const i = MODE_CYCLE.indexOf(mode);
                void changeMode(MODE_CYCLE[(i + 1) % MODE_CYCLE.length]!);
                return;
              }
              if (e.key === "Enter" && !e.shiftKey && !isTouch && !e.nativeEvent.isComposing) {
                e.preventDefault();
                void send();
              }
            }}
            rows={1}
            placeholder={
              isBusy
                ? "Steer Claude… (read when the current tool finishes)"
                : selectedId
                  ? "Reply to Claude…"
                  : "What should Claude do?"
            }
            className="block max-h-[200px] w-full resize-none bg-transparent px-3.5 pt-3 pb-1 text-base outline-none placeholder:text-muted-foreground md:text-sm"
          />
          <div className="flex items-center gap-1.5 px-2 pb-2">
            <input
              ref={fileInputRef}
              type="file"
              multiple
              className="hidden"
              onChange={(e) => {
                if (e.target.files) addFiles(e.target.files);
                e.target.value = "";
              }}
            />
            <Button
              size="icon-sm"
              variant="ghost"
              className="h-8 w-8 shrink-0 rounded-full"
              onClick={() => fileInputRef.current?.click()}
              aria-label="Attach files"
            >
              <Paperclip className="h-4 w-4" />
            </Button>
            <div className="flex min-w-0 flex-1 items-center gap-1.5 overflow-x-auto">
              <PillSelect
                icon={<Shield className="h-3.5 w-3.5 shrink-0" />}
                value={mode}
                options={modeOptions}
                onChange={(v) => void changeMode(v)}
                label="Permission mode (Shift+Tab)"
              />
              {modelOptions.length > 0 && (
                <PillSelect
                  icon={<Sparkles className="h-3.5 w-3.5 shrink-0" />}
                  value={model || modelOptions[0]!.value}
                  options={modelOptions}
                  onChange={(v) => void changeModel(v)}
                  label="Model"
                />
              )}
            </div>
            {isBusy && !input.trim() ? (
              <Button
                size="icon-sm"
                variant="outline"
                className="h-9 w-9 shrink-0 rounded-full"
                onClick={() => void interrupt()}
                aria-label="Interrupt"
                title="Interrupt (Esc)"
              >
                <Square className="h-3.5 w-3.5 fill-current" />
              </Button>
            ) : (
              <Button
                size="icon-sm"
                className="h-9 w-9 shrink-0 rounded-full"
                disabled={sending || uploading || !input.trim()}
                onClick={() => void send()}
                aria-label="Send"
              >
                {sending || uploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
              </Button>
            )}
          </div>
        </div>
        {!isTouch && (
          <div className="mt-1.5 hidden px-2 text-[10px] text-muted-foreground/70 md:block">
            Enter to send · Shift+Enter new line · Shift+Tab switch mode · drop or paste files to attach
          </div>
        )}
      </div>
    </div>
  );

  return (
    <div className="flex h-[calc(100dvh-3.5rem)] min-h-0 overflow-hidden bg-background md:h-[calc(100dvh-2rem)] md:rounded-xl md:border">
      {sessionList}
      <section className={`${showConversation ? "flex" : "hidden"} md:flex min-h-0 min-w-0 flex-1 flex-col`}>
        {header}
        {transcript}
        {composer}
      </section>
      {showSchedules && (
        <ClaudeSchedulesPanel
          onClose={() => {
            setShowSchedules(false);
            void refreshSessions();
          }}
          currentSessionId={selectedId && !isClosed ? selectedId : null}
          cwd={cwd || selected?.cwd || undefined}
          model={model || undefined}
          onOpenSession={(id) => {
            void refreshSessions();
            openSession(id);
          }}
        />
      )}
    </div>
  );
}
