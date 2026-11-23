import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  AlertTriangle, ArrowDown, Menu, Check, ChevronRight, Clock, CornerDownRight, FileText, Folder, FolderInput, GripHorizontal, Loader2, Maximize2, Minimize2, Paperclip, Send, Shield, Sparkles, Square, Trash2, X, XCircle,
} from "lucide-react";
import { notifyClaudeChanged } from "@/hooks/useClaudeNav";
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
  decideClaudeApproval,
  deleteClaudeSession,
  getClaudeConfig,
  getClaudeEvents,
  getClaudeSession,
  interruptClaudeSession,
  listClaudeSessions,
  modelLabel,
  sendClaudeMessage,
  setClaudeSessionFolder,
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
          <pre className="mt-1 max-h-72 overflow-auto whitespace-pre-wrap break-words rounded-lg bg-muted/50 p-3">
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
  compact,
}: {
  icon: React.ReactNode;
  value: T;
  options: { value: T; label: string }[];
  onChange: (v: T) => void;
  label: string;
  disabled?: boolean;
  /** Icon only on phones. */
  compact?: boolean;
}) {
  const current = options.find((o) => o.value === value)?.label ?? value;
  return (
    <label
      className={`relative inline-flex h-8 max-w-[11rem] shrink-0 items-center gap-1.5 rounded-full px-2.5 text-xs text-muted-foreground transition-colors ${
        compact ? "w-8 justify-center px-0 sm:w-auto sm:justify-start sm:px-2.5" : ""
      } ${disabled ? "opacity-50" : "hover:bg-accent hover:text-foreground"}`}
      title={label}
    >
      {icon}
      <span className={`truncate ${compact ? "hidden sm:inline" : ""}`}>{current}</span>
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

/** What the sidebar asked the page to show. `nonce` makes a repeated click count. */
export interface ClaudeNavRequest {
  nonce: number;
  /** Open this session. */
  sessionId?: string;
  /** Start a new session (in `project`, on `device`). */
  newSession?: boolean;
}

export function ClaudeTerminalPage({
  project = null,
  device = null,
  request = null,
  onOpenNav,
}: {
  /** Show only this folder ("项目"); new sessions go into it. */
  project?: string | null;
  /** Show only sessions on this worker ("device"); new sessions run on it. */
  device?: string | null;
  request?: ClaudeNavRequest | null;
  /** Mobile: open the app navigation drawer (the page owns the only top bar). */
  onOpenNav?: () => void;
} = {}) {
  const isTouch = useIsTouch();
  const [config, setConfig] = useState<ClaudeConfig | null>(null);
  const [configError, setConfigError] = useState<string | null>(null);
  const [sessions, setSessions] = useState<ClaudeSession[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selected, setSelected] = useState<ClaudeSession | null>(null);
  // Mobile master/detail: show the conversation pane instead of the list.
  // The session list lives in the app sidebar now; kept as a no-op for the old call sites.
  const setShowConversation = (_open: boolean) => {};

  const [input, setInput] = useState("");
  const [sending, setSending] = useState(false);
  const [files, setFiles] = useState<PendingFile[]>([]);
  const [dragOver, setDragOver] = useState(false);
  const [newFolder, setNewFolder] = useState("");
  // Composer height: null = grow with the text (up to ~8 lines); a number = dragged/maximised.
  const [composerHeight, setComposerHeight] = useState<number | null>(null);
  const dragRef = useRef<{ startY: number; startH: number } | null>(null);

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
  const worker = config?.workers.find((w) => w.worker_id === device) ?? config?.workers[0];
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

  // Sidebar navigation: open a session, or start a new one in this scope.
  useEffect(() => {
    if (!request) return;
    if (request.sessionId) {
      setSelectedId(request.sessionId);
      setShowConversation(true);
      stickRef.current = true;
    } else if (request.newSession) {
      setSelectedId(null);
      setShowConversation(true);
      setNewFolder(project ?? "");
      setMode("default");
      setCwd("");
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [request?.nonce]);

  // Picking a project/device in the sidebar without a session opens a new session there.
  useEffect(() => {
    if (request?.sessionId || request?.newSession) return;
    setSelectedId(null);
    setNewFolder(project ?? "");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [project, device]);

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
    if (composerHeight !== null) {
      el.style.height = `${composerHeight}px`;
      return;
    }
    el.style.height = "auto";
    el.style.height = `${Math.min(el.scrollHeight, 200)}px`;
  }, [input, composerHeight]);

  const maxComposer = () => Math.round(window.innerHeight * 0.7);
  const toggleComposerSize = () =>
    setComposerHeight((h) => (h !== null && h >= window.innerHeight * 0.45 ? null : Math.round(window.innerHeight * 0.5)));

  const openSession = (id: string | null, folderForNew?: string) => {
    setSelectedId(id);
    setShowConversation(true);
    stickRef.current = true;
    setShowJump(false);
    if (!id) {
      setNewFolder(folderForNew ?? "");
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
          folder: newFolder || project || undefined,
          worker_id: worker?.worker_id,
        });
        setSessions((prev) => [s, ...prev]);
        notifyClaudeChanged();
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

  const folders = useMemo(
    () => Array.from(new Set(sessions.map((s) => s.folder || "").filter(Boolean))).sort((a, b) => a.localeCompare(b)),
    [sessions],
  );

  const moveToFolder = async (choice: string) => {
    if (!selectedId) return;
    let folder = choice;
    if (choice === "__new__") {
      const name = window.prompt("New folder name (use / for sub-folders, e.g. altitude/ios)");
      if (!name?.trim()) return;
      folder = name.trim();
    }
    try {
      const s = await setClaudeSessionFolder(selectedId, folder);
      setSelected(s);
      setSessions((prev) => prev.map((x) => (x.session_id === s.session_id ? { ...x, folder: s.folder } : x)));
      notifyClaudeChanged();
    } catch (e) {
      toast.error(`Could not move: ${(e as Error).message}`);
    }
  };

  const deleteSession = async () => {
    if (!selectedId) return;
    const title = selected?.title || "this session";
    if (!window.confirm(`Delete "${title.slice(0, 60)}"? It is ended first if still running, and removed from the list.`)) {
      return;
    }
    try {
      await deleteClaudeSession(selectedId);
      setSessions((prev) => prev.filter((s) => s.session_id !== selectedId));
      setSelectedId(null);
      setShowConversation(false);
      toast.success("Session deleted");
      notifyClaudeChanged();
    } catch (e) {
      toast.error(`Could not delete: ${(e as Error).message}`);
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

  const header = (
    <header className="z-10 flex shrink-0 items-center gap-1 bg-background/80 px-2 pt-[env(safe-area-inset-top)] backdrop-blur-xl md:px-5 md:pt-0">
      {onOpenNav && (
        <Button size="icon-sm" variant="ghost" className="h-10 w-10 md:hidden" onClick={onOpenNav} aria-label="Open menu">
          <Menu className="h-5 w-5" />
        </Button>
      )}
      <div className="flex h-14 min-w-0 flex-1 flex-col justify-center px-1">
        <div className="truncate text-[15px] font-semibold tracking-tight">
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
      {selectedId && selected && (
        <PillSelect
          icon={<FolderInput className="h-3.5 w-3.5 shrink-0" />}
          value={selected.folder || ""}
          options={[
            { value: "", label: "No folder" },
            ...folders.map((f) => ({ value: f, label: f })),
            { value: "__new__", label: "New folder…" },
          ]}
          onChange={(v) => void moveToFolder(v)}
          label="Move to folder"
          compact
        />
      )}
      {selectedId && !isClosed && (
        <Button
          size="sm"
          variant="ghost"
          onClick={() => void endSession()}
          className="h-8 px-2 text-muted-foreground"
          title="End session"
        >
          <XCircle className="h-4 w-4" />
          <span className="hidden sm:inline">End</span>
        </Button>
      )}
      {selectedId && (
        <Button
          size="icon-sm"
          variant="ghost"
          onClick={() => void deleteSession()}
          className="h-8 w-8 text-muted-foreground hover:text-destructive"
          aria-label="Delete session"
          title="Delete session"
        >
          <Trash2 className="h-4 w-4" />
        </Button>
      )}
    </header>
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
        className="h-full overflow-y-auto overscroll-contain px-4 pt-4 pb-6 md:px-8 md:pt-6"
      >
        <div className="mx-auto max-w-3xl space-y-5">
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
              {newFolder && (
                <div className="mt-3 inline-flex items-center gap-1.5 rounded-full bg-accent px-3 py-1 text-xs">
                  <Folder className="h-3.5 w-3.5" /> {newFolder}
                </div>
              )}
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
          className="absolute bottom-3 left-1/2 flex h-9 -translate-x-1/2 items-center gap-1 rounded-full bg-background/90 px-3.5 text-xs shadow-lg backdrop-blur"
        >
          <ArrowDown className="h-3.5 w-3.5" /> Latest
        </button>
      )}
    </div>
  );

  const composer = isClosed ? (
    <div className="shrink-0 px-4 py-4 pb-[max(1rem,env(safe-area-inset-bottom))] text-center text-sm text-muted-foreground">
      This session has ended.{" "}
      <button type="button" className="font-medium text-foreground underline" onClick={() => openSession(null)}>
        Start a new one
      </button>
    </div>
  ) : (
    <div
      className={`shrink-0 bg-background px-3 pt-1 pb-[max(0.75rem,env(safe-area-inset-bottom))] md:px-8 md:pb-5 ${
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
                className={`inline-flex h-8 max-w-[14rem] shrink-0 items-center gap-1.5 rounded-lg bg-muted px-2 text-xs ${
                  f.error ? "bg-destructive/10 text-destructive" : ""
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

        <div className="relative rounded-[22px] bg-muted/70 shadow-[0_1px_2px_rgba(0,0,0,0.04),0_10px_30px_-16px_rgba(0,0,0,0.25)] transition-shadow focus-within:bg-muted focus-within:shadow-[0_1px_2px_rgba(0,0,0,0.06),0_12px_36px_-14px_rgba(0,0,0,0.3)]">
          {/* Drag to resize the input (double-click resets it). */}
          <div
            role="separator"
            aria-orientation="horizontal"
            aria-label="Resize input"
            title="Drag to resize · double-click to reset"
            className="absolute -top-2 left-1/2 flex h-4 w-12 -translate-x-1/2 cursor-ns-resize touch-none items-center justify-center rounded-full text-muted-foreground/50 hover:bg-muted hover:text-foreground"
            onPointerDown={(e) => {
              e.currentTarget.setPointerCapture(e.pointerId);
              dragRef.current = { startY: e.clientY, startH: inputRef.current?.offsetHeight ?? 44 };
            }}
            onPointerMove={(e) => {
              const d = dragRef.current;
              if (!d) return;
              setComposerHeight(Math.max(44, Math.min(maxComposer(), d.startH + (d.startY - e.clientY))));
            }}
            onPointerUp={() => {
              dragRef.current = null;
            }}
            onDoubleClick={() => setComposerHeight(null)}
          >
            <GripHorizontal className="h-3.5 w-3.5" />
          </div>
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
            className={`block w-full resize-none bg-transparent px-3.5 pt-3 pb-1 text-base outline-none placeholder:text-muted-foreground md:text-sm ${
              composerHeight === null ? "max-h-[200px]" : "max-h-[70dvh]"
            }`}
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
            <Button
              size="icon-sm"
              variant="ghost"
              className="h-8 w-8 shrink-0 rounded-full"
              onClick={toggleComposerSize}
              aria-label={composerHeight !== null && composerHeight >= window.innerHeight * 0.45 ? "Shrink input" : "Enlarge input"}
              title="Enlarge / shrink the input"
            >
              {composerHeight !== null && composerHeight >= window.innerHeight * 0.45 ? (
                <Minimize2 className="h-4 w-4" />
              ) : (
                <Maximize2 className="h-4 w-4" />
              )}
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
          <div className="mt-2 hidden text-center text-[10px] text-muted-foreground/60 md:block">
            Enter to send · Shift+Enter new line · Shift+Tab switch mode · drop or paste files to attach
          </div>
        )}
      </div>
    </div>
  );

  return (
    <div className="flex min-h-0 flex-1 overflow-hidden bg-background">
      <section className="flex min-h-0 min-w-0 flex-1 flex-col">
        {header}
        {transcript}
        {composer}
      </section>
    </div>
  );
}
