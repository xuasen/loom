import { useCallback, useEffect, useState } from "react";
import { Clock, Loader2, Pause, Play, Plus, Trash2, X, Zap } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
  MODE_LABELS,
  createClaudeSchedule,
  deleteClaudeSchedule,
  listClaudeSchedules,
  previewClaudeSchedule,
  runClaudeScheduleNow,
  updateClaudeSchedule,
  type ClaudeSchedule,
  type ClaudeSessionMode,
} from "@/api/claude";

const PRESETS: { label: string; cron: string }[] = [
  { label: "每天 09:00", cron: "0 9 * * *" },
  { label: "工作日 09:00", cron: "0 9 * * 1-5" },
  { label: "每小时", cron: "0 * * * *" },
  { label: "每 30 分钟", cron: "*/30 * * * *" },
  { label: "每周一 10:00", cron: "0 10 * * 1" },
];

const SCHEDULE_MODES: ClaudeSessionMode[] = ["unattended", "auto", "acceptEdits", "default", "plan"];

function fmt(iso?: string | null): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

const field = "h-10 w-full rounded-lg border bg-background px-3 text-base md:text-sm outline-none focus:ring-2 focus:ring-ring/40";

/**
 * Scheduled prompts: send a message to Claude on a cron, either into the
 * current session (context carries over) or into a fresh session per run.
 */
export function ClaudeSchedulesPanel({
  onClose,
  currentSessionId,
  cwd,
  model,
  onOpenSession,
}: {
  onClose: () => void;
  currentSessionId: string | null;
  cwd?: string;
  model?: string;
  onOpenSession: (id: string) => void;
}) {
  const [items, setItems] = useState<ClaudeSchedule[] | null>(null);
  const [creating, setCreating] = useState(false);
  const [saving, setSaving] = useState(false);
  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone || "Asia/Shanghai";
  const [form, setForm] = useState({
    name: "",
    cron: "0 9 * * *",
    message: "",
    target: (currentSessionId ? "session" : "new") as "session" | "new",
    mode: "unattended" as ClaudeSessionMode,
  });
  const [preview, setPreview] = useState<string[] | string>([]);

  const load = useCallback(async () => {
    try {
      setItems((await listClaudeSchedules()).schedules);
    } catch (e) {
      toast.error(`Could not load schedules: ${(e as Error).message}`);
      setItems([]);
    }
  }, []);

  useEffect(() => void load(), [load]);

  useEffect(() => {
    if (!creating) return;
    const id = setTimeout(() => {
      previewClaudeSchedule(form.cron.trim(), tz)
        .then((r) => setPreview(r.next_runs))
        .catch((e: unknown) => setPreview((e as Error).message));
    }, 400);
    return () => clearTimeout(id);
  }, [form.cron, creating, tz]);

  const save = async () => {
    if (!form.name.trim() || !form.message.trim()) return;
    setSaving(true);
    try {
      await createClaudeSchedule({
        name: form.name.trim(),
        cron: form.cron.trim(),
        tz,
        message: form.message.trim(),
        target: form.target,
        session_id: form.target === "session" ? currentSessionId ?? undefined : undefined,
        mode: form.mode,
        model: form.target === "new" ? model || undefined : undefined,
        cwd: form.target === "new" ? cwd || undefined : undefined,
      });
      setCreating(false);
      setForm((f) => ({ ...f, name: "", message: "" }));
      toast.success("Schedule created");
      void load();
    } catch (e) {
      toast.error((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const act = async (fn: () => Promise<unknown>, ok?: string) => {
    try {
      await fn();
      if (ok) toast.success(ok);
      void load();
    } catch (e) {
      toast.error((e as Error).message);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center bg-black/40 md:items-center" onClick={onClose}>
      <div
        className="flex max-h-[90dvh] w-full flex-col rounded-t-2xl border bg-background shadow-xl md:max-w-xl md:rounded-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex h-14 shrink-0 items-center gap-2 border-b px-4">
          <Clock className="h-5 w-5" />
          <span className="flex-1 font-semibold">定时任务</span>
          {!creating && (
            <Button size="sm" onClick={() => setCreating(true)}>
              <Plus className="h-4 w-4" /> 新建
            </Button>
          )}
          <Button size="icon-sm" variant="ghost" onClick={onClose} aria-label="Close">
            <X className="h-4 w-4" />
          </Button>
        </div>

        <div className="flex-1 overflow-y-auto overscroll-contain p-4 pb-[max(1rem,env(safe-area-inset-bottom))]">
          {creating && (
            <div className="mb-4 space-y-3 rounded-xl border p-3">
              <input
                className={field}
                placeholder="名称，例如：每日依赖巡检"
                value={form.name}
                maxLength={120}
                onChange={(e) => setForm({ ...form, name: e.target.value })}
              />
              <div className="flex flex-wrap gap-1.5">
                {PRESETS.map((p) => (
                  <button
                    key={p.cron}
                    type="button"
                    onClick={() => setForm({ ...form, cron: p.cron })}
                    className={`h-8 rounded-full border px-3 text-xs ${
                      form.cron === p.cron ? "bg-primary text-primary-foreground" : "hover:bg-accent"
                    }`}
                  >
                    {p.label}
                  </button>
                ))}
              </div>
              <div>
                <input
                  className={`${field} font-mono`}
                  value={form.cron}
                  onChange={(e) => setForm({ ...form, cron: e.target.value })}
                  aria-label="Cron (minute hour day month weekday)"
                />
                <div className="mt-1 text-xs text-muted-foreground">
                  {typeof preview === "string" ? (
                    <span className="text-destructive">{preview}</span>
                  ) : (
                    <>
                      {tz} · 接下来：{preview.map(fmt).join("，") || "…"}
                    </>
                  )}
                </div>
              </div>
              <textarea
                className={`${field} h-24 py-2`}
                placeholder="发给 Claude 的消息"
                value={form.message}
                onChange={(e) => setForm({ ...form, message: e.target.value })}
              />
              <div className="grid grid-cols-2 gap-2">
                <select
                  className={field}
                  value={form.target}
                  onChange={(e) => setForm({ ...form, target: e.target.value as "session" | "new" })}
                  aria-label="Target"
                >
                  <option value="new">每次新开会话</option>
                  <option value="session" disabled={!currentSessionId}>
                    发到当前会话
                  </option>
                </select>
                <select
                  className={field}
                  value={form.mode}
                  onChange={(e) => setForm({ ...form, mode: e.target.value as ClaudeSessionMode })}
                  aria-label="Mode"
                  disabled={form.target === "session"}
                >
                  {SCHEDULE_MODES.map((m) => (
                    <option key={m} value={m}>
                      {MODE_LABELS[m]}
                    </option>
                  ))}
                </select>
              </div>
              <p className="text-xs text-muted-foreground">
                {form.target === "new"
                  ? "每次运行打开一个新会话（项目和模型沿用当前选择），上一次已空闲的会话会被结束。无人值守模式下，白名单外的操作会被拒绝而不是等待审批。"
                  : "消息会像你发的一样进入当前会话，沿用它的上下文和权限模式。"}
              </p>
              <div className="flex justify-end gap-2">
                <Button variant="ghost" onClick={() => setCreating(false)}>
                  取消
                </Button>
                <Button
                  disabled={saving || !form.name.trim() || !form.message.trim() || typeof preview === "string"}
                  onClick={() => void save()}
                >
                  {saving && <Loader2 className="h-4 w-4 animate-spin" />} 保存
                </Button>
              </div>
            </div>
          )}

          {items === null ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading…
            </div>
          ) : items.length === 0 && !creating ? (
            <div className="py-10 text-center text-sm text-muted-foreground">还没有定时任务。</div>
          ) : (
            <div className="space-y-2">
              {items.map((s) => (
                <div key={s.schedule_id} className={`rounded-xl border p-3 ${s.enabled ? "" : "opacity-60"}`}>
                  <div className="flex items-center gap-2">
                    <span className="min-w-0 flex-1 truncate text-sm font-medium">{s.name}</span>
                    <code className="shrink-0 rounded bg-muted px-1.5 py-0.5 text-[11px]">{s.cron}</code>
                  </div>
                  <div className="mt-1 line-clamp-2 text-xs text-muted-foreground">{s.message}</div>
                  <div className="mt-1.5 text-[11px] text-muted-foreground">
                    {s.target === "session" ? "→ 固定会话" : "→ 每次新会话"} · {MODE_LABELS[s.mode]} · 下次{" "}
                    {s.enabled ? fmt(s.next_run) : "已暂停"} · 已运行 {s.runs} 次
                  </div>
                  {s.last_error && <div className="mt-1 text-[11px] text-destructive">{s.last_error}</div>}
                  <div className="mt-2 flex flex-wrap gap-1.5">
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => void act(() => runClaudeScheduleNow(s.schedule_id), "已触发，几秒后开始")}
                    >
                      <Zap className="h-3.5 w-3.5" /> 立即运行
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => void act(() => updateClaudeSchedule(s.schedule_id, { enabled: !s.enabled }))}
                    >
                      {s.enabled ? <Pause className="h-3.5 w-3.5" /> : <Play className="h-3.5 w-3.5" />}
                      {s.enabled ? "暂停" : "恢复"}
                    </Button>
                    {(s.last_session_id || s.session_id) && (
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => {
                          onOpenSession((s.target === "session" ? s.session_id : s.last_session_id)!);
                          onClose();
                        }}
                      >
                        打开会话
                      </Button>
                    )}
                    <Button
                      size="sm"
                      variant="ghost"
                      className="ml-auto text-muted-foreground"
                      onClick={() => {
                        if (window.confirm(`删除定时任务「${s.name}」？`)) {
                          void act(() => deleteClaudeSchedule(s.schedule_id), "已删除");
                        }
                      }}
                      aria-label="Delete"
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
