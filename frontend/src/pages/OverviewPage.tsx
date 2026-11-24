import { useCallback, useEffect, useMemo, useState } from "react";
import {
  AlertTriangle, BellRing, CalendarClock, CheckCircle2, CircleDollarSign, Loader2, RefreshCw, ServerCrash, Shield,
  SquareTerminal,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { listClaudeSchedules, type ClaudeSchedule, type ClaudeSession, type ClaudeWorker } from "@/api/claude";
import { fetchCostDashboard } from "@/api/costs";
import { listApprovalLogs } from "@/api/approvals";
import type { AgentResponse, ApprovalLog, CostDashboardResponse } from "@/api/types";

/**
 * 总览: what needs a person now, what is broken, this month's spend and what
 * is scheduled next. Each source loads on its own; one that fails or is not
 * allowed for this user is shown as unavailable instead of blocking the page.
 */

type Tone = "urgent" | "alert" | "todo";

interface Item {
  key: string;
  tone: Tone;
  title: string;
  detail?: string;
  when?: string | null;
  onOpen?: () => void;
}

const TONE_STYLE: Record<Tone, string> = {
  urgent: "border-destructive/50 bg-destructive/5",
  alert: "border-warning/50 bg-warning-bg",
  todo: "border-border bg-card",
};

function ago(iso?: string | null): string {
  if (!iso) return "";
  const s = (Date.now() - new Date(iso).getTime()) / 1000;
  if (s < 60) return "刚刚";
  if (s < 3600) return `${Math.floor(s / 60)} 分钟前`;
  if (s < 86400) return `${Math.floor(s / 3600)} 小时前`;
  return `${Math.floor(s / 86400)} 天前`;
}

function when(iso?: string | null): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString(undefined, { month: "numeric", day: "numeric", weekday: "short", hour: "2-digit", minute: "2-digit" });
}

const usd = (n: number) => `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

function isFailed(a: AgentResponse): boolean {
  return [a.status, a.deployment_status, a.endpoint_status].some((v) => (v ?? "").toUpperCase().includes("FAIL"));
}

function Card({
  icon: Icon,
  title,
  right,
  children,
  className = "",
}: {
  icon: React.ComponentType<{ className?: string }>;
  title: string;
  right?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <section className={`rounded-xl border bg-card p-4 ${className}`}>
      <div className="mb-3 flex items-center gap-2">
        <Icon className="h-4 w-4 text-muted-foreground" />
        <h2 className="flex-1 text-sm font-semibold">{title}</h2>
        {right}
      </div>
      {children}
    </section>
  );
}

function ItemList({ items, empty }: { items: Item[]; empty: string }) {
  if (items.length === 0) {
    return (
      <div className="flex items-center gap-2 py-3 text-sm text-muted-foreground">
        <CheckCircle2 className="h-4 w-4 text-success" /> {empty}
      </div>
    );
  }
  return (
    <ul className="space-y-2">
      {items.map((i) => (
        <li key={i.key}>
          <button
            type="button"
            disabled={!i.onOpen}
            onClick={i.onOpen}
            className={`w-full rounded-lg border px-3 py-2 text-left transition-colors enabled:hover:bg-accent/50 ${TONE_STYLE[i.tone]}`}
          >
            <div className="flex items-center gap-2">
              <span className="min-w-0 flex-1 truncate text-sm font-medium">{i.title}</span>
              {i.when && <span className="shrink-0 text-[11px] text-muted-foreground">{ago(i.when)}</span>}
            </div>
            {i.detail && <div className="mt-0.5 truncate text-xs text-muted-foreground">{i.detail}</div>}
          </button>
        </li>
      ))}
    </ul>
  );
}

export function OverviewPage({
  agents,
  claudeSessions,
  claudeWorkers,
  canClaude,
  canCosts,
  canApprovals,
  costsGroup,
  onOpenClaudeSession,
  onOpenAgent,
  onOpenAnalytics,
}: {
  agents: AgentResponse[];
  claudeSessions: ClaudeSession[];
  claudeWorkers: ClaudeWorker[];
  canClaude: boolean;
  canCosts: boolean;
  canApprovals: boolean;
  costsGroup?: string;
  onOpenClaudeSession: (id: string) => void;
  onOpenAgent: (id: number) => void;
  onOpenAnalytics: () => void;
}) {
  const [schedules, setSchedules] = useState<ClaudeSchedule[] | null>(null);
  const [costs, setCosts] = useState<CostDashboardResponse | null | "error">(null);
  const [approvals, setApprovals] = useState<ApprovalLog[] | null>(null);
  const [loading, setLoading] = useState(false);

  const now = new Date();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const daysThisMonth = now.getDate();

  const load = useCallback(async () => {
    setLoading(true);
    await Promise.all([
      canClaude
        ? listClaudeSchedules().then((r) => setSchedules(r.schedules)).catch(() => setSchedules([]))
        : Promise.resolve(),
      canCosts
        ? fetchCostDashboard(daysThisMonth, costsGroup).then(setCosts).catch(() => setCosts("error"))
        : Promise.resolve(),
      canApprovals
        ? listApprovalLogs({ status: "pending" }).then(setApprovals).catch(() => setApprovals([]))
        : Promise.resolve(),
    ]);
    setLoading(false);
  }, [canClaude, canCosts, canApprovals, costsGroup, daysThisMonth]);

  useEffect(() => void load(), [load]);

  const { urgent, alerts, todos } = useMemo(() => {
    const urgent: Item[] = [];
    const alerts: Item[] = [];
    const todos: Item[] = [];
    for (const s of claudeSessions) {
      if (s.status === "WAITING_APPROVAL") {
        urgent.push({
          key: `c-${s.session_id}`, tone: "urgent", title: `Claude 等待审批：${s.title || "会话"}`,
          detail: `${s.worker_id}${s.cwd ? ` · ${s.cwd}` : ""}`, when: s.updated_at, onOpen: () => onOpenClaudeSession(s.session_id),
        });
      } else if (s.status === "FAILED") {
        alerts.push({
          key: `cf-${s.session_id}`, tone: "alert", title: `Claude 会话失败：${s.title || "会话"}`,
          detail: s.worker_id, when: s.updated_at, onOpen: () => onOpenClaudeSession(s.session_id),
        });
      } else if (s.status === "RUNNING") {
        todos.push({
          key: `cr-${s.session_id}`, tone: "todo", title: `进行中：${s.title || "会话"}`,
          detail: s.worker_id, when: s.updated_at, onOpen: () => onOpenClaudeSession(s.session_id),
        });
      }
    }
    for (const a of approvals ?? []) {
      urgent.push({
        key: `a-${a.id}`, tone: "urgent", title: `Agent 工具审批：${a.tool_name}`,
        detail: [agents.find((x) => x.id === a.agent_id)?.name, a.policy_name, a.tool_input_summary].filter(Boolean).join(" · "),
        when: a.requested_at, onOpen: a.agent_id ? () => onOpenAgent(a.agent_id!) : undefined,
      });
    }
    for (const a of agents) {
      if (isFailed(a)) {
        alerts.push({
          key: `ag-${a.id}`, tone: "alert", title: `Agent 部署失败：${a.name ?? a.runtime_id}`,
          detail: a.status_reason ?? a.deployment_status ?? a.status ?? undefined, onOpen: () => onOpenAgent(a.id),
        });
      }
    }
    for (const w of claudeWorkers) {
      if (!w.online) {
        alerts.push({
          key: `w-${w.worker_id}`, tone: "alert", title: `Claude device 离线：${w.worker_id}`,
          detail: "worker 超过 90 秒没有心跳", when: w.last_heartbeat,
        });
      }
    }
    for (const sc of schedules ?? []) {
      if (sc.last_error) {
        alerts.push({ key: `se-${sc.schedule_id}`, tone: "alert", title: `定时任务出错：${sc.name}`, detail: sc.last_error, when: sc.last_run });
      }
    }
    return { urgent, alerts, todos };
  }, [claudeSessions, claudeWorkers, approvals, agents, schedules, onOpenClaudeSession, onOpenAgent]);

  const claudeMonth = claudeSessions
    .filter((s) => s.created_at && new Date(s.created_at) >= monthStart)
    .reduce((sum, s) => sum + (s.cost_usd ?? 0), 0);
  const agentMonth = costs && costs !== "error" ? costs.total_estimated_cost : null;

  const upcoming = (schedules ?? [])
    .filter((s) => s.enabled && s.next_run)
    .sort((a, b) => (a.next_run! < b.next_run! ? -1 : 1))
    .slice(0, 6);

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <div className="flex-1">
          <h1 className="text-xl font-semibold">总览</h1>
          <p className="text-sm text-muted-foreground">
            {now.toLocaleDateString(undefined, { year: "numeric", month: "long", day: "numeric", weekday: "long" })}
          </p>
        </div>
        <Button size="sm" variant="outline" onClick={() => void load()} disabled={loading}>
          {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />} 刷新
        </Button>
      </div>

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {[
          { label: "紧急", value: urgent.length, cls: urgent.length ? "text-destructive" : "" },
          { label: "告警", value: alerts.length, cls: alerts.length ? "text-warning" : "" },
          { label: "进行中", value: todos.length, cls: "" },
          {
            label: "本月费用",
            value: agentMonth === null && !canClaude ? "—" : usd((agentMonth ?? 0) + claudeMonth),
            cls: "",
          },
        ].map((k) => (
          <div key={k.label} className="rounded-xl border bg-card p-3">
            <div className="text-xs text-muted-foreground">{k.label}</div>
            <div className={`mt-1 text-2xl font-semibold tabular-nums ${k.cls}`}>{k.value}</div>
          </div>
        ))}
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card icon={BellRing} title="紧急 · 需要你处理">
          <ItemList items={urgent} empty="没有等待你审批的事项" />
        </Card>
        <Card icon={AlertTriangle} title="告警事件">
          <ItemList items={alerts} empty="一切正常" />
        </Card>

        <Card
          icon={CircleDollarSign}
          title={`本月费用（${monthStart.getMonth() + 1} 月 1 日至今）`}
          right={canCosts ? <Button size="sm" variant="ghost" onClick={onOpenAnalytics}>明细</Button> : undefined}
        >
          <div className="space-y-2 text-sm">
            <div className="flex justify-between">
              <span className="text-muted-foreground">AgentCore agents(估算)</span>
              <span className="tabular-nums">
                {!canCosts ? "无权限" : costs === null ? "…" : costs === "error" ? "不可用" : usd(costs.total_estimated_cost)}
              </span>
            </div>
            {costs && costs !== "error" && (
              <div className="flex justify-between text-xs text-muted-foreground">
                <span>{costs.total_invocations.toLocaleString()} 次调用</span>
                <span>
                  {(costs.total_input_tokens / 1000).toFixed(0)}k 入 / {(costs.total_output_tokens / 1000).toFixed(0)}k 出 tokens
                </span>
              </div>
            )}
            {canClaude && (
              <div className="flex justify-between">
                <span className="text-muted-foreground">Claude device 会话(Bedrock)</span>
                <span className="tabular-nums">{usd(claudeMonth)}</span>
              </div>
            )}
            <p className="pt-1 text-[11px] text-muted-foreground">
              Agent 费用是 Baton 按调用量估算的；Claude 会话费用是 Claude Code 报告的 token 费用。都不含 ECS、RDS、NAT 等基础设施。
            </p>
          </div>
        </Card>

        <Card icon={CalendarClock} title="日程">
          {!canClaude ? (
            <div className="py-3 text-sm text-muted-foreground">无权限查看定时任务</div>
          ) : schedules === null ? (
            <div className="py-3 text-sm text-muted-foreground">…</div>
          ) : upcoming.length === 0 ? (
            <div className="py-3 text-sm text-muted-foreground">没有即将运行的定时任务</div>
          ) : (
            <ul className="divide-y">
              {upcoming.map((s) => (
                <li key={s.schedule_id} className="flex items-center gap-3 py-2">
                  <div className="w-28 shrink-0 text-xs tabular-nums text-muted-foreground">{when(s.next_run)}</div>
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-sm">{s.name}</div>
                    <div className="truncate text-xs text-muted-foreground">{s.message}</div>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </Card>

        <Card icon={SquareTerminal} title="进行中" className="lg:col-span-2">
          <ItemList items={todos} empty="当前没有正在运行的会话" />
        </Card>
      </div>

      {!canApprovals && !canCosts && !canClaude && (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Shield className="h-4 w-4" /> 你的账号权限有限，部分卡片不显示。
        </div>
      )}
      {claudeWorkers.length > 0 && claudeWorkers.every((w) => !w.online) && (
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <ServerCrash className="h-4 w-4" /> 所有 Claude device 离线，新消息会排队到设备恢复后执行。
        </div>
      )}
    </div>
  );
}
