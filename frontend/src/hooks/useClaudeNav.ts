import { useCallback, useEffect, useState } from "react";
import { getClaudeConfig, listClaudeSessions, type ClaudeSession, type ClaudeWorker } from "@/api/claude";

/** Fired by the Claude page after it creates, moves or deletes a session. */
export const CLAUDE_CHANGED_EVENT = "loom:claude-changed";

export function notifyClaudeChanged(): void {
  window.dispatchEvent(new Event(CLAUDE_CHANGED_EVENT));
}

const REFRESH_MS = 20000;

/**
 * Claude sessions and devices (workers) for the sidebar: projects are the
 * session folders, conversations are grouped by the device they run on.
 */
export function useClaudeNav(enabled: boolean) {
  const [sessions, setSessions] = useState<ClaudeSession[]>([]);
  const [workers, setWorkers] = useState<ClaudeWorker[]>([]);
  const [available, setAvailable] = useState(false);

  const refresh = useCallback(async () => {
    if (!enabled) return;
    try {
      const [cfg, list] = await Promise.all([getClaudeConfig(), listClaudeSessions()]);
      setAvailable(cfg.enabled);
      setWorkers(cfg.workers);
      setSessions(list.sessions);
    } catch {
      setAvailable(false);
    }
  }, [enabled]);

  useEffect(() => {
    if (!enabled) return;
    void refresh();
    const id = setInterval(() => {
      if (document.visibilityState === "visible") void refresh();
    }, REFRESH_MS);
    const on = () => void refresh();
    window.addEventListener(CLAUDE_CHANGED_EVENT, on);
    return () => {
      clearInterval(id);
      window.removeEventListener(CLAUDE_CHANGED_EVENT, on);
    };
  }, [enabled, refresh]);

  return { sessions, workers, available, refresh };
}
