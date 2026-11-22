#!/usr/bin/env python3
"""Loom Claude worker: runs Claude Code tasks queued by the Loom backend.

The host accepts no inbound connections. This process only connects out:
- AWS IoT Core (MQTT over WSS, IAM SigV4) to be woken for new tasks, approvals
  and cancels. IoT is only a wake-up signal; if it is down the worker still
  works by polling DynamoDB every POLL_SECONDS.
- DynamoDB: the task record (written by the backend) is authoritative. The
  worker claims a task with a conditional QUEUED->RUNNING update, and is the
  only writer of the task's numbered event stream.
- Amazon Bedrock, through Claude Code (Claude Agent SDK).

Every step becomes an event {seq, ts, type, ...} so a task keeps running when
nobody watches and the UI can replay it later.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError
from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient
from claude_agent_sdk.types import PermissionResultAllow, PermissionResultDeny

log = logging.getLogger("loom-claude-worker")

WORKER_ID = os.environ.get("WORKER_ID", "loom-worker-1")
REGION = os.environ.get("AWS_REGION", "us-east-1")
IOT_ENDPOINT = os.environ.get("IOT_ENDPOINT", "")
TASKS_TABLE = os.environ.get("TASKS_TABLE", "loom-claude-tasks")
EVENTS_TABLE = os.environ.get("EVENTS_TABLE", "loom-claude-events")
WORKERS_TABLE = os.environ.get("WORKERS_TABLE", "loom-claude-workers")
WORKDIR = os.path.realpath(os.path.expanduser(os.environ.get("WORKDIR", "~/work")))
MODEL = os.environ.get("ANTHROPIC_MODEL", "us.anthropic.claude-sonnet-4-6")
MAX_TURNS = int(os.environ.get("MAX_TURNS", "200"))
APPROVAL_TIMEOUT_S = int(os.environ.get("APPROVAL_TIMEOUT_SECONDS", "1800"))
EVENT_TTL_DAYS = int(os.environ.get("EVENT_TTL_DAYS", "365"))
POLL_SECONDS = 30
HEARTBEAT_SECONDS = 30

TOPIC_TASKS = f"loom/claude/tasks/{WORKER_ID}"
TOPIC_CONTROL = f"loom/claude/control/{WORKER_ID}"

ddb = boto3.resource("dynamodb", region_name=REGION)
tasks_t = ddb.Table(TASKS_TABLE)
events_t = ddb.Table(EVENTS_TABLE)
workers_t = ddb.Table(WORKERS_TABLE)

# ---------------------------------------------------------------------------
# Tool policy
# ---------------------------------------------------------------------------
READ_ONLY_TOOLS = {"Read", "Glob", "Grep", "LS", "TodoWrite", "NotebookRead"}
EDIT_TOOLS = {"Edit", "Write", "MultiEdit", "NotebookEdit"}
NETWORK_TOOLS = {"WebFetch", "WebSearch"}

SENSITIVE_PATH = re.compile(
    r"(/\.aws(/|$)|/\.ssh(/|$)|(^|/)\.env(\.|$)|id_rsa|id_ed25519|/\.netrc$|\.git-credentials|"
    r"/\.config/gh(/|$)|/\.docker/config\.json|/etc/shadow|/proc/\d+/environ)"
)
# Always refused, whatever the mode: credential reads and environment dumps.
BASH_HARD_DENY = re.compile(
    r"(\.aws/|\.ssh/|\.git-credentials|\.netrc|\bprintenv\b|^\s*env\s*$|/proc/\S*environ|"
    r"169\.254\.169\.254|\bimds\b)"
)
# Refused in unattended mode (no human to ask): network egress, publishing,
# privilege, and destructive patterns.
BASH_UNATTENDED_DENY = re.compile(
    r"(\b(curl|wget|nc|ncat|socat|ssh|scp|sftp|rsync|telnet|ftp)\b|git\s+push|\bsudo\b|\bsu\s|"
    r"\bdocker\b|\baws\s|rm\s+-rf\s+(/|~)(\s|$)|>\s*/dev/tcp|\bchmod\s+777\b|\bcrontab\b)"
)

REDACT = [
    (re.compile(r"\b(AKIA|ASIA)[A-Z0-9]{16}\b"), "[REDACTED_AWS_KEY_ID]"),
    (re.compile(r"(?i)(aws_secret_access_key|aws_session_token)\s*[=:]\s*\S+"), r"\1=[REDACTED]"),
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"), "[REDACTED_PRIVATE_KEY]"),
    (re.compile(r"(?i)\b(password|passwd|secret|token|api[_-]?key)\b(\s*[=:]\s*)(\S+)"), r"\1\2[REDACTED]"),
    (re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b"), "[REDACTED_GITHUB_TOKEN]"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"), "[REDACTED_API_KEY]"),
]


def redact(text: str) -> str:
    for pattern, repl in REDACT:
        text = pattern.sub(repl, text)
    return text


def _paths_in(tool_input: dict) -> list[str]:
    out = []
    for key in ("file_path", "path", "notebook_path"):
        value = tool_input.get(key)
        if isinstance(value, str) and value:
            out.append(os.path.realpath(os.path.join(WORKDIR, os.path.expanduser(value))))
    return out


def hard_deny_reason(tool: str, tool_input: dict) -> str | None:
    for p in _paths_in(tool_input):
        if SENSITIVE_PATH.search(p):
            return f"Access to {p} is blocked (credential/secret location)"
    if tool == "Bash" and BASH_HARD_DENY.search(str(tool_input.get("command", ""))):
        return "Command touches credentials or the environment; blocked"
    return None


def unattended_allowed(tool: str, tool_input: dict) -> tuple[bool, str]:
    if tool in EDIT_TOOLS:
        paths = _paths_in(tool_input)
        if paths and all(p == WORKDIR or p.startswith(WORKDIR + os.sep) for p in paths):
            return True, ""
        return False, f"Unattended edits are limited to {WORKDIR}"
    if tool == "Bash":
        cmd = str(tool_input.get("command", ""))
        if BASH_UNATTENDED_DENY.search(cmd):
            return False, "Command not allowed in unattended mode (network, publish, privilege or destructive)"
        return True, ""
    if tool in NETWORK_TOOLS:
        return False, "Web access is not allowed in unattended mode"
    return False, f"Tool {tool} is not on the unattended allowlist"


# ---------------------------------------------------------------------------
# DynamoDB helpers (sync; called through asyncio.to_thread)
# ---------------------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clip(value: Any, limit: int) -> Any:
    if isinstance(value, str):
        value = redact(value)
        return value if len(value) <= limit else value[:limit] + f"\n…[truncated {len(value) - limit} chars]"
    return value


def _to_ddb(value: Any) -> Any:
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: _to_ddb(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_to_ddb(v) for v in value]
    return value


def get_task(task_id: str) -> dict | None:
    return tasks_t.get_item(Key={"task_id": task_id}, ConsistentRead=True).get("Item")


def update_task(task_id: str, **fields: Any) -> None:
    names = {f"#{k}": k for k in fields}
    values = {f":{k}": _to_ddb(v) for k, v in fields.items()}
    tasks_t.update_item(
        Key={"task_id": task_id},
        UpdateExpression="SET " + ", ".join(f"#{k} = :{k}" for k in fields),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def recent_tasks(statuses: set[str]) -> list[dict]:
    resp = tasks_t.query(
        IndexName="by_created",
        KeyConditionExpression=Key("kind").eq("task"),
        FilterExpression=Attr("worker_id").eq(WORKER_ID) & Attr("status").is_in(sorted(statuses)),
        ScanIndexForward=False,
        Limit=200,
    )
    return sorted(resp.get("Items", []), key=lambda i: i.get("created_at", ""))


def claim(task_id: str) -> bool:
    try:
        tasks_t.update_item(
            Key={"task_id": task_id},
            UpdateExpression="SET #s = :r, started_at = :t",
            ConditionExpression="#s = :q AND worker_id = :w",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":r": "RUNNING", ":t": now_iso(), ":q": "QUEUED", ":w": WORKER_ID},
        )
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def heartbeat(current_task: str | None) -> None:
    workers_t.put_item(Item={
        "worker_id": WORKER_ID,
        "last_heartbeat": now_iso(),
        "last_heartbeat_epoch": Decimal(str(round(time.time(), 1))),
        "current_task": current_task or "",
        "model": MODEL,
    })


# ---------------------------------------------------------------------------
# Event stream for one task
# ---------------------------------------------------------------------------
class TaskStream:
    def __init__(self, task_id: str, start_seq: int) -> None:
        self.task_id = task_id
        self.seq = start_seq
        self.lock = asyncio.Lock()

    async def emit(self, type_: str, **fields: Any) -> None:
        async with self.lock:
            self.seq += 1
            item = {
                "task_id": self.task_id,
                "seq": self.seq,
                "ts": now_iso(),
                "type": type_,
                "expires_at": int(time.time()) + EVENT_TTL_DAYS * 86400,
                **{k: v for k, v in fields.items() if v is not None},
            }
            for key, limit in (("text", 32000), ("output", 16000), ("message", 4000)):
                if key in item:
                    item[key] = _clip(item[key], limit)
            if "input" in item:
                raw = json.dumps(item["input"], ensure_ascii=False, default=str)
                item["input"] = json.loads(raw) if len(raw) <= 8000 else _clip(raw, 8000)
                if isinstance(item["input"], dict):
                    item["input"] = {k: _clip(v, 4000) for k, v in item["input"].items()}
            seq = self.seq
            await asyncio.to_thread(events_t.put_item, Item=_to_ddb(item))
            await asyncio.to_thread(update_task, self.task_id, last_seq=seq)

    async def status(self, status: str, message: str | None = None, **task_fields: Any) -> None:
        await asyncio.to_thread(update_task, self.task_id, status=status, **task_fields)
        await self.emit("status", status=status, message=message)


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------
class Worker:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self.wake = asyncio.Event()
        self.control = asyncio.Event()
        self.current_task: str | None = None
        self.stopping = False
        self.mqtt = None

    # ---- IoT (wake-up channel) --------------------------------------------
    def _on_publish(self, data: Any) -> None:
        topic = getattr(data.publish_packet, "topic", "")
        assert self.loop is not None
        if topic == TOPIC_TASKS:
            self.loop.call_soon_threadsafe(self.wake.set)
        elif topic == TOPIC_CONTROL:
            self.loop.call_soon_threadsafe(self.control.set)

    def start_mqtt(self) -> None:
        if not IOT_ENDPOINT:
            log.warning("IOT_ENDPOINT not set; running in polling-only mode")
            return
        try:
            from awscrt import auth, mqtt5
            from awsiot import mqtt5_client_builder

            client = mqtt5_client_builder.websockets_with_default_aws_signing(
                endpoint=IOT_ENDPOINT,
                region=REGION,
                credentials_provider=auth.AwsCredentialsProvider.new_default_chain(),
                client_id=WORKER_ID,
                on_publish_received=self._on_publish,
            )
            client.start()
            client.subscribe(subscribe_packet=mqtt5.SubscribePacket(subscriptions=[
                mqtt5.Subscription(topic_filter=TOPIC_TASKS, qos=mqtt5.QoS.AT_LEAST_ONCE),
                mqtt5.Subscription(topic_filter=TOPIC_CONTROL, qos=mqtt5.QoS.AT_LEAST_ONCE),
            ])).result(30)
            self.mqtt = client
            log.info("Subscribed to %s and %s", TOPIC_TASKS, TOPIC_CONTROL)
        except Exception as e:  # noqa: BLE001 - polling keeps the worker functional
            log.warning("MQTT unavailable (%s); falling back to polling every %ss", e, POLL_SECONDS)

    async def _wait_control(self, seconds: float) -> None:
        self.control.clear()
        try:
            await asyncio.wait_for(self.control.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    # ---- lifecycle ------------------------------------------------------------
    async def heartbeat_loop(self) -> None:
        while not self.stopping:
            try:
                await asyncio.to_thread(heartbeat, self.current_task)
            except Exception as e:  # noqa: BLE001
                log.warning("heartbeat failed: %s", e)
            await asyncio.sleep(HEARTBEAT_SECONDS)

    async def recover_orphans(self) -> None:
        """Tasks left RUNNING by a previous process cannot be resumed mid-turn."""
        for item in await asyncio.to_thread(recent_tasks, {"RUNNING", "WAITING_APPROVAL"}):
            stream = TaskStream(item["task_id"], int(item.get("last_seq") or 0))
            await stream.emit("error", message="Worker restarted while this task was running")
            await stream.status("FAILED", "worker restarted", finished_at=now_iso(),
                                result_summary="Worker restarted; submit a follow-up to continue")

    async def run(self) -> None:
        self.loop = asyncio.get_running_loop()
        os.makedirs(WORKDIR, exist_ok=True)
        self.start_mqtt()
        await self.recover_orphans()
        hb = asyncio.create_task(self.heartbeat_loop())
        log.info("Worker %s ready (workdir=%s, model=%s)", WORKER_ID, WORKDIR, MODEL)
        try:
            while not self.stopping:
                queued = await asyncio.to_thread(recent_tasks, {"QUEUED"})
                ran = False
                for item in queued:
                    if await asyncio.to_thread(claim, item["task_id"]):
                        await self.run_task(item["task_id"])
                        ran = True
                        break
                if ran:
                    continue
                self.wake.clear()
                try:
                    await asyncio.wait_for(self.wake.wait(), timeout=POLL_SECONDS)
                except asyncio.TimeoutError:
                    pass
        finally:
            hb.cancel()
            if self.mqtt is not None:
                self.mqtt.stop()

    # ---- one task -------------------------------------------------------------
    async def run_task(self, task_id: str) -> None:
        task = await asyncio.to_thread(get_task, task_id)
        if not task:
            return
        self.current_task = task_id
        stream = TaskStream(task_id, int(task.get("last_seq") or 0))
        mode = task.get("mode", "interactive")
        cancelled = False
        await stream.status("RUNNING", f"started on {WORKER_ID} ({mode})")

        resume = None
        if task.get("parent_task_id"):
            parent = await asyncio.to_thread(get_task, task["parent_task_id"])
            resume = (parent or {}).get("claude_session_id") or None

        async def can_use_tool(tool: str, tool_input: dict, _ctx: Any):
            reason = hard_deny_reason(tool, tool_input)
            if reason:
                await stream.emit("error", message=f"Blocked {tool}: {reason}")
                return PermissionResultDeny(message=reason, interrupt=False)
            if tool in READ_ONLY_TOOLS:
                return PermissionResultAllow(updated_input=tool_input)
            if mode == "unattended":
                ok, why = unattended_allowed(tool, tool_input)
                if ok:
                    return PermissionResultAllow(updated_input=tool_input)
                await stream.emit("error", message=f"Denied {tool}: {why}")
                return PermissionResultDeny(message=why, interrupt=False)
            return await self.ask_human(stream, tool, tool_input)

        options = ClaudeAgentOptions(
            cwd=WORKDIR,
            model=MODEL,
            max_turns=MAX_TURNS,
            resume=resume,
            can_use_tool=can_use_tool,
        )

        result_text = ""
        is_error = False
        session_id = resume
        cost = None
        try:
            async with ClaudeSDKClient(options=options) as client:

                async def watch_cancel() -> None:
                    nonlocal cancelled
                    while True:
                        await self._wait_control(2)
                        t = await asyncio.to_thread(get_task, task_id)
                        if t and t.get("cancel_requested"):
                            cancelled = True
                            await stream.emit("status", status="RUNNING", message="cancel requested; stopping")
                            await client.interrupt()
                            return

                watcher = asyncio.create_task(watch_cancel())
                try:
                    await client.query(task["prompt"])
                    async for msg in client.receive_response():
                        kind = type(msg).__name__
                        if kind == "SystemMessage":
                            data = getattr(msg, "data", {}) or {}
                            if getattr(msg, "subtype", "") == "init" and data.get("session_id"):
                                session_id = data["session_id"]
                                await asyncio.to_thread(update_task, task_id, claude_session_id=session_id)
                        elif kind == "AssistantMessage":
                            for block in getattr(msg, "content", []) or []:
                                bkind = type(block).__name__
                                if bkind == "TextBlock" and getattr(block, "text", ""):
                                    await stream.emit("text", text=block.text)
                                elif bkind == "ToolUseBlock":
                                    await stream.emit("tool_use", tool=block.name, input=block.input)
                        elif kind == "UserMessage":
                            content = getattr(msg, "content", [])
                            for block in content if isinstance(content, list) else []:
                                if type(block).__name__ == "ToolResultBlock":
                                    out = block.content
                                    if isinstance(out, list):
                                        out = "\n".join(
                                            c.get("text", "") if isinstance(c, dict) else str(c) for c in out)
                                    await stream.emit("tool_result", output=str(out or ""),
                                                      is_error=bool(getattr(block, "is_error", False)))
                        elif kind == "ResultMessage":
                            result_text = getattr(msg, "result", "") or ""
                            is_error = bool(getattr(msg, "is_error", False))
                            session_id = getattr(msg, "session_id", None) or session_id
                            cost = getattr(msg, "total_cost_usd", None)
                finally:
                    watcher.cancel()
        except Exception as e:  # noqa: BLE001 - report every failure on the task
            log.exception("task %s failed", task_id)
            is_error = True
            result_text = f"{type(e).__name__}: {e}"
            await stream.emit("error", message=result_text)

        final = "CANCELLED" if cancelled else ("FAILED" if is_error else "SUCCEEDED")
        if result_text:
            await stream.emit("result", text=result_text)
        fields: dict[str, Any] = {"finished_at": now_iso(), "result_summary": _clip(result_text, 2000)}
        if session_id:
            fields["claude_session_id"] = session_id
        if cost is not None:
            fields["cost_usd"] = float(cost)
        await stream.status(final, None, **fields)
        self.current_task = None
        log.info("task %s finished: %s", task_id, final)

    async def ask_human(self, stream: TaskStream, tool: str, tool_input: dict):
        approval_id = uuid.uuid4().hex[:12]
        await stream.emit("approval_request", tool=tool, input=tool_input, approval_id=approval_id)
        await stream.status("WAITING_APPROVAL", f"waiting for approval of {tool}")
        deadline = time.monotonic() + APPROVAL_TIMEOUT_S
        decision, by = "deny", "timeout"
        while time.monotonic() < deadline:
            t = await asyncio.to_thread(get_task, stream.task_id)
            d = ((t or {}).get("decisions") or {}).get(approval_id)
            if d:
                decision, by = d.get("decision", "deny"), d.get("by", "")
                break
            if (t or {}).get("cancel_requested"):
                decision, by = "deny", "cancel"
                break
            await self._wait_control(2)
        await stream.emit("approval_decision", approval_id=approval_id, decision=decision, decided_by=by)
        await stream.status("RUNNING", None)
        if decision == "allow":
            return PermissionResultAllow(updated_input=tool_input)
        return PermissionResultDeny(message=f"Denied by {by}", interrupt=False)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    worker = Worker()

    def stop(*_: Any) -> None:
        worker.stopping = True
        worker.wake.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    asyncio.run(worker.run())


if __name__ == "__main__":
    main()
