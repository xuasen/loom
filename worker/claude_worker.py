#!/usr/bin/env python3
"""Loom Claude worker: hosts Claude Code *sessions* for the Loom UI.

A session is one ongoing Claude Code conversation, like a terminal running
``claude``: each user message is a turn and context carries over. The worker
keeps a live ClaudeSDKClient per active session (released after
IDLE_RELEASE_SECONDS or when the session closes); the next message re-opens it
with ``resume=<claude session id>``, so a conversation survives worker restarts.

The host accepts no inbound connections. This process only connects out:
- AWS IoT Core (MQTT over WSS, IAM SigV4) to be woken for new messages,
  approvals, interrupts and closes. IoT is only a wake-up signal; DynamoDB is
  polled every POLL_SECONDS as a fallback.
- DynamoDB: the session record (written by the backend) is authoritative. The
  worker claims a turn with a conditional IDLE->RUNNING update and is the only
  writer of the session's numbered event stream.
- Amazon Bedrock, through Claude Code (Claude Agent SDK).
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
IDLE_RELEASE_SECONDS = int(os.environ.get("IDLE_RELEASE_SECONDS", "1800"))
MAX_LIVE_SESSIONS = int(os.environ.get("MAX_LIVE_SESSIONS", "4"))
POLL_SECONDS = 15
HEARTBEAT_SECONDS = 30
FLUSH_SECONDS = 0.3
FLUSH_CHARS = 400
KIND = "session"

TOPIC_TASKS = f"loom/claude/tasks/{WORKER_ID}"
TOPIC_CONTROL = f"loom/claude/control/{WORKER_ID}"

ddb = boto3.resource("dynamodb", region_name=REGION)
sessions_t = ddb.Table(TASKS_TABLE)
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
# Always refused: credential reads, environment dumps, instance metadata.
BASH_HARD_DENY = re.compile(
    r"(\.aws/|\.ssh/|\.git-credentials|\.netrc|\bprintenv\b|^\s*env\s*$|/proc/\S*environ|"
    r"169\.254\.169\.254|\bimds\b)"
)
# Refused in unattended mode (no human to ask).
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
        if BASH_UNATTENDED_DENY.search(str(tool_input.get("command", ""))):
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


def get_session(sid: str) -> dict | None:
    item = sessions_t.get_item(Key={"task_id": sid}, ConsistentRead=True).get("Item")
    return item if item and item.get("kind") == KIND else None


def set_fields(sid: str, **fields: Any) -> None:
    sessions_t.update_item(
        Key={"task_id": sid},
        UpdateExpression="SET " + ", ".join(f"#{k} = :{k}" for k in fields),
        ExpressionAttributeNames={f"#{k}": k for k in fields},
        ExpressionAttributeValues={f":{k}": _to_ddb(v) for k, v in fields.items()},
    )


def my_sessions(statuses: set[str], pending_only: bool = False) -> list[dict]:
    flt = Attr("worker_id").eq(WORKER_ID) & Attr("status").is_in(sorted(statuses))
    if pending_only:
        flt = flt & Attr("pending_count").gt(0)
    resp = sessions_t.query(
        IndexName="by_created",
        KeyConditionExpression=Key("kind").eq(KIND),
        FilterExpression=flt,
        ScanIndexForward=False,
        Limit=300,
    )
    return sorted(resp.get("Items", []), key=lambda i: i.get("updated_at", ""))


def claim_turn(sid: str) -> bool:
    # Every attribute name goes through a placeholder: several of ours
    # (status, consumed, ...) are DynamoDB reserved words.
    try:
        sessions_t.update_item(
            Key={"task_id": sid},
            UpdateExpression="SET #s = :r, #ir = :f",
            ConditionExpression="#s = :idle AND #pc > :zero AND #w = :w AND #cr = :f",
            ExpressionAttributeNames={"#s": "status", "#ir": "interrupt_requested", "#pc": "pending_count",
                                      "#w": "worker_id", "#cr": "close_requested"},
            ExpressionAttributeValues={":r": "RUNNING", ":idle": "IDLE", ":zero": 0, ":w": WORKER_ID, ":f": False},
        )
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def finish_turn(sid: str, status: str, cost: float | None) -> None:
    names = {"#s": "status", "#u": "updated_at", "#ir": "interrupt_requested", "#c": "consumed",
             "#pc": "pending_count"}
    values: dict[str, Any] = {":s": status, ":t": now_iso(), ":f": False, ":one": 1, ":neg": -1}
    sets = "#s = :s, #u = :t, #ir = :f, #c = #c + :one"
    if cost is not None:
        names["#cost"] = "cost_usd"
        sets += ", #cost = if_not_exists(#cost, :zero) + :cv"
        values[":cv"] = Decimal(str(cost))
        values[":zero"] = Decimal("0")
    sessions_t.update_item(
        Key={"task_id": sid}, UpdateExpression=f"SET {sets} ADD #pc :neg",
        ExpressionAttributeNames=names, ExpressionAttributeValues=values,
    )


def heartbeat(active: list[str]) -> None:
    workers_t.put_item(Item={
        "worker_id": WORKER_ID,
        "last_heartbeat": now_iso(),
        "last_heartbeat_epoch": Decimal(str(round(time.time(), 1))),
        "live_sessions": active,
        "model": MODEL,
    })


# ---------------------------------------------------------------------------
# Event stream for one session
# ---------------------------------------------------------------------------
class EventStream:
    def __init__(self, sid: str, start_seq: int) -> None:
        self.sid = sid
        self.seq = start_seq
        self.lock = asyncio.Lock()

    async def emit(self, type_: str, **fields: Any) -> None:
        async with self.lock:
            self.seq += 1
            item = {
                "task_id": self.sid, "seq": self.seq, "ts": now_iso(), "type": type_,
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
            await asyncio.to_thread(set_fields, self.sid, last_seq=seq)

    async def status(self, status: str, message: str | None = None) -> None:
        await asyncio.to_thread(set_fields, self.sid, status=status, updated_at=now_iso())
        await self.emit("status", status=status, message=message)


# ---------------------------------------------------------------------------
# One live Claude Code conversation
# ---------------------------------------------------------------------------
class LiveSession:
    def __init__(self, worker: "Worker", item: dict) -> None:
        self.worker = worker
        self.sid: str = item["task_id"]
        self.mode: str = item.get("mode", "interactive")
        self.claude_session_id: str | None = item.get("claude_session_id") or None
        self.stream = EventStream(self.sid, int(item.get("last_seq") or 0))
        self.client: ClaudeSDKClient | None = None
        self.last_used = time.monotonic()

    async def _can_use_tool(self, tool: str, tool_input: dict, _ctx: Any):
        reason = hard_deny_reason(tool, tool_input)
        if reason:
            await self.stream.emit("error", message=f"Blocked {tool}: {reason}")
            return PermissionResultDeny(message=reason, interrupt=False)
        if tool in READ_ONLY_TOOLS:
            return PermissionResultAllow(updated_input=tool_input)
        if self.mode == "unattended":
            ok, why = unattended_allowed(tool, tool_input)
            if ok:
                return PermissionResultAllow(updated_input=tool_input)
            await self.stream.emit("error", message=f"Denied {tool}: {why}")
            return PermissionResultDeny(message=why, interrupt=False)
        return await self._ask_human(tool, tool_input)

    async def _ask_human(self, tool: str, tool_input: dict):
        approval_id = uuid.uuid4().hex[:12]
        await self.stream.emit("approval_request", tool=tool, input=tool_input, approval_id=approval_id)
        await self.stream.status("WAITING_APPROVAL", f"waiting for approval of {tool}")
        deadline = time.monotonic() + APPROVAL_TIMEOUT_S
        decision, by = "deny", "timeout"
        while time.monotonic() < deadline:
            item = await asyncio.to_thread(get_session, self.sid) or {}
            d = (item.get("decisions") or {}).get(approval_id)
            if d:
                decision, by = d.get("decision", "deny"), d.get("by", "")
                break
            if item.get("interrupt_requested") or item.get("close_requested"):
                decision, by = "deny", "interrupt"
                break
            await self.worker.wait_control(2)
        await self.stream.emit("approval_decision", approval_id=approval_id, decision=decision, decided_by=by)
        await self.stream.status("RUNNING", None)
        if decision == "allow":
            return PermissionResultAllow(updated_input=tool_input)
        return PermissionResultDeny(message=f"Denied by {by}", interrupt=False)

    async def ensure_client(self) -> ClaudeSDKClient:
        if self.client is None:
            options = ClaudeAgentOptions(
                cwd=WORKDIR,
                model=MODEL,
                max_turns=MAX_TURNS,
                resume=self.claude_session_id,
                can_use_tool=self._can_use_tool,
                include_partial_messages=True,
            )
            client = ClaudeSDKClient(options=options)
            await client.connect()
            self.client = client
            log.info("session %s: Claude Code %s", self.sid, "resumed" if self.claude_session_id else "started")
        return self.client

    async def release(self) -> None:
        if self.client is not None:
            try:
                await self.client.disconnect()
            except Exception as e:  # noqa: BLE001
                log.warning("session %s: disconnect failed: %s", self.sid, e)
            self.client = None
            log.info("session %s: Claude Code released", self.sid)

    async def run_turn(self, message: dict) -> tuple[str, float | None]:
        """Run one user turn. Returns (status after the turn, cost)."""
        self.last_used = time.monotonic()
        await self.stream.emit("user_message", text=message.get("text", ""), by=message.get("by"))
        await self.stream.status("RUNNING", None)

        interrupted = False
        cost: float | None = None
        buf: list[str] = []
        last_flush = time.monotonic()
        streamed_text = False

        async def flush() -> None:
            nonlocal last_flush
            if buf:
                text = "".join(buf)
                buf.clear()
                await self.stream.emit("text", text=text, partial=True)
            last_flush = time.monotonic()

        try:
            client = await self.ensure_client()

            async def watch_interrupt() -> None:
                nonlocal interrupted
                while True:
                    await self.worker.wait_control(1)
                    item = await asyncio.to_thread(get_session, self.sid) or {}
                    if item.get("interrupt_requested") or item.get("close_requested"):
                        interrupted = True
                        await self.stream.emit("status", status="RUNNING", message="interrupting…")
                        await client.interrupt()
                        return

            watcher = asyncio.create_task(watch_interrupt())
            try:
                await client.query(message.get("text", ""))
                async for msg in client.receive_response():
                    kind = type(msg).__name__
                    if kind == "StreamEvent":
                        ev = getattr(msg, "event", {}) or {}
                        delta = ev.get("delta") or {}
                        if ev.get("type") == "content_block_delta" and delta.get("type") == "text_delta":
                            buf.append(delta.get("text", ""))
                            streamed_text = True
                            if time.monotonic() - last_flush >= FLUSH_SECONDS or sum(map(len, buf)) >= FLUSH_CHARS:
                                await flush()
                        elif ev.get("type") in ("content_block_stop", "message_stop"):
                            await flush()
                    elif kind == "SystemMessage":
                        data = getattr(msg, "data", {}) or {}
                        if getattr(msg, "subtype", "") == "init" and data.get("session_id"):
                            if data["session_id"] != self.claude_session_id:
                                self.claude_session_id = data["session_id"]
                                await asyncio.to_thread(set_fields, self.sid,
                                                        claude_session_id=self.claude_session_id)
                    elif kind == "AssistantMessage":
                        await flush()
                        for block in getattr(msg, "content", []) or []:
                            bkind = type(block).__name__
                            if bkind == "TextBlock" and not streamed_text and getattr(block, "text", ""):
                                await self.stream.emit("text", text=block.text)
                            elif bkind == "ToolUseBlock":
                                await self.stream.emit("tool_use", tool=block.name, input=block.input)
                        streamed_text = False
                    elif kind == "UserMessage":
                        content = getattr(msg, "content", [])
                        for block in content if isinstance(content, list) else []:
                            if type(block).__name__ == "ToolResultBlock":
                                out = block.content
                                if isinstance(out, list):
                                    out = "\n".join(c.get("text", "") if isinstance(c, dict) else str(c) for c in out)
                                await self.stream.emit("tool_result", output=str(out or ""),
                                                       is_error=bool(getattr(block, "is_error", False)))
                    elif kind == "ResultMessage":
                        await flush()
                        cost = getattr(msg, "total_cost_usd", None)
                        sid = getattr(msg, "session_id", None)
                        if sid and sid != self.claude_session_id:
                            self.claude_session_id = sid
                            await asyncio.to_thread(set_fields, self.sid, claude_session_id=sid)
                        await self.stream.emit(
                            "turn_end",
                            is_error=bool(getattr(msg, "is_error", False)),
                            message="interrupted" if interrupted else getattr(msg, "subtype", None),
                            cost_usd=float(cost) if cost is not None else None,
                            num_turns=getattr(msg, "num_turns", None),
                        )
            finally:
                watcher.cancel()
                await flush()
        except Exception as e:  # noqa: BLE001 - report and drop the client; next turn resumes
            log.exception("session %s: turn failed", self.sid)
            await self.stream.emit("error", message=f"{type(e).__name__}: {e}")
            await self.release()

        self.last_used = time.monotonic()
        item = await asyncio.to_thread(get_session, self.sid) or {}
        return ("CLOSED" if item.get("close_requested") else "IDLE"), cost


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------
class Worker:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self.wake = asyncio.Event()
        self.control = asyncio.Event()
        self.live: dict[str, LiveSession] = {}
        self.stopping = False
        self.mqtt = None

    # ---- IoT wake-ups -------------------------------------------------------
    def _on_publish(self, data: Any) -> None:
        topic = getattr(data.publish_packet, "topic", "")
        assert self.loop is not None
        if topic == TOPIC_TASKS:
            self.loop.call_soon_threadsafe(self.wake.set)
        elif topic == TOPIC_CONTROL:
            self.loop.call_soon_threadsafe(self.control.set)
            self.loop.call_soon_threadsafe(self.wake.set)

    def start_mqtt(self) -> None:
        if not IOT_ENDPOINT:
            log.warning("IOT_ENDPOINT not set; polling-only mode")
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
        except Exception as e:  # noqa: BLE001
            log.warning("MQTT unavailable (%s); polling every %ss", e, POLL_SECONDS)

    async def wait_control(self, seconds: float) -> None:
        self.control.clear()
        try:
            await asyncio.wait_for(self.control.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    # ---- housekeeping ---------------------------------------------------------
    async def heartbeat_loop(self) -> None:
        while not self.stopping:
            try:
                await asyncio.to_thread(heartbeat, list(self.live))
            except Exception as e:  # noqa: BLE001
                log.warning("heartbeat failed: %s", e)
            await asyncio.sleep(HEARTBEAT_SECONDS)

    async def recover_orphans(self) -> None:
        """A turn cut off by a restart is reported and skipped (not re-run)."""
        for item in await asyncio.to_thread(my_sessions, {"RUNNING", "WAITING_APPROVAL"}):
            sid = item["task_id"]
            try:
                stream = EventStream(sid, int(item.get("last_seq") or 0))
                await stream.emit("error", message="Worker restarted during this turn; it was stopped. "
                                                   "Send a message to continue the conversation.")
                await asyncio.to_thread(finish_turn, sid, "IDLE", None)
                await stream.emit("status", status="IDLE", message=None)
            except Exception:  # noqa: BLE001
                log.exception("session %s: orphan recovery failed", sid)
                await self.fail_session(sid)

    async def release_idle(self) -> None:
        now = time.monotonic()
        closed = {i["task_id"] for i in await asyncio.to_thread(my_sessions, {"CLOSED", "FAILED"})}
        for sid, live in list(self.live.items()):
            if sid in closed or now - live.last_used > IDLE_RELEASE_SECONDS:
                await live.release()
                del self.live[sid]
        while len(self.live) > MAX_LIVE_SESSIONS:
            sid, live = min(self.live.items(), key=lambda kv: kv[1].last_used)
            await live.release()
            del self.live[sid]

    async def close_requested_idle(self) -> None:
        for item in await asyncio.to_thread(my_sessions, {"IDLE"}):
            if item.get("close_requested"):
                await asyncio.to_thread(set_fields, item["task_id"], status="CLOSED", updated_at=now_iso())

    # ---- main loop ------------------------------------------------------------
    async def run(self) -> None:
        self.loop = asyncio.get_running_loop()
        os.makedirs(WORKDIR, exist_ok=True)
        self.start_mqtt()
        await self.recover_orphans()
        hb = asyncio.create_task(self.heartbeat_loop())
        log.info("Worker %s ready (workdir=%s, model=%s)", WORKER_ID, WORKDIR, MODEL)
        try:
            while not self.stopping:
                ran = False
                try:
                    for item in await asyncio.to_thread(my_sessions, {"IDLE"}, True):
                        if await asyncio.to_thread(claim_turn, item["task_id"]):
                            ran = True
                            try:
                                await self.run_one_turn(item["task_id"])
                            except Exception:  # noqa: BLE001 - one bad session must not kill the worker
                                log.exception("session %s: turn bookkeeping failed", item["task_id"])
                                await self.fail_session(item["task_id"])
                            break
                    await self.close_requested_idle()
                    await self.release_idle()
                except Exception:  # noqa: BLE001 - transient AWS errors: back off and retry
                    log.exception("worker loop error; retrying in %ss", POLL_SECONDS)
                    await asyncio.sleep(POLL_SECONDS)
                    continue
                if ran:
                    continue
                self.wake.clear()
                try:
                    await asyncio.wait_for(self.wake.wait(), timeout=POLL_SECONDS)
                except asyncio.TimeoutError:
                    pass
        finally:
            hb.cancel()
            for live in self.live.values():
                await live.release()
            if self.mqtt is not None:
                self.mqtt.stop()

    async def fail_session(self, sid: str) -> None:
        """Last resort: mark the session FAILED so it does not loop forever."""
        live = self.live.pop(sid, None)
        if live is not None:
            await live.release()
        try:
            await asyncio.to_thread(set_fields, sid, status="FAILED", pending_count=0, updated_at=now_iso())
        except Exception:  # noqa: BLE001
            log.exception("session %s: could not mark FAILED", sid)

    async def run_one_turn(self, sid: str) -> None:
        item = await asyncio.to_thread(get_session, sid)
        if not item:
            return
        live = self.live.get(sid)
        if live is None:
            live = LiveSession(self, item)
            self.live[sid] = live
        inbox = item.get("inbox") or []
        consumed = int(item.get("consumed") or 0)
        if consumed >= len(inbox):
            await asyncio.to_thread(set_fields, sid, status="IDLE", pending_count=0)
            return
        status, cost = await live.run_turn(inbox[consumed])
        await asyncio.to_thread(finish_turn, sid, status, cost)
        await live.stream.emit("status", status=status, message=None)
        if status == "CLOSED":
            await live.release()
            self.live.pop(sid, None)


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
