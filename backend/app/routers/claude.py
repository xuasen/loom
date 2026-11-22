"""Claude sessions API (/api/claude/*).

A *session* is one ongoing Claude Code conversation on a private worker host,
like a terminal running ``claude``: the user sends messages, each one is a turn,
and context carries over. Workers only make outbound connections.

Data (table loom-claude-tasks, key ``task_id`` = session id, ``kind="session"``):
- ``inbox``: list of user messages appended by this router; ``consumed``: how
  many the worker has taken; ``pending_count``: inbox - consumed (for queries).
- ``status``: IDLE | RUNNING | WAITING_APPROVAL | CLOSED | FAILED.
- ``decisions`` / ``interrupt_requested`` / ``close_requested``: control state
  written here first, then pushed over IoT, so a worker that missed a push still
  sees it on its next poll.
Events (table loom-claude-events) are written only by the worker, numbered by
``seq`` per session, so the whole conversation can be replayed.

Trust model: callers need a valid Loom/Cognito JWT with ``admin:write``. The
DynamoDB record is authoritative; an IoT message only names a session. IAM
limits who may publish to worker topics.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.dependencies.auth import UserInfo, require_scopes

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/claude", tags=["claude"])

REQUIRED_SCOPE = "admin:write"
KIND = "session"
CLOSED_STATES = {"CLOSED", "FAILED"}
MAX_MESSAGE_CHARS = 20000
MAX_INBOX = 500
MAX_OPEN_SESSIONS_PER_USER = 10
WORKER_ONLINE_SECONDS = 90
STREAM_POLL_SECONDS = 0.5
STREAM_KEEPALIVE_SECONDS = 15
STREAM_MAX_SECONDS = 15 * 60


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _enabled() -> bool:
    return bool(_env("LOOM_CLAUDE_IOT_ENDPOINT") and _env("LOOM_CLAUDE_TASKS_TABLE"))


def _region() -> str:
    return _env("AWS_REGION", "us-east-1")


_ddb = None
_iot = None


def _table(env_name: str, default: str):
    global _ddb
    if _ddb is None:
        _ddb = boto3.resource("dynamodb", region_name=_region())
    return _ddb.Table(_env(env_name, default))


def _sessions():
    return _table("LOOM_CLAUDE_TASKS_TABLE", "loom-claude-tasks")


def _events():
    return _table("LOOM_CLAUDE_EVENTS_TABLE", "loom-claude-events")


def _workers():
    return _table("LOOM_CLAUDE_WORKERS_TABLE", "loom-claude-workers")


def _publish(topic: str, payload: dict) -> None:
    global _iot
    if _iot is None:
        _iot = boto3.client(
            "iot-data", region_name=_region(), endpoint_url=f"https://{_env('LOOM_CLAUDE_IOT_ENDPOINT')}",
        )
    _iot.publish(topic=topic, qos=1, payload=json.dumps(payload).encode())


def _notify(worker_id: str, channel: Literal["tasks", "control"], payload: dict) -> None:
    try:
        _publish(f"loom/claude/{channel}/{worker_id}", payload)
    except Exception as e:  # noqa: BLE001 - the worker also polls DynamoDB
        logger.warning("IoT publish to %s/%s failed (worker poll will catch up): %s", channel, worker_id, e)


def _plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_plain(v) for v in value]
    return value


def _public(item: dict) -> dict:
    inbox = item.get("inbox") or []
    first = inbox[0].get("text", "") if inbox and isinstance(inbox[0], dict) else ""
    return {
        "session_id": item.get("task_id"),
        "title": item.get("title") or first[:120],
        "status": item.get("status"),
        "mode": item.get("mode"),
        "submitted_by": item.get("submitted_by"),
        "worker_id": item.get("worker_id"),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
        "last_seq": _plain(item.get("last_seq") or 0),
        "pending_count": _plain(item.get("pending_count") or 0),
        "turns": _plain(item.get("consumed") or 0),
        "cost_usd": _plain(item.get("cost_usd")) if item.get("cost_usd") is not None else None,
    }


def _require_enabled() -> None:
    if not _enabled():
        raise HTTPException(status_code=404, detail="Claude sessions are not configured")


def _get_item(session_id: str) -> dict:
    try:
        item = _sessions().get_item(Key={"task_id": session_id}, ConsistentRead=True).get("Item")
    except ClientError as e:
        logger.error("get_item %s failed: %s", session_id, e)
        raise HTTPException(status_code=502, detail="Session store unavailable") from e
    if not item or item.get("kind") != KIND:
        raise HTTPException(status_code=404, detail="Session not found")
    return item


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _message(text: str, user: UserInfo) -> dict:
    return {"id": uuid.uuid4().hex[:12], "text": text, "by": user.username, "at": _now_iso()}


def _conditional_failed(e: ClientError) -> bool:
    return e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/config")
def get_config(user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE))) -> dict:
    _require_enabled()
    now = time.time()
    try:
        items = _workers().scan(Limit=50).get("Items", [])
    except ClientError as e:
        logger.warning("workers scan failed: %s", e)
        items = []
    workers = [{
        "worker_id": w.get("worker_id"),
        "online": now - float(w.get("last_heartbeat_epoch") or 0) < WORKER_ONLINE_SECONDS,
        "last_heartbeat": w.get("last_heartbeat"),
        "model": w.get("model"),
    } for w in items]
    default_worker = _env("LOOM_CLAUDE_DEFAULT_WORKER", "loom-worker-1")
    if not any(w["worker_id"] == default_worker for w in workers):
        workers.append({"worker_id": default_worker, "online": False, "last_heartbeat": None, "model": None})
    return {"enabled": True, "workers": workers}


@router.get("/sessions")
def list_sessions(user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE))) -> dict:
    _require_enabled()
    resp = _sessions().query(
        IndexName="by_created",
        KeyConditionExpression=Key("kind").eq(KIND),
        ScanIndexForward=False,
        Limit=100,
    )
    return {"sessions": [_public(i) for i in resp.get("Items", [])]}


class CreateSessionRequest(BaseModel):
    message: str = Field(min_length=1, max_length=MAX_MESSAGE_CHARS)
    mode: Literal["interactive", "unattended"] = "interactive"
    worker_id: str | None = Field(default=None, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")


@router.post("/sessions", status_code=201)
def create_session(req: CreateSessionRequest, user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE))) -> dict:
    _require_enabled()
    worker_id = req.worker_id or _env("LOOM_CLAUDE_DEFAULT_WORKER", "loom-worker-1")

    open_sessions = _sessions().query(
        IndexName="by_created",
        KeyConditionExpression=Key("kind").eq(KIND),
        FilterExpression=Attr("submitted_by").eq(user.username) & Attr("status").is_in(
            ["IDLE", "RUNNING", "WAITING_APPROVAL"]),
        ScanIndexForward=False,
        Limit=200,
    ).get("Items", [])
    if len(open_sessions) >= MAX_OPEN_SESSIONS_PER_USER:
        raise HTTPException(status_code=429,
                            detail=f"At most {MAX_OPEN_SESSIONS_PER_USER} open sessions per user; end one first")

    session_id = uuid.uuid4().hex[:16]
    now = _now_iso()
    item = {
        "task_id": session_id,
        "kind": KIND,
        "title": req.message[:120],
        "mode": req.mode,
        "status": "IDLE",
        "submitted_by": user.username,
        "worker_id": worker_id,
        "created_at": now,
        "updated_at": now,
        "inbox": [_message(req.message, user)],
        "consumed": 0,
        "pending_count": 1,
        "last_seq": 0,
        "decisions": {},
        "interrupt_requested": False,
        "close_requested": False,
    }
    _sessions().put_item(Item=item, ConditionExpression="attribute_not_exists(task_id)")
    logger.info("Claude session %s opened by %s on %s (mode=%s)", session_id, user.username, worker_id, req.mode)
    _notify(worker_id, "tasks", {"session_id": session_id})
    return _public(item)


@router.get("/sessions/{session_id}")
def get_session(session_id: str, user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE))) -> dict:
    _require_enabled()
    return _public(_get_item(session_id))


class SendMessageRequest(BaseModel):
    message: str = Field(min_length=1, max_length=MAX_MESSAGE_CHARS)


@router.post("/sessions/{session_id}/messages", status_code=202)
def send_message(
    session_id: str,
    req: SendMessageRequest,
    user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE)),
) -> dict:
    """Queue the next user turn. Sending while a turn runs queues it (it runs next)."""
    _require_enabled()
    item = _get_item(session_id)
    try:
        resp = _sessions().update_item(
            Key={"task_id": session_id},
            UpdateExpression="SET inbox = list_append(inbox, :m), updated_at = :t ADD pending_count :one",
            ConditionExpression="NOT (#s IN (:closed, :failed)) AND close_requested = :f AND size(inbox) < :max",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":m": [_message(req.message, user)], ":t": _now_iso(), ":one": 1,
                ":closed": "CLOSED", ":failed": "FAILED", ":f": False, ":max": MAX_INBOX,
            },
            ReturnValues="ALL_NEW",
        )
    except ClientError as e:
        if _conditional_failed(e):
            raise HTTPException(status_code=409, detail="Session is closed or full") from e
        raise
    _notify(item["worker_id"], "tasks", {"session_id": session_id})
    return _public(resp["Attributes"])


def _query_events(session_id: str, after: int, limit: int = 1000) -> list[dict]:
    resp = _events().query(
        KeyConditionExpression=Key("task_id").eq(session_id) & Key("seq").gt(after),
        ScanIndexForward=True,
        Limit=limit,
    )
    out = []
    for item in resp.get("Items", []):
        ev = _plain(item)
        ev.pop("task_id", None)
        ev.pop("expires_at", None)
        out.append(ev)
    return out


@router.get("/sessions/{session_id}/events")
def get_events(
    session_id: str,
    after: int = Query(0, ge=0),
    user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE)),
) -> dict:
    _require_enabled()
    item = _get_item(session_id)
    return {"events": _query_events(session_id, after), "status": item.get("status"),
            "pending_count": _plain(item.get("pending_count") or 0)}


@router.get("/sessions/{session_id}/stream")
async def stream_events(
    session_id: str,
    after: int = Query(0, ge=0),
    user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE)),
) -> StreamingResponse:
    """SSE: events after ``after``, plus ``status`` frames when the session state changes.

    Ends after STREAM_MAX_SECONDS (clients reconnect from their last seq) or
    once the session is closed and fully delivered.
    """
    _require_enabled()
    await asyncio.to_thread(_get_item, session_id)

    async def gen():
        last = after
        last_status = None
        started = time.monotonic()
        last_send = started
        while time.monotonic() - started < STREAM_MAX_SECONDS:
            events = await asyncio.to_thread(_query_events, session_id, last)
            for ev in events:
                last = max(last, int(ev.get("seq", last)))
                yield f"id: {last}\ndata: {json.dumps(ev, ensure_ascii=False)}\n\n"
                last_send = time.monotonic()
            if not events:
                item = await asyncio.to_thread(_get_item, session_id)
                status = item.get("status")
                pending = _plain(item.get("pending_count") or 0)
                if (status, pending) != last_status:
                    last_status = (status, pending)
                    frame = {"type": "session", "status": status, "pending_count": pending}
                    yield f"event: session\ndata: {json.dumps(frame)}\n\n"
                    last_send = time.monotonic()
                if status in CLOSED_STATES and int(_plain(item.get("last_seq") or 0)) <= last:
                    return
                if time.monotonic() - last_send > STREAM_KEEPALIVE_SECONDS:
                    yield ": keepalive\n\n"
                    last_send = time.monotonic()
                await asyncio.sleep(STREAM_POLL_SECONDS)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no"},
    )


class DecisionRequest(BaseModel):
    decision: Literal["allow", "deny"]


@router.post("/sessions/{session_id}/approvals/{approval_id}", status_code=204)
def decide(
    session_id: str,
    approval_id: str,
    req: DecisionRequest,
    user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE)),
) -> None:
    _require_enabled()
    if not approval_id.isalnum() or len(approval_id) > 64:
        raise HTTPException(status_code=400, detail="Bad approval id")
    item = _get_item(session_id)
    try:
        _sessions().update_item(
            Key={"task_id": session_id},
            UpdateExpression="SET decisions.#a = :d",
            ConditionExpression="#s = :waiting AND attribute_not_exists(decisions.#a)",
            ExpressionAttributeNames={"#a": approval_id, "#s": "status"},
            ExpressionAttributeValues={
                ":d": {"decision": req.decision, "by": user.username, "at": _now_iso()},
                ":waiting": "WAITING_APPROVAL",
            },
        )
    except ClientError as e:
        if _conditional_failed(e):
            raise HTTPException(status_code=409, detail="Session is not waiting for this approval") from e
        raise
    logger.info("Claude session %s approval %s -> %s by %s", session_id, approval_id, req.decision, user.username)
    _notify(item["worker_id"], "control", {"type": "approval", "session_id": session_id})


@router.post("/sessions/{session_id}/interrupt", status_code=204)
def interrupt(session_id: str, user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE))) -> None:
    """Stop the current turn (like Esc in the terminal). The session stays open."""
    _require_enabled()
    item = _get_item(session_id)
    if item.get("status") not in ("RUNNING", "WAITING_APPROVAL"):
        return
    _sessions().update_item(
        Key={"task_id": session_id},
        UpdateExpression="SET interrupt_requested = :y, interrupt_by = :u",
        ExpressionAttributeValues={":y": True, ":u": user.username},
    )
    logger.info("Claude session %s interrupt requested by %s", session_id, user.username)
    _notify(item["worker_id"], "control", {"type": "interrupt", "session_id": session_id})


@router.post("/sessions/{session_id}/close", status_code=204)
def close(session_id: str, user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE))) -> None:
    """End the session; the worker releases its Claude Code process."""
    _require_enabled()
    item = _get_item(session_id)
    if item.get("status") in CLOSED_STATES:
        return
    try:
        _sessions().update_item(
            Key={"task_id": session_id},
            UpdateExpression="SET #s = :c, close_requested = :y, updated_at = :t",
            ConditionExpression="#s = :idle AND pending_count = :zero",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":c": "CLOSED", ":y": True, ":t": _now_iso(), ":idle": "IDLE", ":zero": 0},
        )
    except ClientError as e:
        if not _conditional_failed(e):
            raise
        # A turn is running or queued: ask the worker to stop and close.
        _sessions().update_item(
            Key={"task_id": session_id},
            UpdateExpression="SET close_requested = :y, interrupt_requested = :y",
            ExpressionAttributeValues={":y": True},
        )
    logger.info("Claude session %s closed by %s", session_id, user.username)
    _notify(item["worker_id"], "control", {"type": "close", "session_id": session_id})
