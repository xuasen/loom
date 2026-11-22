"""Claude task runner API (/api/claude/*).

Tasks run on private worker hosts that only make outbound connections. This
router is the only writer of task records and the only publisher of task and
control notifications; workers are the only writers of events.

Trust model (no shared secrets minted by Loom):
- Callers authenticate with the Loom/Cognito JWT and need ``admin:write``.
  External hooks use a Cognito machine-to-machine token for the same routes.
- The DynamoDB task record written here is authoritative. An IoT message only
  tells a worker *which* task to look at; the worker reads the prompt, mode and
  parent from DynamoDB and claims the task with a conditional QUEUED->RUNNING
  update, so a forged or replayed message cannot inject a prompt or run a task
  twice. IAM limits who may publish to the worker topics at all.
- Approvals and cancels are recorded on the task item first, then pushed, so a
  worker that missed the push still sees them on its next poll.

Topics: loom/claude/tasks/<worker_id>  (payload {"task_id"})
        loom/claude/control/<worker_id> (payload {"type","task_id",...})
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
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}
MAX_PROMPT_CHARS = 20000
MAX_OPEN_TASKS_PER_USER = 5
WORKER_ONLINE_SECONDS = 90
STREAM_POLL_SECONDS = 0.7
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


def _tasks():
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


def _plain(value: Any) -> Any:
    """DynamoDB Decimals/sets -> JSON-friendly values."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_plain(v) for v in value]
    return value


_PUBLIC_TASK_FIELDS = (
    "task_id", "prompt", "status", "mode", "submitted_by", "worker_id", "created_at",
    "started_at", "finished_at", "last_seq", "result_summary", "parent_task_id",
)


def _public_task(item: dict) -> dict:
    out = {k: _plain(item.get(k)) for k in _PUBLIC_TASK_FIELDS}
    out["last_seq"] = out.get("last_seq") or 0
    return out


def _require_enabled() -> None:
    if not _enabled():
        raise HTTPException(status_code=404, detail="Claude task runner is not configured")


def _get_task_item(task_id: str) -> dict:
    try:
        item = _tasks().get_item(Key={"task_id": task_id}, ConsistentRead=True).get("Item")
    except ClientError as e:
        logger.error("get_item %s failed: %s", task_id, e)
        raise HTTPException(status_code=502, detail="Task store unavailable") from e
    if not item or item.get("kind") != "task":
        raise HTTPException(status_code=404, detail="Task not found")
    return item


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    workers = []
    for w in items:
        hb = float(w.get("last_heartbeat_epoch") or 0)
        workers.append({
            "worker_id": w.get("worker_id"),
            "online": now - hb < WORKER_ONLINE_SECONDS,
            "last_heartbeat": w.get("last_heartbeat"),
        })
    default_worker = _env("LOOM_CLAUDE_DEFAULT_WORKER", "loom-worker-1")
    if not any(w["worker_id"] == default_worker for w in workers):
        workers.append({"worker_id": default_worker, "online": False, "last_heartbeat": None})
    return {"enabled": True, "workers": workers}


@router.get("/tasks")
def list_tasks(user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE))) -> dict:
    _require_enabled()
    resp = _tasks().query(
        IndexName="by_created",
        KeyConditionExpression=Key("kind").eq("task"),
        ScanIndexForward=False,
        Limit=100,
    )
    return {"tasks": [_public_task(i) for i in resp.get("Items", [])]}


class CreateTaskRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=MAX_PROMPT_CHARS)
    mode: Literal["interactive", "unattended"] = "interactive"
    worker_id: str | None = Field(default=None, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
    parent_task_id: str | None = Field(default=None, max_length=64)


@router.post("/tasks", status_code=201)
def create_task(req: CreateTaskRequest, user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE))) -> dict:
    _require_enabled()
    worker_id = req.worker_id or _env("LOOM_CLAUDE_DEFAULT_WORKER", "loom-worker-1")

    if req.parent_task_id:
        parent = _get_task_item(req.parent_task_id)
        if parent.get("status") not in TERMINAL:
            raise HTTPException(status_code=409, detail="Parent task is still running")
        worker_id = parent.get("worker_id") or worker_id  # the session lives on that host

    open_tasks = _tasks().query(
        IndexName="by_created",
        KeyConditionExpression=Key("kind").eq("task"),
        FilterExpression=Attr("submitted_by").eq(user.username) & Attr("status").is_in(
            ["QUEUED", "RUNNING", "WAITING_APPROVAL"]),
        ScanIndexForward=False,
        Limit=200,
    ).get("Items", [])
    if len(open_tasks) >= MAX_OPEN_TASKS_PER_USER:
        raise HTTPException(status_code=429, detail=f"At most {MAX_OPEN_TASKS_PER_USER} open tasks per user")

    task_id = uuid.uuid4().hex[:16]
    item = {
        "task_id": task_id,
        "kind": "task",
        "prompt": req.prompt,
        "mode": req.mode,
        "status": "QUEUED",
        "submitted_by": user.username,
        "worker_id": worker_id,
        "created_at": _now_iso(),
        "last_seq": 0,
        "decisions": {},
        "cancel_requested": False,
    }
    if req.parent_task_id:
        item["parent_task_id"] = req.parent_task_id
    _tasks().put_item(Item=item, ConditionExpression="attribute_not_exists(task_id)")
    logger.info("Claude task %s queued by %s for %s (mode=%s)", task_id, user.username, worker_id, req.mode)

    try:
        _publish(f"loom/claude/tasks/{worker_id}", {"task_id": task_id})
    except Exception as e:  # noqa: BLE001 - worker also sweeps QUEUED tasks
        logger.warning("IoT publish for task %s failed (worker sweep will pick it up): %s", task_id, e)
    return _public_task(item)


@router.get("/tasks/{task_id}")
def get_task(task_id: str, user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE))) -> dict:
    _require_enabled()
    return _public_task(_get_task_item(task_id))


def _query_events(task_id: str, after: int, limit: int = 500) -> list[dict]:
    resp = _events().query(
        KeyConditionExpression=Key("task_id").eq(task_id) & Key("seq").gt(after),
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


@router.get("/tasks/{task_id}/events")
def get_events(
    task_id: str,
    after: int = Query(0, ge=0),
    user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE)),
) -> dict:
    _require_enabled()
    task = _get_task_item(task_id)
    return {"events": _query_events(task_id, after), "task_status": task.get("status")}


@router.get("/tasks/{task_id}/stream")
async def stream_events(
    task_id: str,
    after: int = Query(0, ge=0),
    user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE)),
) -> StreamingResponse:
    _require_enabled()
    await asyncio.to_thread(_get_task_item, task_id)  # 404 before opening the stream

    async def gen():
        last = after
        started = time.monotonic()
        last_send = started
        while time.monotonic() - started < STREAM_MAX_SECONDS:
            events = await asyncio.to_thread(_query_events, task_id, last)
            for ev in events:
                last = max(last, int(ev.get("seq", last)))
                yield f"id: {last}\ndata: {json.dumps(ev, ensure_ascii=False)}\n\n"
                last_send = time.monotonic()
            if not events:
                task = await asyncio.to_thread(_get_task_item, task_id)
                if task.get("status") in TERMINAL and int(_plain(task.get("last_seq") or 0)) <= last:
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


@router.post("/tasks/{task_id}/approvals/{approval_id}", status_code=204)
def decide(
    task_id: str,
    approval_id: str,
    req: DecisionRequest,
    user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE)),
) -> None:
    _require_enabled()
    if not approval_id.isalnum() or len(approval_id) > 64:
        raise HTTPException(status_code=400, detail="Bad approval id")
    task = _get_task_item(task_id)
    try:
        _tasks().update_item(
            Key={"task_id": task_id},
            UpdateExpression="SET decisions.#a = :d",
            ConditionExpression="#s = :waiting AND attribute_not_exists(decisions.#a)",
            ExpressionAttributeNames={"#a": approval_id, "#s": "status"},
            ExpressionAttributeValues={
                ":d": {"decision": req.decision, "by": user.username, "at": _now_iso()},
                ":waiting": "WAITING_APPROVAL",
            },
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise HTTPException(status_code=409, detail="Task is not waiting for this approval") from e
        raise
    logger.info("Claude task %s approval %s -> %s by %s", task_id, approval_id, req.decision, user.username)
    try:
        _publish(f"loom/claude/control/{task['worker_id']}",
                 {"type": "approval", "task_id": task_id, "approval_id": approval_id})
    except Exception as e:  # noqa: BLE001 - worker polls the task item too
        logger.warning("IoT publish (approval) failed: %s", e)


@router.post("/tasks/{task_id}/cancel", status_code=204)
def cancel(task_id: str, user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE))) -> None:
    _require_enabled()
    task = _get_task_item(task_id)
    if task.get("status") in TERMINAL:
        return
    if task.get("status") == "QUEUED":
        try:
            _tasks().update_item(
                Key={"task_id": task_id},
                UpdateExpression="SET #s = :c, finished_at = :t, cancel_requested = :y",
                ConditionExpression="#s = :q",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":c": "CANCELLED", ":t": _now_iso(), ":y": True, ":q": "QUEUED"},
            )
            logger.info("Claude task %s cancelled while queued by %s", task_id, user.username)
            return
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                raise
            # it just started running: fall through to the running path
    _tasks().update_item(
        Key={"task_id": task_id},
        UpdateExpression="SET cancel_requested = :y, cancel_requested_by = :u",
        ExpressionAttributeValues={":y": True, ":u": user.username},
    )
    logger.info("Claude task %s cancel requested by %s", task_id, user.username)
    try:
        _publish(f"loom/claude/control/{task['worker_id']}", {"type": "cancel", "task_id": task_id})
    except Exception as e:  # noqa: BLE001
        logger.warning("IoT publish (cancel) failed: %s", e)
