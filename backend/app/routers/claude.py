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
import re

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.dependencies.auth import UserInfo, require_scopes

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/claude", tags=["claude"])

REQUIRED_SCOPE = "admin:write"
KIND = "session"
# Permission modes: Claude Code's own (what Shift+Tab cycles in the CLI) plus
# Loom's "unattended" (deny anything off the worker's allowlist instead of
# asking). bypassPermissions is deliberately not offered.
Mode = Literal["default", "acceptEdits", "plan", "auto", "unattended"]
# Used only if Bedrock cannot be listed.
FALLBACK_MODELS = [
    "us.anthropic.claude-sonnet-5-5",
    "us.anthropic.claude-opus-5-5",
    "us.anthropic.claude-haiku-5-5",
    "us.anthropic.claude-sonnet-4-6",
]
_FAMILY_ORDER = {"opus": 0, "sonnet": 1, "fable": 2, "haiku": 3}
_MODEL_RE = re.compile(r"claude-([a-z]+)-(\d+)(?:-(\d+))?")
_MODELS_TTL = 600
_models_cache: tuple[float, list[str], dict[str, str]] | None = None


def _model_sort_key(model_id: str) -> tuple:
    m = _MODEL_RE.search(model_id)
    if not m:
        return (0, 0, 9, model_id)
    family, major, minor = m.group(1), int(m.group(2)), int(m.group(3) or 0)
    # newest version first; within a version Opus, Sonnet, Fable, Haiku
    return (-major, -(minor if minor < 100 else 0), _FAMILY_ORDER.get(family, 8), model_id)


def _list_bedrock_models() -> tuple[list[str], dict[str, str]]:
    """Active US cross-region Claude inference profiles (Claude 4 and later)."""
    client = boto3.client("bedrock", region_name=_region())
    ids: list[str] = []
    labels: dict[str, str] = {}
    token = None
    while True:
        kwargs: dict[str, Any] = {"maxResults": 1000, "typeEquals": "SYSTEM_DEFINED"}
        if token:
            kwargs["nextToken"] = token
        resp = client.list_inference_profiles(**kwargs)
        for p in resp.get("inferenceProfileSummaries", []):
            pid = p.get("inferenceProfileId", "")
            if p.get("status") != "ACTIVE" or not pid.startswith("us.anthropic.claude-"):
                continue
            m = _MODEL_RE.search(pid)
            if not m or int(m.group(2)) < 4:  # skip Claude 3.x
                continue
            ids.append(pid)
            name = (p.get("inferenceProfileName") or pid).replace("US Anthropic ", "").replace("US ", "")
            labels[pid] = name.replace("Claude ", "")
        token = resp.get("nextToken")
        if not token:
            break
    ids.sort(key=_model_sort_key)
    return ids, labels


def _models_and_labels() -> tuple[list[str], dict[str, str]]:
    global _models_cache
    raw = _env("LOOM_CLAUDE_MODELS")
    if raw:  # explicit allowlist wins
        ids = [m.strip() for m in raw.split(",") if m.strip()]
        return ids, {}
    now = time.time()
    if _models_cache and now - _models_cache[0] < _MODELS_TTL:
        return _models_cache[1], _models_cache[2]
    try:
        ids, labels = _list_bedrock_models()
        if ids:
            _models_cache = (now, ids, labels)
            return ids, labels
    except Exception as e:  # noqa: BLE001 - fall back to a static list
        logger.warning("Could not list Bedrock inference profiles: %s", e)
    return FALLBACK_MODELS, {}


def _models() -> list[str]:
    return _models_and_labels()[0]


def _default_model() -> str:
    ids = _models()
    preferred = _env("LOOM_CLAUDE_DEFAULT_MODEL")
    if preferred in ids:
        return preferred
    # newest Sonnet is the usual default (balanced speed/quality), else the first
    return next((m for m in ids if "-sonnet-" in m), ids[0] if ids else FALLBACK_MODELS[0])


def _validate_model(model: str | None) -> str | None:
    if model is None:
        return None
    if model not in _models():
        raise HTTPException(status_code=400, detail=f"Model not allowed: {model}")
    return model
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
        "cwd": item.get("cwd"),
        "model": item.get("model"),
        "pending_approvals": sorted((item.get("pending_approvals") or {}).keys()),
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


MAX_UPLOAD_BYTES = int(_env("LOOM_CLAUDE_MAX_UPLOAD_MB", "200")) * 1024 * 1024
MAX_ATTACHMENTS = 20
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._ ()\[\]+@,-]")
_s3 = None


def _files_bucket() -> str:
    return _env("LOOM_CLAUDE_FILES_BUCKET")


def _s3_client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=_region())
    return _s3


def _user_prefix(user: UserInfo) -> str:
    # Cognito usernames are already restricted; keep the key path tidy anyway.
    return "uploads/" + re.sub(r"[^A-Za-z0-9._@-]", "_", user.username or user.sub) + "/"


def _safe_filename(name: str) -> str:
    base = (name or "file").replace("\\", "/").rsplit("/", 1)[-1].strip() or "file"
    base = _SAFE_NAME.sub("_", base).lstrip(".") or "file"
    return base[:150]


def _resolve_attachments(file_ids: list[str], user: UserInfo) -> list[dict]:
    """Turn upload ids into attachment records; only the caller's own uploads."""
    if not file_ids:
        return []
    if len(file_ids) > MAX_ATTACHMENTS:
        raise HTTPException(status_code=400, detail=f"At most {MAX_ATTACHMENTS} attachments per message")
    out = []
    for fid in file_ids:
        if not re.fullmatch(r"[a-f0-9]{16}", fid or ""):
            raise HTTPException(status_code=400, detail="Bad file id")
        listed = _s3_client().list_objects_v2(Bucket=_files_bucket(), Prefix=f"{_user_prefix(user)}{fid}/", MaxKeys=2)
        objs = listed.get("Contents") or []
        if len(objs) != 1:
            raise HTTPException(status_code=400, detail=f"Unknown attachment {fid}")
        key = objs[0]["Key"]
        out.append({"file_id": fid, "key": key, "name": key.rsplit("/", 1)[-1], "size": int(objs[0]["Size"])})
    return out


def _message(text: str, user: UserInfo, attachments: list[dict] | None = None) -> dict:
    msg: dict[str, Any] = {"id": uuid.uuid4().hex[:12], "text": text, "by": user.username, "at": _now_iso()}
    if attachments:
        msg["attachments"] = attachments
    return msg


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
        "projects": list(w.get("projects") or []),
        "default_cwd": w.get("default_cwd"),
    } for w in items]
    default_worker = _env("LOOM_CLAUDE_DEFAULT_WORKER", "loom-worker-1")
    if not any(w["worker_id"] == default_worker for w in workers):
        workers.append({"worker_id": default_worker, "online": False, "last_heartbeat": None, "model": None,
                        "projects": [], "default_cwd": None})
    models, labels = _models_and_labels()
    return {"enabled": True, "workers": workers, "models": models, "model_labels": labels,
            "default_model": _default_model(),
            "modes": ["default", "acceptEdits", "plan", "auto", "unattended"]}


@router.post("/files", status_code=201)
async def upload_file(
    file: UploadFile = File(...),
    user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE)),
) -> dict:
    """Store one attachment (any type) in the private files bucket.

    The browser posts it through CloudFront/WAF to this authenticated route;
    nothing is ever uploaded straight to S3. Returns an id to reference in a
    message's ``attachments``. Files expire per the bucket lifecycle.
    """
    _require_enabled()
    if not _files_bucket():
        raise HTTPException(status_code=404, detail="Uploads are not configured")
    name = _safe_filename(file.filename or "file")
    content_type = (file.content_type or "application/octet-stream")[:100]
    # Enforce the size cap while reading, not from a client-declared header.
    file.file.seek(0, 2)
    size = file.file.tell()
    file.file.seek(0)
    if size > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"File larger than {MAX_UPLOAD_BYTES // (1024 * 1024)} MB")
    if size == 0:
        raise HTTPException(status_code=400, detail="Empty file")
    fid = uuid.uuid4().hex[:16]
    key = f"{_user_prefix(user)}{fid}/{name}"
    await asyncio.to_thread(
        _s3_client().upload_fileobj, file.file, _files_bucket(), key,
        ExtraArgs={"ContentType": content_type, "Metadata": {"uploaded-by": user.username[:200]}},
    )
    logger.info("Claude upload %s (%d bytes, %s) by %s", key, size, content_type, user.username)
    return {"file_id": fid, "name": name, "size": size, "content_type": content_type}


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
    attachments: list[str] = Field(default_factory=list, max_length=MAX_ATTACHMENTS)
    mode: Mode = "default"
    model: str | None = Field(default=None, max_length=100)
    worker_id: str | None = Field(default=None, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
    # Project directory on the worker (like `cd <dir> && claude`). The worker
    # re-validates it (must be under its user's home, not a secret location).
    cwd: str | None = Field(default=None, max_length=300, pattern=r"^/[A-Za-z0-9._/@+-]*$")


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
        "inbox": [_message(req.message, user, _resolve_attachments(req.attachments, user))],
        "consumed": 0,
        "pending_count": 1,
        "last_seq": 0,
        "decisions": {},
        "interrupt_requested": False,
        "close_requested": False,
    }
    if req.cwd and ".." not in req.cwd.split("/"):
        item["cwd"] = req.cwd
    if _validate_model(req.model):
        item["model"] = req.model
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
    attachments: list[str] = Field(default_factory=list, max_length=MAX_ATTACHMENTS)


@router.post("/sessions/{session_id}/messages", status_code=202)
def send_message(
    session_id: str,
    req: SendMessageRequest,
    user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE)),
) -> dict:
    """Queue the next user turn. Sending while a turn runs queues it (it runs next)."""
    _require_enabled()
    item = _get_item(session_id)
    message = _message(req.message, user, _resolve_attachments(req.attachments, user))
    try:
        resp = _sessions().update_item(
            Key={"task_id": session_id},
            UpdateExpression="SET #in = list_append(#in, :m), #u = :t ADD #pc :one",
            ConditionExpression="NOT (#s IN (:closed, :failed)) AND #cr = :f AND size(#in) < :max",
            ExpressionAttributeNames={"#s": "status", "#in": "inbox", "#u": "updated_at",
                                      "#pc": "pending_count", "#cr": "close_requested"},
            ExpressionAttributeValues={
                ":m": [message], ":t": _now_iso(), ":one": 1,
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
    # Accept a decision for any approval the worker has listed as open, not
    # just while the session-level status says WAITING_APPROVAL: with several
    # tool calls waiting in parallel, answering one flips the status back.
    try:
        _sessions().update_item(
            Key={"task_id": session_id},
            UpdateExpression="SET #d.#a = :d",
            # Workers that predate pending_approvals never write it: fall back
            # to the session-level WAITING_APPROVAL check for those.
            ConditionExpression="attribute_not_exists(#d.#a) AND "
                                "(attribute_exists(#pa.#a) OR (attribute_not_exists(#pa) AND #s = :waiting))",
            ExpressionAttributeNames={"#d": "decisions", "#a": approval_id, "#pa": "pending_approvals",
                                      "#s": "status"},
            ExpressionAttributeValues={":d": {"decision": req.decision, "by": user.username, "at": _now_iso()},
                                       ":waiting": "WAITING_APPROVAL"},
        )
    except ClientError as e:
        if _conditional_failed(e):
            raise HTTPException(
                status_code=409,
                detail="This approval is no longer open (already answered, the turn ended, or it timed out)",
            ) from e
        raise
    logger.info("Claude session %s approval %s -> %s by %s", session_id, approval_id, req.decision, user.username)
    _notify(item["worker_id"], "control", {"type": "approval", "session_id": session_id})


class SettingsRequest(BaseModel):
    mode: Mode | None = None
    model: str | None = Field(default=None, max_length=100)


@router.post("/sessions/{session_id}/settings")
def update_settings(
    session_id: str,
    req: SettingsRequest,
    user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE)),
) -> dict:
    """Switch permission mode (Shift+Tab) and/or model (/model) on a live session.
    Applies immediately, including to a turn that is running."""
    _require_enabled()
    item = _get_item(session_id)
    if item.get("status") in CLOSED_STATES:
        raise HTTPException(status_code=409, detail="Session has ended")
    fields: dict[str, Any] = {}
    if req.mode:
        fields["mode"] = req.mode
    if req.model:
        fields["model"] = _validate_model(req.model)
    if not fields:
        return _public(item)
    names = {f"#{k}": k for k in fields}
    names["#u"] = "updated_at"
    values = {f":{k}": v for k, v in fields.items()}
    values[":t"] = _now_iso()
    resp = _sessions().update_item(
        Key={"task_id": session_id},
        UpdateExpression="SET " + ", ".join(f"#{k} = :{k}" for k in fields) + ", #u = :t",
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
        ReturnValues="ALL_NEW",
    )
    logger.info("Claude session %s settings %s by %s", session_id, fields, user.username)
    _notify(item["worker_id"], "control", {"type": "settings", "session_id": session_id})
    return _public(resp["Attributes"])


@router.post("/sessions/{session_id}/interrupt", status_code=204)
def interrupt(session_id: str, user: UserInfo = Depends(require_scopes(REQUIRED_SCOPE))) -> None:
    """Stop the current turn (like Esc in the terminal). The session stays open."""
    _require_enabled()
    item = _get_item(session_id)
    if item.get("status") not in ("RUNNING", "WAITING_APPROVAL"):
        return
    _sessions().update_item(
        Key={"task_id": session_id},
        UpdateExpression="SET #ir = :y, #ib = :u",
        ExpressionAttributeNames={"#ir": "interrupt_requested", "#ib": "interrupt_by"},
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
            UpdateExpression="SET #s = :c, #cr = :y, #u = :t",
            ConditionExpression="#s = :idle AND #pc = :zero",
            ExpressionAttributeNames={"#s": "status", "#cr": "close_requested", "#u": "updated_at",
                                      "#pc": "pending_count"},
            ExpressionAttributeValues={":c": "CLOSED", ":y": True, ":t": _now_iso(), ":idle": "IDLE", ":zero": 0},
        )
    except ClientError as e:
        if not _conditional_failed(e):
            raise
        # A turn is running or queued: ask the worker to stop and close.
        _sessions().update_item(
            Key={"task_id": session_id},
            UpdateExpression="SET #cr = :y, #ir = :y",
            ExpressionAttributeNames={"#cr": "close_requested", "#ir": "interrupt_requested"},
            ExpressionAttributeValues={":y": True},
        )
    logger.info("Claude session %s closed by %s", session_id, user.username)
    _notify(item["worker_id"], "control", {"type": "close", "session_id": session_id})
