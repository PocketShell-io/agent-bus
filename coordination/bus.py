"""Aplexer-independent Agent Bus core.

Identities, credentials, devices, projects and tasks are bus-native.
They are never an aplexer session id, APLEXER_* env, or PID. The
installed aplexer CLI is an optional adapter, not this store.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .cursors import payload_digest
from .errors import CoordinationError, IdempotencyConflict


class BusError(CoordinationError):
    def __init__(self, code: str, detail: str = ""):
        super().__init__(f"{code}:{detail}" if detail else code)
        self.code = code


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_id() -> str:
    return str(uuid.uuid4())


@dataclass
class BusIdentity:
    identity_id: str
    device_id: str
    project_id: str
    agent_name: str
    task_id: str | None = None
    kind: str = "bus-agent"
    created_at: str = field(default_factory=_utc)

    def public(self) -> dict[str, Any]:
        data = asdict(self)
        return data


@dataclass
class BusMessage:
    message_id: str
    idempotency_key: str
    sender_id: str
    recipient_id: str
    body: str
    data: dict[str, Any] | None
    kind: str
    reply_to: str | None
    created_at: str
    acked_at: str | None = None
    digest: str = ""

    def to_public(self) -> dict[str, Any]:
        return asdict(self)


class FileLock:
    def __init__(self, path: Path):
        self.path = path
        self._fd: int | None = None

    def __enter__(self) -> FileLock:
        import fcntl

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(self._fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc: object) -> None:
        import fcntl

        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None


class FileBus:
    """Durable JSON-file bus. Crash-restart safe: send returns after fsync."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._identities = self.root / "identities.json"
        self._tokens = self.root / "tokens.json"
        self._messages = self.root / "messages.json"
        self._lock = self.root / "bus.lock"

    def _read(self, path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))

    def _write(self, path: Path, value: Any) -> None:
        tmp = path.with_suffix(".tmp")
        payload = json.dumps(value, indent=2, sort_keys=True)
        fd = os.open(tmp, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
        try:
            os.write(fd, payload.encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, path)

    def register(
        self,
        *,
        agent_name: str,
        device_id: str,
        project_id: str,
        task_id: str | None = None,
    ) -> tuple[BusIdentity, str]:
        with FileLock(self._lock):
            identities = self._read(self._identities, {})
            tokens = self._read(self._tokens, {})
            ident = BusIdentity(
                identity_id=_new_id(),
                device_id=device_id,
                project_id=project_id,
                agent_name=agent_name,
                task_id=task_id,
            )
            token = _new_id()
            identities[ident.identity_id] = ident.public()
            tokens[ident.identity_id] = token
            self._write(self._identities, identities)
            self._write(self._tokens, tokens)
            return ident, token

    def _auth(self, identity_id: str, token: str) -> dict[str, Any]:
        identities = self._read(self._identities, {})
        tokens = self._read(self._tokens, {})
        if identity_id not in identities:
            raise BusError("unknown_identity", identity_id)
        if tokens.get(identity_id) != token:
            raise BusError("auth_failed", identity_id)
        return identities[identity_id]

    def send(
        self,
        *,
        sender_id: str,
        token: str,
        recipient_id: str,
        body: str,
        data: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        kind: str = "note",
        reply_to: str | None = None,
    ) -> BusMessage:
        with FileLock(self._lock):
            self._auth(sender_id, token)
            identities = self._read(self._identities, {})
            if recipient_id not in identities:
                raise BusError("unknown_recipient", recipient_id)
            key = idempotency_key or _new_id()
            digest = payload_digest(body, data)
            messages: dict[str, Any] = self._read(self._messages, {})
            for existing in messages.values():
                if existing.get("idempotency_key") == key:
                    if (
                        existing["sender_id"] == sender_id
                        and existing["recipient_id"] == recipient_id
                        and existing.get("digest") == digest
                    ):
                        return BusMessage(**{k: existing[k] for k in BusMessage.__dataclass_fields__})
                    raise IdempotencyConflict(key)
            msg = BusMessage(
                message_id=_new_id(),
                idempotency_key=key,
                sender_id=sender_id,
                recipient_id=recipient_id,
                body=body,
                data=data,
                kind=kind,
                reply_to=reply_to,
                created_at=_utc(),
                digest=digest,
            )
            messages[msg.message_id] = msg.to_public()
            self._write(self._messages, messages)
            return msg

    def inbox(self, identity_id: str, token: str, *, unread_only: bool = True) -> list[BusMessage]:
        with FileLock(self._lock):
            self._auth(identity_id, token)
            messages = self._read(self._messages, {})
            out = []
            for raw in messages.values():
                if raw["recipient_id"] != identity_id:
                    continue
                if unread_only and raw.get("acked_at"):
                    continue
                out.append(BusMessage(**{k: raw.get(k) for k in BusMessage.__dataclass_fields__}))
            out.sort(key=lambda m: m.created_at)
            return out

    def ack(self, identity_id: str, token: str, message_id: str) -> BusMessage:
        with FileLock(self._lock):
            self._auth(identity_id, token)
            messages = self._read(self._messages, {})
            raw = messages.get(message_id)
            if not raw:
                raise BusError("unknown_message", message_id)
            if raw["recipient_id"] != identity_id:
                raise BusError("not_recipient", message_id)
            if not raw.get("acked_at"):
                raw["acked_at"] = _utc()
                messages[message_id] = raw
                self._write(self._messages, messages)
            return BusMessage(**{k: raw.get(k) for k in BusMessage.__dataclass_fields__})

    def reply(
        self,
        *,
        sender_id: str,
        token: str,
        message_id: str,
        body: str,
        data: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> BusMessage:
        with FileLock(self._lock):
            self._auth(sender_id, token)
            messages = self._read(self._messages, {})
            original = messages.get(message_id)
            if not original:
                raise BusError("unknown_message", message_id)
        return self.send(
            sender_id=sender_id,
            token=token,
            recipient_id=original["sender_id"],
            body=body,
            data=data,
            idempotency_key=idempotency_key,
            kind="reply",
            reply_to=message_id,
        )

    def wait(
        self,
        identity_id: str,
        token: str,
        *,
        timeout: float = 5.0,
        interval: float = 0.05,
    ) -> list[BusMessage]:
        deadline = time.time() + timeout
        while True:
            items = self.inbox(identity_id, token, unread_only=True)
            if items:
                return items
            if time.time() >= deadline:
                return []
            time.sleep(interval)
