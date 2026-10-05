"""Aplexer-independent Agent Bus core.

Identities, credentials, devices, projects and tasks are bus-native.
They are never an aplexer session id, APLEXER_* env, or PID. The
installed aplexer CLI is an optional adapter, not this store.

Trust boundary: local FS permissions (store 0700, files 0600) protect
other OS users. They do not isolate mutually hostile agents that share
the same OS user and can open the store path. Token checks and
project-scope gates are API accident barriers, not a hostile-agent TCB.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .cursors import CursorStore, payload_digest
from .durable import FileLock, atomic_write_json, fsync_dir
from .errors import CoordinationError, IdempotencyConflict


class BusError(CoordinationError):
    def __init__(self, code: str, detail: str = ""):
        super().__init__(f"{code}:{detail}" if detail else code)
        self.code = code


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_id() -> str:
    return str(uuid.uuid4())


def _file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@dataclass
class BusIdentity:
    identity_id: str
    device_id: str
    project_id: str
    agent_name: str
    task_id: str | None = None
    parent_id: str | None = None
    kind: str = "bus-agent"
    created_at: str = field(default_factory=_utc)

    def public(self) -> dict[str, Any]:
        return asdict(self)


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
    delivered_at: str | None = None
    acked_at: str | None = None
    accepted_at: str | None = None
    outcome: dict[str, Any] | None = None
    outcome_at: str | None = None
    digest: str = ""

    def to_public(self) -> dict[str, Any]:
        return asdict(self)


def _msg(raw: dict[str, Any]) -> BusMessage:
    return BusMessage(**{k: raw.get(k) for k in BusMessage.__dataclass_fields__})


class FileBus:
    """Durable JSON-file bus. Crash-restart safe: send returns after fsync.

    Four states stay separate on each message:
    delivered_at (durable receipt), acked_at (read ACK),
    accepted_at (semantic accept), outcome/outcome_at (task result + digest).
    """

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._identities = self.root / "identities.json"
        self._tokens = self.root / "tokens.json"
        self._messages = self.root / "messages.json"
        self._journal = self.root / "journal.json"
        self._lock = self.root / "bus.lock"
        self._cursors = CursorStore(self.root / "cursors")
        with FileLock(self._lock):
            self._recover_locked()

    def _read(self, path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))

    def _write(self, path: Path, value: Any) -> None:
        atomic_write_json(path, value)

    def _commit(self, files: dict[Path, Any]) -> None:
        writes = []
        for path, value in files.items():
            rel = path.relative_to(self.root).as_posix()
            if Path(rel).is_absolute() or ".." in Path(rel).parts:
                raise BusError("journal_path", rel)
            writes.append({"path": rel, "value": value})
        self._write(self._journal, {"writes": writes})
        for path, value in files.items():
            self._write(path, value)
        try:
            self._journal.unlink()
        except FileNotFoundError:
            pass
        fsync_dir(self.root)

    def _recover_locked(self) -> None:
        if self._journal.exists():
            journal = self._read(self._journal, {})
            for item in journal.get("writes", []) if isinstance(journal, dict) else []:
                rel = str(item.get("path", ""))
                if not rel or Path(rel).is_absolute() or ".." in Path(rel).parts:
                    raise BusError("journal_path", rel)
                self._write(self.root / rel, item["value"])
            try:
                self._journal.unlink()
            except FileNotFoundError:
                pass
            fsync_dir(self.root)
        identities = self._read(self._identities, {})
        tokens = self._read(self._tokens, {})
        if not isinstance(identities, dict):
            identities = {}
        if not isinstance(tokens, dict):
            tokens = {}
        id_keys = set(identities)
        tok_keys = set(tokens)
        if id_keys != tok_keys:
            for key in list(identities):
                if key not in tokens:
                    del identities[key]
            for key in list(tokens):
                if key not in identities:
                    del tokens[key]
            self._commit({self._identities: identities, self._tokens: tokens})

    def _auth_locked(self, identity_id: str, token: str) -> dict[str, Any]:
        identities = self._read(self._identities, {})
        tokens = self._read(self._tokens, {})
        if identity_id not in identities:
            raise BusError("unknown_identity", identity_id)
        if tokens.get(identity_id) != token:
            raise BusError("auth_failed", identity_id)
        return identities[identity_id]

    def _require_same_project(self, left: dict[str, Any], right: dict[str, Any]) -> None:
        if left.get("project_id") != right.get("project_id"):
            raise BusError(
                "project_scope",
                f"{left.get('identity_id')}->{right.get('identity_id')}",
            )

    def _require_reply_target_locked(
        self,
        *,
        sender: dict[str, Any],
        sender_id: str,
        identities: dict[str, Any],
        messages: dict[str, Any],
        kind: str,
        reply_to: str | None,
    ) -> None:
        if kind != "reply" and reply_to is None:
            return
        if not reply_to:
            raise BusError("unknown_message", "reply_to")
        original = messages.get(reply_to)
        if not original:
            raise BusError("unknown_message", reply_to)
        orig_sender = identities.get(original["sender_id"])
        orig_recipient = identities.get(original["recipient_id"])
        if orig_sender is None:
            raise BusError("unknown_identity", original["sender_id"])
        if orig_recipient is None:
            raise BusError("unknown_identity", original["recipient_id"])
        self._require_same_project(sender, orig_sender)
        self._require_same_project(sender, orig_recipient)
        if kind == "reply" and original["recipient_id"] != sender_id:
            raise BusError("not_recipient", reply_to)

    def register(
        self,
        *,
        agent_name: str,
        device_id: str,
        project_id: str,
        task_id: str | None = None,
        parent_id: str | None = None,
        parent_token: str | None = None,
    ) -> tuple[BusIdentity, str]:
        with FileLock(self._lock):
            self._recover_locked()
            if parent_id is not None:
                if not parent_token:
                    raise BusError("auth_failed", parent_id)
                parent = self._auth_locked(parent_id, parent_token)
                self._require_same_project(
                    parent,
                    {"identity_id": "child", "project_id": project_id},
                )
            identities = self._read(self._identities, {})
            tokens = self._read(self._tokens, {})
            ident = BusIdentity(
                identity_id=_new_id(),
                device_id=device_id,
                project_id=project_id,
                agent_name=agent_name,
                task_id=task_id,
                parent_id=parent_id,
            )
            token = _new_id()
            identities[ident.identity_id] = ident.public()
            tokens[ident.identity_id] = token
            self._commit({self._identities: identities, self._tokens: tokens})
            return ident, token

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
            self._recover_locked()
            return self._send_locked(
                sender_id=sender_id,
                token=token,
                recipient_id=recipient_id,
                body=body,
                data=data,
                idempotency_key=idempotency_key,
                kind=kind,
                reply_to=reply_to,
            )

    def _send_locked(
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
        sender = self._auth_locked(sender_id, token)
        identities = self._read(self._identities, {})
        if recipient_id not in identities:
            raise BusError("unknown_recipient", recipient_id)
        self._require_same_project(sender, identities[recipient_id])
        messages: dict[str, Any] = self._read(self._messages, {})
        self._require_reply_target_locked(
            sender=sender,
            sender_id=sender_id,
            identities=identities,
            messages=messages,
            kind=kind,
            reply_to=reply_to,
        )
        digest = payload_digest(body, data)
        key = idempotency_key or (
            f"reply:{reply_to}:{sender_id}:{digest}"
            if kind == "reply" and reply_to
            else _new_id()
        )

        for existing in messages.values():
            if existing.get("idempotency_key") == key or key in existing.get("aliases", []):
                if (
                    existing["sender_id"] == sender_id
                    and existing["recipient_id"] == recipient_id
                    and existing.get("digest") == digest
                    and existing.get("kind") == kind
                    and existing.get("reply_to") == reply_to
                ):
                    return _msg(existing)
                raise IdempotencyConflict(key)
        if kind == "reply" and reply_to:
            for existing in messages.values():
                if (
                    existing.get("kind") == "reply"
                    and existing.get("reply_to") == reply_to
                    and existing["sender_id"] == sender_id
                    and existing["recipient_id"] == recipient_id
                    and existing.get("digest") == digest
                ):
                    aliases = existing.setdefault("aliases", [])
                    if key not in aliases and key != existing.get("idempotency_key"):
                        aliases.append(key)
                        messages[existing["message_id"]] = existing
                        self._write(self._messages, messages)
                    self._cursors.remember_send(
                        key,
                        sender=sender_id,
                        recipient=recipient_id,
                        digest=digest,
                        message_id=existing["message_id"],
                    )
                    return _msg(existing)
        now = _utc()
        msg = BusMessage(
            message_id=_new_id(),
            idempotency_key=key,
            sender_id=sender_id,
            recipient_id=recipient_id,
            body=body,
            data=data,
            kind=kind,
            reply_to=reply_to,
            created_at=now,
            delivered_at=now,
            digest=digest,
        )
        messages[msg.message_id] = msg.to_public()
        self._write(self._messages, messages)
        self._cursors.remember_send(
            key,
            sender=sender_id,
            recipient=recipient_id,
            digest=digest,
            message_id=msg.message_id,
        )
        return msg

    def inbox(self, identity_id: str, token: str, *, unread_only: bool = True) -> list[BusMessage]:
        with FileLock(self._lock):
            self._recover_locked()
            ident = self._auth_locked(identity_id, token)
            identities = self._read(self._identities, {})
            messages = self._read(self._messages, {})
            out = []
            for raw in messages.values():
                if raw["recipient_id"] != identity_id:
                    continue
                sender = identities.get(raw["sender_id"])
                if not sender or sender.get("project_id") != ident.get("project_id"):
                    continue
                if unread_only and raw.get("acked_at"):
                    continue
                out.append(_msg(raw))
            out.sort(key=lambda m: m.created_at)
            return out

    def get(self, identity_id: str, token: str, message_id: str) -> BusMessage:
        with FileLock(self._lock):
            self._recover_locked()
            ident = self._auth_locked(identity_id, token)
            messages = self._read(self._messages, {})
            identities = self._read(self._identities, {})
            raw = messages.get(message_id)
            if not raw:
                raise BusError("unknown_message", message_id)
            if identity_id not in (raw["sender_id"], raw["recipient_id"]):
                raise BusError("not_participant", message_id)
            other_id = raw["sender_id"] if identity_id == raw["recipient_id"] else raw["recipient_id"]
            other = identities.get(other_id)
            if not other:
                raise BusError("unknown_identity", other_id)
            self._require_same_project(ident, other)
            return _msg(raw)

    def ack(self, identity_id: str, token: str, message_id: str) -> BusMessage:
        with FileLock(self._lock):
            self._recover_locked()
            return self._touch_locked(identity_id, token, message_id, field="acked_at")

    def accept(self, identity_id: str, token: str, message_id: str) -> BusMessage:
        with FileLock(self._lock):
            self._recover_locked()
            return self._touch_locked(identity_id, token, message_id, field="accepted_at")

    def _touch_locked(self, identity_id: str, token: str, message_id: str, *, field: str) -> BusMessage:
        self._auth_locked(identity_id, token)
        messages = self._read(self._messages, {})
        raw = messages.get(message_id)
        if not raw:
            raise BusError("unknown_message", message_id)
        if raw["recipient_id"] != identity_id:
            raise BusError("not_recipient", message_id)
        if not raw.get(field):
            raw[field] = _utc()
            messages[message_id] = raw
            self._write(self._messages, messages)
        return _msg(raw)

    def complete(
        self,
        identity_id: str,
        token: str,
        message_id: str,
        *,
        status: str,
        artifact: str | None = None,
        digest: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> BusMessage:
        with FileLock(self._lock):
            self._recover_locked()
            self._auth_locked(identity_id, token)
            messages = self._read(self._messages, {})
            raw = messages.get(message_id)
            if not raw:
                raise BusError("unknown_message", message_id)
            if raw["recipient_id"] != identity_id:
                raise BusError("not_recipient", message_id)
            if artifact and not digest:
                digest = _file_digest(Path(artifact))
            outcome = {"status": status}
            if artifact is not None:
                outcome["artifact"] = artifact
            if digest is not None:
                outcome["digest"] = digest
            if extra:
                outcome.update(extra)
            existing = raw.get("outcome")
            if existing:
                if existing != outcome:
                    raise BusError("outcome_conflict", message_id)
                return _msg(raw)
            raw["outcome"] = outcome
            raw["outcome_at"] = _utc()
            messages[message_id] = raw
            self._write(self._messages, messages)
            return _msg(raw)

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
            self._recover_locked()
            self._auth_locked(sender_id, token)
            messages = self._read(self._messages, {})
            original = messages.get(message_id)
            if not original:
                raise BusError("unknown_message", message_id)
            if original["recipient_id"] != sender_id:
                raise BusError("not_recipient", message_id)
            return self._send_locked(
                sender_id=sender_id,
                token=token,
                recipient_id=original["sender_id"],
                body=body,
                data=data,
                idempotency_key=idempotency_key,
                kind="reply",
                reply_to=message_id,
            )

    def queue_offline(
        self,
        *,
        sender_id: str,
        token: str,
        recipient_id: str,
        body: str,
        data: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        kind: str = "note",
    ) -> dict[str, Any]:
        with FileLock(self._lock):
            self._recover_locked()
            sender = self._auth_locked(sender_id, token)
            identities = self._read(self._identities, {})
            if recipient_id not in identities:
                raise BusError("unknown_recipient", recipient_id)
            self._require_same_project(sender, identities[recipient_id])
            record = {
                "sender_id": sender_id,
                "recipient_id": recipient_id,
                "body": body,
                "data": data,
                "idempotency_key": idempotency_key or _new_id(),
                "kind": kind,
                "queued_at": _utc(),
            }
            self._cursors.queue_offline(record)
            return record

    def flush_outbox(self, sender_id: str, token: str) -> list[BusMessage]:
        with FileLock(self._lock):
            self._recover_locked()
            self._auth_locked(sender_id, token)
            sent: list[BusMessage] = []
            for record in self._cursors.pending_outbox():
                if record.get("sender_id") != sender_id:
                    continue
                msg = self._send_locked(
                    sender_id=sender_id,
                    token=token,
                    recipient_id=record["recipient_id"],
                    body=record["body"],
                    data=record.get("data"),
                    idempotency_key=record.get("idempotency_key"),
                    kind=record.get("kind") or "note",
                )
                self._cursors.mark_sent(record["idempotency_key"], msg.message_id)
                sent.append(msg)
            return sent

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
