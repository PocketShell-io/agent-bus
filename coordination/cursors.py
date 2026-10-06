"""Offline outbox, idempotent retries, and receive cursors."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .durable import FileLock, atomic_write_json
from .errors import IdempotencyConflict


def payload_digest(body: str, data: dict[str, Any] | None) -> str:
    blob = json.dumps({"body": body, "data": data or {}}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class CursorStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._idem = self.root / "idempotency.json"
        self._cursors = self.root / "cursors.json"
        self._outbox = self.root / "outbox.json"
        self._lock = self.root / "cursors.lock"

    def _load(self, path: Path) -> Any:
        if not path.exists():
            return {} if path != self._outbox else []
        return json.loads(path.read_text(encoding="utf-8"))

    def _save(self, path: Path, value: Any) -> None:
        atomic_write_json(path, value)

    def lookup_send(
        self,
        key: str,
        *,
        sender: str,
        recipient: str,
        digest: str,
    ) -> str | None:
        with FileLock(self._lock):
            return self._lookup_locked(key, sender=sender, recipient=recipient, digest=digest)

    def _lookup_locked(
        self,
        key: str,
        *,
        sender: str,
        recipient: str,
        digest: str,
    ) -> str | None:
        existing = self._load(self._idem).get(key)
        if not existing:
            return None
        if (
            existing["sender"] == sender
            and existing["recipient"] == recipient
            and existing["digest"] == digest
        ):
            return existing["message_id"]
        raise IdempotencyConflict(key)

    def remember_send(
        self,
        key: str,
        *,
        sender: str,
        recipient: str,
        digest: str,
        message_id: str,
    ) -> str | None:
        with FileLock(self._lock):
            existing = self._lookup_locked(key, sender=sender, recipient=recipient, digest=digest)
            if existing:
                return existing
            table = self._load(self._idem)
            table[key] = {
                "sender": sender,
                "recipient": recipient,
                "digest": digest,
                "message_id": message_id,
            }
            self._save(self._idem, table)
            return None

    def queue_offline(self, record: dict[str, Any]) -> None:
        with FileLock(self._lock):
            rows = self._load(self._outbox)
            if not isinstance(rows, list):
                rows = []
            rows.append(record)
            self._save(self._outbox, rows)

    def pending_outbox(self) -> list[dict[str, Any]]:
        with FileLock(self._lock):
            rows = self._load(self._outbox)
            if not isinstance(rows, list):
                return []
            return [r for r in rows if not r.get("sent")]

    def mark_sent(self, idempotency_key: str, message_id: str) -> None:
        with FileLock(self._lock):
            rows = self._load(self._outbox)
            if not isinstance(rows, list):
                rows = []
            for row in rows:
                if row.get("idempotency_key") == idempotency_key:
                    row["sent"] = True
                    row["message_id"] = message_id
            self._save(self._outbox, rows)

    def cursor(self, mailbox: str) -> str | None:
        with FileLock(self._lock):
            return self._load(self._cursors).get(mailbox)

    def advance(self, mailbox: str, message_id: str) -> None:
        with FileLock(self._lock):
            table = self._load(self._cursors)
            table[mailbox] = message_id
            self._save(self._cursors, table)

    def recover_corrupt_cursor(self, mailbox: str) -> bool:
        with FileLock(self._lock):
            try:
                self._load(self._cursors)
                return False
            except ValueError:
                import time
                backup = self._cursors.with_name(f"{self._cursors.stem}.corrupt.{int(time.time() * 1000)}{self._cursors.suffix}")
                try:
                    import shutil
                    shutil.copy2(self._cursors, backup)
                except Exception:
                    pass
                self._save(self._cursors, {})
                return True

