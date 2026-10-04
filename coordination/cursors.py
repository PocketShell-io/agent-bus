"""Offline outbox, idempotent retries, and receive cursors."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .errors import IdempotencyConflict


def payload_digest(body: str, data: dict[str, Any] | None) -> str:
    blob = json.dumps({"body": body, "data": data or {}}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class CursorStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._idem = self.root / "idempotency.json"
        self._cursors = self.root / "cursors.json"
        self._outbox = self.root / "outbox.jsonl"

    def _load(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _save(self, path: Path, value: dict[str, Any]) -> None:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(path)

    def lookup_send(
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
        existing = self.lookup_send(key, sender=sender, recipient=recipient, digest=digest)
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
        with self._outbox.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")

    def pending_outbox(self) -> list[dict[str, Any]]:
        if not self._outbox.exists():
            return []
        rows = []
        for line in self._outbox.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return [r for r in rows if not r.get("sent")]

    def mark_sent(self, idempotency_key: str, message_id: str) -> None:
        rows = []
        if self._outbox.exists():
            for line in self._outbox.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                row = json.loads(line)
                if row.get("idempotency_key") == idempotency_key:
                    row["sent"] = True
                    row["message_id"] = message_id
                rows.append(row)
        self._outbox.write_text(
            "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows),
            encoding="utf-8",
        )

    def cursor(self, mailbox: str) -> str | None:
        return self._load(self._cursors).get(mailbox)

    def advance(self, mailbox: str, message_id: str) -> None:
        table = self._load(self._cursors)
        table[mailbox] = message_id
        self._save(self._cursors, table)
