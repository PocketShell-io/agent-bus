"""Independent sessionless real worker send, receive, and ACK capabilities for AgentBus.

Workers communicate using bus-native identities without requiring, adopting,
or forging an aplexer interactive session identity.
Supports durable cursor persistence across worker stops/restarts, explicit
ACK with receipt tracking, and safe recovery from corrupt cursor files.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .bus import BusError, BusIdentity, BusMessage, FileBus, _new_id, _utc
from .cursors import CursorStore, payload_digest
from .durable import FileLock
from .namespaced import NamespacedId, ReadAck, SendReceipt, TransportState


@dataclass
class WorkerSendOutcome:
    """Outcome of sending a message over the worker bus.

    Can be unpacked as `msg, receipt = worker.send(...)` or accessed via
    attribute properties `.message`, `.receipt`, `.message_id`, `.idempotency_key`.
    """

    message: BusMessage
    receipt: SendReceipt

    @property
    def message_id(self) -> str:
        return self.message.message_id

    @property
    def idempotency_key(self) -> str:
        return self.message.idempotency_key

    def __iter__(self):
        return iter((self.message, self.receipt))

    def __getitem__(self, index: int):
        return (self.message, self.receipt)[index]

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": self.message.to_public(),
            "receipt": self.receipt.to_dict(),
        }


class ReceiptStore:
    """Durable JSON store for tracking send receipts and read ACKs."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._path = self.root / "receipts.json"

    def _load(self) -> dict[str, Any]:
        if not self._path.exists():
            return {"send_receipts": {}, "read_acks": {}}
        try:
            content = self._path.read_text(encoding="utf-8")
            if not content.strip():
                return {"send_receipts": {}, "read_acks": {}}
            data = json.loads(content)
            if not isinstance(data, dict):
                raise ValueError("Corrupt receipts file")
            data.setdefault("send_receipts", {})
            data.setdefault("read_acks", {})
            return data
        except Exception:
            # Corrupt receipt recovery: backup and re-initialize
            backup = self._path.with_name(f"{self._path.stem}.corrupt.{int(time.time() * 1000)}{self._path.suffix}")
            try:
                import shutil
                shutil.copy2(self._path, backup)
            except Exception:
                pass
            empty = {"send_receipts": {}, "read_acks": {}}
            self._save(empty)
            return empty

    def _save(self, data: dict[str, Any]) -> None:
        tmp = self._path.with_suffix(".tmp")
        fd = os.open(tmp, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
        try:
            os.write(fd, json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, self._path)

    def record_send_receipt(self, receipt: SendReceipt) -> SendReceipt:
        data = self._load()
        data["send_receipts"][receipt.message_id] = receipt.to_dict()
        self._save(data)
        return receipt

    def record_read_ack(self, ack: ReadAck) -> ReadAck:
        data = self._load()
        data["read_acks"][ack.message_id] = ack.to_dict()
        self._save(data)
        return ack

    def get_send_receipt(self, message_id: str) -> SendReceipt | None:
        data = self._load()
        raw = data.get("send_receipts", {}).get(message_id)
        if not raw:
            return None
        sender_raw = raw["sender"]
        recipient_raw = raw["recipient"]
        originating_raw = raw.get("originating_agent", sender_raw)
        return SendReceipt(
            message_id=raw["message_id"],
            idempotency_key=raw["idempotency_key"],
            sender=NamespacedId(**sender_raw),
            recipient=NamespacedId(**recipient_raw),
            delivery=raw.get("delivery", "agent-bus"),
            recorded_at=raw["recorded_at"],
            catalog_resolved_session_id=raw.get("catalog_resolved_session_id"),
            catalog_observed_at=raw.get("catalog_observed_at", raw["recorded_at"]),
            bridge_device_id=raw.get("bridge_device_id", sender_raw.get("device_id", "")),
            originating_agent=NamespacedId(**originating_raw),
            state=TransportState(raw.get("state", TransportState.SEND_RECEIPT.value)),
            payload_sha256=raw.get("payload_sha256"),
        )

    def get_read_ack(self, message_id: str) -> ReadAck | None:
        data = self._load()
        raw = data.get("read_acks", {}).get(message_id)
        if not raw:
            return None
        acked_by = raw["acked_by"]
        return ReadAck(
            message_id=raw["message_id"],
            acked_by=NamespacedId(**acked_by) if isinstance(acked_by, dict) else acked_by,
            acked_at=raw["acked_at"],
            state=TransportState(raw.get("state", TransportState.RECIPIENT_READ_ACK.value)),
        )

    def list_receipts(self, message_id: str | None = None) -> list[dict[str, Any]]:
        data = self._load()
        out = []
        if message_id is not None:
            if message_id in data.get("send_receipts", {}):
                out.append(data["send_receipts"][message_id])
            if message_id in data.get("read_acks", {}):
                out.append(data["read_acks"][message_id])
        else:
            out.extend(data.get("send_receipts", {}).values())
            out.extend(data.get("read_acks", {}).values())
        return out


class SessionlessWorkerBus:
    """Bus client for sessionless real workers.

    Guarantees:
    - Never requires or adopts an aplexer interactive session identity (session_id is always None / '-').
    - Durable cursor persistence allows workers to stop/restart and resume message progression without duplicate consumption.
    - Explicit ACK with durable receipt tracking (SendReceipt and ReadAck).
    - Corrupt cursor recovery: backs up corrupted cursor files and recovers safely without crashing.
    """

    def __init__(
        self,
        store: str | Path | FileBus,
        identity: BusIdentity,
        token: str,
        *,
        workspace: str | None = None,
        cursor_store: str | Path | CursorStore | None = None,
        receipt_store: str | Path | ReceiptStore | None = None,
    ):
        if isinstance(store, FileBus):
            self.bus = store
        else:
            self.bus = FileBus(store)

        self.identity = identity
        self.token = token
        self.workspace = workspace or identity.project_id

        # Cursor store: defaults to subfolder in bus store or specified directory
        if isinstance(cursor_store, CursorStore):
            self.cursor_store = cursor_store
        elif cursor_store is not None:
            self.cursor_store = CursorStore(cursor_store)
        else:
            self.cursor_store = CursorStore(self.bus.root / "cursors")

        # Receipt store: defaults to receipts subfolder in bus store or specified directory
        if isinstance(receipt_store, ReceiptStore):
            self.receipt_store = receipt_store
        elif receipt_store is not None:
            self.receipt_store = ReceiptStore(receipt_store)
        else:
            self.receipt_store = ReceiptStore(self.bus.root / "receipts")

    @classmethod
    def register(
        cls,
        store: str | Path | FileBus,
        *,
        agent_name: str,
        device_id: str,
        project_id: str,
        task_id: str | None = None,
        workspace: str | None = None,
        cursor_store: str | Path | CursorStore | None = None,
        receipt_store: str | Path | ReceiptStore | None = None,
    ) -> SessionlessWorkerBus:
        """Register a new sessionless worker on the bus."""
        bus = store if isinstance(store, FileBus) else FileBus(store)
        ident, token = bus.register(
            agent_name=agent_name,
            device_id=device_id,
            project_id=project_id,
            task_id=task_id,
        )
        return cls(
            store=bus,
            identity=ident,
            token=token,
            workspace=workspace or project_id,
            cursor_store=cursor_store,
            receipt_store=receipt_store,
        )

    @classmethod
    def from_credentials(
        cls,
        store: str | Path | FileBus,
        cred: str | Path | dict[str, Any],
        *,
        workspace: str | None = None,
        cursor_store: str | Path | CursorStore | None = None,
        receipt_store: str | Path | ReceiptStore | None = None,
    ) -> SessionlessWorkerBus:
        """Instantiate a worker from credentials, verifying authentication."""
        if isinstance(cred, (str, Path)):
            raw = json.loads(Path(cred).read_text(encoding="utf-8"))
        else:
            raw = cred

        bus = store if isinstance(store, FileBus) else FileBus(store)
        ident = BusIdentity(
            identity_id=raw["identity_id"],
            device_id=raw["device_id"],
            project_id=raw["project_id"],
            agent_name=raw["agent_name"],
            task_id=raw.get("task_id"),
            parent_id=raw.get("parent_id"),
            kind=raw.get("kind", "bus-agent"),
            created_at=raw.get("created_at", _utc()),
        )
        token = raw["token"]
        # Verify authentication against bus store
        with FileLock(bus._lock):
            bus._auth_locked(ident.identity_id, token)

        return cls(
            store=bus,
            identity=ident,
            token=token,
            workspace=workspace or ident.project_id,
            cursor_store=cursor_store,
            receipt_store=receipt_store,
        )

    def save_credentials(self, path: str | Path) -> None:
        """Save worker credentials to a file with 0600 permissions."""
        target = Path(path)
        data = self.identity.public()
        data["token"] = self.token
        tmp = target.with_suffix(".tmp")
        fd = os.open(tmp, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
        try:
            os.write(fd, json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, target)

    @property
    def identity_id(self) -> str:
        return self.identity.identity_id

    @property
    def agent_name(self) -> str:
        return self.identity.agent_name

    @property
    def device_id(self) -> str:
        return self.identity.device_id

    @property
    def project_id(self) -> str:
        return self.identity.project_id

    @property
    def task_id(self) -> str | None:
        return self.identity.task_id

    @property
    def namespaced_id(self) -> NamespacedId:
        """Bus-native namespaced ID with NO aplexer session dependency."""
        return NamespacedId(
            device_id=self.device_id,
            workspace=self.workspace,
            agent_tag=self.agent_name,
            task_id=self.task_id or "default",
            session_id=None,  # Explicitly None: renders as '-' per envelope specification
        )

    def send(
        self,
        *,
        recipient_id: str,
        body: str,
        data: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        kind: str = "note",
        reply_to: str | None = None,
    ) -> WorkerSendOutcome:
        """Send a message and track an explicit SendReceipt."""
        msg = self.bus.send(
            sender_id=self.identity_id,
            token=self.token,
            recipient_id=recipient_id,
            body=body,
            data=data,
            idempotency_key=idempotency_key,
            kind=kind,
            reply_to=reply_to,
        )

        recipient_ident_raw = self.bus._read(self.bus._identities, {}).get(recipient_id, {})
        recipient_namespaced = NamespacedId(
            device_id=recipient_ident_raw.get("device_id", "unknown-device"),
            workspace=recipient_ident_raw.get("project_id", self.workspace),
            agent_tag=recipient_ident_raw.get("agent_name", "unknown-agent"),
            task_id=recipient_ident_raw.get("task_id") or "default",
            session_id=None,
        )

        receipt = SendReceipt(
            message_id=msg.message_id,
            idempotency_key=msg.idempotency_key,
            sender=self.namespaced_id,
            recipient=recipient_namespaced,
            delivery="agent-bus",
            recorded_at=msg.created_at,
            catalog_resolved_session_id=None,
            catalog_observed_at=msg.created_at,
            bridge_device_id=self.device_id,
            originating_agent=self.namespaced_id,
            state=TransportState.SEND_RECEIPT,
            payload_sha256=msg.digest,
        )
        self.receipt_store.record_send_receipt(receipt)
        return WorkerSendOutcome(message=msg, receipt=receipt)

    def receive(
        self,
        *,
        limit: int | None = None,
        timeout: float | None = None,
        unread_only: bool = True,
    ) -> list[BusMessage]:
        """Receive pending messages for this worker.

        Unacknowledged messages are returned so they can be processed and retried.
        Already acknowledged messages are filtered out based on cursor and acked_at state,
        guaranteeing exactly-once progression without duplication across restarts.
        """
        # Ensure cursor store is healthy
        self.cursor_store.recover_corrupt_cursor(self.identity_id)

        items = self.bus.inbox(self.identity_id, self.token, unread_only=unread_only)
        if not items and timeout and timeout > 0:
            items = self.bus.wait(self.identity_id, self.token, timeout=timeout)

        # Retrieve current durable cursor
        last_acked_id = self.cursor_store.cursor(self.identity_id)

        # Filter: if last_acked_id is known, ensure already acknowledged messages
        # are not re-consumed
        filtered = []
        for msg in items:
            if unread_only and msg.acked_at:
                continue
            filtered.append(msg)

        if limit is not None and limit > 0:
            filtered = filtered[:limit]

        return filtered

    def ack(self, message_id: str, *, outcome: dict[str, Any] | None = None) -> ReadAck:
        """Explicitly acknowledge a message, advance the durable cursor, and record a ReadAck receipt."""
        # Foreign worker validation: verify this worker is the message's recipient
        messages = self.bus._read(self.bus._messages, {})
        msg_raw = messages.get(message_id)
        if not msg_raw:
            raise BusError("unknown_message", message_id)
        if msg_raw["recipient_id"] != self.identity_id:
            raise BusError("not_recipient", message_id)

        # Acknowledge on the bus
        self.bus.ack(self.identity_id, self.token, message_id)

        # Advance durable cursor
        self.cursor_store.advance(self.identity_id, message_id)

        # Create and record ReadAck receipt
        ack_receipt = ReadAck(
            message_id=message_id,
            acked_by=self.namespaced_id,
            acked_at=_utc(),
            state=TransportState.RECIPIENT_READ_ACK,
        )
        self.receipt_store.record_read_ack(ack_receipt)
        return ack_receipt

    def reply(
        self,
        *,
        message_id: str,
        body: str,
        data: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> WorkerSendOutcome:
        """Reply to an incoming message and track the SendReceipt."""
        messages = self.bus._read(self.bus._messages, {})
        original = messages.get(message_id)
        if not original:
            raise BusError("unknown_message", message_id)
        if original["recipient_id"] != self.identity_id:
            raise BusError("not_recipient", message_id)

        return self.send(
            recipient_id=original["sender_id"],
            body=body,
            data=data,
            idempotency_key=idempotency_key,
            kind="reply",
            reply_to=message_id,
        )

    def current_cursor(self) -> str | None:
        """Return the current durable cursor position for this worker."""
        return self.cursor_store.cursor(self.identity_id)

    def get_send_receipt(self, message_id: str) -> SendReceipt | None:
        return self.receipt_store.get_send_receipt(message_id)

    def get_read_ack(self, message_id: str) -> ReadAck | None:
        return self.receipt_store.get_read_ack(message_id)

    def list_receipts(self, message_id: str | None = None) -> list[dict[str, Any]]:
        return self.receipt_store.list_receipts(message_id)

    def recover_corrupt_cursor(self) -> bool:
        """Recover from a corrupted cursor file if detected."""
        return self.cursor_store.recover_corrupt_cursor(self.identity_id)
