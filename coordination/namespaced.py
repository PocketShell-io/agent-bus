"""Namespaced identities and separated transport vs semantic outcomes for AgentBus.

A send receipt is not a recipient read ACK. A read ACK is not semantic
agreement. Semantic agreement is not action completion.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any
from uuid import uuid4


class TransportState(str, Enum):
    RECORDED = "recorded"
    SEND_RECEIPT = "send_receipt"
    RECIPIENT_READ_ACK = "recipient_read_ack"
    SEMANTIC_AGREED = "semantic_agreed"
    ACTION_COMPLETED = "action_completed"


@dataclass(frozen=True)
class NamespacedId:
    device_id: str
    workspace: str
    agent_tag: str
    task_id: str
    session_id: str | None = None

    def render(self) -> str:
        session = self.session_id or "-"
        return f"{self.device_id}/{self.workspace}/{self.agent_tag}/{session}/{self.task_id}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SendReceipt:
    message_id: str
    idempotency_key: str
    sender: NamespacedId
    recipient: NamespacedId
    delivery: str
    recorded_at: str
    catalog_resolved_session_id: str | None
    catalog_observed_at: str
    bridge_device_id: str
    originating_agent: NamespacedId
    state: TransportState = TransportState.SEND_RECEIPT
    payload_sha256: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["state"] = self.state.value
        return data


@dataclass
class ReadAck:
    message_id: str
    acked_by: NamespacedId
    acked_at: str
    state: TransportState = TransportState.RECIPIENT_READ_ACK

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["state"] = self.state.value
        return data


@dataclass
class ActionOutcome:
    message_id: str
    correlation_token: str
    reply_message_id: str | None
    agreed: bool
    completed: bool
    evidence_paths: list[str] = field(default_factory=list)
    state: TransportState = TransportState.SEMANTIC_AGREED

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        if self.completed:
            data["state"] = TransportState.ACTION_COMPLETED.value
        else:
            data["state"] = (
                TransportState.SEMANTIC_AGREED.value
                if self.agreed
                else TransportState.SEND_RECEIPT.value
            )
        return data


def new_message_id() -> str:
    return str(uuid4())


def new_idempotency_key(prefix: str = "ac") -> str:
    return f"{prefix}-{uuid4()}"
