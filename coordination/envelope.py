"""Namespaced identities, envelope definitions, and typed RPC framing for FileBus over SSH.

Provides:
- NamespacedId: Host- and store-scoped identity with URI-safe encoding.
- RpcRequest & RpcResponse: Structured FileBus remote procedure call framing.
- TransportState: Separation of transport vs semantic outcomes.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any
import urllib.parse
from uuid import uuid4


class TransportState(str, Enum):
    RECORDED = "recorded"
    SEND_RECEIPT = "send_receipt"
    RECIPIENT_READ_ACK = "recipient_read_ack"
    SEMANTIC_AGREED = "semantic_agreed"
    ACTION_COMPLETED = "action_completed"


@dataclass(frozen=True)
class NamespacedId:
    """
    Host- and store-qualified agent identity.

    Fields:
    - host: Remote hostname or SSH target (e.g., '192.168.1.10', 'desktop-hetzner').
    - store_path: Target FileBus store path (e.g., '/home/alexey/.local/agent_bus', 'C:\\data\\agent_bus').
    - agent_tag: Logical agent role/identifier (e.g., 'zcode-worker-1').
    - session_id: Aplexer/runtime session identifier (or '-' if unassociated).
    - task_id: Concrete task identifier (or '-' if unassociated).
    """

    host: str
    store_path: str
    agent_tag: str
    session_id: str
    task_id: str

    def to_dict(self) -> dict[str, str]:
        return {
            "host": self.host,
            "store_path": self.store_path,
            "agent_tag": self.agent_tag,
            "session_id": self.session_id,
            "task_id": self.task_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> NamespacedId:
        return cls(
            host=str(data["host"]),
            store_path=str(data["store_path"]),
            agent_tag=str(data["agent_tag"]),
            session_id=str(data.get("session_id", "-")),
            task_id=str(data.get("task_id", "-")),
        )

    def encode_uri(self) -> str:
        """
        Encode this identity into a URI-safe string scheme.
        Components are fully percent-encoded (safe against Windows drive letters like C:\\ and path slashes /).
        Format: nid:<host>:<store_path>:<agent_tag>:<session_id>:<task_id>
        """
        parts = [
            urllib.parse.quote(self.host, safe=""),
            urllib.parse.quote(self.store_path, safe=""),
            urllib.parse.quote(self.agent_tag, safe=""),
            urllib.parse.quote(self.session_id, safe=""),
            urllib.parse.quote(self.task_id, safe=""),
        ]
        return "nid:" + ":".join(parts)

    @classmethod
    def decode_uri(cls, uri_str: str) -> NamespacedId:
        """Decode a URI-safe string representation into a NamespacedId."""
        if not uri_str.startswith("nid:"):
            raise ValueError(f"Invalid NamespacedId URI scheme (expected prefix 'nid:'): {uri_str}")
        raw_parts = uri_str[4:].split(":")
        if len(raw_parts) != 5:
            raise ValueError(f"Expected 5 parts in NamespacedId URI, got {len(raw_parts)}: {uri_str}")
        return cls(
            host=urllib.parse.unquote(raw_parts[0]),
            store_path=urllib.parse.unquote(raw_parts[1]),
            agent_tag=urllib.parse.unquote(raw_parts[2]),
            session_id=urllib.parse.unquote(raw_parts[3]),
            task_id=urllib.parse.unquote(raw_parts[4]),
        )

    def to_uri(self) -> str:
        return self.encode_uri()

    @classmethod
    def from_uri(cls, uri_str: str) -> NamespacedId:
        return cls.decode_uri(uri_str)

    def render(self) -> str:
        """Human-readable string representation."""
        return f"{self.host}:{self.store_path}#{self.agent_tag}/{self.session_id}/{self.task_id}"

    def __str__(self) -> str:
        return self.encode_uri()


@dataclass
class RpcRequest:
    """
    Typed request envelope for FileBus operations over SSH.
    """

    op: str
    request_id: str
    params: dict[str, Any] = field(default_factory=dict)
    target: NamespacedId | None = None

    def to_dict(self) -> dict[str, Any]:
        res: dict[str, Any] = {
            "op": self.op,
            "request_id": self.request_id,
            "params": self.params,
        }
        if self.target is not None:
            res["target"] = self.target.to_dict()
        return res

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RpcRequest:
        target = None
        if "target" in data and data["target"] is not None:
            target = NamespacedId.from_dict(data["target"])
        return cls(
            op=str(data["op"]),
            request_id=str(data.get("request_id", "")),
            params=dict(data.get("params", {})),
            target=target,
        )


@dataclass
class RpcResponse:
    """
    Typed response envelope for FileBus operations over SSH.
    """

    request_id: str
    ok: bool
    result: Any = None
    error: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "ok": self.ok,
            "result": self.result,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RpcResponse:
        raw_ok = data.get("ok")
        if type(raw_ok) is not bool:
            raise ValueError(f"RPC response 'ok' field must be a strict boolean, got {type(raw_ok).__name__}")
        return cls(
            request_id=str(data.get("request_id", "")),
            ok=raw_ok,
            result=data.get("result"),
            error=data.get("error"),
        )


def new_request_id() -> str:
    return str(uuid4())


def new_idempotency_key(prefix: str = "rpc") -> str:
    return f"{prefix}-{uuid4()}"
