"""Validation for Agent Bus message envelopes.

A public envelope is the plain-dict serialization of a bus message
(see ``coordination.bus.BusMessage``). Consumers and heads at the
transport boundary use :func:`validate_bus_envelope` to reject
malformed payloads before any durable state is touched.
"""

from __future__ import annotations

from collections.abc import Mapping

REQUIRED_FIELDS: tuple[str, ...] = (
    "message_id",
    "sender_id",
    "recipient_id",
    "body",
    "created_at",
    "idempotency_key",
)

_MISSING_SENTINEL = object()


def _is_empty(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (bytes, bytearray)):
        return len(value) == 0
    if isinstance(value, (list, tuple, dict, set, frozenset)):
        return len(value) == 0
    return value == _MISSING_SENTINEL or bool(value) is False


def validate_bus_envelope(envelope: dict) -> tuple[bool, str]:
    """Validate an Agent Bus envelope.

    Returns ``(True, 'ok')`` when the envelope carries every required
    field with non-empty content. Otherwise returns
    ``(False, 'missing: <field>')`` for the first offending field, in
    ``REQUIRED_FIELDS`` order.
    """
    if not isinstance(envelope, Mapping):
        return False, "missing: envelope"

    for field in REQUIRED_FIELDS:
        value = envelope.get(field, _MISSING_SENTINEL)
        if value is _MISSING_SENTINEL or _is_empty(value):
            return False, f"missing: {field}"
    return True, "ok"


def _valid_envelope() -> dict:
    return {
        "message_id": "msg-20261005-0001",
        "sender_id": "agent-coordination-head81e801",
        "recipient_id": "bus-executor-7f3a",
        "body": "handoff: verify journal replay",
        "created_at": "2026-10-05T09:00:00Z",
        "idempotency_key": "idem-9d4a1c",
    }


def _run_tests() -> None:
    cases: list[tuple[dict, tuple[bool, str], str]] = [
        (_valid_envelope(), (True, "ok"), "complete envelope"),
        ({}, (False, "missing: message_id"), "empty envelope"),
        (_valid_envelope() | {"message_id": ""},
         (False, "missing: message_id"), "blank message_id"),
        (_valid_envelope() | {"sender_id": None},
         (False, "missing: sender_id"), "none sender_id"),
        (_valid_envelope() | {"recipient_id": "   "},
         (False, "missing: recipient_id"), "whitespace recipient_id"),
        (_valid_envelope() | {"body": 0},
         (False, "missing: body"), "falsy body"),
        (_valid_envelope() | {"created_at": ""},
         (False, "missing: created_at"), "blank created_at"),
        (_valid_envelope() | {"idempotency_key": "  \t\n"},
         (False, "missing: idempotency_key"), "whitespace idempotency_key"),
    ]

    for envelope, expected, label in cases:
        result = validate_bus_envelope(envelope)
        assert result == expected, f"{label}: expected {expected}, got {result}"

    for field in REQUIRED_FIELDS:
        envelope = _valid_envelope()
        del envelope[field]
        result = validate_bus_envelope(envelope)
        assert result == (False, f"missing: {field}"), (
            f"deleted {field}: got {result}"
        )

    print(f"ok: {len(cases) + len(REQUIRED_FIELDS)} tests passed")


if __name__ == "__main__":
    _run_tests()
