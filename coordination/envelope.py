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


if __name__ == '__main__':
    import unittest

    class TestValidateBusEnvelope(unittest.TestCase):
        def setUp(self):
            self.valid_env = {
                "message_id": "msg-20261005-0001",
                "sender_id": "agent-coordination-head81e801",
                "recipient_id": "bus-executor-7f3a",
                "body": "handoff: verify journal replay",
                "created_at": "2026-10-05T09:00:00Z",
                "idempotency_key": "idem-9d4a1c",
            }

        def test_valid_envelope(self):
            result = validate_bus_envelope(self.valid_env)
            self.assertEqual(result, (True, "ok"))
            
        def test_empty_envelope(self):
            result = validate_bus_envelope({})
            self.assertEqual(result, (False, "missing: message_id"))

        def test_missing_field(self):
            for field in REQUIRED_FIELDS:
                env = self.valid_env.copy()
                del env[field]
                result = validate_bus_envelope(env)
                self.assertEqual(result, (False, f"missing: {field}"))

        def test_blank_field(self):
            env = self.valid_env.copy()
            env["message_id"] = ""
            result = validate_bus_envelope(env)
            self.assertEqual(result, (False, "missing: message_id"))

        def test_none_field(self):
            env = self.valid_env.copy()
            env["sender_id"] = None
            result = validate_bus_envelope(env)
            self.assertEqual(result, (False, "missing: sender_id"))

        def test_whitespace_field(self):
            env = self.valid_env.copy()
            env["recipient_id"] = "   "
            result = validate_bus_envelope(env)
            self.assertEqual(result, (False, "missing: recipient_id"))
            
        def test_falsy_field(self):
            env = self.valid_env.copy()
            env["body"] = 0
            result = validate_bus_envelope(env)
            self.assertEqual(result, (False, "missing: body"))

    unittest.main()
