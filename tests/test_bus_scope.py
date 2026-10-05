"""Project-scope and same-user trust-boundary tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from coordination.bus import BusError, FileBus


def test_send_rejects_cross_project(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="alpha")
    b, tb = bus.register(agent_name="b", device_id="d", project_id="beta")
    with pytest.raises(BusError) as err:
        bus.send(sender_id=a.identity_id, token=ta, recipient_id=b.identity_id, body="x")
    assert err.value.code == "project_scope"
    assert bus.inbox(b.identity_id, tb) == []


def test_inbox_hides_other_project_even_if_file_is_planted(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="alpha")
    b, _tb = bus.register(agent_name="b", device_id="d", project_id="beta")
    planted = {
        "m1": {
            "message_id": "m1",
            "idempotency_key": "plant",
            "sender_id": b.identity_id,
            "recipient_id": a.identity_id,
            "body": "leak",
            "data": None,
            "kind": "note",
            "reply_to": None,
            "created_at": "2026-10-04T12:00:00Z",
            "delivered_at": "2026-10-04T12:00:00Z",
            "acked_at": None,
            "accepted_at": None,
            "outcome": None,
            "outcome_at": None,
            "digest": "x",
        }
    }
    (tmp_path / "messages.json").write_text(json.dumps(planted), encoding="utf-8")
    restarted = FileBus(tmp_path)
    assert restarted.inbox(a.identity_id, ta) == []
    with pytest.raises(BusError) as err:
        restarted.get(a.identity_id, ta, "m1")
    assert err.value.code == "project_scope"


def test_child_project_must_match_parent(tmp_path: Path):
    bus = FileBus(tmp_path)
    parent, pt = bus.register(agent_name="head", device_id="d", project_id="alpha")
    with pytest.raises(BusError) as err:
        bus.register(
            agent_name="worker",
            device_id="d",
            project_id="beta",
            parent_id=parent.identity_id,
            parent_token=pt,
        )
    assert err.value.code == "project_scope"


def test_queue_and_flush_outbox_after_restart(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="p")
    b, tb = bus.register(agent_name="b", device_id="d", project_id="p")
    bus.queue_offline(
        sender_id=a.identity_id,
        token=ta,
        recipient_id=b.identity_id,
        body="later",
        idempotency_key="off-1",
    )
    assert bus.inbox(b.identity_id, tb) == []
    restarted = FileBus(tmp_path)
    sent = restarted.flush_outbox(a.identity_id, ta)
    assert len(sent) == 1
    assert sent[0].body == "later"
    assert sent[0].delivered_at
    inbox = restarted.inbox(b.identity_id, tb)
    assert inbox[0].message_id == sent[0].message_id
    again = restarted.flush_outbox(a.identity_id, ta)
    assert again == []
    dup = restarted.send(
        sender_id=a.identity_id,
        token=ta,
        recipient_id=b.identity_id,
        body="later",
        idempotency_key="off-1",
    )
    assert dup.message_id == sent[0].message_id
