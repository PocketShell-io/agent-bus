import hashlib
from pathlib import Path

import pytest

from coordination.bus import BusError, FileBus
from coordination.errors import IdempotencyConflict


def test_register_send_inbox_ack_reply(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="worker-a", device_id="hetzner-rmthz", project_id="agent-coordination", task_id="dogfood")
    b, tb = bus.register(agent_name="worker-b", device_id="hetzner-rmthz", project_id="agent-coordination", task_id="dogfood")
    assert a.identity_id != b.identity_id
    msg = bus.send(
        sender_id=a.identity_id,
        token=ta,
        recipient_id=b.identity_id,
        body="task: write artifact",
        data={"artifact": "out.txt"},
        idempotency_key="dog-1",
    )
    inbox = bus.inbox(b.identity_id, tb)
    assert len(inbox) == 1
    assert inbox[0].message_id == msg.message_id
    acked = bus.ack(b.identity_id, tb, msg.message_id)
    assert acked.acked_at
    assert bus.inbox(b.identity_id, tb) == []
    reply = bus.reply(sender_id=b.identity_id, token=tb, message_id=msg.message_id, body="done", idempotency_key="dog-1-r")
    assert reply.reply_to == msg.message_id
    assert reply.recipient_id == a.identity_id


def test_idempotent_send_and_conflict(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="p")
    b, _ = bus.register(agent_name="b", device_id="d", project_id="p")
    m1 = bus.send(sender_id=a.identity_id, token=ta, recipient_id=b.identity_id, body="x", idempotency_key="k")
    m2 = bus.send(sender_id=a.identity_id, token=ta, recipient_id=b.identity_id, body="x", idempotency_key="k")
    assert m1.message_id == m2.message_id
    with pytest.raises(IdempotencyConflict):
        bus.send(sender_id=a.identity_id, token=ta, recipient_id=b.identity_id, body="y", idempotency_key="k")


def test_crash_restart_redelivers_unacked(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="p")
    b, tb = bus.register(agent_name="b", device_id="d", project_id="p")
    msg = bus.send(sender_id=a.identity_id, token=ta, recipient_id=b.identity_id, body="keep", idempotency_key="k2")
    restarted = FileBus(tmp_path)
    inbox = restarted.inbox(b.identity_id, tb)
    assert [m.message_id for m in inbox] == [msg.message_id]


def test_auth_and_unknown_recipient(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="p")
    with pytest.raises(BusError) as err:
        bus.send(sender_id=a.identity_id, token="wrong", recipient_id=a.identity_id, body="x")
    assert err.value.code == "auth_failed"
    with pytest.raises(BusError) as err2:
        bus.send(sender_id=a.identity_id, token=ta, recipient_id="missing", body="x")
    assert err2.value.code == "unknown_recipient"


def test_identity_is_not_aplexer_session(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("APLEXER_SESSION_ID", "81e8010c-89e4-478b-be3a-4ee6991607f3")
    bus = FileBus(tmp_path)
    ident, _ = bus.register(agent_name="plain", device_id="hetzner-rmthz", project_id="agent-coordination")
    assert ident.identity_id != "81e8010c-89e4-478b-be3a-4ee6991607f3"
    assert ident.kind == "bus-agent"


def test_unknown_identity_unknown_message_not_recipient(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="p")
    b, tb = bus.register(agent_name="b", device_id="d", project_id="p")
    msg = bus.send(sender_id=a.identity_id, token=ta, recipient_id=b.identity_id, body="x")
    with pytest.raises(BusError) as err:
        bus.inbox("missing-id", ta)
    assert err.value.code == "unknown_identity"
    with pytest.raises(BusError) as err2:
        bus.ack(b.identity_id, tb, "missing-msg")
    assert err2.value.code == "unknown_message"
    with pytest.raises(BusError) as err3:
        bus.ack(a.identity_id, ta, msg.message_id)
    assert err3.value.code == "not_recipient"
    with pytest.raises(BusError) as err4:
        bus.accept(a.identity_id, ta, msg.message_id)
    assert err4.value.code == "not_recipient"
    with pytest.raises(BusError) as err5:
        bus.reply(sender_id=a.identity_id, token=ta, message_id=msg.message_id, body="nope")
    assert err5.value.code == "not_recipient"
    with pytest.raises(BusError) as err6:
        bus.complete(a.identity_id, ta, msg.message_id, status="ok")
    assert err6.value.code == "not_recipient"


def test_four_states_and_artifact_digest(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="p")
    b, tb = bus.register(agent_name="b", device_id="d", project_id="p")
    artifact = tmp_path / "out.txt"
    artifact.write_text("result\n", encoding="utf-8")
    msg = bus.send(sender_id=a.identity_id, token=ta, recipient_id=b.identity_id, body="do")
    assert msg.delivered_at
    assert msg.acked_at is None
    assert msg.accepted_at is None
    assert msg.outcome is None
    acked = bus.ack(b.identity_id, tb, msg.message_id)
    accepted = bus.accept(b.identity_id, tb, msg.message_id)
    done = bus.complete(b.identity_id, tb, msg.message_id, status="ok", artifact=str(artifact))
    assert acked.acked_at
    assert accepted.accepted_at
    assert done.outcome_at
    assert done.outcome["status"] == "ok"
    assert done.outcome["artifact"] == str(artifact)
    assert done.outcome["digest"] == hashlib.sha256(artifact.read_bytes()).hexdigest()
    seen = bus.get(a.identity_id, ta, msg.message_id)
    assert seen.delivered_at and seen.acked_at and seen.accepted_at and seen.outcome
    again = bus.complete(b.identity_id, tb, msg.message_id, status="ok", artifact=str(artifact))
    assert again.outcome == done.outcome
    with pytest.raises(BusError) as err:
        bus.complete(b.identity_id, tb, msg.message_id, status="other", artifact=str(artifact))
    assert err.value.code == "outcome_conflict"


def test_send_kind_reply_rejects_unrelated_same_project_identity(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="p")
    b, tb = bus.register(agent_name="b", device_id="d", project_id="p")
    c, tc = bus.register(agent_name="c", device_id="d", project_id="p")
    msg = bus.send(sender_id=a.identity_id, token=ta, recipient_id=b.identity_id, body="task")
    with pytest.raises(BusError) as err:
        bus.send(
            sender_id=c.identity_id,
            token=tc,
            recipient_id=a.identity_id,
            body="spoof",
            kind="reply",
            reply_to=msg.message_id,
        )
    assert err.value.code == "not_recipient"
    assert bus.inbox(a.identity_id, ta) == []
    legit = bus.reply(
        sender_id=b.identity_id,
        token=tb,
        message_id=msg.message_id,
        body="ok",
        idempotency_key="legit-reply",
    )
    assert legit.kind == "reply"
    assert legit.reply_to == msg.message_id
    via_send = bus.send(
        sender_id=b.identity_id,
        token=tb,
        recipient_id=a.identity_id,
        body="ok2",
        kind="reply",
        reply_to=msg.message_id,
        idempotency_key="legit-send-reply",
    )
    assert via_send.reply_to == msg.message_id
    assert via_send.sender_id == b.identity_id


def test_send_reply_to_rejects_different_project_message(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="alpha")
    b, _tb = bus.register(agent_name="b", device_id="d", project_id="alpha")
    c, tc = bus.register(agent_name="c", device_id="d", project_id="beta")
    d, td = bus.register(agent_name="d", device_id="d", project_id="beta")
    msg = bus.send(sender_id=a.identity_id, token=ta, recipient_id=b.identity_id, body="alpha-task")
    with pytest.raises(BusError) as err:
        bus.send(
            sender_id=c.identity_id,
            token=tc,
            recipient_id=d.identity_id,
            body="spoof",
            reply_to=msg.message_id,
        )
    assert err.value.code == "project_scope"
    assert bus.inbox(d.identity_id, td) == []
    with pytest.raises(BusError) as err2:
        bus.send(
            sender_id=c.identity_id,
            token=tc,
            recipient_id=d.identity_id,
            body="spoof",
            kind="reply",
            reply_to=msg.message_id,
        )
    assert err2.value.code == "project_scope"
    assert bus.inbox(d.identity_id, td) == []


def test_idempotent_send_conflict_when_kind_or_reply_to_change(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="p")
    b, tb = bus.register(agent_name="b", device_id="d", project_id="p")
    original = bus.send(
        sender_id=a.identity_id,
        token=ta,
        recipient_id=b.identity_id,
        body="task",
        idempotency_key="orig",
    )
    first = bus.send(
        sender_id=b.identity_id,
        token=tb,
        recipient_id=a.identity_id,
        body="x",
        idempotency_key="k",
        kind="note",
    )
    with pytest.raises(IdempotencyConflict):
        bus.send(
            sender_id=b.identity_id,
            token=tb,
            recipient_id=a.identity_id,
            body="x",
            idempotency_key="k",
            kind="reply",
            reply_to=original.message_id,
        )
    with pytest.raises(IdempotencyConflict):
        bus.send(
            sender_id=b.identity_id,
            token=tb,
            recipient_id=a.identity_id,
            body="x",
            idempotency_key="k",
            kind="note",
            reply_to=original.message_id,
        )
    replay = bus.send(
        sender_id=b.identity_id,
        token=tb,
        recipient_id=a.identity_id,
        body="x",
        idempotency_key="k",
        kind="note",
    )
    assert replay.message_id == first.message_id
    assert bus.inbox(a.identity_id, ta) == [first]


def test_parent_child_independent_credentials(tmp_path: Path):
    bus = FileBus(tmp_path)
    parent, pt = bus.register(agent_name="head", device_id="d", project_id="p", task_id="integrate")
    child, ct = bus.register(
        agent_name="worker",
        device_id="d",
        project_id="p",
        task_id="core",
        parent_id=parent.identity_id,
        parent_token=pt,
    )
    assert child.parent_id == parent.identity_id
    assert child.identity_id != parent.identity_id
    assert ct != pt
    with pytest.raises(BusError) as err:
        bus.inbox(child.identity_id, pt)
    assert err.value.code == "auth_failed"
    msg = bus.send(sender_id=parent.identity_id, token=pt, recipient_id=child.identity_id, body="work")
    assert bus.inbox(child.identity_id, ct)[0].message_id == msg.message_id
    assert bus.inbox(parent.identity_id, pt) == []
