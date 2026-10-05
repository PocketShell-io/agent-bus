"""Concurrent register/send coverage for FileBus flock."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from coordination.bus import FileBus


def test_concurrent_register_unique_identities(tmp_path: Path):
    def one(i: int):
        return FileBus(tmp_path).register(agent_name=f"a{i}", device_id="d", project_id="p")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(one, range(16)))
    ids = [ident.identity_id for ident, _token in results]
    assert len(set(ids)) == 16


def test_concurrent_sends_are_all_delivered(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="p")
    b, tb = bus.register(agent_name="b", device_id="d", project_id="p")

    def send_one(i: int):
        return FileBus(tmp_path).send(
            sender_id=a.identity_id,
            token=ta,
            recipient_id=b.identity_id,
            body=str(i),
            idempotency_key=f"c-{i}",
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        sent = list(pool.map(send_one, range(20)))
    inbox = bus.inbox(b.identity_id, tb)
    assert len(inbox) == 20
    assert {m.message_id for m in inbox} == {m.message_id for m in sent}
