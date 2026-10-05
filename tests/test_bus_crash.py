"""Crash, shortwrite, and journal-recovery tests for FileBus."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from coordination.bus import BusError, FileBus
from coordination.durable import write_all


def test_write_retries_shortwrite(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    real_write = os.write
    hits = {"n": 0}

    def partial(fd: int, data: bytes | memoryview) -> int:
        if hits["n"] == 0 and len(data) > 4:
            hits["n"] += 1
            return 3
        return real_write(fd, data)

    monkeypatch.setattr(os, "write", partial)
    bus = FileBus(tmp_path)
    ident, token = bus.register(agent_name="a", device_id="d", project_id="p")
    assert ident.identity_id
    assert bus.inbox(ident.identity_id, token) == []
    assert hits["n"] >= 1


def test_write_fsyncs_file_and_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    real_fsync = os.fsync
    real_open = os.open
    dir_fds: list[int] = []
    fsync_fds: list[int] = []

    def spy_open(path, flags, *args):
        fd = real_open(path, flags, *args)
        if flags & getattr(os, "O_DIRECTORY", 0):
            dir_fds.append(fd)
        return fd

    def spy_fsync(fd: int) -> None:
        fsync_fds.append(fd)
        return real_fsync(fd)

    monkeypatch.setattr(os, "open", spy_open)
    monkeypatch.setattr(os, "fsync", spy_fsync)
    bus = FileBus(tmp_path)
    dir_fds.clear()
    fsync_fds.clear()
    bus.register(agent_name="a", device_id="d", project_id="p")
    assert dir_fds, "directory fd was never opened for fsync"
    assert set(dir_fds) & set(fsync_fds)


def test_journal_replay_completes_register(tmp_path: Path):
    ident = {
        "identity_id": "id-1",
        "device_id": "d",
        "project_id": "p",
        "agent_name": "a",
        "task_id": None,
        "parent_id": None,
        "kind": "bus-agent",
        "created_at": "2026-10-04T12:00:00Z",
    }
    journal = {
        "writes": [
            {"path": "identities.json", "value": {"id-1": ident}},
            {"path": "tokens.json", "value": {"id-1": "tok-1"}},
        ]
    }
    (tmp_path / "journal.json").write_text(json.dumps(journal), encoding="utf-8")
    bus = FileBus(tmp_path)
    assert not (tmp_path / "journal.json").exists()
    assert bus.inbox("id-1", "tok-1") == []


def test_orphan_identity_without_token_is_dropped(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="p")
    identities = json.loads((tmp_path / "identities.json").read_text(encoding="utf-8"))
    identities["orphan"] = {
        "identity_id": "orphan",
        "device_id": "d",
        "project_id": "p",
        "agent_name": "ghost",
        "kind": "bus-agent",
        "created_at": "2026-10-04T12:00:00Z",
    }
    (tmp_path / "identities.json").write_text(json.dumps(identities), encoding="utf-8")
    restarted = FileBus(tmp_path)
    with pytest.raises(BusError) as err:
        restarted.inbox("orphan", "nope")
    assert err.value.code == "unknown_identity"
    assert restarted.inbox(a.identity_id, ta) == []


def test_write_all_loops_until_complete(monkeypatch: pytest.MonkeyPatch):
    chunks: list[int] = []

    def stepped(fd: int, data: bytes | memoryview) -> int:
        n = min(2, len(data))
        chunks.append(n)
        return n

    monkeypatch.setattr(os, "write", stepped)
    write_all(7, b"abcdef")
    assert sum(chunks) == 6
    assert chunks == [2, 2, 2]
