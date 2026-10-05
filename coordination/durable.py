"""POSIX durable JSON writes for the Agent Bus store.

Trust boundary: directory mode 0700 and file mode 0600 keep other OS users
off the store. Agents that run as the same OS user share the filesystem and
can read any file here; they are not mutually hostile principals. Token and
project checks stop accidental cross-identity use through the API. They do
not encrypt isolation against a same-user process with store path access.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def write_all(fd: int, data: bytes) -> None:
    """Write the full buffer. A single os.write may return a short count."""
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError(f"os.write returned {written}")
        view = view[written:]


def fsync_dir(path: Path) -> None:
    dir_fd = os.open(os.fspath(path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def atomic_write_json(path: Path, value: Any, *, mode: int = 0o600) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_name(path.name + ".tmp")
    payload = json.dumps(value, indent=2, sort_keys=True).encode("utf-8")
    flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    fd = os.open(tmp, flags, mode)
    try:
        write_all(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    fsync_dir(path.parent)


def write_secret_json(path: Path, value: Any) -> None:
    """Write a credential file and force mode 0600 even if it already existed."""
    atomic_write_json(path, value, mode=0o600)
    os.chmod(path, 0o600)


class FileLock:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._fd: int | None = None

    def __enter__(self) -> FileLock:
        import fcntl

        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        self._fd = os.open(self.path, flags, 0o600)
        fcntl.flock(self._fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc: object) -> None:
        import fcntl

        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None
