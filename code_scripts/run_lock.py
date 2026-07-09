"""Cross-process global lock used by dashboard and scheduler-triggered runs."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

from code_scripts.paths import OPS_LOGS_DIR

LOCK_HELD_ENV = "OIAT_RUN_LOCK_HELD"
_LOCK_FILENAME = ".oiat_global_run.lock"

if os.name == "nt":
    import msvcrt  # pragma: no cover
else:
    import fcntl


@dataclass
class LockResult:
    acquired: bool
    reason: str = ""


def lock_file_path():
    OPS_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return OPS_LOGS_DIR / _LOCK_FILENAME


class GlobalRunLock:
    def __init__(self, holder: str):
        self.holder = holder
        self.path = lock_file_path()
        self._handle = None
        self._acquired = False

    def acquire(self) -> LockResult:
        if os.environ.get(LOCK_HELD_ENV) == "1":
            return LockResult(acquired=True, reason="lock already held by parent process")
        if self._acquired:
            return LockResult(acquired=True, reason="lock already acquired")

        self._handle = open(self.path, "a+", encoding="utf-8")
        try:
            if os.name == "nt":
                self._handle.seek(0)
                try:
                    msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)  # pragma: no cover
                except OSError:
                    return LockResult(acquired=False, reason="another run is already active")
            else:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return LockResult(acquired=False, reason="another run is already active")

        self._handle.seek(0)
        self._handle.truncate(0)
        self._handle.write(
            json.dumps(
                {
                    "holder": self.holder,
                    "pid": os.getpid(),
                    "acquired_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        )
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._acquired = True
        return LockResult(acquired=True, reason="acquired")

    def release(self) -> None:
        if not self._handle or not self._acquired:
            return
        try:
            if os.name == "nt":
                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)  # pragma: no cover
            else:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        finally:
            self._acquired = False
            self._handle.close()
            self._handle = None


@contextmanager
def hold_global_lock(holder: str) -> Iterator[LockResult]:
    lock = GlobalRunLock(holder=holder)
    result = lock.acquire()
    try:
        yield result
    finally:
        if result.acquired:
            lock.release()
