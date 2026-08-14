"""
Milestone 5: the manifest.

The main process and the background compaction process are SEPARATE
OS processes (see compaction.py for why it has to be multiprocessing,
not threading) - they share no Python objects, no memory. The only
way they can agree on "what SSTable files currently make up the
database" is by writing it down somewhere both of them can see: this
MANIFEST.json file.

Writes are atomic (temp file + fsync + rename), same pattern as every
other file this project writes - a reader can never observe a
half-written manifest.

Two different processes can still both want to MODIFY the manifest at
the same moment though (the main process appending a newly-flushed
file; the compaction process swapping several old files for one
merged one) - a plain read-modify-write from two processes at once
would be a lost-update race. ManifestLock is a minimal cross-process
mutex (built from os.O_CREAT | os.O_EXCL, which is atomic on both
POSIX and Windows) that serializes those read-modify-write sequences.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Callable

MANIFEST_NAME = "MANIFEST.json"
LOCK_NAME = "MANIFEST.lock"


def read_manifest(dir_path: str | Path) -> list[str] | None:
    """
    Returns the current ordered (oldest -> newest) list of SSTable
    filenames, [] if no manifest has ever been written yet, or None
    if the read raced with a rename and hit a transient error - the
    caller should just try again shortly in that case, not treat it
    as "no data".
    """
    path = Path(dir_path) / MANIFEST_NAME
    if not path.exists():
        return []
    try:
        with open(path, "r") as f:
            data = json.load(f)
        return list(data.get("sstables", []))
    except (json.JSONDecodeError, OSError):
        return None


def write_manifest(dir_path: str | Path, sstable_filenames: list[str]) -> None:
    dir_path = Path(dir_path)
    path = dir_path / MANIFEST_NAME
    tmp_path = dir_path / (MANIFEST_NAME + ".tmp")
    with open(tmp_path, "w") as f:
        json.dump({"sstables": list(sstable_filenames)}, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)  # os.replace, not os.rename: atomically overwrites
    # an existing destination on BOTH POSIX and Windows. os.rename does this too
    # on POSIX, but raises FileExistsError on Windows if `path` already exists -
    # which happens here every time, since the manifest is rewritten on every
    # flush/compaction, not just created once like a fresh .sst file.


def update_manifest(
    dir_path: str | Path,
    mutate_fn: Callable[[list[str]], list[str]],
) -> list[str]:
    """
    Atomically read-modify-write the manifest under the cross-process
    lock: `mutate_fn` receives the current list and returns the new
    one. Used by both DB._flush() (append one filename) and
    compaction's swap step (remove several, insert one) - neither has
    to worry about racing the other.
    """
    dir_path = Path(dir_path)
    with ManifestLock(dir_path):
        current = read_manifest(dir_path)
        if current is None:
            current = []  # a transient read race under our own lock shouldn't
            # be possible since we hold the lock, but stay defensive
        new_list = mutate_fn(current)
        write_manifest(dir_path, new_list)
        return new_list


class ManifestLock:
    """
    A minimal filesystem-based mutex. `os.open` with
    O_CREAT | O_EXCL fails if the file already exists, and creating it
    is atomic at the OS level on both POSIX and Windows - so exactly
    one process can "win" the create at a time.

    Not production-grade: a process that crashes while holding the
    lock leaves an orphaned lock file behind. We handle that with a
    simple staleness override (steal a lock file older than
    `stale_after`) rather than a real liveness check (e.g. storing the
    holder's PID and checking whether it's still running) - good
    enough for a single-machine embedded store, not something you'd
    want in a real distributed lock service.
    """

    def __init__(
        self,
        dir_path: str | Path,
        timeout: float = 10.0,
        poll_interval: float = 0.01,
        stale_after: float = 5.0,
    ) -> None:
        self.lock_path = Path(dir_path) / LOCK_NAME
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.stale_after = stale_after
        self._fd: int | None = None

    def __enter__(self) -> "ManifestLock":
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self._fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                return self
            except FileExistsError:
                self._steal_if_stale()
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        f"Could not acquire manifest lock within {self.timeout}s "
                        f"- possible stale lock at {self.lock_path}"
                    )
                time.sleep(self.poll_interval)

    def _steal_if_stale(self) -> None:
        try:
            age = time.time() - self.lock_path.stat().st_mtime
        except FileNotFoundError:
            return
        if age > self.stale_after:
            self.lock_path.unlink(missing_ok=True)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        self.lock_path.unlink(missing_ok=True)
