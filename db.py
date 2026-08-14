"""
Milestone 5: DB now coordinates with a background compaction process
through the manifest instead of just globbing "*.sst" itself.

Public API (put/get/delete) is still unchanged from Milestone 1. What
changed: DB no longer assumes it's the only thing that can create or
remove SSTable files. The compaction worker (see compaction.py), if
started, runs as a totally separate process and can merge/replace
files at any time - so the SSTable list DB read at startup can go
stale, and DB needs to notice and reconcile.

The manifest (manifest.py) is the shared source of truth both
processes read and write. DB checks it cheaply (a single stat() call)
before every get(), and only does the more expensive reconciliation
work when it's actually changed.

Read path priority, newest data first:
  1. active memtable (in-memory, always most recent)
  2. SSTables on disk, newest -> oldest, per the current manifest

The moment we find ANY record for the key - a real value or a
TOMBSTONE - we stop and use it. If a particular SSTable file has been
compacted away by the background process in between our manifest
refresh and actually reading it, we treat that as "not found here,
keep looking" rather than crashing - the merged replacement is either
already in our reader list, or will be picked up on the next refresh.
"""

from __future__ import annotations

import multiprocessing
import re
from pathlib import Path

from compaction import compaction_worker_loop
from manifest import MANIFEST_NAME, read_manifest, update_manifest, write_manifest
from memtable import TOMBSTONE, Memtable
from merge import k_way_merge_newest_wins
from sstable import SSTableReader, write_sstable
from wal import OP_DELETE, OP_PUT, WAL

DEFAULT_MEMTABLE_MAX_BYTES = 4 * 1024 * 1024  # 4MB, per the guide
_SST_NAME_RE = re.compile(r"^(\d{6})\.sst$")  # plain flush-created files only (not "cNNNNNN.sst")


class DB:
    def __init__(
        self,
        data_dir: str | Path,
        memtable_max_bytes: int = DEFAULT_MEMTABLE_MAX_BYTES,
    ) -> None:
        self._dir = Path(data_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._memtable_max_bytes = memtable_max_bytes
        self._compaction_process: multiprocessing.Process | None = None

        sst_names = self._discover_sstable_names()
        self._sstable_names: list[str] = list(sst_names)
        self._sstables: list[SSTableReader] = [SSTableReader(self._dir / n) for n in sst_names]
        self._manifest_mtime = self._current_manifest_mtime()

        seqs = [int(m.group(1)) for n in sst_names if (m := _SST_NAME_RE.match(n))]
        self._next_seq = (max(seqs) + 1) if seqs else 0

        self._memtable = Memtable()
        self._wal = WAL(self._dir / "wal.log")
        self._replay_wal()

    def _discover_sstable_names(self) -> list[str]:
        manifest_path = self._dir / MANIFEST_NAME
        if manifest_path.exists():
            return read_manifest(self._dir) or []

        # No manifest yet: either a brand-new directory, or one from
        # before Milestone 5 (plain "*.sst" files, no MANIFEST.json,
        # and by definition no "cNNNNNN.sst" compacted files either,
        # since compaction is what creates the manifest in the first
        # place). Fall back to a glob, then persist a manifest so this
        # discovery only ever has to happen once per directory.
        names = sorted(p.name for p in self._dir.glob("*.sst") if _SST_NAME_RE.match(p.name))
        if names:
            write_manifest(self._dir, names)
        return names

    def _current_manifest_mtime(self):
        try:
            return (self._dir / MANIFEST_NAME).stat().st_mtime_ns
        except FileNotFoundError:
            return None

    def _reconcile_sstables(self, names: list[str]) -> None:
        """Bring self._sstables in line with `names`, reusing already-open readers where possible."""
        if names == self._sstable_names:
            return
        existing_by_name = dict(zip(self._sstable_names, self._sstables))
        new_names, new_readers = [], []
        for name in names:
            reader = existing_by_name.get(name)
            if reader is None:
                try:
                    reader = SSTableReader(self._dir / name)
                except FileNotFoundError:
                    # Already gone again (e.g. compacted a second time
                    # since we read the manifest) - skip it, the next
                    # refresh will pick up whatever replaced it.
                    continue
            new_names.append(name)
            new_readers.append(reader)
        self._sstable_names = new_names
        self._sstables = new_readers

    def _maybe_refresh_sstables(self) -> None:
        mtime = self._current_manifest_mtime()
        if mtime == self._manifest_mtime:
            return  # cheap common case: nothing changed since we last looked
        self._manifest_mtime = mtime
        names = read_manifest(self._dir)
        if names is None:
            return  # transient read race - try again on the next call
        self._reconcile_sstables(names)

    def _replay_wal(self) -> None:
        """Rebuild the active memtable from whatever the WAL still has on disk (never-flushed writes)."""
        for op, key, value in self._wal.replay():
            if op == OP_PUT:
                self._memtable.put(key, value)
            elif op == OP_DELETE:
                self._memtable.delete(key)

    # -- public API ----------------------------------------------------

    def put(self, key: bytes, value: bytes) -> None:
        self._validate_bytes(key, "key")
        self._validate_bytes(value, "value")
        self._wal.append_put(key, value)
        self._memtable.put(key, value)
        self._maybe_flush()

    def delete(self, key: bytes) -> None:
        self._validate_bytes(key, "key")
        self._wal.append_delete(key)
        self._memtable.delete(key)
        self._maybe_flush()

    def get(self, key: bytes) -> bytes | None:
        self._validate_bytes(key, "key")

        result = self._memtable.get(key)
        if result is not None:
            return None if result is TOMBSTONE else result

        self._maybe_refresh_sstables()

        # Newest SSTable first: a later flush (or compaction merge)
        # can shadow an earlier one.
        for reader in reversed(self._sstables):
            try:
                result = reader.get(key)
            except (FileNotFoundError, OSError):
                # This file was compacted away between our manifest
                # refresh and this read - not a correctness problem,
                # just skip it and keep checking. Its data lives on in
                # whatever replaced it.
                continue
            if result is not None:
                return None if result is TOMBSTONE else result

        return None

    def scan(self, start_key: bytes | None = None, end_key: bytes | None = None):
        """
        Milestone 6: yields (key, value) pairs in sorted order for
        every LIVE key with start_key <= key <= end_key (either bound
        optional/unbounded), merged across the active memtable and
        every SSTable - newest write for each key wins, exactly like
        get()'s precedence, and deleted keys never appear.

        Both bounds are inclusive. If start_key > end_key, yields
        nothing (an empty range), rather than raising - the same
        convention Python slicing uses.

        Implementation: each source (memtable, each SSTable) already
        hands back its OWN slice in sorted order cheaply (the memtable
        via SortedDict.irange, each SSTable via its sparse index) -
        this just fans those out through the same k-way merge
        compaction uses, with the memtable passed LAST (it's always
        the newest source), then drops tombstones from the result.

        Validates arguments eagerly (bad types raise immediately, like
        put()/get() do) even though the actual work is lazy - a method
        containing `yield` runs none of its body, not even validation,
        until the caller starts iterating, which would otherwise let a
        bad call sit silently until someone finally consumes it.
        """
        self._validate_optional_bytes(start_key, "start_key")
        self._validate_optional_bytes(end_key, "end_key")
        return self._scan_impl(start_key, end_key)

    def _scan_impl(self, start_key: bytes | None, end_key: bytes | None):
        if start_key is not None and end_key is not None and start_key > end_key:
            return

        self._maybe_refresh_sstables()

        sources = [reader.scan(start_key, end_key) for reader in self._sstables]  # already oldest -> newest
        sources.append(self._memtable.items_in_range(start_key, end_key))  # newest source, always last

        for key, value in k_way_merge_newest_wins(sources):
            if value is not TOMBSTONE:
                yield key, value

    def close(self) -> None:
        self.stop_background_compaction()
        self._wal.close()

    # -- background compaction -----------------------------------------

    def start_background_compaction(
        self,
        trigger_count: int | None = None,
        poll_interval: float | None = None,
    ) -> None:
        """
        Spawn compaction as a separate multiprocessing.Process (see
        compaction.py for why not a thread). Safe to call at most once
        per DB instance - a second call is a no-op while one is
        already running.
        """
        if self._compaction_process is not None:
            return
        from compaction import COMPACTION_TRIGGER_COUNT, POLL_INTERVAL_SECONDS

        args = (
            str(self._dir),
            trigger_count if trigger_count is not None else COMPACTION_TRIGGER_COUNT,
            poll_interval if poll_interval is not None else POLL_INTERVAL_SECONDS,
        )
        self._compaction_process = multiprocessing.Process(
            target=compaction_worker_loop, args=args, daemon=True
        )
        self._compaction_process.start()

    def stop_background_compaction(self, timeout: float = 2.0) -> None:
        if self._compaction_process is None:
            return
        self._compaction_process.terminate()
        self._compaction_process.join(timeout)
        self._compaction_process = None

    # -- flush -----------------------------------------------------------

    def _maybe_flush(self) -> None:
        if self._memtable.is_full(self._memtable_max_bytes):
            self._flush()

    def _flush(self) -> None:
        """
        Freeze the active memtable, write it to a brand new SSTable
        file, publish it via the manifest (under the cross-process
        lock, so this can never race a concurrent compaction swap),
        then start clean: fresh empty memtable, fresh empty WAL.
        """
        frozen = self._memtable
        self._memtable = Memtable()

        sst_path = self._dir / f"{self._next_seq:06d}.sst"
        self._next_seq += 1
        write_sstable(sst_path, frozen.items())
        new_name = sst_path.name

        new_manifest = update_manifest(self._dir, lambda cur: cur + [new_name])
        self._reconcile_sstables(new_manifest)
        self._manifest_mtime = self._current_manifest_mtime()

        self._wal.reset()

    # -- misc --------------------------------------------------------

    @staticmethod
    def _validate_bytes(value: object, name: str) -> None:
        if not isinstance(value, (bytes, bytearray)):
            raise TypeError(f"{name} must be bytes, got {type(value).__name__}")

    @staticmethod
    def _validate_optional_bytes(value: object, name: str) -> None:
        if value is not None and not isinstance(value, (bytes, bytearray)):
            raise TypeError(f"{name} must be bytes or None, got {type(value).__name__}")
