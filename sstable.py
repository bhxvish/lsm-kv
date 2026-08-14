"""
Milestone 4: SSTable read path - sparse index + Bloom filter.

Milestone 3's `get_from_sstable()` did a full linear scan of the file
for every single lookup. That's kept below as `naive_get_from_sstable`
specifically so the benchmark in benchmarks/bloom_filter_benchmark.py
has a genuine "before" to compare against - it's not used by DB
anymore.

The real read path is now `SSTableReader`:
  - loads the sparse index and Bloom filter ONCE, when the SSTable is
    opened (at DB startup or right after a flush) - not on every get()
  - a lookup first asks the Bloom filter "could this key be here?";
    if it says no, we're done: zero disk I/O for that file
  - otherwise we ask the sparse index where to start, seek() there,
    and scan forward only until we pass where the key would sort

Still reuses the same [op][key_len][value_len][key][value] record
format as the WAL - no reason to change it now.
"""

from __future__ import annotations

import os
import struct
from pathlib import Path
from typing import Iterable

from bloom import BloomFilter
from memtable import TOMBSTONE
from sparse_index import SparseIndex
from wal import HEADER_FORMAT, HEADER_SIZE, OP_DELETE, OP_PUT


def _sidecar_path(sst_path: Path, extra_suffix: str) -> Path:
    """e.g. _sidecar_path(Path('000000.sst'), '.index') -> Path('000000.sst.index')"""
    return sst_path.with_suffix(sst_path.suffix + extra_suffix)


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """
    Write `data` to a temp file, fsync it, then atomically rename it
    into place. Fsyncs the same handle we wrote with — reopening the
    file read-only just to fsync it works on Linux/macOS but raises
    "Bad file descriptor" on Windows, since Windows requires the
    handle doing the fsync to have been opened for writing.
    """
    tmp_path = _sidecar_path(path, ".tmp")
    with open(tmp_path, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)  # os.replace: portably atomic even if `path` already exists (see manifest.py)


def write_sstable(path: str | Path, sorted_items: Iterable[tuple[bytes, object]]) -> None:
    """
    Write a new SSTable file (+ its .index and .bloom sidecar files)
    from an already-sorted sequence of (key, value_or_TOMBSTONE) pairs
    - exactly what Memtable.items() produces.

    All three files are written to temp paths and fsync'd, then
    atomically renamed into place - a reader can never observe a
    half-written .sst, .index, or .bloom file under its real name.
    """
    path = Path(path)
    items = list(sorted_items)  # need the count upfront to size the Bloom filter

    index = SparseIndex()
    bloom = BloomFilter.for_capacity(len(items))

    tmp_path = _sidecar_path(path, ".tmp")
    with open(tmp_path, "wb") as f:
        offset = 0
        for position, (key, value) in enumerate(items):
            index.maybe_add(key, offset, position)
            bloom.add(key)  # tombstones get added too - a delete must still be "findable"

            if value is TOMBSTONE:
                op, value_bytes = OP_DELETE, b""
            else:
                op, value_bytes = OP_PUT, value

            record = struct.pack(HEADER_FORMAT, op, len(key), len(value_bytes)) + key + value_bytes
            f.write(record)
            offset += len(record)

        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_path, path)  # os.replace: portably atomic even if `path` already exists (see manifest.py)
    _atomic_write_bytes(_sidecar_path(path, ".index"), index.to_bytes())
    _atomic_write_bytes(_sidecar_path(path, ".bloom"), bloom.to_bytes())


def read_sstable(path: str | Path) -> list[tuple[bytes, object]]:
    """Full scan: every (key, value_or_TOMBSTONE) record, in on-disk (sorted) order. Used by compaction (M5) and range scans (M6), not point lookups."""
    records: list[tuple[bytes, object]] = []
    with open(path, "rb") as f:
        while True:
            header = f.read(HEADER_SIZE)
            if len(header) < HEADER_SIZE:
                break
            op, key_len, value_len = struct.unpack(HEADER_FORMAT, header)
            key = f.read(key_len)
            value_bytes = f.read(value_len)
            value = TOMBSTONE if op == OP_DELETE else value_bytes
            records.append((key, value))
    return records


def naive_get_from_sstable(path: str | Path, target_key: bytes):
    """
    Milestone 3's point lookup: full linear scan, no index, no Bloom
    filter. Kept only as the "before" baseline for the Milestone 4
    benchmark - DB no longer calls this.
    """
    with open(path, "rb") as f:
        while True:
            header = f.read(HEADER_SIZE)
            if len(header) < HEADER_SIZE:
                break
            op, key_len, value_len = struct.unpack(HEADER_FORMAT, header)
            key = f.read(key_len)
            value_bytes = f.read(value_len)
            if key == target_key:
                return TOMBSTONE if op == OP_DELETE else value_bytes
    return None


class SSTableReader:
    """
    Opens an existing SSTable for reading. Loads its sparse index and
    Bloom filter into memory once, at construction time, so repeated
    get() calls pay for neither a fresh disk read of the sidecar files
    nor a re-parse of them - only the (usually skipped) seek into the
    data file itself.
    """

    def __init__(self, sst_path: str | Path) -> None:
        self.path = Path(sst_path)
        self.index = SparseIndex.from_bytes(_sidecar_path(self.path, ".index").read_bytes())
        self.bloom = BloomFilter.from_bytes(_sidecar_path(self.path, ".bloom").read_bytes())

    def get(self, target_key: bytes, use_bloom: bool = True):
        """
        Returns bytes (live value), TOMBSTONE (deleted here), or None
        (not present in this file - caller should keep checking older
        SSTables).

        `use_bloom` defaults to True; it exists as a parameter purely
        so the benchmark can measure the same code path with the
        Bloom filter check disabled, isolating its contribution from
        the sparse index's.
        """
        if use_bloom and not self.bloom.might_contain(target_key):
            return None  # definitely not here - no file I/O at all

        start_offset = self.index.find_seek_start(target_key)
        with open(self.path, "rb") as f:
            f.seek(start_offset)
            while True:
                header = f.read(HEADER_SIZE)
                if len(header) < HEADER_SIZE:
                    break
                op, key_len, value_len = struct.unpack(HEADER_FORMAT, header)
                key = f.read(key_len)
                value_bytes = f.read(value_len)
                if key == target_key:
                    return TOMBSTONE if op == OP_DELETE else value_bytes
                if key > target_key:
                    break  # sorted file: we've passed where it would be
        return None

    def scan(self, start_key: bytes | None = None, end_key: bytes | None = None):
        """
        Milestone 6: yields (key, value_or_TOMBSTONE) pairs from this
        file with start_key <= key <= end_key (either bound optional).

        Doesn't touch the Bloom filter - a Bloom filter only answers
        "is this ONE exact key maybe here", which isn't a meaningful
        question for a range of keys. Instead this uses the same
        sparse index as get(): seek near start_key (or the start of
        the file if unbounded), then stream forward, stopping the
        instant we pass end_key - so a narrow range against a huge
        SSTable still only reads the part of the file that matters,
        not the whole thing.

        Safe to keep lazy (unlike Memtable.items_in_range): an SSTable
        file is immutable from the moment write_sstable() renames it
        into place, so nothing can change underneath a caller who
        holds this generator open across other operations.
        """
        start_offset = self.index.find_seek_start(start_key) if start_key is not None else 0
        with open(self.path, "rb") as f:
            f.seek(start_offset)
            while True:
                header = f.read(HEADER_SIZE)
                if len(header) < HEADER_SIZE:
                    break
                op, key_len, value_len = struct.unpack(HEADER_FORMAT, header)
                key = f.read(key_len)
                value_bytes = f.read(value_len)
                if start_key is not None and key < start_key:
                    # The sparse index only gets us CLOSE to start_key
                    # (within one index interval), not exactly there.
                    continue
                if end_key is not None and key > end_key:
                    break  # sorted file: nothing further can be in range
                value = TOMBSTONE if op == OP_DELETE else value_bytes
                yield key, value
