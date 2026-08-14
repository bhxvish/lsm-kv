"""
Milestone 7: the naive baseline the benchmark suite compares against.

Deliberately the simplest thing that could be called a "persistent
key-value store": every put/delete is appended to ONE file, forever,
with no memtable, no sorting, no SSTables, no Bloom filter, no sparse
index, no compaction. A read scans the ENTIRE file from the start,
every single time, keeping track of the last (i.e. newest) matching
record it saw - since it's append-only, "last in the file" always
means "most recent write."

This exists purely so the benchmark numbers can show, concretely,
what Milestones 3-5 actually bought: the naive version's reads get
linearly slower as the file grows; the real DB's don't.
"""

from __future__ import annotations

import os
import struct
from pathlib import Path

HEADER_FORMAT = ">BII"  # op (1=put, 2=delete), key_len, value_len - same shape as wal.py's format
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
OP_PUT = 1
OP_DELETE = 2


class NaiveKV:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._file = open(self.path, "ab")

    def put(self, key: bytes, value: bytes) -> None:
        self._append(OP_PUT, key, value)

    def delete(self, key: bytes) -> None:
        self._append(OP_DELETE, key, b"")

    def _append(self, op: int, key: bytes, value: bytes) -> None:
        header = struct.pack(HEADER_FORMAT, op, len(key), len(value))
        self._file.write(header)
        self._file.write(key)
        self._file.write(value)
        self._file.flush()
        os.fsync(self._file.fileno())  # same durability guarantee as the real WAL, for a fair comparison

    def get(self, key: bytes) -> bytes | None:
        """O(file size), every call, on purpose - this is the entire point of the comparison."""
        result = None
        with open(self.path, "rb") as f:
            while True:
                header = f.read(HEADER_SIZE)
                if len(header) < HEADER_SIZE:
                    break
                op, key_len, value_len = struct.unpack(HEADER_FORMAT, header)
                k = f.read(key_len)
                v = f.read(value_len)
                if k == key:
                    result = None if op == OP_DELETE else v  # keep going: a LATER record could still overwrite this
        return result

    def close(self) -> None:
        self._file.close()
