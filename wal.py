"""
Milestone 2: Write-Ahead Log (WAL).

Record format (packed with `struct`):

    [1 byte op type][4 byte key length][4 byte value length][key bytes][value bytes]

Every put/delete is appended here, flushed, and fsync'd BEFORE it is
applied in memory. That ordering is the entire point: fsync is the
actual line between "looks durable" and "is durable" — skip it and an
OS-level crash can lose acknowledged writes even though the log file
"looks" complete from Python's point of view (the OS may still be
holding the bytes in a page cache buffer, not on the physical disk).
"""

from __future__ import annotations

import os
import struct
from pathlib import Path

OP_PUT = 1
OP_DELETE = 2

# '>' = big-endian/network byte order (portable across machines).
# B = 1-byte op type, I I = two 4-byte unsigned ints (key len, value len).
HEADER_FORMAT = ">BII"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


class WAL:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        # 'ab': always append, never overwrite. Creates the file if missing.
        self._file = open(self.path, "ab")

    def append_put(self, key: bytes, value: bytes) -> None:
        self._append_record(OP_PUT, key, value)

    def append_delete(self, key: bytes) -> None:
        self._append_record(OP_DELETE, key, b"")

    def _append_record(self, op: int, key: bytes, value: bytes) -> None:
        header = struct.pack(HEADER_FORMAT, op, len(key), len(value))
        self._file.write(header)
        self._file.write(key)
        self._file.write(value)
        # flush() moves bytes from Python's buffer to the OS.
        # fsync() forces the OS to commit them from its page cache to
        # the physical disk. Skipping fsync is a common mistake that
        # silently defeats the whole point of a WAL: without it, a
        # power loss (not just a process crash) can still lose data
        # that Python and even the OS reported as "written".
        self._file.flush()
        os.fsync(self._file.fileno())

    def replay(self) -> list[tuple[int, bytes, bytes]]:
        """
        Read every complete record from disk, in order.

        Stops cleanly (does not raise) the moment it hits a record
        that isn't fully on disk — this is expected after a real
        crash, since the last record being written at the moment of
        the crash may be partially flushed. Anything after that point
        is discarded, matching what a real crash actually loses: only
        the one in-flight write, nothing already fsync'd before it.
        """
        records: list[tuple[int, bytes, bytes]] = []
        if not self.path.exists():
            return records

        with open(self.path, "rb") as f:
            while True:
                header = f.read(HEADER_SIZE)
                if len(header) < HEADER_SIZE:
                    # Clean EOF (empty read) or a torn header
                    # (fewer bytes than expected) — either way, stop.
                    break

                op, key_len, value_len = struct.unpack(HEADER_FORMAT, header)
                if op not in (OP_PUT, OP_DELETE):
                    # Header decoded to garbage - treat as corruption
                    # and stop rather than trusting bogus lengths.
                    break

                key = f.read(key_len)
                if len(key) < key_len:
                    break  # torn record: crash happened mid-key-write

                value = f.read(value_len)
                if len(value) < value_len:
                    break  # torn record: crash happened mid-value-write

                records.append((op, key, value))

        return records

    def close(self) -> None:
        self._file.close()

    def reset(self) -> None:
        """
        Discard every record currently in the log and start a fresh,
        empty one at the same path.

        Called right after a memtable flush: everything the WAL was
        holding onto is now durably represented in an immutable
        SSTable file instead, so the WAL no longer needs to replay it
        on the next startup. This is the "truncate/rotate the WAL"
        step from Milestone 3.
        """
        self._file.close()
        self._file = open(self.path, "wb")  # 'wb' truncates to empty
        self._file.close()
        self._file = open(self.path, "ab")  # back to normal append mode
