"""
Milestone 4: sparse index for an SSTable.

While writing an SSTable, we record the on-disk byte offset of every
Nth key instead of every key. That keeps the index small enough to
comfortably hold entirely in memory even for a huge SSTable, while
still letting reads jump straight to roughly the right spot with a
single `seek()` instead of scanning the whole file from byte 0.

Because the file is sorted, "find the offset of the largest indexed
key <= target_key" is always a safe place to start scanning forward
from — the real key, if present, can only be at or after that point,
and we'll never scan more than (INTERVAL - 1) extra records past it.
"""

from __future__ import annotations

import bisect
import struct

INTERVAL = 16  # record every 16th key

# key_len (4 bytes), byte_offset (8 bytes, since files can exceed 4GB)
ENTRY_HEADER_FORMAT = ">IQ"
ENTRY_HEADER_SIZE = struct.calcsize(ENTRY_HEADER_FORMAT)


class SparseIndex:
    def __init__(self) -> None:
        self._entries: list[tuple[bytes, int]] = []  # sorted by key, by construction

    def maybe_add(self, key: bytes, offset: int, position: int) -> None:
        """Call this for every record while writing; it decides whether this one gets indexed."""
        if position % INTERVAL == 0:
            self._entries.append((key, offset))

    def find_seek_start(self, target_key: bytes) -> int:
        """Byte offset to start scanning from for `target_key`. 0 if target_key precedes every indexed key."""
        keys = [k for k, _ in self._entries]
        i = bisect.bisect_right(keys, target_key) - 1
        return self._entries[i][1] if i >= 0 else 0

    def to_bytes(self) -> bytes:
        out = bytearray()
        for key, offset in self._entries:
            out += struct.pack(ENTRY_HEADER_FORMAT, len(key), offset)
            out += key
        return bytes(out)

    @classmethod
    def from_bytes(cls, data: bytes) -> "SparseIndex":
        idx = cls()
        pos = 0
        while pos < len(data):
            key_len, offset = struct.unpack_from(ENTRY_HEADER_FORMAT, data, pos)
            pos += ENTRY_HEADER_SIZE
            key = data[pos : pos + key_len]
            pos += key_len
            idx._entries.append((key, offset))
        return idx
