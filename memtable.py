"""
Milestone 3: Memtable.

Wraps sortedcontainers.SortedDict so keys always iterate in sorted
order — required for writing a sorted SSTable on flush, and later
(Milestone 6) for k-way merged range scans.

Deletes are stored as an explicit TOMBSTONE sentinel rather than
removed outright. This starts to matter the moment data can live in
more than one place (memtable + SSTables on disk): if delete() just
removed the key from the memtable, a get() for that key could
"resurrect" an older value still sitting in an already-flushed
SSTable, because there'd be nothing in the memtable to say "no,
this was deleted after that." The tombstone is what shadows it.
"""

from __future__ import annotations

from sortedcontainers import SortedDict

# A unique sentinel object - never equal to any real bytes value,
# so it can't be confused with a legitimate (possibly empty) value.
TOMBSTONE = object()


class Memtable:
    def __init__(self) -> None:
        self._data: SortedDict = SortedDict()
        self._size_bytes = 0

    def put(self, key: bytes, value: bytes) -> None:
        self._set(key, value)

    def delete(self, key: bytes) -> None:
        self._set(key, TOMBSTONE)

    def _set(self, key: bytes, value) -> None:
        if key in self._data:
            self._size_bytes -= len(key) + self._value_len(self._data[key])
        self._data[key] = value
        self._size_bytes += len(key) + self._value_len(value)

    @staticmethod
    def _value_len(value) -> int:
        return 0 if value is TOMBSTONE else len(value)

    def get(self, key: bytes):
        """
        Returns one of:
          - bytes:      the live value
          - TOMBSTONE:  key was explicitly deleted in this memtable
          - None:       key isn't present in this memtable at all

        Callers must treat TOMBSTONE and None differently: TOMBSTONE
        means "stop looking, this key is deleted"; None means "not
        found here, keep checking older SSTables."
        """
        return self._data.get(key)

    def size_bytes(self) -> int:
        return self._size_bytes

    def is_full(self, max_bytes: int) -> bool:
        return self._size_bytes >= max_bytes

    def items(self):
        """Sorted (key, value_or_TOMBSTONE) pairs — flush input."""
        return self._data.items()

    def items_in_range(self, start_key: bytes | None = None, end_key: bytes | None = None):
        """
        Sorted (key, value_or_TOMBSTONE) pairs with start_key <= key <=
        end_key (either bound optional/unbounded if None). Used by
        Milestone 6's range scans.

        Returns a materialized LIST, not a lazy iterator - deliberately.
        The memtable is mutable (more put()/delete() calls can land in
        it at any moment), so handing back a live view tied to the
        underlying SortedDict risks "dictionary changed size during
        iteration" if the caller does more writes before finishing
        consuming a scan. SSTables, by contrast, are immutable once
        written, so SSTableReader.scan() (sstable.py) stays lazy - only
        this in-memory piece needs the eager snapshot.

        Uses SortedDict.irange(), which walks straight to `start_key`
        via the underlying sorted structure rather than scanning every
        key in the memtable - same "don't do more work than the range
        needs" spirit as the SSTable sparse index.
        """
        return [(key, self._data[key]) for key in self._data.irange(start_key, end_key)]

    def __len__(self) -> int:
        return len(self._data)
