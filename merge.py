"""
A streaming k-way merge across several already-sorted (key, value)
sequences, deduplicating so that when the same key appears in more
than one source, only the value from the NEWEST source survives.

Used by:
  - compaction (Milestone 5): merging several SSTables into one
  - range scans (Milestone 6): merging the memtable + all SSTables

`heapq.merge` merges pre-sorted iterables but doesn't dedupe - if two
sources both have key `b"x"`, it just yields both, in whichever order
they compare. We tag each item with its source's index and merge on
(key, source_index) so that for any run of equal keys, they arrive in
ascending source_index order - meaning the LAST one in the run is
always the newest source's value. That's what lets us dedupe with a
single forward pass instead of buffering.
"""

from __future__ import annotations

import heapq
from typing import Iterable, Iterator


def k_way_merge_newest_wins(
    sources: list[Iterable[tuple[bytes, object]]],
) -> Iterator[tuple[bytes, object]]:
    """
    `sources`: sorted-by-key sequences, ordered OLDEST (index 0) to
    NEWEST (last index) - e.g. SSTables in the order they were
    flushed, or [SSTable_0, SSTable_1, ..., memtable] for a read path
    that includes the memtable as the newest "source".

    Yields (key, value) pairs in sorted order, each key exactly once,
    with the newest source's value for that key.
    """

    def tagged(source_index: int, items: Iterable[tuple[bytes, object]]):
        for key, value in items:
            yield (key, source_index, value)

    tagged_sources = [tagged(i, src) for i, src in enumerate(sources)]
    merged = heapq.merge(*tagged_sources, key=lambda item: (item[0], item[1]))

    pending_key = None
    pending_value = None
    have_pending = False

    for key, _source_index, value in merged:
        if have_pending and key != pending_key:
            yield (pending_key, pending_value)
            have_pending = False
        pending_key = key
        pending_value = value
        have_pending = True

    if have_pending:
        yield (pending_key, pending_value)
