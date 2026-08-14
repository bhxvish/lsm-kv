"""
Milestone 4 deliverable: measure the actual difference the Bloom
filter makes to read latency, with enough SSTables on disk that the
effect is real and visible.

Isolating specifically the Bloom filter's contribution (not the
sparse index's, which both paths use) is the point: every SSTableReader.get()
call below goes through the exact same seek-based sparse-index code
path. The ONLY thing that changes between the two benchmark runs is
`use_bloom`. So any latency difference we measure is attributable
to the Bloom filter itself, not to some other optimization.

Setup: write enough data to force many separate SSTables, then query
for keys that were NEVER written. This is deliberately the case where
a Bloom filter earns its keep the most: for a truly absent key, every
single SSTable has to be checked, and with the Bloom filter, almost
all of those checks cost zero disk I/O; without it, every SSTable
still gets seeked into and scanned near where the key would sort.
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

from db import DB

DATA_DIR = Path(__file__).parent / "_bench_data"
NUM_KEYS = 6000
MEMTABLE_MAX_BYTES = 8 * 1024  # tiny on purpose, to force many flushes -> many SSTables
NUM_QUERIES = 500
VALUE = b"v" * 100


def build_db_with_many_sstables() -> DB:
    shutil.rmtree(DATA_DIR, ignore_errors=True)
    db = DB(DATA_DIR, memtable_max_bytes=MEMTABLE_MAX_BYTES)
    for i in range(NUM_KEYS):
        db.put(f"key{i:06d}".encode(), VALUE)
    return db


def time_lookups(db: DB, keys: list[bytes], use_bloom: bool) -> float:
    start = time.perf_counter()
    for key in keys:
        # Same code as DB.get()'s SSTable loop, but with use_bloom
        # exposed so we can flip it for comparison.
        for reader in reversed(db._sstables):
            result = reader.get(key, use_bloom=use_bloom)
            if result is not None:
                break
    return time.perf_counter() - start


def main() -> None:
    db = build_db_with_many_sstables()
    num_sstables = len(db._sstables)
    print(f"Built {num_sstables} SSTables from {NUM_KEYS} keys "
          f"({MEMTABLE_MAX_BYTES} byte memtable threshold).\n")

    # Keys guaranteed to not exist in ANY SSTable - the worst case
    # (and the case a Bloom filter is specifically built for).
    missing_keys = [f"missing{i:06d}".encode() for i in range(NUM_QUERIES)]

    # Warm up the filesystem cache identically for both runs so we're
    # comparing the algorithms, not cold-cache disk effects.
    time_lookups(db, missing_keys, use_bloom=True)

    without_bloom_s = time_lookups(db, missing_keys, use_bloom=False)
    with_bloom_s = time_lookups(db, missing_keys, use_bloom=True)

    without_us = (without_bloom_s / NUM_QUERIES) * 1_000_000
    with_us = (with_bloom_s / NUM_QUERIES) * 1_000_000
    speedup = without_bloom_s / with_bloom_s if with_bloom_s > 0 else float("inf")

    print(f"Queries for {NUM_QUERIES} keys guaranteed absent from all {num_sstables} SSTables:\n")
    print(f"  {'Without Bloom filter:':<24} {without_us:8.2f} us/lookup   (total {without_bloom_s*1000:.1f} ms)")
    print(f"  {'With Bloom filter:':<24} {with_us:8.2f} us/lookup   (total {with_bloom_s*1000:.1f} ms)")
    print(f"\n  Speedup: {speedup:.1f}x")

    shutil.rmtree(DATA_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()
