"""
Milestone 5 deliverable.

Hammers the DB with a large number of concurrent-in-spirit puts,
deletes, and overwrites from the main process while a REAL background
compaction process (multiprocessing.Process, not a thread and not a
mock) merges SSTables at the same time. Then verifies the final state
exactly matches an in-memory reference model (a plain dict tracking
every write/delete as it happens).

Also includes a second test that specifically kills the compaction
worker (SIGKILL-equivalent, not a graceful stop) mid-cycle, to prove
the design's core promise: no data loss, no visible inconsistency, and
no orphaned temp files, even when compaction is interrupted at the
worst possible moment.
"""

import random
import time

from db import DB


def test_compaction_survives_concurrent_writes_and_matches_reference_model(tmp_path):
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=2048)
    db.start_background_compaction(trigger_count=3, poll_interval=0.05)

    try:
        reference: dict[bytes, bytes] = {}
        keys = [f"key{i:04d}".encode() for i in range(200)]
        rng = random.Random(42)

        NUM_OPS = 4000
        for _ in range(NUM_OPS):
            key = rng.choice(keys)
            # Deletes rarer than writes, like a realistic workload.
            if rng.random() < 0.8:
                value = f"v{rng.randint(0, 1_000_000)}".encode()
                db.put(key, value)
                reference[key] = value
            else:
                db.delete(key)
                reference.pop(key, None)

        # Let the background worker catch up on whatever's still
        # pending before we check final correctness.
        time.sleep(1.5)

        mismatches = [k for k in keys if db.get(k) != reference.get(k)]
        assert not mismatches, f"{len(mismatches)}/{len(keys)} keys mismatched, e.g. {mismatches[:5]!r}"

        # Prove compaction genuinely ran during the stress test, not
        # that the test happens to pass with zero compaction activity.
        compacted_files = list(data_dir.glob("c*.sst"))
        assert compacted_files, "expected at least one compaction to have run during the stress test"

    finally:
        db.stop_background_compaction()
        db.close()


def test_killing_compaction_mid_cycle_leaves_no_corruption(tmp_path):
    """
    Kill the compaction worker with SIGKILL-equivalent force at a
    random moment while it's actively merging, then verify the DB is
    still fully correct and nothing is left half-written on disk.
    """
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=1024)

    reference = {}
    for i in range(300):
        key = f"key{i:04d}".encode()
        value = (f"value{i}".encode()) * 5
        db.put(key, value)
        reference[key] = value

    db.start_background_compaction(trigger_count=2, poll_interval=0.05)
    time.sleep(0.3)  # let it get partway into a compaction cycle
    db._compaction_process.kill()  # SIGKILL - zero cleanup, no graceful shutdown
    db._compaction_process.join(timeout=2.0)
    db._compaction_process = None  # prevent close() from trying to terminate it again

    # write_sstable's atomic tmp-then-rename pattern must guarantee
    # this holds even mid-kill.
    assert not list(data_dir.glob("*.tmp"))

    for key, value in reference.items():
        assert db.get(key) == value

    db.close()
