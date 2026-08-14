"""
Milestone 5: background compaction, run in a separate multiprocessing.Process.

Why multiprocessing and not threading: Python's GIL means regular
threads never get true parallel CPU execution, so a threading-based
compaction wouldn't be a genuine concurrency claim - it would just be
another thing occasionally getting scheduled on the same single core
as everything else. A real OS process gets its own interpreter and
can genuinely run on a different core at the same time as the main
process. The trade-off: a separate process can't share the DB
instance's Python objects directly, so unlike everything else in this
project, the compaction worker only ever talks to the data directory
through the FILESYSTEM (reading/writing .sst files and MANIFEST.json)
- never through shared memory. That's also, not coincidentally, close
to how real distributed storage services coordinate with each other.

Simplified size-tiered strategy: rather than picking an arbitrary
subset of SSTables, we always compact the WHOLE current generation at
once when its size crosses a threshold. That keeps one important
correctness question trivial: it's always safe to drop tombstones
entirely during a full compaction, because there is no older file
left outside the compacted set that could still be holding the
pre-delete value. (A partial/tiered compaction of just a subset would
have to keep tombstones instead of dropping them, in case an older,
not-yet-compacted file still has the original value.)
"""

from __future__ import annotations

import time
import traceback
from pathlib import Path

from manifest import read_manifest, update_manifest
from memtable import TOMBSTONE
from merge import k_way_merge_newest_wins
from sstable import read_sstable, write_sstable

COMPACTION_TRIGGER_COUNT = 4  # compact once this many SSTables exist
POLL_INTERVAL_SECONDS = 0.2


def _next_compacted_seq(dir_path: Path) -> int:
    existing = list(dir_path.glob("c??????.sst"))
    nums = [int(p.stem[1:]) for p in existing if p.stem[1:].isdigit()]
    return (max(nums) + 1) if nums else 0


def run_one_compaction_cycle(
    dir_path: str | Path,
    trigger_count: int = COMPACTION_TRIGGER_COUNT,
) -> bool:
    """
    Returns True if a compaction actually happened, False if there
    wasn't enough work. Split out from the polling loop below so tests
    can call it directly and deterministically, without needing to
    spin up a real subprocess or wait on a timer.
    """
    dir_path = Path(dir_path)
    current = read_manifest(dir_path)
    if current is None or len(current) < trigger_count:
        return False

    # Snapshot: because the main process only ever APPENDS to the
    # manifest (new flushes go on the end) and only compaction ever
    # REMOVES entries, `current` right now is guaranteed to still be
    # a valid, stable prefix-compatible view: nothing in it can have
    # been deleted out from under us before we get to the swap below.
    compacted_set = list(current)
    paths = [dir_path / name for name in compacted_set]

    try:
        sources = [read_sstable(p) for p in paths]
    except FileNotFoundError:
        # Only possible if something else deleted one of these files -
        # shouldn't happen with a single compaction worker, but don't
        # crash the whole background process over it.
        return False

    merged = k_way_merge_newest_wins(sources)
    live_only = [(key, value) for key, value in merged if value is not TOMBSTONE]

    next_seq = _next_compacted_seq(dir_path)
    merged_filename = f"c{next_seq:06d}.sst"
    merged_path = dir_path / merged_filename
    write_sstable(merged_path, live_only)  # atomic tmp+rename, same as a normal flush

    def _swap(manifest_list: list[str]) -> list[str]:
        if manifest_list[: len(compacted_set)] != compacted_set:
            # The front of the manifest isn't what we expected -
            # something unaccounted-for changed it. Bail without
            # modifying anything; the next cycle re-evaluates from
            # scratch. (Defensive: with a single compaction worker and
            # an append-only main process this shouldn't be reachable,
            # but "shouldn't" is not a proof.)
            return manifest_list
        return [merged_filename] + manifest_list[len(compacted_set):]

    new_manifest = update_manifest(dir_path, _swap)

    if new_manifest and new_manifest[0] == merged_filename:
        # The manifest swap is now durable on disk - ONLY NOW is it
        # safe to remove the old files. If we crash before this line,
        # the old files are untouched and still fully correct (still
        # referenced by the manifest); the only cost is one harmless
        # orphaned merged file sitting on disk unreferenced.
        for name in compacted_set:
            old_path = dir_path / name
            old_path.unlink(missing_ok=True)
            (dir_path / (name + ".index")).unlink(missing_ok=True)
            (dir_path / (name + ".bloom")).unlink(missing_ok=True)
        return True

    # Our swap didn't take (raced with something unexpected) - clean
    # up the merged file we wrote rather than leave a permanent orphan.
    merged_path.unlink(missing_ok=True)
    (dir_path / (merged_filename + ".index")).unlink(missing_ok=True)
    (dir_path / (merged_filename + ".bloom")).unlink(missing_ok=True)
    return False


def compaction_worker_loop(
    dir_path: str,
    trigger_count: int = COMPACTION_TRIGGER_COUNT,
    poll_interval: float = POLL_INTERVAL_SECONDS,
) -> None:
    """
    Entry point for the background multiprocessing.Process: wakes up
    periodically and checks whether there's enough work to justify a
    compaction pass. There's no graceful shutdown protocol - the
    parent just calls .terminate()/.kill() on the Process handle. That's
    fine specifically because compaction is idempotent and crash-safe:
    killing it mid-cycle just means the next cycle (in the next
    process lifetime, if one is started) picks up from whatever the
    manifest says is true, per run_one_compaction_cycle's guarantees.
    """
    dir_path = Path(dir_path)
    while True:
        try:
            run_one_compaction_cycle(dir_path, trigger_count=trigger_count)
        except Exception:
            # A background worker should never silently die and leave
            # nobody compacting - print and keep going. (Using print,
            # not logging, since this runs in its own process with no
            # shared logging configuration from the parent.)
            traceback.print_exc()
        time.sleep(poll_interval)
