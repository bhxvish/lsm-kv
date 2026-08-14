"""
Milestone 3 deliverable: write more data than fits in one memtable,
confirm multiple .sst files land on disk, and confirm reads are
correct regardless of whether the answer currently lives in the
active memtable or has already been flushed to a file.
"""

from db import DB


def _kb(n: int) -> bytes:
    return b"x" * n


def test_writes_beyond_threshold_produce_multiple_sstables(tmp_path):
    data_dir = tmp_path / "data"
    # Tiny threshold on purpose - real usage is 4MB, but the test
    # shouldn't need to write 4MB to prove the flush logic works.
    db = DB(data_dir, memtable_max_bytes=1024)

    # Each record is ~100 bytes; 30 of them should cross the 1KB
    # threshold more than once, forcing at least 2 flushes.
    for i in range(30):
        db.put(f"key{i:03d}".encode(), _kb(90))

    sst_files = sorted((data_dir).glob("*.sst"))
    assert len(sst_files) >= 2, f"expected multiple .sst files, got {sst_files}"


def test_reads_correct_across_memtable_and_flushed_sstables(tmp_path):
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=1024)

    written = {}
    for i in range(30):
        key = f"key{i:03d}".encode()
        value = f"value-{i}".encode() + _kb(80)
        db.put(key, value)
        written[key] = value

    # Some of these keys are now in flushed .sst files, some are still
    # in the active memtable - the caller shouldn't be able to tell
    # the difference from the outside.
    assert len(list(data_dir.glob("*.sst"))) >= 1  # sanity: a flush did happen
    for key, value in written.items():
        assert db.get(key) == value


def test_overwrite_across_flush_boundary_returns_newest_value(tmp_path):
    """
    A key written before a flush, then overwritten after the flush,
    must return the NEW value - proving the read path correctly
    checks the memtable before falling back to older SSTables.
    """
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=512)

    db.put(b"target", b"original-value")
    # Pad past the threshold so `target` gets flushed to disk.
    for i in range(10):
        db.put(f"pad{i}".encode(), _kb(80))
    assert list(data_dir.glob("*.sst")), "expected a flush to have happened by now"

    db.put(b"target", b"updated-value")  # now only in the active memtable
    assert db.get(b"target") == b"updated-value"


def test_delete_across_flush_boundary_shadows_old_sstable_value(tmp_path):
    """
    Same idea, but for delete: a tombstone written after the value was
    flushed must correctly shadow the older on-disk value.
    """
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=512)

    db.put(b"target", b"will-be-deleted")
    for i in range(10):
        db.put(f"pad{i}".encode(), _kb(80))
    assert list(data_dir.glob("*.sst")), "expected a flush to have happened by now"

    db.delete(b"target")  # tombstone lands in the fresh active memtable
    assert db.get(b"target") is None


def test_reopening_after_flush_still_finds_flushed_data(tmp_path):
    """SSTables must be rediscovered from disk on a fresh DB() call, not just kept in memory."""
    data_dir = tmp_path / "data"
    db1 = DB(data_dir, memtable_max_bytes=512)
    for i in range(10):
        db1.put(f"key{i}".encode(), _kb(80))
    assert list(data_dir.glob("*.sst"))
    db1.close()

    db2 = DB(data_dir, memtable_max_bytes=512)  # fresh process/instance
    for i in range(10):
        assert db2.get(f"key{i}".encode()) == _kb(80)
