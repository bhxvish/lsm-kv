"""
Milestone 6 deliverable: a range query that correctly merges results
across the active memtable AND multiple on-disk SSTables - including
the tricky cases of an overwrite and a delete that each straddle a
flush boundary, which is exactly where a naive implementation (e.g.
one that just checked SSTables without giving the memtable priority)
would return stale or resurrected data.
"""

import pytest

from db import DB


def _kb(n: int) -> bytes:
    return b"x" * n


def test_scan_across_memtable_and_multiple_sstables_matches_reference(tmp_path):
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=1024)

    reference = {}
    for i in range(200):
        key = f"key{i:04d}".encode()
        value = f"value{i}".encode()
        db.put(key, value)
        reference[key] = value

    assert list(data_dir.glob("*.sst")), "test setup should have forced at least one flush"

    result = dict(db.scan())
    assert result == reference


def test_scan_bounded_range_matches_reference_subset(tmp_path):
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=512)

    reference = {}
    for i in range(150):
        key = f"key{i:04d}".encode()
        value = f"value{i}".encode()
        db.put(key, value)
        reference[key] = value

    start, end = b"key0050", b"key0099"
    expected = {k: v for k, v in reference.items() if start <= k <= end}

    result = dict(db.scan(start, end))
    assert result == expected
    assert len(result) == 50  # sanity: the range really did narrow things down


def test_scan_overwrite_across_flush_boundary_returns_newest_value(tmp_path):
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=512)

    db.put(b"target", b"original-value")
    for i in range(10):  # pad past the flush threshold
        db.put(f"pad{i:03d}".encode(), _kb(80))
    assert list(data_dir.glob("*.sst")), "expected a flush to have happened by now"

    db.put(b"target", b"updated-value")  # now only in the active memtable

    result = dict(db.scan(b"target", b"target"))
    assert result == {b"target": b"updated-value"}


def test_scan_delete_across_flush_boundary_excludes_key(tmp_path):
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=512)

    db.put(b"target", b"will-be-deleted")
    for i in range(10):
        db.put(f"pad{i:03d}".encode(), _kb(80))
    assert list(data_dir.glob("*.sst")), "expected a flush to have happened by now"

    db.delete(b"target")  # tombstone lands in the fresh active memtable

    result = dict(db.scan())
    assert b"target" not in result  # deleted keys never appear in scan results


def test_scan_unbounded_returns_all_live_keys_in_sorted_order(tmp_path):
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=256)

    keys_written = []
    for i in range(80):
        key = f"key{i:04d}".encode()
        db.put(key, b"v")
        keys_written.append(key)
    db.delete(b"key0010")
    db.delete(b"key0050")

    result_keys = [k for k, _ in db.scan()]
    expected_keys = sorted(k for k in keys_written if k not in (b"key0010", b"key0050"))
    assert result_keys == expected_keys


def test_scan_empty_range_when_start_exceeds_end(tmp_path):
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=1024)
    db.put(b"a", b"1")
    db.put(b"b", b"2")
    assert list(db.scan(b"z", b"a")) == []


def test_scan_invalid_bounds_raise_immediately_not_on_first_iteration(tmp_path):
    """
    scan() has `yield` in its implementation, which means a naive
    version would defer ALL its code - including argument validation -
    until the caller starts iterating. Validation must happen eagerly,
    the moment scan() is called, matching put()/get()'s behavior.
    """
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=1024)
    with pytest.raises(TypeError):
        db.scan(start_key="not-bytes")  # should raise here, not on first next()


def test_scan_reflects_data_after_reopening_db(tmp_path):
    data_dir = tmp_path / "data"
    db1 = DB(data_dir, memtable_max_bytes=256)
    for i in range(50):
        db1.put(f"key{i:04d}".encode(), f"value{i}".encode())
    db1.close()

    db2 = DB(data_dir, memtable_max_bytes=256)
    result = dict(db2.scan())
    assert len(result) == 50
    assert result[b"key0000"] == b"value0"
    assert result[b"key0049"] == b"value49"
