from memtable import TOMBSTONE
from sstable import SSTableReader, write_sstable


def _write(tmp_path, items):
    path = tmp_path / "000000.sst"
    write_sstable(path, items)
    return SSTableReader(path)


def test_unbounded_scan_returns_everything_in_order(tmp_path):
    items = [(f"k{i:03d}".encode(), f"v{i}".encode()) for i in range(50)]
    reader = _write(tmp_path, items)
    result = list(reader.scan())
    assert result == items


def test_scan_respects_both_bounds_inclusive(tmp_path):
    items = [(f"k{i:03d}".encode(), f"v{i}".encode()) for i in range(50)]
    reader = _write(tmp_path, items)
    result = list(reader.scan(b"k010", b"k015"))
    assert [k for k, _ in result] == [f"k{i:03d}".encode() for i in range(10, 16)]


def test_scan_start_only(tmp_path):
    items = [(f"k{i:03d}".encode(), b"v") for i in range(20)]
    reader = _write(tmp_path, items)
    result = list(reader.scan(start_key=b"k015"))
    assert [k for k, _ in result] == [f"k{i:03d}".encode() for i in range(15, 20)]


def test_scan_end_only(tmp_path):
    items = [(f"k{i:03d}".encode(), b"v") for i in range(20)]
    reader = _write(tmp_path, items)
    result = list(reader.scan(end_key=b"k004"))
    assert [k for k, _ in result] == [f"k{i:03d}".encode() for i in range(0, 5)]


def test_scan_range_spanning_many_sparse_index_intervals(tmp_path):
    # SparseIndex.INTERVAL is 16 - use a range wide enough to cross
    # several index boundaries, so this actually exercises the
    # seek-then-scan-forward logic, not just a single interval.
    items = [(f"k{i:04d}".encode(), f"v{i}".encode()) for i in range(300)]
    reader = _write(tmp_path, items)
    result = list(reader.scan(b"k0100", b"k0199"))
    assert [k for k, _ in result] == [f"k{i:04d}".encode() for i in range(100, 200)]


def test_scan_includes_tombstones_uninterpreted(tmp_path):
    """Like Memtable.items_in_range, filtering tombstones is DB.scan()'s job, not this."""
    reader = _write(tmp_path, [(b"a", b"1"), (b"b", TOMBSTONE), (b"c", b"3")])
    result = dict(reader.scan())
    assert result[b"b"] is TOMBSTONE


def test_scan_with_no_matches_returns_empty(tmp_path):
    items = [(f"k{i:03d}".encode(), b"v") for i in range(10)]
    reader = _write(tmp_path, items)
    assert list(reader.scan(b"z000", b"z999")) == []


def test_scan_on_empty_sstable(tmp_path):
    reader = _write(tmp_path, [])
    assert list(reader.scan()) == []
