from memtable import TOMBSTONE
from sstable import SSTableReader, write_sstable


def _write(tmp_path, name, items):
    path = tmp_path / name
    write_sstable(path, items)
    return path


def test_reader_finds_existing_key(tmp_path):
    path = _write(tmp_path, "000000.sst", [(b"a", b"1"), (b"m", b"middle"), (b"z", b"9")])
    reader = SSTableReader(path)
    assert reader.get(b"m") == b"middle"


def test_reader_returns_none_for_absent_key(tmp_path):
    path = _write(tmp_path, "000000.sst", [(b"a", b"1")])
    reader = SSTableReader(path)
    assert reader.get(b"never-written") is None


def test_reader_returns_tombstone_for_deleted_key(tmp_path):
    path = _write(tmp_path, "000000.sst", [(b"a", TOMBSTONE)])
    reader = SSTableReader(path)
    assert reader.get(b"a") is TOMBSTONE


def test_reader_correct_across_many_keys_spanning_multiple_index_intervals(tmp_path):
    # SparseIndex.INTERVAL is 16, so 200 keys forces many index jumps -
    # every one of them must still resolve to the right record.
    items = [(f"key{i:04d}".encode(), f"value{i}".encode()) for i in range(200)]
    path = _write(tmp_path, "000000.sst", items)
    reader = SSTableReader(path)

    for i in range(200):
        assert reader.get(f"key{i:04d}".encode()) == f"value{i}".encode()
    assert reader.get(b"key9999") is None


def test_reader_bloom_filter_can_be_disabled_for_comparison(tmp_path):
    """
    use_bloom=False must give the SAME correctness result as the
    default - it only exists to let the benchmark isolate the Bloom
    filter's contribution, it must never change what a lookup returns.
    """
    items = [(f"key{i:04d}".encode(), f"value{i}".encode()) for i in range(50)]
    path = _write(tmp_path, "000000.sst", items)
    reader = SSTableReader(path)

    for i in range(50):
        key = f"key{i:04d}".encode()
        assert reader.get(key, use_bloom=True) == reader.get(key, use_bloom=False)
    assert reader.get(b"absent", use_bloom=True) == reader.get(b"absent", use_bloom=False) is None


def test_reader_loads_index_and_bloom_only_once(tmp_path):
    """Sanity check that repeated get() calls don't re-read the sidecar files from disk."""
    path = _write(tmp_path, "000000.sst", [(b"a", b"1")])
    reader = SSTableReader(path)
    index_before = reader.index
    bloom_before = reader.bloom

    for _ in range(100):
        reader.get(b"a")

    assert reader.index is index_before
    assert reader.bloom is bloom_before
