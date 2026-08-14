from memtable import TOMBSTONE
from sstable import naive_get_from_sstable, read_sstable, write_sstable


def test_write_then_read_sstable_roundtrip(tmp_path):
    path = tmp_path / "000000.sst"
    write_sstable(path, [(b"a", b"1"), (b"b", b"2"), (b"c", TOMBSTONE)])

    records = read_sstable(path)
    assert records == [(b"a", b"1"), (b"b", b"2"), (b"c", TOMBSTONE)]


def test_write_sstable_leaves_no_tmp_files_behind(tmp_path):
    path = tmp_path / "000000.sst"
    write_sstable(path, [(b"a", b"1")])

    assert path.exists()
    assert (tmp_path / "000000.sst.index").exists()
    assert (tmp_path / "000000.sst.bloom").exists()
    assert not list(tmp_path.glob("*.tmp"))


def test_naive_get_from_sstable_finds_existing_key(tmp_path):
    path = tmp_path / "000000.sst"
    write_sstable(path, [(b"a", b"1"), (b"b", b"2")])
    assert naive_get_from_sstable(path, b"b") == b"2"


def test_naive_get_from_sstable_returns_tombstone_for_deleted_key(tmp_path):
    path = tmp_path / "000000.sst"
    write_sstable(path, [(b"a", TOMBSTONE)])
    assert naive_get_from_sstable(path, b"a") is TOMBSTONE


def test_naive_get_from_sstable_returns_none_for_absent_key(tmp_path):
    path = tmp_path / "000000.sst"
    write_sstable(path, [(b"a", b"1")])
    assert naive_get_from_sstable(path, b"never-written") is None
