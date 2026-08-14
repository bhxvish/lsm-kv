from compaction import run_one_compaction_cycle
from db import DB
from manifest import read_manifest


def test_compaction_merges_dedupes_and_drops_tombstones(tmp_path):
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=50)

    # Spread writes across several small flushes on purpose.
    db.put(b"a", b"1")
    db.put(b"b", b"x" * 60)
    db.put(b"c", b"x" * 60)
    db.put(b"a", b"1-updated" * 5)  # overwrite, lands in a LATER sstable than the original "a"
    db.delete(b"b")  # tombstone, lands in a later sstable than the original "b"
    db.put(b"d", b"x" * 60)
    db.put(b"e", b"x" * 60)
    db.close()  # no need for a running compaction worker for this test

    manifest_before = read_manifest(data_dir)
    assert len(manifest_before) >= 2, "test setup should have forced multiple flushes"

    happened = run_one_compaction_cycle(data_dir, trigger_count=1)
    assert happened is True

    manifest_after = read_manifest(data_dir)
    assert len(manifest_after) == 1
    assert manifest_after[0].startswith("c"), "compacted file should use the c-prefixed namespace"

    # Reopen fresh - proves the compacted state is correctly readable
    # from disk, not just coincidentally still right in memory.
    db2 = DB(data_dir, memtable_max_bytes=200)
    assert db2.get(b"a") == b"1-updated" * 5  # newest value survives
    assert db2.get(b"b") is None  # tombstone applied, then correctly dropped
    assert db2.get(b"c") == b"x" * 60
    assert db2.get(b"d") == b"x" * 60
    assert db2.get(b"e") == b"x" * 60
    db2.close()


def test_compaction_leaves_no_tmp_files_or_orphans(tmp_path):
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=100)
    for i in range(20):
        db.put(f"key{i}".encode(), f"value{i}".encode() * 5)
    db.close()

    run_one_compaction_cycle(data_dir, trigger_count=1)

    assert not list(data_dir.glob("*.tmp"))
    # Every file still on disk must be referenced by the manifest -
    # nothing orphaned once a compaction cycle completes cleanly.
    manifest = set(read_manifest(data_dir))
    on_disk_sst = {p.name for p in data_dir.glob("*.sst")}
    assert on_disk_sst == manifest


def test_compaction_is_a_noop_below_the_trigger_threshold(tmp_path):
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=100)
    db.put(b"a", b"1")
    db.close()

    manifest_before = read_manifest(data_dir)
    happened = run_one_compaction_cycle(data_dir, trigger_count=999)
    assert happened is False
    assert read_manifest(data_dir) == manifest_before


def test_repeated_compaction_cycles_keep_converging_to_one_file(tmp_path):
    data_dir = tmp_path / "data"
    db = DB(data_dir, memtable_max_bytes=100)
    for round_num in range(3):
        for i in range(6):
            db.put(f"r{round_num}k{i}".encode(), f"v{i}".encode() * 10)
        run_one_compaction_cycle(data_dir, trigger_count=2)
    db.close()

    while run_one_compaction_cycle(data_dir, trigger_count=2):
        pass

    manifest = read_manifest(data_dir)
    assert len(manifest) == 1

    db2 = DB(data_dir, memtable_max_bytes=100)
    for round_num in range(3):
        for i in range(6):
            assert db2.get(f"r{round_num}k{i}".encode()) == f"v{i}".encode() * 10
    db2.close()
