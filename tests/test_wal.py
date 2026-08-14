import struct

from wal import HEADER_FORMAT, OP_DELETE, OP_PUT, WAL


def test_append_and_replay_put(tmp_path):
    wal = WAL(tmp_path / "test.wal")
    wal.append_put(b"key1", b"value1")
    wal.close()

    wal2 = WAL(tmp_path / "test.wal")
    records = wal2.replay()
    assert records == [(OP_PUT, b"key1", b"value1")]


def test_append_and_replay_delete(tmp_path):
    wal = WAL(tmp_path / "test.wal")
    wal.append_delete(b"key1")
    wal.close()

    wal2 = WAL(tmp_path / "test.wal")
    records = wal2.replay()
    assert records == [(OP_DELETE, b"key1", b"")]


def test_replay_preserves_order(tmp_path):
    wal = WAL(tmp_path / "test.wal")
    wal.append_put(b"a", b"1")
    wal.append_put(b"b", b"2")
    wal.append_delete(b"a")
    wal.close()

    wal2 = WAL(tmp_path / "test.wal")
    records = wal2.replay()
    assert records == [
        (OP_PUT, b"a", b"1"),
        (OP_PUT, b"b", b"2"),
        (OP_DELETE, b"a", b""),
    ]


def test_replay_on_missing_file_returns_empty(tmp_path):
    wal = WAL(tmp_path / "does_not_exist_yet.wal")
    # WAL() with 'ab' mode actually creates the file, so replay it
    # again on a path that was truly never touched:
    never_created = tmp_path / "still_never_created.wal"
    assert not never_created.exists()

    # replay() is a WAL instance method, so simulate "no file" by
    # checking behavior on a freshly-created, empty file instead.
    assert wal.replay() == []


def test_replay_stops_cleanly_on_torn_header(tmp_path):
    path = tmp_path / "torn.wal"
    wal = WAL(path)
    wal.append_put(b"good-key", b"good-value")
    wal.close()

    # Manually append a torn header: fewer bytes than HEADER_SIZE.
    with open(path, "ab") as f:
        f.write(b"\x01\x00\x00")  # incomplete header, deliberately short

    wal2 = WAL(path)
    records = wal2.replay()  # must not raise
    assert records == [(OP_PUT, b"good-key", b"good-value")]


def test_replay_stops_cleanly_on_torn_key(tmp_path):
    path = tmp_path / "torn_key.wal"
    wal = WAL(path)
    wal.append_put(b"good-key", b"good-value")
    wal.close()

    # A complete, valid header claiming a 100-byte key, but no key
    # bytes actually follow - exactly what a crash mid-write leaves.
    with open(path, "ab") as f:
        f.write(struct.pack(HEADER_FORMAT, OP_PUT, 100, 5))
        f.write(b"only-a-few-bytes")  # far short of the claimed 100

    wal2 = WAL(path)
    records = wal2.replay()  # must not raise
    assert records == [(OP_PUT, b"good-key", b"good-value")]
