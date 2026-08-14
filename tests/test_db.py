"""
Milestone 1 test suite.

This exact suite gets rerun after every later milestone (WAL, memtable,
SSTable flush, compaction, ...) to catch regressions — the public
behavior of DB should never change even as the internals get replaced.
"""

import pytest

from db import DB


@pytest.fixture
def db(tmp_path) -> DB:
    # Each test gets its own throwaway data directory so tests never interfere.
    return DB(tmp_path / "data")


def test_put_then_get_returns_value(db: DB) -> None:
    db.put(b"key1", b"value1")
    assert db.get(b"key1") == b"value1"


def test_get_missing_key_returns_none(db: DB) -> None:
    assert db.get(b"does-not-exist") is None


def test_overwrite_replaces_value(db: DB) -> None:
    db.put(b"key1", b"value1")
    db.put(b"key1", b"value2")
    assert db.get(b"key1") == b"value2"


def test_delete_removes_key(db: DB) -> None:
    db.put(b"key1", b"value1")
    db.delete(b"key1")
    assert db.get(b"key1") is None


def test_delete_missing_key_is_noop(db: DB) -> None:
    # Should not raise.
    db.delete(b"never-existed")
    assert db.get(b"never-existed") is None


def test_multiple_keys_independent(db: DB) -> None:
    db.put(b"a", b"1")
    db.put(b"b", b"2")
    db.delete(b"a")
    assert db.get(b"a") is None
    assert db.get(b"b") == b"2"


def test_empty_value_is_valid(db: DB) -> None:
    # Distinguish "empty value" from "missing key" — this matters a lot
    # once tombstones enter the picture in later milestones.
    db.put(b"key1", b"")
    assert db.get(b"key1") == b""


def test_put_rejects_non_bytes_key(db: DB) -> None:
    with pytest.raises(TypeError):
        db.put("not-bytes", b"value1")  # type: ignore[arg-type]


def test_put_rejects_non_bytes_value(db: DB) -> None:
    with pytest.raises(TypeError):
        db.put(b"key1", "not-bytes")  # type: ignore[arg-type]
