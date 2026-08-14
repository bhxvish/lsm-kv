from memtable import TOMBSTONE, Memtable


def test_put_then_get():
    m = Memtable()
    m.put(b"key1", b"value1")
    assert m.get(b"key1") == b"value1"


def test_get_missing_key_returns_none():
    m = Memtable()
    assert m.get(b"nope") is None


def test_delete_stores_tombstone_not_removal():
    m = Memtable()
    m.put(b"key1", b"value1")
    m.delete(b"key1")
    # Must be exactly TOMBSTONE, not None - this is what lets DB.get()
    # distinguish "deleted" from "never in this memtable at all".
    assert m.get(b"key1") is TOMBSTONE


def test_items_are_sorted():
    m = Memtable()
    for k in [b"charlie", b"alpha", b"bravo"]:
        m.put(k, b"v")
    assert [k for k, _ in m.items()] == [b"alpha", b"bravo", b"charlie"]


def test_size_bytes_tracks_key_and_value_length():
    m = Memtable()
    assert m.size_bytes() == 0
    m.put(b"key1", b"value1")  # 4 + 6 = 10
    assert m.size_bytes() == 10


def test_size_bytes_accounts_for_overwrite_not_double_counted():
    m = Memtable()
    m.put(b"key1", b"aaaaaaaaaa")  # 4 + 10 = 14
    m.put(b"key1", b"bb")  # should REPLACE, not add: 4 + 2 = 6
    assert m.size_bytes() == 6


def test_size_bytes_shrinks_on_delete_of_existing_key():
    m = Memtable()
    m.put(b"key1", b"value1")  # 10 bytes
    size_before = m.size_bytes()
    m.delete(b"key1")  # tombstone value contributes 0 bytes
    assert m.size_bytes() == size_before - len(b"value1")


def test_is_full():
    m = Memtable()
    m.put(b"key1", b"value1")  # 10 bytes
    assert not m.is_full(100)
    assert m.is_full(10)
    assert m.is_full(5)
