from memtable import Memtable, TOMBSTONE


def _populate():
    m = Memtable()
    for k in [b"a", b"c", b"e", b"g", b"i"]:
        m.put(k, k + b"-value")
    return m


def test_unbounded_range_returns_everything_sorted():
    m = _populate()
    assert [k for k, _ in m.items_in_range()] == [b"a", b"c", b"e", b"g", b"i"]


def test_start_only_bound():
    m = _populate()
    assert [k for k, _ in m.items_in_range(start_key=b"e")] == [b"e", b"g", b"i"]


def test_end_only_bound():
    m = _populate()
    assert [k for k, _ in m.items_in_range(end_key=b"e")] == [b"a", b"c", b"e"]


def test_both_bounds_inclusive():
    m = _populate()
    assert [k for k, _ in m.items_in_range(b"c", b"g")] == [b"c", b"e", b"g"]


def test_bounds_between_existing_keys():
    m = _populate()
    assert [k for k, _ in m.items_in_range(b"b", b"f")] == [b"c", b"e"]


def test_range_with_no_matches_returns_empty():
    m = _populate()
    assert m.items_in_range(b"x", b"z") == []


def test_range_includes_tombstones_uninterpreted():
    """items_in_range is a low-level primitive - it's DB.scan()'s job to filter tombstones, not this."""
    m = _populate()
    m.delete(b"e")
    result = dict(m.items_in_range())
    assert result[b"e"] is TOMBSTONE


def test_returns_a_list_not_a_live_view():
    """Must be safe to keep after further mutation - see the docstring in memtable.py for why."""
    m = _populate()
    snapshot = m.items_in_range()
    m.put(b"z", b"new")
    m.delete(b"a")
    assert [k for k, _ in snapshot] == [b"a", b"c", b"e", b"g", b"i"]  # unaffected by the mutations above
