from memtable import TOMBSTONE
from merge import k_way_merge_newest_wins


def test_single_source_passthrough():
    result = list(k_way_merge_newest_wins([[(b"a", b"1"), (b"b", b"2")]]))
    assert result == [(b"a", b"1"), (b"b", b"2")]


def test_disjoint_sources_interleaved_in_sorted_order():
    a = [(b"a", b"1"), (b"c", b"3")]
    b = [(b"b", b"2"), (b"d", b"4")]
    result = list(k_way_merge_newest_wins([a, b]))
    assert result == [(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")]


def test_duplicate_key_newest_source_wins():
    old = [(b"a", b"old-value")]
    new = [(b"a", b"new-value")]
    # old passed at index 0 = oldest, new at index 1 = newest
    result = list(k_way_merge_newest_wins([old, new]))
    assert result == [(b"a", b"new-value")]


def test_duplicate_key_order_independent_of_argument_order_semantics():
    # Swapping which index is "newest" must flip the winner - proves
    # the result depends on source ORDER (position in the list), not
    # on the values themselves or dict/set iteration order.
    a = [(b"a", b"from-a")]
    b = [(b"a", b"from-b")]
    assert list(k_way_merge_newest_wins([a, b])) == [(b"a", b"from-b")]
    assert list(k_way_merge_newest_wins([b, a])) == [(b"a", b"from-a")]


def test_tombstone_from_newest_source_wins_over_older_value():
    old = [(b"a", b"value")]
    new = [(b"a", TOMBSTONE)]
    result = list(k_way_merge_newest_wins([old, new]))
    assert result == [(b"a", TOMBSTONE)]


def test_value_from_newest_source_wins_over_older_tombstone():
    old = [(b"a", TOMBSTONE)]
    new = [(b"a", b"resurrected-by-a-later-put")]
    result = list(k_way_merge_newest_wins([old, new]))
    assert result == [(b"a", b"resurrected-by-a-later-put")]


def test_many_sources_preserve_overall_sorted_order():
    sources = [
        [(f"k{i:02d}".encode(), b"v") for i in range(0, 30, 3)],
        [(f"k{i:02d}".encode(), b"v") for i in range(1, 30, 3)],
        [(f"k{i:02d}".encode(), b"v") for i in range(2, 30, 3)],
    ]
    result = [k for k, _ in k_way_merge_newest_wins(sources)]
    assert result == sorted(result)
    assert len(result) == 30


def test_empty_sources_are_ignored():
    result = list(k_way_merge_newest_wins([[], [(b"a", b"1")], []]))
    assert result == [(b"a", b"1")]


def test_all_empty_sources_yields_nothing():
    assert list(k_way_merge_newest_wins([[], [], []])) == []
