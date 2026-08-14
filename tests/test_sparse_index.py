from sparse_index import INTERVAL, SparseIndex


def _build_index(num_keys: int) -> SparseIndex:
    idx = SparseIndex()
    offset = 0
    for i in range(num_keys):
        key = f"key{i:04d}".encode()
        idx.maybe_add(key, offset, i)
        offset += 20  # arbitrary fixed-size stand-in for a real record's byte length
    return idx


def test_only_every_nth_key_is_indexed():
    idx = _build_index(50)
    expected_entries = (50 + INTERVAL - 1) // INTERVAL  # ceil(50 / INTERVAL)
    assert len(idx._entries) == expected_entries


def test_find_seek_start_before_first_indexed_key_returns_zero():
    idx = _build_index(50)
    assert idx.find_seek_start(b"key0000") == 0
    assert idx.find_seek_start(b"aaaa") == 0  # sorts before every real key


def test_find_seek_start_returns_offset_of_largest_indexed_key_leq_target():
    idx = _build_index(50)
    # key at position INTERVAL (e.g. key0016) is indexed; anything
    # between it and the next indexed key should resolve to ITS offset.
    exact_offset = idx.find_seek_start(f"key{INTERVAL:04d}".encode())
    just_after_offset = idx.find_seek_start(f"key{INTERVAL:04d}z".encode())
    assert exact_offset == just_after_offset


def test_serialization_roundtrip():
    idx = _build_index(100)
    restored = SparseIndex.from_bytes(idx.to_bytes())
    assert restored._entries == idx._entries
