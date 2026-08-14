from bloom import BloomFilter


def test_added_key_is_always_reported_as_maybe_present():
    bf = BloomFilter.for_capacity(100)
    bf.add(b"key1")
    assert bf.might_contain(b"key1") is True


def test_never_added_key_is_usually_reported_absent():
    bf = BloomFilter.for_capacity(100)
    for i in range(100):
        bf.add(f"key{i}".encode())
    # Zero false negatives is guaranteed; false positives are only
    # PROBABILISTICALLY rare, so we check the aggregate rate rather
    # than asserting on any single key.
    false_positives = sum(bf.might_contain(f"absent{i}".encode()) for i in range(1000))
    assert false_positives / 1000 < 0.05  # well under 5%, generous margin over the 1% target


def test_no_false_negatives_ever_for_added_keys():
    bf = BloomFilter.for_capacity(500)
    keys = [f"key{i}".encode() for i in range(500)]
    for k in keys:
        bf.add(k)
    # A false negative here would be a correctness bug, not bad luck -
    # this must hold for every single key, always.
    assert all(bf.might_contain(k) for k in keys)


def test_serialization_roundtrip_preserves_behavior():
    bf = BloomFilter.for_capacity(50)
    for i in range(50):
        bf.add(f"key{i}".encode())

    restored = BloomFilter.from_bytes(bf.to_bytes())
    assert restored.num_bits == bf.num_bits
    assert restored.num_hashes == bf.num_hashes
    for i in range(50):
        assert restored.might_contain(f"key{i}".encode())


def test_larger_capacity_produces_larger_bit_array():
    small = BloomFilter.for_capacity(10)
    large = BloomFilter.for_capacity(10_000)
    assert large.num_bits > small.num_bits
