"""
Milestone 4: Bloom filter.

A Bloom filter answers one question, cheaply, without touching disk:
"could this key possibly be in this SSTable?" It can false-positive
(says "maybe" when the key isn't actually there) but can NEVER
false-negative (if it says "no", the key is definitely not in the
file). That one-directional guarantee is exactly what we want: a
"definitely not here" lets the read path skip opening that SSTable's
data file at all.

The bit array itself is a plain bytearray, built and indexed by hand -
that's most of the value of this milestone. mmh3 (MurmurHash3) is only
used for the hash *functions*; implementing a good non-cryptographic
hash from scratch isn't the point of this exercise, the bit-array
mechanics and false-positive-rate math are.
"""

from __future__ import annotations

import math
import struct

import mmh3

# num_bits, num_hashes - both needed to reconstruct the filter on load.
HEADER_FORMAT = ">II"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


class BloomFilter:
    def __init__(self, num_bits: int, num_hashes: int) -> None:
        self.num_bits = num_bits
        self.num_hashes = num_hashes
        self._bits = bytearray((num_bits + 7) // 8)  # round up to whole bytes

    @classmethod
    def for_capacity(cls, expected_items: int, false_positive_rate: float = 0.01) -> "BloomFilter":
        """
        Standard sizing formulas for a Bloom filter targeting a given
        false-positive rate `p` for `n` expected items:

            m (bits)        = ceil(-(n * ln(p)) / (ln(2)^2))
            k (hash count)  = round((m / n) * ln(2))
        """
        n = max(expected_items, 1)
        num_bits = max(8, math.ceil(-(n * math.log(false_positive_rate)) / (math.log(2) ** 2)))
        num_hashes = max(1, round((num_bits / n) * math.log(2)))
        return cls(num_bits, num_hashes)

    def _bit_positions(self, key: bytes):
        # A different seed per round gives k effectively-independent
        # hash functions out of one hash implementation, rather than
        # needing k different hash algorithms.
        for seed in range(self.num_hashes):
            yield mmh3.hash(key, seed=seed) % self.num_bits

    def add(self, key: bytes) -> None:
        for pos in self._bit_positions(key):
            self._bits[pos // 8] |= 1 << (pos % 8)

    def might_contain(self, key: bytes) -> bool:
        return all(
            self._bits[pos // 8] & (1 << (pos % 8))
            for pos in self._bit_positions(key)
        )

    def to_bytes(self) -> bytes:
        return struct.pack(HEADER_FORMAT, self.num_bits, self.num_hashes) + bytes(self._bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        num_bits, num_hashes = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
        bf = cls(num_bits, num_hashes)
        bf._bits = bytearray(data[HEADER_SIZE:])
        return bf
