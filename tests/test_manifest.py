import os
import time

import pytest

from manifest import ManifestLock, read_manifest, update_manifest, write_manifest


def test_read_manifest_returns_empty_list_when_no_file_exists(tmp_path):
    assert read_manifest(tmp_path) == []


def test_write_then_read_roundtrip(tmp_path):
    write_manifest(tmp_path, ["000000.sst", "000001.sst"])
    assert read_manifest(tmp_path) == ["000000.sst", "000001.sst"]


def test_write_overwrites_previous_contents(tmp_path):
    write_manifest(tmp_path, ["a.sst"])
    write_manifest(tmp_path, ["b.sst", "c.sst"])
    assert read_manifest(tmp_path) == ["b.sst", "c.sst"]


def test_update_manifest_applies_mutation_and_persists_it(tmp_path):
    write_manifest(tmp_path, ["a.sst"])
    result = update_manifest(tmp_path, lambda cur: cur + ["b.sst"])
    assert result == ["a.sst", "b.sst"]
    assert read_manifest(tmp_path) == ["a.sst", "b.sst"]


def test_update_manifest_works_when_no_manifest_exists_yet(tmp_path):
    result = update_manifest(tmp_path, lambda cur: cur + ["first.sst"])
    assert result == ["first.sst"]


def test_lock_blocks_concurrent_acquisition(tmp_path):
    with ManifestLock(tmp_path):
        with pytest.raises(TimeoutError):
            with ManifestLock(tmp_path, timeout=0.2, poll_interval=0.02):
                pass


def test_lock_is_released_on_context_exit(tmp_path):
    with ManifestLock(tmp_path):
        pass
    start = time.monotonic()
    with ManifestLock(tmp_path, timeout=1.0):
        pass
    assert time.monotonic() - start < 0.5  # should acquire near-instantly, not wait out a timeout


def test_lock_released_even_if_body_raises(tmp_path):
    with pytest.raises(ValueError):
        with ManifestLock(tmp_path):
            raise ValueError("boom")
    with ManifestLock(tmp_path, timeout=1.0):
        pass


def test_stale_lock_is_stolen_after_timeout(tmp_path):
    lock_path = tmp_path / "MANIFEST.lock"
    lock_path.write_text("orphaned by a crashed process")
    old_time = time.time() - 100
    os.utime(lock_path, (old_time, old_time))

    start = time.monotonic()
    with ManifestLock(tmp_path, timeout=2.0, poll_interval=0.02, stale_after=0.1):
        pass
    assert time.monotonic() - start < 1.0
