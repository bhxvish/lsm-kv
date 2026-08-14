"""
Milestone 2 deliverable: prove the DB survives a hard kill mid-write.

This spawns a REAL subprocess (not a mocked crash) that writes data,
then deliberately tears a record and calls os._exit(1) - no cleanup,
no exception handling, the closest thing to `kill -9` we can do
portably from a test. Then we verify the parent process can reopen the
WAL, recover everything that was durably written, and does NOT crash
on the corrupt trailing bytes left behind.
"""

import os
import subprocess
import sys
from pathlib import Path

from db import DB

PROJECT_ROOT = Path(__file__).parent.parent
CRASH_SCRIPT = Path(__file__).parent / "_crash_subprocess.py"


def test_crash_mid_write_recovers_cleanly_with_no_data_loss(tmp_path):
    data_dir = tmp_path / "crash_test_data"

    # Python only auto-adds the *script's own* directory to sys.path,
    # not the cwd - so `from db import DB` inside the subprocess needs
    # the project root on PYTHONPATH explicitly.
    env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
    result = subprocess.run(
        [sys.executable, str(CRASH_SCRIPT), str(data_dir)],
        cwd=str(PROJECT_ROOT),
        env=env,
    )

    # os._exit(1) in the subprocess should surface as this exact code.
    assert result.returncode == 1

    # This must not raise. A naive implementation that assumes every
    # trailing record is complete would throw a struct.error or read
    # past EOF here.
    db = DB(data_dir)

    # All 5 fully-fsync'd writes must have survived.
    for i in range(5):
        assert db.get(f"key{i}".encode()) == f"value{i}".encode()

    # The torn record must NOT appear - it was never fully durable,
    # so "not there" is the only correct outcome, not a crash and not
    # a half-applied value.
    assert db.get(b"torn-key") is None

    # DB must still be fully usable after recovery, not just readable.
    db.put(b"post-recovery-key", b"post-recovery-value")
    assert db.get(b"post-recovery-key") == b"post-recovery-value"


def test_replay_rebuilds_state_across_normal_restart(tmp_path):
    """Non-crash sanity check: close cleanly, reopen, data is still there."""
    data_dir = tmp_path / "restart_test_data"

    db1 = DB(data_dir)
    db1.put(b"a", b"1")
    db1.put(b"b", b"2")
    db1.delete(b"a")
    db1.close()

    db2 = DB(data_dir)  # replays the WAL from scratch
    assert db2.get(b"a") is None  # delete was durable
    assert db2.get(b"b") == b"2"
