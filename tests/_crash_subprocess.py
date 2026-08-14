"""
Not a test file itself — this is invoked BY test_crash_recovery.py as a
real subprocess. It has to be a separate process (not just a function
called in-process) because we need os._exit(1), which skips all Python
and OS cleanup (no flush of unflushed buffers, no file-close handlers,
no exception unwinding). That's the only honest way to simulate what a
`kill -9` or power loss actually does to a process mid-write.
"""

import os
import struct
import sys

from db import DB
from wal import HEADER_FORMAT, OP_PUT


def main() -> None:
    data_dir = sys.argv[1]
    db = DB(data_dir)

    # These 5 writes each go through the normal put() path, so each one
    # is individually fsync'd. All 5 must survive the crash below.
    for i in range(5):
        db.put(f"key{i}".encode(), f"value{i}".encode())

    # Now hand-craft a TORN record to simulate a crash that happens
    # mid-write: a header claiming a key/value follow, but we only let
    # part of the key actually hit disk before dying. Reaching into
    # `_wal` directly here (instead of calling put()) is intentional —
    # it's the only way to reproduce a torn write on purpose.
    key = b"torn-key"
    value = b"torn-value"
    header = struct.pack(HEADER_FORMAT, OP_PUT, len(key), len(value))
    db._wal._file.write(header)
    db._wal._file.write(key[:3])  # only 3 of 8 key bytes make it out
    db._wal._file.flush()
    os.fsync(db._wal._file.fileno())
    # Deliberately no write of `value` at all, and no clean exit.

    os._exit(1)  # hard kill: no cleanup, no exception handling


if __name__ == "__main__":
    main()
