# Design notes: WAL fsync correctness and compaction atomicity

These are the two hardest correctness questions in this project. Everything else (memtables, SSTables, Bloom filters, sparse indexes) is mostly "implement the standard technique correctly." These two are where a subtly wrong implementation would *look* correct in every normal test run and only fail the one time it actually matters — a crash at the wrong instant.

## 1. WAL fsync correctness

### The problem

A write-ahead log's entire purpose is: if the process dies right after `put()` returns, the write must still be recoverable on restart. That requires more care than it sounds like.

```python
self._file.write(header)
self._file.write(key)
self._file.write(value)
self._file.flush()
os.fsync(self._file.fileno())
```

Three separate layers sit between this code and the physical disk platter/flash cell, and each one can silently hold onto your bytes instead of persisting them:

1. **Python's `io` buffer** — `file.write()` by default writes into a Python-level buffer, not straight to the OS. `flush()` empties this buffer into the OS.
2. **The OS page cache** — even after `flush()`, the OS is free to hold the bytes in memory and write them to physical disk on its own schedule (this is normal, and usually a performance win). A process crash doesn't touch this cache — the bytes are still safe. A full **system** crash or power loss does lose it.
3. **The physical disk's own write cache** — most drives buffer writes internally too, though `fsync()` is specified to wait for this to be flushed on well-behaved hardware.

`os.fsync(fd)` is what forces layer 2 (and ideally layer 3) to actually commit. Skip it, and the write "looks" durable — it survives a process crash — but a real power-loss event can still lose it, which defeats the entire point of having a WAL at all. This is why `WAL.append_put`/`append_delete` call `flush()` *and* `fsync()` on every single record, not just at some batching interval — see [What I'd do differently at larger scale](../README.md#what-id-do-differently-at-larger-scale) for the throughput cost of that choice.

### Ordering: why the WAL write happens *before* the memtable update

```python
def put(self, key: bytes, value: bytes) -> None:
    self._wal.append_put(key, value)   # 1: durable first
    self._memtable.put(key, value)     # 2: visible second
```

If this were reversed, a crash between the two lines would leave a value visible to a `get()` that was never actually durable — the exact opposite of what a database should guarantee. Durable-then-visible is the only safe order.

### What "recoverable" actually means: torn writes

A crash can happen in the middle of writing a record — after the header lands but before the value does, for instance. `WAL.replay()` has to treat this as a normal, expected condition, not an error:

```python
key = f.read(key_len)
if len(key) < key_len:
    break  # torn record: crash happened mid-key-write
```

The design choice here: stop replay at the first incomplete record and silently discard everything from that point forward, rather than raising. This is correct because of the ordering guarantee above — the only record that can *ever* be torn is the one that was in flight at the exact moment of the crash. Every record before it was fully written and `fsync`'d (because `append_*` doesn't return until that's done), and there is no record after it (nothing was written after the crash, by definition). So "keep everything up to the first torn record, silently drop the rest" is provably exactly correct, not just a convenient heuristic — it's tested directly in `tests/test_crash_recovery.py`, which hand-crafts a torn record via a real subprocess and a real `os._exit(1)`.

## 2. Compaction atomicity

### The problem

Compaction has to replace several SSTable files with one merged file, *while the main process might be reading any of those files at that exact moment*, and the two processes share zero memory. Getting this wrong could mean: a reader sees a half-written merged file, a reader gets a `FileNotFoundError` on a file that's genuinely still supposed to exist, or — worst of all — a crash mid-compaction silently loses data.

### The protocol, step by step

```python
# 1. Snapshot the current manifest - see "why this snapshot stays valid" below
compacted_set = list(current)

# 2. Merge, entirely offline - nothing else can see this in progress
merged = k_way_merge_newest_wins(sources)
live_only = [(k, v) for k, v in merged if v is not TOMBSTONE]

# 3. Write the merged file atomically (tmp + fsync + rename) - same
#    pattern as every other file this project writes
write_sstable(merged_path, live_only)

# 4. Swap the manifest UNDER THE LOCK - this is the linearization point
new_manifest = update_manifest(dir_path, _swap)

# 5. ONLY AFTER the swap is confirmed durable, delete the old files
if new_manifest[0] == merged_filename:
    for name in compacted_set:
        ...unlink...
```

Step 4 is the moment the merged file becomes "real" from every other process's perspective — before that instant, it's just an orphaned file nobody references; after it, every reader that refreshes its manifest view will use it instead of the old files.

### Why the snapshot in step 1 is safe to use, even though compaction takes time

Between reading the manifest (step 1) and swapping it (step 4), the main process could have flushed several new SSTables. Naively, that's a race: what if the compacted set isn't the front of the manifest anymore by the time we get to the swap?

It's safe because of one invariant maintained by construction: **the main process only ever appends to the manifest, and only compaction ever removes from it.** Since compaction always picks the SSTables at the *front* of the list (the oldest generation), and the main process can only ever add to the *back*, the front of the list — `compacted_set` — cannot change out from under compaction no matter how many new flushes happen in the meantime. The swap function double-checks this defensively anyway:

```python
def _swap(manifest_list):
    if manifest_list[: len(compacted_set)] != compacted_set:
        return manifest_list  # abort, don't guess
    return [merged_filename] + manifest_list[len(compacted_set):]
```

If that assertion ever failed, it would mean the "only one compaction worker, main process only appends" assumption was violated somewhere — better to no-op and let the next cycle re-evaluate from scratch than to silently do something wrong.

### Crash scenarios, and why each one is safe

| Crash point | What's on disk afterward | Consequence |
|---|---|---|
| Before step 3 completes | Old files untouched, no merged file (or a torn `.tmp` that never got renamed) | Nothing to clean up — the `.tmp` file was never visible under its real name |
| After step 3, before step 4 | Old files untouched + one complete, fully-written, but **unreferenced** merged file | Harmless orphan. Correctness is untouched — the manifest still points at the original files, which are all still present |
| After step 4, before step 5 completes | Manifest already points at the merged file; some/all old files still physically present | Still correct — old files are just unreferenced now, same as the row above, except now via the *new* manifest state instead of the old one |
| Mid-compaction, general (the stress test's actual scenario) | Whatever state the above table implies, depending on exact timing | `tests/test_compaction_stress.py`'s second test proves this directly: `kill()` the compaction process mid-cycle, confirm no `.tmp` files remain and every key still reads correctly |

The pattern that makes all of this work: **never delete anything until the thing that makes it safe to delete is itself durable.** The manifest swap is that durability checkpoint for compaction, exactly the way `fsync()` is the durability checkpoint for a single write.

### The manifest lock

`ManifestLock` exists because two different actors want to modify the manifest — the main process appending a newly-flushed filename, and compaction swapping a merged file in — and a plain read-modify-write from two processes at once is a lost-update race. It's a minimal `os.O_CREAT | os.O_EXCL`-based mutex (atomic on both POSIX and Windows), not a production-grade distributed lock: a process that crashes while holding it leaves an orphaned lock file, handled here with a simple timestamp-based staleness override rather than a real PID-liveness check. That's a deliberate, documented simplification — see the README's "what I'd do differently at larger scale" section.
