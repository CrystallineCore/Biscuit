# Architecture

This page describes how Biscuit 3.1.0 stores and evaluates an index. It is
intended for readers who want to understand the performance characteristics
described elsewhere, or who want to work on the code.

---

## Overview

Biscuit is an index access method for `text` columns. For each indexed
string it records, per character:

* the character's position counted from the **start** of the string
  (0, 1, 2, …);
* the character's position counted from the **end** of the string
  (−1, −2, …); and
* that the character occurs somewhere in the string.

It also records each string's **length**, both as "length = n" and
"length ≥ n". All of these are stored as bitmaps of row slots. A `LIKE`
pattern is answered by intersecting the relevant bitmaps.

```text
String "Hello" in row slot 7

Forward positions        Backward positions
  H@0  → {…, 7, …}         o@-1 → {…, 7, …}
  e@1  → {…, 7, …}         l@-2 → {…, 7, …}
  l@2  → {…, 7, …}         l@-3 → {…, 7, …}
  l@3  → {…, 7, …}         e@-4 → {…, 7, …}
  o@4  → {…, 7, …}         H@-5 → {…, 7, …}

Length bitmaps
  length[5]     → {…, 7, …}
  length_ge[1..5] → {…, 7, …}
```

Positions are counted in **characters**. Bitmaps are keyed by byte value, and
each byte of a multibyte UTF-8 character is recorded at that character's
position, so a pattern such as `'caf_'` matches `'café'` by character
position.

---

## Case Modes and Operator Classes

Each column has up to two complete structure sets:

* a **case-sensitive** set, built from the original string, used by `LIKE`,
  `NOT LIKE`, `~` and `!~`; and
* a **case-insensitive** set, built from the string after `lower()`, used by
  `ILIKE`, `NOT ILIKE` and `~*`.

The two sets have separate length bitmaps, because case folding can change a
string's length. `biscuit_ops` builds both sets; `biscuit_like_ops` and
`biscuit_ilike_ops` build one each. The case mode is derived from the
column's operator family whenever the index is built or loaded; it is not
stored on disk.

Case folding uses PostgreSQL's `lower()` under the database default
collation. The folded string is computed when the row is written and stored
in the index, so later reads, compactions and standbys use the stored value
rather than recomputing it.

---

## On-Disk Storage

All index state lives in the index relation's own pages and every change is
WAL-logged through PostgreSQL's Generic WAL facility. There is no external
file and no separate persistence mechanism, so crash recovery, point-in-time
recovery and physical replication apply without any Biscuit-specific step.

| Component | Contents |
|---|---|
| **Metapage** (block 0) | Format version, per-column directory roots, generation counter, pending-log head and tail, drain counters, page-recycling state |
| **Directory** | Per column, a chain of pages mapping each structure's identity — `(column, case mode, kind, character, position)` — to the head of its compacted blob. Index-wide sets such as tombstones and free slots are stored the same way |
| **Compacted blobs** | The serialized bitmap for one structure, chunked across as many pages as needed |
| **Pending log** | A single, index-wide, append-only log of `{slot, add/remove}` records for writes not yet merged into the blobs |
| **Row store** | Fixed-size slot pages holding each row's heap TID, and a string heap holding each row's value (and its case-folded form), both updated in place |
| **Header page** | Scalar bookkeeping such as row counts and statistics |

Pages released by compaction are recycled only after no running transaction
can still be reading them.

### Why a single pending log

A single row belongs to many structures (several per character, for each
case mode, plus the length structures). Writing to each structure's storage
separately would touch many pages per row, and PostgreSQL writes a full page
image the first time each page is modified after a checkpoint. The shared log
instead records only *which slot changed and in which direction*, so a row
write touches the log's tail page and the row store. The set of structures
affected is derived later from the stored string.

The trade-off is on the read side, and in write concurrency: every write to
an index appends to the same tail page, so concurrent writers to one index
serialize there.

---

## Write Path

**Build.** `CREATE INDEX` and `REINDEX` use PostgreSQL's
`table_index_build_scan()`, which supplies the correct HOT-chain root TIDs,
skips tuples that are not eligible, applies partial-index predicates and
reports `reltuples`. The whole index is constructed in memory and then
written to disk as compacted blobs and row-store pages.

**Insert.** An insert into a live index:

1. claims a row slot (reusing a free slot where possible);
2. writes the TID and the string (and its folded form) to the row store; and
3. appends an `add` record for the slot to the pending log.

When the pending log reaches `biscuit.delta_compaction_slots` rows (or a hard
ceiling of 512 pages), the writing backend compacts a prefix of the log into
the blobs. Compaction always applies a strict prefix of the log, so that a
slot that was added, removed and re-added is applied in order.

**Delete.** Entries for dead heap tuples are removed during `VACUUM`
(`ambulkdelete`): the slot's TID and string are cleared in the row store, a
`remove` record is logged, and the slot is marked as a tombstone and placed on
the free list. Tombstones are purged from the in-memory bitmaps in batches of
1,000.

**VACUUM** also drains the entire pending log into the blobs
(`amvacuumcleanup`).

**Unlogged indexes.** `ambuildempty()` writes a valid empty metapage into the
init fork. After crash recovery the index is therefore readable but has no
persisted state, and scans report `no on-disk snapshot found`; `REINDEX`
restores it.

---

## Load Path and Session Cache

Each backend keeps a copy of each index it uses in a session cache in
`CacheMemoryContext`. On first use, the copy is loaded from the directory,
blobs and row store; nothing is rebuilt from the heap.

Records still in the pending log are applied at read time. The log is
materialized once per statement into an in-memory map keyed by structure
identity, by reading the affected rows' strings back from the row store and
deriving their structures. This is the work that a large pending log makes
expensive, and the reason `biscuit.delta_compaction_slots` bounds the log's
size.

**Coherency.** The metapage carries a generation counter that is advanced by
writes and drains. Each cached copy records the generation it was loaded at,
and every scan compares it with the metapage, reloading the index on a
mismatch. A committed write in one backend therefore causes other backends to
reload the index on their next scan. The counter is advanced
non-transactionally, so an aborted write may cause an unnecessary reload but
never a missed one.

**Drain guard.** A compaction by another backend while a scan is building its
candidate set would leave the scan reading a mixture of pre- and
post-compaction state. Each scan records the drain counters when it starts,
checks them after building its candidate set, and on a change discards the
result, reloads and retries. After ten retries the scan fails with
SQLSTATE `40001`.

**Invalidation on drop.** An object-access hook evicts the cache entry when a
Biscuit index is dropped. It does not free pages itself; the relation's
storage is removed by PostgreSQL at commit, which keeps `DROP INDEX` inside a
rolled-back transaction safe.

---

## Query Path

### Planning

`biscuit_costestimate()` classifies each index qual by pattern shape and
prices it relative to a sequential scan of the table (see
[Performance and Operations](performance.md#query-cost-model)). Regular
expressions are first rewritten to a pattern by the same function the
executor uses, so planning and execution agree on the pattern's shape. A path
that the index cannot serve usefully (no quals, a match-all pattern, or a
regular expression outside the rewritable subset) is given a prohibitive cost
and, on PostgreSQL 18, is also counted in the path's `disabled_nodes`.

### Building the query plan

For each scan key, `biscuit_build_query_plan()`:

1. determines the **effective strategy**. For regex strategies (5–8), it
   rewrites the regular expression with `biscuit_regex_to_glob()` and
   substitutes the corresponding `LIKE`/`ILIKE` strategy. If the rewrite is
   not exact, the key is marked **lossy**: it is skipped, and the scan
   requests a recheck. `~*` keys are marked as needing a recheck even when
   rewritten.
2. analyses the pattern: prefix, suffix, exact, infix, number of parts,
   number of `_`, concrete characters, anchor strength; and assigns an
   estimated selectivity.
3. orders the keys so the most selective runs first.

No code after this step is aware of regular expressions.

### Evaluating patterns

Each key is evaluated against the structures for its column and case mode,
restricted to the candidate set produced by the keys before it (the
**candidate mask**):

| Pattern | Evaluation |
|---|---|
| `''` | `length[0]` |
| `'___'` | `length[3]` |
| `'___%'` | `length_ge[3]` |
| `'abc'` | forward positions 0–2 ∩ `length[3]` |
| `'abc%'` | forward positions 0–2 |
| `'%abc'` | backward positions −3…−1 |
| `'abc%xyz'` | forward prefix ∩ backward suffix ∩ `length_ge[6]` |
| `'%abc%'` | union over every feasible start position of the part's positional intersection |
| `'%a%b%'` and longer | recursive windowed matching: each part is placed at every position after the previous part, with the remaining parts' minimum length bounding the search |

`_` advances the position without adding an intersection. An empty
intersection ends evaluation of that key early. Negated keys are evaluated as
the positive match and complemented against the live, non-NULL rows.

The unanchored cases are why infix cost scales with row count and string
length: the number of candidate positions grows with the length of the
strings, and each position's intersection is over bitmaps whose size grows
with the row count.

### Producing results

Tombstoned slots are removed from the final candidate set, and the remaining
slots are mapped to heap TIDs through the row store. The complete TID list is
built before the first row is returned. For `amgettuple` the executor then
consumes the list one TID at a time; for `amgetbitmap` the TIDs are added to
PostgreSQL's TID bitmap, which orders heap access itself.

The recheck flag is set only for lossy keys and `~*` keys. For all other
operators the index result is exact.

### Parallel scans

Parallel index scans are disabled (`amcanparallel = false`). The earlier
design had every participant compute the full result and take a slice of it,
which is only correct if every participant computes an identical result; a
concurrent compaction can make participants disagree, which would produce
duplicated and missing rows. The parallel code is retained for a future
design that shares one result between participants. Parallel Bitmap Heap
Scan, which goes through `amgetbitmap`, is unaffected.

---

## Bitmaps

With CRoaring (`make WITH_ROARING=1`), bitmaps are Roaring bitmaps, which
choose between array, bitmap and run-length containers per 64K range.
Without CRoaring, a built-in uncompressed block bitmap is used. Results are
identical; the fallback uses more memory and is slower for large sets.

---

## Source Layout

| File | Responsibility |
|---|---|
| `biscuit.c` | Module entry point, access-method handler, GUCs, SQL-callable diagnostic functions, drop hook |
| `biscuit_index.c` | Build, insert, bulk delete, vacuum cleanup, cost estimation, load |
| `biscuit_scan.c` | `beginscan`, `rescan`, `gettuple`, `getbitmap`, `endscan`; drain-guard retry loop |
| `biscuit_pattern.c` | Pattern parsing and analysis, query-plan construction, bitmap matching, pending-log reconciliation |
| `biscuit_regex.c` | Regular-expression to `LIKE` pattern rewriting; strategy helpers; case-insensitive safety checks |
| `biscuit_persist.c` | Saving and loading the in-memory index to and from index pages |
| `biscuit_dir.c` | Directory chains |
| `biscuit_blob.c` | Compacted-blob chains, page allocation and recycling |
| `biscuit_pendlog.c` | Shared pending log: append, read-time snapshot, drain |
| `biscuit_delta.c` | Expanding pending-log slots into structure identities |
| `biscuit_fanout.c` | The single definition of which structures a string belongs to |
| `biscuit_rowstore.c` | In-place TID, string and header storage |
| `biscuit_cache.c` | Per-session index cache |
| `biscuit_tid.c` | TID collection and sorting; retained (disabled) parallel-scan support |
| `biscuit_bitmap.c` | CRoaring wrapper and fallback bitmap |
| `biscuit_utf8.c` | UTF-8 helpers, case folding, datum-to-string conversion |
| `biscuit_common.h` | Shared types, constants and on-disk structures |

---

## Design Limitations

These follow from the design and are described operationally in
[Performance and Operations](performance.md):

* Each backend holds a full copy of each index it uses.
* A committed write causes other backends to reload the index in full.
* Writes are amplified relative to the heap write, and writers to one index
  serialize on the pending log's tail.
* Unanchored patterns cost time proportional to rows × length².
* No ordered, backward, index-only or parallel index scans.
* Build memory grows with the table and is not bounded by
  `maintenance_work_mem`.
