# Biscuit Index Extension – Changelog

## Version 3.0.0 — Costly Cookies

First release intended for production use, within the workload profile
described in the README. This is a breaking on-disk format change: indexes
built under 2.x must be `REINDEX`ed.

### New Features

* **WAL-logged, crash-safe on-disk storage.** Replaces the external-file
  snapshot mechanism (temp-file-then-rename, CRC32C checksum) introduced in
  2.5.0 with fully in-relation, `GenericXLog`-protected page storage. Every
  persistent structure — per-character and length bitmaps, the TID array,
  tombstones, the free-slot list, and per-record string caches — now lives in
  one of two page-chain types:
  * a **compacted-blob chunk chain** (`biscuit_blob.c`/`.h`), storing a
    structure's serialized bytes across as many pages as needed, and
  * an **append-only pending-delta list chain**, following GIN's pending-list
    design.

  A new per-column **directory** (`biscuit_dir.c`/`.h`) maps each structure's
  identity — `(col, is_lower, kind, char, position)` — to its blob-chain and
  pending-chain heads, and a new **row store** (`biscuit_rowstore.c`/`.h`)
  holds TID slots and cached strings in their own page kinds. Because every
  mutation goes through ordinary WAL-logged buffer writes, index state now
  survives a crash and replicates correctly, which the flat-file snapshot did
  not.

  Verified against crash recovery following an abrupt server termination, and
  against physical streaming replication, including index scans served from a
  hot standby.

* **Pending-list write path with opportunistic draining.** Steady-state
  `INSERT`, `UPDATE` and `DELETE` no longer rewrite a whole snapshot; each
  touched structure durably appends a small delta record to its own pending
  chain. Once a structure's pending chain exceeds the drain threshold (fixed in
  the metapage; no SQL-level setting yet), it is re-serialized into a fresh
  compacted blob and its previous chains are retired to a deferred-recycle
  freelist. `VACUUM` (`biscuit_vacuumcleanup()`) additionally performs an
  unconditional full drain pass and records lifetime `total_drains` and
  `total_pending_bytes` counters in the metapage.

* **Read-time pending-list reconciliation.** Query evaluation
  (`biscuit_pattern.c`) transparently merges not-yet-drained pending records
  into the bitmap it reads, so a backend sees a consistent view of a structure
  regardless of whether another backend's mutations have been drained, without
  triggering a drain itself.

* **Cross-backend cache coherency.** The per-session index copy now lives in
  `rd_amcache` and carries the generation it was loaded at; each scan compares
  that generation against the metapage and reloads on mismatch. This corrects a
  pre-release defect in which a backend that had already used an index would
  not observe other backends' committed `INSERT`s through that index, returning
  results that did not reflect them. Deletes had propagated; inserts had not.

  Verified with a long-lived reader tracking another backend's `INSERT`,
  `UPDATE` and `DELETE` activity across transaction boundaries, on both primary
  and hot standby. See [Known Limitations](#known-limitations) for the reload
  cost this introduces.

* **Candidate-mask threading across scan keys.** `biscuit_rescan()` now sorts
  scan keys by estimated selectivity, evaluates the cheapest first, and threads
  the surviving row set into each later key as a mask
  (`biscuit_query_pattern_masked()` and its ILIKE and multi-column variants).
  Later keys examine only rows still under consideration, rather than computing
  a full-table result that is subsequently intersected. Queries combining an
  anchored predicate with an unanchored one benefit substantially; conjunctions
  in which no key is selective are unchanged.

* **Rewritten cost model.** `biscuit_costestimate()` replaces a placeholder flat
  cost with a model derived from pattern shape, column statistics and relation
  size. Anchored and length-predicate patterns are priced as a small fraction of
  the sequential-scan baseline; unanchored patterns scale with row count and the
  square of average string length, taken from `pg_statistic`; multi-part
  patterns are discounted relative to a single part; and conjunction cost is the
  cheapest key at full price plus each later key scaled by the selectivity of
  those preceding it. Real `clauselist_selectivity()` replaces a hardcoded
  constant, and index correlation is reported as `0.0` rather than `1.0`.

  In testing with Biscuit, `pg_trgm` GIN and a `text_pattern_ops` B-tree all
  present, the planner selected the fastest available access path for the
  large majority of a mixed query suite.

* **Length-predicate support.** Patterns consisting only of `_` wildcards
  (`'______'`, `'______%'`) are recognised as length predicates and answered
  from the length bitmaps in a single lookup, rather than being treated as
  unusable.

* **New `biscuit_like_ops` / `biscuit_ilike_ops` operator classes.** In addition
  to the default `biscuit_ops`, which builds both case-sensitive and
  case-insensitive structures, a column may be indexed with `biscuit_like_ops`
  (LIKE and NOT LIKE only) or `biscuit_ilike_ops` (ILIKE and NOT ILIKE only),
  skipping the build and maintenance cost of the structure set it will never be
  queried with. The mode is derived from the column's opfamily at build and load
  time and is never persisted, so it cannot go stale across a `REINDEX` under a
  different opclass.

### Bug Fixes

* **Cross-backend invisibility of committed `INSERT`s.** See *Cross-backend
  cache coherency* above. This was the most significant defect addressed in
  this release, as it produced incorrect results without raising an error.

* **Off-by-one boundary reads in multi-column length-bitmap lookups.** Several
  `<=` comparisons against `max_length` / `max_length_lower` could read one
  `RoaringBitmap*` past the end of a palloc'd array when a pattern's length
  equalled the column's maximum indexed length. Tightened to `<` throughout.

* **`NOT LIKE` / `NOT ILIKE` inversion in multi-column scans.** The set of all
  non-null rows used to build the inverted result always consulted the
  case-sensitive length-≥ bitmap, even for `NOT ILIKE`. It now selects the
  case-matching array.

* **Wildcard-unaware substring matching in multi-column `%needle%` queries.**
  Candidate verification used a literal `strstr()`, treating `_` as an ordinary
  byte rather than a single-character wildcard. Replaced with the
  wildcard-aware `biscuit_wildcard_contains()`.

* **Length-only patterns excluded from index paths.** Patterns with no concrete
  characters were priced as unusable and never produced an index path.

### Internal Changes

* **Removed the background preload worker.** `biscuit_preload.c`/`.h` —
  skeleton loading, the shared-memory ring buffer, the background worker, and
  the `strstr`/`strcasestr` fallback scan used during warm-up — has been
  deleted. `beginscan()` resolves the index through the session cache or loads
  it synchronously from its on-disk directory. There is no warm-up window or
  degraded-scan period after a restart.

* **`biscuit_persist_save()`, `biscuit_persist_load()` and
  `biscuit_persist_drop()` rewritten** against the directory and blob/pending
  chain machinery. The 2.5.0 flat-file snapshot mechanism is removed outright
  rather than version-gated; there is no dual-format reader.

* **Metapage format bumped (`BISCUIT_VERSION` 1 → 3).** Adds per-column
  directory roots, a deferred-recycle freelist root, and pending-list tuning and
  observability fields. A separate `page_format_version` tracks the binary
  layout of individual page structs.

* **Monotonic generation counter** (`idx->gen`, mirrored in the metapage) is
  bumped non-transactionally on every successful `INSERT` and `bulkdelete`,
  replacing `preload_state`-based staleness tracking. Over-invalidation is
  harmless — it costs an additional reload — whereas under-invalidation would be
  a correctness problem, so the counter errs toward invalidating.

* **`bulkdelete()` and the `UPDATE`-as-delete path remove records individually**
  through `biscuit_remove_from_all_indices()`, each durably recording its
  removal via a pending-list append, replacing the previous bulk
  `andnot_inplace()` sweep that depended on an eager whole-index resave.

* **`biscuit_cache.c`'s proc-exit callback simplified.** Mutations are durable
  when WAL-logged, so nothing remains to flush at backend shutdown; the callback
  now only drops the process-local cache.

* Unused constants and an unreferenced pattern-classification field removed from
  the cost model.

### Known Limitations

These follow from the design and should be planned for. Figures observed during
testing will vary with hardware, data and workload.

* **Write amplification.** Because one indexed string touches many
  per-character structures, `INSERT` and `UPDATE` against a live index generate
  considerably more WAL than the corresponding heap writes — in testing, by
  roughly two orders of magnitude. `DELETE` is much cheaper, recording a
  tombstone rather than rewriting structures. Size `pg_wal` accordingly, monitor
  free space, and where replication slots are in use consider setting
  `max_slot_wal_keep_size`.

* **Bulk-load before indexing.** Creating the index after a load is
  substantially faster, and generates far less WAL, than inserting the same
  rows into an already-indexed table.

* **Per-connection memory.** Each backend holds its own copy of the index in
  session-local memory for the life of the connection, loaded lazily as
  patterns are queried. Memory therefore scales with concurrent connections;
  `biscuit_index_memory_size()` reports the current session's copy.

* **Cache reload on invalidation.** A committed write by any backend
  invalidates cached copies, which are then reloaded in full rather than
  refreshed incrementally. Read latency rises for a period after each write, and
  the effect is more pronounced with many concurrent readers. Incremental
  refresh is planned.

* **Build cost and index size.** Biscuit indexes are larger and slower to build
  than comparable `pg_trgm` or B-tree indexes on the same column. `VACUUM` does
  not reduce index size; use `REINDEX`. Build memory scales with row count.

* **Unanchored query cost grows with the square of string length**, so a small
  number of unusually long values can affect query cost across the table.

* **No ordered, backward, index-only or unique scans**, and Biscuit indexes are
  not clusterable.

### Testing

* Added `tests/crud.sql`, an end-to-end CRUD maintenance test comparing
  sequential-scan and Biscuit index-scan row counts across INSERT, UPDATE and
  DELETE phases.
* Benchmark scripts (`tests/forced_index_usage.sh`, `tests/planner_usage.sh`)
  now target an explicit PostgreSQL port and cluster rather than the system
  default, and report index size via `pg_relation_size()`.
* Recommended addition: a two-session concurrency test in which one session
  queries the index, a second commits a change, and the first must observe it.
  The cross-backend visibility defect corrected in this release was not
  detectable by single-session tests.

### Upgrade Notes

**This is a breaking on-disk format change.** Indexes built under 2.x must be
`REINDEX`ed after upgrading; there is no automatic migration and no dual-format
reader. Until an index is rebuilt, its first cold load under 3.0.0 fails with an
error referring to this note.

```sql
ALTER EXTENSION biscuit UPDATE TO '3.0.0';

-- Identify indexes requiring a rebuild
SELECT schema_name, index_name FROM biscuit_indexes;

-- Rebuild each one; CONCURRENTLY avoids a write lock
REINDEX INDEX CONCURRENTLY <index_name>;
```

Plan a maintenance window sized for the rebuild: index build is slower than for
`pg_trgm` GIN on the same data and generates substantial WAL.

---

## Version 2.5.0

### New Features

* **Persistent on-disk snapshots for fast index reload.** `BiscuitIndex` state can now be saved to disk and restored at server startup without rebuilding from the heap. Snapshots persist index metadata, record bookkeeping (`tids`, tombstones, free list), cached string data, and per-character and length-based bitmap structures for both single- and multi-column indexes. Bitmaps are serialized using CRoaring's portable format when available, with a raw bitmap fallback otherwise. Snapshots are written atomically (temp file → `fsync()` → rename) to prevent partial writes, and magic/version validation rejects incompatible formats on load. This significantly reduces restart time by avoiding a full heap rebuild.

* **Generation-based staleness detection for snapshots.** A monotonic generation counter (`gen`) is now incremented after successful inserts and vacuum deletions and persisted to the metapage. Each snapshot records the generation it was written at; on load, this is compared against the live metapage value, and stale snapshots are discarded with a `WARNING`, falling back to a normal heap rebuild. Snapshots are automatically rewritten after a configurable number of mutations, during `VACUUM` cleanup when needed, and at backend shutdown if unsaved changes remain. The generation is only advanced after a successful save, so failed writes are retried.

### Bug Fixes

* **Fixed wildcard handling for substring `LIKE`/`ILIKE` matching.** The `%needle%` fast path used `strstr()`, which treated `_` as a literal character rather than a SQL wildcard, and always seeded candidate bitmaps from the first pattern byte — causing patterns like `%_lex%` to search on `_` instead of the first concrete character. Substring verification now uses the wildcard-aware `biscuit_wildcard_contains()`, and candidate seeding uses `biscuit_part_seed_byte()`, which skips leading wildcards (falling back to length-based candidates when no concrete seed exists). This restores correct results for `LIKE`, `ILIKE`, `NOT LIKE`, `NOT ILIKE`, and compound predicates involving `_`.

* **Fixed single-column `ILIKE` length bitmap allocation.** The single-column build path computed `max_length_lower` incorrectly, causing all single-column `ILIKE` queries to fail their length bounds checks. This was a regression from the preceding `libroaring` crash fix, which removed a per-record update the allocation had been silently relying on. `max_length_lower` is now computed directly from `data_cache_lower`, matching the multi-column implementation.

* **Fixed out-of-bounds access in bitmap lookups.** Tightened boundary checks to prevent invalid bitmap array accesses that could trigger crashes in CRoaring.

* **Fixed NULL row handling during index build.** `biscuit_build()` previously skipped an entire row if any indexed column was `NULL`, omitting the row's TID from the index and making its non-NULL columns unsearchable. The row-level NULL check has been removed in favor of the existing per-column handling, matching `biscuit_insert()`.

* **Fixed off-by-one bounds in length "≥" bitmap lookups.** Requests where the minimum length equaled the maximum indexed length could read past the end of the bitmap arrays; bounds checks are now tightened while preserving correct behavior for valid lengths.

### Internal Changes

* **Replaced background preload with synchronous cache loading.** Removed the skeleton preload pipeline, background worker, associated shared-memory state, and fallback scan path. Indexes are now built synchronously on cache miss and cached fully initialized, simplifying the codebase and eliminating much of warm-up latency.

* **`biscuit_persist_save()`** now accepts an `Oid` instead of a `Relation`.

* Reserved padding in snapshot and metapage headers for future metadata.

---

## Version 2.4.2

### Bug Fixes

* **Fixed a use-after-free in the index cache.** `BiscuitIndex` objects were inadvertently owned by both `biscuit_cache` and PostgreSQL's `rd_amcache`, allowing relcache invalidation to leave stale pointers in the session cache. `biscuit_cache` is now the sole owner of `BiscuitIndex` objects.

### Build

* **Fixed compiler warnings for unused parameters.** Contributed by Devrim Gündüz.
* **Fixed signed/unsigned comparison warnings.**
* **Fixed an unused-variable warning** in `biscuit_rescan_multicolumn`.

### Biscuit

* Version bumped to **2.4.2**.

---
## Version 2.4.1
### Build
- Fix hardcoded `PG_CONFIG` paths that prevented builds on non-Debian distributions.

### Biscuit
- No functional changes.

---
## Version 2.4.0 — Donut

### New Features

* **Expression index support:** Biscuit now correctly evaluates arbitrary index key expressions during index builds, enabling indexes such as:
  ```sql
  CREATE INDEX idx ON table USING biscuit (lower(column_1), (column_2::text));
  ```


* **Multi-version build support (PG 16, 17, 18, 19beta1):** Biscuit can now be compiled and installed against PostgreSQL 16 and 17, in addition to the already supported PG 18 and PG 19 Beta. All version-specific API differences are handled at compile time via `#if PG_VERSION_NUM` guards.

### Bug Fixes

* **Multi-column parallel scan returned duplicate rows:** In the multi-column fallback scan path, every Gather participant was calling `biscuit_collect_sorted_tids_single()` unconditionally, causing each worker to return the full TID set and the Gather node to assemble N× the expected rows. The call site now mirrors the single-column path by resolving the shared-memory parallel scan descriptor and dispatching through `biscuit_collect_sorted_tids_parallel()`, so each participant claims a disjoint slice of the pre-partitioned TID array.

* **`biscuit_operators` view no longer breaks when additional operator classes are added:** The view previously filtered on a hardcoded `opfname = 'biscuit_text_ops'`. It now joins through `pg_am` and filters on `am.amname = 'biscuit'`, staying correct without edits if new opclasses or opfamilies are later added. The view also surfaces the opfamily name per row.

### Internal Changes

* **Parallel scan callbacks are conditionally compiled for PG 18+:** `amcanparallel`, `amestimateparallelscan`, `aminitparallelscan`, and `amparallelrescan` are only registered when `PG_VERSION_NUM >= 180000`. On PG 16 and 17 the parallel fields are set to `false` / `NULL`.

* **Cross-version compatibility macros added to `biscuit_common.h`:**
  * `BISCUIT_PARALLEL_AM_OFFSET(ps)` abstracts the rename of `ps_offset` → `ps_offset_am` in PG 18.
  * `BISCUIT_COUNT_INDEX_SEARCH(scan)` abstracts the index search counter, which moved from `xs_numIndexSearches` (PG 17) to `scan->instrument->nsearches` (PG 18+) and did not exist in PG 16.
  * `biscuit_estimateparallelscan` is declared with the correct signature for each major version (`void` on PG 16, `int nkeys, int norderbys` on PG 17, `Relation indexRelation, int nworkers, int nchunks` on PG 18+).

* **Version string updated to `2.4.0 - Donut`.**

### Notes

* **CHAR(n) / `bpchar` native operator class is not yet available.** PostgreSQL defines LIKE/ILIKE operators only over `(text, text)`, so a dedicated `biscuit_bpchar_ops` operating directly on padded `bpchar` values would require new C-level operator implementations. As a supported workaround, CHAR(n) columns can be indexed today via an expression index on the text cast:
  ```sql
  CREATE INDEX idx ON table USING biscuit ((char_col::text));
  ```
  This is documented in `biscuit.sql` and reflected in the updated `biscuit_operators` view comment.

---

## Version 2.3.0 — Bagel

### New Features

* **Parallel index scan support:** Biscuit now integrates with parallel query execution in PostgreSQL, allowing Gather plans to distribute work across workers without duplicate results.

* **Pre-lowercased cache for multi-column indexes:** Added `column_data_cache_lower` to accelerate ILIKE queries by eliminating repeated string normalization during scans.

* **LIKE / ILIKE matching:** Pattern matching now correctly handles `%`, `_`, escape sequences, and complex wildcard combinations.

* **Version updated to `2.3.0 - Bagel`.**

### Bug Fixes


* Fixed a crash that could occur when INSERT operations followed SELECT queries on partially loaded indexes.

* Fixed an issue where newly inserted rows could become invisible to subsequent queries due to stale session cache entries.

* Fixed multi-column indexes failing to update length-based bitmap structures during inserts.

* Fixed several memory initialization issues during cache growth that could cause incorrect results or instability.

* Fixed insert operations losing in-memory changes after relcache invalidation.

* Prevented the planner from selecting Biscuit for unqualified scans where no index predicates are present.

* Fixed single-column scans using incorrect query paths for LIKE and ILIKE operations.


* Fixed an issue where indexes could remain in a cold state even after background preloading had completed.

* Improved cache update behavior to avoid unnecessary remove-and-reinsert cycles during inserts.

### Performance Improvements

* Eliminated per-row allocations during ILIKE fallback scans by using pre-lowercased caches.

* Simplified TID collection by consolidating scan paths into a single parallel-aware implementation.

### Internal Changes

* Reworked the parallel scan infrastructure around a shared-memory descriptor model and added support for PostgreSQL's parallel index scan callbacks.

* Removed unused LIMIT-tracking logic that was ineffective with the PostgreSQL access method API.

---

## Version 2.2.3

### Structural Changes
 
- **Monolith split into modules.** The single `biscuit.c` file has been
  decomposed into focused translation units, each with its own header:
  | Module | Responsibility |
  |---|---|
  | `biscuit.c` | AM handler, SQL-callable functions, `_PG_init` |
  | `biscuit_bitmap.{c,h}` | Roaring bitmap abstraction + fallback bitset |
  | `biscuit_cache.{c,h}` | Session-scoped index cache |
  | `biscuit_index.{c,h}` | Index build, load, disk I/O, CRUD helpers |
  | `biscuit_pattern.{c,h}` | LIKE/ILIKE pattern parsing and bitmap matching |
  | `biscuit_preload.{c,h}` | Background preload worker and skeleton loader |
  | `biscuit_scan.{c,h}` | Scan lifecycle (beginscan/rescan/gettuple/getbitmap/endscan) |
  | `biscuit_tid.{c,h}` | TID sorting (radix + qsort) and parallel collection |
  | `biscuit_utf8.{c,h}` | UTF-8 character utilities and Datum→text helpers |

  All shared types, constants, and macros have been consolidated into
  `biscuit_common.h`. 
  
  No SQL-level API changes.
  
- **Version bumped to `2.2.3`** (`BISCUIT_LIBRARY_VERSION`).

### New Features

- **PostgreSQL 19 Beta 1 support.** `PG_MODULE_MAGIC_EXT` (introduced in PG 19)
  is now used when available, with a fallback to `PG_MODULE_MAGIC` for older
  versions. The extension can now be built and loaded against PG 19 development
  builds without modification.


### Improvements

- **Memory context correctness.** The session cache (`biscuit_cache.c`) now
  explicitly switches to `CacheMemoryContext` before allocating cache list
  nodes, ensuring index structures survive transaction boundaries without
  relying on caller context. The `biscuit_cleanup_index` stub correctly avoids
  double-freeing memory owned by the context.

- **`biscuit_complete_preload_local()`** added as a fast in-process upgrade
  path: rebuilds bitmaps from the already-resident string cache without
  reopening the relation or re-scanning the heap. Used by `beginscan` when
  it detects the worker has finished between queries.

- **TID collection refactored into `biscuit_tid.c`.** The unified entry point
  `biscuit_collect_tids_optimized()` selects parallel vs. single-threaded
  collection automatically and supports an optional `limit_hint` to avoid
  collecting more TIDs than the executor needs.

- **Fallback scan in `biscuit_preload.c`** supports NOT LIKE and NOT ILIKE
  during warm-up via a hash-map TID→record-index lookup, maintaining correct
  inversion semantics without bitmaps.

- **UTF-8 helpers isolated in `biscuit_utf8.{c,h}`**, removing scattered
  inline character-length and lowercase conversion code from the pattern and
  index modules.

- **`biscuit_columnindex_memory_usage()`** now validates `max_length >= 0`
  before iterating length bitmap arrays and emits a `WARNING` on corrupt
  state rather than reading out-of-bounds.

### Bug Fixes

- `biscuit_cache_remove()` no longer calls `pfree` on list nodes; they are
  owned by `CacheMemoryContext` and must not be freed manually.

---

## Version 2.2.2

### Performance Improvements

* **Refined TID sorting implementation**

  Replaced the previous hybrid dense/sparse block radix sorter with a uniform 4-pass radix sort covering the full 32-bit BlockNumber.

  Sorting is now performed using four 8-bit passes, eliminating assumptions about block number density or range.

### Correctness & Stability

* **Aligned TID comparison with PostgreSQL core**

  Replaced custom TID comparison logic with PostgreSQL’s native comparison routine to ensure consistent ordering behavior.

---

## Version 2.2.1

### Bug Fixes

* **Fixed recursive pattern matching**

  Resolved incorrect behavior when evaluating nested or repeated wildcard patterns during recursive matching.

* **Corrected underscore (`_`) handling in single-column indexing**

  `_` now correctly operates on character-based offsets (not byte offsets), in accordance with SQL `LIKE` / `ILIKE` semantics, eliminating false matches in multi-byte UTF-8 text.


### Correctness & Stability

* Improved internal consistency between single-column and multi-column pattern evaluation paths.
* Resolved observed edge cases that could lead to incorrect matches under complex wildcard patterns.

---

## Version 2.2.0

### Major Changes

**Switched from byte-based to character-based indexing**

* Biscuit now indexes **Unicode characters instead of raw UTF-8 bytes**.
* Eliminates incorrect behavior caused by multi-byte UTF-8 sequences being treated as independent index entries.
* Index structure now aligns with PostgreSQL’s character semantics rather than byte-level representation.

### UTF-8 & Internationalization Improvements

**Enhanced UTF-8 compatibility**

* Improved handling of multi-byte UTF-8 characters (e.g., accented Latin characters, non-Latin scripts).
* Index lookups, comparisons, and filtering now operate on logical characters rather than byte fragments.

**Correct UTF-8 support for `ILIKE`**

* `ILIKE` now works reliably with UTF-8 text, including case-insensitive matching on multi-byte characters.
* Fixes previously incorrect matches and missed results in non-ASCII datasets.

### CRUD Correctness Fixes

**Resolved multiple CRUD-related bugs**

* Fixed inconsistencies during **INSERT**, **UPDATE**, and **DELETE** operations that could leave the index in an incorrect state.
* Ensured index entries are properly added, updated, and removed in sync with heap tuples.
* Improved stability under mixed read/write workloads.

### Correctness & Planner Consistency

* Improved alignment between Biscuit’s index behavior and PostgreSQL’s text semantics.
* Reduced false positives during pattern matching and eliminated character-splitting artifacts.
* More predictable planner behavior due to improved index consistency.

### Internal Refactoring

* Refactored index layout and lookup logic to support character-aware traversal.
* Hardened UTF-8 decoding paths and edge-case handling.
* Simplified internal invariants for better maintainability and debugging.

---

## Version 2.1.5

### Improvements

**Removed arbitrary limits on multi-column indexes**

*  Biscuit no longer enforces hard-coded limits when creating indexes over multiple columns, allowing more flexible index definitions.

### Safety & Correctness

**Restricted indexing to text-based datatypes**

* Support for non-text datatypes has been removed. Biscuit now explicitly enforces text-only columns to ensure correct operator semantics, planner behavior, and index consistency.

**Explicit error for expression indexing**

*  Biscuit now raises a clear error when users attempt to create an index on an expression (e.g., `lower(col)`), which is not currently supported.
  This prevents silent misconfiguration and enforces Biscuit’s column-based indexing semantics.

> **Note:** Biscuit currently indexes **base columns only**. This may be revisited in future versions.

---

## Version 2.1.4

### Build & Packaging

* Improved Makefile detection logic for CRoaring bitmap support by checking multiple common installation paths, increasing portability across systems and build environments.


### New Features

#### Build and configuration introspection

Added SQL functions to inspect Biscuit build-time configuration, useful for debugging,
reproducibility, and deployment verification.

* **`biscuit_version() → text`**    

Returns the Biscuit extension version string.

* **`biscuit_build_info() → table`**    

Returns detailed build-time configuration information.

* **`biscuit_build_info_json() → text`**    

Returns build configuration as a JSON string for automation and scripting.

#### Roaring Bitmap support introspection

Added built-in SQL functions to inspect CRoaring bitmap support in Biscuit.

* **`biscuit_has_roaring() → boolean`**    

Checks whether the extension was compiled with CRoaring bitmap support.

* **`biscuit_roaring_version() → text`** 

Returns the CRoaring library version if available.

#### Diagnostic views

Added a built-in diagnostic view for quick inspection of Biscuit status
and configuration.

* **`biscuit_status`**  
  A single-row view providing an overview of:
  - extension version
  - CRoaring enablement
  - bitmap backend in use
  - total number of Biscuit indexes
  - combined on-disk index size

## Version 2.1.3

### New Features

#### Added Index Memory Introspection Utilities

Added built-in SQL functions and a view to inspect **Biscuit index in-memory footprint**.

* **`biscuit_index_memory_size(index_oid oid) → bigint`**

  Low-level C-backed function returning the exact memory usage (in bytes) of a Biscuit index currently resident in memory.

* **`biscuit_index_memory_size(index_name text) → bigint`**

  Convenience SQL wrapper accepting an index name instead of an OID.

* **`biscuit_size_pretty(index_name text) → text`**

  Human-readable formatter that reports Biscuit index memory usage in bytes, KB, MB, or GB while preserving the exact byte count.

* **`biscuit_memory_usage` view**

  A consolidated view exposing:

  * schema name
  * table name
  * index name
  * Biscuit in-memory size
  * human-readable memory size
  * on-disk index size (via `pg_relation_size`)

  This allows direct comparison between **in-memory Biscuit structures** and their **persistent disk representation**.


```sql
SELECT * FROM biscuit_memory_usage;
```

#### Notes

* Memory accounting reflects Biscuit’s deliberate cache persistence design, intended to optimize repeated pattern-matching workloads.
* Functions are marked `VOLATILE` to ensure accurate reporting of live memory state.
* `pg_size_pretty(pg_relation_size(...))` reports only the on-disk footprint of the Biscuit index.
Since Biscuit maintains its primary structures in memory (cache buffers / AM cache), the reported disk size may significantly underrepresent the index’s effective total footprint during execution. Hence, we recommend the usage of `biscuit_size_pretty(...)` to view the actual size of the index.

### Performance improvements

#### Removed redundant bitmaps

Separate bitmaps for length-based filtering for case-insensitive search were removed. Case insensitive searches now use the same length-based filtering bitmaps as case-sensitive ones.

---

## Version 2.1.2 (2025-12-11)

### New Features

#### ILIKE Operator Support (Case-Insensitive Matching)

Biscuit now provides **full support for the `ILIKE` operator**, enabling efficient case-insensitive wildcard searches directly through the index.

**Capabilities:**

* Optimized execution path for `ILIKE` and `NOT ILIKE`
* Works seamlessly in mixed predicate chains alongside `LIKE` / `NOT LIKE`
* Fully compatible with multi-column Biscuit indexes

**Examples:**

```sql
-- Case-insensitive suffix search
SELECT * FROM users WHERE name ILIKE '%son';

-- Combination queries
SELECT * FROM users
WHERE name ILIKE 'a%' AND email NOT ILIKE '%test%';
```

##

#### Removed Length Constraint for Indexing

The previous hardcoded **256-character indexing limit** has been removed.
Biscuit now indexes values of **any length**, including very long strings.

**Impact:**

* All text values—short or arbitrarily long—are now included in bitmap generation
* More consistent query coverage for fields like descriptions, logs, and message bodies

---

## Version 2.1.0 - 2.1.1

> Contain build issues. Fixed in version - 2.1.2.

---

## Version 2.0.1 (2024-12-06)

### Bug Fixes

#### Fixed Incorrect Results with Multiple Filter Predicates
**Issue:** Queries with multiple `LIKE` or `NOT LIKE` predicates on the same column could return incorrect results.

**Root Cause:** When executing queries with multiple filter predicates (e.g., `name LIKE '%a%' AND name NOT LIKE '%3%'`), the bitmap inversion logic for `NOT LIKE` was being applied globally instead of per-predicate, causing the wrong result set to be returned.

**Example of Affected Query:**
```sql
-- Query with multiple filters
SELECT COUNT(*) FROM users WHERE name LIKE '%a%' AND name NOT LIKE '%3%';

-- v2.0.0: Returned incorrect count (e.g., 252,167)
-- v2.0.1: Returns correct count (e.g., 251,482) ✅
-- Verified against sequential scan
```

**Fix:** Implemented per-predicate bitmap inversion logic that correctly handles each filter independently before combining results.

**Impact:**
- **Affected Queries:** Any query with 2+ predicates using `LIKE` and/or `NOT LIKE` on indexed columns
- **Severity:** HIGH - Results were incorrect but deterministic
- **Data Safety:** No data corruption - index structure unchanged

**Verification:**
```sql
-- All these patterns now return correct results:

-- Pattern 1: LIKE + NOT LIKE
WHERE name LIKE '%abc%' AND name NOT LIKE '%xyz%'

-- Pattern 2: Multiple NOT LIKE
WHERE name NOT LIKE '%a%' AND name NOT LIKE '%b%'

-- Pattern 3: Complex combinations
WHERE col1 LIKE 'A%' AND col2 NOT LIKE '%test%' AND col1 LIKE '%end'
```

#### NOT LIKE Operator Support
- Full support for `NOT LIKE` pattern matching (Strategy #2)
- Efficient bitmap negation for exclusion queries
- Example: `WHERE name NOT LIKE '%test%'`

### Upgrade Notes

**Compatibility:**
- Fully backward compatible with v2.0.0

**Recommended Actions:**
1. Update extension: `ALTER EXTENSION biscuit UPDATE TO '2.0.1';`
2. Re-run any critical queries that used multiple predicates to verify corrected results


---

## Version 2.0.0 (2024-11-05)

### Major Features

#### Multi-Column Index Support
- Create Biscuit indices on multiple columns simultaneously
- Per-column bitmap optimization for efficient filtering
- Example: `CREATE INDEX idx ON table USING biscuit(name, email, description);`


#### Query Optimization Engine
- Intelligent predicate reordering based on selectivity analysis
- Executes most selective filters first to minimize candidate set
- Supports exact, prefix, suffix, and substring pattern detection

#### Performance Enhancements
- TID sorting for sequential heap access (5000+ results)
- Parallel bitmap collection for large result sets (10K+ matches)
- Direct Roaring bitmap iteration without intermediate arrays
- Skip sorting for bitmap scans (COUNT/aggregate queries)
- LIMIT-aware early termination

#### Memory Management Improvements
- Persistent caching in CacheMemoryContext
- Automatic cache invalidation on index drop/ALTER
- Batch cleanup with configurable threshold (1000 tombstones)

### 🔧 Technical Improvements

**Pattern Matching:**
- Fast-path optimizations for pure wildcard patterns (`%`, `_`)
- Exact length matching for underscore-only patterns
- Optimized single-part and two-part pattern execution
- Recursive windowed matching for complex multi-part patterns

**Type Support:**
- Text, VARCHAR, CHAR (native)
- Integer types (INT2, INT4, INT8) with sortable encoding
- Float types (FLOAT4, FLOAT8) with scientific notation
- Date/Timestamp types with microsecond precision
- Boolean type

**Index Statistics:**
- `biscuit_index_stats(index_oid)` function for diagnostics
- CRUD operation tracking (inserts, updates, deletes)
- Tombstone and free slot monitoring


---

**Full Documentation:** See [README.md](https://github.com/CrystallineCore/Biscuit) or visit [ReadTheDocs](https://biscuit.readthedocs.io/) for complete usage guide and examples.

---
