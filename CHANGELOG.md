# Biscuit Index Extension – Changelog

## Version 3.1.0

Adds regular-expression support for the subset of patterns that can be
rewritten exactly as a `LIKE` glob, fixes several correctness bugs that
predate this release, and replaces the previous test scripts with a
differential test suite.

**No `REINDEX` is required.** There is no on-disk format change: the rewrite
happens entirely at plan time and the existing structures answer the resulting
glob unchanged. Existing 3.0.0 indexes gain regex support as soon as the
extension is updated.

### New Features

* **Regular-expression operators `~`, `!~`, `~*` and `!~*`** (strategies 5–8).
  Biscuit does not gain a regex engine. A regex qual is decomposed at plan time
  into an equivalent `LIKE` glob and then evaluated by the same positional
  bitmaps that already serve `LIKE`, so anchored regexes inherit Biscuit's
  existing strengths rather than introducing a second matching path.

  The decomposition is required to be *exact* — the emitted glob must match
  precisely the same strings as the regex, no more and no less. Anything that
  cannot be proven exact is refused rather than approximated. The supported
  subset is:

  | Construct | Rewritten as |
  |---|---|
  | `^` / `$` anchors | Whole-string anchoring; a missing anchor becomes `%` |
  | Literal characters, `\X` escapes for non-alphanumeric `X` | Literal, with `%`, `_` and `\` escaped |
  | `.` | `_` |
  | `.*` / `.+` | `%` / `_%` |
  | `.{n}`, `.{n,}`, `X{n}` | `n` repetitions, with `%` for the open-ended form |
  | Trailing `?` (non-greedy marker) | Ignored; it never changes the match set |

  Because `~` is unanchored while `LIKE` is whole-string, `~ 'abc'` becomes
  `LIKE '%abc%'` and `~ '^abc$'` becomes `LIKE 'abc'`.

* **Safe handling of regexes outside the subset.** Alternation, bracket
  expressions, groups, unbounded or optional repetition of a literal, bounded
  `{n,m}` ranges, `\d`/`\w`/`\y` class shorthands, backreferences and
  embedded-option directives are not decomposable. Such quals remain correct:
  the key is skipped, the scan reports `xs_recheck` so the executor
  re-evaluates the original regex, and `biscuit_costestimate()` disables the
  path so the planner chooses a sequential scan instead. An unsupported regex
  therefore costs nothing but the missed optimisation.

* **Operator-class gating for regex, matching `LIKE`/`ILIKE`.** `~` and `!~`
  require the case-sensitive structures and are registered only for
  `biscuit_ops` and `biscuit_like_ops`; `~*` and `!~*` require the
  case-insensitive structures and are registered only for `biscuit_ops` and
  `biscuit_ilike_ops`. An index built with the narrower class is not considered
  by the planner for the operators it cannot answer.

### Bug Fixes

* **Index builds no longer scan the heap directly.** `biscuit_build()` used its
  own `table_beginscan(heap, SnapshotAny, ...)` loop; it now goes through
  `table_index_build_scan()` as every other access method does. The hand-rolled
  scan skipped four things core performs on an access method's behalf:

  * **HOT-root mapping.** Each tuple version was indexed under its own TID
    rather than its HOT chain's root. A scan for a superseded value therefore
    returned a TID whose chain resolves to the *live* tuple, so the index
    reported rows that do not match the predicate. It needed only an `UPDATE`
    performed while the column was not yet indexed — that is, any `CREATE
    INDEX` on an existing, updated table — and `REINDEX` did not repair it,
    because it re-ran the same scan.
  * **Tuple eligibility.** Dead, recently-dead and aborted tuples were indexed
    indiscriminately, and `ii_BrokenHotChain` — hence `pg_index.indcheckxmin`
    — was never set.
  * **`ii_Predicate`.** Partial indexes were built over every row, so they
    returned rows failing their own `WHERE` clause. Nothing filtered them out,
    as the planner had already dropped that qual as implied by the predicate.
  * **`pg_class.reltuples`.** The build reported its own slot count, which
    undercounts on a nullable column because the single-column path allocates
    no slot for a row whose key is NULL. This skewed every subsequent plan on
    the table.

* **Fixed a use-after-free and double free in the pending-list snapshot.**
  `pendlog_expand_touched()` grew the snapshot's bitmap in whatever memory
  context the caller happened to be in. On the read path that is the executor's
  per-query context, which `ExecutorEnd()` deletes — while the snapshot itself
  lives on under `CacheMemoryContext` in the process-wide slot cache. The next
  query read the dangling pointer, and the eventual snapshot free released it a
  second time, typically surfacing as a glibc "double free or corruption" abort
  during `VACUUM`.

* **Fixed anchored patterns losing rows written since the last checkpoint,
  after crash recovery.** An exact-length lookup consulted the base length
  bitmap but skipped the pending log whenever that bitmap was absent, treating
  "no base bitmap at this length" as "no row has this length". Those are not
  the same statement: a row's length membership lives in the pending log until
  a drain folds it in. The base bitmap is only ever absent after the in-memory
  index is rebuilt from disk, since the length arrays are reconstructed from
  the last persisted directory and nothing else — so any length first seen
  after that snapshot came back missing.

  ```sql
  -- every value is exactly 5 characters
  INSERT INTO t SELECT g, 'row' || lpad((g%50)::text, 2, '0')
    FROM generate_series(1, 2000) g;
  CREATE INDEX ON t USING biscuit (v);
  CHECKPOINT;
  INSERT INTO t VALUES (1, 'zzzzzz');   -- new value, new length
  -- crash (pg_ctl -m immediate stop), restart, then:
  SELECT count(*) FROM t WHERE v LIKE 'zzzzzz';   -- returned 0, correct answer is 1
  SELECT count(*) FROM t WHERE v LIKE 'zzzzzz%';  -- returned 1, correct
  ```

  Prefix and infix patterns were unaffected, because they never consult an
  exact-length bitmap — which is why the symptom looked like anchored matching
  breaking rather than a length being missing. Nothing errored; rows quietly
  stopped being returned. Fixed at all eight exact-length call sites:
  single-column and multi-column, `LIKE` and `ILIKE`.

* **Fixed anchored `LIKE`/`ILIKE` silently dropping its length constraint.**
  A fully-anchored pattern (one with no `%`) is evaluated as a positional match
  intersected with the exact-length bitmap. When the table contained no value
  of the pattern's length, that bitmap was absent, and the absent case fell
  through both branches of the surrounding condition — so the length constraint
  was skipped entirely and the anchored pattern degraded into a bare prefix
  match, returning rows that do not match the predicate:

  ```sql
  CREATE TABLE t (v text);
  INSERT INTO t VALUES ('abc'), ('abcd');
  CREATE INDEX ON t USING biscuit (v);
  SELECT count(*) FROM t WHERE v LIKE 'a';   -- returned 2, correct answer is 0
  ```

  An absent length bitmap means no row has that length, so the correct result
  is empty. This affects `LIKE` and `ILIKE` directly and predates regex
  support; it is listed here because it was found while testing the new path,
  which reaches the same code through any `^...$` pattern. It only manifests
  when the table happens to contain no value of the pattern's exact length,
  which is why it survived earlier testing. The two multi-column code paths
  already handled the absent case correctly and were unaffected.

* **Fixed disabled index paths still being chosen on PostgreSQL 18.** Biscuit
  refuses a qual it cannot serve — a scan with no usable keys, an unusable glob
  shape, and now a non-decomposable regex — by assigning the path an
  astronomical cost. Through PG 17 that was sufficient, because a GUC-disabled
  path simply had `disable_cost` folded into its own cost and path choice
  remained a pure cost comparison. PG 18 (commit `e2225346`) replaced that with
  a `disabled_nodes` counter compared *before* cost, so under
  `enable_seqscan = off` the disabled sequential path lost to the index path no
  matter how the costs compared. Such paths now increment `disabled_nodes` as
  well as setting the cost, which composes with the GUC rather than overwriting
  it. PG 16 and 17 are unaffected and continue to rely on the cost alone.

* **Fixed `ambuildempty()` being a no-op, which left unlogged indexes
  unreadable after a crash.** Recovery resets an unlogged relation by copying
  its `INIT` fork over the main fork, so an empty init fork produced a
  zero-length index and the first scan afterwards failed with *"could not read
  block 0 … read only 0 of 8192 bytes"*. A valid empty metapage is now written
  there. See [Known Limitations](#known-limitations) for what this does not yet
  cover.

* **Fixed `-DHAVE_ROARING` being silently dropped from the build.** `PG_CPPFLAGS`
  was appended to after `include $(PGXS)`, but PGXS folds it into `CPPFLAGS`
  with immediate expansion at include time, so the definition never reached the
  compiler. `SHLIB_LINK` is expanded lazily and was still honoured, so the
  build produced an extension that linked against CRoaring while being compiled
  with the fallback bitmap — it built and ran correctly, but silently without
  the performance CRoaring was meant to provide. The flags are now set before
  the include, and the build fails loudly if the two ever disagree again.
  CRoaring is no longer auto-detected: link it explicitly with
  `make WITH_ROARING=1`.

### Internal Changes

* New `biscuit_regex.c` / `biscuit_regex.h` module holding the decomposer and
  the strategy-number helpers. The decomposition is a single left-to-right pass
  with one atom of lookahead and no backtracking, structured so that every
  construct outside the subset reaches one rejection point.
* `QueryPredicate` gained `effective_strategy`, `is_lossy` and `needs_recheck`.
  Evaluation sites now switch on `effective_strategy` rather than
  `ScanKey.sk_strategy`, so the rewrite happens once per key in
  `biscuit_build_query_plan()` and no downstream code is regex-aware.
* `BiscuitScanOpaque.needs_recheck` threads through to `scan->xs_recheck` and
  the bitmap-scan recheck flag. It is recomputed on every `biscuit_rescan()`,
  so a scan node reused across many outer rows cannot latch the flag on or off.
* Regex operators are recognised by looking up the operator in the index
  column's opfamily via `get_op_opfamily_strategy()`, rather than by comparing
  against `OID_TEXT_*` macros. The catalog is the same data the SQL script
  populates, so there is no second operator list to drift out of step.
* `amstrategies` raised from 4 to 8 for the new operators.
* A scan whose keys are *all* non-decomposable regexes seeds its candidate set
  from the reconciled live non-NULL row set rather than a raw
  `[0, num_records)` range, which would include never-populated free slots.

### Testing

The previous `check-sql` / `check-stress` scripts are replaced by a
differential suite under `tests/`, run with `make test`. Every case is
evaluated twice against the same rows: once with index paths disabled —
PostgreSQL's own matching over a sequential scan, used as the oracle — and once
with sequential scans disabled. The two must agree on the row count *and* on a
fingerprint of which rows came back, since two scans can agree on `COUNT(*)`
and still return different rows.

Each case also declares whether the access method must serve it, must not, or
either, so both failure directions are caught: refusing a supported pattern is
a silent performance regression, accepting an unsupported one is a silent wrong
answer.

| Category | Covers |
|---|---|
| `01_like`, `02_ilike` | Anchoring, `_` placement, escapes, wildcards as data, NULLs, case folding |
| `03_regex` | The decomposable subset, the rejected constructs, and the rewrite identities |
| `04_composition` | `AND`/`OR`/`NOT` over globs and regexes, mixed operator families, set algebra |
| `05_multicolumn` | Multi-column indexes cross-checked against three single-column ones |
| `06_opclass` | Operator-class gating in both directions, plus catalogue and storage checks |
| `07_dml_mvcc` | Insert/update/delete after build, savepoints, rollback, `VACUUM`, TOAST |
| `08_unicode` | Character-versus-byte positions, 1–4 byte characters, combining sequences |
| `09_wal` | Crash recovery: `pg_ctl -m immediate stop`, WAL replay, unlogged relations |
| `10_stress` | Generated patterns from a fixed seed, straddling the decomposable boundary |

The suite is not wired up as a `pg_regress` target. Each file asserts
internally and raises on failure, so exit status is the result and there is no
expected-output file to regenerate when a fixture changes. None of the `.sql`
files contain psql-specific syntax, so they also run through pgAdmin, DBeaver,
JDBC or a migration runner; the only step needing a shell is the crash in the
middle of `09_wal`.

`make check-wal` runs that category alone. **It stops and restarts the server**
with no clean shutdown, so do not point it at anything you care about.
`make test` excludes it.

### Known Limitations

* **Only the decomposable subset is accelerated.** Alternation, character
  classes and the other constructs listed above fall back to a sequential scan.
  Where regular-expression matching is central to a workload, `pg_trgm`
  extracts trigrams from an arbitrary regex and remains the better choice; the
  two indexes are complementary and can be used together.

* **`~*` requires a recheck, and `!~*` is not accelerated.** PostgreSQL's regex
  case folding and `ILIKE`'s `lower()`-based folding are not the same relation,
  and they disagree in both directions on characters such as `İ`, `ß` and the
  `ǅ`/`ǈ`/`ǋ` titlecase family. `~*` is therefore decomposed only for
  pure-ASCII patterns under a collation Biscuit judges safe (see below), which
  confines the remaining disagreement to the direction where `ILIKE`
  over-matches, and those scans set `xs_recheck` so the executor removes the
  surplus. `!~*` is never decomposed, because the complement of an
  over-matching set omits rows and no recheck can restore them. `~` and `!~`
  are unaffected and remain exact.

* **`~*`/`!~*` decomposition is refused under collations it cannot prove
  safe.** Nondeterministic collations are refused outright — PostgreSQL's
  regex engine itself does not support them, so this only guards against a
  future core change. More narrowly, an ICU collation refuses decomposition
  of any pattern shape that depends on character *position* (one containing
  regex `.`, emitted as `_`): ICU's `lower()` maps `İ` (U+0130) to two
  characters where the database's default/libc `lower()` maps it to one, and
  that length change can shift a `_`-aligned match out from under a row that
  `~*` would otherwise have matched — an under-match that `xs_recheck`, being
  a pure filter, cannot repair. Unanchored, non-positional patterns
  (`LIKE '%literal%'` shapes with no `.`) remain decomposable under ICU, since
  substring containment does not depend on any other character's folded
  length. Deterministic non-ICU collations (the default/libc case covered
  above) are unaffected.

* **Unanchored regexes inherit the unanchored `LIKE` cost model.** `~ 'abc'`
  decomposes to `LIKE '%abc%'` and is priced as an infix pattern, which the
  planner will often decline in favour of a sequential scan. This is existing
  cost-model behaviour rather than anything specific to regex.

* **An unlogged index must be `REINDEX`ed after crash recovery.**
  `ambuildempty()` now writes a valid metapage into the `INIT` fork, so the
  reset index is readable rather than a zero-length file, but loading it also
  requires a persisted header blob, and a blob lives in pages the init fork
  does not contain. A scan therefore fails cleanly with *"no on-disk snapshot
  found for index"* and a hint to reindex, rather than failing in the storage
  layer. `REINDEX INDEX` fully restores it. Logged indexes are unaffected.

### Upgrade Notes

From 3.0.0, no rebuild is needed:

```sql
ALTER EXTENSION biscuit UPDATE TO '3.1.0';
```

Install the new shared library first, since strategies 5–8 begin dispatching to
it as soon as the operators are registered. The upgrade script adds the regex
operators to the existing operator families with `ALTER OPERATOR FAMILY`, so
indexes built under 3.0.0 pick up regex support immediately and without being
rebuilt.

This release ships only the 3.1.0 install script and the 3.0.0 → 3.1.0 upgrade
script. Upgrading from 2.x therefore goes through a 3.0.0 installation first,
and still requires a `REINDEX`; see the 3.0.0 notes below.

Rebuilding existing 3.0.0 indexes is not required, but is worth considering on
tables that were updated before the index was created: the HOT-root fix above
corrects how rows are indexed at build time, and an index built under an
earlier version carries whatever that scan recorded until it is rebuilt.

---

## Version 3.0.0

First release integrated with WAL logging. This is a breaking on-disk 
format change: indexes built under 2.x must be `REINDEX`ed.

The focus of this release is durability: index state now lives in the index
relation's own WAL-logged pages, participating in PostgreSQL's ordinary
recovery machinery.

### New Features

* **WAL-logged, crash-safe on-disk storage.** Replaces the external-file
  snapshot mechanism from 2.5.0 with in-relation, `GenericXLog`-protected page
  storage covering per-character and length bitmaps, the TID array,
  tombstones, the free-slot list, and per-record string caches. Index state
  now survives a crash and replicates correctly.


* **Pending-list write path with opportunistic draining.** Steady-state
  `INSERT`, `UPDATE` and `DELETE` append a small delta record to a structure's
  own pending chain instead of rewriting a whole snapshot. Once a structure's
  pending chain passes a threshold (`biscuit.delta_compaction_slots`, a new
  GUC), it is re-serialized into a fresh compacted blob. `VACUUM` also
  performs a full drain pass and tracks lifetime drain counters.

* **Read-time pending-list reconciliation.** Queries transparently merge
  not-yet-drained pending records into the results they read, so a backend
  sees a consistent view regardless of whether another backend's writes have
  been drained yet.

* **Cross-backend cache coherency.** Each session's cached copy of the index
  carries the generation it was loaded at; every scan compares that
  generation against the metapage and reloads on mismatch, so a backend
  always sees other backends' committed writes.

* **Candidate-mask threading across scan keys.** Conjunctive queries evaluate
  their most selective key first and restrict later keys to the surviving
  rows, instead of computing each key independently and intersecting.
  Queries combining an anchored predicate with an unanchored one benefit
  substantially.

* **Rewritten cost model.** Costs are now derived from pattern shape, column
  statistics and relation size, letting the planner weigh Biscuit against
  `gin_trgm_ops` pg_trgm and a `text_pattern_ops` B-tree. Costing for unanchored 
  patterns continues to be refined; verify with `EXPLAIN` where a specific plan
  matters.

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

### Internal Changes

* Removed the background preload worker; index loading is now synchronous.
* Rewrote the on-disk persistence layer to use the new directory and
  blob/pending-chain storage; the 2.5.0 flat-file snapshot format is no
  longer read.
* Deletes and updates now remove index entries individually and durably,
  replacing the previous bulk in-memory sweep.

### Known Limitations

These follow from the design and should be planned for. Figures observed during
testing will vary with hardware, data and workload.

* **Write amplification.** Because one indexed string touches many
  per-character structures, `INSERT` and `UPDATE` against a live index generate
  considerably more WAL than the corresponding heap writes alone. Substantial
  WAL is characteristic of maintaining any secondary text-search structure, and
  in testing Biscuit's WAL volume per row was comparable to a `pg_trgm` GIN
  index on the same data. WAL per row also grows as the index grows, so
  measurements taken on a small index will understate a large one. `DELETE` is
  much cheaper, recording a tombstone rather than rewriting structures.

  Size `pg_wal` accordingly, monitor free space, and where replication slots
  are in use consider setting `max_slot_wal_keep_size`. Allow for the
  corresponding effect on crash-recovery duration when planning restart
  windows.

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

### Upgrade Notes

**This is a breaking on-disk format change.** Indexes built under 2.x must be
`REINDEX`ed after upgrading; there is no automatic migration and no dual-format
reader. Until an index is rebuilt, its first cold load under the new version
fails with an error referring to this note.

Plan a maintenance window sized for the rebuild: index build is slower than for
`pg_trgm` GIN on the same data.

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
## Version 2.4.0 

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

## Version 2.3.0 

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
