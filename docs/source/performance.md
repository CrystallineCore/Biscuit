# Performance and Operations

This page describes how Biscuit behaves under load, what to plan for when
deploying it, and which settings affect it. The behaviours below were
observed in testing on a single environment; exact figures depend on hardware,
data and workload, but the characteristics follow from the design.

---

## Checklist

* Build the index **after** loading data, then run `ANALYZE`.
* Confirm plans with `EXPLAIN`, especially for unanchored patterns.
* Prefer anchored patterns (`abc%`, `%xyz`, `a%z`, `a_c`).
* Use `biscuit_like_ops` or `biscuit_ilike_ops` when a column needs only one
  case mode.
* Size `pg_wal`, memory and connection pools for the characteristics below.
* Build with CRoaring (`make WITH_ROARING=1`) and confirm with
  `SELECT biscuit_has_roaring();`.

---

## Query Cost Model

`biscuit_costestimate()` prices a pattern by its shape, relative to the cost
of a sequential scan of the same table:

| Shape | Basis |
|---|---|
| Anchored (prefix and/or suffix, including `_` positions) | Small fixed fraction of the sequential-scan cost; `_` receives a further discount |
| Length predicate (`_` only) | Single bitmap lookup; very low fixed fraction |
| Single unanchored infix part | Absolute cost proportional to row count × (average string length)² |
| Two infix parts | About one tenth of a single part |
| Three or more infix parts | About 1.5 × a single part |
| Match-all (`%`) | No index path offered |
| Regex outside the decomposable subset | No index path offered |
| Pattern not known at plan time (parameter) | 90% of the sequential-scan cost |

Additional details:

* **Average string length** comes from `pg_statistic` for the index's first
  column. Before the first `ANALYZE`, it is estimated from the declared type,
  so plans for unanchored patterns may change after `ANALYZE`. Expression
  indexes use a default length of 32.
* **Conjunctions** are priced as the cheapest predicate in full plus each
  later predicate scaled by the selectivity of the predicates before it,
  matching the executor's evaluation order.
* **Selectivity** is computed with PostgreSQL's standard clause selectivity
  functions, so row estimates above the index scan are realistic.
* **Correlation** is reported as zero.

The constants are compiled in and are not currently configurable.

### Known cost-model limitations

* **Selective prefixes against a B-tree.** A B-tree with `text_pattern_ops`
  typically wins the cost comparison for selective prefix patterns, and in
  testing was also faster on most of them. For broader prefixes Biscuit was
  often faster in practice even when the planner chose the B-tree. The
  difference lies in the heap-access part of the plan cost, which Biscuit's
  own estimate cannot offset; index-only scan support would be needed to
  change it.
* **Unanchored patterns** are priced conservatively, and the planner will
  often decline infix patterns that the index could serve. Recalibrating this
  is on the roadmap.

---

## Write Amplification

A single indexed string belongs to many per-character structures, so writes
against a live Biscuit index cost considerably more than the heap write
alone.

* Each write appends one small record to a shared, index-wide **pending
  log**. The structures a row belongs to are derived later from the row's
  stored text, so the append itself is small.
* The string itself (and, for case-insensitive columns, its case-folded form)
  is written to the index's row store.
* When the pending log reaches `biscuit.delta_compaction_slots` rows, the
  writing backend compacts it into the base structures. `VACUUM` performs a
  full drain regardless of size.

WAL volume per written row grows with string length and with the index's
size, so measurements on a small index understate a large one. In testing,
Biscuit's WAL per row was comparable to a `pg_trgm` GIN index on the same
data. `DELETE` is cheaper than `INSERT` or `UPDATE`.

In addition, each insert into a live index checks the index's existing slots
for a matching heap TID. That check is proportional to the number of indexed
rows, so the CPU cost of a single insert also grows with index size.

Recommendations:

* Size `pg_wal` accordingly and monitor free space.
* Where replication slots are used, consider `max_slot_wal_keep_size` so that
  a lagging standby cannot retain WAL indefinitely.
* Allow for WAL volume in crash-recovery time when planning restart windows.
* All appends to one index serialize on the pending log's tail page, so
  many concurrent writers to the same index contend with each other.

### Bulk loads

Build the index after loading. For large periodic loads, consider dropping the
index, loading, and recreating it:

```sql
DROP INDEX IF EXISTS idx_events_msg;
COPY events FROM '/path/to/data.csv' WITH (FORMAT csv);
CREATE INDEX idx_events_msg ON events USING biscuit (message);
ANALYZE events;
```

---

## Memory

Each backend that uses a Biscuit index loads its own copy into session-local
memory the first time the index is used, and keeps it for the life of the
connection. Consequences:

* Total memory scales with the number of connections that use the index.
  Size connection pools accordingly; a pooler that keeps a small number of
  long-lived server connections is a better fit than many short-lived ones.
* The first query against an index in a new connection pays the load cost.
* Calling `biscuit_index_memory_size()`, `biscuit_size_pretty()`,
  `biscuit_index_stats()`, or querying the `biscuit_memory_usage` view, loads
  the index into the calling session if it is not already loaded.

The in-memory size can differ substantially from the on-disk size that
`pg_relation_size()` reports. Use `biscuit_index_memory_size()` to see it:

```sql
SELECT indexname, human_readable, disk_size
FROM biscuit_memory_usage;
```

The settings `shared_buffers`, `work_mem` and `maintenance_work_mem` do not
limit or size this session-local copy. `shared_buffers` still caches the
index's on-disk pages, which speeds up loading.

### Build memory

Index build holds the whole index in memory and does not use
`maintenance_work_mem`. Build memory grows with row count and string length.
In development testing, an 8-million-row build exceeded the memory of a 4 GB
host, and at 4 million rows the index was roughly five times the size of the
table. Test builds at your target size before relying on them.

---

## Cache Reload After Writes

Each cached copy records the index generation it was loaded at. Every scan
compares that generation with the one on the index metapage and reloads the
index if they differ. A committed write by any backend therefore causes other
backends to reload the index in full on their next scan; there is no
incremental refresh yet.

Read latency rises for a period after each write, and more so with many
concurrent readers. Interleaving frequent writes with a read-heavy load on the
same index is best avoided.

### Concurrent drains

If the pending log is compacted by another backend repeatedly while a scan is
building its candidate set, the scan discards its work and retries. After ten
consecutive retries it fails with:

```text
ERROR:  biscuit: index scan could not obtain a stable view of index "..."
HINT:  Retry the statement. ...
```

The SQLSTATE is `40001` (serialization failure), so applications that already
retry serialization failures handle it. If it recurs, check whether
`biscuit.delta_compaction_slots` has been set very low for the write rate.

---

## String Length

The cost of unanchored patterns grows with the square of string length, so a
small number of very long values can raise the cost of unanchored queries
across the table. Anchored patterns are not affected in the same way. Where
practical, avoid indexing very long free-text columns with Biscuit, or index a
bounded expression such as `left(col, 200)` if queries only need that prefix
(queries must then use the same expression).

---

## Scans, Parallelism and LIMIT

* **Scan types.** Biscuit supports Index Scan and Bitmap Index Scan. It does
  not support ordered, backward or index-only scans, so `ORDER BY` always
  requires a sort and every matching row is fetched from the heap.
* **Parallel index scans are disabled.** A Biscuit index scan always runs in
  one process. A Parallel Bitmap Heap Scan above a Biscuit Bitmap Index Scan
  still works, so heap access and aggregation can run in parallel.
* **LIMIT.** The index computes the full candidate set for a scan before
  returning the first row. `LIMIT` reduces heap fetches but not index-side
  work.
* **No recheck** is requested for `LIKE`, `ILIKE`, `~` and `!~`; the
  executor does not re-evaluate the pattern for rows returned by the index.
  This benefits `count(*)` in particular. `~*` and non-rewritable regex
  predicates request a recheck.

---

## Maintenance

### VACUUM

`VACUUM` (including autovacuum) removes index entries for dead heap tuples and
drains the pending log into the base structures. Regular vacuuming keeps the
pending log short, which keeps the first read after a write burst fast.

`VACUUM` does not reduce the on-disk size of the index.

### REINDEX

Use `REINDEX` to reclaim space or to rebuild after large-scale changes:

```sql
REINDEX INDEX CONCURRENTLY idx_products_name;
```

Consider rebuilding when:

* a large fraction of the table has been deleted or updated;
* the index was built by a release earlier than 3.1.0 on a table that had
  been updated, or is a partial index built before 3.1.0 (see
  [Upgrading](installation.md#upgrading));
* an unlogged index has been through crash recovery (required; see
  [FAQ](faq.md#unlogged-tables)).

### Monitoring

```sql
-- per-index report
SELECT biscuit_index_stats('idx_products_name'::regclass);

-- all Biscuit indexes with in-memory and on-disk size
SELECT * FROM biscuit_memory_usage;

-- standard PostgreSQL index usage statistics
SELECT s.indexrelname, s.idx_scan, s.idx_tup_read, s.idx_tup_fetch
FROM pg_stat_user_indexes s
JOIN biscuit_indexes b ON b.index_oid = s.indexrelid;
```

---

## Settings

### Biscuit settings

| Setting | Default | Context | Description |
|---|---|---|---|
| `biscuit.delta_compaction_slots` | `20000` | superuser | Rows allowed to accumulate in the pending log before a writing backend compacts it. Raising it defers compaction during write bursts but lengthens the rebuild work the next reader must do; lowering it keeps each compaction short but makes compaction more frequent. |
| `biscuit.diag_scan_trace` | `off` | user | Emits per-key candidate counts and related diagnostics as `WARNING` and `LOG` messages. For troubleshooting only. |

The pending log also has a fixed hard ceiling (512 pages) at which it is
compacted regardless of the setting.

Biscuit does not define any per-index storage parameters, so
`CREATE INDEX ... WITH (...)` and `ALTER INDEX ... SET (...)` have no effect
on its behaviour.

### PostgreSQL settings

* `enable_seqscan = off` is useful in a test session to confirm that the
  index *can* serve a query. Do not use it as a production setting. A
  regular expression outside the rewritable subset still produces a
  sequential scan with this setting, by design.
* `random_page_cost`, `seq_page_cost` and the CPU cost settings influence the
  sequential-scan baseline that Biscuit's anchored costs are expressed
  against, and therefore plan choice.
* `max_parallel_workers_per_gather` affects Parallel Bitmap Heap Scans above
  a Biscuit bitmap scan, not the index scan itself.

---

## Troubleshooting

### The index is not used

1. Run `ANALYZE` on the table.
2. Check the pattern shape. Unanchored and match-all patterns are often, and
   correctly, left to a sequential scan.
3. Check the operator class: an index built with `biscuit_like_ops` is not
   considered for `ILIKE` or `~*`, and `biscuit_ilike_ops` is not considered
   for `LIKE` or `~`.
4. For regular expressions, check that the expression is in the
   [rewritable subset](regex.md#supported-subset). `!~*` is never served.
5. In a test session, `SET enable_seqscan = off;` shows whether the index can
   be used at all.

### A query using the index is slow

* Check whether the pattern is unanchored; its cost depends on row count and
  string length rather than on how many rows match.
* Check for very long values in the column (`SELECT max(length(col))`).
* Check whether many connections are reloading the index after frequent
  writes.
* Check `biscuit_index_stats()` for the index's size and pending-log activity.

### Memory use is high

* Count the connections that use the index; each holds a copy.
* Use `biscuit_like_ops` or `biscuit_ilike_ops` where possible.
* Consider a partial index (`CREATE INDEX ... WHERE ...`) that covers only the
  rows that are searched.
