# Frequently Asked Questions

## General

### What is Biscuit?

A PostgreSQL index access method for `LIKE` and `ILIKE` pattern matching. It
also serves `~`, `!~` and (with a recheck) `~*` for regular expressions that
can be rewritten exactly as a `LIKE` pattern. It is most effective for
anchored patterns, patterns with `_` wildcards, and length predicates.

### When is Biscuit a good fit?

* Read-mostly tables that are loaded, indexed and then queried.
* Queries with prefix, suffix, both-anchored or positional (`_`) patterns,
  including case-insensitive ones.
* Queries that combine pattern predicates on several text columns.
* `count(*)` over pattern matches, since `LIKE`/`ILIKE` results are not
  rechecked against the heap.

### When are other options better?

* **Unanchored substring search** (`%abc%`): `pg_trgm` is usually faster.
* **General regular expressions, similarity and fuzzy matching:** `pg_trgm`.
* **Word-based search with stemming and ranking:** full-text search
  (`tsvector` with a GIN index).
* **Equality, ranges and sorted access:** B-tree.
* **Selective prefixes:** a B-tree with `text_pattern_ops` is often smaller and
  faster.
* **Tables with continuous writes, or many concurrent connections:** see
  [Performance and Operations](performance.md).

### Is Biscuit production-ready?

Biscuit is under active development and has not yet had the testing and
operational exposure expected of production software. Evaluate it in
development and staging with representative data, workloads, upgrade
procedures, and backup and recovery workflows before relying on it. It is
currently best suited to evaluation, experimentation and non-critical
workloads.

### Is there a limit on string length?

No fixed limit. Very long values increase memory use and the cost of
unanchored patterns, which grows with the square of string length.

---

## Installation

### How do I enable CRoaring?

Build with `make WITH_ROARING=1`, reinstall, and check
`SELECT biscuit_has_roaring();`. CRoaring is not detected automatically. See
[Installation](installation.md#building-with-croaring).

### Which PostgreSQL versions are supported?

PostgreSQL 16 and later.

### How do I upgrade from 3.0.0?

Install the new library, then run `ALTER EXTENSION biscuit UPDATE TO
'3.1.0';` in each database. No `REINDEX` is required. See
[Upgrading](installation.md#upgrading).

---

## Indexing

### Which column types can be indexed?

`text` and `varchar` directly. Other types, including `char(n)`, through an
expression that casts to `text`; queries must use the same expression. See
[Supported column types](api.md#supported-column-types).

### Which operator class should I use?

* `biscuit_ops` (default) if the column is queried both case-sensitively and
  case-insensitively.
* `biscuit_like_ops` if it is only queried with `LIKE`, `NOT LIKE`, `~` or
  `!~`.
* `biscuit_ilike_ops` if it is only queried with `ILIKE`, `NOT ILIKE` or
  `~*`.

The single-mode classes roughly halve build time and memory for that column.

### Can I create partial indexes?

Yes:

```sql
CREATE INDEX idx_active_users ON users USING biscuit (username)
WHERE status = 'active';
```

Partial indexes created by releases before 3.1.0 were built over every row
and should be rebuilt with `REINDEX`.

### Can I build an index without blocking writes?

Yes, with `CREATE INDEX CONCURRENTLY` or `REINDEX INDEX CONCURRENTLY`.

### How long does an index build take, and how much memory does it need?

Build time and memory grow with row count, string length and the number of
columns and case modes. Builds take longer than `pg_trgm` GIN builds on the
same data, and the build holds the whole index in memory without regard to
`maintenance_work_mem`. Test at your target size.

### How are NULL values handled?

NULL values are not indexed. Following SQL semantics, a NULL never satisfies
`LIKE` or `NOT LIKE`, so NULLs are never returned by the index. `IS NULL`
predicates are not served by Biscuit.

### Does Biscuit handle multibyte characters?

Yes. Positions and lengths are counted in characters, so `_` matches exactly
one character regardless of its UTF-8 byte length.

---

## Queries

### Why is my query not using the index?

Common reasons:

1. The table has not been analyzed since loading.
2. The pattern is unanchored (`%abc%`) or matches most of the table, and a
   sequential scan or another index is estimated to be cheaper, which is often
   correct.
3. The pattern is `%` (match-all), which the index does not serve.
4. The operator class does not include the operator (for example, `ILIKE`
   against a `biscuit_like_ops` index).
5. The regular expression is outside the rewritable subset, or is `!~*`.

Check with `EXPLAIN`. In a test session, `SET enable_seqscan = off;` shows
whether the index can be used at all.

### Does Biscuit support regular expressions?

Partially, from 3.1.0. Regular expressions built from anchors, literals, `.`,
`.*`, `.+` and fixed or open-ended repetition counts are rewritten to `LIKE`
patterns and served by the index. Alternation, bracket expressions, groups,
class shorthands and similar constructs are answered correctly by a
sequential scan. `!~*` is never served by the index. See
[Regular Expressions](regex.md).

### Does Biscuit support case-insensitive search?

Yes. `ILIKE` and `NOT ILIKE` are served by indexes using `biscuit_ops` or
`biscuit_ilike_ops`, without a `lower()` expression index.

### Can I use `OR`?

Yes. PostgreSQL combines separate index scans with a `BitmapOr`:

```sql
SELECT * FROM products WHERE name LIKE 'laptop%' OR name LIKE 'desktop%';
```

### Does `LIMIT` make the index scan faster?

It reduces the number of heap rows fetched, but the index computes the full
set of matches before returning the first one.

### Does Biscuit return results in sorted order?

No. Biscuit does not support ordered scans, so `ORDER BY` requires a sort.

### Why does `EXPLAIN` show `Recheck Cond`?

A Bitmap Heap Scan always prints `Recheck Cond`. For `LIKE`, `ILIKE`, `~` and
`!~`, Biscuit does not request a recheck; `~*` and non-rewritable regular
expressions do.

### Are parallel queries supported?

Biscuit index scans run in a single process. A Parallel Bitmap Heap Scan above
a Biscuit bitmap scan is supported, so heap access and aggregation can be
parallelised.

---

## Maintenance and Operations

### Do I need to maintain the index manually?

Inserts, updates and deletes maintain the index automatically. Regular
`VACUUM` (or autovacuum) removes entries for dead rows and drains the pending
log. `VACUUM` does not shrink the index; use `REINDEX` to reclaim space.

### What are tombstones?

Row slots whose entries `VACUUM` has removed but whose bitmap entries have not
yet been purged. They are excluded from results and purged in batches of
1,000. The count appears in `biscuit_index_stats()`.

### How much memory does Biscuit use?

Each backend that uses an index holds its own copy of it for the life of the
connection. Check the current session's copy with:

```sql
SELECT biscuit_size_pretty('idx_name');
```

Multiply by the number of connections that use the index to estimate the
total. The in-memory size can differ substantially from the on-disk size
reported by `pg_relation_size()`, so measure both.

### Why did read latency rise after a write?

A committed write causes other backends to reload their cached copy of the
index on their next scan. Incremental refresh is not yet implemented. See
[Cache Reload After Writes](performance.md#cache-reload-after-writes).

### A query failed with "could not obtain a stable view of index"

The pending log was compacted repeatedly by other backends while the scan was
running. The error has SQLSTATE `40001`; retry the statement. If it recurs,
check `biscuit.delta_compaction_slots`.

### Is the index crash-safe?

Yes, for ordinary (logged) tables. Index state is WAL-logged and is recovered
by PostgreSQL's crash recovery along with the table. No manual rebuild is
expected; if an index appears inconsistent after recovery, please report it.

### Unlogged tables

Unlogged tables and their indexes are reset by crash recovery, as with any
PostgreSQL index. After a crash, a Biscuit index on an unlogged table is
readable but has no stored state, and scans fail with `no on-disk snapshot
found for index`. Run `REINDEX INDEX` to restore it. Logged tables are not
affected.

### Does Biscuit work with replication and point-in-time recovery?

* **Physical streaming replication and hot standby:** yes. The index reaches
  standbys through WAL, and standbys serve index scans from it.
* **Point-in-time recovery:** yes. Index state is restored to the recovery
  target along with the table.
* **Logical replication:** indexes are not replicated logically; create the
  index on the subscriber if it is needed there.

### Can I use Biscuit on partitioned tables?

Yes. Create the index on the partitioned table (which creates it on each
partition) or on individual partitions.

### Can Biscuit and B-tree or pg_trgm indexes coexist on the same column?

Yes. The planner chooses among them per query. Running Biscuit alongside a
`pg_trgm` GIN index is a practical arrangement for mixed anchored and
unanchored workloads.

---

## Troubleshooting

### Results differ from a sequential scan

This should not happen. To compare, run the query with and without the index
in the same transaction:

```sql
BEGIN;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_bitmapscan = off;
SELECT count(*) FROM t WHERE col LIKE 'pattern%';
ROLLBACK;
```

If the counts differ, please report it with the details listed below. If the
index was built before 3.1.0 on a table that had been updated, rebuild it
first; see [Upgrading](installation.md#upgrading).

### How do I report a bug?

Include:

1. `SELECT version();` and `SELECT biscuit_version();`
2. `SELECT biscuit_has_roaring();`
3. The table and index definitions
4. The query and its `EXPLAIN (ANALYZE)` output
5. Relevant server log entries
6. Whether the index was built before or after any upgrade

Report issues at
[GitHub Issues](https://github.com/crystallinecore/biscuit/issues).

### How can I contribute?

Pull requests, bug reports, test results on other data sets and platforms,
and documentation fixes are welcome. See
[Development and Testing](development.md) for building and running the test
suite.
