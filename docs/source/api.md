# API Reference

Reference for the SQL objects, operators and settings provided by Biscuit
3.1.0.

---

## Extension

```sql
CREATE EXTENSION biscuit [ WITH ] [ SCHEMA schema_name ] [ VERSION '3.1.0' ];
ALTER EXTENSION biscuit UPDATE TO '3.1.0';
DROP EXTENSION biscuit [ CASCADE ];
```

* The extension is not relocatable; choose the schema at creation time.
* `DROP EXTENSION biscuit CASCADE` drops every Biscuit index.
* Upgrade paths are described in [Installation](installation.md#upgrading).

---

## Index Access Method

```text
CREATE INDEX [ CONCURRENTLY ] [ IF NOT EXISTS ] name
    ON table
    USING biscuit ( { column | ( expression ) } [ opclass ] [, ...] )
    [ WHERE predicate ];
```

| Feature | Supported | Notes |
|---|---|---|
| Single- and multi-column indexes | Yes | Any subset of columns can be queried |
| Expression indexes | Yes | The expression must yield `text` or `varchar` |
| Partial indexes (`WHERE`) | Yes | Built correctly from 3.1.0; rebuild partial indexes created by earlier versions |
| `CREATE INDEX CONCURRENTLY`, `REINDEX [CONCURRENTLY]` | Yes | |
| Unlogged tables | Yes | The index must be rebuilt after crash recovery; see [FAQ](faq.md#unlogged-tables) |
| Index Scan, Bitmap Index Scan | Yes | |
| `UNIQUE` | No | |
| `INCLUDE` columns | No | |
| Ordered or backward scans | No | `ORDER BY` requires a sort |
| Index-only scans | No | |
| Parallel index scan | No | Parallel Bitmap Heap Scan above the index is supported |
| `CLUSTER` using a Biscuit index | No | |
| Storage parameters (`WITH (...)`) | None defined | |

### Supported column types

`text` and `varchar` columns are indexed directly; `varchar` is coerced to
`text` by the operator classes.

Other types are indexed through an expression that produces `text`, and
queries must use the same expression:

```sql
-- char(n): index and query the text cast
CREATE INDEX idx_code ON items USING biscuit ((code::text));
SELECT * FROM items WHERE code::text LIKE 'AB%';

-- non-text types whose text form is immutable, such as integers
CREATE INDEX idx_order_no ON orders USING biscuit ((order_no::text));
SELECT * FROM orders WHERE order_no::text LIKE '2024%';
```

Index expressions must be immutable. Casts whose text output depends on
session settings, such as `timestamptz::text` (which depends on `TimeZone` and
`DateStyle`), cannot be used directly; use an immutable formatting expression
instead.

Casting `char(n)` to `text` removes trailing padding, so patterns are matched
against the unpadded value.

Attempting to index an unsupported type directly fails with an error such as
`data type integer has no default operator class for access method "biscuit"`.

### Operator classes

| Operator class | Default | Structures built | Operators |
|---|---|---|---|
| `biscuit_ops` | Yes | Case-sensitive and case-insensitive | All eight |
| `biscuit_like_ops` | No | Case-sensitive only | `~~`, `!~~`, `~`, `!~` |
| `biscuit_ilike_ops` | No | Case-insensitive only | `~~*`, `!~~*`, `~*`, `!~*` |

Each operator class has its own operator family, so the planner does not
consider an index for an operator its class lacks. The single-mode classes
roughly halve build time and memory for that column compared with
`biscuit_ops`. The operator class is chosen per column:

```sql
CREATE INDEX idx ON t USING biscuit (a biscuit_like_ops, b biscuit_ilike_ops, c);
```

### Operators and strategies

| Strategy | Operator | SQL form | Served by the index | Recheck |
|---|---|---|---|---|
| 1 | `~~` | `LIKE` | Yes | No |
| 2 | `!~~` | `NOT LIKE` | Yes | No |
| 3 | `~~*` | `ILIKE` | Yes | No |
| 4 | `!~~*` | `NOT ILIKE` | Yes | No |
| 5 | `~` | regex match | Rewritable subset only | No |
| 6 | `!~` | regex non-match | Rewritable subset only | No |
| 7 | `~*` | case-insensitive regex match | Rewritable, pure-ASCII subset, subject to collation | Yes |
| 8 | `!~*` | case-insensitive regex non-match | Never (planner uses another path) | — |

All operators take `(text, text)`. The pattern syntax is described in
[Pattern Syntax](patterns.md) and [Regular Expressions](regex.md).

---

## Functions

### `biscuit_version`

```sql
biscuit_version() RETURNS text
```

Version of the loaded shared library, for example `3.1.0`. Compare with
`pg_extension.extversion` to detect a library/catalog mismatch after an
upgrade.

### `biscuit_has_roaring`

```sql
biscuit_has_roaring() RETURNS boolean
```

`true` if the library was compiled with CRoaring (`make WITH_ROARING=1`),
`false` if it uses the fallback bitmap implementation.

### `biscuit_roaring_version`

```sql
biscuit_roaring_version() RETURNS text
```

CRoaring version the library was compiled against (for example `2.0.4`), or
`NULL` without CRoaring.

### `biscuit_build_info`

```sql
biscuit_build_info() RETURNS TABLE (feature text, enabled boolean, description text)
```

Returns two rows: `CRoaring Bitmaps` (whether CRoaring is compiled in) and
`PostgreSQL` (the server version the library was compiled for).

```text
     feature      | enabled |                 description
------------------+---------+---------------------------------------------
 CRoaring Bitmaps | t       | High-performance bitmap operations enabled
 PostgreSQL       | t       | Compiled for PostgreSQL 17.6
```

### `biscuit_build_info_json`

```sql
biscuit_build_info_json() RETURNS text
```

The same information as a JSON string:

```json
{"version": "3.1.0", "roaring_enabled": true, "roaring_version": "2.0.4", "postgres_version": "17.6"}
```

`roaring_version` is omitted when CRoaring is not compiled in.

### `biscuit_check_config`

```sql
biscuit_check_config() RETURNS TABLE (check_name text, status text, recommendation text)
```

Returns three rows: `Roaring Support`, `Extension Version` and
`Active Indexes` (the number of Biscuit indexes in the current database).

### `biscuit_index_stats`

```sql
biscuit_index_stats(index_oid oid) RETURNS text
```

A text report for one index. A `regclass` can be passed directly:

```sql
SELECT biscuit_index_stats('idx_products_name'::regclass);
```

Calling it loads the index into the current session if it is not already
loaded. Report fields:

| Field | Meaning |
|---|---|
| Active records | Slots holding a value that is not tombstoned |
| Total slots | Allocated row slots, including free ones |
| Free slots | Slots released by `VACUUM` and available for reuse |
| Tombstones | Slots removed by `VACUUM` whose bitmap entries have not yet been purged; purged in batches of 1,000 |
| Max length | Longest indexed value, in characters |
| Inserts | Rows added through index insertion since the index was built (rows present at build time are not counted) |
| Updates | Insertions that replaced an existing entry for the same heap TID |
| Deletes | Entries removed by `VACUUM` |
| Drain threshold (bytes/structure) | Stored metapage value (default 65536). Retained from the 3.0.0 layout; compaction is governed by `biscuit.delta_compaction_slots` |
| Total pending bytes | Not maintained on the write path in 3.1.0 and normally reads 0; do not use it as a measure of undrained writes |
| Lifetime drains performed | Number of times the pending log has been compacted or drained |
| Active Optimizations | A fixed descriptive list; it does not vary between indexes |

### `biscuit_pending_list_stats`

```sql
biscuit_pending_list_stats(index_oid oid,
    OUT pending_list_limit  int,
    OUT total_pending_bytes bigint,
    OUT total_drains        bigint)
```

The pending-list fields of `biscuit_index_stats()` as columns. The same
caveats apply: `total_drains` is maintained; `pending_list_limit` and
`total_pending_bytes` are retained for compatibility. Does not load the index
into memory.

```sql
SELECT * FROM biscuit_pending_list_stats('idx_products_name'::regclass);
```

### `biscuit_index_memory_size`

```sql
biscuit_index_memory_size(index_oid oid)  RETURNS bigint
biscuit_index_memory_size(index_name text) RETURNS bigint
```

Size in bytes of the current session's in-memory copy of the index: strings,
bitmaps and bookkeeping arrays. Loads the index into the session if needed.
The text form resolves the name with `regclass` rules, so schema-qualify it if
the index is not on the search path.

### `biscuit_size_pretty`

```sql
biscuit_size_pretty(index_name text) RETURNS text
```

`biscuit_index_memory_size()` formatted for display, for example
`128.00 MB (134217728 bytes)`.

### Internal functions

`biscuit_handler(internal)` is the access-method handler and
`biscuit_like_support(internal)` is registered as operator-class support
function 1. Neither is intended to be called directly.

---

## Views

### `biscuit_indexes`

One row per Biscuit index in the current database.

| Column | Description |
|---|---|
| `schema_name`, `index_name`, `table_name` | Names |
| `num_columns` | Number of index columns |
| `columns` | Column name for a single-column index, `(expression)` for a single expression, or `N columns` |
| `index_size` | On-disk size (`pg_relation_size`), formatted |
| `index_oid` | OID of the index |

### `biscuit_indexes_detailed`

As `biscuit_indexes`, with `index_definition` (`pg_get_indexdef()`) in place
of `columns`. Useful for recording definitions before a rebuild.

### `biscuit_operators`

Operators registered for Biscuit, one row per operator family and strategy:
`opfamily`, `strategy`, `operator`, `left_type`, `right_type`, `description`
and `coverage` (`fully supported` for strategies 1–4, or a note that only the
rewritable regex subset is accelerated for 5–8).

### `biscuit_status`

A single row: `version`, `roaring_enabled`, `bitmap_implementation`,
`total_indexes` and `total_index_size` (on-disk, formatted).

### `biscuit_memory_usage`

One row per Biscuit index: `schemaname`, `tablename`, `indexname`, `bytes`
(in-memory size), `human_readable`, `disk_size`, `pending_bytes` and
`pending_pretty`. Querying it loads every listed index into the current
session.

### `biscuit_pending_list_usage`

One row per Biscuit index with the output of `biscuit_pending_list_stats()`:
`schema_name`, `index_name`, `table_name`, `pending_list_limit`,
`total_pending_bytes`, `total_pending_pretty`, `total_drains` and `index_oid`.

### `biscuit_version_table`

A table created by the extension recording the version history descriptions
(`version`, `installed_at`, `description`).

---

## Settings

| Setting | Type | Default | Who can set it | Description |
|---|---|---|---|---|
| `biscuit.delta_compaction_slots` | integer (≥ 1) | `20000` | Superuser | Pending-log rows tolerated before a writing backend compacts the log |
| `biscuit.diag_scan_trace` | boolean | `off` | Any user | Per-scan diagnostic output as `WARNING`/`LOG` messages; for troubleshooting only |

Other names under the `biscuit.` prefix are reserved. See
[Performance and Operations](performance.md#settings).

---

## Error Messages

### `access method "biscuit" does not exist`

The extension has not been created in this database. Run
`CREATE EXTENSION biscuit;`.

### `data type ... has no default operator class for access method "biscuit"`

The column type is not `text` or `varchar`. Index an expression that casts to
`text` (see [Supported column types](#supported-column-types)).

### `biscuit: no on-disk snapshot found for index "..."`

The index has no persisted state to load. The expected case is an index on an
unlogged table after crash recovery. `REINDEX INDEX` restores it. In other
circumstances the index may be damaged; rebuild it and report the occurrence.

### `biscuit: index scan could not obtain a stable view of index "..."`

SQLSTATE `40001`. The pending log was compacted by other backends repeatedly
while this scan was running. Retry the statement. If it recurs, check whether
`biscuit.delta_compaction_slots` is set very low for the write rate.

### `biscuit: could not obtain a stable read of index "..."` / `blob chain ... was concurrently compacted`

Transient read conflicts with a concurrent compaction. Retry the statement.

### `biscuit: this index was not built with LIKE support` (or `ILIKE support`)

The operator class of the index (or column) does not include the requested
case mode. The planner does not normally route such queries to the index, so
this error indicates an unexpected plan; please report it with the query and
index definition.

### Messages mentioning corruption, truncation or an unrecognized metapage

These carry the hint `consider running REINDEX`. Rebuild the index and report
the occurrence with the server log.
