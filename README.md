# Biscuit — Positional Pattern-Matching Index for PostgreSQL

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PostgreSQL: 16+](https://img.shields.io/badge/PostgreSQL-16%2B-blue.svg)](https://www.postgresql.org/)
[![Read the Docs](https://img.shields.io/badge/Read%20the%20Docs-8CA1AF?logo=readthedocs&logoColor=fff)](https://biscuit.readthedocs.io/)

**Biscuit** is a PostgreSQL index access method for `LIKE` and `ILIKE` pattern
matching, with native multi-column support. It evaluates patterns by
intersecting bitmaps that record which character occurs at which position in
each indexed string. Matches are therefore exact, and PostgreSQL does not need
to recheck candidates against the heap (`xs_recheck = false`).

The name stands for _**B**itmap **I**ndexed **S**earching with
**C**omprehensive **U**nion and **I**ntersection **T**echniques_.

Biscuit indexes *position* rather than *content*. This shapes its performance
profile: it is strongest where the position of characters forms part of the
predicate — anchored patterns, `_` wildcards, and string length — and less
suited to unanchored substring search, which is well served by existing options
such as `pg_trgm`.

---

## Suitability

Biscuit is designed for **read-mostly, analytical workloads**: load data, build
the index, then query. Within that pattern it performs well, and as of 3.0.0 it
is crash-safe and replicates correctly.

Before deploying, please review [Operational
Considerations](#operational-considerations). In summary:

* Writes against a live index generate substantially more WAL than the
  underlying heap writes alone. Bulk loading before index creation is strongly
  recommended.
* Each backend maintains its own in-memory copy of the index for the life of
  the connection, so memory use scales with the number of concurrent
  connections.
* A committed write by any backend invalidates cached copies, which are
  reloaded on next use.

Biscuit is not currently recommended for OLTP tables, tables under continuous
write load, or deployments with large connection pools.

---

## What's new in 3.0.0 — "Costly Cookies"

This is the first release intended for production use, within the workload
profile described above. It is a **breaking on-disk format change**: indexes
built under 2.x must be `REINDEX`ed. See
[Upgrade Notes](CHANGELOG.md#upgrade-notes).

* **WAL-logged, crash-safe on-disk storage.** All index state now lives in the
  index relation's own pages and is WAL-logged, replacing the external-file
  snapshot mechanism used in 2.5.0. Verified against crash recovery and
  physical streaming replication, including index scans served from a hot
  standby.
* **Cross-backend cache coherency.** Cached index copies are validated against
  the metapage generation and reloaded when stale. This corrects a pre-release
  defect in which a backend could continue to serve results that did not
  reflect other backends' committed inserts.
* **Candidate-mask threading across scan keys.** Conjunctive queries evaluate
  the most selective key first and restrict subsequent keys to the surviving
  rows, rather than evaluating each key independently. This is a substantial
  improvement for queries combining an anchored predicate with an unanchored
  one.
* **Rewritten cost model.** Costs are derived from pattern shape, column
  statistics and relation size, allowing the planner to choose sensibly among
  Biscuit, `pg_trgm` and a sequential scan.
* **Length-predicate support.** Patterns consisting only of `_` wildcards are
  recognised as length predicates and answered directly from the length
  bitmaps.
* **`biscuit_like_ops` / `biscuit_ilike_ops` operator classes**, to avoid
  building the case-mode structures a column will never use.

---

## Installation

### Requirements

- Build tools: `gcc`, `make`, `pg_config`
- PostgreSQL 16 or later
- Recommended: the CRoaring library, for faster bitmap operations

### From source

```bash
git clone https://github.com/Crystallinecore/biscuit.git
cd biscuit
make
sudo make install
psql -d your_database -c "CREATE EXTENSION biscuit;"
```

### From PGXN

```bash
pgxn install biscuit
psql -d your_database -c "CREATE EXTENSION biscuit;"
```

---

## Quick start

Load data first, then build the index. This ordering is significantly more
efficient than inserting into an already-indexed table — see
[Operational Considerations](#operational-considerations).

```sql
CREATE TABLE users(id bigserial, name text);
INSERT INTO users(name) SELECT ...;                        -- load
CREATE INDEX idx_users_name ON users USING biscuit(name);  -- then index
ANALYZE users;
```

```sql
SELECT * FROM users WHERE name LIKE 'john%';     -- prefix
SELECT * FROM users WHERE name LIKE '%son';      -- suffix
SELECT * FROM users WHERE name LIKE 'j_hn%';     -- wildcard position
SELECT * FROM users WHERE name LIKE '________';  -- length predicate
```

### Multi-column indexes

```sql
CREATE INDEX idx_products_search
ON products USING biscuit(name, description, category);

SELECT * FROM products
WHERE name LIKE '%widget%'
  AND description LIKE '%blue%'
  AND category LIKE 'electronics%'
LIMIT 10;
```

Predicates are evaluated in order of estimated selectivity, and each restricts
the candidate set passed to the next.

### Operator classes

The default `biscuit_ops` builds both case-sensitive and case-insensitive
structures. Where a column requires only one case mode, the narrower classes
reduce build time and index size:

```sql
CREATE INDEX idx_name       ON users USING biscuit (name);                   -- LIKE and ILIKE
CREATE INDEX idx_name_like  ON users USING biscuit (name biscuit_like_ops);  -- LIKE only
CREATE INDEX idx_name_ilike ON users USING biscuit (name biscuit_ilike_ops); -- ILIKE only
```

Querying an index with an operator it was not built for raises an error rather
than silently falling back to a full scan.

### Supported data types

`text`, `varchar` and `char`/`bpchar` are indexed directly. Other types can be
indexed through an expression that casts to text:

```sql
CREATE INDEX idx_expr ON events ((code::text));
```

---

## Choosing an index

The characterisations below reflect testing on a single environment. Index
selection is workload-dependent; please benchmark against your own data and
query mix.

| Query shape | Biscuit | `pg_trgm` (GIN) | B-tree (`text_pattern_ops`) |
|---|---|---|---|
| Prefix `abc%` | Effective | Applicable | Typically fastest |
| Suffix `%abc` | Typically fastest | Applicable | Requires a `reverse()` expression index |
| Both-anchored `a%z` | Typically fastest | Applicable | Not applicable |
| Unanchored infix `%abc%` | Applicable | Typically fastest | Not applicable |
| Wildcard position `a_c` | Typically fastest | Limited | Not applicable |
| Length only `______` | Supported | Not applicable | Not applicable |
| `ILIKE` | Effective | Applicable | Requires a `lower()` expression index |
| Regular expressions | Not supported | Supported | Not applicable |
| Similarity / fuzzy search | Not supported | Supported | Not applicable |

**Biscuit is a good fit for** anchored patterns, patterns containing `_`
wildcards, length predicates, `ILIKE`-heavy workloads, and queries where exact
results without a heap recheck are valuable — `COUNT(*)` in particular.

**Other options are often preferable for** selective prefix lookups, where a
B-tree is smaller and quicker to build; unanchored substring search, for which
`pg_trgm` is purpose-built; and regular-expression or similarity matching,
which Biscuit does not support.

Running Biscuit alongside a `pg_trgm` GIN index and letting the planner select
between them is a practical arrangement, and the 3.0.0 cost model is calibrated
with it in mind.

---

## How it works

### Positional bitmaps

For each indexed string, Biscuit records which record has which character at
which position, both forward and backward, together with length bitmaps.

```
String: "Hello"

Forward index                    Backward index
  H@0  → {record ids}              o@-1 → {record ids}   (last character)
  e@1  → {record ids}              l@-2 → {record ids}
  l@2  → {record ids}              l@-3 → {record ids}
  l@3  → {record ids}              e@-4 → {record ids}
  o@4  → {record ids}              H@-5 → {record ids}

Length bitmaps
  length[5]    → strings of exactly 5 characters
  length_ge[3] → strings of at least 3 characters
```

Case-insensitive variants of both are built unless the column uses
`biscuit_like_ops`.

### Evaluating `LIKE 'abc%def'`

```
1. Parse into parts:       ["abc", "def"], anchored at both ends
2. Prefix, forward index:  C = pos[a@0] ∩ pos[b@1] ∩ pos[c@2]
3. Suffix, backward index: C = C ∩ neg[f@-1] ∩ neg[e@-2] ∩ neg[d@-3]
4. Length constraint:      C = C ∩ length_ge[6]
→ exact matches, with no heap recheck
```

An anchored pattern resolves to a fixed number of bitmap intersections. An
unanchored pattern has no known position and must consider every candidate
position, so its cost grows with row count and string length and is largely
independent of how selective the pattern is. This asymmetry explains most of
Biscuit's behaviour.

### Wildcards

* `_` is inexpensive: the position is skipped in the intersection chain.
* `%` divides the pattern into parts. Additional parts act as further
  constraints and generally reduce rather than increase evaluation cost.

### Query planning

`biscuit_costestimate()` prices a pattern by its shape:

| Shape | Basis |
|---|---|
| Anchored (prefix and/or suffix) | Small fraction of the sequential-scan baseline |
| Length predicate (`_` only) | Single bitmap lookup |
| Unanchored infix | Scales with row count and the square of average string length |
| Multi-part infix | Discounted relative to a single part |
| All-wildcard (`%`) | No index path offered |

Average string length is taken from `pg_statistic`, so plans for unanchored
patterns may change after the first `ANALYZE` on a newly loaded table.

For conjunctions, the cheapest key is priced in full and each subsequent key is
scaled by the selectivity of those preceding it, matching the executor's
evaluation order.

---

## Diagnostics

```sql
-- Human-readable report for one index
SELECT biscuit_index_stats('idx_biscuit'::regclass);

-- Size of the current backend's in-memory copy, in bytes
SELECT biscuit_index_memory_size('idx_biscuit'::regclass);

-- Unmerged write volume (pending-list) statistics
SELECT * FROM biscuit_pending_list_stats('idx_biscuit'::regclass);
SELECT * FROM biscuit_pending_list_usage;

-- All Biscuit indexes in the database
SELECT * FROM biscuit_indexes;
SELECT * FROM biscuit_status;
```

`total_pending_bytes` is refreshed during `VACUUM` rather than on every write,
so it may lag actual unmerged write volume by up to one `VACUUM` cycle.

---

## Operational Considerations

The behaviours below were observed during testing on a single environment.
Exact figures will vary with hardware, data and workload; the characteristics
themselves follow from the design and should be planned for.

### Write amplification

A single indexed string touches many per-character structures, so an `INSERT`
or `UPDATE` against a live Biscuit index generates considerably more WAL than
the corresponding heap write — in testing, by roughly two orders of magnitude.
`DELETE` is much cheaper, as it records a tombstone rather than rewriting
structures.

Sustained inserts against a live index can therefore consume WAL space quickly.
Size `pg_wal` accordingly and monitor free space. Where replication slots are in
use, consider setting `max_slot_wal_keep_size` so a lagging or disconnected
standby cannot retain WAL indefinitely.

### Build the index after loading

Creating the index after a bulk load is substantially faster than inserting the
same rows into an already-indexed table, and generates far less WAL. For large
periodic loads, consider dropping and rebuilding the index around the load.

### Memory scales with connections

Each backend holds a copy of the index in session-local memory for the life of
the connection, loaded lazily as patterns are queried. Total memory therefore
scales with the number of concurrent connections using the index. Use
`biscuit_index_memory_size()` to inspect the current session's copy, and size
connection pools accordingly.

### Cache reload after writes

A committed write by any backend invalidates cached copies; the next use
reloads the index rather than applying the change incrementally. Read latency
therefore increases for a period after each write, and the effect is more
pronounced with many concurrent readers. Interleaving frequent writes with a
read-heavy query load on the same index is best avoided. Incremental refresh is
planned.

### Index size and build cost

Biscuit indexes are larger than comparable `pg_trgm` or B-tree indexes on the
same column, and take longer to build. `VACUUM` does not reduce index size; use
`REINDEX` to reclaim space. Build memory scales with row count, so very large
tables may require additional working memory.

### String length

The cost of unanchored patterns grows with the square of string length, so a
small number of unusually long values can affect query cost across the table.
Where practical, consider limiting or bucketing indexed length.

---

## Compatibility

| Capability | Supported | Notes |
|---|---|---|
| WAL logging and crash recovery | Yes | |
| Physical streaming replication | Yes | Standby serves index scans |
| Hot standby reads | Yes | |
| MVCC / cross-backend visibility | Yes | |
| Index Scan and Bitmap Scan | Yes | |
| Multi-column indexes | Yes | |
| Exclusion constraints | Yes | |
| Partitioned tables | Yes | |
| `REINDEX CONCURRENTLY` | Yes | |
| `pg_dump` / restore | Yes | |
| Expression indexes | Yes | Cast to a supported text type |
| Ordered scans (`amcanorder`) | No | |
| Backward scans | No | |
| Index-only scans | No | |
| Unique constraints | No | |
| `CLUSTER` on a Biscuit index | No | |
| Regular expressions | No | `LIKE` / `ILIKE` only |
| Similarity / fuzzy search | No | |
| Locale-aware collation | No | Comparisons are byte-based |

Parallel-scan callbacks are registered only on PostgreSQL 18 and later; on
earlier supported versions scans are always serial. For large scans the planner
may still prefer a parallel sequential scan, which the cost model is designed to
allow.

### `ORDER BY` with `LIMIT`

Biscuit does not produce sorted output, so PostgreSQL sorts above the scan:

```
Limit → Sort → Biscuit Index Scan
```

For selective patterns the intermediate result is small and the sort cost is
minor. For broad patterns, review the plan — a sequential scan may be the
better choice, and the cost model is designed to select it where appropriate.

---

## Configuration

### Build options

Enabling CRoaring is recommended for better bitmap performance.

### Index options

Biscuit does not currently expose tunable index options. The pending-list drain
threshold is fixed in the metapage, and cost-model constants are set at compile
time. Exposing these as runtime settings is planned.

---

## Development

```bash
git clone https://github.com/Crystallinecore/biscuit.git
cd biscuit
make clean
CFLAGS="-g -O0 -DDEBUG" make
make installcheck
sudo make install
```

### Testing

```sql
CREATE EXTENSION biscuit;
CREATE TABLE test (id SERIAL, name TEXT);
INSERT INTO test (name) VALUES ('hello'), ('world'), ('test');
CREATE INDEX idx_test ON test USING biscuit(name);
EXPLAIN ANALYZE SELECT * FROM test WHERE name LIKE '%ell%';
```

Changes to the scan or cache paths should be accompanied by a **two-session**
test: one session queries the index, a second session commits a change, and the
first session must then observe it. Single-session tests do not exercise cache
invalidation.

---

## Roadmap

- [ ] Incremental cache refresh in place of full reload on invalidation
- [ ] Reduced write amplification
- [ ] Index-only scan support (`amcanreturn`)
- [ ] Runtime-configurable cost-model parameters
- [ ] Regular-expression support via glob decomposition
- [ ] `amcanorder` for native sorted scans
- [ ] Parallel index build
- [ ] Length bucketing to bound unanchored query cost

---

## License

MIT License — see the LICENSE file.

## Author

Sivaprasad Murali · [@Crystallinecore](https://github.com/Crystallinecore) ·
sivaprasad.off@gmail.com

## Acknowledgments

* The PostgreSQL community, for the extensible index access method framework
* The **B-tree** and **pg_trgm** implementations, which define the design space
  for pattern matching in PostgreSQL
* The **CRoaring** library, for efficient compressed bitmap operations

## Support

- **Issues**: [GitHub Issues](https://github.com/Crystallinecore/biscuit/issues)
- **Discussions**: [GitHub Discussions](https://github.com/Crystallinecore/biscuit/discussions)
- **Documentation**: [ReadTheDocs](https://biscuit.readthedocs.io/)

---

**Happy pattern matching. Grab a biscuit 🍪**
