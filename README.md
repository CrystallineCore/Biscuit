# Biscuit — Positional Pattern-Matching Index for PostgreSQL

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PostgreSQL: 16+](https://img.shields.io/badge/PostgreSQL-16%2B-blue.svg)](https://www.postgresql.org/)
[![Read the Docs](https://img.shields.io/badge/Read%20the%20Docs-8CA1AF?logo=readthedocs&logoColor=fff)](https://biscuit.readthedocs.io/)

**Biscuit** is a PostgreSQL index access method for `LIKE` and `ILIKE` pattern
matching, with native multi-column support and regular-expression support for
patterns that reduce exactly to a glob. It evaluates patterns by intersecting
bitmaps that record which character occurs at which position in each indexed
string. For `LIKE`, `ILIKE`, `~` and `!~`, matches are exact and PostgreSQL does
not need to recheck candidates against the heap (`xs_recheck = false`); `~*` is
the one case that sets a recheck, for the reason given under
[Regular expressions](#regular-expressions).

The name stands for _**B**itmap **I**ndexed **S**earching with
**C**omprehensive **U**nion and **I**ntersection **T**echniques_.

Biscuit indexes *position* rather than *content*. This shapes its performance
profile: it is strongest where the position of characters forms part of the
predicate — anchored patterns, `_` wildcards, and string length — and less
suited to unanchored substring search, which is well served by existing options
such as `pg_trgm`.

---

## Stability Notice

This extension is currently under active development and has not yet received the level of testing and operational experience expected of production-ready software.

Users are encouraged to evaluate the extension thoroughly in development and staging environments before considering deployment in production systems. In particular, testing should include representative datasets, workloads, upgrade procedures, backup and recovery workflows, and performance validation.

Although the extension is intended to operate safely and reliably, defects or unexpected behavior may still be present. As with any new database component, appropriate backups and validation procedures should be maintained before use.

At this stage, the extension is best suited for evaluation, experimentation, and non-critical workloads. Production deployment should be undertaken only after careful testing and assessment of its suitability for the intended environment.

---

## Suitability

Biscuit is designed for **read-mostly, analytical workloads**: load data, build
the index, then query. Within that pattern it performs well. Index state is
WAL-logged, so it takes part in PostgreSQL's ordinary crash recovery,
point-in-time recovery and physical replication rather than relying on a
separate persistence mechanism.

Before deploying, please review [Operational
Considerations](#operational-considerations). In summary:

* Writes against a live index generate substantially more WAL than the
  underlying heap writes alone, and WAL per row grows as the index grows. Bulk
  loading before index creation is strongly recommended.
* Each backend maintains its own in-memory copy of the index for the life of
  the connection, so memory use scales with the number of concurrent
  connections.
* A committed write by any backend invalidates cached copies, which are
  reloaded on next use.
* The first-time loading of an index per session can suffer from a high cold
  latency, but subsequent queries under warm state should work faster.

Biscuit is not currently recommended for OLTP tables, tables under continuous
write load, or deployments with large connection pools.

Regular-expression support covers the subset of patterns that decompose exactly
to a glob — chiefly anchored patterns, `.`, `.*` and repetition counts. Regexes
outside that subset remain correct but are not accelerated. Where regular
expressions are central to a workload, `pg_trgm` handles arbitrary patterns and
is the better choice; the two are complementary.

---

## What's new in 3.1.0

Regular-expression support, a set of correctness fixes that predate this
release, and a new differential test suite. **No `REINDEX` is required** —
there is no on-disk format change, so existing 3.0.0 indexes gain regex support
as soon as the extension is updated:

```sql
ALTER EXTENSION biscuit UPDATE TO '3.1.0';
```

* **Regular-expression operators `~`, `!~`, `~*`, `!~*`.** Biscuit gains no
  regex engine. A regex is rewritten at plan time into an equivalent `LIKE`
  glob and evaluated by the existing positional bitmaps, so anchored regexes
  inherit the same performance profile as the anchored `LIKE` patterns Biscuit
  is already strongest at. The rewrite must be exact; anything that cannot be
  proven exact is refused rather than approximated. See
  [Regular expressions](#regular-expressions).
* **Regexes outside the subset stay correct.** They are not accelerated: the
  key is skipped, the executor rechecks the original regex, and the cost model
  disables the path so the planner picks a sequential scan. An unsupported
  regex costs nothing but the missed optimisation.

### Correctness fixes

All of these predate 3.1.0 and affect `LIKE`/`ILIKE` as much as regex. The
[changelog](CHANGELOG.md#version-310) has reproductions.

* **Index builds now go through `table_index_build_scan()`** instead of a
  hand-rolled `SnapshotAny` heap scan. The old scan missed HOT-root mapping,
  so an index built on a table that had been updated could return rows that do
  not match the predicate — and `REINDEX` did not repair it. It also ignored
  partial-index predicates, indexed dead tuples, and reported a row count that
  skewed planning on nullable columns.
* **Fixed anchored patterns losing rows after crash recovery.** Rows written
  since the last checkpoint went missing from whole-string matches, while
  prefix matches found them normally.
* **Fixed anchored `LIKE`/`ILIKE` dropping its length constraint** when the
  table contained no value of the pattern's exact length, which caused an
  anchored pattern to behave as a bare prefix match.
* **Fixed a use-after-free and double free in the pending-list snapshot**,
  which typically surfaced as a "double free or corruption" abort during
  `VACUUM`.
* **Fixed disabled index paths still being chosen on PostgreSQL 18**, where
  the planner now compares a disabled-node count ahead of cost, so pricing a
  path out no longer disabled it.
* **Fixed unlogged indexes being unreadable after a crash** — `ambuildempty()`
  wrote nothing, leaving a zero-length index. It now writes a valid empty
  metapage; see the changelog for the remaining `REINDEX` caveat.
* **Fixed `-DHAVE_ROARING` being silently dropped from the build**, which
  produced an extension linked against CRoaring but compiled with the fallback
  bitmap. CRoaring is no longer auto-detected — link it with
  `make WITH_ROARING=1`.

### Testing

The previous test scripts are replaced by a differential suite under `tests/`,
run with `make test`. Every predicate is evaluated both through the index and
through a sequential scan, and the two must agree on the rows returned, not
merely on how many. See [Development](#development).

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

Regular expressions are supported where they reduce exactly to one of the
above:

```sql
SELECT * FROM users WHERE name ~ '^john';        -- prefix     -> 'john%'
SELECT * FROM users WHERE name ~ 'son$';         -- suffix     -> '%son'
SELECT * FROM users WHERE name ~ '^j.hn';        -- wildcard   -> 'j_hn%'
SELECT * FROM users WHERE name ~ '^.{8}$';       -- length     -> '________'
SELECT * FROM users WHERE name ~ '^j.*n$';       -- both ends  -> 'j%n'
```

Patterns outside the subset — `~ '^(john|jane)'`, `~ '[0-9]+'` — still return
the right rows, but are answered by a sequential scan. `EXPLAIN` shows which
you got.

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

The regex operators follow the same split, because they are answered from the
same structures: `~` and `!~` are available on `biscuit_ops` and
`biscuit_like_ops`, and `~*` and `!~*` on `biscuit_ops` and
`biscuit_ilike_ops`. An index built with the narrower class is simply not
considered by the planner for the operators it cannot answer.

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
| Anchored regex `^abc`, `^a.{3}z$` | Typically fastest | Applicable | Not applicable |
| Regex with alternation or classes | Not accelerated | Supported | Not applicable |
| Similarity / fuzzy search | Not supported | Supported | Not applicable |

**Biscuit is a good fit for** anchored patterns, patterns containing `_`
wildcards, length predicates, `ILIKE`-heavy workloads, and queries where exact
results without a heap recheck are valuable — `COUNT(*)` in particular.

For anchored patterns, `ILIKE` is evaluated over its own structure set rather
than by rewriting the query, and in testing performed comparably to the
equivalent `LIKE`. Case-insensitive anchored search therefore needs no
`lower()` expression index.

**Other options are often preferable for** selective prefix lookups, where a
B-tree is smaller and quicker to build; unanchored substring search, for which
`pg_trgm` is purpose-built; and general regular-expression or similarity
matching, which Biscuit does not accelerate.

The regex split follows the same logic as the `LIKE` one, because it is the
same machinery underneath: an anchored regex reduces to a fixed number of
bitmap intersections, whereas `pg_trgm` extracts trigrams from an arbitrary
regex and is not restricted to a decomposable subset. On a 200,000-row test
table the two were close to complementary — Biscuit far ahead on anchored and
wildcard-position patterns, `pg_trgm` far ahead on unanchored and
character-class ones. Neither ordering is a property of the indexes alone;
benchmark against your own data.

Running Biscuit alongside a `pg_trgm` GIN index and letting the planner select
between them is a practical arrangement, and the cost model is written with it
in mind. For unanchored patterns in particular, confirm the plan you expect
with `EXPLAIN` against your own data and query mix.

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

### Regular expressions

Biscuit has no regex engine. A regex qual is rewritten at plan time into an
equivalent `LIKE` glob and handed to the machinery above, so everything from
selectivity scoring to the bitmap intersections is unchanged.

The rewrite has to be *exact* — the glob must match precisely the same strings
as the regex — because Biscuit does not recheck candidates against the heap. An
approximate rewrite would not be slower, it would be wrong. Anything that
cannot be proven exact is refused.

Since `~` is unanchored while `LIKE` matches the whole string, a missing anchor
becomes `%`:

```
^abc$      ->  abc          exact match
^abc       ->  abc%         prefix
abc$       ->  %abc         suffix
abc        ->  %abc%        infix (~ is unanchored)
^a.c$      ->  a_c          wildcard position
^a.{3}z$   ->  a___z        repetition count
^a.*z$     ->  a%z          both-anchored
^usr_1     ->  usr\_1%      '_' is a regex literal, escaped for LIKE
```

What is **not** decomposable: alternation `(a|b)`, bracket expressions `[abc]`,
groups, unbounded or optional repetition of a specific character (`a*`, `a+`,
`a?` — note that `a*` is not `a%`, which would require the `a`), bounded ranges
`{n,m}`, the `\d`/`\w`/`\b` shorthands, backreferences, and embedded-option
directives. These are answered by a sequential scan.

Three asymmetries are worth knowing:

* **`~*` sets a recheck, and `!~*` is not accelerated.** `~*` is rewritten onto
  `ILIKE`, but PostgreSQL's regex case folding and `ILIKE`'s `lower()`-based
  folding are different relations. They disagree in both directions on
  characters such as `İ`, `ß` and the `ǅ`/`ǈ`/`ǋ` titlecase family — `ILIKE`
  matching where the regex does not, and vice versa. `~*` is therefore
  decomposed only for pure-ASCII patterns under a collation Biscuit judges
  safe (next point), which confines the remaining disagreement to the
  direction where `ILIKE` over-matches, and the executor rechecks to remove
  the surplus. `!~*` is never decomposed: the complement of an over-matching
  set is missing rows, and no recheck can add rows back. `~` and `!~` are
  unaffected and remain exact.

* **`~*`/`!~*` decomposition is refused under a collation it cannot prove
  safe for the pattern shape.** Nondeterministic collations are refused
  outright (PostgreSQL's own regex engine does not support them either, so
  this only guards a future core change). Under an ICU collation, decomposition
  is additionally refused for any pattern that depends on character
  *position* — one containing regex `.`, which becomes LIKE's `_` — because
  ICU's `lower()` maps `İ` (U+0130) to two characters where the database's
  default/libc `lower()` maps it to one, and that length change can shift a
  `_`-aligned match out from under a row `~*` would otherwise match. That is
  an under-match, which a recheck (a pure filter) cannot repair, so the safer
  choice is to not decompose rather than risk it. A plain `%literal%` shape
  has no position to shift and stays decomposable under ICU. Deterministic
  non-ICU collations (the default/libc case above) are unaffected by this
  point.

* **Unanchored regexes inherit the unanchored `LIKE` cost.** `~ 'abc'` becomes
  `LIKE '%abc%'` and is priced as an infix pattern, which the planner will
  often decline. Check with `EXPLAIN` if it matters.

Note that a `Bitmap Heap Scan` prints `Recheck Cond:` for every plan, whatever
the index reported, so it is not a signal of which of the above applies. What
`EXPLAIN` does tell you reliably is whether the index was used at all: a regex
outside the subset shows a `Seq Scan` even with `enable_seqscan = off`, because
the path is priced as unusable rather than merely expensive.

### Query planning

`biscuit_costestimate()` prices a pattern by its shape:

| Shape | Basis |
|---|---|
| Anchored (prefix and/or suffix) | Small fraction of the sequential-scan baseline |
| Length predicate (`_` only) | Single bitmap lookup |
| Unanchored infix | Scales with row count and the square of average string length |
| Multi-part infix | Discounted relative to a single part |
| All-wildcard (`%`) | No index path offered |

A regex is priced by the shape of the glob it decomposes to, using the same
rewrite the executor will perform, so the cost model and the executor cannot
disagree about what a given regex costs. A regex outside the subset is priced
as unusable, which keeps the recheck fallback a correctness backstop rather
than a plan the planner would actually choose.

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
the corresponding heap write. Substantial WAL is characteristic of maintaining
any secondary text-search structure, and in testing Biscuit's WAL volume per
row was comparable to a `pg_trgm` GIN index on the same data. WAL per row also
grows as the index grows, so a measurement taken on a small index will
understate a large one. `DELETE` is much cheaper, as it records a tombstone
rather than rewriting structures.

Sustained inserts against a live index can therefore consume WAL space quickly.
Size `pg_wal` accordingly and monitor free space. Where replication slots are in
use, consider setting `max_slot_wal_keep_size` so a lagging or disconnected
standby cannot retain WAL indefinitely. WAL volume also affects how long crash
recovery takes to replay, which is worth allowing for when planning restart
windows.

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
| Point-in-time recovery | Yes | Index state reconstructed from archived WAL |
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
| Regular expressions | Partial | Decomposable subset accelerated; the rest is correct but not accelerated |
| Exact results without recheck | Yes | Except `~*`, which rechecks |
| Similarity / fuzzy search | No | |
| Locale-aware collation | No | Comparisons are byte-based |

Biscuit index scans run serially. Parallel Bitmap Heap Scan and parallel
sequential scan still work as usual.

---

## Configuration

### Build options

Enabling CRoaring is recommended for better bitmap performance:

```bash
make WITH_ROARING=1 && sudo make install
```

Confirm it is actually active rather than assuming — the flag has to reach the
compiler, not just the linker:

```sql
SELECT biscuit_has_roaring();   -- t when CRoaring is compiled in
```

### Index options

Biscuit does not currently expose per-index (`WITH (...)`) options.

Two database-wide settings are available:

* `biscuit.delta_compaction_slots` (default `20000`) — how many pending
  writes can build up before Biscuit compacts them. Raise it for large write
  bursts; lower it to keep each compaction quick.
* `biscuit.diag_scan_trace` (default `off`) — detailed scan logging for
  troubleshooting. Leave it off otherwise.

---

## Development

```bash
git clone https://github.com/Crystallinecore/biscuit.git
cd biscuit
make clean
CFLAGS="-g -O0 -DDEBUG" make
sudo make install
make test
```

### Testing

The test suite lives in `tests/`, one file per category:

```bash
make test                                    # everything except crash recovery
make test-all                                # including it
make check-suite CATEGORIES="03_regex 08_unicode"   # selected categories
make check-wal                               # crash recovery only
```

Every case is differential. The same predicate is evaluated twice against the
same rows — once with index paths disabled, so PostgreSQL's own matching over a
sequential scan acts as the oracle, and once with sequential scans disabled —
and the two must agree on the row count *and* on a fingerprint of which rows
came back. Two scans can agree on `COUNT(*)` and still return different rows.

Each case also declares whether the index must serve it, must not, or either,
so a supported pattern that stops being accelerated and an unsupported one that
starts being served both fail the suite.

The categories are `01_like`, `02_ilike`, `03_regex`, `04_composition`,
`05_multicolumn`, `06_opclass`, `07_dml_mvcc`, `08_unicode`, `09_wal` and
`10_stress`. Each file asserts internally and raises on failure, so exit status
is the result and there is no expected-output file to maintain. None contain
psql-specific syntax, so they can be run through any client; the only step
needing a shell is the crash in the middle of `09_wal`.

`make check-wal` **stops and restarts the server** with no clean shutdown, which
is the point of the test — do not aim it at a cluster you care about. `make
test` excludes it, and `SKIP_WAL=1` does the same for a direct
`tests/run_all.sh` invocation.

Standard libpq variables (`PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`) apply.
`PGDATA` is needed only by the crash category and is discovered from the server
itself when unset.

Changes to the scan or cache paths should also be accompanied by a
**two-session** test: one session queries the index, a second commits a change,
and the first must then observe it. Single-session tests do not exercise cache
invalidation. It is worth comparing index and sequential-scan results for the
same predicate *within a single snapshot* while another session writes
concurrently, so that any divergence is attributable to the index rather than
to timing — and inspecting the server log as well as client output, since a
backend can emit diagnostics that never reach the client driving the test.

---

## Roadmap

- [ ] Incremental cache refresh in place of full reload on invalidation
- [ ] Reduced write amplification
- [ ] Index-only scan support (`amcanreturn`)
- [ ] Runtime-configurable cost-model parameters
- [x] Regular-expression support via glob decomposition *(3.1.0, for the
      exactly-decomposable subset)*
- [ ] Wider regex coverage — alternation and character classes, which need
      more than a single glob to express
- [ ] Recalibrate the unanchored cost model, which currently declines infix
      patterns the index can serve
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
