# Biscuit Documentation

Biscuit is a PostgreSQL index access method for `LIKE` and `ILIKE` pattern
matching. It also serves the regular-expression operators `~`, `!~`, `~*` and
`!~*` for the subset of regular expressions that can be rewritten exactly as a
`LIKE` pattern. This documentation describes version **3.1.0**.

```{toctree}
:maxdepth: 2
:caption: User Guide

installation
quickstart
patterns
regex
multicolumn
performance
faq
```

```{toctree}
:maxdepth: 2
:caption: Reference

api
architecture
development
tribute
changelog
```

```{toctree}
:maxdepth: 1
:caption: Historical Benchmarks (v2.1.3)

benchmark
benchmark_roaring
benchmark_env
```

---

## Overview

Biscuit evaluates a pattern by intersecting bitmaps that record which
character occurs at which position in each indexed string, counted both from
the start and from the end of the string, together with bitmaps that record
string length. The name stands for *Bitmap Indexed Searching with
Comprehensive Union and Intersection Techniques*.

Because the index records *position* rather than *content*, its performance
depends mainly on whether the pattern is anchored:

* **Anchored patterns** (`abc%`, `%xyz`, `abc%xyz`, `a_c`) resolve to a fixed
  number of bitmap intersections. This is where Biscuit is most effective.
* **Length predicates** (`'_____'`, `'_____%'`) are answered from the length
  bitmaps in a single lookup.
* **Unanchored patterns** (`%abc%`) must consider every candidate position, so
  their cost grows with row count and with the square of string length. For
  these, `pg_trgm` is often the better choice.

For `LIKE`, `ILIKE`, `~` and `!~`, index results are exact and PostgreSQL does
not recheck them against the heap. `~*` is the one supported operator that
requests a recheck; see [Regular Expressions](regex.md).

## Stability Notice

Biscuit is under active development and has not yet received the level of
testing and operational experience expected of production-ready software.
Evaluate it in development and staging environments first, with
representative data, workloads, upgrade procedures, and backup and recovery
workflows. Maintain backups and validation procedures as you would for any new
database component. At this stage Biscuit is best suited to evaluation,
experimentation and non-critical workloads.

## Suitability

Biscuit is designed for **read-mostly, analytical workloads**: load data,
build the index, then query. Index state is WAL-logged, so it takes part in
PostgreSQL's crash recovery, point-in-time recovery and physical replication.

Plan for the following characteristics before deploying (details in
[Performance and Operations](performance.md)):

* Writes against a live index generate substantially more WAL than the heap
  writes alone. Building the index after a bulk load is strongly recommended.
* Each backend holds its own in-memory copy of the index for the life of the
  connection, so memory use scales with the number of connections that use
  the index.
* A committed write by any backend invalidates cached copies in other
  backends, which reload the index on next use.
* The first query against an index in a new connection pays a load cost.

Biscuit is not currently recommended for OLTP tables, tables under continuous
write load, or deployments with large connection pools.

## What's New in 3.1.0

* **Regular-expression operators.** `~`, `!~`, `~*` and `!~*` are registered
  as strategies 5–8. A regex is rewritten at plan time into an equivalent
  `LIKE` pattern and evaluated by the existing bitmaps. Only rewrites that are
  exactly equivalent are used; other regular expressions are still answered
  correctly, by a sequential scan. See [Regular Expressions](regex.md).
* **Correctness fixes** that affect `LIKE` and `ILIKE` as well as regex:
  index builds now go through `table_index_build_scan()` (fixing HOT-chain
  mapping, partial-index predicates, dead-tuple handling and `reltuples`);
  anchored patterns no longer lose rows after crash recovery or drop their
  length constraint; a use-after-free in the pending-list snapshot is fixed;
  disabled index paths are no longer chosen on PostgreSQL 18; an unlogged
  index now fails cleanly with a `REINDEX` hint after a crash instead of a
  storage-level read error; and `-DHAVE_ROARING` is no longer silently dropped
  from the build.
* **Build change.** CRoaring is no longer auto-detected. Enable it explicitly
  with `make WITH_ROARING=1`.
* **Test suite.** A differential test suite under `tests/` replaces the
  previous test scripts. See [Development and Testing](development.md).
* **Upgrade.** There is no on-disk format change from 3.0.0, and no `REINDEX`
  is required. See [Upgrading](installation.md#upgrading).

The full list is in the [Changelog](changelog.md).

## Minimal Example

```sql
CREATE EXTENSION biscuit;

-- Load data first, then build the index.
CREATE INDEX idx_products_name ON products USING biscuit (name);
ANALYZE products;

SELECT * FROM products WHERE name LIKE 'Wireless%';   -- prefix
SELECT * FROM products WHERE name LIKE '%Mouse';      -- suffix
SELECT * FROM products WHERE name ~ '^W.reless';      -- regex, rewritten to 'W_reless%'
```

## Choosing an Index

The characterisations below come from testing in a single environment. Index
selection depends on the workload; benchmark against your own data and query
mix.

| Query shape | Biscuit | `pg_trgm` (GIN) | B-tree (`text_pattern_ops`) |
|---|---|---|---|
| Prefix `abc%` | Effective | Applicable | Typically fastest for selective prefixes |
| Suffix `%abc` | Effective | Applicable | Requires a `reverse()` expression index |
| Both-anchored `a%z` | Effective | Applicable | Not applicable |
| Unanchored infix `%abc%` | Applicable | Typically faster | Not applicable |
| Wildcard position `a_c` | Effective | Limited | Not applicable |
| Length only `______` | Supported | Not applicable | Not applicable |
| `ILIKE` | Effective (anchored) | Applicable | Requires a `lower()` expression index |
| Anchored regex `^abc`, `^a.{3}z$` | Effective | Applicable | Not applicable |
| Regex with alternation or classes | Not accelerated | Supported | Not applicable |
| Similarity / fuzzy search | Not supported | Supported | Not applicable |

Biscuit and a `pg_trgm` GIN index can be created on the same column; the
planner chooses between them per query. Confirm the plan you expect with
`EXPLAIN`, particularly for unanchored patterns. See
[Relationship to pg_trgm and B-tree](tribute.md).

## Requirements

* PostgreSQL 16 or later. Version differences between 16, 17 and 18 are
  handled at compile time; behaviour that differs by version is noted where
  relevant.
* A C compiler, `make` and the PostgreSQL server development files
  (`pg_config`)
* Optional, recommended: the CRoaring library

Biscuit is developed and tested on Linux.

## License

Biscuit is released under the MIT License. See the `LICENSE` file in the
repository.

## Support

* **Issues:** [GitHub Issues](https://github.com/crystallinecore/biscuit/issues)
* **Discussions:** [GitHub Discussions](https://github.com/crystallinecore/biscuit/discussions)
* **Email:** sivaprasad.off@gmail.com
