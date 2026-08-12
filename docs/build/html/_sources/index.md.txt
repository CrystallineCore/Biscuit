# Biscuit Documentation

Welcome to the official documentation for Biscuit.


```{toctree}
:maxdepth: 2
:caption: Contents:

installation
quickstart
api
architecture
patterns
multicolumn
benchmark
benchmark_roaring
benchmark_env
performance
tribute
faq
```


# Biscuit Index for PostgreSQL

**High-Performance Pattern Matching Index for LIKE Queries**

---

## What is Biscuit?

Biscuit is a specialized PostgreSQL index access method designed to dramatically accelerate `LIKE` pattern matching queries. Unlike traditional B-tree or GIN indexes, Biscuit uses bitmap-based character indexing to provide near-instantaneous pattern matching across single or multiple columns.

## Key Features

- **Blazing Fast**: Optimized for complex `LIKE` patterns including `%`, `_`, prefixes, suffixes, and substrings
- **Multi-Column Support**: Query across multiple columns simultaneously with intelligent predicate reordering
- **Aggregate Optimization**: Special optimizations for `COUNT(*)` and `EXISTS` queries
- **Full CRUD Support**: INSERT, UPDATE, DELETE operations with automatic index maintenance
- **Crash-Safe and Replicated**: WAL-logged storage, point-in-time recovery, and physical streaming replication
- **Smart Query Planning**: Automatic reordering of predicates based on selectivity analysis
- **Memory Efficient**: Uses Roaring Bitmaps for compact in-memory representation

> Biscuit suits **read-mostly, analytical** workloads: load data, build the
> index, then query. Writes against a live index generate considerably more WAL
> than the heap writes alone, and each backend holds its own cached copy of the
> index, so plan for connection-pool size. See
> [Performance Tuning](performance.md).

## What's New — 3.0.0

### Durability

* **WAL-logged, crash-safe storage.** All index state lives in the index
  relation's own pages and is WAL-logged, replacing the earlier external-file
  snapshot mechanism. The index takes part in crash recovery, point-in-time
  recovery and physical streaming replication, including index scans served
  from a hot standby.

* **Cross-backend cache coherency.** Cached copies are validated against the
  metapage generation and reloaded when stale, so a backend reliably observes
  other backends' committed writes through the index.

### Query Execution

* **Candidate-mask threading across scan keys.** Conjunctive queries evaluate
  the most selective key first and restrict later keys to the surviving rows,
  rather than evaluating each key independently.

* **Length-predicate support.** Patterns made up only of `_` wildcards are
  recognised as length predicates and answered directly from the length
  bitmaps.

* **Rewritten cost model.** Costs derive from pattern shape, column statistics
  and relation size, so the planner can weigh Biscuit against `pg_trgm` and a
  sequential scan.

### Index Definition

* **`biscuit_like_ops` / `biscuit_ilike_ops` operator classes**, for columns
  that need only one case mode, avoiding the build and maintenance cost of the
  structure set they will never be queried with.

> **Upgrading:** this is a breaking on-disk format change. Indexes built under
> 2.x must be `REINDEX`ed; there is no automatic migration.

## Quick Start

```sql
-- Install the extension
CREATE EXTENSION biscuit;

-- Create a Biscuit index
CREATE INDEX idx_products_name ON products 
USING biscuit (name);

-- Query using LIKE patterns
SELECT * FROM products 
WHERE name LIKE '%laptop%';
```

## Documentation Structure

- **[Installation Guide](installation.md)** - Get started with Biscuit
- **[Quick Start Tutorial](quickstart.md)** - Your first Biscuit index in 5 minutes
- **[Pattern Syntax](patterns.md)** - Understanding LIKE pattern matching
- **[Multi-Column Indexes](multicolumn.md)** - Advanced multi-column queries
- **[Performance Tuning](performance.md)** - Optimize your queries
- **[API Reference](api.md)** - Complete function reference
- **[Benchmarks](benchmark.md)** - Benchmarks and other statistics
- **[FAQ](faq.md)** - Common questions and troubleshooting

## When to Use Biscuit

**Perfect For:**
- Text search with wildcards (`%product%`, `item_%`)
- Email/domain filtering (`%@company.com`)
- Prefix/suffix matching (`admin%`, `%_test`)
- Multi-column pattern queries
- High-cardinality string columns
- Frequent `LIKE/ILIKE` queries in analytics

**Not Ideal For:**
- Full-text search (use `tsvector` instead)
- Exact equality matches (B-tree is sufficient)
- Low-selectivity patterns (single `%`)
- Tables under continuous write load, or deployments with large connection pools

## System Requirements

- PostgreSQL 16 or higher
- Linux, macOS, or Windows
- Recommended: CRoaring library for enhanced performance

## License

Biscuit is open-source software released under the PostgreSQL License.

## Support

- **Issues**: [GitHub Issues](https://github.com/crystallinecore/biscuit)
- **Discussions**: [GitHub Discussions](https://github.com/crystallinecore/biscuit/discussions)
- **Email**: sivaprasad.off@gmail.com

---

**Ready to accelerate your pattern matching queries?** 

👉 [Start with the Installation Guide](installation.md)
