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
- **Smart Query Planning**: Automatic reordering of predicates based on selectivity analysis
- **Memory Efficient**: Uses Roaring Bitmaps for compact in-memory representation

## Version 2.2.1

### 🐞 Bug Fixes

* **Fixed recursive pattern matching**

  Resolved incorrect behavior when evaluating nested or repeated wildcard patterns during recursive matching.

* **Corrected underscore (`_`) handling in single-column indexing**

  `_` now correctly operates on character-based offsets (not byte offsets), in accordance with SQL `LIKE` / `ILIKE` semantics, eliminating false matches in multi-byte UTF-8 text.


### 🛡️ Correctness & Stability

* Improved internal consistency between single-column and multi-column pattern evaluation paths.
* Resolved observed edge cases that could lead to incorrect matches under complex wildcard patterns.


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
