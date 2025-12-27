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

## Version 2.2.0

### ✨ Major Changes

**Switched from byte-based to character-based indexing**

* Biscuit now indexes **Unicode characters instead of raw UTF-8 bytes**.
* Eliminates incorrect behavior caused by multi-byte UTF-8 sequences being treated as independent index entries.
* Index structure now aligns with PostgreSQL’s character semantics rather than byte-level representation.

### 🛠️ UTF-8 & Internationalization Improvements

**Enhanced UTF-8 compatibility**

* Improved handling of multi-byte UTF-8 characters (e.g., accented Latin characters, non-Latin scripts).
* Index lookups, comparisons, and filtering now operate on logical characters rather than byte fragments.

**Correct UTF-8 support for `ILIKE`**

* `ILIKE` now works reliably with UTF-8 text, including case-insensitive matching on multi-byte characters.
* Fixes previously incorrect matches and missed results in non-ASCII datasets.

### 🐛 CRUD Correctness Fixes

**Resolved multiple CRUD-related bugs**

* Fixed inconsistencies during **INSERT**, **UPDATE**, and **DELETE** operations that could leave the index in an incorrect state.
* Ensured index entries are properly added, updated, and removed in sync with heap tuples.
* Improved stability under mixed read/write workloads.

### 🛡️ Correctness & Planner Consistency

* Improved alignment between Biscuit’s index behavior and PostgreSQL’s text semantics.
* Reduced false positives during pattern matching and eliminated character-splitting artifacts.
* More predictable planner behavior due to improved index consistency.

### 🔧 Internal Refactoring

* Refactored index layout and lookup logic to support character-aware traversal.
* Hardened UTF-8 decoding paths and edge-case handling.
* Simplified internal invariants for better maintainability and debugging.

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
