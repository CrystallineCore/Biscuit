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

## What's new in version 2.1.4?

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
