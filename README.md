# Biscuit - High-Performance Pattern Matching Index for PostgreSQL

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PostgreSQL: 16+](https://img.shields.io/badge/PostgreSQL-16%2B-blue.svg)](https://www.postgresql.org/)
[![Read the Docs](https://img.shields.io/badge/Read%20the%20Docs-8CA1AF?logo=readthedocs&logoColor=fff)](https://biscuit.readthedocs.io/)


**Biscuit** is a specialized PostgreSQL index access method (IAM) designed for blazing-fast pattern matching on `LIKE` and `ILIKE` queries, with native support for multi-column searches. It eliminates the recheck overhead of trigram indexes while delivering significant performance improvements on wildcard-heavy queries. It stands for _**B**itmap **I**ndexed **S**earching with **C**omprehensive **U**nion and **I**ntersection **T**echniques_.

---
## Stability Notice

This extension is currently under active development and has not yet received the level of testing and operational experience expected of production-ready software.

Users are encouraged to evaluate the extension thoroughly in development and staging environments before considering deployment in production systems. In particular, testing should include representative datasets, workloads, upgrade procedures, backup and recovery workflows, and performance validation.

Although the extension is intended to operate safely and reliably, defects or unexpected behavior may still be present. As with any new database component, appropriate backups and validation procedures should be maintained before use.

At this stage, the extension is best suited for evaluation, experimentation, and non-critical workloads. Production deployment should be undertaken only after careful testing and assessment of its suitability for the intended environment.

---
## Version - 2.2.3

### Structural Changes
 
- **Monolith split into modules.** The single `biscuit.c` file has been
  decomposed into focused translation units, each with its own header:
  | Module | Responsibility |
  |---|---|
  | `biscuit.c` | AM handler, SQL-callable functions, `_PG_init` |
  | `biscuit_bitmap.{c,h}` | Roaring bitmap abstraction + fallback bitset |
  | `biscuit_cache.{c,h}` | Session-scoped index cache |
  | `biscuit_index.{c,h}` | Index build, load, disk I/O, CRUD helpers |
  | `biscuit_pattern.{c,h}` | LIKE/ILIKE pattern parsing and bitmap matching |
  | `biscuit_preload.{c,h}` | Background preload worker and skeleton loader |
  | `biscuit_scan.{c,h}` | Scan lifecycle (beginscan/rescan/gettuple/getbitmap/endscan) |
  | `biscuit_tid.{c,h}` | TID sorting (radix + qsort) and parallel collection |
  | `biscuit_utf8.{c,h}` | UTF-8 character utilities and Datum→text helpers |
  All shared types, constants, and macros have been consolidated into
  `biscuit_common.h`. No SQL-level API changes.
  
- **Version bumped to `2.2.3`** (`BISCUIT_LIBRARY_VERSION`).

### New Features

- **PostgreSQL 19 Beta 1 support.** `PG_MODULE_MAGIC_EXT` (introduced in PG 19)
  is now used when available, with a fallback to `PG_MODULE_MAGIC` for older
  versions. The extension can now be built and loaded against PG 19 development
  builds without modification.


### Improvements

- **Memory context correctness.** The session cache (`biscuit_cache.c`) now
  explicitly switches to `CacheMemoryContext` before allocating cache list
  nodes, ensuring index structures survive transaction boundaries without
  relying on caller context. The `biscuit_cleanup_index` stub correctly avoids
  double-freeing memory owned by the context.

- **`biscuit_complete_preload_local()`** added as a fast in-process upgrade
  path: rebuilds bitmaps from the already-resident string cache without
  reopening the relation or re-scanning the heap. Used by `beginscan` when
  it detects the worker has finished between queries.

- **TID collection refactored into `biscuit_tid.c`.** The unified entry point
  `biscuit_collect_tids_optimized()` selects parallel vs. single-threaded
  collection automatically and supports an optional `limit_hint` to avoid
  collecting more TIDs than the executor needs.

- **Fallback scan in `biscuit_preload.c`** supports NOT LIKE and NOT ILIKE
  during warm-up via a hash-map TID→record-index lookup, maintaining correct
  inversion semantics without bitmaps.

- **UTF-8 helpers isolated in `biscuit_utf8.{c,h}`**, removing scattered
  inline character-length and lowercase conversion code from the pattern and
  index modules.

- **`biscuit_columnindex_memory_usage()`** now validates `max_length >= 0`
  before iterating length bitmap arrays and emits a `WARNING` on corrupt
  state rather than reading out-of-bounds.

### Bug Fixes

- `biscuit_cache_remove()` no longer calls `pfree` on list nodes; they are
  owned by `CacheMemoryContext` and must not be freed manually.


---

##  **Installation**

### **Requirements**
- Build tools: `gcc`, `make`, `pg_config`
- Recommended: CRoaring library for enhanced performance

### **From Source**

```bash
# Clone repository
git clone https://github.com/Crystallinecore/biscuit.git
cd biscuit

# Build and install
make
sudo make install

# Enable in PostgreSQL
psql -d your_database -c "CREATE EXTENSION biscuit;"
```

### **From PGXN**

```bash
pgxn install biscuit
psql -d your_database -c "CREATE EXTENSION biscuit;"
```

---

##  **Quick Start**

### **Basic Usage**

```sql
-- Create a Biscuit index
CREATE INDEX idx_users_name ON users USING biscuit(name);

-- Query with wildcard patterns
SELECT * FROM users WHERE name LIKE '%john%';
SELECT * FROM users WHERE name NOT LIKE 'a%b%c';
SELECT COUNT(*) FROM users WHERE name LIKE '%test%';
```

### **Multi-Column Indexes**

```sql
-- Create multi-column index
CREATE INDEX idx_products_search 
ON products USING biscuit(name, description, category);

-- Multi-column query (optimized automatically)
SELECT * FROM products 
WHERE name LIKE '%widget%' 
  AND description LIKE '%blue%'
  AND category LIKE 'electronics%'
LIMIT 10;
```

---

##  **How It Works**

### **Core Concept: Bitmap Position Indices**

Biscuit builds the following bitmaps for every string:

#### **1. Positive Indices (Forward)**
Tracks which records have character `c` at position `p`:

```
String: "Hello"
Bitmaps:
  H@0 → {record_ids...}
  e@1 → {record_ids...}
  l@2 → {record_ids...}
  l@3 → {record_ids...}
  o@4 → {record_ids...}
```

#### **2. Negative Indices (Backward)**
Tracks which records have character `c` at position `-p` from the end:

```
String: "Hello"
Bitmaps:
  o@-1 → {record_ids...}  (last char)
  l@-2 → {record_ids...}  (second to last)
  l@-3 → {record_ids...}
  e@-4 → {record_ids...}
  H@-5 → {record_ids...}
```

#### **3. Positive Indices (Case-insensitive)**
Tracks which records have character `c` at position `p`:

```
String: "Hello"
Bitmaps:
  h@0 → {record_ids...}
  e@1 → {record_ids...}
  l@2 → {record_ids...}
  l@3 → {record_ids...}
  o@4 → {record_ids...}
```

#### **4. Negative Indices (Case-insensitive)**
Tracks which records have character `c` at position `-p` from the end:

```
String: "Hello"
Bitmaps:
  o@-1 → {record_ids...}  (last char)
  l@-2 → {record_ids...}  (second to last)
  l@-3 → {record_ids...}
  e@-4 → {record_ids...}
  h@-5 → {record_ids...}
```

#### **5. Length Bitmaps**
Two types for fast length filtering:
- **Exact length**: `length[5]` → all 5-character strings
- **Minimum length**: `length_ge[3]` → all strings ≥ 3 characters

---

### **Pattern Matching Algorithm**

#### **Example: `LIKE 'abc%def'`**

**Step 1: Parse pattern into parts**
```
Parts: ["abc", "def"]
Starts with %: NO
Ends with %: NO
```

**Step 2: Match first part as prefix**
```sql
-- "abc" must start at position 0
Candidates = pos[a@0] ∩ pos[b@1] ∩ pos[c@2]
```

**Step 3: Match last part at end (negative indexing)**
```sql
-- "def" must end at string end
Candidates = Candidates ∩ neg[f@-1] ∩ neg[e@-2] ∩ neg[d@-3]
```

**Step 4: Apply length constraint**
```sql
-- String must be at least 6 chars (abc + def)
Candidates = Candidates ∩ length_ge[6]
```

**Result: Exact matches, zero false positives**

---

### **Why It's Fast**

#### **1. Pure Bitmap Operations**
```c
// Traditional approach (pg_trgm)
for each trigram in pattern:
    candidates = scan_trigram_index(trigram)
    for each candidate:
        if !heap_fetch_and_recheck(candidate):  // SLOW: Random I/O
            remove candidate

// Biscuit approach
for each character at position:
    candidates &= bitmap[char][pos]  // FAST: In-memory AND
// No recheck needed!
```

#### **2. Roaring Bitmaps**
Compressed bitmap representation:
- Sparse data: array of integers
- Dense data: bitset
- Automatic conversion for optimal memory

#### **3. Negative Indexing Optimization**
```sql
-- Pattern: '%xyz'
-- Traditional: Scan all strings, check suffix
-- Biscuit: Direct lookup in neg[z@-1] ∩ neg[y@-2] ∩ neg[x@-3]
```

---

##  **12 Performance Optimizations**

### **1. Skip Wildcard Intersections**
```c
// Pattern: "a_c" (underscore = any char)
// OLD: Intersect all 256 chars at position 1
// NEW: Skip position 1 entirely, only check a@0 and c@2
```

### **2. Early Termination on Empty**
```c
result = bitmap[a][0];
result &= bitmap[b][1];
if (result.empty()) return empty;  // Don't process remaining chars
```

### **3. Avoid Redundant Bitmap Copies**
```c
// OLD: Copy bitmap for every operation
// NEW: Operate in-place, copy only when branching
```

### **4. Optimized Single-Part Patterns**
Fast paths for common cases:
- **Exact**: `'abc'` → Check position 0-2 and length = 3
- **Prefix**: `'abc%'` → Check position 0-2 and length ≥ 3
- **Suffix**: `'%xyz'` → Check negative positions -3 to -1 and length ≥ 3
- **Substring**: `'%abc%'` → Check all positions, OR results

### **5. Skip Unnecessary Length Operations**
```c
// Pure wildcard patterns
if (pattern == "%%%___%%")  // 3 underscores
    return length_ge[3];     // No character checks needed!
```

### **6. TID Sorting for Sequential Heap Access**
```c
// Sort TIDs by (block_number, offset) before returning
// Converts random I/O into sequential I/O
// Uses radix sort for >5000 TIDs, quicksort for smaller sets
```

### **7. Batch TID Insertion**
```c
// For bitmap scans, insert TIDs in chunks
for (i = 0; i < num_results; i += 10000) {
    tbm_add_tuples(tbm, &tids[i], batch_size, false);
}
```

### **8. Direct Roaring Iteration**
```c
// OLD: Convert bitmap to array, then iterate
// NEW: Direct iterator, no intermediate allocation
roaring_uint32_iterator_t *iter = roaring_create_iterator(bitmap);
while (iter->has_value) {
    process(iter->current_value);
    roaring_advance_uint32_iterator(iter);
}
```


### **9. Batch Cleanup on Threshold**
```c
// After 1000 deletes, clean tombstones from all bitmaps
if (tombstone_count >= 1000) {
    for each bitmap:
        bitmap &= ~tombstones;  // Batch operation
    tombstones.clear();
}
```

### **10. Aggregate Query Detection**
```c
// COUNT(*), EXISTS, etc. don't need sorted TIDs
if (!scan->xs_want_itup) {
    skip_sorting = true;  // Save sorting time
}
```

### **11. LIMIT-Aware TID Collection**
```c
// If LIMIT 10 in query, don't collect more than needed
if (limit_hint > 0 && collected >= limit_hint)
    break;  // Early termination
```

### **12. Multi-Column Query Optimization**

#### **Predicate Reordering**
Analyzes each column's pattern and executes in order of selectivity:

```sql
-- Query:
WHERE name LIKE '%common%'           -- Low selectivity
  AND sku LIKE 'PROD-2024-%'         -- High selectivity (prefix)
  AND description LIKE '%rare_word%' -- Medium selectivity

-- Execution order (Biscuit automatically reorders):
1. sku LIKE 'PROD-2024-%'         (PREFIX, priority=20, selectivity=0.02)
2. description LIKE '%rare_word%' (SUBSTRING, priority=35, selectivity=0.15)
3. name LIKE '%common%'           (SUBSTRING, priority=55, selectivity=0.60)
```

**Selectivity scoring formula:**
```
score = 1.0 / (concrete_chars + 1)
      - (underscore_count × 0.05)
      + (partition_count × 0.15)
      - (anchor_strength / 200)
```

**Priority tiers:**
1. **0-10**: Exact matches, many underscores
2. **10-20**: Non-% patterns with underscores
3. **20-30**: Strong anchored patterns (prefix/suffix)
4. **30-40**: Weak anchored patterns
5. **40-50**: Multi-partition patterns
6. **50-60**: Substring patterns (lowest priority)

---

##  **Benchmarking**

### **Setup Test Data**

```sql
-- Create 1M row test table
CREATE TABLE benchmark (
    id SERIAL PRIMARY KEY,
    name TEXT,
    description TEXT,
    category TEXT,
    score FLOAT
);

INSERT INTO benchmark (name, description, category, score)
SELECT 
    'Name_' || md5(random()::text),
    'Description_' || md5(random()::text),
    'Category_' || (random() * 100)::int,
    random() * 1000
FROM generate_series(1, 1000000);

-- Create indexes
CREATE INDEX idx_trgm ON benchmark 
    USING gin(name gin_trgm_ops, description gin_trgm_ops);

CREATE INDEX idx_biscuit ON benchmark 
    USING biscuit(name, description, category);

ANALYZE benchmark;
```

### **Run Benchmarks**

```sql
-- Single column, simple pattern
EXPLAIN ANALYZE
SELECT * FROM benchmark WHERE name LIKE '%abc%' LIMIT 100;

-- Multi-column, complex pattern
EXPLAIN ANALYZE
SELECT * FROM benchmark 
WHERE name LIKE '%a%b' 
  AND description LIKE '%bc%cd%'
ORDER BY score DESC 
LIMIT 10;

-- Aggregate query (COUNT)
EXPLAIN ANALYZE
SELECT COUNT(*) FROM benchmark 
WHERE name LIKE 'a%l%' 
  AND category LIKE 'f%d';

-- Complex multi-part pattern
EXPLAIN ANALYZE
SELECT * FROM benchmark 
WHERE description LIKE 'u%dc%x'
LIMIT 50;
```

### **View Index Statistics**

```sql
-- Show internal statistics
SELECT biscuit_index_stats('idx_biscuit'::regclass);
```

**Output:**
```
----------------------------------------------------
 Biscuit Index Statistics (FULLY OPTIMIZED)        +
 ==========================================        +
 Index: idx_biscuit                                +
 Active records: 1000002                           +
 Total slots: 1000002                              +
 Free slots: 0                                     +
 Tombstones: 0                                     +
 Max length: 44                                    +
 ------------------------                          +
 CRUD Statistics:                                  +
   Inserts: 0                                      +
   Updates: 0                                      +
   Deletes: 0                                      +
 ------------------------                          +
 Active Optimizations:                             +
   ✓ 1. Skip wildcard intersections                +
   ✓ 2. Early termination on empty                 +
   ✓ 3. Avoid redundant copies                     +
   ✓ 4. Optimized single-part patterns             +
   ✓ 5. Skip unnecessary length ops                +
   ✓ 6. TID sorting for sequential I/O             +
   ✓ 7. Batch TID insertion                        +
   ✓ 8. Direct bitmap iteration                    +
   ✓ 9. Parallel bitmap scan support               +
   ✓ 10. Batch cleanup on threshold                +
   ✓ 11. Skip sorting for bitmap scans (aggregates)+
   ✓ 12. LIMIT-aware TID collection                +
```


---

##  **Use Cases**

### **1. Full-Text Search Applications**
```sql
-- E-commerce product search
CREATE INDEX idx_products ON products 
    USING biscuit(name, brand, description);

SELECT * FROM products 
WHERE name LIKE '%laptop%' 
  AND brand LIKE 'ABC%'
  AND description LIKE '%gaming%'
ORDER BY price DESC 
LIMIT 20;
```

### **2. Log Analysis**
```sql
-- Search error logs
CREATE INDEX idx_logs ON logs 
    USING biscuit(message, source, level);

SELECT * FROM logs 
WHERE message LIKE '%ERROR%connection%timeout%'
  AND source LIKE 'api.%'
  AND timestamp > NOW() - INTERVAL '1 hour'
LIMIT 100;
```

### **3. Customer Support / CRM**
```sql
-- Search tickets by multiple fields
CREATE INDEX idx_tickets ON tickets 
    USING biscuit(subject, description, customer_name);

SELECT * FROM tickets 
WHERE subject LIKE '%refund%'
  AND customer_name LIKE 'John%'
  AND status = 'open';
```

### **4. Code Search / Documentation**
```sql
-- Search code repositories
CREATE INDEX idx_files ON code_files 
    USING biscuit(filename, content, author);

SELECT * FROM code_files 
WHERE filename LIKE '%.py'
  AND content LIKE '%def%parse%json%'
  AND author LIKE 'team-%';
```

### **5. Analytics with Aggregates**
```sql
-- Fast COUNT queries (no sorting overhead)
CREATE INDEX idx_events ON events 
    USING biscuit(event_type, user_agent, referrer);

SELECT COUNT(*) FROM events 
WHERE event_type LIKE 'click%'
  AND user_agent LIKE '%Mobile%'
  AND referrer LIKE '%google%';
```

---

##  **Configuration**

### **Build Options**

Enable CRoaring for better performance:


### **Index Options**

Currently, Biscuit doesn't expose tunable options. All optimizations are automatic.

---

##  **Limitations and Trade-offs**

### **What Biscuit Does NOT Support**

1. **Regular expressions** - Only `LIKE` / `ILIKE` patterns with `%` and `_`
2. **Locale-specific collations** - String comparisons are byte-based
3. **Amcanorder = false** - Cannot provide ordered scans directly (but see below)

### **ORDER BY + LIMIT Behavior**

Biscuit doesn't support ordered index scans (`amcanorder = false`), BUT:

**PostgreSQL's planner handles this efficiently:**
```sql
SELECT * FROM table WHERE col LIKE '%pattern%' ORDER BY score LIMIT 10;
```

**Execution plan:**
```
Limit
  -> Sort (cheap, small result set)
    -> Biscuit Index Scan (fast filtering)
```

**Why this works:**
- Biscuit filters candidates extremely fast 
- Result set is small after filtering
- Sorting 100-1000 rows in memory is negligible (<1ms)
- **Net result**: Still much faster than pg_trgm with recheck overhead in many cases

### **Memory Usage**

Biscuit stores bitmaps in memory:
- Use `REINDEX` to rebuild if index grows too large

### **Write Performance**

- **INSERT**: Similar to B-tree (must update bitmaps)
- **UPDATE**: Two operations (remove old, insert new)
- **DELETE**: Marks as tombstone, batch cleanup at threshold

---

##  **Comparison with pg_trgm**

| Feature                  | Biscuit                     | pg_trgm (GIN)        |
|--------------------------|------------------------------|----------------------|
| **Wildcard patterns**    | ✔ Native              | ✔ Approximate        |
| **Recheck overhead**     | ✔ None (deterministic)       | ✗ Required    |
| **Regex support**        | ✗ No                         | ✔ Yes                |
| **Similarity search**    | ✗ No                         | ✔ Yes                |
| **ILIKE support**        | ✔ Full       | ✔ Native             |


**When to use Biscuit:**
- Wildcard-heavy `LIKE` / `ILIKE` queries (`%`, `_`)
-  Multi-column pattern matching
-  Need exact results (no false positives)
-  `COUNT(*)` / aggregate queries
-  High query volume, can afford memory

**When to use pg_trgm:**
- Fuzzy/similarity search (`word <-> pattern`)
- Regular expressions
- Memory-constrained environments
- Write-heavy workloads

---

## **Development**

### **Build from Source**

```bash
git clone https://github.com/Crystallinecore/biscuit.git
cd biscuit

# Development build with debug symbols
make clean
CFLAGS="-g -O0 -DDEBUG" make

# Run tests
make installcheck

# Install
sudo make install
```

### **Testing**

```bash
# Unit tests
make installcheck

# Manual testing
psql -d testdb

CREATE EXTENSION biscuit;

-- Create test table
CREATE TABLE test (id SERIAL, name TEXT);
INSERT INTO test (name) VALUES ('hello'), ('world'), ('test');

-- Create index
CREATE INDEX idx_test ON test USING biscuit(name);

-- Test queries
EXPLAIN ANALYZE SELECT * FROM test WHERE name LIKE '%ell%';
```

### **Debugging**

Enable PostgreSQL debug logging:

```sql
SET client_min_messages = DEBUG1;
SET log_min_messages = DEBUG1;

-- Now run queries to see Biscuit's internal logs
SELECT * FROM test WHERE name LIKE '%pattern%';
```

---

##  **Contributing**

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing`)
3. Make your changes with tests
4. Submit a pull request

### **Areas for Contribution**

- [ ] Implement `amcanorder` for native sorted scans
- [ ] Add statistics collection for better cost estimation
- [ ] Support for more data types 
- [ ] Parallel index build
- [ ] Index compression options

---

##  **License**

MIT License - See LICENSE file for details.

---

## **Author**

Sivaprasad Murali
- Email: sivaprasad.off@gmail.com
- GitHub: [@Crystallinecore](https://github.com/Crystallinecore)

---


## **Acknowledgments**

* The PostgreSQL community for the extensible index access method (AM) framework
* **B-tree** and **pg_trgm** indexes that shaped the design space for pattern matching in PostgreSQL
* The **CRoaring** library for efficient compressed bitmap operations

---

## **Support**

- **Issues**: [GitHub Issues](https://github.com/Crystallinecore/biscuit/issues)
- **Discussions**: [GitHub Discussions](https://github.com/Crystallinecore/biscuit/discussions)
- **Documentation**: [ReadTheDocs Page](https://biscuit.readthedocs.io/) 
---

**Happy pattern matching! Grab a biscuit 🍪 when others feel half-baked!**

---
