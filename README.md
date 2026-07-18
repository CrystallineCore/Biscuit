# Biscuit - High-Performance Pattern Matching Index for PostgreSQL

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PostgreSQL: 16+](https://img.shields.io/badge/PostgreSQL-16%2B-blue.svg)](https://www.postgresql.org/)
[![Read the Docs](https://img.shields.io/badge/Read%20the%20Docs-8CA1AF?logo=readthedocs&logoColor=fff)](https://biscuit.readthedocs.io/)


**Biscuit** is a PostgreSQL index access method (IAM) for `LIKE` and `ILIKE` pattern matching, with native support for multi-column indexes. It matches patterns using bitmap intersections over per-character position indices rather than trigram approximation, so results are exact and PostgreSQL does not need to recheck candidates against the heap (`xs_recheck = false`). Whether this is faster than a trigram (`pg_trgm`) index for a given workload depends on the data and query patterns involved — see [Comparison with pg_trgm](#comparison-with-pg_trgm) and benchmark it against your own data before relying on either. It stands for _**B**itmap **I**ndexed **S**earching with **C**omprehensive **U**nion and **I**ntersection **T**echniques_.

---
## Stability Notice

This extension is currently under active development and has not yet received the level of testing and operational experience expected of production-ready software.

Users are encouraged to evaluate the extension thoroughly in development and staging environments before considering deployment in production systems. In particular, testing should include representative datasets, workloads, upgrade procedures, backup and recovery workflows, and performance validation.

Although the extension is intended to operate safely and reliably, defects or unexpected behavior may still be present. As with any new database component, appropriate backups and validation procedures should be maintained before use.

At this stage, the extension is best suited for evaluation, experimentation, and non-critical workloads. Production deployment should be undertaken only after careful testing and assessment of its suitability for the intended environment.

---

## What's new in Version 3.0.0?

### New Features

* **WAL-logged, crash-safe on-disk storage.** Replaces the external-file snapshot mechanism (temp-file-then-rename, CRC32C checksum) introduced in 2.5.0 with fully in-relation, `GenericXLog`-protected page storage. Every persistent structure — per-character/length bitmaps, the TID array, tombstones, the free-slot list, and per-record string caches — now lives in one of two page-chain types:
  * a **compacted-blob chunk chain** (`biscuit_blob.c`/`.h`) storing a structure's serialized bytes across as many pages as needed, and
  * an **append-only pending-delta list chain**, mirroring GIN's pending-list design.

  A new per-column **directory** (`biscuit_dir.c`/`.h`) maps each structure's identity — `(col, is_lower, kind, char, position)` — to its blob-chain and pending-chain heads. Because every mutation goes through ordinary WAL-logged buffer writes, index state now survives a crash and replicates correctly, which the old flat-file snapshot never did.

* **Pending-list write path with opportunistic draining.** Steady-state `INSERT`/`UPDATE`/`DELETE` no longer rewrites a whole snapshot; each touched structure durably appends a small delta record to its own pending chain. Once a structure's pending chain exceeds `pending_list_limit` (a fixed 64 KB default stored in the metapage — there is currently no SQL-level setting to change it), it is opportunistically re-serialized ("drained") into a fresh compacted blob and its old blob/pending chains are retired to a deferred-recycle freelist. `VACUUM` (`biscuit_vacuumcleanup()`) additionally performs an unconditional full drain pass over every structure with outstanding pending records, and records lifetime `total_drains` / `total_pending_bytes` counters in the metapage, queryable via `biscuit_pending_list_stats()`.

* **Read-time pending-list reconciliation.** Query evaluation (`biscuit_pattern.c`) transparently merges any not-yet-drained pending records into the bitmap it reads, so a backend always sees a consistent view of a structure regardless of whether another backend's mutations have been drained yet — without ever triggering a drain itself.

* **New `biscuit_like_ops` / `biscuit_ilike_ops` operator classes.** In addition to the existing default `biscuit_ops` (which builds both case-sensitive and case-insensitive structures), a column can now be indexed with `biscuit_like_ops` (LIKE / NOT LIKE only) or `biscuit_ilike_ops` (ILIKE / NOT ILIKE only), skipping the build/maintenance cost of the structure set it will never be queried with. The mode is derived from the column's opfamily at build/load time and is never itself persisted, so it can't go stale across a `REINDEX` under a different opclass.

### Bug Fixes

* **Fixed off-by-one boundary reads in multi-column length-bitmap lookups.** Several `<=`-against-`max_length`/`max_length_lower` comparisons could read one `RoaringBitmap*` past the end of a palloc'd array on an ordinary LIKE/ILIKE query whose pattern length equaled the column's maximum indexed length. Tightened to `<` throughout.

* **Fixed `NOT LIKE` / `NOT ILIKE` inversion in multi-column scans.** The "all non-null rows" set used to build the inverted result always consulted the case-sensitive length-≥ bitmap, even for `NOT ILIKE`. It now selects the case-matching (`_lower` vs. non-`_lower`) array, consistent with each column's actual case-mode gating.

* **Fixed wildcard-unaware substring matching in multi-column `%needle%` queries.** Candidate verification used a literal `strstr()`, which treated `_` as an ordinary byte rather than a single-character wildcard. Replaced with the wildcard-aware `biscuit_wildcard_contains()`.

### Internal Changes

* **Removed the background preload worker entirely.** `biscuit_preload.c`/`.h` (skeleton loading, the shared-memory ring buffer, the background worker, and the `strstr`/`strcasestr` fallback scan used during warm-up) have been deleted. `beginscan()` now always resolves the index through the session cache or loads it — fully built, every bitmap included — synchronously from its on-disk directory via `biscuit_persist_load()`. There is no more warm-up window or degraded-scan period after a restart.

* **`biscuit_persist_save()` / `biscuit_persist_load()` / `biscuit_persist_drop()` rewritten** against the directory + blob/pending-chain machinery; the 2.5.0 flat-file-per-index snapshot mechanism is deleted outright, not version-gated. There is no dual-format reader — existing indexes must be `REINDEX`ed after upgrading.

* **Metapage format bumped (`BISCUIT_VERSION` 1 → 3).** Adds per-column directory roots, a deferred-recycle FSM freelist root, and pending-list tuning/observability fields. A new, independently-tracked `page_format_version` covers the binary layout of the individual page structs, separate from the extension-level format version.

* **New monotonic generation counter** (`idx->gen`, mirrored in the metapage) is bumped non-transactionally on every successful `INSERT`/`bulkdelete`, replacing the old `preload_state`-based staleness tracking.

* **`bulkdelete()` and the `UPDATE`-as-delete path now remove records one at a time** through `biscuit_remove_from_all_indices()`, each durably recording its removal via a pending-list append, in place of the old bulk `andnot_inplace()` sweep that depended on an eager whole-index resave for durability.

* **`biscuit_cache.c`'s proc-exit callback simplified.** Every mutation is now durable at the moment it's WAL-logged, so there is nothing left to flush at backend shutdown; the callback just drops the process-local cache. (It can no longer call `biscuit_persist_save()`, since that now requires opening a real `Relation`, which isn't safe this late in shutdown — the proc-exit-flush design itself is slated for removal in a later change.)

* Simplified `biscuit_costestimate()`.

### Testing

* Added `tests/crud.sql`, an end-to-end CRUD maintenance test that compares sequential-scan and Biscuit-index-scan row counts across INSERT, UPDATE, and DELETE phases.
* Benchmark scripts (`tests/forced_index_usage.sh`, `tests/planner_usage.sh`) now target an explicit PostgreSQL port/cluster instead of the system default, and report index size via `pg_relation_size()`.

### Upgrade Notes

**This is a breaking on-disk format change.** Indexes built under 2.x must be `REINDEX`ed after upgrading — there is no automatic migration or dual-format reader.

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

### **Operator Classes**

Biscuit ships three operator classes. The default, `biscuit_ops`, builds both the case-sensitive and case-insensitive structures for a column, so it supports `LIKE`/`NOT LIKE` and `ILIKE`/`NOT ILIKE` on the same index. If a column only ever needs one case mode, `biscuit_like_ops` or `biscuit_ilike_ops` build only the structure set that operator class needs, reducing build time and memory for that column:

```sql
-- Default: supports both LIKE and ILIKE
CREATE INDEX idx_name ON users USING biscuit (name);

-- LIKE / NOT LIKE only (skips building the case-insensitive structures)
CREATE INDEX idx_name_like ON users USING biscuit (name biscuit_like_ops);

-- ILIKE / NOT ILIKE only (skips building the case-sensitive structures)
CREATE INDEX idx_name_ilike ON users USING biscuit (name biscuit_ilike_ops);
```

Querying an index with an operator it wasn't built for (e.g. `ILIKE` against a `biscuit_like_ops` column) raises an error rather than silently falling back to a full scan.

### **Supported Data Types**

Biscuit indexes `text`, `varchar`, and `char`/`bpchar` columns directly. Other column types are not supported natively; index them via an expression that casts to text:

```sql
CREATE INDEX idx_expr ON events ((code::text));
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

**Selectivity scoring (lower runs first):** an exact match (no wildcards) scores `0.0`; a prefix or suffix pattern (`abc%` / `%abc`) starts at `0.1 + 0.1` per additional `%`; a substring pattern (`%abc%`) starts at `0.5`; anything else starts at `0.8`. Every concrete (non-wildcard) character in the pattern then reduces the score by `0.05`, and the result is clamped to `[0.01, 1.0]`. Predicates are evaluated in ascending score order, so the most selective predicate runs first and can shrink the candidate set before less selective ones are applied.

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

### **Index Statistics and Diagnostics**

```sql
-- Human-readable report
SELECT biscuit_index_stats('idx_biscuit'::regclass);

-- Estimated size of this backend's in-memory copy of the index, in bytes
SELECT biscuit_index_memory_size('idx_biscuit'::regclass);

-- Structured pending-list drain statistics (per-structure write buffer)
SELECT * FROM biscuit_pending_list_stats('idx_biscuit'::regclass);
```

`biscuit_index_stats()` output (values below are illustrative, not measured):
```
Biscuit Index Statistics
==========================================
Index: idx_biscuit
Active records: 1000002
Total slots: 1000002
Free slots: 0
Tombstones: 0
Max length: 44
------------------------
CRUD Statistics:
  Inserts: 0
  Updates: 0
  Deletes: 0
------------------------
Pending-List Statistics (unmerged write volume):
  Drain threshold (bytes/structure): 65536
  Total pending bytes (approx, as of last VACUUM): 0
  Lifetime drains performed: 0
------------------------
Active Optimizations:
  ✓ 1. Skip wildcard intersections
  ✓ 2. Early termination on empty
  ✓ 3. Avoid redundant copies
  ✓ 4. Optimized single-part patterns
  ✓ 5. Skip unnecessary length ops
  ✓ 6. TID sorting for sequential I/O
  ✓ 7. Batch TID insertion
  ✓ 8. Direct bitmap iteration
  ✓ 9. Parallel bitmap scan support (PostgreSQL 18+ only)
  ✓ 10. Batch cleanup on threshold
  ✓ 11. Skip sorting for bitmap scans
  ✓ 12. LIMIT-aware TID collection
```

`total_pending_bytes` is refreshed once per `VACUUM`, not on every write, so it can lag actual unmerged write volume by up to one `VACUUM` cycle.



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

Enable CRoaring for better performance.


### **Index Options**

Currently, Biscuit doesn't expose tunable options. All optimizations are automatic.

---

##  **Limitations and Trade-offs**

### **What Biscuit Does NOT Support**

1. **Regular expressions** - Only `LIKE` / `ILIKE` patterns with `%` and `_`
2. **Locale-specific collations** - String comparisons are byte-based
3. **Amcanorder = false** - Cannot provide ordered scans directly (but see below)
4. **Non-text column types** - Only `text`, `varchar`, and `char`/`bpchar` are indexed directly; other types need an expression index that casts to text (see [Supported Data Types](#supported-data-types))
5. **Parallel index scans on PostgreSQL < 18** - the AM only registers parallel-scan callbacks on PostgreSQL 18 and later; earlier supported versions (16, 17) always scan serially

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

### **Storage and Memory Usage**

The index's durable state — every bitmap, the TID array, tombstones, and string caches — lives in the index relation's own pages on disk and is WAL-logged like any other index. Separately, each backend that scans or modifies the index keeps a fully-built, in-memory copy of it (every bitmap included) in session-local (`CacheMemoryContext`) memory for the life of that backend, loaded from the on-disk pages on first use. That per-session copy is what drives query execution; use `biscuit_index_memory_size()` to inspect its size, and `REINDEX` if you need to shrink or reorganize the on-disk index itself.

### **Write Performance**

`INSERT`, `UPDATE` (internally a delete-then-insert), and `VACUUM`-driven `DELETE` all mutate the in-memory copy directly and durably append a small delta record per touched structure to that structure's on-disk pending-delta chain — there is no whole-index rewrite on every write. A structure's pending chain is periodically re-serialized ("drained") into a compacted blob once it crosses a size threshold, and `VACUUM` performs a full drain pass regardless of size. Because a single indexed string can touch many per-character structures, `INSERT`/`UPDATE` on a Biscuit index still does more work than on a B-tree; for very write-heavy workloads, evaluate `pg_trgm` or a plain B-tree against your workload as alternatives. `VACUUM` also marks deleted rows as tombstones and reclaims their slots for reuse, with periodic cleanup once outstanding tombstones cross an internal threshold.

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
