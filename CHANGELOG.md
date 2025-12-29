# Biscuit Index Extension - Changelog

## Version 2.2.1

### 🐞 Bug Fixes

* **Fixed recursive pattern matching**

  Resolved incorrect behavior when evaluating nested or repeated wildcard patterns during recursive matching.

* **Corrected underscore (`_`) handling in single-column indexing**

  `_` now correctly operates on character-based offsets (not byte offsets), in accordance with SQL `LIKE` / `ILIKE` semantics, eliminating false matches in multi-byte UTF-8 text.


### 🛡️ Correctness & Stability

* Improved internal consistency between single-column and multi-column pattern evaluation paths.
* Resolved observed edge cases that could lead to incorrect matches under complex wildcard patterns.

---

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

---

## Version 2.1.5

### 🔧 Improvements

**Removed arbitrary limits on multi-column indexes**

*  Biscuit no longer enforces hard-coded limits when creating indexes over multiple columns, allowing more flexible index definitions.

### 🛡️ Safety & Correctness

**Restricted indexing to text-based datatypes**

* Support for non-text datatypes has been removed. Biscuit now explicitly enforces text-only columns to ensure correct operator semantics, planner behavior, and index consistency.

**Explicit error for expression indexing**

*  Biscuit now raises a clear error when users attempt to create an index on an expression (e.g., `lower(col)`), which is not currently supported.
  This prevents silent misconfiguration and enforces Biscuit’s column-based indexing semantics.

> **Note:** Biscuit currently indexes **base columns only**. This may be revisited in future versions.

---

## Version 2.1.4

### 🛠️ Build & Packaging

* Improved Makefile detection logic for CRoaring bitmap support by checking multiple common installation paths, increasing portability across systems and build environments.


### ✨ New Features

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

## Version 2.1.3

### ✨ New Features

#### Added Index Memory Introspection Utilities

Added built-in SQL functions and a view to inspect **Biscuit index in-memory footprint**.

* **`biscuit_index_memory_size(index_oid oid) → bigint`**

  Low-level C-backed function returning the exact memory usage (in bytes) of a Biscuit index currently resident in memory.

* **`biscuit_index_memory_size(index_name text) → bigint`**

  Convenience SQL wrapper accepting an index name instead of an OID.

* **`biscuit_size_pretty(index_name text) → text`**

  Human-readable formatter that reports Biscuit index memory usage in bytes, KB, MB, or GB while preserving the exact byte count.

* **`biscuit_memory_usage` view**

  A consolidated view exposing:

  * schema name
  * table name
  * index name
  * Biscuit in-memory size
  * human-readable memory size
  * on-disk index size (via `pg_relation_size`)

  This allows direct comparison between **in-memory Biscuit structures** and their **persistent disk representation**.


```sql
SELECT * FROM biscuit_memory_usage;
```

#### Notes

* Memory accounting reflects Biscuit’s deliberate cache persistence design, intended to optimize repeated pattern-matching workloads.
* Functions are marked `VOLATILE` to ensure accurate reporting of live memory state.
* `pg_size_pretty(pg_relation_size(...))` reports only the on-disk footprint of the Biscuit index.
Since Biscuit maintains its primary structures in memory (cache buffers / AM cache), the reported disk size may significantly underrepresent the index’s effective total footprint during execution. Hence, we recommend the usage of `biscuit_size_pretty(...)` to view the actual size of the index.

### ⚙️ Performance improvements

#### Removed redundant bitmaps

Separate bitmaps for length-based filtering for case-insensitive search were removed. Case insensitive searches now use the same length-based filtering bitmaps as case-sensitive ones.

---

## Version 2.1.2 (2025-12-11)

### ✨ New Features

#### ILIKE Operator Support (Case-Insensitive Matching)

Biscuit now provides **full support for the `ILIKE` operator**, enabling efficient case-insensitive wildcard searches directly through the index.

**Capabilities:**

* Optimized execution path for `ILIKE` and `NOT ILIKE`
* Works seamlessly in mixed predicate chains alongside `LIKE` / `NOT LIKE`
* Fully compatible with multi-column Biscuit indexes

**Examples:**

```sql
-- Case-insensitive suffix search
SELECT * FROM users WHERE name ILIKE '%son';

-- Combination queries
SELECT * FROM users
WHERE name ILIKE 'a%' AND email NOT ILIKE '%test%';
```

##

#### Removed Length Constraint for Indexing

The previous hardcoded **256-character indexing limit** has been removed.
Biscuit now indexes values of **any length**, including very long strings.

**Impact:**

* All text values—short or arbitrarily long—are now included in bitmap generation
* More consistent query coverage for fields like descriptions, logs, and message bodies

---

## Version 2.1.0 - 2.1.1

> Contain build issues. Fixed in version - 2.1.2.

---

## Version 2.0.1 (2024-12-06)

### 🐞 Bug Fixes

#### Fixed Incorrect Results with Multiple Filter Predicates
**Issue:** Queries with multiple `LIKE` or `NOT LIKE` predicates on the same column could return incorrect results.

**Root Cause:** When executing queries with multiple filter predicates (e.g., `name LIKE '%a%' AND name NOT LIKE '%3%'`), the bitmap inversion logic for `NOT LIKE` was being applied globally instead of per-predicate, causing the wrong result set to be returned.

**Example of Affected Query:**
```sql
-- Query with multiple filters
SELECT COUNT(*) FROM users WHERE name LIKE '%a%' AND name NOT LIKE '%3%';

-- v2.0.0: Returned incorrect count (e.g., 252,167)
-- v2.0.1: Returns correct count (e.g., 251,482) ✅
-- Verified against sequential scan
```

**Fix:** Implemented per-predicate bitmap inversion logic that correctly handles each filter independently before combining results.

**Impact:**
- **Affected Queries:** Any query with 2+ predicates using `LIKE` and/or `NOT LIKE` on indexed columns
- **Severity:** HIGH - Results were incorrect but deterministic
- **Data Safety:** No data corruption - index structure unchanged

**Verification:**
```sql
-- All these patterns now return correct results:

-- Pattern 1: LIKE + NOT LIKE
WHERE name LIKE '%abc%' AND name NOT LIKE '%xyz%'

-- Pattern 2: Multiple NOT LIKE
WHERE name NOT LIKE '%a%' AND name NOT LIKE '%b%'

-- Pattern 3: Complex combinations
WHERE col1 LIKE 'A%' AND col2 NOT LIKE '%test%' AND col1 LIKE '%end'
```

#### NOT LIKE Operator Support
- Full support for `NOT LIKE` pattern matching (Strategy #2)
- Efficient bitmap negation for exclusion queries
- Example: `WHERE name NOT LIKE '%test%'`

### 📝 Upgrade Notes

**Compatibility:**
- Fully backward compatible with v2.0.0

**Recommended Actions:**
1. Update extension: `ALTER EXTENSION biscuit UPDATE TO '2.0.1';`
2. Re-run any critical queries that used multiple predicates to verify corrected results


---

## Version 2.0.0 (2024-11-05)

### 🎯 Major Features

#### Multi-Column Index Support
- Create Biscuit indices on multiple columns simultaneously
- Per-column bitmap optimization for efficient filtering
- Example: `CREATE INDEX idx ON table USING biscuit(name, email, description);`


#### Query Optimization Engine
- Intelligent predicate reordering based on selectivity analysis
- Executes most selective filters first to minimize candidate set
- Supports exact, prefix, suffix, and substring pattern detection

#### Performance Enhancements
- TID sorting for sequential heap access (5000+ results)
- Parallel bitmap collection for large result sets (10K+ matches)
- Direct Roaring bitmap iteration without intermediate arrays
- Skip sorting for bitmap scans (COUNT/aggregate queries)
- LIMIT-aware early termination

#### Memory Management Improvements
- Persistent caching in CacheMemoryContext
- Automatic cache invalidation on index drop/ALTER
- Batch cleanup with configurable threshold (1000 tombstones)

### 🔧 Technical Improvements

**Pattern Matching:**
- Fast-path optimizations for pure wildcard patterns (`%`, `_`)
- Exact length matching for underscore-only patterns
- Optimized single-part and two-part pattern execution
- Recursive windowed matching for complex multi-part patterns

**Type Support:**
- Text, VARCHAR, CHAR (native)
- Integer types (INT2, INT4, INT8) with sortable encoding
- Float types (FLOAT4, FLOAT8) with scientific notation
- Date/Timestamp types with microsecond precision
- Boolean type

**Index Statistics:**
- `biscuit_index_stats(index_oid)` function for diagnostics
- CRUD operation tracking (inserts, updates, deletes)
- Tombstone and free slot monitoring


---

**Full Documentation:** See [README.md](https://github.com/CrystallineCore/Biscuit) or visit [ReadTheDocs](https://biscuit.readthedocs.io/) for complete usage guide and examples.

---