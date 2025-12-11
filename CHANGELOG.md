# Biscuit Index Extension - Changelog


## Version 2.1.0 (2025-12-11)

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

**Full Documentation:** See README.md for complete usage guide and examples.

---