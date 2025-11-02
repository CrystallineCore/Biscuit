# BISCUIT: Bitmap Indexed Searching with Combinatorial Union and Intersection Techniques

## PostgreSQL Extension for Wildcard Pattern Matching

### Version 1.0.3

---

## Executive Summary

BISCUIT is a PostgreSQL C extension that provides bitmap indexing for wildcard pattern matching operations. The extension addresses performance limitations in traditional database indexes for leading wildcard queries (e.g., `LIKE '%pattern'`), which typically require full table scans.

### Key Capabilities

- **Optimized Wildcard Matching**: Bitmap operations for pattern matching regardless of wildcard placement
- **Automatic Index Maintenance**: Transparent CRUD synchronization via PostgreSQL triggers
- **Memory-Efficient Storage**: Compressed bitmap structures using Roaring Bitmap algorithm
- **Lazy Deletion Strategy**: Deferred cleanup with batching for optimal throughput
- **Incremental Update Optimization**: Minimized reindexing overhead for similar string modifications
- **Universal Primary Key Support**: Compatible with any PostgreSQL data type
- **Production-Ready Design**: Comprehensive error handling and diagnostic capabilities

### Honest Performance Characteristics

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| Simple Pattern Match (bitmap ops) | O(k) | k = matching records to filter |
| Complex Pattern (multi-part) | O(k × m) | k = parts, m = window size |
| Result Retrieval | O(n) | n = matching records returned |
| Insert Operation | O(L) | L = string length (max 256 indexed) |
| Update (Incremental) | O(d) | d = character differences |
| Update (Full) | O(L) | L = new string length |
| Delete Operation | O(1) | Lazy tombstone marking |
| PK Lookup | O(1) | Hash table lookup |
| Cleanup Operation | O(t × c) | t = tombstones, c = charset operations |

**Important**: Query time includes O(n) for returning n matching records. The bitmap operations are fast, but you still need to retrieve and return the actual data.

---

## Technical Overview

### Problem Statement

Traditional database indexes (B-tree, GiST, GIN) exhibit suboptimal performance for wildcard pattern matching queries, particularly when wildcards appear at the beginning of patterns. A query such as `SELECT * FROM table WHERE column LIKE '%pattern%'` typically requires a sequential scan.

### Solution Architecture

BISCUIT implements a multi-dimensional bitmap index structure that decomposes strings into positional character mappings. Each character at each position is tracked using compressed bitmaps (Roaring Bitmaps), enabling efficient intersection operations for pattern matching.

#### Core Principle

Instead of scanning strings sequentially, BISCUIT:
1. Pre-indexes all character positions (up to 256 characters)
2. Uses bitmap intersections to find candidate records
3. Filters results through tombstone bitmap (for deleted records)
4. Returns matching records

**Reality Check**: This trades initial indexing time and memory for faster queries. It's not magic—you're paying upfront to make searches faster later.

#### Core Index Structures

1. **Positional Character Index**: Maps each character to position-bitmap pairs
   - Forward positions: `{0, 1, 2, ..., 255}` from string start
   - Reverse positions: `{-1, -2, -3, ..., -256}` from string end
   
2. **Character Existence Cache**: Union of all positional bitmaps per character for containment queries

3. **Length Index**: Bitmap array indexed by string length

4. **Primary Key Hash Table**: O(1) lookup structure mapping primary keys to internal indices

5. **Lazy Deletion Layer**: Tombstone bitmap tracking deleted records with deferred cleanup

---

## Installation and Requirements

### System Requirements

- **PostgreSQL Version**: 11.0 or higher
- **Operating System**: Linux, macOS, Windows (with MinGW)
- **Architecture**: x86_64, ARM64
- **Memory**: Minimum 512MB available RAM per index
- **Compiler**: GCC 4.8+, Clang 3.9+, or MSVC 2017+

### Optional Dependencies

- **CRoaring Library**: For optimized Roaring Bitmap operations (recommended)
  - Installation: `apt-get install libroaring-dev` (Debian/Ubuntu)
  - Falls back to built-in implementation if unavailable

### Installation Procedure

#### Method 1: PostgreSQL Extension System

```sql
CREATE EXTENSION biscuit;
```

#### Method 2: Manual Compilation

```bash
git clone https://github.com/crystallinecore/biscuit.git
cd biscuit
make
sudo make install
psql -d your_database -c "CREATE EXTENSION biscuit;"
```

#### Verification

```sql
SELECT biscuit_version();
-- Expected output: 1.0.3-Biscuit
```

---

## Configuration and Setup

### Quick Start (Recommended)

```sql
SELECT biscuit_setup('customer_records', 'email_address', 'customer_id');
```

This single function:
1. Builds the bitmap index
2. Creates type-safe wrapper functions
3. Installs automatic update triggers

### Manual Configuration (Advanced)

```sql
-- Step 1: Build index
SELECT biscuit_build_index('table_name', 'column_name', 'pk_column');

-- Step 2: Create query functions
SELECT biscuit_create_match_function();

-- Step 3: Enable triggers
SELECT biscuit_enable_triggers();
```

### Important Limitations

- **One Index Per Database Instance**: Currently only supports one active index
- **In-Memory Only**: Index is not persistent across server restarts
- **256 Character Limit**: Only first 256 characters are indexed (though full strings are stored)
- **Case Sensitive**: Patterns are case-sensitive by default

---

## Query Interface

### Primary Query Functions

#### 1. Full Record Retrieval

```sql
SELECT * FROM biscuit_match('%@example.com%');
SELECT id, email FROM biscuit_match('user%') WHERE created_at > '2024-01-01';
```

Returns complete rows from the indexed table.

#### 2. Count Only (Fastest)

```sql
SELECT biscuit_match_count('%search_term%');
```

Returns count without materializing result set.

#### 3. Key-Value Pairs

```sql
SELECT * FROM biscuit_match_rows('%pattern%');
```

Returns (primary_key, indexed_value) pairs.

---

## API Reference

### Setup and Configuration Functions

#### biscuit_setup()

Complete one-step setup function that builds index, creates wrapper functions, and enables triggers.

**Signature:**
```sql
biscuit_setup(
    p_table_name TEXT,
    p_column_name TEXT,
    p_pk_column_name TEXT DEFAULT 'id'
) RETURNS TEXT
```

**Parameters:**
- `p_table_name`: Name of the table to index
- `p_column_name`: Name of the text column to index
- `p_pk_column_name`: Name of the primary key column (default: 'id')

**Returns:** Status message describing operations performed

**Usage:**
```sql
-- Basic setup with default primary key
SELECT biscuit_setup('products', 'name');

-- Setup with custom primary key
SELECT biscuit_setup('customers', 'email', 'customer_id');

-- Setup with UUID primary key
SELECT biscuit_setup('users', 'username', 'user_uuid');
```

**Example Output:**
```
Biscuit index built successfully.
Created biscuit_match() and biscuit_match_rows() functions for table: products
Columns: id integer, name text
Successfully created trigger on table: products
The index will now automatically update on INSERT, UPDATE, and DELETE operations.
```

---

#### biscuit_build_index()

Constructs the bitmap index structure from existing table data.

**Signature:**
```sql
biscuit_build_index(
    table_name TEXT,
    column_name TEXT,
    pk_column_name TEXT DEFAULT 'id'
) RETURNS BOOLEAN
```

**Parameters:**
- `table_name`: Name of the table to index
- `column_name`: Name of the text column to index
- `pk_column_name`: Name of the primary key column (default: 'id')

**Returns:** TRUE on success

**Usage:**
```sql
-- Build index for products table
SELECT biscuit_build_index('products', 'name', 'id');

-- Rebuild existing index (drops old index first)
SELECT biscuit_build_index('products', 'name', 'id');

-- Build index with custom primary key
SELECT biscuit_build_index('customers', 'email', 'customer_id');
```

**Performance Note:** For 1 million records, expect build time of approximately 60-90 seconds depending on string length and complexity.

---

#### biscuit_create_match_function()

Generates strongly-typed wrapper functions for querying.

**Signature:**
```sql
biscuit_create_match_function() RETURNS TEXT
```

**Returns:** Confirmation message with function signatures

**Usage:**
```sql
-- Must call after building index
SELECT biscuit_build_index('products', 'name', 'id');
SELECT biscuit_create_match_function();
```

**Generated Functions:**
- `biscuit_match(pattern TEXT)`: Returns complete table rows
- `biscuit_match_rows(pattern TEXT)`: Returns (pk, value) tuples

---

#### biscuit_enable_triggers()

Activates automatic index maintenance triggers.

**Signature:**
```sql
biscuit_enable_triggers() RETURNS TEXT
```

**Returns:** Status message

**Usage:**
```sql
-- Enable automatic updates
SELECT biscuit_enable_triggers();

-- Typical workflow
SELECT biscuit_build_index('products', 'name', 'id');
SELECT biscuit_create_match_function();
SELECT biscuit_enable_triggers();
```

**Creates Trigger:** `biscuit_auto_update` (AFTER INSERT OR UPDATE OR DELETE)

---

#### biscuit_disable_triggers()

Deactivates automatic index maintenance for bulk operations.

**Signature:**
```sql
biscuit_disable_triggers() RETURNS TEXT
```

**Returns:** Status message

**Usage:**
```sql
-- Disable for bulk operation
SELECT biscuit_disable_triggers();

-- Perform bulk insert
COPY products FROM '/data/products.csv' CSV;

-- Rebuild and re-enable
SELECT biscuit_build_index('products', 'name', 'id');
SELECT biscuit_enable_triggers();
```

---

### Query Functions

#### biscuit_match()

Primary query interface returning complete table rows with proper type safety.

**Signature:**
```sql
biscuit_match(pattern TEXT) RETURNS SETOF table_type
```

**Parameters:**
- `pattern`: SQL LIKE-style wildcard pattern (% and _)

**Returns:** Complete rows from indexed table matching the pattern

**Usage:**
```sql
-- Simple containment search
SELECT * FROM biscuit_match('%laptop%');

-- Prefix search
SELECT * FROM biscuit_match('PROD-%');

-- Suffix search
SELECT * FROM biscuit_match('%@gmail.com');

-- With additional filters
SELECT * FROM biscuit_match('%electronics%')
WHERE price > 100 AND in_stock = true;

-- With ordering
SELECT * FROM biscuit_match('%widget%')
ORDER BY created_at DESC
LIMIT 10;

-- In joins
SELECT p.*, c.category_name
FROM biscuit_match('%special%') p
JOIN categories c ON p.category_id = c.id;

-- Count matches
SELECT COUNT(*) FROM biscuit_match('%premium%');

-- Aggregate operations
SELECT category, AVG(price)
FROM biscuit_match('%electronics%')
GROUP BY category;
```

---

#### biscuit_match_count()

Returns count of matching records without materializing result set. This is the fastest query method.

**Signature:**
```sql
biscuit_match_count(pattern TEXT) RETURNS INTEGER
```

**Parameters:**
- `pattern`: SQL LIKE-style wildcard pattern

**Returns:** Integer count of matching records

**Usage:**
```sql
-- Get count only
SELECT biscuit_match_count('%@example.com');

-- Compare counts
SELECT 
    biscuit_match_count('%@gmail.com') AS gmail_users,
    biscuit_match_count('%@yahoo.com') AS yahoo_users,
    biscuit_match_count('%@hotmail.com') AS hotmail_users;

-- Conditional logic
DO $
DECLARE
    match_count INTEGER;
BEGIN
    match_count := biscuit_match_count('%error%');
    IF match_count > 1000 THEN
        RAISE NOTICE 'High error count: %', match_count;
    END IF;
END $;

-- Use in WHERE clause
SELECT * FROM summary_table
WHERE error_count = biscuit_match_count('%critical%');
```

**Performance:** Performs only bitmap operations without tuple retrieval, making it significantly faster than `COUNT(*)` on full result set.

---

#### biscuit_match_rows()

Returns (primary_key, indexed_value) pairs for matching records.

**Signature:**
```sql
biscuit_match_rows(pattern TEXT) RETURNS TABLE(pk primary_key_type, value TEXT)
```

**Parameters:**
- `pattern`: SQL LIKE-style wildcard pattern

**Returns:** Table with two columns: pk (original type) and value (text)

**Usage:**
```sql
-- Get key-value pairs
SELECT * FROM biscuit_match_rows('%search%');

-- Extract just primary keys
SELECT pk FROM biscuit_match_rows('%pattern%');

-- Join back to original table for specific columns
SELECT t.id, t.name, t.price
FROM products t
WHERE t.id IN (SELECT pk FROM biscuit_match_rows('%laptop%'));

-- Use in subquery
WITH matches AS (
    SELECT pk, value FROM biscuit_match_rows('%error%')
)
SELECT * FROM logs l
JOIN matches m ON l.id = m.pk
WHERE l.timestamp > NOW() - INTERVAL '1 hour';
```

---

#### biscuit_match_keys()

Low-level function returning primary keys and values as text. Used internally by higher-level functions.

**Signature:**
```sql
biscuit_match_keys(pattern TEXT) RETURNS TABLE(pk TEXT, value TEXT)
```

**Parameters:**
- `pattern`: SQL LIKE-style wildcard pattern

**Returns:** Table with two text columns

**Usage:**
```sql
-- Direct usage (returns everything as text)
SELECT * FROM biscuit_match_keys('%pattern%');

-- Cast primary key back to original type
SELECT pk::INTEGER AS id, value
FROM biscuit_match_keys('%search%');

-- For UUID primary keys
SELECT pk::UUID AS user_id, value
FROM biscuit_match_keys('%@domain.com');
```

**Note:** Generally prefer `biscuit_match_rows()` which preserves primary key type.

---

### Monitoring and Status Functions

#### biscuit_index_status()

Comprehensive status report including table information, memory usage, and CRUD statistics.

**Signature:**
```sql
biscuit_index_status() RETURNS TEXT
```

**Returns:** Formatted multi-line status report

**Usage:**
```sql
-- View complete status
SELECT biscuit_index_status();

-- Extract specific metrics using regex
SELECT (regexp_matches(biscuit_index_status(), 'Active Records: ([0-9]+)'))[1]::INTEGER AS active;
SELECT (regexp_matches(biscuit_index_status(), 'Memory: ([0-9.]+) MB'))[1]::NUMERIC AS memory_mb;

-- Create monitoring view
CREATE VIEW biscuit_health AS
SELECT 
    NOW() AS checked_at,
    (regexp_matches(biscuit_index_status(), 'Active Records: ([0-9]+)'))[1]::INTEGER AS active_records,
    (regexp_matches(biscuit_index_status(), 'Tombstoned Slots: ([0-9]+)'))[1]::INTEGER AS tombstones,
    (regexp_matches(biscuit_index_status(), 'Memory: ([0-9.]+) MB'))[1]::NUMERIC AS memory_mb;

-- Check status in script
DO $
DECLARE
    status_text TEXT;
BEGIN
    status_text := biscuit_index_status();
    RAISE NOTICE '%', status_text;
END $;
```

**Sample Output:**
```
========================================
Biscuit Index v3 - FIXED
========================================
Table: products
Column: name
Primary Key: id
Active Records: 998567
Total Slots: 1000000
Free Slots: 1433
Tombstoned Slots: 856
Max length: 128
Memory: 18.3 MB
----------------------------------------
CRUD Statistics:
  Inserts: 1000000
  Deletes: 1433
  Updates: 125678 (incr: 89234, 71.0%)
  Queries: 45892
----------------------------------------
Lazy Deletion Status:
  Pending tombstones: 856 (59.7% of deletes)
  Cleanup threshold: 1000
  Total cleanups: 1
  Items cleaned: 577
  Query/Delete ratio: 32.02
----------------------------------------
```

---

#### biscuit_get_active_count()

Returns the number of active (non-deleted) records in the index.

**Signature:**
```sql
biscuit_get_active_count() RETURNS INTEGER
```

**Returns:** Count of active records

**Usage:**
```sql
-- Get active count
SELECT biscuit_get_active_count();

-- Compare with table
SELECT 
    COUNT(*) AS table_count,
    biscuit_get_active_count() AS index_count
FROM products;

-- Monitor growth
CREATE TABLE index_metrics (
    logged_at TIMESTAMPTZ DEFAULT NOW(),
    active_count INTEGER
);

INSERT INTO index_metrics (active_count)
SELECT biscuit_get_active_count();

-- Alert on discrepancy
DO $
DECLARE
    table_count BIGINT;
    index_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO table_count FROM products;
    index_count := biscuit_get_active_count();
    
    IF table_count != index_count THEN
        RAISE WARNING 'Index out of sync: table=%, index=%', table_count, index_count;
    END IF;
END $;
```

---

#### biscuit_get_free_slots()

Returns the number of available slots for reuse (from deleted records).

**Signature:**
```sql
biscuit_get_free_slots() RETURNS INTEGER
```

**Returns:** Count of free slots

**Usage:**
```sql
-- Check available slots
SELECT biscuit_get_free_slots();

-- Monitor slot reuse efficiency
SELECT 
    biscuit_get_active_count() AS active,
    biscuit_get_free_slots() AS free,
    ROUND(100.0 * biscuit_get_free_slots() / 
          (biscuit_get_active_count() + biscuit_get_free_slots()), 2) AS free_pct;

-- Capacity planning
WITH stats AS (
    SELECT 
        biscuit_get_active_count() AS active,
        biscuit_get_free_slots() AS free,
        (regexp_matches(biscuit_index_status(), 'Total Slots: ([0-9]+)'))[1]::INTEGER AS capacity
)
SELECT 
    active,
    free,
    capacity,
    capacity - active - free AS used_tombstoned,
    ROUND(100.0 * active / capacity, 2) AS utilization_pct
FROM stats;
```

---

#### biscuit_get_tombstone_count()

Returns the number of deleted records pending cleanup.

**Signature:**
```sql
biscuit_get_tombstone_count() RETURNS INTEGER
```

**Returns:** Count of pending tombstones

**Usage:**
```sql
-- Check tombstone count
SELECT biscuit_get_tombstone_count();

-- Conditional cleanup
DO $
BEGIN
    IF biscuit_get_tombstone_count() > 5000 THEN
        PERFORM biscuit_cleanup();
        RAISE NOTICE 'Cleanup completed';
    END IF;
END $;

-- Monitor cleanup efficiency
SELECT 
    biscuit_get_tombstone_count() AS pending,
    (regexp_matches(biscuit_index_status(), 'Total cleanups: ([0-9]+)'))[1]::BIGINT AS total_cleanups,
    (regexp_matches(biscuit_index_status(), 'Items cleaned: ([0-9]+)'))[1]::BIGINT AS items_cleaned;

-- Alert threshold
CREATE OR REPLACE FUNCTION check_tombstone_threshold()
RETURNS VOID AS $
DECLARE
    count INTEGER;
BEGIN
    count := biscuit_get_tombstone_count();
    IF count > 10000 THEN
        RAISE WARNING 'CRITICAL: % tombstones pending cleanup', count;
    ELSIF count > 5000 THEN
        RAISE NOTICE 'WARNING: % tombstones pending cleanup', count;
    END IF;
END;
$ LANGUAGE plpgsql;
```

---

#### biscuit_version()

Returns the version string of the BISCUIT extension.

**Signature:**
```sql
biscuit_version() RETURNS TEXT
```

**Returns:** Version string

**Usage:**
```sql
-- Check version
SELECT biscuit_version();

-- Verify installation
DO $
DECLARE
    version TEXT;
BEGIN
    version := biscuit_version();
    RAISE NOTICE 'BISCUIT version: %', version;
END $;

-- Version check in application
SELECT biscuit_version() AS version;
-- Expected: 1.0.3-Biscuit
```

---

### Maintenance Functions

#### biscuit_cleanup()

Manually triggers tombstone cleanup process.

**Signature:**
```sql
biscuit_cleanup() RETURNS TEXT
```

**Returns:** Summary of cleanup operations

**Usage:**
```sql
-- Manual cleanup
SELECT biscuit_cleanup();

-- Scheduled cleanup (using pg_cron)
SELECT cron.schedule(
    'biscuit-nightly-cleanup',
    '0 2 * * *',  -- 2 AM daily
    $SELECT biscuit_cleanup()$
);

-- Cleanup before performance-critical operation
BEGIN;
    SELECT biscuit_cleanup();
    -- Perform critical queries
COMMIT;

-- Conditional cleanup
DO $
DECLARE
    tombstone_count INTEGER;
    cleanup_result TEXT;
BEGIN
    tombstone_count := biscuit_get_tombstone_count();
    
    IF tombstone_count > 1000 THEN
        cleanup_result := biscuit_cleanup();
        RAISE NOTICE '%', cleanup_result;
    ELSE
        RAISE NOTICE 'Cleanup skipped: only % tombstones', tombstone_count;
    END IF;
END $;
```

**Example Output:**
```
Tombstone cleanup complete:
  Cleaned: 856 tombstones
  Remaining: 0
  Total cleanups: 2
```

**When to Use:**
- Before performance-critical queries
- After bulk delete operations
- When tombstone count exceeds 5,000
- During scheduled maintenance windows

---

## Pattern Matching Specification

### Wildcard Operators

| Operator | Matches | Examples |
|----------|---------|----------|
| `%` | Zero or more characters | `'a%'` → "a", "abc" |
| `_` | Exactly one character | `'a_c'` → "abc", "axc" |

### Pattern Examples

```sql
-- Exact match
SELECT * FROM biscuit_match('hello');

-- Prefix
SELECT * FROM biscuit_match('user_%');

-- Suffix
SELECT * FROM biscuit_match('%@gmail.com');

-- Contains
SELECT * FROM biscuit_match('%error%');

-- Complex multi-part
SELECT * FROM biscuit_match('%user_%@%.com');

-- Length-specific
SELECT * FROM biscuit_match('___');  -- Exactly 3 chars
```

### Performance Guidance

**Optimal Patterns** (simple bitmap operations):
- Single character: `'%a%'`
- Prefix: `'abc%'`
- Suffix: `'%xyz'`
- Exact length: `'___'`

**Acceptable Patterns** (windowed matching):
- Two parts: `'%a%b%'`
- Three parts: `'%a%b%c%'`

**Avoid When Possible** (slower performance):
- Four or more parts: `'%a%b%c%d%'`
- Recommendation: Simplify patterns or combine terms

---

## CRUD Operations

### Automatic Index Maintenance

Once triggers are enabled, all DML operations automatically update the index:

```sql
-- Insert: O(L) where L = string length
INSERT INTO products (id, name) VALUES (1, 'New Product');

-- Update: O(d) for incremental, O(L) for full reindex
UPDATE products SET name = 'Updated Name' WHERE id = 1;

-- Delete: O(1) lazy deletion
DELETE FROM products WHERE id = 1;
```

### Incremental Update Optimization

For updates where:
- Same string length
- String length ≥ 3 characters
- < 20% character changes
- Maximum 3 changed characters

BISCUIT uses incremental updates (only updating changed positions).

### Lazy Deletion

Deletes are O(1) operations that:
1. Mark record in tombstone bitmap
2. Remove from hash table
3. Add to free list for reuse
4. **Defer** cleanup of position bitmaps

Cleanup triggers automatically when tombstone count reaches threshold (default: 1000).

### Bulk Operations

For large bulk operations, consider:

```sql
-- Disable triggers
SELECT biscuit_disable_triggers();

-- Perform bulk operation
COPY products FROM '/data/products.csv' CSV;

-- Rebuild and re-enable
SELECT biscuit_build_index('products', 'name', 'id');
SELECT biscuit_enable_triggers();
```

---

## Monitoring and Diagnostics

### Check Index Status

```sql
SELECT biscuit_index_status();
```

Sample output:
```
========================================
Biscuit Index v3 - FIXED
========================================
Table: products
Column: name
Primary Key: id
Active Records: 998567
Total Slots: 1000000
Free Slots: 1433
Tombstoned Slots: 856
Max length: 128
Memory: 18.3 MB
----------------------------------------
CRUD Statistics:
  Inserts: 1000000
  Deletes: 1433
  Updates: 125678 (incr: 89234, 71.0%)
  Queries: 45892
----------------------------------------
```

### Key Metrics

```sql
-- Active record count
SELECT biscuit_get_active_count();

-- Available free slots
SELECT biscuit_get_free_slots();

-- Pending tombstones
SELECT biscuit_get_tombstone_count();
```

### Manual Cleanup

```sql
SELECT biscuit_cleanup();
```

Triggers immediate tombstone cleanup. Use when:
- Tombstone count exceeds 5000
- Before performance-critical operations
- During maintenance windows

---

## CRUD Operations and Index Maintenance

### Automatic Index Maintenance

Once triggers are enabled via `biscuit_setup()` or `biscuit_enable_triggers()`, all DML operations automatically maintain index consistency.

### INSERT Operations

**Behavior:** New records are immediately indexed and available for queries.

**Complexity:** O(L) where L = string length (up to 256 characters indexed)

**Usage:**
```sql
-- Simple insert
INSERT INTO products (id, name) VALUES (1001, 'New Product');

-- Insert with immediate availability
INSERT INTO products (id, name) VALUES (1002, 'Premium Widget');
SELECT * FROM biscuit_match('%Widget%');  -- Returns new row immediately

-- Bulk insert (keep triggers enabled for small batches)
INSERT INTO products (id, name)
VALUES 
    (1003, 'Product A'),
    (1004, 'Product B'),
    (1005, 'Product C');

-- Bulk insert (disable triggers for large batches)
SELECT biscuit_disable_triggers();

COPY products FROM '/data/products.csv' CSV;

SELECT biscuit_build_index('products', 'name', 'id');
SELECT biscuit_enable_triggers();
```

### UPDATE Operations

**Behavior:** Updates are optimized based on string similarity.

**Incremental Update Criteria:**
- Same string length (before and after)
- String length ≥ 3 characters
- Character differences < 20% of string length
- Maximum 3 changed characters

**Complexity:**
- Incremental: O(d) where d = number of different characters
- Full reindex: O(L) where L = new string length

**Usage:**
```sql
-- Incremental update (same length, few changes)
UPDATE products 
SET name = 'Premium Widget Pro'  -- Similar to 'Premium Widget Max'
WHERE id = 1001;

-- Full reindex (different length)
UPDATE products 
SET name = 'Completely Different Name'
WHERE id = 1002;

-- Bulk update (disable triggers)
SELECT biscuit_disable_triggers();

UPDATE products SET name = UPPER(name);

SELECT biscuit_build_index('products', 'name', 'id');
SELECT biscuit_enable_triggers();

-- Update with immediate query
UPDATE products SET name = 'Updated Product' WHERE id = 1003;
SELECT * FROM biscuit_match('%Updated%');  -- Sees updated value
```

**Statistics:**
```sql
-- Check incremental update efficiency
SELECT biscuit_index_status();
-- Look for: Updates: 125678 (incr: 89234, 71.0%)
```

### DELETE Operations

**Behavior:** Implements lazy deletion with O(1) tombstone marking.

**Complexity:** O(1) for delete operation, O(t × c) for cleanup

**Usage:**
```sql
-- Single delete (instant)
DELETE FROM products WHERE id = 1001;

-- Bulk delete
DELETE FROM products WHERE category = 'obsolete';

-- Delete with immediate query
DELETE FROM products WHERE id = 1002;
SELECT * FROM biscuit_match('%Product%');  -- Does not include deleted record

-- Check tombstone accumulation
SELECT biscuit_get_tombstone_count();

-- Manual cleanup
SELECT biscuit_cleanup();

-- Bulk delete with manual cleanup
SELECT biscuit_disable_triggers();

DELETE FROM products WHERE created_at < '2020-01-01';

SELECT biscuit_build_index('products', 'name', 'id');
SELECT biscuit_enable_triggers();
```

**Automatic Cleanup:** Triggers when tombstone count reaches threshold (default: 1000)

### Transaction Semantics

**ACID Compliance:** Index updates commit/rollback with transactions.

**Usage:**
```sql
-- Transaction rollback
BEGIN;
    INSERT INTO products (id, name) VALUES (9999, 'Test Product');
    SELECT * FROM biscuit_match('%Test%');  -- Visible in transaction
ROLLBACK;

SELECT * FROM biscuit_match('%Test%');  -- Not visible, index reverted

-- Transaction commit
BEGIN;
    INSERT INTO products (id, name) VALUES (9999, 'Test Product');
    UPDATE products SET name = 'Test Product Updated' WHERE id = 9999;
    DELETE FROM products WHERE id = 9998;
COMMIT;

-- All changes now visible
SELECT * FROM biscuit_match('%Test%');
```

---

## Limitations and Constraints

### Architectural Limitations

#### 1. Single Index Per Database Instance

**Limitation:** Only one BISCUIT index can be active per PostgreSQL instance.

**Impact:**
```sql
-- This replaces the first index
SELECT biscuit_setup('table1', 'col1', 'id');
SELECT biscuit_setup('table2', 'col2', 'id');  -- Replaces table1 index
```

**Workaround:**
```sql
-- Use separate databases
CREATE DATABASE db1;
\c db1
SELECT biscuit_setup('table', 'column', 'id');

CREATE DATABASE db2;
\c db2
SELECT biscuit_setup('table', 'column', 'id');
```

#### 2. Non-Persistent Storage

**Limitation:** Index resides in shared memory and is lost on server restart.

**Impact:** Must rebuild after PostgreSQL restart.

**Workaround:**
```sql
-- Create startup script: /etc/postgresql/init_biscuit.sql
\c your_database
SELECT biscuit_setup('products', 'name', 'id');
SELECT biscuit_setup('customers', 'email', 'customer_id');  -- If in separate DB

-- Or use application startup code
-- On application initialization:
-- execute: SELECT biscuit_setup('table', 'column', 'pk');
```

#### 3. 256 Character Indexing Limit

**Limitation:** Only first 256 characters are indexed for pattern matching.

**Impact:**
```sql
-- This works (match at position 10)
INSERT INTO products (id, name) VALUES (1, 'Product: Special Widget');
SELECT * FROM biscuit_match('%Special%');  -- Found

-- This fails (match at position 300)
INSERT INTO products (id, description) 
VALUES (2, 'Long description... [300 chars] ...Special keyword');
SELECT * FROM biscuit_match('%Special%');  -- Not found (beyond position 256)
```

**Note:** Full strings are still stored and returned, just not indexed beyond 256 characters.

#### 4. Single Column Per Index

**Limitation:** One index covers one text column per table.

**Workaround - Concatenated Search Column:**
```sql
-- Create combined search column
ALTER TABLE products 
ADD COLUMN search_text TEXT 
GENERATED ALWAYS AS (name || ' ' || description || ' ' || sku) STORED;

-- Index the combined column
SELECT biscuit_setup('products', 'search_text', 'id');

-- Search across all fields
SELECT * FROM biscuit_match('%widget%');
```

#### 5. Case Sensitivity

**Limitation:** Patterns are case-sensitive by default.

**Impact:**
```sql
INSERT INTO products (id, name) VALUES (1, 'Premium Widget');

SELECT * FROM biscuit_match('%widget%');   -- Not found
SELECT * FROM biscuit_match('%Widget%');   -- Found
```

**Workaround - Normalized Column:**
```sql
-- Add normalized column
ALTER TABLE products 
ADD COLUMN name_lower TEXT 
GENERATED ALWAYS AS (LOWER(name)) STORED;

-- Index normalized column
SELECT biscuit_setup('products', 'name_lower', 'id');

-- Case-insensitive search
SELECT * FROM biscuit_match(LOWER('%WIDGET%'));  -- Found
```

### Performance Limitations

#### 1. Memory Usage

**Limitation:** Memory usage scales with dataset size and characteristics.

**Typical Usage:**
- 10-30% of indexed data size
- Example: 1M records × 50 chars avg ≈ 15-25 MB

**Monitoring:**
```sql
-- Check memory usage
SELECT (regexp_matches(biscuit_index_status(), 'Memory: ([0-9.]+) MB'))[1]::NUMERIC;

-- Monitor over time
CREATE TABLE memory_tracking (
    logged_at TIMESTAMPTZ DEFAULT NOW(),
    memory_mb NUMERIC
);

INSERT INTO memory_tracking (memory_mb)
SELECT (regexp_matches(biscuit_index_status(), 'Memory: ([0-9.]+) MB'))[1]::NUMERIC;
```

#### 2. Pattern Complexity

**Limitation:** Query performance degrades with pattern complexity.

**Performance Tiers:**
```sql
-- Fast
SELECT * FROM biscuit_match('%single%');

-- Good 
SELECT * FROM biscuit_match('%two%parts%');

-- Acceptable
SELECT * FROM biscuit_match('%three%part%pattern%');

-- Slower 
SELECT * FROM biscuit_match('%four%or%more%parts%here%');
```

**Recommendation:** Keep patterns to 2-3 parts for optimal performance.

#### 3. Write Concurrency

**Limitation:** Trigger-based updates may experience contention under very high write loads.

**Impact:** Write operations serialize at the row level through PostgreSQL's trigger mechanism.

**Mitigation:**
```sql
-- For high-volume writes, batch operations
BEGIN;
    INSERT INTO products SELECT * FROM staging_table;
COMMIT;

-- Or disable triggers for bulk operations
SELECT biscuit_disable_triggers();
-- Perform bulk writes
SELECT biscuit_build_index('products', 'name', 'id');
SELECT biscuit_enable_triggers();
```

### Known Issues

1. **No Regular Expression Support**: Only SQL LIKE wildcards (`%` and `_`)
2. **No Full-Text Search Features**: No stemming, stop words, or language processing
3. **No Cross-Database Queries**: Each database has its own index
4. **No Partial String Updates**: Updates always reprocess entire string

---

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: "Index not built" Error

**Symptom:**
```sql
SELECT * FROM biscuit_match('%pattern%');
ERROR: Index not built. Call biscuit_build_index() first.
```

**Diagnosis:**
```sql
-- Check if index exists
SELECT biscuit_version();  -- Should return version if extension loaded

-- Attempt to view status
SELECT biscuit_index_status();  -- Will error if no index
```

**Solution:**
```sql
-- Method 1: Complete setup
SELECT biscuit_setup('table_name', 'column_name', 'pk_column');

-- Method 2: Manual build
SELECT biscuit_build_index('table_name', 'column_name', 'pk_column');
SELECT biscuit_create_match_function();
SELECT biscuit_enable_triggers();
```

**Prevention:**
```bash
# Add to PostgreSQL startup script
# File: /etc/postgresql/14/main/postgresql.conf
# Or application startup routine
```

```sql
-- Startup SQL script
\c your_database
SELECT biscuit_setup('products', 'name', 'id');
```

---

#### Issue 2: Query Returns No Results

**Symptom:**
```sql
SELECT biscuit_match_count('%known_value%');
-- Returns: 0 (expected > 0)
```

**Diagnosis:**
```sql
-- Verify value exists in table
SELECT COUNT(*) FROM products WHERE name LIKE '%known_value%';

-- Check index status
SELECT biscuit_index_status();

-- Verify active records
SELECT biscuit_get_active_count();

-- Compare with table count
SELECT 
    COUNT(*) AS table_count,
    biscuit_get_active_count() AS index_count
FROM products;

-- Check trigger status
SELECT tgname, tgenabled 
FROM pg_trigger 
WHERE tgname = 'biscuit_auto_update'
AND tgrelid = 'products'::regclass;
```

**Common Causes:**

**A. Index Out of Sync**
```sql
-- Solution: Rebuild index
SELECT biscuit_build_index('products', 'name', 'id');

-- Verify sync
SELECT 
    COUNT(*) AS table_count,
    biscuit_get_active_count() AS index_count
FROM products;
```

**B. Case Sensitivity Mismatch**
```sql
-- Problem: Pattern case doesn't match data
INSERT INTO products (id, name) VALUES (1, 'Widget');
SELECT * FROM biscuit_match('%widget%');  -- Returns nothing

-- Solution 1: Match case exactly
SELECT * FROM biscuit_match('%Widget%');

-- Solution 2: Use normalized column
ALTER TABLE products 
ADD COLUMN name_lower TEXT 
GENERATED ALWAYS AS (LOWER(name)) STORED;

SELECT biscuit_setup('products', 'name_lower', 'id');
SELECT * FROM biscuit_match(LOWER('%WIDGET%'));
```

**C. Pattern Beyond 256 Characters**
```sql
-- Diagnosis: Check string length
SELECT id, LENGTH(name) FROM products WHERE id = 123;

-- Problem: Match occurs after position 256
SELECT * FROM biscuit_match('%rare_keyword%');  -- Not found

-- Solution: Ensure critical terms are within first 256 characters
-- Or restructure data to prioritize important terms
```

**D. Triggers Disabled**
```sql
-- Check trigger status
SELECT tgname, tgenabled FROM pg_trigger WHERE tgname = 'biscuit_auto_update';

-- Solution: Re-enable triggers
SELECT biscuit_enable_triggers();

-- Rebuild to sync
SELECT biscuit_build_index('products', 'name', 'id');
```

---

#### Issue 3: Slow Query Performance

**Symptom:**
```sql
\timing on
SELECT * FROM biscuit_match('%pattern%');
Time: 850.234 ms  -- Expected: < 50ms
\timing off
```

**Diagnosis:**
```sql
-- Check tombstone count
SELECT biscuit_get_tombstone_count();
-- If > 5000: Cleanup needed

-- Count pattern parts (% symbols)
-- Pattern: '%a%b%c%d%' has 5 parts (too many)

-- Check result set size
SELECT biscuit_match_count('%pattern%');
-- If > 50% of table: Poor selectivity

-- Check memory usage
SELECT (regexp_matches(biscuit_index_status(), 'Memory: ([0-9.]+) MB'))[1]::NUMERIC;

-- View full statistics
SELECT biscuit_index_status();
```

**Solutions:**

**A. High Tombstone Count**
```sql
-- Problem: Excessive tombstones slow queries
SELECT biscuit_get_tombstone_count();  -- Returns: 8543

-- Solution: Run cleanup
SELECT biscuit_cleanup();

-- Verify improvement
\timing on
SELECT * FROM biscuit_match('%pattern%');
\timing off
```

**B. Complex Pattern**
```sql
-- Problem: Too many pattern parts
SELECT * FROM biscuit_match('%a%b%c%d%e%');  -- 6 parts, slow

-- Solution: Simplify pattern
SELECT * FROM biscuit_match('%abc%de%');  -- 2 parts, faster

-- Or use multiple simpler queries
SELECT * FROM biscuit_match('%abc%') 
WHERE value LIKE '%de%' AND value LIKE '%f%';
```

**C. Large Result Set**
```sql
-- Problem: Returning too many results
SELECT biscuit_match_count('%a%');  -- Returns: 450000 (45% of table)

-- Solution: Add filters to reduce result set
SELECT * FROM biscuit_match('%a%')
WHERE created_at > NOW() - INTERVAL '30 days'
AND status = 'active'
LIMIT 1000;

-- Or reconsider if BISCUIT is appropriate for this query
```

**D. Memory Fragmentation**
```sql
-- Problem: High memory usage affecting performance
SELECT biscuit_index_status();
-- Memory: 250 MB (expected ~50MB for dataset)

-- Solution: Rebuild to compact
SELECT biscuit_build_index('products', 'name', 'id');
```

---

#### Issue 4: High Memory Usage

**Symptom:**
```sql
SELECT biscuit_index_status();
-- Memory: 500 MB (expected ~20 MB for dataset)
```

**Diagnosis:**
```sql
-- Check record count and characteristics
SELECT 
    COUNT(*) AS records,
    AVG(LENGTH(name)) AS avg_length,
    MAX(LENGTH(name)) AS max_length,
    COUNT(DISTINCT name) AS unique_values,
    COUNT(DISTINCT SUBSTRING(name, 1, 1)) AS unique_first_chars
FROM products;

-- Check tombstone accumulation
SELECT biscuit_get_tombstone_count();

-- Monitor memory over time
SELECT (regexp_matches(biscuit_index_status(), 'Memory: ([0-9.]+) MB'))[1]::NUMERIC AS memory_mb;
```

**Solutions:**

**A. Tombstone Accumulation**
```sql
-- Problem: Tombstones not cleaned up
SELECT biscuit_get_tombstone_count();  -- Returns: 15000

-- Solution: Manual cleanup
SELECT biscuit_cleanup();

-- Check memory after cleanup
SELECT (regexp_matches(biscuit_index_status(), 'Memory: ([0-9.]+) MB'))[1]::NUMERIC;
```

**B. Memory Fragmentation**
```sql
-- Solution: Rebuild to compact
SELECT biscuit_build_index('products', 'name', 'id');
```

**C. Unexpected Data Characteristics**
```sql
-- Problem: Very long strings or high diversity
SELECT MAX(LENGTH(name)) FROM products;  -- Returns: 2048

-- Solution: Truncate indexed data (only first 256 chars matter)
ALTER TABLE products ADD COLUMN name_indexed TEXT 
GENERATED ALWAYS AS (LEFT(name, 200)) STORED;

SELECT biscuit_setup('products', 'name_indexed', 'id');
```

---

#### Issue 5: Trigger Not Firing

**Symptom:**
```sql
INSERT INTO products (id, name) VALUES (999, 'test');
SELECT * FROM biscuit_match('%test%');
-- Returns: 0 rows (expected 1 row)
```

**Diagnosis:**
```sql
-- Check trigger existence and status
SELECT 
    tgname,
    tgenabled,
    tgisinternal,
    pg_get_triggerdef(oid) AS definition
FROM pg_trigger
WHERE tgrelid = 'products'::regclass
AND tgname LIKE '%biscuit%';

-- Check PostgreSQL logs for trigger errors
-- Location: /var/log/postgresql/postgresql-14-main.log

-- Test trigger manually
DO $
BEGIN
    PERFORM biscuit_trigger();
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Trigger error: %', SQLERRM;
END $;
```

**Solutions:**

**A. Trigger Disabled**
```sql
-- Check status
SELECT tgenabled FROM pg_trigger 
WHERE tgname = 'biscuit_auto_update'
AND tgrelid = 'products'::regclass;
-- Returns: 'D' (disabled) or no rows

-- Solution: Enable trigger
SELECT biscuit_enable_triggers();

-- Verify
SELECT tgenabled FROM pg_trigger 
WHERE tgname = 'biscuit_auto_update'
AND tgrelid = 'products'::regclass;
-- Should return: 'O' (enabled)
```

**B. Trigger Missing**
```sql
-- Check if trigger exists
SELECT COUNT(*) FROM pg_trigger 
WHERE tgname = 'biscuit_auto_update'
AND tgrelid = 'products'::regclass;
-- Returns: 0

-- Solution: Create trigger
SELECT biscuit_enable_triggers();

-- Or complete setup
SELECT biscuit_setup('products', 'name', 'id');
```

**C. Trigger Error**
```sql
-- Check logs for errors like:
-- "NULL primary key in INSERT"
-- "Column not found"

-- Solution: Verify table structure
\d products

-- Ensure primary key is NOT NULL
ALTER TABLE products ALTER COLUMN id SET NOT NULL;

-- Rebuild with correct configuration
SELECT biscuit_setup('products', 'name', 'id');
```

---

#### Issue 6: "NULL primary key" Error

**Symptom:**
```sql
INSERT INTO products (name) VALUES ('test');
ERROR: NULL primary key in INSERT
```

**Cause:** Primary key column contains NULL value

**Solution:**

**A. Add Default Value**
```sql
-- For SERIAL columns
CREATE SEQUENCE products_id_seq;
ALTER TABLE products 
ALTER COLUMN id SET DEFAULT nextval('products_id_seq');

-- For IDENTITY columns (PostgreSQL 10+)
ALTER TABLE products 
ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY;
```

**B. Ensure NOT NULL Constraint**
```sql
-- Add NOT NULL constraint
ALTER TABLE products ALTER COLUMN id SET NOT NULL;

-- Verify constraint
\d products
```

**C. Provide Explicit Values**
```sql
-- Always specify primary key
INSERT INTO products (id, name) VALUES (1, 'test');

-- Or use nextval
INSERT INTO products (id, name) 
VALUES (nextval('products_id_seq'), 'test');
```

---

#### Issue 7: Out of Memory

**Symptom:**
```sql
SELECT biscuit_build_index('large_table', 'text_column', 'id');
ERROR: out of memory
DETAIL: Failed on request of size X
```

**Diagnosis:**
```sql
-- Check table size
SELECT 
    pg_size_pretty(pg_total_relation_size('large_table')) AS table_size,
    COUNT(*) AS row_count,
    AVG(LENGTH(text_column)) AS avg_length,
    MAX(LENGTH(text_column)) AS max_length
FROM large_table;

-- Check PostgreSQL memory settings
SHOW shared_buffers;
SHOW work_mem;
SHOW maintenance_work_mem;

-- Estimate memory requirement
-- Rough estimate: (row_count × avg_length × 0.2) bytes
```

**Solutions:**

**A. Increase Memory Allocation**
```sql
-- Edit postgresql.conf
-- /etc/postgresql/14/main/postgresql.conf

shared_buffers = 2GB          # Increase from default (128MB)
work_mem = 256MB              # Increase from default (4MB)
maintenance_work_mem = 1GB    # Increase from default (64MB)
```

```bash
# Restart PostgreSQL
sudo systemctl restart postgresql
```

**B. Partition Large Tables**
```sql
-- Create partitioned table
CREATE TABLE large_table_partitioned (
    id SERIAL,
    text_column TEXT,
    created_date DATE
) PARTITION BY RANGE (created_date);

-- Create partitions
CREATE TABLE large_table_2023 
PARTITION OF large_table_partitioned
FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

CREATE TABLE large_table_2024
PARTITION OF large_table_partitioned
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- Index each partition separately
SELECT biscuit_setup('large_table_2023', 'text_column', 'id');
-- Note: Only one partition can be indexed at a time
```

**C. Reduce Data Complexity**
```sql
-- Normalize/simplify data before indexing
UPDATE large_table 
SET text_column = LOWER(LEFT(text_column, 200));

-- Then build index
SELECT biscuit_build_index('large_table', 'text_column', 'id');
```

---

#### Issue 8: Index-Table Mismatch

**Symptom:**
```sql
SELECT COUNT(*) FROM products;
-- Returns: 100000

SELECT biscuit_get_active_count();
-- Returns: 95000  -- Mismatch!
```

**Diagnosis:**
```sql
-- Compare counts
WITH comparison AS (
    SELECT 
        COUNT(*) AS table_count,
        biscuit_get_active_count() AS index_count
    FROM products
)
SELECT 
    table_count,
    index_count,
    table_count - index_count AS discrepancy,
    ROUND(100.0 * index_count / table_count, 2) AS index_coverage_pct
FROM comparison;

-- Check trigger status
SELECT tgname, tgenabled FROM pg_trigger 
WHERE tgname = 'biscuit_auto_update'
AND tgrelid = 'products'::regclass;

-- Check for recent errors
SELECT biscuit_index_status();
```

**Solutions:**

**A. Rebuild Index**
```sql
-- Simple rebuild
SELECT biscuit_build_index('products', 'name', 'id');
SELECT biscuit_enable_triggers();

-- Verify sync
SELECT 
    COUNT(*) AS table_count,
    biscuit_get_active_count() AS index_count
FROM products;
```

**B. Ensure Exclusive Access During Rebuild**
```sql
-- Lock table during rebuild to prevent concurrent modifications
BEGIN;
    LOCK TABLE products IN EXCLUSIVE MODE;
    SELECT biscuit_build_index('products', 'name', 'id');
COMMIT;

SELECT biscuit_enable_triggers();
```

**C. Investigate Trigger Issues**
```sql
-- If mismatch persists, check for trigger errors
-- Review PostgreSQL logs

-- Test manual insert
INSERT INTO products (id, name) VALUES (99999, 'test_sync');

-- Verify immediate sync
SELECT biscuit_match_count('%test_sync%');  -- Should return 1

-- If not synced, rebuild and monitor
SELECT biscuit_setup('products', 'name', 'id');
```

---

### Diagnostic Queries

#### Comprehensive Health Check

```sql
DO $ 
DECLARE
    table_count BIGINT;
    index_count INTEGER;
    tombstone_count INTEGER;
    free_slots INTEGER;
    memory_mb NUMERIC;
    health_status TEXT;
BEGIN
    -- Get metrics
    EXECUTE 'SELECT COUNT(*) FROM products' INTO table_count;
    index_count := biscuit_get_active_count();
    tombstone_count := biscuit_get_tombstone_count();
    free_slots := biscuit_get_free_slots();
    memory_mb := (regexp_matches(biscuit_index_status(), 'Memory: ([0-9.]+) MB'))[1]::NUMERIC;
    
    -- Display report
    RAISE NOTICE '========================================';
    RAISE NOTICE 'BISCUIT Health Check';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Table rows: %', table_count;
    RAISE NOTICE 'Index active: %', index_count;
    RAISE NOTICE 'Discrepancy: % rows', table_count - index_count;
    RAISE NOTICE 'Tombstones: %', tombstone_count;
    RAISE NOTICE 'Free slots: %', free_slots;
    RAISE NOTICE 'Memory: % MB', memory_mb;
    RAISE NOTICE '';
    
    -- Health assessment
    IF table_count != index_count THEN
        RAISE WARNING 'Index out of sync! Rebuild recommended.';
    END IF;
    
    IF tombstone_count > 10000 THEN
        RAISE WARNING 'CRITICAL: % tombstones pending cleanup', tombstone_count;
    ELSIF tombstone_count > 5000 THEN
        RAISE WARNING 'WARNING: % tombstones pending cleanup', tombstone_count;
    END IF;
    
    IF memory_mb > (index_count::NUMERIC / 10000) THEN
        RAISE WARNING 'High memory usage detected';
    END IF;
    
    -- Overall status
    IF table_count = index_count AND tombstone_count < 5000 THEN
        RAISE NOTICE 'Status: HEALTHY';
    ELSIF table_count = index_count AND tombstone_count < 10000 THEN
        RAISE NOTICE 'Status: GOOD (cleanup recommended)';
    ELSE
        RAISE NOTICE 'Status: NEEDS ATTENTION';
    END IF;
END $;
```

#### Performance Baseline Test

```sql
-- Establish performance baselines
\timing on

-- Test 1: All records
SELECT biscuit_match_count('%');

-- Test 2: Common pattern
SELECT biscuit_match_count('%prod%');

-- Test 3: Prefix pattern
SELECT biscuit_match_count('A%');

-- Test 4: Suffix pattern
SELECT biscuit_match_count('%@gmail.com');

-- Test 5: Complex pattern
SELECT biscuit_match_count('%a%b%');

\timing off

-- Save baselines for comparison
CREATE TABLE performance_baseline (
    test_name TEXT,
    pattern TEXT,
    execution_time_ms NUMERIC,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);
```

---

## Performance Expectations

### E-Commerce Product Search

```sql
-- Setup
SELECT biscuit_setup('products', 'name', 'id');

-- Search
SELECT * FROM biscuit_match('%laptop%');
SELECT * FROM biscuit_match('%macbook%') WHERE price < 2000;

-- Performance comparison
\timing on
-- Traditional LIKE: ~1850ms
SELECT COUNT(*) FROM products WHERE name LIKE '%laptop%';

-- BISCUIT: ~3ms (bitmap ops) + result retrieval time
SELECT biscuit_match_count('%laptop%');
\timing off
```

### Customer Email Analysis

```sql
-- Setup
SELECT biscuit_setup('customers', 'email', 'customer_id');

-- Find all Gmail users
SELECT * FROM biscuit_match('%@gmail.com');

-- Domain distribution
WITH email_domains AS (
    SELECT 
        customer_id,
        email,
        SUBSTRING(email FROM '@(.*)') AS domain
    FROM biscuit_match('%')
)
SELECT 
    domain,
    COUNT(*) AS customer_count
FROM email_domains
GROUP BY domain
ORDER BY customer_count DESC;
```

### Log Analysis

```sql
-- Setup
SELECT biscuit_setup('application_logs', 'message', 'log_id');

-- Find errors
SELECT * FROM biscuit_match('%error%')
WHERE level = 'ERROR'
AND timestamp > NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC
LIMIT 100;
```

---

## API Reference

### Index Management

- `biscuit_setup(table, column, pk)` - Complete one-step setup
- `biscuit_build_index(table, column, pk)` - Build index structure
- `biscuit_enable_triggers()` - Activate automatic maintenance
- `biscuit_disable_triggers()` - Deactivate triggers

### Query Functions

- `biscuit_match(pattern)` - Returns full records (type-safe)
- `biscuit_match_rows(pattern)` - Returns (pk, value) pairs
- `biscuit_match_count(pattern)` - Returns count only
- `biscuit_match_keys(pattern)` - Returns (pk_text, value_text)

### Status Functions

- `biscuit_index_status()` - Comprehensive status report
- `biscuit_get_active_count()` - Active record count
- `biscuit_get_free_slots()` - Available slots
- `biscuit_get_tombstone_count()` - Pending tombstones
- `biscuit_version()` - Extension version

### Maintenance

- `biscuit_cleanup()` - Manual tombstone cleanup

---

## Performance Expectations

### What BISCUIT Is Good At

1. **Leading Wildcard Queries**: `'%pattern%'` (traditionally requires full scan)
2. **Multiple Pattern Searches**: Better than repeated LIKE queries
3. **Read-Heavy Workloads**: Optimized for many queries, fewer updates
4. **Moderate Data Sizes**: Works well with millions of records

### What BISCUIT Is Not

1. **Not a Full-Text Search**: Use PostgreSQL's `tsvector` for that
2. **Not a Regex Engine**: Only supports `%` and `_` wildcards
3. **Not Write-Optimized**: Updates require index maintenance
4. **Not Memory-Free**: Requires ~10-30% of data size in RAM

### When to Use BISCUIT

✅ **Good fit**:
- Wildcard search on structured text fields (emails, usernames, SKUs)
- Read-heavy applications
- Need for fast `LIKE '%pattern%'` queries
- Moderate result sets (< 10K matches per query)

❌ **Poor fit**:
- Full-text search with language processing
- Extremely write-heavy workloads (> 10K updates/sec)
- Very large result sets (> 50% of table)
- Need for regex or complex text matching

---

## License

PostgreSQL License (similar to MIT/BSD)

Copyright (c) 2024 BISCUIT Contributors

---

## Version History

**v1.0.3** (Current)
- Improved documentation accuracy
- Bug fixes in slot resurrection
- Enhanced monitoring

**v1.0.0**
- Initial release
- Roaring Bitmap integration
- Lazy deletion with tombstones
- Incremental updates
- O(1) hash table PK lookup

---

## Contributors

Developed and maintained by [Sivaprasad Murali](https://linkedin.com/in/sivaprasad-murali)

**Issues**: https://github.com/crystallinecore/biscuit/issues

---

## Honest Summary

**BISCUIT makes wildcard queries faster by trading memory and build time for query performance.** 

It's not magic—it's a bitmap index that:
- Pre-computes character positions
- Uses fast bitmap operations instead of string scanning
- Still requires O(n) time to return n results
- Requires memory proportional to your data size

**Use it when**: You have moderate-sized datasets with lots of wildcard queries and don't mind the memory overhead.

**Don't use it when**: You need full-text search, have extreme write loads, or are memory-constrained.

---

**When pg_trgm feels half-baked, grab a BISCUIT 🍪**

---