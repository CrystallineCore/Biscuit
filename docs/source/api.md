# API Reference

Complete reference for Biscuit index SQL API, functions, and operators.

---


## Extension Management

### CREATE EXTENSION

Creates the Biscuit extension in the current database.

**Syntax**:
```sql
CREATE EXTENSION biscuit [ WITH ] [ SCHEMA schema_name ] [ VERSION version ];
```

**Parameters**:
- `schema_name` - Schema to install extension (default: current schema)
- `version` - Specific version to install (default: latest)

**Example**:
```sql
-- Install in public schema
CREATE EXTENSION biscuit;

-- Install in specific schema
CREATE EXTENSION biscuit SCHEMA extensions;

-- Install specific version
CREATE EXTENSION biscuit VERSION '1.0';
```

**Returns**: Nothing on success

**Errors**:
- `ERROR: extension "biscuit" already exists` - Extension already installed
- `ERROR: could not open extension control file` - Installation files missing

---

### DROP EXTENSION

Removes the Biscuit extension from the database.

**Syntax**:
```sql
DROP EXTENSION [ IF EXISTS ] biscuit [ CASCADE | RESTRICT ];
```

**Parameters**:
- `IF EXISTS` - Don't error if extension doesn't exist
- `CASCADE` - Drop dependent objects (including indexes)
- `RESTRICT` - Refuse if dependent objects exist (default)

**Example**:
```sql
-- Drop extension (fails if indexes exist)
DROP EXTENSION biscuit;

-- Drop extension and all Biscuit indexes
DROP EXTENSION biscuit CASCADE;

-- Drop only if exists
DROP EXTENSION IF EXISTS biscuit CASCADE;
```

**Side Effects**:
- All Biscuit indexes are dropped (with CASCADE)
- Index cache is cleared
- Memory is freed

---

## Index Operations

### CREATE INDEX

Creates a Biscuit index on one or more columns.

**Syntax**:
```sql
CREATE INDEX [ CONCURRENTLY ] [ IF NOT EXISTS ] index_name
    ON table_name
    USING biscuit ( column_name [, ...] )
    [ WITH ( storage_parameter [= value] [, ... ] ) ]
    [ WHERE predicate ];
```

**Parameters**:
- `index_name` - Name of the index to create
- `table_name` - Table to index
- `column_name` - Column(s) to index (1 or more)
- `CONCURRENTLY` - Build without blocking writes
- `IF NOT EXISTS` - Don't error if index exists
- `UNIQUE` - Not supported (will error)
- `WHERE predicate` - Create partial index

**Supported Column Types**:
- `TEXT`, `VARCHAR`, `CHAR` - Direct indexing

**Examples**:

```sql
-- Single column index
CREATE INDEX idx_products_name 
ON products USING biscuit (name);

-- Multi-column index
CREATE INDEX idx_products_search 
ON products USING biscuit (name, sku, category);

-- Concurrent build (non-blocking)
CREATE INDEX CONCURRENTLY idx_products_name 
ON products USING biscuit (name);

-- Partial index (filtered)
CREATE INDEX idx_active_products 
ON products USING biscuit (name)
WHERE status = 'active';
```

---

### DROP INDEX

Removes a Biscuit index.

**Syntax**:
```sql
DROP INDEX [ CONCURRENTLY ] [ IF EXISTS ] index_name [, ...] [ CASCADE | RESTRICT ];
```

**Example**:
```sql
-- Drop index
DROP INDEX idx_products_name;

-- Drop without blocking
DROP INDEX CONCURRENTLY idx_products_name;

-- Drop multiple indexes
DROP INDEX idx_products_name, idx_products_sku;
```

---

### REINDEX

Rebuilds a Biscuit index.

**Syntax**:
```sql
REINDEX [ ( option [, ...] ) ] { INDEX | TABLE | SCHEMA | DATABASE | SYSTEM } name;
```

**Example**:
```sql
-- Rebuild single index
REINDEX INDEX idx_products_name;

-- Rebuild all indexes on table
REINDEX TABLE products;

-- Rebuild concurrently (PG14+)
REINDEX INDEX CONCURRENTLY idx_products_name;
```

**When to Reindex**:
- After massive data changes (>50% rows)
- Tombstone count exceeds 5000
- Performance degradation detected
- After pg_upgrade

---

## Query Operators

### LIKE Operator

Pattern matching operator supported by Biscuit indexes.

**Syntax**:
```sql
column_name LIKE pattern [ ESCAPE escape_character ]
```

**Pattern Syntax**:
- `%` - Matches zero or more characters
- `_` - Matches exactly one character
- Other characters match themselves
- Case-sensitive matching

**Examples**:

```sql
-- Prefix match
SELECT * FROM products WHERE name LIKE 'Wireless%';

-- Suffix match
SELECT * FROM products WHERE name LIKE '%Mouse';

-- Substring match
SELECT * FROM products WHERE name LIKE '%gaming%';

-- Underscore wildcard
SELECT * FROM products WHERE sku LIKE 'PROD-____-2024';

-- Exact match
SELECT * FROM products WHERE name LIKE 'Wireless Mouse';

-- Complex pattern
SELECT * FROM products WHERE name LIKE 'Wireless%RGB%Gaming%';

-- With ESCAPE character
SELECT * FROM products WHERE name LIKE 'Price: 100\%' ESCAPE '\';
```

**Performance**: See [Pattern Syntax Guide](patterns.md) for optimization details

---

### NOT LIKE Operator

Negated pattern matching.

**Syntax**:
```sql
column_name NOT LIKE pattern
```

**Example**:
```sql
-- Find products NOT containing "test"
SELECT * FROM products WHERE name NOT LIKE '%test%';
```

---

### ILIKE Operator

Case-insensitive pattern matching is supported from versions >= 2.1.0

**Workaround**:
```sql
-- Create index
CREATE INDEX idx_products
ON products USING biscuit (name);

-- Query 
SELECT * FROM products WHERE name ILIKE '%wireless%';
```

---

### NOT ILIKE Operator

Case-insensitive pattern matching is supported from versions >= 2.1.0

**Workaround**:
```sql
-- Create index
CREATE INDEX idx_products
ON products USING biscuit (name);

-- Query 
SELECT * FROM products WHERE name NOT ILIKE '%wireless%';
```

---

### Multi-Column Queries

Query multiple columns with automatic optimization.

**Syntax**:
```sql
WHERE column1 LIKE pattern1
  AND column2 LIKE pattern2
  AND column3 LIKE pattern3;
```

**Example**:
```sql
-- Automatic predicate reordering
SELECT * FROM products 
WHERE name LIKE '%laptop%'          -- Priority: 50 (substring)
  AND brand LIKE 'Dell%'            -- Priority: 20 (prefix)
  AND category LIKE 'Computer%';    -- Priority: 21 (prefix)

-- Execution order: brand → category → name
```

**Performance**: Biscuit automatically reorders predicates by selectivity for optimal performance.

---

## Build Diagnostic Functions

### biscuit_has_roaring()

Checks if the extension was compiled with CRoaring bitmap support.

**Syntax**:
```sql
biscuit_has_roaring() RETURNS boolean
```

**Parameters**: None

**Returns**: `true` if compiled with Roaring support, `false` otherwise

**Example**:
```sql
-- Check Roaring support
SELECT biscuit_has_roaring();

-- Conditional query
SELECT 
    CASE 
        WHEN biscuit_has_roaring() THEN 'Optimal performance'
        ELSE 'Using fallback implementation'
    END as performance_status;
```

**Performance Impact**:
- `true`: High-performance CRoaring bitmaps 
- `false`: Fallback implementation with high memory footprint

**When to Rebuild**:
If this returns `false`, install CRoaring and rebuild:
```bash
# Debian/Ubuntu
sudo apt-get install libroaring-dev
make clean && make && sudo make install

# macOS
brew install croaring
make clean && make && sudo make install
```

---

### biscuit_version()

Returns the Biscuit extension version string.

**Syntax**:
```sql
biscuit_version() RETURNS text
```

**Parameters**: None

**Returns**: Version string (e.g., `"1.0.0"`)

**Example**:
```sql
-- Get version
SELECT biscuit_version();

-- Check minimum version
SELECT biscuit_version() >= '1.0.0' as meets_requirement;
```

---

### biscuit_roaring_version()

Returns the CRoaring library version if available.

**Syntax**:
```sql
biscuit_roaring_version() RETURNS text
```

**Parameters**: None

**Returns**: 
- Roaring version string (e.g., `"2.0.4"`) if compiled with Roaring
- `NULL` if not compiled with Roaring support

**Example**:
```sql
-- Get Roaring version
SELECT biscuit_roaring_version();

-- Check if Roaring is available
SELECT biscuit_roaring_version() IS NOT NULL as has_roaring;

-- Full version report
SELECT 
    biscuit_version() as extension_version,
    biscuit_roaring_version() as roaring_version,
    CASE 
        WHEN biscuit_roaring_version() IS NOT NULL THEN 'Optimal'
        ELSE 'Fallback'
    END as performance_mode;
```

**Output**:
```
 extension_version | roaring_version | performance_mode
-------------------+-----------------+------------------
 2.1.5            | 2.0.4           | Optimal
```

---

### biscuit_build_info()

Returns detailed build-time configuration information.

**Syntax**:
```sql
biscuit_build_info() RETURNS TABLE (
    feature text,
    enabled boolean,
    description text
)
```

**Parameters**: None

**Returns**: Table with build configuration details

**Columns**:
- `feature` - Feature or library name
- `enabled` - Whether the feature is enabled
- `description` - Detailed description

**Example**:
```sql
-- Get all build information
SELECT * FROM biscuit_build_info();

-- Check specific features
SELECT feature, enabled 
FROM biscuit_build_info() 
WHERE feature = 'CRoaring Bitmaps';

-- Filter enabled features only
SELECT feature, description 
FROM biscuit_build_info() 
WHERE enabled = true;
```

**Output**:
```
      feature       | enabled |              description
--------------------+---------+---------------------------------------
 CRoaring Bitmaps   | t       | High-performance bitmap operations...
 PostgreSQL         | t       | Compiled for PostgreSQL 16.1
```

---

### biscuit_build_info_json()

Returns build configuration as a JSON string for automation and scripting.

**Syntax**:
```sql
biscuit_build_info_json() RETURNS text
```

**Parameters**: None

**Returns**: JSON-formatted string with build information

**Example**:
```sql
-- Get JSON build info
SELECT biscuit_build_info_json();

-- Parse as JSON
SELECT biscuit_build_info_json()::json;

-- Extract specific fields
SELECT 
    (biscuit_build_info_json()::json)->>'version' as version,
    (biscuit_build_info_json()::json)->>'roaring_enabled' as roaring,
    (biscuit_build_info_json()::json)->>'postgres_version' as pg_version;
```

**Output Format**:
```json
{
  "version": "1.0.0",
  "roaring_enabled": true,
  "roaring_version": "2.0.4",
  "postgres_version": "16.1",
  "build_date": "Dec 16 2025 10:30:00"
}
```

**Use Cases**:
- CI/CD validation scripts
- Automated deployment checks
- Monitoring systems
- Version compatibility verification

**Shell Script Example**:
```bash
#!/bin/bash
# Check if Roaring is enabled
ROARING=$(psql -t -c "SELECT (biscuit_build_info_json()::json)->>'roaring_enabled';")

if [ "$ROARING" != "true" ]; then
    echo "Warning: Roaring not enabled. Installing CRoaring..."
    sudo apt-get install libroaring-dev
    make clean && make && sudo make install
fi
```

---

### biscuit_check_config()

Performs a comprehensive configuration health check and provides recommendations.

**Syntax**:
```sql
biscuit_check_config() RETURNS TABLE (
    check_name text,
    status text,
    recommendation text
)
```

**Parameters**: None

**Returns**: Table with configuration checks and recommendations

**Columns**:
- `check_name` - Name of the configuration check
- `status` - Current status with visual indicators
- `recommendation` - Action recommendation

**Example**:
```sql
-- Run full configuration check
SELECT * FROM biscuit_check_config();

-- Check for issues
SELECT * FROM biscuit_check_config() 
WHERE status NOT LIKE '✓%';

-- Save check results
CREATE TABLE biscuit_health_log AS
SELECT now() as checked_at, * 
FROM biscuit_check_config();
```

**Output**:
```
   check_name     |      status       |                recommendation
------------------+-------------------+----------------------------------------------
 Roaring Support  | ✓ Enabled         | Optimal configuration (v2.0.4)
 Extension Ver... | 1.0.0             | Current version
 Active Indexes   | 5                 | Indexes are active
```



### biscuit_index_stats()

Returns detailed statistics about a Biscuit index.

**Syntax**:
```sql
biscuit_index_stats(index_oid regclass) RETURNS text
```

**Parameters**:
- `index_oid` - OID or name of the Biscuit index

**Returns**: Multi-line text report with statistics

**Example**:
```sql
-- Get statistics for an index
SELECT biscuit_index_stats('idx_products_name'::regclass);

-- With formatted output
\x
SELECT biscuit_index_stats('idx_products_name'::regclass);
\x
```

**Output Format**:
```
Biscuit Index Statistics (FULLY OPTIMIZED)
==========================================
Index: idx_products_name
Active records: 50000
Total slots: 50245
Free slots: 245
Tombstones: 0
Max length: 128
------------------------
CRUD Statistics:
  Inserts: 50245
  Updates: 1205
  Deletes: 245
------------------------
Active Optimizations:
  ✓ 1. Skip wildcard intersections
  ✓ 2. Early termination on empty
  [... 12 optimizations listed ...]
```

**Metrics Explained**:

| Metric | Description | Ideal Value |
|--------|-------------|-------------|
| Active records | Current valid records | = table row count |
| Total slots | Allocated memory slots | ≥ active records |
| Free slots | Reusable slots from deletes | < 1000 |
| Tombstones | Deleted but not cleaned | < 1000 |
| Inserts | Total insert operations | Monotonic increase |
| Updates | Total update operations | - |
| Deletes | Total delete operations | - |



---

### biscuit_index_memory_size()

Returns the in-memory footprint of a Biscuit index in bytes.

**Syntax**:

```sql
biscuit_index_memory_size(index_oid oid) RETURNS bigint
biscuit_index_memory_size(index_name text) RETURNS bigint
```

**Parameters**:

* `index_oid` — OID of the Biscuit index
* `index_name` — Name of the Biscuit index

**Returns**:
Memory usage in bytes for the specified Biscuit index.

**Example**:

```sql
-- Get memory usage using index OID
SELECT biscuit_index_memory_size('idx_products_name'::regclass::oid);

-- Get memory usage using index name
SELECT biscuit_index_memory_size('idx_products_name');
```

---

### biscuit_size_pretty()

Returns a human-readable representation of the in-memory footprint of a Biscuit index.

**Syntax**:

```sql
biscuit_size_pretty(index_name text) RETURNS text
```

**Parameters**:

* `index_name` — Name of the Biscuit index

**Returns**:
Formatted memory usage (bytes, KB, MB, or GB).

**Example**:

```sql
SELECT biscuit_size_pretty('idx_products_name');
```

**Output Format**:

```
128 MB (134217728 bytes)
```


---

## Diagnostic Views

### biscuit_status

Quick overview of extension status and configuration.

**Definition**:
```sql
biscuit_status
```

**Columns**:
- `version` - Extension version
- `roaring_enabled` - Whether CRoaring is enabled
- `bitmap_implementation` - Current bitmap backend
- `total_indexes` - Number of active Biscuit indexes
- `total_index_size` - Combined size of all indexes

**Example**:
```sql
-- View current status
SELECT * FROM biscuit_status;

-- Monitor status changes
CREATE TABLE biscuit_status_history AS
SELECT now() as timestamp, * FROM biscuit_status;
```

**Output**:
```
 version | roaring_enabled | bitmap_implementation  | total_indexes | total_index_size
---------+-----------------+------------------------+---------------+------------------
 1.0.0   | t               | Optimal (CRoaring)     | 5             | 245 MB
```

---

### biscuit_memory_usage

Displays memory and disk usage for all Biscuit indexes in the current database.

**Definition**:

```sql
biscuit_memory_usage
```

**Columns**:

* `schemaname` — Schema containing the index
* `tablename` — Table on which the index is defined
* `indexname` — Name of the Biscuit index
* `bytes` — In-memory size (bytes)
* `human_readable` — Formatted memory usage
* `disk_size` — On-disk size (`pg_relation_size`)

**Example**:

```sql
-- List all Biscuit indexes with memory usage
SELECT * FROM biscuit_memory_usage;
```

**Usage**:

```sql
-- Identify largest Biscuit indexes in memory
SELECT
    indexname,
    human_readable
FROM biscuit_memory_usage
ORDER BY bytes DESC
LIMIT 5;
```



### Notes

* `pg_relation_size()` and `pg_size_pretty()` report **only the on-disk footprint** of a Biscuit index.
* Biscuit maintains its primary data structures in memory for performance; disk size may significantly underrepresent total runtime usage.
* Reported values reflect the current in-memory state and may change over time.


---

## Configuration Parameters

### Server Parameters

These PostgreSQL parameters affect Biscuit performance:

#### shared_buffers

**Description**: Memory for caching index data

**Syntax**:
```sql
-- In postgresql.conf
shared_buffers = '4GB'

-- Or per session
SET shared_buffers = '4GB';  -- Requires restart
```

**Recommendation**: 25% of system RAM

---

#### work_mem

**Description**: Memory for sorting and bitmap operations

**Syntax**:
```sql
-- In postgresql.conf
work_mem = '256MB'

-- Or per session
SET work_mem = '256MB';
```

**Recommendation**: 
- Small queries: 64MB
- Medium queries: 256MB
- Large queries: 512MB-1GB

---

#### maintenance_work_mem

**Description**: Memory for index building

**Syntax**:
```sql
-- In postgresql.conf
maintenance_work_mem = '1GB'

-- Or for current session
SET maintenance_work_mem = '1GB';
```


---

#### effective_cache_size

**Description**: Hint to planner about available cache

**Syntax**:
```sql
-- In postgresql.conf
effective_cache_size = '12GB'
```

---

#### enable_seqscan

**Description**: Allow/disallow sequential scans

**Syntax**:
```sql
-- Disable sequential scans (testing only!)
SET enable_seqscan = off;

-- Re-enable
SET enable_seqscan = on;
```

**Use Case**: Force index usage during testing

---

#### max_parallel_workers_per_gather

**Description**: Workers for parallel bitmap scans

**Syntax**:
```sql
-- In postgresql.conf
max_parallel_workers_per_gather = 4

-- Or per session
SET max_parallel_workers_per_gather = 4;
```

**Recommendation**: 2-4 for most workloads

---

### Session Parameters

Parameters you can set per connection:

```sql
-- Increase work memory for this session
SET work_mem = '512MB';

-- Enable query timing
\timing on

-- Show query plans
SET client_min_messages = INFO;

-- Force index usage (testing)
SET enable_seqscan = off;

-- Enable parallel queries
SET max_parallel_workers_per_gather = 4;

-- Reset all to defaults
RESET ALL;
```


---

## System Views

### pg_index

Check if an index is a Biscuit index:

```sql
SELECT 
    indexrelid::regclass as index_name,
    indrelid::regclass as table_name,
    indnatts as num_columns
FROM pg_index
WHERE indexrelid::regclass::text LIKE '%biscuit%';
```

---

### pg_stat_user_indexes

Monitor index usage:

```sql
SELECT 
    indexrelname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch,
    pg_size_pretty(pg_relation_size(indexrelid)) as size
FROM pg_stat_user_indexes
WHERE indexrelname LIKE '%biscuit%';
```

---

### pg_indexes

View index definitions:

```sql
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE indexdef LIKE '%biscuit%';
```

---

## Error Messages

Common errors and solutions:

### ERROR: access method "biscuit" does not exist

**Cause**: Extension not installed

**Solution**:
```sql
CREATE EXTENSION biscuit;
```

---

### ERROR: data type X is not supported for biscuit index

**Cause**: Attempting to index unsupported type

**Solution**: Cast to TEXT or use expression index
```sql
CREATE INDEX idx ON table1 USING biscuit (column::TEXT);
```

---

### ERROR: could not open relation with OID

**Cause**: Index cache corruption after crash

**Solution**:
```sql
REINDEX INDEX idx_name;
```

---

### WARNING: Biscuit: Index cache miss

**Cause**: Normal - index loaded on first use

**Solution**: No action needed (informational only)


---

### ISSUE: biscuit_has_roaring() returns false

**Symptoms**:
```sql
SELECT biscuit_has_roaring();
-- Returns: false
```

**Diagnosis**:
```sql
SELECT * FROM biscuit_check_config();
-- Shows: Roaring Support | ✗ Disabled | Install CRoaring...
```

**Solution**:
```bash
# Install CRoaring development library
# Debian/Ubuntu
sudo apt-get update
sudo apt-get install libroaring-dev

# Verify installation
dpkg -L libroaring-dev | grep roaring.h

# Rebuild extension
cd /path/to/biscuit
make clean
make
sudo make install

# Restart PostgreSQL
sudo systemctl restart postgresql

# Verify in PostgreSQL
psql -c "DROP EXTENSION biscuit CASCADE;"
psql -c "CREATE EXTENSION biscuit;"
psql -c "SELECT biscuit_has_roaring();"
-- Should return: true
```

## Next Steps

- Learn how it works: [Architecture Guide](architecture.md)
-  Master patterns: [Pattern Syntax](patterns.md)
-  Optimize queries: [Performance Tuning](performance.md)
-  Get help: [FAQ](faq.md)
