# API Reference

Complete reference for Biscuit index SQL API, functions, and operators.

---

## Table of Contents

1. [Extension Management](#extension-management)
2. [Index Operations](#index-operations)
3. [Query Operators](#query-operators)
4. [Diagnostic Functions](#diagnostic-functions)
5. [Configuration Parameters](#configuration-parameters)
6. [Data Types](#data-types)

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
CREATE [ UNIQUE ] INDEX [ CONCURRENTLY ] [ IF NOT EXISTS ] index_name
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
- `INTEGER`, `BIGINT`, `SMALLINT` - Converted to sortable text
- `FLOAT`, `DOUBLE PRECISION` - Converted to scientific notation
- `DATE` - Converted to sortable integer
- `TIMESTAMP`, `TIMESTAMPTZ` - Converted to microseconds
- `BOOLEAN` - Converted to 't' or 'f'

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

-- Index with type conversion
CREATE INDEX idx_orders_date 
ON orders USING biscuit (created_at);

-- Index on expression
CREATE INDEX idx_products_lower 
ON products USING biscuit (LOWER(name));
```

**Limitations**:
- Maximum 256 characters per string (longer strings truncated)
- Maximum 256 positions indexed per string
- UNIQUE constraint not supported
- ORDER BY not supported in index

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

Case-insensitive pattern matching (not directly supported).

**Workaround**:
```sql
-- Create index on LOWER()
CREATE INDEX idx_products_lower 
ON products USING biscuit (LOWER(name));

-- Query with LOWER()
SELECT * FROM products WHERE LOWER(name) LIKE '%wireless%';
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

## Diagnostic Functions

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
| Max length | Longest indexed string | < 256 |
| Inserts | Total insert operations | Monotonic increase |
| Updates | Total update operations | - |
| Deletes | Total delete operations | - |

**Usage**:
```sql
-- Monitor index health
SELECT 
    indexrelid::regclass as index_name,
    biscuit_index_stats(indexrelid) as stats
FROM pg_index 
WHERE indexrelid::regclass::text LIKE '%biscuit%';

-- Create monitoring view
CREATE VIEW biscuit_health AS
SELECT 
    i.indexrelid::regclass as index_name,
    t.relname as table_name,
    pg_size_pretty(pg_relation_size(i.indexrelid)) as index_size,
    biscuit_index_stats(i.indexrelid) as stats
FROM pg_index i
JOIN pg_class t ON i.indrelid = t.oid
WHERE i.indexrelid::regclass::text LIKE '%biscuit%';
```

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

**Recommendation**: 1GB minimum for large indexes

---

#### effective_cache_size

**Description**: Hint to planner about available cache

**Syntax**:
```sql
-- In postgresql.conf
effective_cache_size = '12GB'
```

**Recommendation**: 75% of system RAM

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

## Data Types

### Supported Input Types

Biscuit indexes can be created on these column types:

#### Text Types

```sql
-- Direct support (no conversion)
TEXT
VARCHAR(n)
CHAR(n)
NAME
```

**Storage**: As-is, up to 256 characters

**Example**:
```sql
CREATE INDEX idx_text ON table1 USING biscuit (text_column);
```

---

#### Numeric Types

```sql
-- Converted to sortable text
SMALLINT, INTEGER, BIGINT
NUMERIC, DECIMAL
REAL, DOUBLE PRECISION
```

**Storage Format**:
- Integers: `+0000000042` (sign + zero-padded)
- Floats: `1.234567e+02` (scientific notation)

**Example**:
```sql
CREATE INDEX idx_price ON products USING biscuit (price);

-- Query with pattern
SELECT * FROM products WHERE price::TEXT LIKE '1%';
```

**Use Cases**:
- Price ranges: `WHERE price::TEXT LIKE '1%'` (100-199)
- ID prefixes: `WHERE id::TEXT LIKE '2024%'`

---

#### Date/Time Types

```sql
-- Converted to sortable integers
DATE
TIMESTAMP
TIMESTAMPTZ
TIME
```

**Storage Format**:
- DATE: Days since epoch (zero-padded)
- TIMESTAMP: Microseconds since epoch

**Example**:
```sql
CREATE INDEX idx_created ON orders USING biscuit (created_at);

-- Query by year-month
SELECT * FROM orders WHERE created_at::TEXT LIKE '2024-01%';

-- Query by year
SELECT * FROM orders WHERE created_at::TEXT LIKE '2024%';
```

---

#### Boolean Type

```sql
BOOLEAN
```

**Storage**: `'t'` or `'f'`

**Example**:
```sql
CREATE INDEX idx_active ON users USING biscuit (is_active);

-- Query
SELECT * FROM users WHERE is_active::TEXT LIKE 't';
```

**Note**: B-tree is more efficient for boolean columns

---

### Unsupported Types

These types cannot be indexed directly:

- `BYTEA` - Binary data
- `JSON`, `JSONB` - Use expression index on text field
- `ARRAY` - Use expression index on array_to_string
- `UUID` - Cast to TEXT first
- `INET`, `CIDR` - Cast to TEXT first
- `GEOMETRY` (PostGIS) - Not supported

**Workarounds**:

```sql
-- JSON field
CREATE INDEX idx_json ON table1 USING biscuit ((data->>'name'));

-- Array
CREATE INDEX idx_array ON table1 
USING biscuit (array_to_string(tags, ','));

-- UUID
CREATE INDEX idx_uuid ON table1 USING biscuit (id::TEXT);
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

## Next Steps

- Learn how it works: [Architecture Guide](architecture.md)
-  Master patterns: [Pattern Syntax](patterns.md)
-  Optimize queries: [Performance Tuning](performance.md)
-  Get help: [FAQ](faq.md)

---