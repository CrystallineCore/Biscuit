# Multi-Column Indexes

Advanced guide to using Biscuit with multiple columns, including automatic query optimization and best practices.

---

## Overview

Biscuit's multi-column support allows you to create indexes spanning multiple text columns. The query planner automatically reorders predicates for optimal performance based on selectivity analysis.

**Key Features**:
- Automatic predicate reordering
-  Per-column bitmap indices  
-  Intelligent query planning
-  Support for mixed data types
-  Early termination optimization

---

## Creating Multi-Column Indexes

### Basic Syntax

```sql
CREATE INDEX index_name ON table_name 
USING biscuit (column1, column2, column3, ...);
```

### Example: E-Commerce Product Search

```sql
-- Create multi-column index
CREATE INDEX idx_products_search ON products 
USING biscuit (name, description, category, sku);

-- All columns are now searchable with LIKE
SELECT * FROM products 
WHERE name LIKE '%laptop%'
  AND description LIKE '%gaming%'
  AND category LIKE 'Elect%'
  AND sku LIKE 'SKU-2024%';
```

---

## How Query Optimization Works

Biscuit analyzes each predicate and assigns a **priority** based on:

1. **Selectivity**: How much data gets filtered
2. **Pattern type**: Exact > Prefix > Suffix > Substring
3. **Concrete characters**: More = better selectivity
4. **Anchor strength**: Strong anchors filter more

### Automatic Reordering Example

```sql
-- Your query (predicates in arbitrary order)
SELECT * FROM users 
WHERE department LIKE '%engineering%'    -- Low selectivity
  AND username LIKE 'admin%'              -- High selectivity
  AND email LIKE '%@company.com';         -- Medium selectivity

-- Biscuit automatically executes as:
-- 1. username LIKE 'admin%'           (Priority 20 - prefix, high select)
-- 2. email LIKE '%@company.com'       (Priority 30 - suffix, medium)
-- 3. department LIKE '%engineering%'  (Priority 50 - substring, low)
```

**Result**: Query runs faster by filtering aggressively first!

---

## Query Planning Deep Dive

### Priority Calculation

Biscuit assigns priorities in tiers:

| Tier | Priority | Pattern Type | Example |
|------|----------|--------------|---------|
| 1 | 0-10 | Exact match | `'admin'` |
| 2 | 10-20 | Underscore patterns | `'test__%'` |
| 3 | 20-30 | Strong prefix/suffix | `'admin%'`, `'%@company.com'` |
| 4 | 30-40 | Weak anchors | `'a%'`, `'%m'` |
| 5 | 40-50 | Multi-partition | `'%a%b%c%'` |
| 6 | 50+ | Pure substring | `'%test%'` |

### Selectivity Scoring

```
Selectivity Formula:
  base = 1.0 / (concrete_chars + 1)
  - underscore_bonus = count('_') × 0.05
  - partition_penalty = count(partitions) × 0.15
  - anchor_bonus = anchor_strength / 200
  + substring_penalty = 0.5 (if '%...%')
```

**Lower score = more selective = executes first**

---

## Viewing Query Plans

Use the debug logging to see execution order:

```sql
-- Enable query logging
SET client_min_messages = INFO;

-- Run query
SELECT * FROM products 
WHERE name LIKE '%laptop%'
  AND sku LIKE 'SKU-2024%'
  AND category LIKE 'Elect%';
```

**Output**:
```
INFO:  === BISCUIT QUERY EXECUTION PLAN ===
INFO:  Total predicates: 3
INFO:  [0] Col=1 Pattern='SKU-2024%' Type=PREFIX Priority=20 
       Selectivity=0.048
INFO:  [1] Col=2 Pattern='Elect%' Type=PREFIX Priority=21 
       Selectivity=0.091
INFO:  [2] Col=0 Pattern='%laptop%' Type=SUBSTRING Priority=50 
       Selectivity=0.450
INFO:  ====================================
```

---

## Best Practices

### 1. Order Columns by Query Frequency

```sql
-- ✅ GOOD: Most-queried columns first
CREATE INDEX idx_products ON products 
USING biscuit (name, category, sku);

--  LESS OPTIMAL: Rarely-queried columns first  
CREATE INDEX idx_products ON products 
USING biscuit (internal_notes, name, category);
```

**Why?** Column order doesn't affect query performance (Biscuit reorders automatically), but it's clearer for maintenance.

---

### 2. Mix Selective and General Columns

```sql
-- ✅ GOOD: Mix of selective and broad columns
CREATE INDEX idx_users ON users 
USING biscuit (username, email, department);
-- username: high selectivity (unique)
-- email: medium selectivity (domains)
-- department: low selectivity (few values)

-- This allows queries like:
WHERE department LIKE '%sales%'     -- Broad filter
  AND username LIKE 'john%';        -- Narrows down quickly
```

---

### 3. Consider Column Cardinality

```sql
-- Check column cardinality
SELECT 
    COUNT(DISTINCT name) as name_unique,
    COUNT(DISTINCT category) as category_unique,
    COUNT(*) as total
FROM products;

-- High cardinality (many unique values) = better for Biscuit
-- Low cardinality (few values) = consider other strategies
```

**Rule of Thumb**:
- High cardinality (>1000 unique): ✅ Excellent for Biscuit
- Medium cardinality (100-1000): ✅ Good for Biscuit
- Low cardinality (<100): ⚠️ Consider B-tree or partial index

---

### 4. Avoid Redundant Indexes

```sql
--  DON'T: Create overlapping indexes
CREATE INDEX idx1 ON products USING biscuit (name);
CREATE INDEX idx2 ON products USING biscuit (name, category);
-- idx1 is redundant!

-- ✅ DO: Use multi-column for flexibility
CREATE INDEX idx_products ON products 
USING biscuit (name, category, sku);
-- Handles queries on any combination of these columns
```

---

## Advanced Patterns

### Pattern 1: User Search with Email Domain

```sql
CREATE INDEX idx_users_search ON users 
USING biscuit (username, email, full_name);

-- Find all admins at company.com
SELECT * FROM users 
WHERE username LIKE 'admin%'
  AND email LIKE '%@company.com';

-- Execution:
-- 1. username filter (prefix, high selectivity)
-- 2. email filter (suffix, medium selectivity)
-- Result: ~2-5ms
```

---

### Pattern 2: Product Search with Multiple Attributes

```sql
CREATE INDEX idx_products_full ON products 
USING biscuit (name, brand, category, sku, tags);

-- Complex product search
SELECT * FROM products 
WHERE name LIKE '%laptop%'
  AND brand LIKE 'Dell%'
  AND category LIKE 'Computers%'
  AND tags LIKE '%business%';

-- Biscuit reorders to execute brand first (prefix),
-- then category, then tags, then name (substring last)
```

---

### Pattern 3: Log Analysis

```sql
CREATE INDEX idx_logs ON application_logs 
USING biscuit (level, service, message, user_id);

-- Find errors from auth service with specific message
SELECT * FROM application_logs 
WHERE level LIKE 'ERROR'              -- Exact (fastest)
  AND service LIKE 'auth%'            -- Prefix (fast)
  AND message LIKE '%timeout%'        -- Substring (slower)
  AND user_id LIKE 'user_%';          -- Underscore (fast)

-- Execution order: level → service → user_id → message
```

---

## Type Conversion Support

Biscuit supports multiple data types through automatic text conversion:

### Supported Types

```sql
CREATE INDEX idx_orders ON orders 
USING biscuit (
    customer_name,      -- TEXT
    order_id,           -- INTEGER → sortable text
    created_at,         -- TIMESTAMP → sortable text
    total_amount,       -- NUMERIC → sortable text
    status              -- VARCHAR
);

-- Query with type conversion
SELECT * FROM orders 
WHERE created_at::TEXT LIKE '2024-01%'
  AND total_amount::TEXT LIKE '1%'
  AND customer_name LIKE '%Smith%';
```

### Type Conversion Details

| Type | Conversion | Example |
|------|------------|---------|
| INTEGER | Zero-padded with sign | `+0000000042` |
| FLOAT | Scientific notation | `1.500000e+02` |
| DATE | Zero-padded days | `+0018628` |
| TIMESTAMP | Microsecond precision | `00001704067200000000` |
| BOOLEAN | 't' or 'f' | `t` |

**Note**: Conversions are sortable and comparable!

---

## Performance Characteristics

### Query Complexity vs. Performance

| Predicate Count | Typical Time | Optimization |
|-----------------|--------------|--------------|
| 1 column | 1-5ms | Single bitmap lookup |
| 2 columns | 2-8ms | Sequential intersection |
| 3-4 columns | 5-15ms | Early termination |
| 5+ columns | 10-30ms | LIMIT-aware collection |

### Early Termination Example

```sql
-- Query with 5 predicates
SELECT * FROM products 
WHERE name LIKE '%laptop%'        -- Priority 50
  AND brand LIKE 'Dell%'          -- Priority 20
  AND price::TEXT LIKE '1%'       -- Priority 25
  AND category LIKE 'Comp%'       -- Priority 21  
  AND rating::TEXT LIKE '4%';     -- Priority 22

-- Execution:
-- [1] brand filter → 1000 results
-- [2] category filter → 500 results
-- [3] rating filter → 200 results
-- [4] price filter → 50 results
-- [5] name filter (stopped early if <10 results remain)
```

---

## LIMIT Optimization

Biscuit optimizes LIMIT queries with early termination:

```sql
-- Only need 10 results
SELECT * FROM products 
WHERE name LIKE '%laptop%'
  AND brand LIKE '%'
  AND category LIKE '%'
LIMIT 10;

-- Biscuit stops collecting after ~20 TIDs
-- (2x buffer for safety)
```

**Benefit**: Faster for small LIMIT values!

---

## Monitoring and Diagnostics

### Check Index Statistics

```sql
SELECT biscuit_index_stats('idx_products_search'::regclass);
```

Output shows per-column statistics:
```
Biscuit Index Statistics
========================
Index: idx_products_search
Active records: 50000
Columns: 4

Column 0 (name):
  Max length: 128
  Character bitmaps: 256
  Length bitmaps: [0..128]
  
Column 1 (category):  
  Max length: 64
  Character bitmaps: 256
  Length bitmaps: [0..64]

[... continues for each column ...]
```

---

### Query Performance Analysis

```sql
EXPLAIN (ANALYZE, BUFFERS) 
SELECT * FROM products 
WHERE name LIKE '%laptop%'
  AND category LIKE 'Elect%';
```

Look for:
- **Index Scan using Biscuit**: ✅ Good
- **Seq Scan**:  Index not being used
- **Planning Time**: Should be <1ms
- **Execution Time**: Should be <20ms for typical queries

---

## Common Pitfalls

###  Pitfall 1: Too Many Columns

```sql
-- DON'T: Index everything
CREATE INDEX idx_huge ON products 
USING biscuit (col1, col2, col3, col4, col5, col6, col7, col8);
```

**Problem**: Larger index, slower builds, more memory

**Solution**: Index only frequently-queried columns 

---

###  Pitfall 2: Low Selectivity Patterns

```sql
-- All predicates are low-selectivity substrings
WHERE col1 LIKE '%a%'
  AND col2 LIKE '%e%'  
  AND col3 LIKE '%i%';
```

**Problem**: Even with reordering, all filters are weak

**Solution**: Combine with equality filters or use better patterns

---

###  Pitfall 3: Not Using EXPLAIN

```sql
-- Assuming the index is being used
SELECT * FROM products WHERE name LIKE '%laptop%';
```

**Problem**: Might be doing sequential scan!

**Solution**: Always verify with `EXPLAIN ANALYZE`

---

## Migration from Single-Column

Migrating from single-column to multi-column is straightforward:

```sql
-- Old: Multiple single-column indexes
CREATE INDEX idx_name ON products USING biscuit (name);
CREATE INDEX idx_sku ON products USING biscuit (sku);
CREATE INDEX idx_category ON products USING biscuit (category);

-- New: Single multi-column index
CREATE INDEX idx_products_multi ON products 
USING biscuit (name, sku, category);

-- Drop old indexes
DROP INDEX idx_name;
DROP INDEX idx_sku;
DROP INDEX idx_category;
```

**Benefits**:
- Single index to maintain
- Automatic query optimization
- Support for multi-column queries
- Reduced disk space

---

## Next Steps

-  Learn [Performance Tuning](performance.md) techniques
-  Understand the [Architecture](architecture.md)
-  Review [Pattern Syntax](patterns.md)
-  Check the [FAQ](faq.md)
