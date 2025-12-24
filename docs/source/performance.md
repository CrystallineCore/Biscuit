# Performance Tuning Guide

Comprehensive guide to optimizing Biscuit index performance for maximum query speed.

---

## Quick Performance Checklist

Before diving deep, verify these essentials:

-  Index is actually being used (`EXPLAIN ANALYZE`)
-  Patterns have concrete characters (not just `%`)
-  PostgreSQL memory settings are appropriate
-  Statistics are up to date (`ANALYZE`)
-  No competing indexes interfering

---

## Understanding Biscuit Performance


### Active Optimizations

Biscuit includes 12 built-in optimizations:

1. **Skip wildcard intersections** - `_` wildcards don't create bitmaps
2. **Early termination** - Stop when no candidates remain
3. **Avoid redundant copies** - Reuse bitmaps when possible
4. **Single-part fast path** - Optimized prefix/suffix/exact
5. **Skip unnecessary length ops** - Length filters only when needed
6. **TID sorting** - Sequential I/O for large result sets
7. **Batch TID insertion** - Efficient bitmap scan support
8. **Direct bitmap iteration** - No intermediate arrays
9. **Parallel bitmap scan** - Multi-worker support
10. **Batch cleanup** - Tombstone removal at thresholds
11. **Skip sorting for aggregates** - COUNT(*) doesn't need order
12. **LIMIT-aware collection** - Early termination for small limits

---

## PostgreSQL Configuration

### Memory Settings

```ini
# postgresql.conf

# Shared memory for all connections
shared_buffers = 4GB           # 25% of RAM (for caching)

# Per-query work memory
work_mem = 256MB               # For sorting/hash ops

# Index build memory
maintenance_work_mem = 1GB     # For CREATE INDEX

# Effective cache size (helps planner)
effective_cache_size = 12GB    # 75% of RAM
```

**Reload config**:
```sql
SELECT pg_reload_conf();
```

---

### Planner Settings

```sql
-- Ensure Biscuit index usage
SET enable_seqscan = off;  -- Force index use (testing only!)

-- Cost settings (fine-tuning)
SET random_page_cost = 1.1;  -- For SSDs
SET cpu_tuple_cost = 0.01;

-- Parallel query settings
SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 100;
```

---

## Index Design Optimization

### Choose Selective Columns

```sql
-- Analyze column selectivity
SELECT 
    COUNT(DISTINCT name) * 1.0 / COUNT(*) as name_selectivity,
    COUNT(DISTINCT category) * 1.0 / COUNT(*) as category_selectivity,
    COUNT(DISTINCT sku) * 1.0 / COUNT(*) as sku_selectivity
FROM products;

-- High selectivity (>0.5) = excellent for Biscuit
-- Low selectivity (<0.1) = consider other approaches
```

**Rule**: Index high-cardinality columns (many unique values)

---

### Multi-Column Order Strategy

```sql
-- ✅ GOOD: High to low selectivity
CREATE INDEX idx_products ON products 
USING biscuit (
    sku,        -- Unique (selectivity ~1.0)
    name,       -- High variety (selectivity ~0.8)
    category    -- Few values (selectivity ~0.1)
);

-- Biscuit will reorder predicates automatically,
-- but this order makes the index structure clearer
```

---

### Avoid Over-Indexing

```sql
-- ❌ BAD: Redundant indexes
CREATE INDEX idx1 ON products USING biscuit (name);
CREATE INDEX idx2 ON products USING biscuit (name, sku);
-- idx1 is completely redundant!

-- ✅ GOOD: Single comprehensive index
CREATE INDEX idx_products ON products 
USING biscuit (name, sku, category);
```

**Benefit**: Reduces maintenance overhead, saves memory

---

## Query Optimization

### Pattern Design

#### Maximize Concrete Characters

```sql
-- ❌ SLOW: Too generic (matches 90% of data)
WHERE name LIKE '%e%'

-- ✅ FAST: More specific (matches 5% of data)
WHERE name LIKE '%wireless mouse%'

-- ✅✅ FASTEST: Very specific (matches <1%)
WHERE name LIKE '%Dell Wireless USB Mouse Pro%'
```

**Benchmark**:
```sql
-- Test different pattern selectivity
EXPLAIN ANALYZE SELECT COUNT(*) FROM products WHERE name LIKE '%e%';
EXPLAIN ANALYZE SELECT COUNT(*) FROM products WHERE name LIKE '%wireless%';
EXPLAIN ANALYZE SELECT COUNT(*) FROM products WHERE name LIKE '%wireless mouse%';
```

---

#### Use Strong Anchors

```sql
-- ❌ WEAKEST: No anchor
WHERE email LIKE '%@%'

-- ✅ BETTER: Suffix anchor  
WHERE email LIKE '%@company.com'

-- ✅✅ BEST: Both anchors
WHERE email LIKE 'admin%@company.com'
```

---

#### Minimize Pattern Complexity

```sql
-- ❌ COMPLEX: 4 partitions
WHERE message LIKE '%ERROR%timeout%connection%retry%'

-- ✅ SIMPLER: Combine into fewer parts
WHERE message LIKE '%ERROR timeout connection%'
```

---

### Combine with Other Filters

```sql
-- Use multiple index types together
SELECT * FROM orders 
WHERE created_at > '2024-01-01'      -- B-tree index
  AND status = 'pending'              -- B-tree index
  AND customer_name LIKE '%Smith%';   -- Biscuit index

-- PostgreSQL combines all three efficiently
```

---

### Aggregate Query Optimization

Biscuit has special optimizations for aggregates:

```sql
-- ✅ OPTIMIZED: No tuple fetching, no sorting
SELECT COUNT(*) FROM products WHERE name LIKE '%laptop%';

-- ✅ OPTIMIZED: Bitmap-only operation
SELECT EXISTS(SELECT 1 FROM products WHERE name LIKE '%laptop%');

-- ✅ OPTIMIZED: Grouped aggregates
SELECT category, COUNT(*) 
FROM products 
WHERE name LIKE '%wireless%'
GROUP BY category;
```

**Performance**: 2-5x faster than regular queries!

---

### LIMIT Query Optimization

```sql
-- ✅ OPTIMIZED: Early termination after ~20 results
SELECT * FROM products 
WHERE name LIKE '%laptop%'
LIMIT 10;

-- Biscuit stops collecting TIDs once LIMIT is satisfied
```

**Benchmark**:
```sql
-- Compare with and without LIMIT
EXPLAIN ANALYZE 
SELECT * FROM products WHERE name LIKE '%gaming%';
-- Execution Time: 45ms

EXPLAIN ANALYZE 
SELECT * FROM products WHERE name LIKE '%gaming%' LIMIT 10;
-- Execution Time: 8ms (5x faster!)
```

---

## Maintenance and Monitoring

### Regular Statistics Updates

```sql
-- Update table statistics
ANALYZE products;

-- Update specific columns
ANALYZE products (name, category, sku);

-- Check last analyze time
SELECT schemaname, tablename, last_analyze, last_autoanalyze
FROM pg_stat_user_tables 
WHERE tablename = 'products';
```

**Frequency**: After bulk inserts/updates (>10% of rows)

---

### Monitor Index Health

```sql
-- Check index statistics
SELECT biscuit_index_stats('idx_products_name'::regclass);
```

Key metrics to watch:

```
Active records: 100000     ← Should match table row count
Tombstones: 245           ← Should be < 1000
Free slots: 15            ← Reused space
Max length: 256           ← Pattern matching limit

CRUD Statistics:
  Inserts: 105000         ← Total insertions
  Updates: 2400           ← Update operations
  Deletes: 5000           ← Deletion operations
```

---

### Cleanup Tombstones

Tombstones accumulate from DELETEs:

```sql
-- Check tombstone count
SELECT biscuit_index_stats('idx_products_name'::regclass);

-- Trigger cleanup (automatic at 1000 tombstones)
-- Or force cleanup via:
VACUUM FULL products;
REINDEX INDEX idx_products_name;
```

---

### Index Rebuild Strategy

When to rebuild:

- ✅ After massive data changes (>50% rows)
- ✅ Tombstone count > 5000
- ✅ Performance degradation detected
- ✅ Schema changes to indexed columns

```sql
-- Concurrent rebuild (minimal downtime)
CREATE INDEX CONCURRENTLY idx_products_new ON products 
USING biscuit (name, sku, category);

DROP INDEX idx_products;
ALTER INDEX idx_products_new RENAME TO idx_products;
```

---

## Hardware Considerations

### Memory Requirements

**Formula**: `Index Size ≈ (num_records × avg_string_length × 0.1)`

Example:
- 1M records × 50 bytes avg × 0.1 = ~5MB per column

**Rule of Thumb**:
- Small dataset (<100K rows): Any hardware works
- Medium dataset (100K-1M rows): 4GB+ RAM recommended
- Large dataset (>1M rows): 16GB+ RAM recommended

---

### CPU Considerations

Biscuit is CPU-bound for:
- Pattern matching (bitmap intersections)
- Multi-column predicate ordering
- Sorting large TID arrays

**Recommendations**:
- 4+ cores for production
- High clock speed > many cores
- Modern CPU with good cache (L3 cache helps)

---

## Benchmarking Your Setup

### Create Benchmark Suite

```sql
-- Create test table
CREATE TABLE bench_products AS
SELECT 
    i as id,
    'Product ' || i as name,
    'Category ' || (i % 10) as category,
    'SKU-' || LPAD(i::TEXT, 8, '0') as sku
FROM generate_series(1, 100000) i;

-- Create index
CREATE INDEX idx_bench ON bench_products 
USING biscuit (name, category, sku);

-- Run benchmark
\timing on

-- Test 1: Prefix query
SELECT COUNT(*) FROM bench_products WHERE name LIKE 'Product 1%';

-- Test 2: Substring query
SELECT COUNT(*) FROM bench_products WHERE name LIKE '%Product 5%';

-- Test 3: Multi-column query
SELECT COUNT(*) FROM bench_products 
WHERE name LIKE '%Product%'
  AND category LIKE 'Category 5%'
  AND sku LIKE 'SKU-00005%';

-- Test 4: Complex pattern
SELECT COUNT(*) FROM bench_products 
WHERE name LIKE '%Product%1%0%';
```

---

## Troubleshooting Performance Issues

### Issue 1: Index Not Being Used

**Symptoms**:
```sql
EXPLAIN SELECT * FROM products WHERE name LIKE '%laptop%';
-- Shows: Seq Scan on products
```

**Solutions**:

```sql
-- 1. Update statistics
ANALYZE products;

-- 2. Check if index exists
\d products

-- 3. Verify extension loaded
SELECT * FROM pg_extension WHERE extname = 'biscuit';

-- 4. Force index usage (testing only)
SET enable_seqscan = off;
```

---

### Issue 2: Slow Query Despite Index

**Symptoms**: Query uses Biscuit but takes 100ms+

**Debug steps**:

```sql
-- 1. Check pattern selectivity
SELECT COUNT(*) FROM products WHERE name LIKE '%e%';
-- If this returns >50% of rows, pattern is too generic

-- 2. Verify index health
SELECT biscuit_index_stats('idx_products_name'::regclass);
-- Look for high tombstone count

-- 3. Check for pattern complexity
EXPLAIN (ANALYZE, VERBOSE) 
SELECT * FROM products WHERE name LIKE '%a%b%c%d%e%';
-- Many partitions = slower
```

**Solutions**:
- Use more specific patterns
- Rebuild index if tombstone count > 1000
- Combine with additional filters

---

### Issue 3: High Memory Usage

**Symptoms**: PostgreSQL using excessive RAM

**Solutions**:

```sql
-- 1. Reduce work_mem for this connection
SET work_mem = '64MB';

-- 2. Check index size
SELECT pg_size_pretty(pg_relation_size('idx_products_name'));

-- 3. Consider partial indexes for large tables
CREATE INDEX idx_products_active ON products 
USING biscuit (name)
WHERE status = 'active';  -- Only index active products
```

---

### Issue 4: Slow Index Builds

**Symptoms**: `CREATE INDEX` takes minutes

**Solutions**:

```sql
-- 1. Increase maintenance_work_mem
SET maintenance_work_mem = '2GB';

-- 2. Use CONCURRENTLY for production
CREATE INDEX CONCURRENTLY idx_products_name ON products 
USING biscuit (name);

-- 3. Build during low-traffic periods
-- 4. Consider building index in steps for huge tables (>10M rows)
```

---

## Advanced Tuning

### Custom Cost Parameters

```sql
-- Fine-tune Biscuit's cost estimation
ALTER INDEX idx_products_name SET (fillfactor = 90);

-- Adjust planner costs
SET random_page_cost = 1.0;   -- SSD-optimized
SET seq_page_cost = 1.0;
SET cpu_index_tuple_cost = 0.001;
```

---

### Parallel Query Configuration

```sql
-- Enable parallel bitmap scans
SET max_parallel_workers_per_gather = 4;
SET parallel_tuple_cost = 0.01;
SET parallel_setup_cost = 100;

-- Test parallel execution
EXPLAIN (ANALYZE, VERBOSE) 
SELECT COUNT(*) FROM products WHERE name LIKE '%laptop%';
-- Look for "Parallel Bitmap Heap Scan"
```

---

## Performance Monitoring Dashboard

Create a monitoring view:

```sql
CREATE OR REPLACE VIEW biscuit_performance AS
SELECT 
    i.indexrelid::regclass AS index_name,
    pg_size_pretty(pg_relation_size(i.indexrelid)) AS index_size,
    s.idx_scan AS scans,
    s.idx_tup_read AS tuples_read,
    s.idx_tup_fetch AS tuples_fetched,
    ROUND(s.idx_tup_fetch::numeric / NULLIF(s.idx_scan, 0), 2) AS avg_tuples_per_scan
FROM pg_index i
JOIN pg_stat_user_indexes s ON i.indexrelid = s.indexrelid
WHERE i.indexrelid::regclass::text LIKE '%biscuit%';

-- Monitor over time
SELECT * FROM biscuit_performance;
```

---

## Next Steps

- Learn about [Architecture](architecture.md) internals
-  Review [Pattern Syntax](patterns.md) for optimization
-  Explore [Multi-Column Indexes](multicolumn.md)
-  Check the [FAQ](faq.md)