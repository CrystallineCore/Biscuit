

# Quick Start Tutorial

Get started with Biscuit in 5 minutes! This tutorial demonstrates creating your first Biscuit index and running optimized pattern matching queries.

---

## Step 1: Create Sample Data

Let's create a realistic dataset - an e-commerce product catalog:

```sql
-- Create products table
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    sku TEXT NOT NULL,
    category TEXT,
    description TEXT
);

-- Insert sample data
INSERT INTO products (name, sku, category, description)
VALUES
    ('Wireless Mouse Pro', 'MOUSE-001', 'Electronics', 'Ergonomic wireless mouse'),
    ('Gaming Keyboard RGB', 'KEY-GAME-002', 'Electronics', 'Mechanical gaming keyboard'),
    ('USB-C Cable 2m', 'CABLE-USB-003', 'Accessories', 'Fast charging cable'),
    ('Laptop Stand Aluminum', 'STAND-LAP-004', 'Accessories', 'Adjustable laptop stand'),
    ('Webcam HD 1080p', 'CAM-HD-005', 'Electronics', 'High definition webcam'),
    ('Mousepad Extended', 'PAD-MOUSE-006', 'Accessories', 'Large gaming mousepad'),
    ('Wireless Headphones', 'HEAD-WIRE-007', 'Electronics', 'Noise-canceling headphones'),
    ('Phone Charger Fast', 'CHRG-FAST-008', 'Accessories', 'Quick charge adapter'),
    ('Monitor 27 inch 4K', 'MON-27-4K-009', 'Electronics', 'Ultra HD monitor'),
    ('Desk Lamp LED', 'LAMP-LED-010', 'Accessories', 'Adjustable LED desk lamp');

-- Add more realistic data
INSERT INTO products (name, sku, category, description)
SELECT 
    'Product ' || i,
    'SKU-' || LPAD(i::TEXT, 6, '0'),
    CASE (i % 3)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Accessories'
        ELSE 'Office Supplies'
    END,
    'Description for product ' || i
FROM generate_series(11, 10000) i;
```

---

## Step 2: Measure Baseline Performance

First, let's see how PostgreSQL handles pattern matching **without** Biscuit:

```sql
-- Enable timing
\timing on

-- Test various LIKE patterns
SELECT COUNT(*) FROM products WHERE name LIKE '%Mouse%';
SELECT COUNT(*) FROM products WHERE name LIKE 'Wireless%';
SELECT COUNT(*) FROM products WHERE sku LIKE '%USB%';
SELECT COUNT(*) FROM products WHERE description LIKE '%gaming%';
```


Check the query plan:
```sql
EXPLAIN ANALYZE
SELECT * FROM products WHERE name LIKE '%Mouse%';
```

Output shows **Sequential Scan**:
```
Seq Scan on products  (cost=0.00..180.00 rows=10 width=...)
  Filter: (name ~~ '%Mouse%'::text)
  Rows Removed by Filter: 9990
Planning Time: 0.123 ms
Execution Time: 45.678 ms
```

---

## Step 3: Create Biscuit Index

Now create a Biscuit index on the `name` column:

```sql
-- Create single-column Biscuit index
CREATE INDEX idx_products_name ON products USING biscuit (name);
```


Check index size:
```sql
SELECT 
    pg_size_pretty(pg_relation_size('idx_products_name')) AS index_size;
```

---

## Step 4: Run Optimized Queries

Re-run the same queries with the index:

```sql
-- Same queries as before
SELECT COUNT(*) FROM products WHERE name LIKE '%Mouse%';
SELECT COUNT(*) FROM products WHERE name LIKE 'Wireless%';
SELECT COUNT(*) FROM products WHERE name LIKE '%Laptop%Stand%';
```


Check the new query plan:
```sql
EXPLAIN ANALYZE
SELECT * FROM products WHERE name LIKE '%Mouse%';
```

Output shows **Index Scan using Biscuit**:
```
Index Scan using idx_products_name on products
  (cost=0.00..8.27 rows=2 width=...)
  Index Cond: (name ~~ '%Mouse%'::text)
Planning Time: 0.089 ms
Execution Time: 1.234 ms 
```

---

## Step 5: Try Different Pattern Types

Biscuit optimizes different LIKE patterns differently:

### Prefix Patterns (Fastest)
```sql
-- Matches strings starting with "Wireless"
SELECT * FROM products WHERE name LIKE 'Wireless%';
```

### Suffix Patterns
```sql
-- Matches strings ending with "Cable"
SELECT * FROM products WHERE name LIKE '%Cable';
```

### Substring Patterns
```sql
-- Matches strings containing "gaming"
SELECT * FROM products WHERE name LIKE '%gaming%';
```

### Complex Patterns
```sql
-- Multiple wildcards with concrete characters
SELECT * FROM products WHERE name LIKE '%USB%C%';
SELECT * FROM products WHERE name LIKE 'Wireless%Mouse%';
```

### Underscore Wildcards
```sql
-- _ matches exactly one character
SELECT * FROM products WHERE sku LIKE 'MOUSE-00_';
SELECT * FROM products WHERE name LIKE 'W_reless%';
```

---

## Step 6: Multi-Column Indexes

Create an index on multiple columns for even more powerful queries:

```sql
-- Create multi-column Biscuit index
CREATE INDEX idx_products_multi ON products 
USING biscuit (name, sku, category);
```

Query across all indexed columns:
```sql
-- Biscuit automatically optimizes predicate order
SELECT * FROM products 
WHERE name LIKE '%Mouse%'
  AND sku LIKE 'MOUSE%'
  AND category LIKE 'Elect%';
```

**Automatic optimization**: Biscuit reorders predicates by selectivity for best performance!

---

## Step 7: Aggregate Queries (Special Optimization)

Biscuit has special optimizations for `COUNT(*)` and `EXISTS`:

```sql
-- COUNT optimization (no tuple fetching, no sorting)
SELECT COUNT(*) FROM products WHERE name LIKE '%Wireless%';

-- EXISTS optimization
SELECT EXISTS(
    SELECT 1 FROM products WHERE name LIKE '%Gaming%'
);

-- Aggregate with GROUP BY
SELECT category, COUNT(*) 
FROM products 
WHERE name LIKE '%Pro%'
GROUP BY category;
```


---

## Step 8: Monitor Index Statistics

Check your index health:

```sql
-- View detailed statistics
SELECT biscuit_index_stats('idx_products_name'::regclass);
```

Output:
```
Biscuit Index Statistics (FULLY OPTIMIZED)
==========================================
Index: idx_products_name
Active records: 10000
Total slots: 10000
Free slots: 0
Tombstones: 0
Max length: 45
------------------------
CRUD Statistics:
  Inserts: 10000
  Updates: 0
  Deletes: 0
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
  ✓ 9. Parallel bitmap scan support
  ✓ 10. Batch cleanup on threshold
  ✓ 11. Skip sorting for bitmap scans
  ✓ 12. LIMIT-aware TID collection
```

---

## Step 9: Test CRUD Operations

Biscuit supports full CRUD with automatic index updates:

### INSERT
```sql
INSERT INTO products (name, sku, category, description)
VALUES ('New Wireless Mouse', 'MOUSE-999', 'Electronics', 'Latest model');

-- Index automatically updated
SELECT * FROM products WHERE name LIKE '%New Wireless%';
```

### UPDATE
```sql
UPDATE products 
SET name = 'Premium Wireless Mouse'
WHERE sku = 'MOUSE-999';

-- Index reflects changes immediately
SELECT * FROM products WHERE name LIKE '%Premium%';
```

### DELETE
```sql
DELETE FROM products WHERE sku = 'MOUSE-999';

-- Tombstone tracking for efficient cleanup
SELECT biscuit_index_stats('idx_products_name'::regclass);
```

---

## Best Practices

### ✅ DO:
- Use Biscuit for frequent LIKE queries
- Create multi-column indexes for complex filters
- Monitor index statistics regularly
- Use for high-cardinality string columns

### ❌ DON'T:
- Index very long text (>256 chars truncated)
- Use for full-text search (use tsvector)
- Create redundant indexes

---

## Next Steps

🎉 **Congratulations!** You've created your first Biscuit index!

**Continue learning:**
-  [Pattern Syntax Guide](patterns.md) - Master all pattern types
-  [Multi-Column Indexes](multicolumn.md) - Advanced techniques
-  [Performance Tuning](performance.md) - Optimize further
-  [Architecture Deep Dive](architecture.md) - How it works

**Try advanced features:**
```sql
-- Type conversion support (integers, dates, timestamps)
CREATE INDEX idx_created ON orders USING biscuit (created_at);
SELECT * FROM orders WHERE created_at::TEXT LIKE '2024-01%';

-- Complex multi-column queries
CREATE INDEX idx_users_all ON users USING biscuit (email, username, department);
SELECT * FROM users 
WHERE email LIKE '%@company.com'
  AND username LIKE 'admin%'
  AND department LIKE '%eng%';
```

---

## Common Questions

**Q: Can I use Biscuit for case-insensitive matching?**

A: Yes, Biscuit versions >= 2.1.0 support ILIKE queries.

---

Need help? Check the [FAQ](faq.md) or open an issue on [GitHub](https://github.com/crystallinecore/biscuit).