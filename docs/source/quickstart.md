# Quick Start

This walkthrough creates a sample table, builds a Biscuit index, and runs the
main query shapes it supports. It assumes the extension is installed (see
[Installation](installation.md)).

---

## 1. Create the Extension and Sample Data

```sql
CREATE EXTENSION IF NOT EXISTS biscuit;

CREATE TABLE products (
    id          bigserial PRIMARY KEY,
    name        text NOT NULL,
    sku         text NOT NULL,
    category    text,
    description text
);

INSERT INTO products (name, sku, category, description) VALUES
    ('Wireless Mouse Pro',    'MOUSE-001',     'Electronics', 'Ergonomic wireless mouse'),
    ('Gaming Keyboard RGB',   'KEY-GAME-002',  'Electronics', 'Mechanical gaming keyboard'),
    ('USB-C Cable 2m',        'CABLE-USB-003', 'Accessories', 'Fast charging cable'),
    ('Laptop Stand Aluminum', 'STAND-LAP-004', 'Accessories', 'Adjustable laptop stand'),
    ('Webcam HD 1080p',       'CAM-HD-005',    'Electronics', 'High definition webcam');

INSERT INTO products (name, sku, category, description)
SELECT 'Product ' || i,
       'SKU-' || lpad(i::text, 6, '0'),
       (ARRAY['Electronics', 'Accessories', 'Office Supplies'])[1 + i % 3],
       'Description for product ' || i
FROM generate_series(6, 200000) AS i;
```

Load the data **before** creating the index. Building an index over existing
rows is considerably faster, and generates far less WAL, than inserting the
same rows into an already-indexed table.

---

## 2. Record a Baseline

```sql
EXPLAIN (ANALYZE)
SELECT count(*) FROM products WHERE name LIKE 'Wireless%';
```

Without a suitable index, the plan is a sequential scan with a `Filter` on
the pattern. Note the execution time for comparison.

---

## 3. Create a Biscuit Index

```sql
CREATE INDEX idx_products_name ON products USING biscuit (name);
ANALYZE products;
```

`ANALYZE` matters: the cost model uses the column's average width from
`pg_statistic`, so plans for unanchored patterns can change after the first
`ANALYZE` on a newly loaded table.

Compare on-disk and in-memory size:

```sql
SELECT pg_size_pretty(pg_relation_size('idx_products_name')) AS on_disk,
       biscuit_size_pretty('idx_products_name')              AS in_memory;
```

`biscuit_size_pretty()` reports the size of the current session's in-memory
copy, loading the index into the session first if necessary.

---

## 4. Run Pattern Queries

```sql
EXPLAIN (ANALYZE)
SELECT count(*) FROM products WHERE name LIKE 'Wireless%';
```

The plan should now use `idx_products_name`, typically as a Bitmap Index Scan
under a Bitmap Heap Scan.

The query shapes Biscuit handles most efficiently are the anchored ones:

```sql
-- Prefix
SELECT * FROM products WHERE name LIKE 'Wireless%';

-- Suffix
SELECT * FROM products WHERE name LIKE '%Cable 2m';

-- Anchored at both ends
SELECT * FROM products WHERE name LIKE 'Wireless%Pro';

-- Single-character wildcard at a fixed position
SELECT * FROM products WHERE sku LIKE 'MOUSE-00_';   -- needs an index on sku
SELECT * FROM products WHERE name LIKE 'W_reless%';

-- Length only: names of exactly 14 characters
SELECT count(*) FROM products WHERE name LIKE '______________';
```

Unanchored patterns are supported, but their cost grows with row count and
string length, and the planner will often prefer a sequential scan or a
`pg_trgm` index:

```sql
SELECT * FROM products WHERE name LIKE '%Keyboard%';
```

Use `EXPLAIN` to see which plan was chosen. See
[Pattern Syntax](patterns.md) for how each shape is evaluated.

---

## 5. Case-Insensitive Search

The default operator class, `biscuit_ops`, builds case-sensitive and
case-insensitive structures, so the same index serves `ILIKE`:

```sql
SELECT * FROM products WHERE name ILIKE 'wireless%';
SELECT * FROM products WHERE name NOT ILIKE '%cable%';
```

No `lower()` expression index is needed.

If a column is only ever queried one way, a narrower operator class skips the
unused structure set and reduces build time and index size:

```sql
CREATE INDEX idx_sku_like ON products USING biscuit (sku biscuit_like_ops);   -- LIKE, NOT LIKE, ~, !~
CREATE INDEX idx_cat_ilike ON products USING biscuit (category biscuit_ilike_ops); -- ILIKE, NOT ILIKE, ~*, !~*
```

---

## 6. Regular Expressions

Regular expressions that can be rewritten exactly as a `LIKE` pattern use the
same index:

```sql
SELECT * FROM products WHERE name ~ '^Wireless';      -- rewritten to 'Wireless%'
SELECT * FROM products WHERE name ~ 'Pro$';           -- rewritten to '%Pro'
SELECT * FROM products WHERE name ~ '^W.reless';      -- rewritten to 'W_reless%'
SELECT * FROM products WHERE sku  ~ '^SKU-.{6}$';     -- rewritten to 'SKU-______'
```

Regular expressions outside that subset still return correct results, but the
index is not used:

```sql
EXPLAIN SELECT * FROM products WHERE name ~ '^(Wireless|Gaming)';
-- Seq Scan on products ...
```

See [Regular Expressions](regex.md) for the supported subset.

---

## 7. Multi-Column Indexes

```sql
CREATE INDEX idx_products_multi ON products
USING biscuit (name, sku, category);

SELECT * FROM products
WHERE name LIKE 'Product 1%'
  AND sku LIKE 'SKU-00%'
  AND category LIKE 'Elec%';
```

The index evaluates the predicates in order of estimated selectivity, and each
predicate only examines the rows that survived the ones before it. See
[Multi-Column Indexes](multicolumn.md).

---

## 8. Inspect the Index

```sql
SELECT biscuit_index_stats('idx_products_name'::regclass);
```

The report has this form (values depend on your data):

```text
Biscuit Index Statistics
==========================================
Index: idx_products_name
Active records: 200000
Total slots: 200000
Free slots: 0
Tombstones: 0
Max length: 21
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
  ...
```

The fields are described in the [API Reference](api.md#biscuit_index_stats).

Other views:

```sql
SELECT * FROM biscuit_indexes;   -- all Biscuit indexes in the database
SELECT * FROM biscuit_status;    -- version, CRoaring status, totals
```

---

## 9. Writes After the Index Exists

`INSERT`, `UPDATE` and `DELETE` maintain the index automatically and are
visible to subsequent queries in all sessions:

```sql
INSERT INTO products (name, sku, category)
VALUES ('New Wireless Mouse', 'MOUSE-999', 'Electronics');

SELECT * FROM products WHERE name LIKE 'New Wireless%';

UPDATE products SET name = 'Premium Wireless Mouse' WHERE sku = 'MOUSE-999';
DELETE FROM products WHERE sku = 'MOUSE-999';
```

Each write is recorded in a shared pending log and merged into the compacted
index structures later, either when the log reaches
`biscuit.delta_compaction_slots` rows or during `VACUUM`. As with other
PostgreSQL indexes, entries for deleted or superseded rows are removed when
`VACUUM` processes the table; until then MVCC visibility checks hide them. The
`Deletes` and `Tombstones` counters in `biscuit_index_stats()` therefore
change after `VACUUM`, not at the time of the `DELETE`. Writes to a live
index generate considerably more WAL than the heap writes alone; for large
loads, drop and recreate the index around the load. See
[Performance and Operations](performance.md).

---

## Guidelines

* Build the index after loading data, then run `ANALYZE`.
* Prefer anchored patterns; check unanchored ones with `EXPLAIN`.
* Use `biscuit_like_ops` or `biscuit_ilike_ops` when only one case mode is
  queried.
* Keep Biscuit on read-mostly tables. Each connection holds its own copy of
  the index in memory.
* Use `tsvector` full-text search for word-based search with stemming and
  ranking, and B-tree indexes for equality and range queries.

---

## Next Steps

* [Pattern Syntax](patterns.md)
* [Regular Expressions](regex.md)
* [Multi-Column Indexes](multicolumn.md)
* [Performance and Operations](performance.md)
