# Multi-Column Indexes

A single Biscuit index can cover several text columns. Each column has its own
independent set of positional and length structures, and predicates on any
subset of the columns can be served by the index.

---

## Creating a Multi-Column Index

```sql
CREATE INDEX idx_products_search
ON products USING biscuit (name, description, category, sku);
```

Each column can use its own operator class:

```sql
CREATE INDEX idx_users_search ON users USING biscuit (
    username biscuit_like_ops,     -- LIKE, NOT LIKE, ~, !~
    email    biscuit_ilike_ops,    -- ILIKE, NOT ILIKE, ~*, !~*
    city                           -- default biscuit_ops: all operators
);
```

The operator class determines which structure sets are built for that column
and which operators the planner will route to it.

Columns can be `text` or `varchar`. Other types, including `char(n)`, are
indexed through an expression that casts to `text` (see
[API Reference](api.md#supported-column-types)).

---

## Querying

Any combination of the indexed columns can be queried; unlike a B-tree, a
predicate on a later column does not require a predicate on the first:

```sql
-- all four columns
SELECT * FROM products
WHERE name        LIKE 'Laptop%'
  AND description LIKE '%gaming%'
  AND category    LIKE 'Elect%'
  AND sku         LIKE 'SKU-2024%';

-- a single, non-leading column
SELECT * FROM products WHERE category ILIKE 'office%';

-- mixed operators, including regular expressions
SELECT * FROM products
WHERE sku ~ '^SKU-.{4}-B$'
  AND name NOT LIKE '%refurbished%';
```

---

## Predicate Ordering and Candidate Masks

When a query has several index predicates, Biscuit:

1. classifies each predicate by pattern shape (exact, prefix, suffix, infix,
   number of parts, `_` count, concrete characters) and assigns an estimated
   selectivity;
2. evaluates the predicate estimated to be most selective first; and
3. passes the surviving rows to each subsequent predicate as a *candidate
   mask*, so later predicates only examine rows that are still possible
   matches.

The order in which predicates are written in the query does not matter, and
the order of columns in the index definition does not affect evaluation
order.

The practical effect is that combining an anchored predicate with an
unanchored one costs roughly what the anchored predicate costs alone. The
planner's cost model reflects this: the cheapest predicate is priced in full,
and each later predicate is scaled by the selectivity of those before it.

---

## Multi-Column Index or Separate Indexes

Both arrangements work.

* **One multi-column index** evaluates all predicates inside a single index
  scan, with candidate masks between them. It holds one row store and one
  cached copy per backend.
* **Separate single-column indexes** are combined by PostgreSQL with a
  `BitmapAnd` or `BitmapOr`. Each index can be created, dropped and rebuilt
  independently.

Queries that routinely filter on several columns at once are a natural fit for
a multi-column index. If columns are usually queried alone, separate indexes
are simpler to manage. Measure both with your queries if the difference
matters.

---

## Sizing Considerations

Index size, build time, write amplification and per-backend memory all grow
with the number of indexed columns and with their string lengths, and roughly
double for a column that uses `biscuit_ops` (both case modes) rather than a
single-mode class. Index only the columns that are queried with patterns, and
use `biscuit_like_ops` or `biscuit_ilike_ops` where a column needs only one
case mode.

Low-cardinality columns (for example, a status column with a handful of
values) are usually better served by a B-tree or by including the condition
in a partial-index predicate than by adding them to a Biscuit index.

The planner estimates unanchored-pattern cost from the average string width
of the index's **first** column. If the columns differ greatly in length,
plans for unanchored patterns on other columns may be less accurate; verify
them with `EXPLAIN`.

---

## Checking Plans

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM products
WHERE name LIKE 'Laptop%'
  AND category LIKE 'Elect%';
```

Expect a Bitmap Index Scan or Index Scan on the Biscuit index with all
indexable predicates listed in `Index Cond`. A sequential scan indicates the
planner judged the index more expensive, which is often correct for broad or
unanchored patterns.

For per-key diagnostics, `biscuit.diag_scan_trace` reports each key's
candidate count to the server log and as warnings. It is intended for
troubleshooting only:

```sql
SET biscuit.diag_scan_trace = on;
SELECT count(*) FROM products WHERE name LIKE 'Laptop%' AND category LIKE 'Elect%';
RESET biscuit.diag_scan_trace;
```

---

## Next Steps

* [Performance and Operations](performance.md)
* [Architecture](architecture.md)
