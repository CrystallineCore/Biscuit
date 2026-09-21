# Relationship to pg_trgm and B-tree

PostgreSQL already provides two well-established ways to index pattern
matching: trigram indexes from the `pg_trgm` extension, and B-tree indexes
with `text_pattern_ops`. Biscuit is designed to complement them, not to
replace them. This page describes where each fits.

---

## pg_trgm

`pg_trgm` splits text into three-character sequences (trigrams) and indexes
them, usually with GIN.

### Where pg_trgm is the better choice

* **Similarity and fuzzy matching.** `pg_trgm` supports the `%` similarity
  operator, `similarity()`, and distance-ordered searches. Biscuit has no
  similarity or edit-distance support.

  ```sql
  SELECT * FROM products WHERE name % 'iPone';
  ```

* **General regular expressions.** `pg_trgm` extracts trigrams from arbitrary
  regular expressions, including alternation and character classes. Biscuit
  serves only regular expressions that can be rewritten exactly as a `LIKE`
  pattern (see [Regular Expressions](regex.md)).

* **Unanchored substring search.** For `%abc%`, `pg_trgm` narrows the search
  using the trigrams in the literal, and its cost falls as the pattern becomes
  more selective. Biscuit's unanchored cost depends on row count and string
  length and changes little with selectivity.

* **Write-heavy tables and many connections.** `pg_trgm` GIN indexes use
  shared buffers and do not keep a per-connection copy of the index.

* **Index size and build time.** `pg_trgm` indexes are typically smaller and
  faster to build than Biscuit indexes on the same data.

### Where Biscuit may be preferable

* **Patterns without three consecutive literal characters**, such as
  `'%ab%'`, `'a_c%'` or `'_____'`. `pg_trgm` cannot extract a usable trigram
  from these and must scan most of its index; Biscuit evaluates them with
  positional and length bitmaps.
* **Anchored patterns**, including suffixes and patterns anchored at both
  ends, which Biscuit resolves with a fixed number of bitmap intersections.
* **Exact results.** `pg_trgm` results are always rechecked against the heap;
  Biscuit's `LIKE`, `ILIKE`, `~` and `!~` results are not.

Word-based full-text search (stemming, stop words, ranking) is provided by
PostgreSQL's `tsvector` and `tsquery` types, not by `pg_trgm` or Biscuit.

---

## B-tree

A B-tree with `text_pattern_ops` (or a `C` collation) can serve `LIKE`
patterns with a literal prefix.

### Where B-tree is the better choice

* **Equality, range queries and sorted access.** B-tree supports ordered
  scans, `ORDER BY` without a sort, index-only scans and uniqueness. Biscuit
  supports none of these.
* **Selective prefixes.** For prefixes that match few rows, a B-tree is
  smaller, faster to build and often faster to query.
* **Non-text types.** B-tree indexes any ordered type; Biscuit indexes text.
* **Write-heavy tables.** B-tree maintenance is inexpensive and does not
  invalidate per-connection copies.

### Where Biscuit may be preferable

* **Suffix, both-anchored and positional patterns.** A B-tree cannot serve
  `%abc` without a `reverse()` expression index, and cannot serve `a_c`,
  `a%z` or length predicates at all.
* **Case-insensitive patterns** without a separate `lower()` expression
  index.
* **Broad prefixes** that match many rows, where Biscuit's bitmap evaluation
  was often faster in testing than a B-tree range scan.

---

## Using Them Together

The indexes can coexist on the same table and even the same column. The
planner chooses among them per query based on cost:

```sql
CREATE INDEX users_email_btree ON users (email);                             -- equality, ranges
CREATE INDEX users_name_trgm   ON users USING gin (name gin_trgm_ops);        -- similarity, infix, general regex
CREATE INDEX users_name_bisc   ON users USING biscuit (name);                 -- anchored and positional patterns
```

Each additional index adds write cost and storage, so create only the ones
the workload uses, and check with `EXPLAIN` which index each query actually
chooses.

---

## Summary

| Need | Usually suitable |
|---|---|
| `col = 'value'`, ranges, `ORDER BY` | B-tree |
| `col LIKE 'prefix%'`, selective | B-tree (`text_pattern_ops`) |
| `col LIKE 'prefix%'`, broad | Biscuit or B-tree; measure |
| `col LIKE '%suffix'` | Biscuit |
| `col LIKE 'a%z'`, `col LIKE 'a_c%'`, `col LIKE '_____'` | Biscuit |
| `col LIKE '%middle%'` | `pg_trgm`; Biscuit where the literal is shorter than three characters |
| `col ILIKE 'prefix%'` | Biscuit, or B-tree on `lower(col)` |
| `col ~ '^abc'`, `col ~ '^a.{3}z$'` | Biscuit |
| `col ~ '(a\|b)'`, `col ~ '[0-9]+'` | `pg_trgm` |
| `col % 'similar'` | `pg_trgm` |
| Word search with ranking | Full-text search (`tsvector`) |
| Frequent writes, many connections | B-tree or `pg_trgm` |

These are starting points drawn from testing in a single environment.
Benchmark with your own data and queries before choosing.

---

## Acknowledgments

Biscuit builds on work by others:

* the PostgreSQL community, for the extensible index access method interface;
* the `pg_trgm` and B-tree implementations, which define the established
  approaches to pattern matching in PostgreSQL; and
* the CRoaring library (Lemire et al.), for compressed bitmap operations.
