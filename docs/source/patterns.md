# Pattern Syntax

This page describes how Biscuit evaluates `LIKE`, `NOT LIKE`, `ILIKE` and
`NOT ILIKE` patterns, and how the shape of a pattern affects its cost.
Regular expressions are covered separately in
[Regular Expressions](regex.md); they are rewritten into the same pattern
forms described here.

---

## Wildcards and Escapes

Biscuit implements standard SQL `LIKE` semantics.

| Syntax | Meaning | Example | Matches |
|---|---|---|---|
| `%` | Any sequence of zero or more characters | `'%test%'` | `test`, `mytest`, `testing` |
| `_` | Exactly one character | `'test_'` | `test1`, `testA`; not `test` |
| `\%`, `\_`, `\\` | A literal `%`, `_` or `\` | `'100\%'` | `100%` |

* Matching is by **character**, not by byte. `_` matches one character
  regardless of how many bytes it occupies in UTF-8, and string lengths are
  counted in characters.
* The default escape character is backslash. A `LIKE ... ESCAPE 'c'` clause
  with a constant escape character is converted by PostgreSQL into the
  backslash form before planning and is served by the index. A non-constant
  escape expression cannot be folded this way; do not rely on the index for
  such queries.
* The pattern must be available to the planner as a text constant for the
  cost model to classify it. A pattern supplied as a parameter in a generic
  prepared-statement plan is priced conservatively, at 90% of the
  sequential-scan cost.

---

## How a Pattern Is Evaluated

A pattern is split on unescaped `%` into literal *parts*. Each part is matched
against per-position bitmaps:

* the part before the first `%` is matched at positions counted from the
  **start** of the string (0, 1, 2, …);
* the part after the last `%` is matched at positions counted from the
  **end** of the string (−1, −2, …);
* parts in between have no fixed position and must be searched for.

`_` inside a part skips one position without adding an intersection. Length
bitmaps (`length = n` and `length ≥ n`) enforce the minimum or exact length the
pattern implies.

Example, `LIKE 'abc%def'`:

```text
1. Parse:            parts ["abc", "def"], anchored at both ends
2. Prefix:           C = pos[a@0] ∩ pos[b@1] ∩ pos[c@2]
3. Suffix:           C = C ∩ neg[f@-1] ∩ neg[e@-2] ∩ neg[d@-3]
4. Length:           C = C ∩ length_ge[6]
Result:              exact matches; no heap recheck
```

---

## Pattern Shapes

The cost of a pattern depends almost entirely on whether it is anchored, not
on how many rows it matches.

### Exact match: `'abc'`

No wildcards. Evaluated as prefix positions intersected with the exact-length
bitmap. For pure equality, a B-tree index is usually smaller and at least as
fast; Biscuit is useful here when the same column also serves wildcard
queries.

### Prefix: `'abc%'`

Matched against start-relative positions. A fixed number of intersections,
independent of string length.

```sql
SELECT * FROM inventory WHERE sku LIKE 'PROD-2024%';
```

For highly selective prefixes, a B-tree with `text_pattern_ops` is often
faster and smaller; Biscuit tends to compare better on broader prefixes.

### Suffix: `'%abc'`

Matched against end-relative positions, at the same cost as a prefix. A
B-tree can only serve suffixes through a `reverse()` expression index.

```sql
SELECT * FROM users WHERE email LIKE '%@example.com';
SELECT * FROM files WHERE filename LIKE '%.pdf';
```

### Anchored at both ends: `'abc%xyz'`

Prefix and suffix intersections combined with a minimum-length constraint.

```sql
SELECT * FROM urls WHERE path LIKE '/api/%/users';
```

### Positional wildcards: `'a_c'`, `'PROD-___-2024'`

`_` fixes the position of the characters around it, and a pattern with no `%`
also fixes the length. These patterns are evaluated as anchored patterns.

```sql
SELECT * FROM products WHERE sku LIKE 'PROD-___-2024';
SELECT * FROM words    WHERE word LIKE '_ouse';
```

### Length predicates: `'_____'`, `'_____%'`

A pattern made only of `_` (optionally followed by `%`) is a length predicate
and is answered from the length bitmaps in a single lookup.

```sql
SELECT * FROM codes WHERE code LIKE '________';    -- exactly 8 characters
SELECT * FROM codes WHERE code LIKE '________%';   -- at least 8 characters
```

Very unselective length predicates (for example, one that matches most of the
table) are normally left to a sequential scan by the planner.

### Unanchored infix: `'%abc%'`

The part has no fixed position, so every candidate position must be
considered. Cost grows with row count and roughly with the square of string
length, and is largely independent of how many rows match. A more selective
literal does not make the scan much cheaper.

```sql
SELECT * FROM articles WHERE title LIKE '%postgres%';
```

The planner prices these accordingly and will often choose a sequential scan
or a `pg_trgm` GIN index instead. Check with `EXPLAIN`.

### Multiple infix parts: `'%abc%def%'`

Each additional part is a further constraint that prunes the search. Two
infix parts are typically cheaper than one; three or more are costed higher
than a single part.

A pattern that also has a prefix or suffix, such as `'ERROR%timeout%'`, is
priced on its anchor and is usually inexpensive regardless of the infix
parts.

### Match-all: `'%'`

A pattern with no literal characters and no `_` matches every non-NULL value.
The index offers no path for it, and the planner uses a sequential scan.

### Empty pattern: `''`

Matches only empty strings, answered from the length-0 bitmap.

---

## Negation: `NOT LIKE` and `NOT ILIKE`

Negated patterns are evaluated as the complement of the positive match over
the live, non-NULL rows. Following SQL semantics, `NULL NOT LIKE 'x'` is not
true, so NULL values are never returned by either form.

```sql
SELECT * FROM products WHERE name NOT LIKE '%test%';
```

A negated pattern usually matches most of the table. The planner will
normally prefer a sequential scan unless the negation is combined with a
selective positive predicate.

---

## Case-Insensitive Matching: `ILIKE`

`ILIKE` is evaluated against a separate set of case-folded structures, built
by the default `biscuit_ops` and by `biscuit_ilike_ops`. The query is not
rewritten, and no `lower()` expression index is required. For anchored
patterns `ILIKE` performs comparably to the equivalent `LIKE`.

Notes:

* Case folding uses PostgreSQL's `lower()` under the database default
  collation. The folded strings are stored in the index when rows are written,
  so all backends and standbys evaluate the same folded values.
* For unanchored `ILIKE` patterns, the case-insensitive path has additional
  per-statement overhead and can be noticeably slower than the equivalent
  `LIKE`. Where unanchored case-insensitive search is central to a workload,
  compare against `pg_trgm`.
* An index built with `biscuit_like_ops` is not considered for `ILIKE`, and
  one built with `biscuit_ilike_ops` is not considered for `LIKE`.

---

## Combining Predicates

Several pattern predicates on the same index are evaluated together: the key
estimated to be most selective runs first, and each later key only examines
rows that are still candidates.

```sql
SELECT * FROM logs
WHERE message LIKE 'ERROR%'
  AND message LIKE '%timeout%';
```

Pairing an anchored predicate with an unanchored one therefore costs close to
the anchored predicate alone, rather than the sum of both.

`OR` across Biscuit-indexable predicates is handled by PostgreSQL with a
`BitmapOr` of separate index scans. Biscuit predicates can also be combined
with predicates served by other indexes (for example, a B-tree on a date
column) through `BitmapAnd`.

---

## Summary by Shape

| Shape | Example | Relative cost | Notes |
|---|---|---|---|
| Length only | `'________'` | Lowest | Single length-bitmap lookup |
| Exact | `'abc'` | Low | Prefix positions plus exact length |
| Prefix | `'abc%'` | Low | Fixed number of intersections |
| Suffix | `'%abc'` | Low | Same cost as prefix |
| Both anchored | `'abc%xyz'` | Low | Prefix, suffix and minimum length |
| Positional `_` | `'a_c%'` | Low | Treated as anchored |
| Anchored with infix parts | `'a%b%c'` | Low | Priced on the anchor |
| Two infix parts | `'%ab%cd%'` | Moderate | Cheaper than a single infix part |
| Single infix part | `'%abc%'` | High | Grows with rows × length² |
| Three or more infix parts | `'%a%b%c%'` | Highest | Priced above a single part |
| Match-all | `'%'` | — | Not served by the index |

---

## Next Steps

* [Regular Expressions](regex.md)
* [Multi-Column Indexes](multicolumn.md)
* [Performance and Operations](performance.md)
