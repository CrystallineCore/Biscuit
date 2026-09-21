# Regular Expressions

Biscuit 3.1.0 accepts the POSIX regular-expression operators `~`, `!~`, `~*`
and `!~*` as index operators (strategies 5–8). Biscuit does not contain a
regular-expression engine. Instead, at plan time it tries to rewrite a regular
expression into a `LIKE` pattern that matches **exactly** the same strings,
and evaluates that pattern with the machinery described in
[Pattern Syntax](patterns.md).

Regular expressions that cannot be rewritten exactly are still answered
correctly, but not by the index.

---

## Why the Rewrite Must Be Exact

For `LIKE`, `ILIKE`, `~` and `!~`, Biscuit reports its results as exact and
PostgreSQL does not recheck them against the heap. An approximate rewrite
would therefore return wrong rows rather than merely extra ones. Any regular
expression the rewriter cannot prove equivalent is refused.

---

## Supported Subset

| Construct | Rewritten as |
|---|---|
| `^` at the start, `$` at the end | Anchors the pattern; a missing anchor becomes `%` |
| Literal characters (including multibyte characters) | The same characters, with `%`, `_` and `\` escaped |
| `\X` where `X` is not alphanumeric (`\.`, `\*`, `\$`, `\\`, …) | Literal `X` |
| `.` | `_` |
| `.*` | `%` |
| `.+` | `_%` |
| `.{n}` | `n` × `_` |
| `.{n,}` | `n` × `_` followed by `%` |
| `X{n}` for a literal `X` | `n` copies of `X` |
| A trailing `?` after one of the quantifiers above (non-greedy marker) | Ignored; it does not change which strings match |

Repetition counts are limited to 255, matching PostgreSQL's own limit.

Because `~` matches anywhere in the string while `LIKE` matches the whole
string, an unanchored regular expression becomes an unanchored pattern:

```text
Regular expression     LIKE pattern     Shape
^abc$                  abc              exact match
^abc                   abc%             prefix
abc$                   %abc             suffix
abc                    %abc%            unanchored infix
^a.c$                  a_c              positional wildcard
^a.{3}z$               a___z            repetition count
^a.*z$                 a%z              anchored at both ends
^.{8}$                 ________         length predicate
^usr_1                 usr\_1%          '_' is a literal in a regex
```

---

## Constructs That Are Not Rewritten

| Construct | Example | Reason |
|---|---|---|
| Alternation and groups | `(a\|b)`, `a\|b`, `(ab)+` | A set of alternatives is not a single pattern |
| Bracket expressions and POSIX classes | `[abc]`, `[0-9]`, `[[:digit:]]` | `_` is too broad and a literal too narrow |
| `*`, `+` or `?` on a literal | `a*`, `a+`, `a?` | No equivalent pattern (`a%` would require the `a`) |
| `.?` and bounded ranges | `x{2,4}` | An upper bound needs alternation |
| Class shorthands and escapes | `\d`, `\w`, `\s`, `\y` | Any backslash followed by a letter or digit |
| Backreferences | `(a)\1` | |
| `^` or `$` other than at the ends | `a^b`, `^a$b` | Mid-pattern anchors have no pattern form |
| Embedded options and directors | `(?i)abc`, `***=abc` | May change matching rules |

When a query uses one of these:

* the predicate contributes nothing to the index's candidate set;
* the scan asks the executor to recheck, so the original regular expression is
  evaluated against each row; and
* the cost model marks the index path as unusable, so the planner chooses a
  sequential scan (or another index).

The result is correct; only the index acceleration is lost. `EXPLAIN` shows a
`Seq Scan` for such a query, even with `enable_seqscan = off`, because the
index path is priced as unusable rather than merely expensive.

```sql
EXPLAIN SELECT * FROM users WHERE name ~ '^(john|jane)';
-- Seq Scan on users
--   Filter: (name ~ '^(john|jane)'::text)
```

On PostgreSQL 18, the disabled path is also counted in the planner's
`disabled_nodes`, which is how PostgreSQL 18 compares disabled paths. Earlier
versions rely on the cost alone.

---

## Case-Insensitive Operators: `~*` and `!~*`

`~*` is rewritten onto the `ILIKE` structures. PostgreSQL's regular-expression
case folding and `ILIKE`'s `lower()`-based folding are not identical; they
disagree on characters such as `İ`, `ß` and the titlecase letters `ǅ`, `ǈ`,
`ǋ`. Biscuit handles this as follows:

* **`~*` is rewritten only when the resulting pattern is pure ASCII.** Under
  that restriction the remaining disagreement can only make `ILIKE` match
  *more* rows than `~*`, never fewer. Such scans request a recheck, and the
  executor removes the extra rows. A non-ASCII `~*` pattern is not rewritten.
* **`!~*` is never rewritten.** The complement of an over-matching set is
  missing rows, and a recheck cannot add rows back. `!~*` is always answered
  by a sequential scan (or another index), even though it is listed in the
  operator class.
* **Collation.** `~*` rewriting is refused under nondeterministic collations.
  Under an ICU collation, it is also refused for any pattern containing `.`
  (rewritten as `_`), because ICU's `lower()` can change a string's length
  (for example, `İ`, U+0130), which could shift a position-based match and
  cause a row to be missed. Plain `%literal%` shapes without `.` remain
  eligible under ICU. The default and libc collations are not affected by
  this restriction.

`~` and `!~` are unaffected by any of the above and are always exact.

---

## Operator Classes

The regular-expression operators follow the same split as `LIKE` and `ILIKE`,
because they use the same structures:

| Operator class | `~`, `!~` | `~*`, `!~*` |
|---|---|---|
| `biscuit_ops` (default) | Yes | Yes (`!~*` not accelerated) |
| `biscuit_like_ops` | Yes | Not considered |
| `biscuit_ilike_ops` | Not considered | Yes (`!~*` not accelerated) |

An index is not considered by the planner for an operator its operator class
does not include.

```sql
SELECT * FROM biscuit_operators;   -- lists operators and strategy numbers per operator family
```

---

## Cost and Plan Selection

A regular expression is priced as the pattern it rewrites to, using the same
rewrite the executor performs, so the planner and executor agree on its
shape. Consequences:

* Anchored regular expressions (`^abc`, `abc$`, `^a.*z$`) are priced as
  anchored patterns and are typically chosen.
* Unanchored regular expressions (`~ 'abc'`, which becomes `LIKE '%abc%'`)
  are priced as unanchored infix patterns and are often declined in favour of
  a sequential scan or `pg_trgm`.

`EXPLAIN` reliably shows whether the index was used. It does not show whether
a recheck was requested: a Bitmap Heap Scan prints `Recheck Cond:` in every
plan, whatever the index reported.

---

## When to Use `pg_trgm` Instead

Where regular expressions are central to a workload, `pg_trgm` is usually the
better fit. It extracts trigrams from arbitrary regular expressions, including
alternation and character classes, and handles unanchored patterns well. The
two indexes can coexist on the same column; the planner selects between them
per query.

---

## Upgrading

Indexes created under 3.0.0 gain regular-expression support as soon as the
extension is updated with `ALTER EXTENSION biscuit UPDATE TO '3.1.0'`. No
`REINDEX` is required, because the rewrite happens at plan time and uses the
existing index structures. See [Upgrading](installation.md#upgrading).
