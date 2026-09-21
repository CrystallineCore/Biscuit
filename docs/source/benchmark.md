# Benchmark: v2.1.3, Fallback Bitmaps

```{important}
**These results are historical.** They were measured in December 2025 with
Biscuit **2.1.3**, built **without** CRoaring, and have not been repeated for
3.x. Since then, Biscuit has moved to WAL-logged on-disk storage with a
pending log (3.0.0), received a rewritten cost model (3.0.0), and had several
correctness fixes (3.1.0). Absolute timings, index sizes and plan choices for
3.1.0 will differ.

Read the numbers with the method in mind: sequential and bitmap scans were
disabled to force index use, so the B-tree figures mostly reflect patterns a
B-tree cannot serve efficiently, and none of the figures reflect the plans
PostgreSQL would choose on its own. More recent testing on 3.x found Biscuit
and `pg_trgm` to be largely complementary: Biscuit well ahead on anchored and
positional patterns, `pg_trgm` well ahead on unanchored and character-class
patterns. See [Choosing an Index](index.md#choosing-an-index) for current
guidance, and benchmark with your own data before choosing.
```

The same benchmark with CRoaring enabled is in
[Benchmark: v2.1.3, CRoaring](benchmark_roaring.md). The machine is described
in [Benchmark Environment](benchmark_env.md).

---

## Setup

### Dataset

* **Source:** [Synthetic Online Community Data 2025](https://www.kaggle.com/datasets/emirhanakku/synthetic-online-community-data-2025)
* **Table:** `interactions`, 1,000,000 rows
* **Indexed columns:** `interaction_type`, `username`, `country`, `device`

| Column | Distinct values | Examples |
|---|---|---|
| `interaction_type` | 8 | `post`, `comment`, `like`, `share` |
| `username` | 140,914 | `john_smith`, `alice_jones`, `bob123` |
| `country` | 243 | `United States`, `Japan`, `Kazakhstan` |
| `device` | 3 | `iOS`, `Android`, `Web` |

### Indexes compared

```sql
CREATE INDEX int_bisc ON interactions USING biscuit (
    interaction_type, username, country, device);

CREATE INDEX int_trgm ON interactions USING gin (
    interaction_type gin_trgm_ops, username gin_trgm_ops,
    country gin_trgm_ops, device gin_trgm_ops);

CREATE INDEX int_tree ON interactions (
    interaction_type text_pattern_ops, username text_pattern_ops,
    country text_pattern_ops, device text_pattern_ops);
```

| Object | Size |
|---|---|
| Table | 129 MB |
| B-tree index | 43 MB |
| `pg_trgm` GIN index | 86 MB |
| Biscuit index (fallback bitmaps) | 914.59 MB |

In 2.1.3, Biscuit held its structures in backend memory rather than in index
pages, and the Biscuit figure is the in-memory size in the format reported by
`biscuit_size_pretty()`. It is not directly comparable with the on-disk size
of a 3.x index.

### Software and settings

* PostgreSQL 16.11 (Ubuntu 16.11-0ubuntu0.24.04.1)
* AMD Ryzen 7 5700U, 14 GiB usable RAM, NVMe SSD, Ubuntu 24.04

```sql
work_mem          = '256MB'
random_page_cost  = 1.1
enable_seqscan    = off   -- force index use
enable_bitmapscan = off   -- force plain index scans
```

### Method

* **Isolation:** each index type was tested alone, after a server restart and
  an OS cache drop, with only that index present.
* **Cache states:** *cold* (immediately after restart and index creation) and
  *warm* (after 25 warm-up queries across all columns).
* **Iterations:** 10 per cache state, giving 190 queries × 20 runs = 3,800
  measurements per index, 11,400 in total.
* **Order:** index types were run in randomised order.
* **Statistics:** means with 95% confidence intervals (Student's *t*), Welch's
  *t*-test between index types, and Cohen's *d*.
* **Captured per query:** execution and planning time, shared-buffer hits,
  reads and writes, row count, and top plan node.

### Query suite

| Category | Queries | Examples |
|---|---|---|
| Prefix, suffix and infix | 24 | `country LIKE 'Uni%'`, `LIKE '%stan'`, `LIKE '%united%'` |
| `_` wildcards | 12 | `LIKE 'Ja_an'`, `LIKE 'S___h%'`, `LIKE 'Bo%_a'` |
| `ILIKE` | 20 | `ILIKE 'DAVID%'`, `ILIKE '%africa'` |
| `NOT LIKE` / `NOT ILIKE` | 16 | `NOT LIKE 'Uni%'`, `NOT ILIKE '%africa%'` |
| Boolean combinations | 48 | Two to four predicates with `AND`, `OR` and nesting |
| Edge cases | 20 | Short, exact, empty-result, match-all, long and all-`_` patterns |
| Workload-style queries | 20 | Pattern predicates combined with non-pattern filters |
| Selectivity range | 10 | From a handful of rows to over 100,000 |
| Special characters | 4 | Parentheses, hyphens, dots, apostrophes |
| `ORDER BY` + `LIMIT` | 4 | Pagination over pattern matches |
| **Total** | **190** | |

Result sizes across the 190 queries: 12 empty, 7 with 1–100 rows, 13 with
100–1,000, 54 with 1,000–10,000, 48 with 10,000–100,000, and 56 with more than
100,000.

---

## Results

### Overall (warm cache)

| Metric | Biscuit | `pg_trgm` | B-tree |
|---|---|---|---|
| Mean | 38.82 ms | 134.54 ms | 193.38 ms |
| Median | 11.76 ms | 85.85 ms | 168.92 ms |
| Standard deviation | 49.15 ms | 113.99 ms | 133.78 ms |
| Minimum | 1.41 ms | 33.25 ms | 19.19 ms |
| Maximum | 679.76 ms | 621.21 ms | 799.55 ms |
| 95% CI of mean | ± 2.21 ms | ± 5.34 ms | ± 6.11 ms |
| Samples | 1,900 | 1,900 | 1,900 |

All three pairwise differences in mean were statistically significant
(Welch's *t*-test, *p* < 0.0001). Effect sizes (Cohen's *d*): Biscuit vs
`pg_trgm` 0.96, Biscuit vs B-tree 1.38, `pg_trgm` vs B-tree 0.51.

The distributions are strongly right-skewed: most queries were fast, and the
mean was dominated by queries returning large fractions of the table.

### Percentiles (warm cache, ms)

| Percentile | Biscuit | `pg_trgm` | B-tree |
|---|---|---|---|
| p10 | 2.84 | 38.92 | 40.28 |
| p25 | 5.12 | 56.34 | 89.45 |
| p50 | 11.76 | 85.85 | 168.92 |
| p75 | 35.89 | 145.67 | 253.78 |
| p90 | 102.45 | 267.89 | 412.56 |
| p95 | 156.23 | 389.12 | 567.23 |
| p99 | 258.67 | 605.43 | 784.90 |

### Cold versus warm cache

| Index | Cold mean | Warm mean | Change |
|---|---|---|---|
| Biscuit | 39.81 ms | 38.82 ms | −2.5% |
| `pg_trgm` | 125.47 ms | 134.54 ms | +7.2% |
| B-tree | 201.27 ms | 193.38 ms | −3.9% |

| Index | Cold hit ratio | Warm hit ratio |
|---|---|---|
| Biscuit | 89.77% | 90.50% |
| `pg_trgm` | 90.35% | 91.34% |
| B-tree | 75.58% | 75.65% |

Hit ratios near 90% even when cold indicate that the data set largely fitted
in memory, so cold-cache effects were small for all three. The increase for
`pg_trgm` in the warm state was not explained.

### By pattern type (warm cache)

| Pattern | Biscuit mean / median | `pg_trgm` mean / median | B-tree mean / median |
|---|---|---|---|
| Prefix `'x%'` | 7.76 / 5.41 ms | 58.41 / 45.23 ms | 63.81 / 48.67 ms |
| Suffix `'%x'` | 44.76 / 28.93 ms | 133.32 / 97.45 ms | 237.06 / 189.34 ms |
| Infix `'%x%'` | 29.72 / 18.45 ms | 93.28 / 74.32 ms | 152.03 / 124.56 ms |

The B-tree was a four-column `text_pattern_ops` index queried under forced
index use, which disadvantages it on prefixes of non-leading columns and on
every suffix and infix pattern.

### By result size (warm cache, mean)

| Rows returned | Biscuit | `pg_trgm` | B-tree |
|---|---|---|---|
| 1–100 | 2.45 ms | 38.92 ms | 42.18 ms |
| 1,000–10,000 | 12.34 ms | 78.45 ms | 145.67 ms |
| More than 100,000 | 156.78 ms | 389.23 ms | 534.12 ms |

### By boolean structure (warm cache, mean)

| Structure | Biscuit | `pg_trgm` | B-tree |
|---|---|---|---|
| Two predicates with `AND` | 8.92 ms | 67.34 ms | 123.45 ms |
| `(A OR B) AND (C OR D)` | 45.67 ms | 189.23 ms | 356.78 ms |

### Plan nodes observed

Despite the settings, PostgreSQL did not always use the forced plan type:

| Index | Index Scan | Bitmap Heap Scan | Seq Scan | Other |
|---|---|---|---|---|
| Biscuit | 3,120 (82%) | 580 (15%) | 40 (1%) | 60 (2%) |
| `pg_trgm` | 0 (0%) | 2,860 (75%) | 880 (23%) | 60 (2%) |
| B-tree | 1,160 (31%) | 40 (1%) | 2,400 (63%) | 200 (5%) |

GIN supports only bitmap scans, so `pg_trgm` queries ran as Bitmap Heap Scans
or, where no trigram could be extracted, as sequential scans. The B-tree fell
back to sequential scans for patterns it cannot serve.

### Buffer access (total over 3,800 runs per index)

| Index and state | Shared hit | Shared read | Total | Hit ratio |
|---|---|---|---|---|
| Biscuit, cold | 1,147,970 | 128,060 | 1,276,030 | 89.97% |
| Biscuit, warm | 975,424 | 103,182 | 1,078,606 | 90.50% |
| `pg_trgm`, cold | 1,578,384 | 168,519 | 1,746,903 | 90.35% |
| `pg_trgm`, warm | 1,369,768 | 129,750 | 1,499,518 | 91.34% |
| B-tree, cold | 9,016,492 | 2,913,998 | 11,930,490 | 75.58% |
| B-tree, warm | 9,009,231 | 2,899,828 | 11,909,059 | 75.65% |

In 2.1.3, Biscuit's bitmaps were held in backend memory, so its shared-buffer
figures cover heap access and do not include the index structures themselves.

### Consistency

| Query | Pattern | Biscuit CV | `pg_trgm` CV | B-tree CV |
|---|---|---|---|---|
| Q01 | `country LIKE 'Uni%'` | 0.056 | 0.113 | 0.163 |
| Q04 | `country LIKE '%stan'` | 0.102 | 0.152 | 0.019 |
| Q19 | `country LIKE '%united%'` | 0.047 | 0.314 | 0.053 |

The coefficient of variation across all queries was about 0.9 for each index,
reflecting the wide range of result sizes rather than run-to-run noise.

Queries more than three standard deviations above the mean occurred in 1.6%
of Biscuit runs, 2.5% of `pg_trgm` runs and 1.1% of B-tree runs. They were
predominantly queries returning more than 100,000 rows, such as
`username LIKE '%e%'` (about 40% of the table).

---

## Correctness Check

For every query, the row counts returned by the three indexes were compared
with each other, and each index's count was compared across all 20
iterations. All 190 queries returned identical counts across indexes and
iterations.

This check compared **counts** only. Two scans can return the same number of
rows while returning different rows, and several defects fixed in 3.1.0 would
not necessarily have been visible to it. The current test suite compares a
fingerprint of the returned rows against a sequential scan; see
[Development and Testing](development.md).

---

## Limitations of This Benchmark

* **Forced index usage.** Sequential and bitmap scans were disabled, so the
  results show how each index performs when used, not which plan PostgreSQL
  would choose or how fast the chosen plan would be.
* **Read-only.** Insert, update and delete costs were not measured.
* **Single machine, single data set, single size.** One laptop-class machine
  and one 1-million-row table.
* **No concurrency.** All queries ran in a single session. Per-connection
  memory and cache reloads, which matter for Biscuit, were not exercised.
* **Architecture.** 2.1.3 differs substantially from 3.x in storage, cost
  model and correctness; see the note at the top of this page.
* **Multi-column indexes only.** All three indexes covered the same four
  columns; single-column indexes may compare differently.
