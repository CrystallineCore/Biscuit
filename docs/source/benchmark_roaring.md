# Benchmark: v2.1.3, CRoaring

```{important}
**These results are historical.** They were measured in December 2025 with
Biscuit **2.1.3**, built **with** CRoaring, and have not been repeated for
3.x. Since then, Biscuit has moved to WAL-logged on-disk storage with a
pending log (3.0.0), received a rewritten cost model (3.0.0), and had several
correctness fixes (3.1.0). Absolute timings, index sizes and plan choices for
3.1.0 will differ.

Sequential and bitmap scans were disabled to force index use, so the B-tree
figures mostly reflect patterns a B-tree cannot serve efficiently, and none of
the figures reflect the plans PostgreSQL would choose on its own. More recent
testing on 3.x found Biscuit and `pg_trgm` to be largely complementary:
Biscuit well ahead on anchored and positional patterns, `pg_trgm` well ahead
on unanchored and character-class patterns. See
[Choosing an Index](index.md#choosing-an-index) for current guidance.
```

This run repeats [Benchmark: v2.1.3, Fallback Bitmaps](benchmark.md) with
Biscuit compiled against CRoaring. The data set, index definitions, query
suite, settings and method are the same and are described on that page. The
machine is described in [Benchmark Environment](benchmark_env.md).

Figures that the original report marked as estimated rather than measured
have been omitted here.

---

## Index Size

| Object | Size |
|---|---|
| Table | 129 MB |
| B-tree index | 43 MB |
| `pg_trgm` GIN index | 86 MB |
| Biscuit index, fallback bitmaps | 914.59 MB |
| Biscuit index, CRoaring | 277.09 MB (290,550,951 bytes) |

With CRoaring, Biscuit's size was 30% of the fallback build (a 3.3:1
reduction), and 3.2 times the size of the `pg_trgm` index. As in the fallback
run, the Biscuit figure is the 2.1.3 in-memory size and is not directly
comparable with the on-disk size of a 3.x index.

---

## Results

### Overall (warm cache)

| Metric | Biscuit | `pg_trgm` | B-tree |
|---|---|---|---|
| Mean | 38.37 ms | 111.45 ms | 192.42 ms |
| Median | 11.34 ms | 63.74 ms | 170.26 ms |
| Standard deviation | 48.17 ms | 98.39 ms | 134.76 ms |
| Minimum | 1.41 ms | 33.25 ms | 19.19 ms |
| Maximum | 261.20 ms | 569.23 ms | 783.53 ms |
| 95% CI of mean | ± 2.17 ms | ± 4.41 ms | ± 6.06 ms |
| Samples | 1,900 | 1,900 | 1,900 |

All three pairwise differences in mean were statistically significant
(Welch's *t*-test, *p* < 0.0001; *t* = −25.08 for Biscuit vs `pg_trgm`,
−34.12 for Biscuit vs B-tree, −18.94 for `pg_trgm` vs B-tree).

### Compared with the fallback build (warm cache)

| Metric | Fallback | CRoaring | Change |
|---|---|---|---|
| Mean | 38.82 ms | 38.37 ms | −1.2% |
| Median | 11.76 ms | 11.34 ms | −3.6% |
| Standard deviation | 49.15 ms | 48.17 ms | −2.0% |
| Index size | 914.59 MB | 277.09 MB | −69.7% |

Query times were essentially unchanged; the main effect of CRoaring in this
run was the reduction in memory size.

### Cold versus warm cache

| Index | Cold mean | Warm mean | Change |
|---|---|---|---|
| Biscuit | 38.96 ms | 38.37 ms | −1.5% |
| `pg_trgm` | 112.30 ms | 111.45 ms | −0.8% |
| B-tree | 193.04 ms | 192.42 ms | −0.3% |

| Index | Cold hit ratio | Warm hit ratio |
|---|---|---|
| Biscuit | 89.83% | 90.49% |
| `pg_trgm` | 90.33% | 91.35% |
| B-tree | 75.58% | 75.65% |

As in the fallback run, the data set largely fitted in memory, so cold-cache
effects were small.

### By pattern type (warm cache, mean)

| Pattern | Biscuit | `pg_trgm` | B-tree |
|---|---|---|---|
| Prefix `'x%'` | 7.64 ms | 46.81 ms | 61.43 ms |
| Suffix `'%x'` | 44.76 ms | 113.58 ms | 226.77 ms |
| Infix `'%x%'` | 29.22 ms | 81.25 ms | 147.37 ms |

The B-tree was a four-column `text_pattern_ops` index queried under forced
index use, which disadvantages it on prefixes of non-leading columns and on
every suffix and infix pattern.

### Plan nodes observed

| Index | Index Scan | Bitmap Heap Scan | Seq Scan | Limit | Gather |
|---|---|---|---|---|---|
| Biscuit | 3,120 (82%) | 580 (15%) | 40 (1%) | 60 (2%) | 0 |
| `pg_trgm` | 0 | 2,860 (75%) | 880 (23%) | 60 (2%) | 0 |
| B-tree | 1,160 (31%) | 40 (1%) | 2,400 (63%) | 60 (2%) | 140 (4%) |

### Buffer access (total over 3,800 runs per index)

| Index and state | Shared hit | Shared read | Total | Hit ratio |
|---|---|---|---|---|
| Biscuit, cold | 1,257,341 | 128,060 | 1,385,401 | 89.83% |
| Biscuit, warm | 1,076,943 | 103,182 | 1,180,125 | 90.49% |
| `pg_trgm`, cold | 1,749,957 | 168,519 | 1,918,476 | 90.33% |
| `pg_trgm`, warm | 1,494,316 | 129,750 | 1,624,066 | 91.35% |
| B-tree, cold | 11,931,438 | 2,913,998 | 14,845,436 | 75.58% |
| B-tree, warm | 11,909,333 | 2,899,828 | 14,809,161 | 75.65% |

In 2.1.3, Biscuit's bitmaps were held in backend memory, so its shared-buffer
figures cover heap access and do not include the index structures themselves.

### Consistency

| Index | Mean coefficient of variation across queries |
|---|---|
| Biscuit | 0.945 |
| `pg_trgm` | 0.883 |
| B-tree | 0.700 |

| Query | Pattern | Biscuit CV | `pg_trgm` CV | B-tree CV |
|---|---|---|---|---|
| Q01 | `country LIKE 'Uni%'` | 0.023 | 0.098 | 0.182 |
| Q04 | `country LIKE '%stan'` | 0.012 | 0.006 | 0.013 |
| Q05 | `country LIKE '%ia'` | 0.014 | 0.029 | 0.020 |

Queries more than three standard deviations above the mean occurred in 1.6%
of Biscuit runs, 2.6% of `pg_trgm` runs and 1.1% of B-tree runs, mostly
queries returning very large result sets. Examples:

| Index | Query | Time | Mean for that index |
|---|---|---|---|
| Biscuit | `username LIKE '%e%'` (about 40% of rows) | 261.20 ms | 38.37 ms |
| `pg_trgm` | `OR` of several infix patterns | 569.10 ms | 111.45 ms |
| B-tree | `OR` of several suffix patterns | 783.53 ms | 192.42 ms |

---

## Correctness Check

For every query, the row counts returned by the three indexes were compared
with each other, and each index's count was compared across all 20
iterations. All 190 queries returned identical counts across indexes and
iterations; for example, `country LIKE 'Uni%'` returned 20,247 rows,
`country LIKE '%stan'` 3,017, and `country LIKE '%ia'` 333,637 with all three
indexes.

This check compared **counts** only, which cannot detect two scans returning
different rows in equal numbers. The current test suite compares a
fingerprint of the returned rows against a sequential scan; see
[Development and Testing](development.md).

---

## Limitations of This Benchmark

The limitations listed for the
[fallback run](benchmark.md#limitations-of-this-benchmark) apply equally:
forced index usage, read-only workload, a single machine and data set, no
concurrency, multi-column indexes only, and a pre-3.0 architecture.
