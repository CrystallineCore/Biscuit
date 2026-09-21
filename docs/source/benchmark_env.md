# Benchmark Environment

This page describes the machine used for the historical v2.1.3 benchmarks
([fallback bitmaps](benchmark.md) and [CRoaring](benchmark_roaring.md)). It
is a single laptop-class system; results on other hardware will differ in
absolute terms and may differ in relative terms.

---

## System

| Component | Details |
|---|---|
| CPU | AMD Ryzen 7 5700U (8 cores / 16 threads, up to 4.37 GHz, Zen 2) |
| Memory | 16 GB (14 GiB usable) |
| Storage | WD PC SN560 NVMe SSD, 1 TB |
| Operating system | Ubuntu 24.04.2 LTS, 64-bit |
| Kernel | Linux 6.14.0-36-generic |
| PostgreSQL | 16.x from the Ubuntu packages (the benchmark reports record 16.11) |
| Extensions | `biscuit` 2.1.3, `pg_trgm` |

---

## Server Configuration

Server-level settings (largely distribution defaults):

| Setting | Value |
|---|---|
| `shared_buffers` | 16 MB |
| `work_mem` | 4 MB (raised to 256 MB in the benchmark sessions) |
| `maintenance_work_mem` | 64 MB |
| `effective_cache_size` | 512 MB |
| `synchronous_commit` | on |
| `wal_level` | replica |

The benchmark sessions additionally set `random_page_cost = 1.1`,
`enable_seqscan = off` and `enable_bitmapscan = off`. With these settings the
results describe index performance under forced index use, not the plans
PostgreSQL would choose by default.

---

## Procedure

* Queries were timed with `\timing on`, and plans were captured with
  `EXPLAIN (ANALYZE, BUFFERS)`.
* Before each cold-cache run, PostgreSQL was restarted and the OS page cache
  was dropped:

  ```bash
  sudo systemctl restart postgresql
  sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
  ```

* For repeatable timings, set the CPU frequency governor to `performance`:

  ```bash
  sudo cpupower frequency-set -g performance
  ```

---

## Reproducing on 3.x

The 2.1.3 results have not been repeated for 3.x. When measuring 3.x, note
the differences that affect results:

* Index state is stored in WAL-logged index pages, and each backend loads its
  own copy on first use, so first-query latency in a new connection and
  memory per connection should be measured separately from warm-query
  latency.
* The cost model was rewritten in 3.0.0. Running with the planner unrestricted
  (`enable_seqscan` and `enable_bitmapscan` left on) shows which plans are
  chosen in practice, which is usually the more useful measurement.
* Write cost (WAL volume and time per row) and concurrent-reader behaviour
  after writes are relevant to 3.x deployments and were not covered by the
  2.1.3 benchmark.
