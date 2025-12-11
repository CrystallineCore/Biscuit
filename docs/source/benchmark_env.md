
# Benchmark Environment 

This section documents the hardware and system environment used to run all benchmarks in this guide. 
## System Overview

| Component              | Details                                                                                    |
| ---------------------- | ------------------------------------------------------------------------------------------ |
| **CPU**                | AMD Ryzen 7 5700U with Radeon Graphics (8 cores / 16 threads, up to 4.37 GHz, Zen 2)       |
| **CPU Architecture**   | x86_64 (supports AVX, AVX2, FMA, BMI1/2, SHA-NI, AES-NI)                                   |
| **Memory (RAM)**       | 16 GB total (14 GiB usable)                     |
| **Storage**            | WD PC SN560 NVMe SSD — 1 TB (PCIe NVMe, non-rotational)                                    |
| **Operating System**   | Ubuntu 24.04.2 LTS (Noble Numbat), 64-bit                                              |
| **Kernel Version**     | Linux 6.14.0-36-generic (PREEMPT_DYNAMIC)                                              |
| **PostgreSQL Version** | 16.10 (Ubuntu 16.10-0ubuntu0.24.04.1) |
| **Extensions Used**    | `biscuit` , `pg_trgm`       |

---

## Benchmark Configuration

| Setting                  | Value                     | Notes                                                       |
| ------------------------ | ------------------------- | ----------------------------------------------------------- |
| **shared_buffers**       | 16384 → **16 MB**         | Very small (default on many distros)                        |
| **work_mem**             | 4096 → **4 MB per query** | Affects sort/hash operations                                |
| **maintenance_work_mem** | 65536 → **64 MB**         | Used for CREATE INDEX                                       |
| **effective_cache_size** | 524288 → **512 MB**       | Planner estimate of OS page cache                           |
| **synchronous_commit**   | **ON**                    | Safer but slightly slower writes                            |
| **wal_level**            | **replica**               | Allows logical/physical replication (default for modern PG) |


---

## Benchmark Methodology

* Queries were executed with `\timing on`.
* `EXPLAIN ANALYZE` is used for detailed plan inspection.

---

## Reproducibility Instructions

To reproduce identical performance:

1. Restart PostgreSQL before cold-cache benchmarks:

   ```bash
   sudo systemctl restart postgresql
   ```
2. Ensure OS disk cache is cleared (optional, requires sudo):

   ```bash
   sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
   ```
3. Disable power-saving CPU governors:

   ```bash
   sudo cpupower frequency-set -g performance
   ```
4. Run each query **three times**, discard the first warm run, take the median.

---

## Notes

* Hardware differences significantly impact absolute latency numbers.
* Your Biscuit index speedups will vary depending on:

  * Disk speed
  * CPU branch prediction
  * Text pattern distribution
  * PostgreSQL configuration


