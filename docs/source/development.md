# Development and Testing

This page covers building Biscuit for development and running its test
suite.

---

## Development Build

```bash
git clone https://github.com/crystallinecore/biscuit.git
cd biscuit
make clean
make COPT="-g -O0"              # add WITH_ROARING=1 to test the CRoaring build
sudo make install
```

Test both bitmap back ends where a change touches bitmap code: the default
build uses the fallback implementation, and `make WITH_ROARING=1` uses
CRoaring.

Every `.c` file under `src/` is compiled automatically. The installed SQL
scripts are `sql/biscuit--3.1.0.sql` and `sql/biscuit--3.0.0--3.1.0.sql`.

`make help` lists the build and test targets.

---

## Test Suite

The suite lives in `tests/` and runs against a live PostgreSQL server with
the extension installed.

### Method

Every test case is **differential**. The same predicate is evaluated twice
against the same rows:

1. with index paths disabled, so PostgreSQL's own matching over a sequential
   scan acts as the reference; and
2. with sequential scans disabled, so the Biscuit index is used where it can
   be.

The two must agree on the row count **and** on a fingerprint of which rows
were returned, since two scans can agree on `count(*)` and still return
different rows.

Each case also declares whether the index **must** serve it, **must not**
serve it, or **either**. This catches regressions in both directions: a
supported pattern that stops using the index, and an unsupported pattern that
the index starts accepting.

Each file asserts internally and raises an error on failure, so the exit
status is the result and there are no expected-output files to maintain. The
`.sql` files contain no `psql`-specific syntax and can be run from any client;
the only step that needs a shell is the crash in the middle of the WAL
category.

### Categories

| File | Covers |
|---|---|
| `00_harness` | Shared setup and assertion helpers, used by the others |
| `01_like` | Anchoring, `_` placement, escapes and the `ESCAPE` clause, wildcards as data, NULLs |
| `02_ilike` | The same for `ILIKE`, plus case folding |
| `03_regex` | The rewritable subset, rejected constructs, rewrite identities, `~*` rules |
| `04_composition` | `AND`/`OR`/`NOT` over patterns and regular expressions, mixed operator families |
| `05_multicolumn` | Multi-column indexes cross-checked against single-column indexes |
| `06_opclass` | Operator-class gating in both directions, catalog and storage checks |
| `07_dml_mvcc` | Inserts, updates and deletes after build, savepoints, rollback, `VACUUM`, TOAST |
| `08_unicode` | Character versus byte positions, 1–4 byte characters, combining sequences |
| `09_wal_setup`, `09_wal_verify` | Crash recovery: immediate shutdown, WAL replay, unlogged relations |
| `10_stress` | Generated patterns from a fixed seed, around the rewritable-subset boundary |

### Running the suite

Standard libpq environment variables (`PGHOST`, `PGPORT`, `PGUSER`,
`PGDATABASE`) select the server and database. The Makefile uses the `psql`
belonging to the `pg_config` the extension was built with.

| Command | Runs |
|---|---|
| `make check-suite-nowal` | All categories except crash recovery. Does not restart the server. |
| `make check-suite` | All categories. Includes crash recovery if the data directory can be determined (see below). |
| `make check-suite CATEGORIES="03_regex 08_unicode"` | Selected categories (matched by name) |
| `make check-wal` | Crash recovery only. **Stops and restarts the server.** |
| `make test`, `make test-all`, `make check-all` | Aliases for `make check-suite` |

```{warning}
In 3.1.0, `make test` is an alias for `make check-suite`, not for
`make check-suite-nowal`. If the server's data directory can be discovered,
it runs the crash-recovery category, which stops the server with
`pg_ctl -m immediate stop` and restarts it. Against any server you do not
want restarted, use `make check-suite-nowal`, or set `SKIP_WAL=1`.
```

The runner can also be invoked directly:

```bash
tests/run_all.sh                    # everything
tests/run_all.sh 03_regex 05_multi  # selected categories
SKIP_WAL=1 tests/run_all.sh         # skip crash recovery
```

### Crash-recovery category

The crash test performs an immediate shutdown (no shutdown checkpoint), so
recovery must replay WAL. It needs a data directory it can control:

* `PGDATA` is required. The Makefile targets discover it from the server
  (`SHOW data_directory`) if it is not set; if the server cannot be reached,
  the category is skipped with a message.
* `PGBIN` selects the directory containing `psql` and `pg_ctl`.
* `PGCTL_OPTS` passes options for the restart that are not in
  `postgresql.conf` (for example, a non-default port or socket directory).
* `PG_START_CMD` and `PG_STOP_CMD` override the restart and stop commands.
  Debian and Ubuntu packaged clusters (`pg_ctlcluster`) are detected
  automatically.
* `SKIP_WAL=1` skips the category, for example on a managed service where the
  data directory is not local.

Do not run the crash-recovery category against a server you care about.

---

## Concurrency Testing

The suite runs in a single session. Changes to the scan, cache or pending-log
code should additionally be tested with at least two sessions: one querying
the index, and another committing changes that the first must then observe.
Single-session tests do not exercise cache invalidation or concurrent
compaction.

Useful practices:

* Compare index and sequential-scan results for the same predicate within a
  single snapshot while another session writes concurrently, so any
  divergence is attributable to the index rather than to timing.
* Check the server log as well as client output; background diagnostics may
  not reach the client.
* `SET biscuit.diag_scan_trace = on;` reports per-key candidate counts and
  reload activity for the session.

---

## Reporting Results

When reporting a failure, include the PostgreSQL version, whether CRoaring was
enabled (`SELECT biscuit_has_roaring();`), the failing category and case
name, and the relevant part of the server log.
