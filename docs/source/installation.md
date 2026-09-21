# Installation

This page covers building Biscuit from source, enabling it in a database,
verifying the installation, upgrading, and removal.

---

## Prerequisites

* PostgreSQL 16 or later
* PostgreSQL server development files, which provide `pg_config` and the
  PGXS build infrastructure
* A C compiler (`gcc` or `clang`) and `make`
* `git`, for a source checkout
* Optional, recommended: the CRoaring library (see
  [Building with CRoaring](#building-with-croaring))

Check the server version:

```bash
pg_config --version
```

Biscuit is built against the PostgreSQL installation that `pg_config` points
to. If several versions are installed, pass the intended one explicitly with
`PG_CONFIG=/path/to/pg_config` on every `make` invocation.

---

## Building from Source

### 1. Install build dependencies

**Debian / Ubuntu** (replace `16` with your server's major version):

```bash
sudo apt-get install -y postgresql-server-dev-16 build-essential git
```

**RHEL / Fedora** (package names vary with the PostgreSQL repository in use):

```bash
sudo dnf install -y postgresql-server-devel gcc make git
```

### 2. Clone the repository

```bash
git clone https://github.com/crystallinecore/biscuit.git
cd biscuit
```

### 3. Build and install

```bash
make
sudo make install
```

To target a specific installation:

```bash
make PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config
sudo make install PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config
```

`make install` copies the shared library to `$(pg_config --pkglibdir)` and the
control file and SQL scripts to `$(pg_config --sharedir)/extension`. The 3.1.0
release installs two SQL scripts: `biscuit--3.1.0.sql` (fresh install) and
`biscuit--3.0.0--3.1.0.sql` (upgrade from 3.0.0).

### Building with CRoaring

Without CRoaring, Biscuit uses a built-in fallback bitmap implementation. It
returns the same results but is slower and uses more memory. CRoaring is
**not** detected automatically; it must be requested:

```bash
# Debian / Ubuntu
sudo apt-get install -y libroaring-dev

make clean
make WITH_ROARING=1
sudo make install
```

If CRoaring is installed under a non-standard prefix, pass its include and
library directories through the usual variables, for example:

```bash
make WITH_ROARING=1 \
     CPPFLAGS="-I/opt/croaring/include" \
     SHLIB_LINK="-L/opt/croaring/lib -lroaring"
```

The Makefile stops with an error if `WITH_ROARING` is set but
`-DHAVE_ROARING` does not reach the compiler. After installing, confirm that
CRoaring is active from SQL rather than assuming it:

```sql
SELECT biscuit_has_roaring();      -- true when compiled with CRoaring
SELECT biscuit_roaring_version();  -- CRoaring version, or NULL
```

```{note}
Earlier versions of this documentation referred to `make HAVE_ROARING=1`, and
releases before 3.1.0 attempted to detect CRoaring automatically. In 3.1.0
the only supported switch is `make WITH_ROARING=1`.
```

### From PGXN

```bash
pgxn install biscuit
```

---

## Enabling the Extension

Connect to the target database and create the extension:

```sql
CREATE EXTENSION biscuit;
```

Confirm the installed version:

```sql
SELECT extversion FROM pg_extension WHERE extname = 'biscuit';
SELECT biscuit_version();
```

Both should report `3.1.0`. `biscuit_version()` reports the version of the
loaded shared library, so a mismatch between the two means the library and the
catalog objects are from different releases (see [Upgrading](#upgrading)).

The extension is not relocatable after creation. To place its objects in a
specific schema, specify it at creation time:

```sql
CREATE EXTENSION biscuit SCHEMA extensions;
```

---

## Verifying the Installation

```sql
CREATE TABLE test_biscuit (id serial PRIMARY KEY, name text);

INSERT INTO test_biscuit (name)
SELECT 'product_' || i FROM generate_series(1, 100000) AS i;

CREATE INDEX idx_test_name ON test_biscuit USING biscuit (name);
ANALYZE test_biscuit;

EXPLAIN (ANALYZE)
SELECT count(*) FROM test_biscuit WHERE name LIKE 'product\_4%';
```

The plan should show a scan on `idx_test_name`, for example:

```text
Aggregate
  ->  Bitmap Heap Scan on test_biscuit
        Recheck Cond: (name ~~ 'product\_4%'::text)
        ->  Bitmap Index Scan on idx_test_name
              Index Cond: (name ~~ 'product\_4%'::text)
```

`Recheck Cond` is printed for every Bitmap Heap Scan regardless of what the
index reports, so its presence does not mean rows were rechecked.

On small tables the planner may choose a sequential scan, which is usually
the right decision. To confirm that the index *can* be used, repeat the query
after `SET enable_seqscan = off;` in a test session only.

To check the build configuration:

```sql
SELECT * FROM biscuit_build_info();
SELECT * FROM biscuit_check_config();
```

---

## Upgrading

### From 3.0.0

No on-disk format change and no `REINDEX` is required.

1. Install the 3.1.0 shared library and SQL scripts (`make && sudo make
   install`). Install the library **before** running the SQL upgrade: the
   upgrade registers the regex operators as strategies 5–8, and those are
   dispatched to the new library as soon as they exist.
2. In each database that uses Biscuit:

   ```sql
   ALTER EXTENSION biscuit UPDATE TO '3.1.0';
   ```

The upgrade script adds the regex operators to the existing operator families
with `ALTER OPERATOR FAMILY`, so existing indexes can serve regex queries
immediately.

Rebuilding is optional but worth considering for indexes that were created on
tables that had already been updated. 3.1.0 corrects how rows are mapped to
HOT-chain roots at build time; an index built by an earlier version keeps
whatever the earlier build recorded until it is rebuilt:

```sql
REINDEX INDEX CONCURRENTLY idx_name;
```

The same applies to partial indexes built before 3.1.0, which were built over
every row rather than only rows satisfying the index predicate.

### From 2.x

3.0.0 changed the on-disk format, and indexes built under 2.x cannot be read
by 3.x. Every Biscuit index must be rebuilt. The 3.1.0 packaging installs only
the 3.1.0 install script and the 3.0.0 → 3.1.0 upgrade script, so choose one of
the following:

* Upgrade to 3.0.0 first (following the 3.0.0 upgrade notes, including
  `REINDEX`), then upgrade to 3.1.0 as above; or
* Record the definitions of existing Biscuit indexes
  (`SELECT index_definition FROM biscuit_indexes_detailed;`), run
  `DROP EXTENSION biscuit CASCADE`, install 3.1.0, run `CREATE EXTENSION
  biscuit`, and recreate the indexes.

Plan a maintenance window sized for the rebuild; Biscuit indexes take longer
to build than `pg_trgm` GIN indexes on the same data.

---

## Troubleshooting

### `could not access file "$libdir/biscuit"`

The shared library is not in the directory the server loads from. Check that
`make install` used the same `pg_config` as the running server:

```bash
pg_config --pkglibdir
ls "$(pg_config --pkglibdir)"/biscuit*
```

### `extension "biscuit" has no installation script` / `could not open extension control file`

The control file or SQL scripts are missing from the server's extension
directory:

```bash
ls "$(pg_config --sharedir)"/extension/biscuit*
```

Expected files include `biscuit.control`, `biscuit--3.1.0.sql` and
`biscuit--3.0.0--3.1.0.sql`.

### `biscuit_has_roaring()` returns `false` after building with CRoaring

Rebuild from clean with `WITH_ROARING=1`, reinstall, and start a new session
(or restart the server) so the new library is loaded:

```bash
make clean
make WITH_ROARING=1
sudo make install
```

### CRoaring headers not found

Install the CRoaring development package, or pass its include directory as
shown in [Building with CRoaring](#building-with-croaring).

---

## Removal

Dropping the extension drops every Biscuit index along with it:

```sql
DROP EXTENSION biscuit CASCADE;
```

Then remove the installed files:

```bash
sudo rm "$(pg_config --pkglibdir)"/biscuit.so
sudo rm "$(pg_config --sharedir)"/extension/biscuit*
```

---

## Next Steps

* [Quick Start](quickstart.md)
* [Pattern Syntax](patterns.md)
* [Performance and Operations](performance.md)
