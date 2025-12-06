# Installation Guide

This guide walks you through installing the Biscuit index extension for PostgreSQL.

---

## Prerequisites

Before installing Biscuit, ensure you have:

- PostgreSQL 16 or higher installed
- PostgreSQL development headers (`postgresql-server-dev` package)
- C compiler (gcc or clang)
- Make build tool
- Git (for source installation)

### Check PostgreSQL Version

```bash
psql --version
# Should show: psql (PostgreSQL) 16.x or higher
```

---

## Installation Methods

### Method 1: From Source (Recommended)

#### Step 1: Install Build Dependencies

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y postgresql-server-dev-14 build-essential git
```

**RHEL/CentOS/Fedora:**
```bash
sudo yum install -y postgresql-devel gcc make git
```

**macOS (Homebrew):**
```bash
brew install postgresql
```

#### Step 2: Clone Repository

```bash
git clone https://github.com/crystallinecore/biscuit.git
cd biscuit
```

#### Step 3: Build and Install

```bash
# Build the extension
make

# Install system-wide (requires sudo)
sudo make install

# Or install to PostgreSQL extension directory
make install PG_CONFIG=/path/to/pg_config
```

#### Step 4: Verify Installation

```bash
# Check that biscuit.so was installed
ls $(pg_config --pkglibdir)/biscuit.so

# Check control file
ls $(pg_config --sharedir)/extension/biscuit.control
```

---

### Method 2: With CRoaring Support (Enhanced Performance)

For optimal performance, install with the CRoaring library:

#### Install CRoaring Library

**Ubuntu/Debian:**
```bash
sudo apt-get install -y libroaring-dev
```

**From Source:**
```bash
git clone https://github.com/RoaringBitmap/CRoaring.git
cd CRoaring
mkdir build && cd build
cmake ..
make
sudo make install
```

#### Build Biscuit with Roaring

```bash
cd biscuit
make HAVE_ROARING=1
sudo make install
```

---

## Enable Extension in Database

After installation, enable Biscuit in your PostgreSQL database:

```sql
-- Connect to your database
psql -U postgres -d your_database

-- Create the extension
CREATE EXTENSION biscuit;

-- Verify installation
SELECT * FROM pg_available_extensions WHERE name = 'biscuit';
```

Expected output:
```
   name   | default_version | installed_version | comment
----------+-----------------+-------------------+---------
 biscuit  | 1.0             | 1.0               | Bitmap-based...
```

---

## Post-Installation Configuration

### Set Appropriate Memory Limits

Biscuit stores indexes in memory. Adjust PostgreSQL memory settings:

```sql
-- In postgresql.conf
shared_buffers = 4GB           # Increase for large indexes
work_mem = 256MB               # For sorting operations
maintenance_work_mem = 1GB     # For index building
```

### Enable Query Logging (Optional)

Monitor Biscuit performance:

```sql
-- Enable timing
\timing on

-- Log slow queries
SET log_min_duration_statement = 100;  -- Log queries > 100ms
```

---

## Verify Installation with Test

Create a test table and index:

```sql
-- Create test table
CREATE TABLE test_biscuit (
    id SERIAL PRIMARY KEY,
    name TEXT
);

-- Insert test data
INSERT INTO test_biscuit (name)
SELECT 'product_' || i FROM generate_series(1, 10000) i;

-- Create Biscuit index
CREATE INDEX idx_test_name ON test_biscuit USING biscuit (name);

-- Test query
EXPLAIN ANALYZE
SELECT * FROM test_biscuit WHERE name LIKE '%product_42%';
```

Expected output should show:
```
Index Scan using idx_test_name on test_biscuit
  (cost=0.00..8.27 rows=1 width=...)
  Index Cond: (name ~~ '%product_42%'::text)
Planning Time: 0.123 ms
Execution Time: 0.567 ms
```

---

## Troubleshooting

### Error: "could not load library"

**Problem**: PostgreSQL can't find `biscuit.so`

**Solution**:
```bash
# Check installation directory
pg_config --pkglibdir

# Manually copy if needed
sudo cp biscuit.so $(pg_config --pkglibdir)/
```

### Error: "extension does not exist"

**Problem**: Extension files not in correct location

**Solution**:
```bash
# Check extension directory
pg_config --sharedir

# Verify files exist
ls $(pg_config --sharedir)/extension/biscuit*
```

### Build Errors with Roaring

**Problem**: CRoaring headers not found

**Solution**:
```bash
# Specify include path explicitly
make HAVE_ROARING=1 CFLAGS="-I/usr/local/include"
```

---

## Uninstallation

To remove Biscuit:

```sql
-- Drop from database first
DROP EXTENSION biscuit CASCADE;
```

```bash
# Remove files
sudo rm $(pg_config --pkglibdir)/biscuit.so
sudo rm $(pg_config --sharedir)/extension/biscuit*
```

---

## Next Steps

-  **Installation complete!** 
-  Continue to [Quick Start Tutorial](quickstart.md)
-  Read about [Pattern Syntax](patterns.md)
-  Learn about [Performance Tuning](performance.md)

---

## Platform-Specific Notes

### Windows

For Windows installation:

1. Install Visual Studio with C++ support
2. Use PostgreSQL Windows build tools
3. Compile with: `nmake /f Makefile.win`

### Docker

Run Biscuit in Docker:

```dockerfile
FROM postgres:14

# Install build dependencies
RUN apt-get update && apt-get install -y \
    postgresql-server-dev-14 \
    build-essential \
    git

# Build and install Biscuit
RUN git clone https://github.com/crystallinecore/biscuit.git /tmp/biscuit
WORKDIR /tmp/biscuit
RUN make && make install

# Cleanup
RUN rm -rf /tmp/biscuit
```

---

## Getting Help

If you encounter issues:

1. Check the [FAQ](faq.md)
2. Search [GitHub Issues](https://github.com/crystallinecore/biscuit/issues)
3. Ask on [GitHub Discussions](https://github.com/crystallinecore/biscuit/discussions)
4. Email: sivaprasad.off@gmail.com