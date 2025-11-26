# BISCUIT: Bitmap-Indexed String Comparison Using Intelligent Traversal

A PostgreSQL extension for accelerating wildcard pattern matching queries using bitmap indexing.

### Updates in 1.0.6
 - Introduced a new bitmap that stores records having length greater than 'n', to reduce computational cost.

## Technical Overview

### Problem Statement

Traditional database indexes (B-tree, GiST, GIN) exhibit suboptimal performance for wildcard pattern matching queries, particularly when wildcards appear at the beginning of patterns. A query such as `SELECT * FROM table WHERE column LIKE '%pattern%'` typically requires a sequential scan.

### Solution Architecture

BISCUIT implements a multi-dimensional bitmap index structure that decomposes strings into positional character mappings. Each character at each position is tracked using compressed bitmaps (Roaring Bitmaps), enabling efficient intersection operations for pattern matching.

#### Core Principle

Instead of scanning strings sequentially, BISCUIT:
1. Pre-indexes all character positions (up to 256 characters)
2. Uses bitmap intersections to find candidate records
3. Filters results through tombstone bitmap (for deleted records)
4. Returns matching records

**Reality Check**: This trades initial indexing time and memory for faster queries. It's not magic—you're paying upfront to make searches faster later.

#### Core Index Structures

1. **Positional Character Index**: Maps each character to position-bitmap pairs
   - Forward positions: `{0, 1, 2, ..., 255}` from string start
   - Reverse positions: `{-1, -2, -3, ..., -256}` from string end
   
2. **Character Existence Cache**: Union of all positional bitmaps per character for containment queries

3. **Length Index**: Bitmap array indexed by string length

4. **Primary Key Hash Table**: O(1) lookup structure mapping primary keys to internal indices

5. **Lazy Deletion Layer**: Tombstone bitmap tracking deleted records with deferred cleanup

---


### Key Features

- **Bitmap-based pattern matching**: Optimized for wildcard searches
- **Hash table primary key lookup**: Fast record access by primary key
- **Lazy deletion**: Deferred cleanup of deleted records using tombstones
- **Incremental updates**: Optimized path for similar string updates
- **Automatic maintenance**: Optional triggers to keep index synchronized
- **Roaring Bitmap support**: Uses CRoaring library when available

### Limitations

- **In-memory only**: Entire index must fit in RAM
- **Single column**: Indexes one column at a time
- **Session-scoped**: Index is rebuilt per session (not persistent)
- **String data only**: Designed for text/varchar columns
- **Memory overhead**: Bitmap indices can be memory-intensive for large datasets

## Installation and Requirements

### System Requirements

- **PostgreSQL Version**: 11.0 or higher
- **Operating System**: Linux, macOS, Windows (with MinGW)
- **Architecture**: x86_64, ARM64
- **Memory**: Minimum 512MB available RAM per index
- **Compiler**: GCC 4.8+, Clang 3.9+, or MSVC 2017+

### Optional Dependencies

- **CRoaring Library**: For optimized Roaring Bitmap operations (recommended)
  - Installation: `apt-get install libroaring-dev` (Debian/Ubuntu)
  - Falls back to built-in implementation if unavailable


## Installation

### Prerequisites

- PostgreSQL 16 or higher
- C compiler (gcc/clang)
- (Optional) CRoaring library for optimized bitmap operations

### Build and Install

```bash
# With Roaring Bitmap support (recommended)
make HAVE_ROARING=1
sudo make install HAVE_ROARING=1

# Without Roaring (uses fallback implementation)
make
sudo make install
```

### Enable Extension

```sql
CREATE EXTENSION biscuit;
```

## Quick Start

```sql
-- One-command setup: builds index, creates query function, enables triggers
SELECT biscuit_setup('users', 'email', 'id');

-- Query with patterns
SELECT * FROM biscuit_match('%@gmail.com');
SELECT * FROM biscuit_match('admin%');
SELECT * FROM biscuit_match('%test%');

-- Get count only (faster than full query)
SELECT biscuit_match_count('%@gmail.com');

-- Check index status
SELECT biscuit_index_status();
```

## Function Reference

### Setup Functions

#### `biscuit_setup(table_name, column_name, pk_column_name)`

Complete initialization in one step. Builds the bitmap index, creates a type-safe `biscuit_match()` function, and enables automatic triggers.

```sql
SELECT biscuit_setup('users', 'email', 'id');
```

**Parameters:**
- `table_name` (text): Name of the table to index
- `column_name` (text): Name of the column to index (must be text/varchar)
- `pk_column_name` (text): Name of the primary key column (default: 'id') 

**Returns:** Success message with usage instructions

#### `biscuit_build_index(table_name, column_name, pk_column_name)`

Low-level function to build the bitmap index only, without creating query functions or triggers.

```sql
SELECT biscuit_build_index('users', 'email', 'id');
```

**Returns:** Boolean (true on success)

### Query Functions

#### `biscuit_match(pattern)`

Returns complete table rows matching the pattern. This function is created by `biscuit_setup()` and is strongly-typed to match your table schema.

```sql
-- Get all matching rows
SELECT * FROM biscuit_match('%@gmail.com');

-- Filter and sort results
SELECT id, email FROM biscuit_match('%test%') 
WHERE created_at > '2024-01-01' 
ORDER BY id LIMIT 10;
```

**Parameters:**
- `pattern` (text): Wildcard pattern using `%` (zero or more chars) and `_` (exactly one char)

**Returns:** TABLE matching the indexed table's schema

#### `biscuit_match_count(pattern)`

Returns only the count of matching records. This is more efficient than counting rows from `biscuit_match()`.

```sql
SELECT biscuit_match_count('%@gmail.com');
```

**Parameters:**
- `pattern` (text): Wildcard pattern

**Returns:** INTEGER count of matches

#### `biscuit_match_keys(pattern)`

Returns primary key and indexed value pairs as text. Useful for joining or when you only need these fields.

```sql
-- Get keys and values
SELECT * FROM biscuit_match_keys('%@gmail.com');

-- Cast primary key back to original type
SELECT pk::integer AS id, value FROM biscuit_match_keys('%pattern%');
```

**Parameters:**
- `pattern` (text): Wildcard pattern

**Returns:** TABLE(pk text, value text)

### Monitoring Functions

#### `biscuit_index_status()`

Returns detailed status information about the current index including table/column info, record counts, memory usage, CRUD statistics, and lazy deletion status.

```sql
SELECT biscuit_index_status();
```

**Returns:** TEXT formatted status report

#### `biscuit_get_active_count()`

Returns the number of live (non-deleted) records in the index.

```sql
SELECT biscuit_get_active_count();
```

**Returns:** INTEGER

#### `biscuit_get_free_slots()`

Returns the number of reusable slots from deleted records.

```sql
SELECT biscuit_get_free_slots();
```

**Returns:** INTEGER

#### `biscuit_get_tombstone_count()`

Returns the number of soft-deleted records awaiting cleanup.

```sql
SELECT biscuit_get_tombstone_count();
```

**Returns:** INTEGER

### Maintenance Functions

#### `biscuit_cleanup()`

Manually triggers tombstone cleanup. Normally cleanup happens automatically when the tombstone threshold is reached, but you can force it with this function.

```sql
SELECT biscuit_cleanup();
```

**Returns:** TEXT cleanup report

#### `biscuit_enable_triggers()`

Activates automatic index maintenance triggers on the indexed table.

```sql
SELECT biscuit_enable_triggers();
```

**Returns:** TEXT confirmation message

#### `biscuit_disable_triggers()`

Disables automatic index updates. Useful for bulk operations to avoid per-row overhead. Remember to rebuild the index afterward.

```sql
SELECT biscuit_disable_triggers();

-- Perform bulk operations
INSERT INTO users ...;
UPDATE users ...;

-- Rebuild index
SELECT biscuit_build_index('users', 'email', 'id');
SELECT biscuit_enable_triggers();
```

**Returns:** TEXT confirmation message

#### `biscuit_version()`

Returns the version of the BISCUIT extension.

```sql
SELECT biscuit_version();
```

**Returns:** TEXT version string

## Pattern Syntax

BISCUIT supports SQL LIKE pattern syntax:

- `%` - Matches zero or more characters
- `_` - Matches exactly one character

### Pattern Examples

```sql
-- Prefix match
SELECT * FROM biscuit_match('john%');           -- Starts with 'john'

-- Suffix match
SELECT * FROM biscuit_match('%@gmail.com');     -- Ends with '@gmail.com'

-- Contains match
SELECT * FROM biscuit_match('%test%');          -- Contains 'test'

-- Exact length
SELECT * FROM biscuit_match('____');            -- Exactly 4 characters

-- Single wildcard
SELECT * FROM biscuit_match('a_c');             -- 'a', any char, 'c'

-- Complex patterns
SELECT * FROM biscuit_match('admin%2024');      -- Starts 'admin', ends '2024'
SELECT * FROM biscuit_match('%user%@%');        -- Contains 'user', then '@'
```

## CRUD Operations

Once triggers are enabled, the index automatically maintains itself:

```sql
-- INSERT: New records are automatically indexed
INSERT INTO users (id, email) VALUES (999, 'new@example.com');

-- UPDATE: Index is updated (uses incremental update when possible)
UPDATE users SET email = 'updated@example.com' WHERE id = 1;

-- DELETE: Records are soft-deleted with tombstones (O(1) operation)
DELETE FROM users WHERE id = 2;

-- Queries immediately reflect all changes
SELECT * FROM biscuit_match('%example.com');
```

### Incremental Updates

The extension uses an incremental update optimization when:
- Old and new values have the same length
- String is at least 3 characters
- Fewer than 20% of characters changed
- Maximum 3 characters changed

This provides faster updates for similar strings (e.g., typo corrections, case changes).

### Lazy Deletion

Deletions use tombstone marking for O(1) performance:
1. Record is marked as tombstoned (bitmap operation)
2. Primary key is removed from hash table
3. Slot is added to free list for reuse
4. Actual cleanup happens in batches when threshold is reached

## Performance Considerations

### When to Use BISCUIT

**Good use cases:**

- Frequent wildcard searches on a string column
- Dataset size fits comfortably in available memory
- Pattern matching is a bottleneck in your application
- Mixed CRUD operations with pattern queries

**Not recommended for:**  
- Very large datasets (multi-GB text columns)
- Infrequent pattern matching queries
- Exact match lookups (use regular B-tree index)
- Limited memory environments

### Bulk Operations

For bulk inserts/updates/deletes, temporarily disable triggers:

```sql
SELECT biscuit_disable_triggers();

-- Perform bulk operations
COPY users FROM 'data.csv';
UPDATE users SET email = lower(email);
DELETE FROM users WHERE inactive = true;

-- Rebuild and re-enable
SELECT biscuit_build_index('users', 'email', 'id');
SELECT biscuit_enable_triggers();
```

### Memory Usage

Monitor memory consumption:

```sql
SELECT biscuit_index_status();  -- Shows memory usage
```

Memory usage depends on:
- Number of records
- String length distribution
- Character diversity
- Number of unique lengths

### Tombstone Management

Monitor tombstone accumulation:

```sql
-- Check tombstone count
SELECT biscuit_get_tombstone_count();

-- Manual cleanup if needed
SELECT biscuit_cleanup();
```

Automatic cleanup triggers when tombstone count reaches threshold (default: 1000).

## Architecture

### Index Structure

BISCUIT maintains several bitmap indices:
- **Position index**: Character presence at each position (forward)
- **Negative position index**: Character presence from end (backward)
- **Length index**: Records grouped by string length
- **Character cache**: All positions where each character appears
- **Primary key hash table**: O(1) lookup from PK to index position

### Lazy Deletion

The lazy deletion system:
- Marks deleted records in a tombstone bitmap
- Filters tombstones during query execution
- Batches actual cleanup operations
- Reuses deleted slots for new inserts

### Query Optimization

Pattern matching is optimized through:
- Fast path for simple patterns (single character, prefix, suffix)
- Length filtering to reduce candidate set
- Windowed position matching for complex patterns
- Bitmap intersection/union operations

## Limitations and Caveats

1. **Memory requirement**: Entire index must fit in RAM
2. **Session scope**: Index is not persistent across PostgreSQL restarts
3. **Single column**: One index per column (no composite indices)
4. **No crash recovery**: Index must be rebuilt after crashes
5. **Character limit**: Strings longer than 256 characters are truncated
6. **No case-insensitive**: Pattern matching is case-sensitive
7. **Primary key required**: Must have a unique primary key column

## Troubleshooting

### Index not found

```sql
ERROR: No index found
```

**Solution**: Build the index first:
```sql
SELECT biscuit_setup('table_name', 'column_name', 'pk_column');
```

### Out of memory

```sql
ERROR: out of memory
```

**Solution**: Dataset too large for available memory. Consider:
- Increasing PostgreSQL memory limits
- Indexing a subset of data
- Using partitioned tables

### Slow queries after many deletes

**Solution**: High tombstone count may slow queries. Trigger cleanup:
```sql
SELECT biscuit_cleanup();
```

### Trigger conflicts

**Solution**: If you have other triggers, ensure proper ordering or disable BISCUIT triggers during maintenance windows.


## License

BISCUIT is released under the PostgreSQL License.

## Version

**Current version:** 1.0.6-Biscuit

Use `SELECT biscuit_version();` to check your installed version.

## Summary

BISCUIT makes wildcard queries faster by trading memory and build time for query performance.

It's not magic—it's a bitmap index that:
- Pre-computes character positions
- Uses fast bitmap operations instead of string scanning
- Still requires O(n) time to return n results
- Requires memory proportional to your data size

## Contributors

BISCUIT is developed and maintained by [Sivaprasad Murali](https://linkedin.com/in/sivaprasad-murali) .


## Support and Contact


**Issues:** https://github.com/crystallinecore/biscuit/issues

**Discussions:** https://github.com/crystallinecore/biscuit/discussions

##

**When pg_trgm feels half-baked, grab a BISCUIT 🍪**

---