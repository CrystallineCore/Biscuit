# Frequently Asked Questions

Common questions and answers about Biscuit index for PostgreSQL.

---

## General Questions

### What is Biscuit?

Biscuit is a PostgreSQL index access method designed for LIKE pattern matching queries. It uses bitmap-based indexing to accelerate searches with wildcards (`%`, `_`). Performance improvements vary depending on your data characteristics, query patterns, and system configuration.

---

### When should I use Biscuit?

Consider Biscuit when you:
- ✅ Run frequent LIKE queries with wildcards
- ✅ Need to search across multiple text columns
- ✅ Have high-cardinality string columns
- ✅ Want to optimize queries with pattern matching
- ✅ Need prefix, suffix, or substring matching

**Other options may be better for**:
- Full-text search with stemming/ranking (use tsvector/GIN)
- Very long strings (>256 chars are truncated)
- Columns with very few distinct values


---

### Is Biscuit production-ready?

Biscuit includes:
- Full CRUD support (INSERT, UPDATE, DELETE)
- Automatic index maintenance
- Multi-column indexes
- Multiple performance optimizations

However, as with any index type, we recommend:
1. Test thoroughly in your staging environment
2. Benchmark with your actual data and queries
3. Monitor performance metrics
4. Have a rollback plan
5. Start with non-critical workloads



---

### How much memory does Biscuit use?

Memory usage depends on:
- Number of records
- Average string length
- Number of indexed columns
- Character distribution in your data

The index is stored in memory for performance. For large tables, ensure you have adequate RAM. Test with your actual data to determine memory requirements.

**Monitor with**:
```sql
SELECT pg_size_pretty(pg_relation_size('idx_name'));
```

---

## Installation & Setup

### How do I install Biscuit?

See the [Installation Guide](installation.md) for detailed instructions.

Quick steps:
```bash
# Ubuntu/Debian
sudo apt-get install postgresql-server-dev-14 build-essential
git clone https://github.com/crystallinecore/biscuit.git
cd biscuit
make
sudo make install
```

Then in PostgreSQL:
```sql
CREATE EXTENSION biscuit;
```

---

### Installation fails with "could not load library"

**Problem**: PostgreSQL can't find `biscuit.so`

**Solution**:
```bash
# Find PostgreSQL library directory
pg_config --pkglibdir

# Verify file exists
ls $(pg_config --pkglibdir)/biscuit.so

# If missing, reinstall
cd biscuit
sudo make install
```

---

### Extension creation fails: "extension does not exist"

**Problem**: Extension control files not in correct location

**Solution**:
```bash
# Check extension directory
pg_config --sharedir

# Verify files
ls $(pg_config --sharedir)/extension/biscuit*

# Should see:
# biscuit.control
# biscuit--1.0.sql

# If missing, reinstall
sudo make install
```

---

### Should I install with CRoaring support?

**Yes, recommended** for production use.

CRoaring provides:
- Better memory compression
- More efficient set operations

Install:
```bash
# Ubuntu/Debian
sudo apt-get install libroaring-dev

# Then build Biscuit
make HAVE_ROARING=1
sudo make install
```

Without CRoaring, Biscuit uses a fallback implementation that's slower but still functional.

---

## Index Creation

### How do I create a Biscuit index?

**Single column**:
```sql
CREATE INDEX idx_name ON table_name 
USING biscuit (column_name);
```

**Multiple columns**:
```sql
CREATE INDEX idx_multi ON table_name 
USING biscuit (col1, col2, col3);
```

See [Quick Start Tutorial](quickstart.md) for examples.

---

### Can I use Biscuit on non-text columns?

**Yes!** Biscuit supports automatic type conversion for:
- Integers (SMALLINT, INTEGER, BIGINT)
- Floats (REAL, DOUBLE PRECISION)
- Dates (DATE, TIMESTAMP, TIMESTAMPTZ)
- Booleans (BOOLEAN)

**Example**:
```sql
-- Index a date column
CREATE INDEX idx_created ON orders USING biscuit (created_at);

-- Query by year-month
SELECT * FROM orders WHERE created_at::TEXT LIKE '2024-01%';
```

See [API Reference - Data Types](api.md#data-types) for details.

---

### How long does index creation take?

Build time varies significantly based on:
- Table size
- Number of columns
- Average string length
- Available memory
- System load

**Benchmark with your data** to determine actual build times.

**For large tables**, use CONCURRENTLY:
```sql
CREATE INDEX CONCURRENTLY idx_name ON table_name 
USING biscuit (column_name);
```

---

### Can I create partial Biscuit indexes?

**Yes!** Use a WHERE clause:

```sql
-- Index only active users
CREATE INDEX idx_active_users ON users 
USING biscuit (username, email)
WHERE status = 'active';

-- Index only recent orders
CREATE INDEX idx_recent_orders ON orders 
USING biscuit (customer_name)
WHERE created_at > '2024-01-01';
```

**Benefits**:
- Smaller index size
- Faster queries on filtered data
- Reduced maintenance overhead

---

### How many columns can I index?

While there's no hard limit enforced, indexing many columns increases:
- Index size
- Build time
- Memory usage
- Maintenance overhead

**Recommendation**: Index only columns you frequently query with LIKE patterns.

**Test performance** with your specific column combinations.

---

## Query Performance

### Why is my query not using the index?

**Check with EXPLAIN**:
```sql
EXPLAIN ANALYZE
SELECT * FROM products WHERE name LIKE '%laptop%';
```

**Common causes**:

1. **Statistics out of date**
   ```sql
   ANALYZE products;
   ```

2. **Pattern matches too many rows**
   ```sql
   -- Check selectivity
   SELECT COUNT(*) FROM table WHERE col LIKE '%pattern%';
   ```

3. **Wrong column indexed**
   ```sql
   -- Check which columns are indexed
   \d products
   ```

4. **Planner estimates sequential scan is faster**
   ```sql
   -- Test with index forced (testing only!)
   SET enable_seqscan = off;
   ```

Always verify index usage with EXPLAIN ANALYZE before and after creating indexes.

---

### How can I measure query performance?

**Use EXPLAIN ANALYZE**:
```sql
EXPLAIN ANALYZE
SELECT * FROM products WHERE name LIKE '%pattern%';
```

Look for:
- Execution time
- Index scan vs sequential scan
- Rows processed
- Planning time

**Compare before and after** creating the index to measure improvement.

Performance varies based on:
- Data characteristics
- Pattern selectivity
- System resources
- Concurrent load

Always benchmark with your actual workload.

---

### Does Biscuit support case-insensitive search?

**Not directly**, but you can use LOWER():

```sql
-- Create index on lowercase
CREATE INDEX idx_name_lower ON products 
USING biscuit (LOWER(name));

-- Query with lowercase
SELECT * FROM products 
WHERE LOWER(name) LIKE '%wireless%';
```

---

### Can I use OR conditions with Biscuit?

**Yes**, but be aware of performance:

```sql
-- Uses Biscuit (efficient)
SELECT * FROM products 
WHERE name LIKE '%laptop%'
   OR name LIKE '%desktop%';

-- Better: Combine patterns when possible
SELECT * FROM products 
WHERE name LIKE '%laptop%' 
   OR name LIKE '%desktop%'
   OR name LIKE '%tablet%';
```

PostgreSQL will use bitmap OR to combine results.

---

### Does Biscuit support regular expressions?

**No.** Biscuit only supports SQL LIKE patterns (`%`, `_`).

For regex, use:
- PostgreSQL's `~` operator with GIN trigram index
- Full-text search with tsvector

**Example**:
```sql
-- Regex (not Biscuit)
SELECT * FROM products WHERE name ~ 'laptop|desktop';

-- LIKE (uses Biscuit)
SELECT * FROM products WHERE name LIKE '%laptop%';
```

---

### Does Biscuit optimize COUNT(*) queries?

Biscuit includes optimizations for aggregate queries like COUNT(*):

```sql
-- May benefit from aggregate optimizations
SELECT COUNT(*) FROM products WHERE name LIKE '%laptop%';
```

The index can process bitmap operations without fetching tuples, which can improve performance for certain query patterns.

**Test with your queries** to measure the actual benefit.

---

### Can I use LIMIT with Biscuit?

**Yes**. Biscuit works with LIMIT clauses:

```sql
SELECT * FROM products 
WHERE name LIKE '%gaming%'
LIMIT 10;
```

The index includes optimizations for queries with LIMIT, though the actual performance benefit depends on your data and query patterns.

---

## Index Maintenance

### Do I need to manually maintain the index?

**No.** Biscuit automatically maintains itself:
- INSERT: Adds new records
- UPDATE: Removes old, adds new
- DELETE: Marks as tombstone

PostgreSQL's VACUUM handles cleanup.

---

### What are tombstones?

**Tombstones** are deleted records tracked in memory until cleanup.

Check tombstone count:
```sql
SELECT biscuit_index_stats('idx_name'::regclass);
```

Output:
```
Tombstones: 245
```

**Automatic cleanup** triggers at 1000 tombstones.

**Manual cleanup**:
```sql
VACUUM table_name;
-- or
REINDEX INDEX idx_name;
```

---

### When should I rebuild the index?

Consider rebuilding when:
- ✅ Significant data changes have occurred
- ✅ Index statistics show high tombstone counts
- ✅ Query performance has degraded
- ✅ After major PostgreSQL version upgrades

**How to rebuild**:
```sql
-- Non-blocking rebuild
CREATE INDEX CONCURRENTLY idx_new ON table_name 
USING biscuit (column_name);

DROP INDEX idx_old;
ALTER INDEX idx_new RENAME TO idx_old;
```

Monitor index health regularly to determine when rebuilding is beneficial.

---

### How do I monitor index health?

**Use the diagnostic function**:
```sql
SELECT biscuit_index_stats('idx_name'::regclass);
```

**Key metrics provided**:
- **Active records**: Current valid records
- **Total slots**: Allocated memory slots
- **Free slots**: Reusable slots from deletes
- **Tombstones**: Deleted but not cleaned records
- **Max length**: Longest indexed string
- **CRUD statistics**: Insert/update/delete counts

**Create monitoring view**:
```sql
CREATE VIEW biscuit_health AS
SELECT 
    indexrelid::regclass as index_name,
    biscuit_index_stats(indexrelid) as stats
FROM pg_index
WHERE indexrelid::regclass::text LIKE '%biscuit%';
```

Monitor these metrics over time to understand index behavior and maintenance needs.

---

### Does Biscuit support parallel operations?

Biscuit includes support for parallel bitmap scans during query execution. Parallel index building is not currently implemented.

Whether parallel queries are used depends on PostgreSQL's query planner and your configuration settings.

---

## Troubleshooting

### Queries are slower than expected

**Debug checklist**:

1. **Verify index is being used**
   ```sql
   EXPLAIN ANALYZE SELECT * FROM table WHERE col LIKE '%pattern%';
   -- Should show: Index Scan using idx_name
   ```

2. **Check pattern selectivity**
   ```sql
   -- How many rows match?
   SELECT COUNT(*) FROM table WHERE col LIKE '%pattern%';
   -- High match percentage may make index less beneficial
   ```

3. **Update statistics**
   ```sql
   ANALYZE table_name;
   ```

4. **Check index health**
   ```sql
   SELECT biscuit_index_stats('idx_name'::regclass);
   ```

5. **Review memory settings**
   ```sql
   SHOW work_mem;
   SHOW shared_buffers;
   -- See [Performance Tuning](performance.md)
   ```

6. **Compare with baseline**
   - Measure query time without the index
   - Document before/after performance
   - Consider data characteristics

---

### Index build fails with out of memory

**Problem**: Not enough RAM for index construction

**Solutions**:

1. **Increase maintenance_work_mem**
   ```sql
   SET maintenance_work_mem = '2GB';
   CREATE INDEX ...
   ```

2. **Build during low-traffic period**

3. **Consider partial index**
   ```sql
   -- Index subset of data
   CREATE INDEX idx_recent ON table 
   USING biscuit (col)
   WHERE created_at > '2024-01-01';
   ```

4. **Add more RAM to server**

---

### Crashes or unexpected restarts

**Possible causes**:

1. **Memory corruption**: Ensure CRoaring is properly installed
2. **PostgreSQL crash**: Check PostgreSQL logs
3. **Extension bug**: Report at GitHub Issues

**Recovery**:
```sql
-- Drop and rebuild index
DROP INDEX idx_name;
CREATE INDEX idx_name ON table USING biscuit (col);

-- Or reindex
REINDEX INDEX idx_name;
```

---

### "Index is not valid" error

**Cause**: Index build failed or was interrupted

**Solution**:
```sql
-- Drop invalid index
DROP INDEX idx_name;

-- Rebuild
CREATE INDEX CONCURRENTLY idx_name ON table 
USING biscuit (col);
```

---

### Pattern not matching expected rows

**Check pattern syntax**:
```sql
-- Case-sensitive!
SELECT * FROM products WHERE name LIKE '%Mouse%';  -- Matches "Mouse"
SELECT * FROM products WHERE name LIKE '%mouse%';  -- Matches "mouse"

-- Underscore is single-character wildcard
SELECT * FROM products WHERE sku LIKE 'PROD-___';  -- Exactly 3 chars
```

**Test without index**:
```sql
SET enable_indexscan = off;
SELECT * FROM products WHERE name LIKE '%pattern%';
-- Compare results
```

---

## Advanced Topics

### Can I use Biscuit with partitioned tables?

**Yes!** Create indexes on each partition:

```sql
-- Create partitioned table
CREATE TABLE orders (
    id SERIAL,
    customer_name TEXT,
    created_at DATE
) PARTITION BY RANGE (created_at);

-- Create partitions
CREATE TABLE orders_2024_q1 PARTITION OF orders
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

-- Create Biscuit index on partition
CREATE INDEX idx_orders_2024_q1 ON orders_2024_q1
USING biscuit (customer_name);
```

**Note**: Each partition needs its own index.

---

### Does Biscuit work with replication?

**Yes.** Biscuit indexes work with:
- ✅ Streaming replication
- ✅ Logical replication
- ✅ Hot standby

**Caveat**: Indexes are built independently on each server.

---

### Can I use Biscuit in read replicas?

**Yes**, but indexes must be built separately:

```sql
-- On primary
CREATE INDEX idx_name ON table USING biscuit (col);

-- On replica (after replication catches up)
-- Index is automatically created from WAL
-- Or rebuild if needed:
REINDEX INDEX idx_name;
```

---

### How does Biscuit handle NULL values?

**NULL values are not indexed.**

```sql
-- Create index
CREATE INDEX idx_name ON users USING biscuit (middle_name);

-- This uses index
SELECT * FROM users WHERE middle_name LIKE '%Smith%';

-- This does NOT use index (NULL check)
SELECT * FROM users WHERE middle_name IS NULL;
```

**For NULL queries**, add separate condition:
```sql
WHERE middle_name IS NULL
   OR middle_name LIKE '%pattern%'
```

---

### Can I use Biscuit with JSON columns?

**Yes**, with expression index:

```sql
-- Index a JSON field
CREATE INDEX idx_json_name ON products 
USING biscuit ((data->>'name'));

-- Query
SELECT * FROM products 
WHERE (data->>'name') LIKE '%laptop%';
```

---

### Does Biscuit support multi-byte characters (Unicode)?

**Yes**, UTF-8 is fully supported:

```sql
-- Japanese, Chinese, Arabic, emoji, etc.
CREATE INDEX idx_name ON products USING biscuit (name);

-- Query with multi-byte characters
SELECT * FROM products WHERE name LIKE '%日本%';
```

**Note**: Each multi-byte character counts toward the 256 character limit.

---

## Migration & Compatibility

### How do I migrate from GIN trigram?

**Evaluate first**:

```sql
-- 1. Create Biscuit index alongside GIN
CREATE INDEX idx_biscuit ON table USING biscuit (col);

-- 2. Test queries with both
EXPLAIN ANALYZE SELECT * FROM table WHERE col LIKE '%pattern%';

-- 3. Compare performance with your actual queries
-- Document query times, index sizes, and maintenance costs

-- 4. If Biscuit meets your needs better, consider transition
DROP INDEX idx_gin_trigram;

-- 5. Keep Biscuit
```

Different index types have different strengths. Choose based on your specific workload requirements.

---

### Can I have both B-tree and Biscuit on same column?

**Yes**, and it's often beneficial:

```sql
-- B-tree for exact matches and ORDER BY
CREATE INDEX idx_name_btree ON products (name);

-- Biscuit for LIKE patterns
CREATE INDEX idx_name_biscuit ON products USING biscuit (name);
```

PostgreSQL will choose the appropriate index automatically.

---

### How do I uninstall Biscuit?

```sql
-- 1. Drop all Biscuit indexes
DROP INDEX idx1, idx2, idx3;

-- 2. Drop extension (drops any remaining indexes)
DROP EXTENSION biscuit CASCADE;
```

```bash
# 3. Remove files
sudo rm $(pg_config --pkglibdir)/biscuit.so
sudo rm $(pg_config --sharedir)/extension/biscuit*
```

---

## Getting Help

### Where can I get support?

-  **Documentation**: [ReadTheDocs](https://biscuit.readthedocs.io)
-  **Discussions**: [GitHub Discussions](https://github.com/crystallinecore/biscuit/discussions)
-  **Bug Reports**: [GitHub Issues](https://github.com/crystallinecore/biscuit/issues)
-  **Email**: sivaprasad.off@gmail.com

---

### How do I report a bug?

**Include in your report**:
1. PostgreSQL version: `SELECT version();`
2. Biscuit version: `SELECT * FROM pg_extension WHERE extname = 'biscuit';`
3. Table schema: `\d table_name`
4. Index definition: `\d+ index_name`
5. Query that fails: `EXPLAIN ANALYZE ...`
6. Error messages from PostgreSQL logs

**Submit at**: [GitHub Issues](https://github.com/crystallinecore/biscuit/issues)

---

### How can I contribute?

We welcome contributions!

- **Code**: Submit pull requests
-  **Documentation**: Improve docs
-  **Testing**: Test with your data
-  **Ideas**: Suggest features
-  **Bugs**: Report issues

---

### Is there a Slack/Discord community?

**Not yet!** 

---

## Roadmap

### What features are being considered?

Future development may include:
- Enhanced parallel index builds
- Case-insensitive indexing support
- Additional compression options
- Extended monitoring capabilities

**Long-term possibilities**:
- Persistent bitmap storage options
- Approximate matching capabilities
- Additional pattern matching features
- Enhanced cloud optimizations

Development priorities are based on community feedback and usage patterns. Features listed here are under consideration but not guaranteed for any specific timeline.

---

## Performance Comparisons

### How does Biscuit compare to other indexes?

Performance characteristics vary significantly based on:
- Data distribution
- Query patterns
- Hardware specifications
- PostgreSQL configuration
- Concurrent workload

**General considerations**:

**B-tree**:
- Excellent for exact matches and range queries
- Good for prefix patterns with concrete characters
- Not suitable for suffix or substring patterns

**GIN (trigram)**:
- Designed for full-text and trigram matching
- Larger index size
- Different optimization characteristics

**Biscuit**:
- Optimized for LIKE pattern matching
- Bitmap-based approach
- In-memory index structures
- Multi-column query optimization

**Always benchmark with your specific workload** to make informed decisions. Test with:
- Your actual data
- Representative queries
- Expected query volume
- Production-like hardware

---

## Still Have Questions?

Can't find your answer? 

- Check the [Installation Guide](installation.md)
- Read the [Quick Start](quickstart.md)
- Browse [Performance Tuning](performance.md)
- Contact us: sivaprasad.off@gmail.com

---

**Happy pattern matching! 🚀**

