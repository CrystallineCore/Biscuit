# Tribute to Trigrams and Trees

## Acknowledging PostgreSQL's Pattern Matching Heritage

Biscuit exists because PostgreSQL already has **exceptional** pattern matching capabilities. We didn't invent pattern matching for databases—we're building on decades of research and production-hardened implementations. This section honors the tools that inspired Biscuit and explains where they still reign supreme.

---

## pg_trgm: The Swiss Army Knife of Text Search

### What pg_trgm Does Brilliantly

The **trigram (pg_trgm) GIN index** is PostgreSQL's workhorse for fuzzy text matching, and for good reason:

#### 1. **Fuzzy Matching & Similarity Search**
```sql
-- Find similar strings (Levenshtein-style matching)
SELECT * FROM products 
WHERE name % 'iPone';  -- Matches "iPhone" despite typo

-- Similarity scoring
SELECT name, similarity(name, 'PostgreSQL') as score
FROM databases
ORDER BY score DESC;
```

**Biscuit cannot do this.** We only support exact pattern matching with `LIKE`/`ILIKE` wildcards. There's no fuzzy matching, no edit distance, no similarity scoring.

#### 2. **Full-Text Search Integration**
```sql
-- Complex text search queries
SELECT * FROM articles
WHERE to_tsvector('english', body) @@ to_tsquery('postgres & performance');

-- Works with GIN indices on tsvector
CREATE INDEX ON articles USING GIN(to_tsvector('english', body));
```

**PostgreSQL’s text search ecosystem shines here:**
- Stemming (running → run)
- Stop word removal (the, a, an)
- Language-aware tokenization
- Boolean operators (AND, OR, NOT)
- Phrase matching

**Biscuit** has none of this. We're strictly character-level pattern matching.

#### 3. **Regular Expression Support**
```sql
-- Regex matching with index support
SELECT * FROM logs 
WHERE message ~ '^ERROR.*database.*connection';

-- GIN trigram index can accelerate regex queries
CREATE INDEX ON logs USING GIN(message gin_trgm_ops);
```

**pg_trgm can index many regex patterns.** Biscuit cannot support regex at all—our position-based indexing breaks down with complex regex operators like `*`, `+`, `{n,m}`, lookaheads, etc.

#### 4. **Persistent Storage**
```sql
-- pg_trgm index is written to disk
-- No rebuild needed after cache eviction or restart
\di+ articles_content_idx
```

**Critical advantage:** pg_trgm indices are **persistent**. They survive:
- PostgreSQL restarts
- Cache evictions
- Relation cache invalidations

**Biscuit's Achilles' Heel:**
```
⚠️  WARNING: Biscuit indices are IN-MEMORY ONLY
    
    - Stored in PostgreSQL's shared cache buffers
    - Rebuilt from heap on EVERY cache invalidation
    - Rebuild triggers:
      * First query after server restart
      * Cache pressure evictions
      * Schema changes (ALTER TABLE)
      * Extension updates
      * Manual cache clearing
    
    For large tables (10M+ rows), rebuild can take:
    - 10-30 seconds for 10M rows
    - 1-5 minutes for 100M rows
    depending on the hardware and other configurations.

    This can be unacceptable for latency-sensitive applications.
```

### When to Use pg_trgm Instead of Biscuit

| Use Case | Use pg_trgm | Why |
|----------|-------------|-----|
| **Fuzzy matching** | ✅ Always | Biscuit: exact patterns only |
| **Typo tolerance** | ✅ Always | `'iPone' % 'iPhone'` works |
| **Full-text search** | ✅ Always | Biscuit: no stemming, no ranking |
| **Regex patterns** | ✅ Always | Biscuit: no regex support |
| **Very long strings** | ✅ Preferred | Biscuit: memory scales with length |

---

## B-tree: The Foundation of Database Indexing

### What B-tree Does Brilliantly

The B-tree is the **most battle-tested index structure** in database history (50+ years). It's PostgreSQL's default for good reason.

#### 1. **Exact Equality & Range Queries**
```sql
-- B-tree excels at these:
SELECT * FROM users WHERE email = 'user@example.com';  -- O(log n)
SELECT * FROM orders WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31';
SELECT * FROM products WHERE price > 100 AND price < 500;
```

Biscuit is not built to address these.

#### 2. **Sorted Data Access**
```sql
-- B-tree provides sorted results for free
SELECT * FROM products ORDER BY name LIMIT 10;  -- No sort needed!

-- Index-only scan when possible
SELECT COUNT(*) FROM users WHERE created_at > '2024-01-01';
```

**Biscuit cannot do this:**
- Must sort TIDs after collection: (uses Radix sort for large volumes)
- No index-only scans (data cached separately)

#### 3. **Space Efficiency**

The benchmarks conducted on a 1M row dataset showed that BTrees consumed 43 MB disk space for multi-columnar indexing, whereas Biscuit consumed ~900 MBs for the same.

#### 4. **Universal Compatibility**
```sql
-- B-tree works on ANY orderable type:
CREATE INDEX ON events(user_id);        -- integers
CREATE INDEX ON orders(total_price);    -- numerics
CREATE INDEX ON logs(timestamp);        -- timestamps
CREATE INDEX ON users(email);           -- text
```

**Biscuit only works on text-like data.** We convert everything to strings, which:
- Wastes space for integers (20 bytes vs 4 bytes)
- Slower for numeric comparisons
- No native support for complex types

#### 5. **Lock-Free Concurrent Access**
```sql
-- B-tree: MVCC-friendly, high concurrency
-- 1000s of concurrent readers + writers, no problem
```

**Biscuit's concurrency limitations:**
```c
// INSERT requires exclusive lock on entire index
LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
// All queries blocked during insert!

// This is acceptable for read-heavy workloads
// Unacceptable for high-write scenarios
```

### When to Use B-tree Instead of Biscuit

| Use Case | Use B-tree | Why |
|----------|------------|-----|
| **Equality queries** | ✅ Almost always | Simpler than Biscuit |
| **Range queries** | ✅ Always | Biscuit: no range support |
| **Sorted access** | ✅ Always | Biscuit: must sort results |
| **Numeric data** | ✅ Preferred | Biscuit: wasteful string conversion |
| **High write load** | ✅ Preferred | Biscuit: exclusive locks on insert |
| **Space-constrained** | ✅ Always | Biscuit: Often larger |
| **Small strings** | ✅ Preferred | B-tree overhead acceptable |

---

## The Complementary Index Strategy

**The Best Approach: Use Multiple Indices**

```sql
-- Use each index for what it does best:

CREATE INDEX users_email_btree ON users(email);           -- Equality
CREATE INDEX users_name_trgm ON users USING GIN(name gin_trgm_ops);  -- Fuzzy
CREATE INDEX users_bio_biscuit ON users USING biscuit(bio);  -- Pattern matching

-- Queries automatically use the best index:

-- B-tree wins:
SELECT * FROM users WHERE email = 'exact@match.com';

-- pg_trgm wins:
SELECT * FROM users WHERE name % 'Jhon Doe';  -- Typo tolerance

-- Biscuit wins:
SELECT * FROM users WHERE bio LIKE '%worked at Google%software engineer%';
```

---

##  Honest Trade-off Summary

### Biscuit's Strengths

- **Pattern matching**: Faster  than seq scan  
- **Case-insensitive**: No `lower()` overhead  
- **Multi-column patterns**: Intelligent reordering  
- **Anchored patterns**: Prefix/suffix with dual indexing  

### Biscuit's Weaknesses

- **Memory hog**: Larger than B-tree  
- **Cache-dependent**: Rebuilds on invalidation (2-300s for large tables)  
- **No fuzzy matching**: Can't handle typos  
- **No regex**: Limited to LIKE wildcards  
- **Poor for equality**: 100x slower than B-tree  
- **Write-unfriendly**: Exclusive locks on insert  
- **No persistence**: In-memory only, lost on restart  

### When NOT to Use Biscuit

🚫 **Production systems with:**
- Strict latency SLAs (<10ms)
- Frequent server restarts
- Limited memory (<4GB per index)
- High write throughput (>1k inserts/sec)
- Primarily equality/range queries

🚫 **Workloads requiring:**
- Fuzzy matching or similarity search
- Regular expressions
- Full-text search with ranking
- Sub-millisecond equality lookups
- 24/7 uptime with zero rebuild latency

---

##  Acknowledgments

> Any shortcomings described here are Biscuit’s design trade-offs, not deficiencies in PostgreSQL’s core index implementations.

Biscuit stands on the shoulders of giants:

- **PostgreSQL Core Team**: For the extensible index AM framework
- **pg_trgm Contributors**: For proving text indexing can be fast
- **B-tree Pioneers**: 50+ years of foundational research (Bayer & McCreight, 1972)
- **Roaring Bitmap Authors**: For making compressed bitmaps practical (Lemire et al.)

**We are not replacing these tools. We are complementing them.**

---

## Recommendation Matrix

| Your Need | Recommended Index | Why |
|-----------|------------------|-----|
| `WHERE col = 'exact'` | **B-tree** | Simpler for this case |
| `WHERE col LIKE 'prefix%'` | **B-tree** | Good enough, persistent |
| `WHERE col LIKE '%suffix'` | **Biscuit** | B-tree can't help |
| `WHERE col LIKE '%middle%'` | **Biscuit** or **pg_trgm** | Biscuit if memory available |
| `WHERE col ~ 'regex'` | **pg_trgm (GIN)** | Biscuit: no regex |
| `WHERE col % 'similar'` | **pg_trgm (GIN)** | Biscuit: no fuzzy |
| Multi-column patterns | **Biscuit** | Unique strength |
| Case-insensitive | **Biscuit** | Avoids lower() |
| Production 24/7 | **pg_trgm** | Persistent, no rebuilds |
| Limited memory | **B-tree** or **pg_trgm** | Biscuit too large |

---

## Summary

**There is no "best" index. Only the right index for your workload.**

- **B-tree**: The reliable workhorse 
- **pg_trgm**: The fuzzy matcher 
- **Biscuit**: The pattern specialist 

**Choose wisely. Benchmark honestly. Respect the classics.**

---

*"If I have seen further, it is by standing on the shoulders of giants."*  
— Isaac Newton

*"If we have indexed faster, it is by studying B-trees and trigrams first."*  
— Biscuit Team