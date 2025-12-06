# Pattern Syntax Guide

Complete guide to LIKE pattern matching with Biscuit indexes, including optimization strategies for each pattern type.

---

## Wildcard Characters

Biscuit supports standard SQL LIKE wildcards:

| Wildcard | Meaning | Example | Matches |
|----------|---------|---------|---------|
| `%` | Zero or more characters | `'%test%'` | "test", "mytest", "testing" |
| `_` | Exactly one character | `'test_'` | "test1", "testA", not "test" |

---

## Pattern Types

Biscuit classifies patterns into categories for optimization:

### 1. Exact Match (Fastest)

**Pattern**: No wildcards at all

```sql
-- Exact string match
SELECT * FROM products WHERE name LIKE 'Wireless Mouse';
```

**Performance**: 
- **How it works**: Direct bitmap lookup + length filter
- **Use case**: When you know the exact string

**Note**: For exact matches, regular `=` with B-tree may be equally fast. Use Biscuit when mixing exact and wildcard queries.

---

### 2. Prefix Match (Very Fast)

**Pattern**: `'string%'` - Starts with concrete characters, ends with `%`

```sql
-- Find products starting with "Wireless"
SELECT * FROM products WHERE name LIKE 'Wireless%';

-- Email domain filtering
SELECT * FROM users WHERE email LIKE 'admin%';

-- SKU prefix search
SELECT * FROM inventory WHERE sku LIKE 'PROD-2024%';
```

**Performance**:
- **How it works**: Position-based bitmap intersection from start
- **Optimization**: Strong anchor at position 0

**Best Practices**:
```sql
-- ✅ GOOD: Specific prefix
WHERE name LIKE 'Wireless%'

-- ❌ AVOID: Very short prefix (low selectivity)
WHERE name LIKE 'W%'

-- ✅ BETTER: Longer prefix
WHERE name LIKE 'Wireless Mouse%'
```

---

### 3. Suffix Match (Fast)

**Pattern**: `'%string'` - Ends with concrete characters, starts with `%`

```sql
-- Find files with specific extension
SELECT * FROM files WHERE filename LIKE '%.pdf';

-- Domain matching
SELECT * FROM users WHERE email LIKE '%@company.com';

-- Product category suffix
SELECT * FROM products WHERE name LIKE '%Mouse';
```

**Performance**:
- **How it works**: Negative-position bitmap indexing from end
- **Optimization**: Uses suffix bitmaps for direct matching

**Examples**:
```sql
-- Email domain filtering (very efficient)
SELECT COUNT(*) FROM users WHERE email LIKE '%@gmail.com';

-- File extension queries
SELECT * FROM documents WHERE filename LIKE '%.docx';
```

---

### 4. Substring Match (Moderate Speed)

**Pattern**: `'%string%'` - Contains substring anywhere

```sql
-- Find products containing "wireless"
SELECT * FROM products WHERE name LIKE '%wireless%';

-- Description search
SELECT * FROM articles WHERE content LIKE '%important%';

-- Tag searching
SELECT * FROM posts WHERE tags LIKE '%python%';
```

**Performance**:
- **How it works**: Tries all positions from 0 to max_length
- **Optimization**: Early termination when no matches found

**Optimization Tips**:
```sql
-- ✅ GOOD: Specific substring
WHERE description LIKE '%wireless mouse%'

-- ⚠️ SLOWER: Very common substring
WHERE description LIKE '%the%'

-- ✅ COMBINE with other filters
WHERE description LIKE '%wireless%' 
  AND category = 'Electronics'
```

---

### 5. Infix Match (Fast)

**Pattern**: `'prefix%suffix'` - Anchored at both ends

```sql
-- URL path matching
SELECT * FROM urls WHERE path LIKE '/api/%/users';

-- Log filtering
SELECT * FROM logs WHERE message LIKE 'ERROR%timeout';

-- Pattern with both anchors
SELECT * FROM products WHERE name LIKE 'Wireless%Mouse';
```

**Performance**:
- **How it works**: Intersects prefix and suffix bitmaps
- **Optimization**: Both anchors provide strong filtering

**Why it's fast**:
```sql
-- Matches must:
-- 1. Start with "Wireless" (position 0)
-- 2. End with "Mouse" (using negative positions)
-- 3. Have length >= len("Wireless") + len("Mouse")
WHERE name LIKE 'Wireless%Mouse'
```

---

### 6. Complex Multi-Part Patterns

**Pattern**: Multiple `%` separators with concrete parts

```sql
-- Multiple substrings
SELECT * FROM products 
WHERE name LIKE '%Wireless%RGB%Gaming%';

-- Log pattern matching
SELECT * FROM logs 
WHERE message LIKE 'ERROR%connection%timeout%retry';

-- Multiple conditions
SELECT * FROM users 
WHERE email LIKE '%admin%@%company%';
```

**Performance**:
- **How it works**: Recursive windowed matching algorithm
- **Optimization**: Each part must appear in sequence

**Execution Strategy**:
```
Pattern: '%word1%word2%word3%'

Steps:
1. Find all positions where "word1" appears
2. For each match, find "word2" after it
3. For each of those, find "word3" after it
4. Return only complete matches
```

---

### 7. Underscore Patterns (Position-Specific)

**Pattern**: Using `_` for single-character wildcards

```sql
-- Exactly 4 characters starting with "test"
SELECT * FROM codes WHERE code LIKE 'test____';

-- Pattern with specific positions
SELECT * FROM products WHERE sku LIKE 'PROD-___-2024';

-- First character wildcard
SELECT * FROM words WHERE word LIKE '_ouse';

-- Mixed wildcards
SELECT * FROM data WHERE value LIKE 'A_C%';
```

**Performance**:
- **How it works**: Each `_` constrains exact length and positions
- **Key insight**: `_` is NOT skipped - it's a position constraint!

**Important Distinction**:
```sql
-- These are DIFFERENT queries:

-- Match any 8-character string
WHERE code LIKE '________'  -- Exact length filter

-- Match any string with length >= 0
WHERE code LIKE '%%'  -- All non-null strings

-- Match 5+ character strings starting with "test"
WHERE code LIKE 'test_%'  -- Length >= 5, prefix "test"
```

**Optimization Examples**:
```sql
-- ✅ FAST: Exact length known
WHERE code LIKE '____'  -- Exactly 4 characters

-- ✅ FAST: Position constraints
WHERE sku LIKE 'A___-___'  -- Specific format

-- ⚠️ SLOWER: Many underscores without anchors
WHERE value LIKE '%_____%'  -- 5+ chars substring
```

---

## Pure Wildcard Patterns (Fastest Special Cases)

Biscuit has special optimizations for patterns containing only wildcards:

### Empty Pattern
```sql
SELECT * FROM products WHERE name LIKE '';
-- Optimized to: length_bitmaps[0]
-- Returns: Only empty strings
```

### Single Percent
```sql
SELECT * FROM products WHERE name LIKE '%';
-- Optimized to: All non-tombstoned records
-- Returns: Everything (except NULLs)
```

### Pure Underscores
```sql
SELECT * FROM codes WHERE code LIKE '____';
-- Optimized to: length_bitmaps[4]
-- Returns: Exactly 4-character strings
```

### Mixed Pure Wildcards
```sql
-- Pattern: '%%%___%%'
-- Has: 5× '%' and 3× '_'
-- Optimized to: length_ge_bitmaps[3]
-- Returns: Strings with length >= 3

SELECT * FROM data WHERE value LIKE '%_%_%_%';
-- Equivalent to: WHERE LENGTH(value) >= 3
```


---

## Pattern Optimization Strategies

### Strategy 1: Maximize Concrete Characters

```sql
-- ❌ SLOW: Too generic
WHERE name LIKE '%a%'

-- ✅ FAST: More specific
WHERE name LIKE '%wireless%'

-- ✅✅ FASTER: Very specific
WHERE name LIKE '%wireless mouse pro%'
```

**Rule**: More concrete characters = better selectivity = faster query

---

### Strategy 2: Use Strong Anchors

```sql
-- ❌ SLOWER: No anchors
WHERE name LIKE '%mouse%'

-- ✅ FASTER: Prefix anchor
WHERE name LIKE 'Wireless%'

-- ✅✅ FASTEST: Both anchors
WHERE name LIKE 'Wireless%Mouse'
```

**Anchor Strength**:
1. Both prefix + suffix: ⭐⭐⭐⭐⭐
2. Prefix only: ⭐⭐⭐⭐
3. Suffix only: ⭐⭐⭐⭐
4. No anchors: ⭐⭐

---

### Strategy 3: Minimize Partitions

```sql
-- ❌ COMPLEX: 4 partitions
WHERE message LIKE '%ERROR%failed%retry%timeout%'

-- ✅ SIMPLER: 2 partitions
WHERE message LIKE '%ERROR%failed%'

-- ✅✅ SIMPLEST: 1 partition
WHERE message LIKE '%ERROR failed%'
```

**Rule**: Fewer `%` separators = faster matching

---

### Strategy 4: Combine with Other Indexes

```sql
-- Use Biscuit with other filters
SELECT * FROM products 
WHERE category = 'Electronics'  -- B-tree index
  AND name LIKE '%Mouse%';      -- Biscuit index

-- PostgreSQL combines both indexes efficiently
```

---

## Pattern Examples by Use Case

### Email Filtering
```sql
-- Domain-specific emails
WHERE email LIKE '%@company.com'      -- Suffix (fast)

-- Admin emails
WHERE email LIKE 'admin%'             -- Prefix (fast)

-- Specific pattern
WHERE email LIKE 'admin%@company.com' -- Infix (very fast)
```

### SKU/Product Code Matching
```sql
-- Year-based SKUs
WHERE sku LIKE 'PROD-2024%'           -- Prefix (fast)

-- Category codes
WHERE sku LIKE '%ELEC%'               -- Substring (moderate)

-- Format matching
WHERE sku LIKE '____-____-____'       -- Underscores (fast)
```

### Log Searching
```sql
-- Error messages
WHERE message LIKE 'ERROR:%'          -- Prefix (fast)

-- Specific errors
WHERE message LIKE '%timeout%'        -- Substring (moderate)

-- Complex patterns
WHERE message LIKE 'ERROR%connection%timeout%'  -- Multi-part
```

### URL/Path Matching
```sql
-- API routes
WHERE path LIKE '/api/%'              -- Prefix (fast)

-- Specific endpoints
WHERE path LIKE '/api/%/users'        -- Infix (fast)

-- File extensions
WHERE path LIKE '%.jpg'               -- Suffix (fast)
```

---

## Case Sensitivity

LIKE is case-sensitive by default. For case-insensitive matching:

```sql
-- Create index on lowercase version
CREATE INDEX idx_name_lower ON products 
USING biscuit (LOWER(name));

-- Query with lowercase
SELECT * FROM products 
WHERE LOWER(name) LIKE '%wireless%';
```

**Alternative**: Use ILIKE (but requires sequential scan):
```sql
-- No Biscuit optimization
SELECT * FROM products WHERE name ILIKE '%wireless%';
```

---

## Pattern Performance Hierarchy

From fastest to slowest:

1. ⚡⚡⚡⚡⚡ Pure wildcards (length-based)
2. ⚡⚡⚡⚡⚡ Exact match
3. ⚡⚡⚡⚡ Prefix patterns
4. ⚡⚡⚡⚡ Suffix patterns
5. ⚡⚡⚡⚡ Infix patterns (both anchors)
6. ⚡⚡⚡⚡ Underscore patterns
7. ⚡⚡⚡ Substring patterns
8. ⚡⚡ Complex multi-part patterns

---

## Next Steps

- Learn about [Multi-Column Indexes](multicolumn.md)
-  Explore [Performance Tuning](performance.md)
-  Understand the [Architecture](architecture.md)
-  Check the [FAQ](faq.md)