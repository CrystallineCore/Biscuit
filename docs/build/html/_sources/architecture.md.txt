# Biscuit Index Architecture

## Overview

Biscuit is a PostgreSQL index access method (AM) optimized for pattern matching operations (`LIKE`, `ILIKE`, `NOT LIKE`, `NOT ILIKE`). It uses compressed bitmap indices with character-position indexing to achieve faster query performance on pattern matching workloads.

## Core Design Principles

### 1. **Position-Based Character Indexing**
Instead of traditional B-tree or GIN approaches, Biscuit maintains bitmaps for each character at each position:

```
String: "hello"
Position 0: 'h' → bitmap of record IDs
Position 1: 'e' → bitmap of record IDs  
Position 2: 'l' → bitmap of record IDs
Position 3: 'l' → bitmap of record IDs
Position 4: 'o' → bitmap of record IDs
```

This enables O(1) lookup for any character at any position, making pattern matching extremely fast.

### 2. **Dual Indexing (Forward + Reverse)**
Biscuit maintains two complementary index structures:

- **Positive Index**: Character positions from the start (0, 1, 2, ...)
- **Negative Index**: Character positions from the end (-1, -2, -3, ...)

This allows anchored patterns to be resolved instantly:
- `'abc%'` → Query positive index at positions 0, 1, 2
- `'%xyz'` → Query negative index at positions -3, -2, -1
- `'abc%xyz'` → Query both indices and intersect results

### 3. **Roaring Bitmap Compression**
All bitmaps use [Roaring Bitmaps](https://roaringbitmap.org/) for space efficiency:

- **Run-length encoding** for consecutive IDs
- **Array containers** for sparse data
- **Bitmap containers** for dense data
- Typical compression: 10-100x vs. raw bitmaps

### 4. **Case-Sensitive + Case-Insensitive Support**
Each column maintains **two parallel index structures**:

```c
typedef struct ColumnIndex {
    // Case-sensitive (LIKE, NOT LIKE)
    CharIndex pos_idx[256];        // Forward positions
    CharIndex neg_idx[256];        // Reverse positions
    RoaringBitmap *char_cache[256];
    
    // Case-insensitive (ILIKE, NOT ILIKE)
    CharIndex pos_idx_lower[256];  // Lowercase forward
    CharIndex neg_idx_lower[256];  // Lowercase reverse
    RoaringBitmap *char_cache_lower[256];
    
    // Shared length metadata
    RoaringBitmap **length_bitmaps;    // Exact length
    RoaringBitmap **length_ge_bitmaps; // Length >= N
} ColumnIndex;
```

Lowercase data is stored separately, enabling fast ILIKE queries without runtime case conversion.

---

## Index Structure

### Main Index Object

```c
typedef struct BiscuitIndex {
    // Multi-column support
    int num_columns;
    Oid *column_types;
    FmgrInfo *output_funcs;
    char ***column_data_cache;  // [col][record]
    
    // Per-column indices (multi-column mode)
    ColumnIndex *column_indices;
    
    // Legacy single-column indices (backward compatibility)
    CharIndex pos_idx_legacy[256];
    CharIndex neg_idx_legacy[256];
    RoaringBitmap *char_cache_legacy[256];
    RoaringBitmap **length_bitmaps_legacy;
    RoaringBitmap **length_ge_bitmaps_legacy;
    int max_length_legacy;
    
    // Case-insensitive single-column
    CharIndex pos_idx_lower[256];
    CharIndex neg_idx_lower[256];
    RoaringBitmap *char_cache_lower[256];
    char **data_cache_lower;
    
    // Record storage
    ItemPointerData *tids;       // Heap tuple IDs
    char **data_cache;           // Original strings
    int num_records;
    int capacity;
    int max_len;
    
    // CRUD support
    RoaringBitmap *tombstones;   // Deleted records
    uint32_t *free_list;         // Reusable slots
    int free_count;
    int tombstone_count;
    
    // Statistics
    int64 insert_count;
    int64 update_count;
    int64 delete_count;
} BiscuitIndex;
```

### Character Index Structure

```c
typedef struct {
    int pos;                // Position (or negative offset)
    RoaringBitmap *bitmap;  // Record IDs matching this char/pos
} PosEntry;

typedef struct {
    PosEntry *entries;
    int count;
    int capacity;
} CharIndex;
```

Each character (0-255) has a `CharIndex` containing all positions where it appears.

### Length Bitmaps

```c
// Exact length queries: "___" (3 underscores)
RoaringBitmap *length_bitmaps[max_len + 1];

// Range length queries: "%___" (length >= 3)
RoaringBitmap *length_ge_bitmaps[max_len + 1];
```

These enable instant resolution of pure wildcard patterns.

---

## Query Processing

### 1. **Pattern Analysis**

```c
typedef struct {
    int column_index;
    char *pattern;
    
    // Pattern characteristics
    bool has_percent;
    bool is_prefix;      // 'abc%'
    bool is_suffix;      // '%abc'
    bool is_exact;       // 'abc'
    bool is_substring;   // '%abc%'
    
    int concrete_chars;
    int underscore_count;
    int percent_count;
    int partition_count;
    int anchor_strength; // 0-100
    
    double selectivity_score;
    int priority;
} QueryPredicate;
```

Each pattern is analyzed to determine:
- **Type**: Exact, prefix, suffix, substring, or complex
- **Selectivity**: Estimated result set size
- **Priority**: Execution order (most selective first)

### 2. **Query Optimization**

The query optimizer reorders predicates to minimize candidates:

```
Priority Tiers:
0-10:  Exact matches, many underscores
10-20: Non-% patterns with underscores
20-30: Strong anchored patterns (>30 concrete chars)
30-40: Weak anchored patterns
40-50: Multi-partition patterns
50+:   Substring patterns (least selective)
```

### 3. **Fast Paths**

Biscuit recognizes and optimizes common patterns:

**Empty Pattern (`''`):**
```c
// Returns only zero-length strings
if (length_bitmaps[0])
    return copy(length_bitmaps[0]);
```
Instant lookup—just return the pre-computed bitmap of empty strings.


**Universal Match (`'%'`):**
```c
// Matches everything except deleted records
for (i = 0; i < num_records; i++)
    if (!is_tombstoned(i))
        add_to_result(i);
```
No filtering needed, just skip tombstones. Linear scan of the record list.



**Pure Underscore Wildcards (`'___'`):**
```c
// Exact length constraint: 3 characters
return copy(length_bitmaps[3]);
```
Each underscore (`_`) matches exactly one character, so `'___'` means "any string of length 3". Direct bitmap lookup.



**Length Range (`'%___'` or `'___%%'`):**
```c
// Any string with length >= 3
return copy(length_ge_bitmaps[3]);
```
The `%` can match zero or more characters, and underscores require at least 3 characters. The `length_ge_bitmaps` stores cumulative counts: `length_ge[3]` contains all records with length ≥ 3.



**Exact Match (`'abc'`):**
```c
// Must match at position 0 with exact length 3
result = pos_bitmap['a'][0];
result &= pos_bitmap['b'][1];
result &= pos_bitmap['c'][2];
result &= length_bitmaps[3];  // Enforce exact length
```
Three bitmap intersections (one per character) plus a length check. If any character doesn't match, the intersection becomes empty immediately (early termination).



**Prefix Match (`'abc%'`):**
```c
// Must start with 'abc', any length >= 3
result = pos_bitmap['a'][0];
result &= pos_bitmap['b'][1];
result &= pos_bitmap['c'][2];
// No exact length constraint—trailing % allows any length
```
Same as exact match but without enforcing a specific length. Strings like `"abc"`, `"abcd"`, `"abcxyz"` all match.



**Suffix Match (`'%abc'`):**
```c
// Must end with 'abc', any length >= 3
result = neg_bitmap['a'][-3];  // 3rd from end
result &= neg_bitmap['b'][-2];  // 2nd from end
result &= neg_bitmap['c'][-1];  // Last character
```
Uses **negative indexing** to anchor at the end. The negative index bitmaps are pre-computed during index build. Strings like `"abc"`, `"xyzabc"`, `"123abc"` all match.



**Substring Match (`'%abc%'`):**
```c
// Can appear anywhere in the string
result = empty_bitmap();
for (pos = 0; pos <= max_len - 3; pos++) {
    match = pos_bitmap['a'][pos];
    match &= pos_bitmap['b'][pos+1];
    match &= pos_bitmap['c'][pos+2];
    result |= match;  // Union across all positions
}
```
This is the worst case—must check every possible starting position. For a 100-character max string, this means up to 98 intersections (positions 0 through 97). However:
- Bitmap operations are extremely fast (CPU vectorized)
- Early termination if `max_len - pattern_len` is small
- Results are unioned incrementally, so empty intermediate results don't hurt

**Why is this still fast?**
Even with O(m) complexity, Roaring bitmap operations are **faster** than string scanning. For a 1M record table.

The bitmap approach wins by avoiding per-record string operations entirely.

### 4. **Windowed Matching (Complex Patterns)**

For patterns like `'abc%def%ghi'`, Biscuit uses recursive windowed matching:

```
1. Match first part 'abc' at position 0
2. For each match, try 'def' at positions 3...max_pos
3. For each match, try 'ghi' at valid positions
4. Last part anchored to end if no trailing %
```

This avoids brute-force string scanning by leveraging bitmap intersections.


## Performance Optimizations

### 1. **Skip Wildcard Intersections**
`'_'` wildcards match everything at a position, so they're skipped during bitmap AND operations:

```c
for (i = 0; i < part_len; i++) {
    if (part[i] == '_') continue;  // Skip wildcard
    
    bm = get_pos_bitmap(idx, part[i], pos + i);
    result = bitmap_and(result, bm);
}
```

### 2. **Early Termination**
Stop processing immediately if any intersection yields zero results:

```c
bitmap_and_inplace(result, char_bitmap);
if (bitmap_is_empty(result))
    return result;  // No point continuing
```

### 3. **TID Sorting for Sequential I/O**
Results are sorted by (block, offset) to enable sequential heap scans:

```c
if (count < 5000) {
    qsort(tids, count, sizeof(ItemPointer), compare_tids);
} else {
    radix_sort_tids(tids, count);  // O(n) for large sets
}
```

### 4. **Skip Sorting for Aggregates**
`COUNT(*)` and `EXISTS` queries don't need sorted TIDs:

```c
bool is_aggregate = !scan->xs_want_itup;
if (is_aggregate) {
    // Return unsorted TIDs for bitmap heap scan
    // Saves O(n log n) sorting cost
}
```

### 5. **Parallel TID Collection**
Large result sets (10k+ TIDs) are collected using multiple workers:

```c
if (count >= 10000) {
    int num_workers = (count < 100000) ? 2 : 4;
    // Distribute bitmap iteration across workers
    // Each worker processes a chunk independently
}
```

### 6. **Direct Bitmap Iteration**
Avoid materializing arrays when possible:

```c
#ifdef HAVE_ROARING
roaring_uint32_iterator_t *iter = roaring_create_iterator(bitmap);
while (iter->has_value) {
    process(iter->current_value);
    roaring_advance_uint32_iterator(iter);
}
#endif
```

### 7. **LIMIT-Aware Collection**
Stop collecting TIDs early when LIMIT is detected:

```c
int limit_hint = estimate_limit(scan);
if (limit_hint > 0 && idx >= limit_hint) {
    break;  // Collected enough
}
```

---

## Multi-Column Support

### Architecture

Each column maintains its own independent Biscuit index:

```c
BiscuitIndex {
    ColumnIndex column_indices[N];  // One per column
}
```

### Query Execution

Multi-column queries use query optimization:

```sql
WHERE col1 LIKE 'abc%' AND col2 LIKE '%xyz'
```

Execution plan:
1. **Analyze**: Score each predicate for selectivity
2. **Reorder**: Execute most selective first
3. **Execute**: 
   ```
   candidates = query_column(col1, 'abc%')
   candidates = intersect(candidates, query_column(col2, '%xyz'))
   ```
4. **Filter tombstones**: Remove deleted records
5. **Collect TIDs**: Convert bitmap to tuple IDs


---

## CRUD Operations

### Insert

```c
bool biscuit_insert(values[], isnull[], ht_ctid) {
    // 1. Try to reuse deleted slot
    if (pop_free_slot(&rec_idx)) {
        remove_from_tombstones(rec_idx);
    } else {
        rec_idx = num_records++;
    }
    
    // 2. Store TID and data
    tids[rec_idx] = *ht_ctid;
    data_cache[rec_idx] = copy_string(value);
    
    // 3. Update all indices
    for (pos = 0; pos < len; pos++) {
        add_to_pos_bitmap(str[pos], pos, rec_idx);
        add_to_neg_bitmap(str[pos], -(len-pos), rec_idx);
    }
    add_to_length_bitmaps(len, rec_idx);
}
```

### Delete (Lazy)

```c
bool callback(ItemPointer tid) {
    // Mark as tombstone, don't remove immediately
    bitmap_add(tombstones, rec_idx);
    push_free_slot(rec_idx);
    tombstone_count++;
    
    // Cleanup when threshold reached
    if (tombstone_count >= 1000) {
        cleanup_all_bitmaps();
    }
}
```

### Update

Updates are **delete + insert**:
```c
// PostgreSQL calls:
// 1. bulkdelete(old_tid)
// 2. insert(new_values, new_tid)
```

---

## Memory Management

### Cache Strategy

Indices are cached in `CacheMemoryContext` for persistence across queries:

```c
static BiscuitIndexCacheEntry *cache_head = NULL;

// On first access:
idx = load_index(relation);
cache_insert(relation_oid, idx);

// On subsequent access:
idx = cache_lookup(relation_oid);
```

### Cleanup

Biscuit registers callbacks for safe cleanup:

```c
void biscuit_register_callback() {
    // Invalidate cache when relation changes
    CacheRegisterRelcacheCallback(biscuit_relcache_callback);
    
    // Clean up on process exit
    on_proc_exit(biscuit_module_unload_callback);
}
```

---

## Disk Persistence

### Metadata Only

Biscuit stores only a metadata marker on disk:

```c
typedef struct BiscuitMetaPageData {
    uint32 magic;        // 0x42495343 ("BISC")
    uint32 version;      // 1
    BlockNumber root;    // 0
    uint32 num_records;
} BiscuitMetaPageData;
```

### Rebuild Strategy

Bitmaps are **not serialized**. On index load:

1. Read metadata marker
2. Scan heap table
3. Rebuild all bitmaps in memory
4. Cache in `CacheMemoryContext`

**Rationale**: 
- Bitmap serialization is complex and large
- Memory representation is optimal for queries

---

## Concurrency & MVCC

### Isolation

Biscuit respects PostgreSQL's MVCC:

```c
// Index stores ALL versions
tids[rec_idx] = *heap_tid;

// Visibility checked during heap scan
if (!HeapTupleSatisfiesVisibility(tuple, snapshot))
    continue;  // Skip invisible tuple
```

### Locking

- **Reads**: AccessShareLock on index
- **Writes**: ExclusiveLock during insert/delete
- **Vacuum**: ShareLock during cleanup

### Tombstones

Deleted records remain in index as tombstones until vacuum:

```c
// Deleted record still in bitmaps, but marked:
bitmap_add(tombstones, rec_idx);

// Filtered during query:
bitmap_andnot(result, tombstones);

// Removed during vacuum when threshold reached
if (tombstone_count >= 1000) {
    cleanup_all_bitmaps();
}
```

---

## Limitations & Tradeoffs

### What Biscuit Does Well

- Pattern matching (`LIKE`, `ILIKE`)  
- Prefix/suffix queries (`'abc%'`, `'%xyz'`)  
- Complex multi-wildcard patterns  
- Multi-column pattern searches  
- Case-sensitive and case-insensitive  
- Aggregates (`COUNT(*)`, `EXISTS`)  

### What Biscuit Doesn't Do

- **Equality/range queries**: Use B-tree  
- **Full-text search**: Use GIN with tsvector  
- **Regex**: Not supported 
- **Very long strings**: Memory usage scales with length  
- **Online serialization**: Requires rebuild on load  

---

## Future Enhancements

### Potential Improvements

1. **Regex Support**: Extend pattern matching to simple regex
2. **Incremental Serialization**: Write bitmaps to disk incrementally
3. **Adaptive Compression**: Choose bitmap format based on density
4. **Parallel Build**: Multi-threaded index construction
5. **Statistics Collection**: Better selectivity estimates
6. **GPU Acceleration**: Offload bitmap operations to GPU
7. **Distributed Queries**: Shard index across nodes

### Research Directions

- **LSM-tree Integration**: Merge Biscuit with LSM for write-heavy workloads
- **Learned Indices**: ML-based selectivity prediction
- **Approximate Matching**: Fuzzy pattern matching with edit distance

---

## Comparison to Other Approaches

| Index Type | Build Time | Query Time | Memory | Pattern Support |
|------------|-----------|-----------|--------|-----------------|
| **Biscuit** | Medium | **Fast** | Very High | LIKE, ILIKE, anchored |
| **B-tree** | Fast | Slow | Low | Prefix only (`'abc%'`) |
| **GIN (trigram)** | Slow | Medium | High | All patterns, but slower |
| **Hash** | Fast | Fast | Low | Equality only |
| **Full scan** | - | Slowest | - | All patterns |

**When to use Biscuit:**
- Pattern matching is primary workload
- Strings are moderate length 
- Memory is available
- Need sub-ms query latency

---

## Appendix: Code Navigation


### Important Functions

- `biscuit_build()` - Index construction
- `biscuit_rescan()` - Query execution entry
- `biscuit_query_pattern()` - LIKE query processing
- `biscuit_query_pattern_ilike()` - ILIKE query processing
- `biscuit_match_part_at_pos()` - Windowed matching core
- `create_query_plan()` - Multi-column optimizer
- `biscuit_collect_tids_optimized()` - Result collection
