# Biscuit Index Architecture

## Overview

Biscuit is a PostgreSQL index access method (AM) optimized for pattern matching operations (`LIKE`, `ILIKE`, `NOT LIKE`, `NOT ILIKE`). It uses compressed bitmap indices with **UTF-8-aware character-position indexing** to achieve faster query performance on pattern matching workloads.

## Core Design Principles

### 1. **UTF-8 Character-Level Position-Based Indexing**
Biscuit indexes strings at the **character level**, not byte level, correctly handling multi-byte UTF-8 sequences:

```
String: "café" = [63 61 66 C3 A9] (5 bytes)
Character 0: 'c' → bitmap of record IDs
Character 1: 'a' → bitmap of record IDs  
Character 2: 'f' → bitmap of record IDs
Character 3: 'é' (bytes C3 A9) → bitmap of record IDs
```

**Key Innovation**: Multi-byte UTF-8 characters are indexed with **ALL their bytes at the SAME character position**. This enables correct pattern matching for international text while maintaining O(1) character lookup.

**Why this matters:**
- `'café'` has 4 **characters** but 5 **bytes**
- Pattern `'caf_'` (4 characters) should match `'café'`
- Character counts are used for length filters, not byte counts

### 2. **Dual Indexing (Forward + Reverse)**
Biscuit maintains two complementary index structures:

- **Positive Index**: Character positions from the start (0, 1, 2, ...)
- **Negative Index**: Character positions from the end (-1, -2, -3, ...)

This allows anchored patterns to be resolved instantly:
- `'abc%'` → Query positive index at character positions 0, 1, 2
- `'%xyz'` → Query negative index at character positions -3, -2, -1
- `'abc%xyz'` → Query both indices and intersect results

### 3. **Roaring Bitmap Compression**
All bitmaps use [Roaring Bitmaps](https://roaringbitmap.org/) for space efficiency:

- **Run-length encoding** for consecutive IDs
- **Array containers** for sparse data
- **Bitmap containers** for dense data

### 4. **Dual Case Sensitivity Architecture**
Each column maintains **two completely separate index structures** with their own length bitmaps:

```c
typedef struct ColumnIndex {
    // ==================== CASE-SENSITIVE (LIKE, NOT LIKE) ====================
    CharIndex pos_idx[256];               // Forward character positions
    CharIndex neg_idx[256];               // Reverse character positions
    RoaringBitmap *char_cache[256];       // Character presence cache
    RoaringBitmap **length_bitmaps;       // Exact length (based on original string)
    RoaringBitmap **length_ge_bitmaps;    // Length >= N (based on original string)
    int max_length;                       // Max character count (original)
    
    // ==================== CASE-INSENSITIVE (ILIKE, NOT ILIKE) ====================
    CharIndex pos_idx_lower[256];         // Lowercase forward positions
    CharIndex neg_idx_lower[256];         // Lowercase reverse positions
    RoaringBitmap *char_cache_lower[256]; // Lowercase character cache
    RoaringBitmap **length_bitmaps_lower;    // Exact length (lowercase string)
    RoaringBitmap **length_ge_bitmaps_lower; // Length >= N (lowercase string)
    int max_length_lower;                 // Max character count (lowercase)
} ColumnIndex;
```

**Critical Design Decision**: Case-sensitive and case-insensitive indices have **separate length bitmaps** because lowercasing can change character counts (e.g., German `'ß'` → `'ss'`).

**Why separate data caches?**
```c
// Original data (for LIKE)
data_cache[rec_idx] = "Café";  // 4 characters

// Lowercase data (for ILIKE)
data_cache_lower[rec_idx] = biscuit_str_tolower("Café");  // "café", still 4 chars
```

Lowercase conversion is done **once during index build** using PostgreSQL's locale-aware `lower()` function, making ILIKE queries as fast as LIKE.

---

## Index Structure

### Main Index Object

```c
typedef struct BiscuitIndex {
    // ==================== MULTI-COLUMN SUPPORT ====================
    int num_columns;
    Oid *column_types;
    FmgrInfo *output_funcs;
    char ***column_data_cache;      // [col][record] - original data
    
    // Per-column indices (multi-column mode)
    ColumnIndex *column_indices;    // Each column has dual indices
    
    // ==================== SINGLE-COLUMN LEGACY (BACKWARD COMPAT) ====================
    // Case-sensitive
    CharIndex pos_idx_legacy[256];
    CharIndex neg_idx_legacy[256];
    RoaringBitmap *char_cache_legacy[256];
    RoaringBitmap **length_bitmaps_legacy;
    RoaringBitmap **length_ge_bitmaps_legacy;
    int max_length_legacy;
    
    // Case-insensitive
    CharIndex pos_idx_lower[256];
    CharIndex neg_idx_lower[256];
    RoaringBitmap *char_cache_lower[256];
    RoaringBitmap **length_bitmaps_lower;
    RoaringBitmap **length_ge_bitmaps_lower;
    int max_length_lower;
    char **data_cache_lower;        // Lowercase data cache
    
    // ==================== RECORD STORAGE ====================
    ItemPointerData *tids;          // Heap tuple IDs
    char **data_cache;              // Original strings (case-sensitive)
    int num_records;
    int capacity;
    int max_len;                    // Max character count (case-sensitive)
    
    // ==================== CRUD SUPPORT ====================
    RoaringBitmap *tombstones;      // Deleted records
    uint32_t *free_list;            // Reusable slots
    int free_count;
    int free_capacity;
    int tombstone_count;
    
    // ==================== STATISTICS ====================
    int64 insert_count;
    int64 update_count;
    int64 delete_count;
} BiscuitIndex;
```

### Character Index Structure

```c
typedef struct {
    int pos;                        // Character position (or negative offset)
    RoaringBitmap *bitmap;          // Record IDs matching this char/pos
} PosEntry;

typedef struct {
    PosEntry *entries;
    int count;
    int capacity;
} CharIndex;
```

Each character (0-255) has a `CharIndex` containing all **character positions** where it appears.

### Length Bitmaps (Dual Architecture)

```c
// ==================== CASE-SENSITIVE LENGTH BITMAPS ====================
// Exact length queries: "___" (3 underscores)
RoaringBitmap *length_bitmaps[max_length + 1];

// Range length queries: "%___" (length >= 3)
RoaringBitmap *length_ge_bitmaps[max_length + 1];

// ==================== CASE-INSENSITIVE LENGTH BITMAPS (SEPARATE!) ====================
// Exact lowercase length: ILIKE '___'
RoaringBitmap *length_bitmaps_lower[max_length_lower + 1];

// Lowercase length range: ILIKE '%___'
RoaringBitmap *length_ge_bitmaps_lower[max_length_lower + 1];
```

**Why separate?** Lowercasing can change character counts:
- German: `'ß'` (1 char) → `'ss'` (2 chars)
- Turkish: `'İ'` (1 char) → `'i̇'` (potentially 2 chars depending on locale)

Each set of length bitmaps is populated based on the **actual character count** of the indexed strings (original or lowercase).

---

## UTF-8 Character Handling

### Character Counting

```c
static int 
biscuit_utf8_char_count(const char *str, int byte_len)
{
    int char_count = 0;
    int pos = 0;
    
    while (pos < byte_len) {
        int char_len = biscuit_utf8_char_length((unsigned char)str[pos]);
        
        // Safety: Don't read past buffer
        if (pos + char_len > byte_len)
            char_len = byte_len - pos;
        
        pos += char_len;
        char_count++;
    }
    
    return char_count;
}
```

**Example:**
```
"café" = [0x63 0x61 0x66 0xC3 0xA9]
         'c'  'a'  'f'  'é' (multi-byte)
         
byte_len = 5
char_count = 4  ✓ Correct!
```

### Multi-Byte Character Indexing

```c
// Index UTF-8 character at character position
int byte_pos = 0;
int char_pos = 0;

while (byte_pos < byte_len) {
    unsigned char first_byte = str[byte_pos];
    int char_len = biscuit_utf8_char_length(first_byte);
    
    // ✓ CRITICAL: Index ALL bytes at SAME character position
    for (int b = 0; b < char_len; b++) {
        unsigned char byte_val = str[byte_pos + b];
        
        // Positive position
        bitmap = get_pos_bitmap(idx, byte_val, char_pos);
        bitmap_add(bitmap, rec_idx);
        
        // Negative position
        int remaining_chars = utf8_char_count(str + byte_pos, byte_len - byte_pos);
        int neg_offset = -remaining_chars;
        bitmap = get_neg_bitmap(idx, byte_val, neg_offset);
        bitmap_add(bitmap, rec_idx);
    }
    
    byte_pos += char_len;
    char_pos++;  // Increment ONCE per character
}
```

**Example: Indexing "café"**
```
char_pos=0: 'c' (0x63) → pos_bitmap[0x63][0]
char_pos=1: 'a' (0x61) → pos_bitmap[0x61][1]
char_pos=2: 'f' (0x66) → pos_bitmap[0x66][2]
char_pos=3: 'é' (0xC3 0xA9) → pos_bitmap[0xC3][3] AND pos_bitmap[0xA9][3]
                              ^^^^^^^^^^^^^^^^^^    ^^^^^^^^^^^^^^^^^^
                              Both bytes at SAME position!
```

This ensures that pattern `'caf_'` correctly matches `'café'` by checking character position 3, not byte position 3.

---

## Query Processing

### 1. **UTF-8-Aware Pattern Analysis**

```c
typedef struct ParsedPattern {
    char **parts;              // Pattern parts split by %
    int *part_lens;            // CHARACTER counts (for length filters)
    int *part_byte_lens;       // BYTE lengths (for string operations)
    int part_count;
    bool starts_percent;
    bool ends_percent;
} ParsedPattern;
```

**Critical Fix**: Store both character counts and byte lengths:
- **Character counts** used for length filters (`length_ge_bitmaps`)
- **Byte lengths** used for string operations (`memcmp`, offsets)

### 2. **Query Optimization (Multi-Column)**

```c
typedef struct {
    int column_index;
    char *pattern;
    ScanKey scan_key;
    
    // Pattern analysis
    bool has_percent;
    bool is_prefix;      // 'abc%'
    bool is_suffix;      // '%abc'
    bool is_exact;       // 'abc'
    bool is_substring;   // '%abc%'
    
    int concrete_chars;       // UTF-8 character count
    int underscore_count;
    int percent_count;
    int partition_count;
    int anchor_strength;      // 0-100
    
    double selectivity_score; // Lower = more selective
    int priority;             // Execution order
} QueryPredicate;
```

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

### 3. **Fast Paths (UTF-8-Aware)**

**Empty Pattern (`''`):**
```c
// Returns only zero-length strings
if (length_bitmaps[0])
    return copy(length_bitmaps[0]);
```

**Universal Match (`'%'`):**
```c
// Matches everything except deleted records
for (i = 0; i < num_records; i++)
    if (!is_tombstoned(i))
        add_to_result(i);
```

**Pure Underscore Wildcards (`'___'`):**
```c
// Exact CHARACTER length constraint: 3 characters
return copy(length_bitmaps[3]);  // Uses CHARACTER counts!
```

**Character Length Range (`'%___'`):**
```c
// Any string with CHARACTER length >= 3
return copy(length_ge_bitmaps[3]);  // Uses CHARACTER counts!
```

**Exact Match (`'abc'`):**
```c
// Must match at character position 0 with exact CHARACTER length 3
result = pos_bitmap['a'][0];
result &= pos_bitmap['b'][1];
result &= pos_bitmap['c'][2];
result &= length_bitmaps[3];  // CHARACTER length = 3
```

**Prefix Match (`'abc%'`):**
```c
// Must start with 'abc', any CHARACTER length >= 3
result = pos_bitmap['a'][0];
result &= pos_bitmap['b'][1];
result &= pos_bitmap['c'][2];
// No exact length constraint
```

**Suffix Match (`'%abc'`):**
```c
// Must end with 'abc', any CHARACTER length >= 3
result = neg_bitmap['a'][-3];  // 3rd CHARACTER from end
result &= neg_bitmap['b'][-2];  // 2nd CHARACTER from end
result &= neg_bitmap['c'][-1];  // Last CHARACTER
```

**Substring Match (`'%abc%'` or `'%café%'`):**
```c
// CRITICAL FIX: UTF-8-aware substring search
// Strategy:
// 1. Get candidates using first byte of pattern
// 2. Validate UTF-8 boundaries with character-aware matching

result = empty_bitmap();

// Get candidates (fast filter)
if (char_cache[first_byte])
    candidates = copy(char_cache[first_byte]);

// Apply CHARACTER length filter (not byte length!)
int pattern_char_count = utf8_char_count(pattern, pattern_byte_len);
length_filter = get_length_ge(idx, pattern_char_count);
bitmap_and(candidates, length_filter);

// Validate each candidate (correct but slower)
for each candidate rec_idx {
    haystack = data_cache[rec_idx];
    haystack_char_count = utf8_char_count(haystack);
    
    // Try matching at each CHARACTER position
    for (char_pos = 0; char_pos <= haystack_char_count - pattern_char_count; char_pos++) {
        // Convert character position to byte offset
        byte_offset = utf8_char_to_byte_offset(haystack, char_pos);
        
        // Match pattern bytes at this position
        if (match_utf8_pattern(haystack + byte_offset, pattern)) {
            bitmap_add(result, rec_idx);
            break;
        }
    }
}
```

**Why substring is slow:**
- Must validate UTF-8 character boundaries
- Cannot rely solely on byte-level bitmap intersections
- Uses hybrid approach: bitmap filtering + validation

### 4. **Windowed Matching (Complex Patterns)**

For patterns like `'abc%def%ghi'`, Biscuit uses UTF-8-aware recursive windowed matching:

```c
static void 
biscuit_recursive_windowed_match(
    RoaringBitmap *result, BiscuitIndex *idx,
    const char **parts, int *part_lens,      // CHARACTER counts
    int part_count, bool ends_percent,
    int part_idx, int min_pos,               // CHARACTER position
    RoaringBitmap *current_candidates, int max_len)
{
    // Base case: all parts matched
    if (part_idx >= part_count) {
        bitmap_or(result, current_candidates);
        return;
    }
    
    // Calculate remaining CHARACTER length needed
    int remaining_chars = 0;
    for (int i = part_idx + 1; i < part_count; i++)
        remaining_chars += part_lens[i];  // CHARACTER counts
    
    // Last part without trailing % must match at end
    if (part_idx == part_count - 1 && !ends_percent) {
        end_match = match_part_at_end(idx, parts[part_idx]);
        bitmap_and(end_match, current_candidates);
        
        // CHARACTER length constraint
        min_length = min_pos + part_lens[part_idx];
        length_filter = get_length_ge(idx, min_length);
        bitmap_and(end_match, length_filter);
        
        bitmap_or(result, end_match);
        return;
    }
    
    // Try all valid CHARACTER positions
    int current_part_chars = part_lens[part_idx];
    int max_pos = max_len - current_part_chars - remaining_chars;
    
    for (int char_pos = min_pos; char_pos <= max_pos; char_pos++) {
        part_match = match_part_at_pos(idx, parts[part_idx], char_pos);
        next_candidates = bitmap_copy(current_candidates);
        bitmap_and(next_candidates, part_match);
        
        if (!bitmap_is_empty(next_candidates)) {
            // Recurse with next CHARACTER position
            recurse(result, idx, parts, part_lens, part_count,
                    ends_percent, part_idx + 1, 
                    char_pos + current_part_chars,  // CHARACTER arithmetic
                    next_candidates, max_len);
        }
    }
}
```

**Key invariant:** All position arithmetic uses **character offsets**, not byte offsets.

---

## ILIKE Implementation

### Architecture

```c
// ILIKE query processing
static RoaringBitmap* 
biscuit_query_pattern_ilike(BiscuitIndex *idx, const char *pattern) {
    // Step 1: Convert pattern to lowercase ONCE
    char *pattern_lower = biscuit_str_tolower(pattern, strlen(pattern));
    
    // Step 2: Parse lowercase pattern
    ParsedPattern *parsed = biscuit_parse_pattern(pattern_lower);
    
    // Step 3: Query lowercase indices
    result = match_using_lowercase_indices(idx, parsed);
    
    // Cleanup
    free_parsed_pattern(parsed);
    pfree(pattern_lower);
    
    return result;
}
```

### Case Folding

```c
static char *
biscuit_str_tolower(const char *str, int len)
{
    text *input = cstring_to_text_with_len(str, len);
    Oid collation = get_typcollation(TEXTOID);  // Database default
    
    text *result_text = DatumGetTextP(
        DirectFunctionCall2Coll(
            lower,              // PostgreSQL's locale-aware lower()
            collation,
            PointerGetDatum(input),
            collation
        )
    );
    
    char *result = text_to_cstring(result_text);
    return result;  // Allocated with palloc
}
```

**Why use PostgreSQL's `lower()`?**
- Locale-aware (handles Turkish İ/i, German ß, etc.)
- Consistent with database collation
- Stable across index builds

### Data Flow

```
Index Build:
1. Original: "Café" → data_cache[idx] = "Café"
2. Lowercase: "Café" → data_cache_lower[idx] = biscuit_str_tolower("Café") = "café"
3. Index both:
   - pos_idx[...] indexes "Café" (case-sensitive)
   - pos_idx_lower[...] indexes "café" (case-insensitive)

LIKE Query:
"WHERE col LIKE 'Caf%'"
→ Query pos_idx with "Caf%" → Matches "Café" ✓

ILIKE Query:
"WHERE col ILIKE 'caf%'"
→ Convert pattern: "caf%" → "caf%"
→ Query pos_idx_lower with "caf%" → Matches "café" ✓
```

---

## Performance Optimizations

### 1. **Skip Wildcard Intersections**
`'_'` wildcards match everything at a character position, so they're skipped:

```c
for (i = 0; i < part_len; i++) {
    if (part[i] == '_') {
        char_pos++;  // Skip wildcard, advance CHARACTER position
        continue;
    }
    
    bm = get_pos_bitmap(idx, part[i], char_pos);
    result = bitmap_and(result, bm);
    char_pos++;
}
```

### 2. **Early Termination**
Stop processing immediately if any intersection yields zero results:

```c
bitmap_and_inplace(result, char_bitmap);
if (bitmap_is_empty(result))
    return result;  // No point continuing
```

### 3. **Character vs. Byte Length Separation**

```c
typedef struct ParsedPattern {
    char **parts;
    int *part_lens;         // ✓ CHARACTER counts (for length filters)
    int *part_byte_lens;    // ✓ BYTE lengths (for string operations)
    int part_count;
} ParsedPattern;

// Parsing:
part_lens[i] = utf8_char_count(part, part_byte_len);  // Characters
part_byte_lens[i] = part_byte_len;                    // Bytes
```

**Why both?**
- **Character counts** for length filters: `length_ge_bitmaps[char_count]`
- **Byte lengths** for memory operations: `memcmp(str, part, byte_len)`

### 4. **TID Sorting for Sequential I/O**
Results are sorted by (block, offset) to enable sequential heap scans:

```c
if (count < 5000) {
    qsort(tids, count, sizeof(ItemPointer), compare_tids);
} else {
    radix_sort_tids(tids, count);  // O(n) for large sets
}
```

### 5. **Skip Sorting for Aggregates**
`COUNT(*)` and `EXISTS` queries don't need sorted TIDs:

```c
bool is_aggregate = !scan->xs_want_itup;
if (is_aggregate) {
    // Return unsorted TIDs for bitmap heap scan
    // Saves O(n log n) sorting cost
}
```

### 6. **Parallel TID Collection**
Large result sets (10k+ TIDs) are collected using multiple workers:

```c
if (count >= 10000) {
    int num_workers = (count < 100000) ? 2 : 4;
    // Distribute bitmap iteration across workers
}
```

### 7. **LIMIT-Aware Collection**
Stop collecting TIDs early when LIMIT is detected:

```c
int limit_hint = estimate_limit(scan);
if (limit_hint > 0 && idx >= limit_hint)
    break;  // Collected enough
```

---

## Multi-Column Support

### Architecture

Each column maintains its own **completely independent** Biscuit index:

```c
BiscuitIndex {
    ColumnIndex column_indices[N];  // Each column: dual indices + dual length bitmaps
}
```

### Query Execution

Multi-column queries use intelligent predicate reordering:

```sql
WHERE col1 LIKE 'abc%' AND col2 ILIKE '%xyz'
```

Execution plan:
1. **Analyze**: Score each predicate for selectivity
   ```
   col1 'abc%': priority=20, selectivity=0.15 (strong prefix)
   col2 '%xyz': priority=30, selectivity=0.25 (weak suffix)
   ```

2. **Reorder**: Execute most selective first
   ```
   Execute col1 first (lower priority value = higher priority)
   ```

3. **Execute**: 
   ```c
   // Execute col1 LIKE (case-sensitive indices)
   candidates = query_column_pattern(idx, col1_idx, 'abc%');
   
   // Execute col2 ILIKE (case-insensitive indices)
   col2_result = query_column_pattern_ilike(idx, col2_idx, '%xyz');
   
   // Intersect
   bitmap_and(candidates, col2_result);
   ```

4. **Filter tombstones**: Remove deleted records

5. **Collect TIDs**: Convert bitmap to tuple IDs

### Strategy Routing

```c
// Route based on scan key strategy
switch (key->sk_strategy) {
    case BISCUIT_LIKE_STRATEGY:
        result = query_column_pattern(idx, col_idx, pattern);
        break;
        
    case BISCUIT_NOT_LIKE_STRATEGY:
        result = query_column_pattern(idx, col_idx, pattern);
        bitmap_invert(result);  // Negate
        break;
        
    case BISCUIT_ILIKE_STRATEGY:
        result = query_column_pattern_ilike(idx, col_idx, pattern);
        break;
        
    case BISCUIT_NOT_ILIKE_STRATEGY:
        result = query_column_pattern_ilike(idx, col_idx, pattern);
        bitmap_invert(result);  // Negate
        break;
}
```

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
    
    // 3. Create lowercase version
    data_cache_lower[rec_idx] = biscuit_str_tolower(value, strlen(value));
    
    // 4. Update case-sensitive indices
    int byte_pos = 0, char_pos = 0;
    while (byte_pos < byte_len) {
        int char_len = utf8_char_length(str[byte_pos]);
        
        // Index ALL bytes at SAME character position
        for (int b = 0; b < char_len; b++) {
            add_to_pos_bitmap(str[byte_pos + b], char_pos, rec_idx);
            add_to_neg_bitmap(str[byte_pos + b], -(len_chars - char_pos), rec_idx);
        }
        
        byte_pos += char_len;
        char_pos++;
    }
    
    // 5. Update case-insensitive indices (same logic with lowercase data)
    // ... (similar to step 4 but using data_cache_lower)
    
    // 6. Update length bitmaps (BOTH case-sensitive and case-insensitive)
    int char_count = utf8_char_count(str, byte_len);
    add_to_length_bitmaps(char_count, rec_idx);
    
    int lower_char_count = utf8_char_count(data_cache_lower[rec_idx]);
    add_to_length_bitmaps_lower(lower_char_count, rec_idx);
}
```

### Delete (Lazy + Cleanup)

```c
bool callback(ItemPointer tid) {
    // Step 1: Mark as tombstone (lazy delete)
    bitmap_add(tombstones, rec_idx);
    push_free_slot(rec_idx);
    tombstone_count++;
    
    // Step 2: Periodic cleanup when threshold reached
    if (tombstone_count >= 1000) {
        // Remove from ALL indices (case-sensitive + case-insensitive)
        for (ch = 0; ch < 256; ch++) {
            for (j = 0; j < pos_idx[ch].count; j++)
                bitmap_andnot(pos_idx[ch].entries[j].bitmap, tombstones);
            
            for (j = 0; j < neg_idx[ch].count; j++)
                bitmap_andnot(neg_idx[ch].entries[j].bitmap, tombstones);
            
            bitmap_andnot(char_cache[ch], tombstones);
            
            // Same for lowercase indices
            for (j = 0; j < pos_idx_lower[ch].count; j++)
                bitmap_andnot(pos_idx_lower[ch].entries[j].bitmap, tombstones);
            // ... etc
        }
        
        // Clean up length bitmaps (BOTH sets)
        for (i = 0; i <= max_length; i++) {
            bitmap_andnot(length_bitmaps[i], tombstones);
            bitmap_andnot(length_ge_bitmaps[i], tombstones);
        }
        
        for (i = 0; i <= max_length_lower; i++) {
            bitmap_andnot(length_bitmaps_lower[i], tombstones);
            bitmap_andnot(length_ge_bitmaps_lower[i], tombstones);
        }
        
        // Reset tombstone bitmap
        bitmap_free(tombstones);
        tombstones = bitmap_create();
        tombstone_count = 0;
    }
}
```


### Update

Updates are **delete + insert**:
```c
// PostgreSQL calls:
// 1. bulkdelete(old_tid)  → Marks old version as tombstone
// 2. insert(new_values, new_tid) → Creates new version
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

### Size Calculation

```c
PG_FUNCTION_INFO_V1(biscuit_index_memory_size);

Datum biscuit_index_memory_size(Oid indexoid) {
    BiscuitIndex *idx = load_index(indexoid);
    
    size_t total = 0;
    
    // Base structure
    total += sizeof(BiscuitIndex);
    
    // TID array
    total += capacity * sizeof(ItemPointerData);
    
    // String data caches (original + lowercase)
    for (i = 0; i < num_records; i++) {
        total += strlen(data_cache[i]) + 1;
        total += strlen(data_cache_lower[i]) + 1;  // Lowercase cache
    }
    
    // Case-sensitive indices (256 characters)
    for (ch = 0; ch < 256; ch++) {
        total += charindex_size(&pos_idx[ch]);
        total += charindex_size(&neg_idx[ch]);
        total += bitmap_size(char_cache[ch]);
    }
    
    // Case-insensitive indices (256 characters)
    for (ch = 0; ch < 256; ch++) {
        total += charindex_size(&pos_idx_lower[ch]);
        total += charindex_size(&neg_idx_lower[ch]);
        total += bitmap_size(char_cache_lower[ch]);
    }
    
    // Length bitmaps (BOTH sets)
    for (i = 0; i <= max_length; i++) {
        total += bitmap_size(length_bitmaps[i]);
        total += bitmap_size(length_ge_bitmaps[i]);
    }
    
    for (i = 0; i <= max_length_lower; i++) {
        total += bitmap_size(length_bitmaps_lower[i]);
        total += bitmap_size(length_ge_bitmaps_lower[i]);
    }
    
    // Tombstones + free list
    total += bitmap_size(tombstones);
    total += free_capacity * sizeof(uint32_t);
    
    return total;
}
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
3. Rebuild **all bitmaps** (case-sensitive + case-insensitive) in memory
4. Generate **lowercase data cache**
5. Cache in `CacheMemoryContext`

**Rationale**: 
- Bitmap serialization is complex and large
- Memory representation is optimal for queries
- Dual indices (LIKE + ILIKE) would double disk size

---

## Limitations & Tradeoffs

### What Biscuit Does Well

- Pattern matching (`LIKE`, `ILIKE`, `NOT LIKE`, `NOT ILIKE`)  
- UTF-8 multi-byte character support  
- Prefix/suffix queries (`'abc%'`, `'%xyz'`)  
- Complex multi-wildcard patterns  
- Multi-column pattern searches  
- Case-sensitive and case-insensitive (separate indices)  
- Aggregates (`COUNT(*)`, `EXISTS`)  
- International text (German, Turkish, Chinese, etc.)  

### What Biscuit Doesn't Do

- **Equality/range queries**: Use B-tree  
- **Full-text search**: Use GIN with tsvector  
- **Regex**: Not supported  
- **Very long strings**: Memory usage scales with character length  
- **Online serialization**: Requires rebuild on load  
- **Locale changes**: Lowercase cache is locale-dependent  

---

## Code Navigation

### Critical UTF-8 Functions

- `biscuit_utf8_char_length()` - Get UTF-8 character byte length
- `biscuit_utf8_char_count()` - Count characters (not bytes)
- `biscuit_utf8_char_to_byte_offset()` - Convert char position to byte offset
- `biscuit_str_tolower()` - Locale-aware lowercase conversion

### Index Build

- `biscuit_build()` - Single-column index construction
- `biscuit_build_multicolumn()` - Multi-column index construction
- `biscuit_load_index()` - Load/rebuild index from disk

### Query Processing

- `biscuit_rescan()` - Query execution entry (single-column)
- `biscuit_rescan_multicolumn()` - Query execution (multi-column)
- `biscuit_query_pattern()` - LIKE query (case-sensitive)
- `biscuit_query_pattern_ilike()` - ILIKE query (case-insensitive)
- `biscuit_match_part_at_pos()` - Windowed matching (forward)
- `biscuit_match_part_at_end()` - Windowed matching (reverse)
- `create_query_plan()` - Multi-column optimizer
- `biscuit_collect_tids_optimized()` - Result collection

### CRUD

- `biscuit_insert()` - Insert with dual indexing
- `biscuit_bulkdelete()` - Lazy delete with cleanup
- `biscuit_remove_from_all_indices()` - Remove from all bitmaps

### Diagnostics

- `biscuit_index_stats()` - Runtime statistics
- `biscuit_index_memory_size()` - Memory footprint calculation
- `biscuit_has_roaring()` - Check Roaring support
- `biscuit_build_info()` - Build configuration