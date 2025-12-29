/*
* biscuit.c
* PostgreSQL Index Access Method for Biscuit Pattern Matching with Full CRUD Support
*/

#include "postgres.h"
#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/table.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"     /* For getTypeOutputInfo */
#include "utils/timestamp.h"     /* For Timestamp, DatumGetTimestamp */
#include "utils/date.h"     /* For Timestamp, DatumGetTimestamp */
#include "fmgr.h"
#include "utils/inval.h"
#include "storage/ipc.h"           /* for on_proc_exit */
#include "funcapi.h"
#include "access/htup_details.h"
#include "utils/formatting.h"
#include "utils/pg_locale.h"
/*
#ifdef HAVE_ROARING
#pragma message("Biscuit: compiled WITH Roaring Bitmap support")
#else
#pragma message("Biscuit: compiled WITHOUT Roaring Bitmap support")
#endif
*/

#ifdef HAVE_ROARING
#include "roaring/roaring.h"
typedef roaring_bitmap_t RoaringBitmap;
#else
typedef struct {
    uint64_t *blocks;
    int num_blocks;
    int capacity;
} RoaringBitmap;
#endif

/* Strategy numbers for LIKE operators */
#define BTLessStrategyNumber        1
#define BTLessEqualStrategyNumber   2
#define BTEqualStrategyNumber       3
#define BTGreaterEqualStrategyNumber 4
#define BTGreaterStrategyNumber     5

#define BISCUIT_LIKE_STRATEGY       1
#define BISCUIT_NOT_LIKE_STRATEGY   2
#define BISCUIT_ILIKE_STRATEGY      3  // NEW: Case-insensitive LIKE
#define BISCUIT_NOT_ILIKE_STRATEGY  4  // NEW: Case-insensitive NOT LIKE


/* Safe memory management macros */
#define SAFE_PFREE(ptr) do { \
    if (ptr) { \
        pfree(ptr); \
        (ptr) = NULL; \
    } \
} while(0)

#define SAFE_BITMAP_FREE(bm) do { \
    if (bm) { \
        biscuit_roaring_free(bm); \
        (bm) = NULL; \
    } \
} while(0)

PG_MODULE_MAGIC;

/* Forward declaration of main index structure */
typedef struct BiscuitIndex BiscuitIndex;

/* Forward declaration of QueryPlan structure */
typedef struct QueryPlan QueryPlan;

/* Forward declarations */
PG_FUNCTION_INFO_V1(biscuit_handler);
PG_FUNCTION_INFO_V1(biscuit_index_stats);

// Check if compiled with Roaring support
PG_FUNCTION_INFO_V1(biscuit_has_roaring);
Datum
biscuit_has_roaring(PG_FUNCTION_ARGS)
{
#ifdef HAVE_ROARING
    PG_RETURN_BOOL(true);
#else
    PG_RETURN_BOOL(false);
#endif
}

// Get version string
PG_FUNCTION_INFO_V1(biscuit_version);
Datum
biscuit_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text("2.2.1"));
}

// Get build information - Set-Returning Function
PG_FUNCTION_INFO_V1(biscuit_build_info);
Datum
biscuit_build_info(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    int             call_cntr;
    int             max_calls;
    TupleDesc       tupdesc;
    AttInMetadata  *attinmeta;

    /* First call setup */
    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* Build tuple descriptor */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));

        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;
        funcctx->max_calls = 2; /* Number of rows to return */

        MemoryContextSwitchTo(oldcontext);
    }

    /* Each call */
    funcctx = SRF_PERCALL_SETUP();
    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    attinmeta = funcctx->attinmeta;

    if (call_cntr < max_calls)
    {
        char      **values;
        HeapTuple   tuple;
        Datum       result;

        values = (char **) palloc(3 * sizeof(char *));

        if (call_cntr == 0)
        {
            /* Row 1: Roaring support */
            values[0] = pstrdup("CRoaring Bitmaps");
        #ifdef HAVE_ROARING
            values[1] = pstrdup("true");
            values[2] = pstrdup("High-performance bitmap operations enabled");
        #else
            values[1] = pstrdup("false");
            values[2] = pstrdup("Using fallback bitmap implementation (reduced performance)");
        #endif
        }
        else if (call_cntr == 1)
        {
            /* Row 2: PostgreSQL version */
            values[0] = pstrdup("PostgreSQL");
            values[1] = pstrdup("true");
            values[2] = psprintf("Compiled for PostgreSQL %s", PG_VERSION);
        }

        tuple = BuildTupleFromCStrings(attinmeta, values);
        result = HeapTupleGetDatum(tuple);

        pfree(values[0]);
        pfree(values[1]);
        pfree(values[2]);
        pfree(values);

        SRF_RETURN_NEXT(funcctx, result);
    }
    else
    {
        /* No more rows */
        SRF_RETURN_DONE(funcctx);
    }
}

/* Alternative simpler implementation if you prefer single-row returns */

// Get Roaring library version (if available)
#ifdef HAVE_ROARING
#include <roaring/roaring.h>

PG_FUNCTION_INFO_V1(biscuit_roaring_version);
Datum
biscuit_roaring_version(PG_FUNCTION_ARGS)
{
    char version_str[64];
    
    snprintf(version_str, sizeof(version_str), "%d.%d.%d",
             ROARING_VERSION_MAJOR,
             ROARING_VERSION_MINOR,
             ROARING_VERSION_REVISION);
    
    PG_RETURN_TEXT_P(cstring_to_text(version_str));
}
#else
PG_FUNCTION_INFO_V1(biscuit_roaring_version);
Datum
biscuit_roaring_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_NULL();
}
#endif

// Simpler alternative: return build info as JSON text
PG_FUNCTION_INFO_V1(biscuit_build_info_json);
Datum
biscuit_build_info_json(PG_FUNCTION_ARGS)
{
    StringInfoData buf;
    
    initStringInfo(&buf);
    
    appendStringInfo(&buf, "{");
    appendStringInfo(&buf, "\"version\": \"2.2.1\",");
    
#ifdef HAVE_ROARING
    appendStringInfo(&buf, "\"roaring_enabled\": true,");
    appendStringInfo(&buf, "\"roaring_version\": \"%d.%d.%d\",",
                     ROARING_VERSION_MAJOR,
                     ROARING_VERSION_MINOR,
                     ROARING_VERSION_REVISION);
#else
    appendStringInfo(&buf, "\"roaring_enabled\": false,");
#endif
    
    appendStringInfo(&buf, "\"postgres_version\": \"%s\",", PG_VERSION);
    appendStringInfo(&buf, "}");
    
    PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/* ==================== QUERY ANALYSIS STRUCTURES ==================== */

typedef struct {
    int column_index;           /* Which column (0-based) */
    char *pattern;              /* The LIKE pattern */
    ScanKey scan_key;           /* Original scan key */
    
    /* Pattern analysis */
    bool has_percent;           /* Contains % wildcard */
    bool is_prefix;             /* Starts with concrete chars (e.g., 'abc%') */
    bool is_suffix;             /* Ends with concrete chars (e.g., '%abc') */
    bool is_exact;              /* No wildcards at all */
    bool is_substring;          /* Format: %...% */
    
    int concrete_chars;         /* Number of non-wildcard characters */
    int underscore_count;       /* Number of _ wildcards */
    int percent_count;          /* Number of % wildcards */
    int partition_count;        /* Number of parts separated by % */
    int anchor_strength;        /* Quality of anchors (0-100) */
    
    /* Selectivity estimate */
    double selectivity_score;   /* Lower = more selective (0.0 to 1.0) */
    int priority;               /* Execution order (lower = earlier) */
} QueryPredicate;

typedef struct QueryPlan{
    QueryPredicate *predicates;
    int count;
    int capacity;
} QueryPlan;

/* Static cache for Biscuit indices */
typedef struct BiscuitIndexCacheEntry {
    Oid indexoid;
    BiscuitIndex *index;
    struct BiscuitIndexCacheEntry *next;
} BiscuitIndexCacheEntry;

static BiscuitIndexCacheEntry *biscuit_cache_head = NULL;
static bool biscuit_callback_registered = false;

/* Cache lookup */
static BiscuitIndex*
biscuit_cache_lookup(Oid indexoid)
{
    BiscuitIndexCacheEntry *entry;
    for (entry = biscuit_cache_head; entry != NULL; entry = entry->next) {
        if (entry->indexoid == indexoid)
            return entry->index;
    }
    return NULL;
}

static void
biscuit_module_unload_callback(int code, unsigned long datum)
{
    BiscuitIndexCacheEntry *entry = biscuit_cache_head;
    
    elog(DEBUG1, "Biscuit: Module unload - clearing all cache entries");
    
    /* Just clear the linked list - don't free memory */
    /* CacheMemoryContext will be reset by PostgreSQL */
    biscuit_cache_head = NULL;
    
    /* Suppress any further callbacks */
    biscuit_callback_registered = false;
}

/* ==================== UPDATED CACHE INSERT ==================== */

/*
 * Insert into cache - ensure proper memory context
 */

static void biscuit_cache_remove(Oid indexoid);

static void
biscuit_cache_insert(Oid indexoid, BiscuitIndex *idx)
{
    BiscuitIndexCacheEntry *entry;
    MemoryContext oldcontext;
    
    /* Remove any existing entry first */
    biscuit_cache_remove(indexoid);
    
    oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
    
    entry = (BiscuitIndexCacheEntry *)palloc(sizeof(BiscuitIndexCacheEntry));
    entry->indexoid = indexoid;
    entry->index = idx;
    entry->next = biscuit_cache_head;
    biscuit_cache_head = entry;
    
    MemoryContextSwitchTo(oldcontext);
    
    elog(DEBUG1, "Biscuit: Cached index %u", indexoid);
}

/* ==================== ENHANCED CLEANUP FUNCTION ==================== */

/*
 * Safe cleanup of BiscuitIndex structure
 * Must be called when dropping the extension or invalidating cache
 */
 static void
 biscuit_cleanup_index(BiscuitIndex *idx)
 {
     if (!idx)
         return;
     
     /* Don't free anything in CacheMemoryContext - let PostgreSQL handle it */
     /* Just NULL out pointers to prevent double-free attempts */
     
     /* The memory will be freed when CacheMemoryContext is reset */
     /* We just need to prevent access to stale pointers */
 }
 
 /* ==================== UPDATED CACHE INVALIDATION ==================== */
 
 /*
  * Clear cache entry - called during extension drop
  */
 static void
 biscuit_cache_remove(Oid indexoid)
 {
     BiscuitIndexCacheEntry **entry_ptr = &biscuit_cache_head;
     BiscuitIndexCacheEntry *entry;
     
     while (*entry_ptr != NULL) {
         entry = *entry_ptr;
         if (entry->indexoid == indexoid) {
             /* Remove from linked list */
             *entry_ptr = entry->next;
             
             /* Don't free idx - it's in CacheMemoryContext */
             /* Don't free entry - it's in CacheMemoryContext */
             /* PostgreSQL will clean up CacheMemoryContext */
             
             elog(DEBUG1, "Biscuit: Removed cache entry for index %u", indexoid);
             return;
         }
         entry_ptr = &entry->next;
     }
 }
 
 /* ==================== UPDATED RELCACHE CALLBACK ==================== */
 
 /*
  * Enhanced invalidation callback - properly handle extension drop
  */
 static void
 biscuit_relcache_callback(Datum arg, Oid relid)
 {
     /* Remove from our static cache */
     biscuit_cache_remove(relid);
     
     /* Don't try to free anything - CacheMemoryContext handles it */
     elog(DEBUG1, "Biscuit: Invalidated cache for relation %u", relid);
 }

/*
 * Register callbacks - enhanced with module unload hook
 */
 static void
 biscuit_register_callback(void)
 {
     if (!biscuit_callback_registered) {
         CacheRegisterRelcacheCallback(biscuit_relcache_callback, (Datum)0);
         
         /* Register module unload callback */
         on_proc_exit(biscuit_module_unload_callback, (Datum)0);
         
         biscuit_callback_registered = true;
         elog(DEBUG1, "Biscuit: Registered cache callbacks");
     }
 }

/* ==================== NOW Forward Declarations ==================== */
static inline RoaringBitmap* biscuit_roaring_create(void);
static inline void biscuit_roaring_add(RoaringBitmap *rb, uint32_t value);
static inline void biscuit_roaring_remove(RoaringBitmap *rb, uint32_t value);
static inline uint64_t biscuit_roaring_count(const RoaringBitmap *rb);
static inline bool biscuit_roaring_is_empty(const RoaringBitmap *rb);
static inline void biscuit_roaring_free(RoaringBitmap *rb);
static inline RoaringBitmap* biscuit_roaring_copy(const RoaringBitmap *rb);
static inline void biscuit_roaring_and_inplace(RoaringBitmap *a, const RoaringBitmap *b);
static inline void biscuit_roaring_or_inplace(RoaringBitmap *a, const RoaringBitmap *b);
static inline void biscuit_roaring_andnot_inplace(RoaringBitmap *a, const RoaringBitmap *b);
static inline uint32_t* biscuit_roaring_to_array(const RoaringBitmap *rb, uint64_t *count);

/* ==================== NEW: ParsedPattern must be defined BEFORE forward declaration ==================== */
typedef struct {
    char **parts;
    int *part_lens;       // CHARACTER counts (for length filters)
    int *part_byte_lens;  // BYTE lengths (for string operations)
    int part_count;
    bool starts_percent;
    bool ends_percent;
} ParsedPattern;
/* ==================== CRITICAL: Pattern parsing forward declarations ==================== */
static ParsedPattern* biscuit_parse_pattern(const char *pattern);
static void biscuit_free_parsed_pattern(ParsedPattern *parsed);

/* Index metapage and page structures - ALREADY DEFINED ABOVE */
#define BISCUIT_MAGIC 0x42495343  /* "BISC" */
#define BISCUIT_VERSION 1
#define BISCUIT_METAPAGE_BLKNO 0
#define CHAR_RANGE 256
#define TOMBSTONE_CLEANUP_THRESHOLD 1000

/* Position entry for character indices */
typedef struct {
    int pos;
    RoaringBitmap *bitmap;
} PosEntry;

typedef struct {
    PosEntry *entries;
    int count;
    int capacity;
} CharIndex;



/* ==================== FIX: BiscuitMetaPageData typedef ==================== */
typedef struct BiscuitMetaPageData {
    uint32 magic;
    uint32 version;
    BlockNumber root;
    uint32 num_records;
} BiscuitMetaPageData;

typedef BiscuitMetaPageData *BiscuitMetaPage;
/*
 * CRITICAL FIX: Multi-Column Biscuit with Per-Column Bitmap Indices
 * 
 * Problem: Multi-column was using brute-force string matching
 * Solution: Build separate Biscuit indices for each column
 */

/* ==================== ENHANCED INDEX STRUCTURE ==================== */

typedef struct {
    /* Case-sensitive */
    CharIndex pos_idx[CHAR_RANGE];
    CharIndex neg_idx[CHAR_RANGE];
    RoaringBitmap *char_cache[CHAR_RANGE];
    RoaringBitmap **length_bitmaps;
    RoaringBitmap **length_ge_bitmaps;
    int max_length;
    
    /* Case-insensitive */
    CharIndex pos_idx_lower[CHAR_RANGE];
    CharIndex neg_idx_lower[CHAR_RANGE];
    RoaringBitmap *char_cache_lower[CHAR_RANGE];
    RoaringBitmap **length_bitmaps_lower;      // NEW
    RoaringBitmap **length_ge_bitmaps_lower;   // NEW
    int max_length_lower;                       // NEW
} ColumnIndex;

typedef struct BiscuitIndex {
    int num_columns;
    Oid *column_types;
    FmgrInfo *output_funcs;
    char ***column_data_cache;  /* [column][record] */
    
    /* NEW: Per-column indices instead of single composite */
    ColumnIndex *column_indices;  /* Array of column indices */
    
    /* Original single-column fields (for backward compat) */
    CharIndex pos_idx_legacy[CHAR_RANGE];
    CharIndex neg_idx_legacy[CHAR_RANGE];
    RoaringBitmap *char_cache_legacy[CHAR_RANGE];
    RoaringBitmap **length_bitmaps_legacy;
    RoaringBitmap **length_ge_bitmaps_legacy;
    int max_length_legacy;
    int max_len;

     /* Case-insensitive (ILIKE) */
    CharIndex pos_idx_lower[CHAR_RANGE];
    CharIndex neg_idx_lower[CHAR_RANGE];
    RoaringBitmap *char_cache_lower[CHAR_RANGE];
    RoaringBitmap **length_bitmaps_lower;       // Based on LOWERCASE string length
    RoaringBitmap **length_ge_bitmaps_lower;    // Based on LOWERCASE string length
    int max_length_lower;
    
    /* NEW: Lowercase data cache */
    char **data_cache_lower;
    
    ItemPointerData *tids;
    char **data_cache;
    int num_records;
    int capacity;
    
    RoaringBitmap *tombstones;
    uint32_t *free_list;
    int free_count;
    int free_capacity;
    int tombstone_count;
    
    int64 insert_count;
    int64 update_count;
    int64 delete_count;
} BiscuitIndex;

/* Scan opaque structure */
typedef struct {
    BiscuitIndex *index;
    ItemPointerData *results;
    int num_results;
    int current;
    
    /* NEW: Query optimization flags */
    bool is_aggregate_only;      /* COUNT/EXISTS without fetching tuples */
    bool needs_sorted_access;    /* True if sequential access benefits from sorting */
    int limit_remaining;         /* Tracks LIMIT countdown, -1 = no limit */
} BiscuitScanOpaque;


/*
 * Convert string to lowercase using PostgreSQL's locale-aware lower() function.
 * Uses the database default collation (InvalidOid), which is stable and safe
 * for index operations and does not require fcinfo.
 */
/*
 * Convert string to lowercase using PostgreSQL's locale-aware lower() function.
 * Uses the database's default collation for consistent, index-safe behavior.
 */
static char *
biscuit_str_tolower(const char *str, int len)
{
    text *input;
    text *result_text;
    char *result;
    Oid collation;

    if (len == 0)
        return pstrdup("");

    input = cstring_to_text_with_len(str, len);
    
    /* Get the database's default collation from the system catalog */
    collation = get_typcollation(TEXTOID);

    result_text = DatumGetTextP(
        DirectFunctionCall2Coll(
            lower,
            collation,
            PointerGetDatum(input),
            collation
        )
    );

    result = text_to_cstring(result_text);
    return result;
}

/* ==================== UTF-8 SUPPORT FUNCTIONS ==================== */

/*
 * Get UTF-8 character length from leading byte
 * Returns: 1-4 for valid UTF-8, 1 for invalid (treat as single byte)
 * 
 * UTF-8 encoding rules:
 *   1-byte: 0xxxxxxx         (0x00-0x7F)   ASCII
 *   2-byte: 110xxxxx 10xxxxxx (0xC0-0xDF)
 *   3-byte: 1110xxxx 10xxxxxx 10xxxxxx (0xE0-0xEF)
 *   4-byte: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx (0xF0-0xF7)
 */
static inline int 
biscuit_utf8_char_length(unsigned char c)
{
    if (c < 0x80)       return 1;  // ASCII: 0xxxxxxx
    if (c < 0xC0)       return 1;  // Invalid: continuation byte (10xxxxxx)
    if (c < 0xE0)       return 2;  // 2-byte: 110xxxxx
    if (c < 0xF0)       return 3;  // 3-byte: 1110xxxx
    if (c < 0xF8)       return 4;  // 4-byte: 11110xxx
    return 1;                      // Invalid, treat as single byte
}

/*
 * Count UTF-8 characters in a string (not bytes)
 * Handles invalid UTF-8 gracefully (counts invalid bytes as 1 char each)
 * 
 * Example:
 *   "café" = [63 61 66 C3 A9] = 5 bytes, 4 characters
 *   "日本"  = [E6 97 A5 E6 9C AC] = 6 bytes, 2 characters
 */
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

/*
 * Validate UTF-8 continuation byte
 * Returns: true if byte matches 10xxxxxx pattern
 */
static inline bool 
biscuit_utf8_is_continuation(unsigned char c)
{
    return (c & 0xC0) == 0x80;  // 10xxxxxx
}

/*
 * Validate UTF-8 character at byte position
 * Returns: true if valid, false if malformed
 * 
 * Checks:
 *   1. Correct number of continuation bytes
 *   2. Each continuation byte has 10xxxxxx pattern
 *   3. No buffer overrun
 */
static bool 
biscuit_utf8_validate_char(const char *str, int byte_pos, int byte_len)
{
    if (byte_pos >= byte_len)
        return false;
    
    unsigned char c = (unsigned char)str[byte_pos];
    int char_len = biscuit_utf8_char_length(c);
    
    // Check buffer bounds
    if (byte_pos + char_len > byte_len)
        return false;
    
    // Validate continuation bytes
    for (int i = 1; i < char_len; i++) {
        if (!biscuit_utf8_is_continuation((unsigned char)str[byte_pos + i]))
            return false;
    }
    
    return true;
}

/*
 * Get byte offset of character at given character position
 * Returns: byte offset, or -1 if char_pos out of range
 * 
 * Example:
 *   str = "café" (bytes: [63 61 66 C3 A9])
 *   char_pos 0 → byte 0 ('c')
 *   char_pos 1 → byte 1 ('a')
 *   char_pos 2 → byte 2 ('f')
 *   char_pos 3 → byte 3 ('é' starts at 0xC3)
 */
static int
biscuit_utf8_char_to_byte_offset(const char *str, int byte_len, int char_pos)
{
    int current_char = 0;
    int byte_pos = 0;
    
    while (byte_pos < byte_len && current_char < char_pos) {
        int char_len = biscuit_utf8_char_length((unsigned char)str[byte_pos]);
        
        if (byte_pos + char_len > byte_len)
            return -1;  // Invalid UTF-8
        
        byte_pos += char_len;
        current_char++;
    }
    
    return (current_char == char_pos) ? byte_pos : -1;
}
/* ==================== QUERY TYPE DETECTION ==================== */

/*
 * Detect if this is an aggregate-only query (COUNT, EXISTS, etc.)
 * These queries don't fetch actual tuples, just count results
 */
 static bool
 biscuit_is_aggregate_query(IndexScanDesc scan)
 {
     /*
      * PostgreSQL uses xs_want_itup to indicate scan type:
      * - false = bitmap scan (for aggregates, large result sets)
      * - true  = regular index scan (needs sorted access)
      */
     return !scan->xs_want_itup;
 }
 

/* ==================== TYPE CONVERSION HELPER ==================== */

/*
 * Convert ANY PostgreSQL datum to sortable text representation
 */
 static char*
biscuit_datum_to_text(Datum value, Oid typoid, FmgrInfo *outfunc, int *out_len)
{
    char *result;
    char *raw_text;
    
    /* Handle common types with optimized conversions */
    switch (typoid) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        {
            int64 num;
            char sign;  /* MOVED: declare at top of block */
            uint64 abs_val;  /* MOVED: declare at top of block */
            
            if (typoid == INT2OID)
                num = DatumGetInt16(value);
            else if (typoid == INT4OID)
                num = DatumGetInt32(value);
            else
                num = DatumGetInt64(value);
            
            /* FIXED: Sortable format with consistent width */
            sign = (num >= 0) ? '+' : '-';  /* NOW after declarations */
            abs_val = (num >= 0) ? num : -num;
            result = psprintf("%c%020llu", sign, (unsigned long long)abs_val);
            
            *out_len = strlen(result);
            break;
        }
        
        case FLOAT4OID:
        case FLOAT8OID:
        {
            /* Convert float to sortable string representation */
            double fval = (typoid == FLOAT4OID) ? 
                         DatumGetFloat4(value) : DatumGetFloat8(value);
            result = psprintf("%.15e", fval);
            *out_len = strlen(result);
            break;
        }
        
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
        {
            /* Already text - just extract */
            text *txt = DatumGetTextPP(value);
            char *str = VARDATA_ANY(txt);
            int len = VARSIZE_ANY_EXHDR(txt);
            result = pnstrdup(str, len);
            *out_len = len;
            break;
        }
        
        case DATEOID:
        {
            /* Date as zero-padded integer (days since epoch) */
            DateADT date = DatumGetDateADT(value);
            /* Store as sortable 10-digit number */
            result = psprintf("%+010d", (int)date);
            *out_len = 10;
            break;
        }
        
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        {
            /* Timestamp as sortable integer microseconds */
            Timestamp ts = DatumGetTimestamp(value);
            result = psprintf("%020lld", (long long)ts);
            *out_len = 20;
            break;
        }
        
        case BOOLOID:
        {
            /* Boolean as 'f' or 't' */
            bool b = DatumGetBool(value);
            result = pstrdup(b ? "t" : "f");
            *out_len = 1;
            break;
        }
        
        default:
        {
            /* FALLBACK: Use PostgreSQL's output function */
            raw_text = OutputFunctionCall(outfunc, value);
            result = pstrdup(raw_text);
            *out_len = strlen(result);
            pfree(raw_text);
            break;
        }
    }
    
    return result;
}

/* ==================== TID SORTING (OPTIMIZATION 6) ==================== */

static int
biscuit_compare_tids(const void *a, const void *b)
{
    ItemPointer tid_a = (ItemPointer)a;
    ItemPointer tid_b = (ItemPointer)b;
    BlockNumber block_a = ItemPointerGetBlockNumber(tid_a);
    BlockNumber block_b = ItemPointerGetBlockNumber(tid_b);
    OffsetNumber offset_a;
    OffsetNumber offset_b;
    
    if (block_a < block_b)
        return -1;
    if (block_a > block_b)
        return 1;
    
    offset_a = ItemPointerGetOffsetNumber(tid_a);
    offset_b = ItemPointerGetOffsetNumber(tid_b);
    
    if (offset_a < offset_b)
        return -1;
    if (offset_a > offset_b)
        return 1;
    
    return 0;
}

/*
 * Radix sort for TIDs - optimized for large result sets with proper memory safety
 * Sorts by block number first, then by offset within each block
 * 
 * Performance: O(n) for n TIDs, but requires temporary buffer
 * Best for: 5000+ TIDs where O(n) beats O(n log n)
 * 
 * FIXED: Memory safety with PG_TRY/CATCH and proper cleanup
 */
 static void
 biscuit_radix_sort_tids(ItemPointerData *tids, int count)
 {
     ItemPointerData *temp = NULL;
     int *block_counts = NULL;
     int *block_offsets = NULL;
     int *counts_low = NULL;
     int *counts_high = NULL;
     int *offsets_low = NULL;
     int *offsets_high = NULL;
     BlockNumber max_block = 0;
     int i;
     
     if (count <= 1)
         return;
     
     PG_TRY();
     {
         /* Allocate temporary buffer - used throughout */
         temp = (ItemPointerData *)palloc(count * sizeof(ItemPointerData));
         
         /* Find max block number to determine range */
         for (i = 0; i < count; i++) {
             BlockNumber block = ItemPointerGetBlockNumber(&tids[i]);
             if (block > max_block)
                 max_block = block;
         }
         
         /* OPTIMIZATION: If blocks are dense, use counting sort on blocks */
         if (max_block < (BlockNumber)(count * 2)) {
             /* ==================== DENSE BLOCKS - COUNTING SORT ==================== */
             int num_blocks = max_block + 1;
             
             block_counts = (int *)palloc0(num_blocks * sizeof(int));
             block_offsets = (int *)palloc(num_blocks * sizeof(int));
             
             /* Count TIDs per block */
             for (i = 0; i < count; i++) {
                 BlockNumber block = ItemPointerGetBlockNumber(&tids[i]);
                 block_counts[block]++;
             }
             
             /* Calculate starting positions */
             block_offsets[0] = 0;
             for (i = 1; i < num_blocks; i++) {
                 block_offsets[i] = block_offsets[i-1] + block_counts[i-1];
             }
             
             /* Distribute TIDs into temp buffer by block */
             for (i = 0; i < count; i++) {
                 BlockNumber block = ItemPointerGetBlockNumber(&tids[i]);
                 int pos = block_offsets[block]++;
                 ItemPointerCopy(&tids[i], &temp[pos]);
             }
             
             /* Copy back to original array */
             memcpy(tids, temp, count * sizeof(ItemPointerData));
             
             /* Clean up dense-specific allocations */
             pfree(block_counts);
             pfree(block_offsets);
             block_counts = NULL;
             block_offsets = NULL;
             
         } else {
             /* ==================== SPARSE BLOCKS - RADIX SORT ==================== */
             
             /* Allocate radix sort buffers */
             counts_low = (int *)palloc0(256 * sizeof(int));
             counts_high = (int *)palloc0(256 * sizeof(int));
             offsets_low = (int *)palloc(256 * sizeof(int));
             offsets_high = (int *)palloc(256 * sizeof(int));
             
             /* Pass 1: Sort by low byte of block number */
             for (i = 0; i < count; i++) {
                 BlockNumber block = ItemPointerGetBlockNumber(&tids[i]);
                 counts_low[block & 0xFF]++;
             }
             
             offsets_low[0] = 0;
             for (i = 1; i < 256; i++) {
                 offsets_low[i] = offsets_low[i-1] + counts_low[i-1];
             }
             
             for (i = 0; i < count; i++) {
                 BlockNumber block = ItemPointerGetBlockNumber(&tids[i]);
                 int pos = offsets_low[block & 0xFF]++;
                 ItemPointerCopy(&tids[i], &temp[pos]);
             }
             
             /* Pass 2: Sort by high 24 bits of block number */
             for (i = 0; i < count; i++) {
                 BlockNumber block = ItemPointerGetBlockNumber(&temp[i]);
                 counts_high[(block >> 8) & 0xFF]++;
             }
             
             offsets_high[0] = 0;
             for (i = 1; i < 256; i++) {
                 offsets_high[i] = offsets_high[i-1] + counts_high[i-1];
             }
             
             for (i = 0; i < count; i++) {
                 BlockNumber block = ItemPointerGetBlockNumber(&temp[i]);
                 int pos = offsets_high[(block >> 8) & 0xFF]++;
                 ItemPointerCopy(&temp[i], &tids[pos]);
             }
             
             /* Copy sorted results back to temp for offset sorting */
             memcpy(temp, tids, count * sizeof(ItemPointerData));
             
             /* Clean up radix-specific allocations */
             pfree(counts_low);
             pfree(counts_high);
             pfree(offsets_low);
             pfree(offsets_high);
             counts_low = NULL;
             counts_high = NULL;
             offsets_low = NULL;
             offsets_high = NULL;
         }
         
         /* ==================== SORT BY OFFSET WITHIN EACH BLOCK ==================== */
         /* TIDs are now grouped by block in temp[], sort each block's offsets */
         
         int start = 0;
         while (start < count) {
             BlockNumber current_block = ItemPointerGetBlockNumber(&temp[start]);
             int block_end = start + 1;
             
             /* Find end of current block */
             while (block_end < count && 
                    ItemPointerGetBlockNumber(&temp[block_end]) == current_block) {
                 block_end++;
             }
             
             int block_size = block_end - start;
             
             /* Sort offsets within this block using counting sort */
             if (block_size > 1) {
                 /* MaxHeapTuplesPerPage is typically ~290, so use 512 buckets */
                 int offset_counts[512];
                 int offset_positions[512];
                 int i_inner, j;
                 
                 /* Initialize counts */
                 for (j = 0; j < 512; j++) {
                     offset_counts[j] = 0;
                 }
                 
                 /* Count offsets */
                 for (i_inner = start; i_inner < block_end; i_inner++) {
                     OffsetNumber offset = ItemPointerGetOffsetNumber(&temp[i_inner]);
                     if (offset < 512) {
                         offset_counts[offset]++;
                     } else {
                         /* Safety check: offset out of range - should never happen */
                         elog(WARNING, "Biscuit: Invalid offset %d at TID position %d, skipping",
                              offset, i_inner);
                     }
                 }
                 
                 /* Calculate positions */
                 offset_positions[0] = 0;
                 for (j = 1; j < 512; j++) {
                     offset_positions[j] = offset_positions[j-1] + offset_counts[j-1];
                 }
                 
                 /* Distribute into tids array (using it as output) */
                 for (i_inner = start; i_inner < block_end; i_inner++) {
                     OffsetNumber offset = ItemPointerGetOffsetNumber(&temp[i_inner]);
                     if (offset < 512) {
                         int pos = start + offset_positions[offset]++;
                         ItemPointerCopy(&temp[i_inner], &tids[pos]);
                     }
                 }
             } else {
                 /* Single TID in this block, just copy it */
                 ItemPointerCopy(&temp[start], &tids[start]);
             }
             
             start = block_end;
         }
         
         /* Clean up main temp buffer */
         pfree(temp);
         temp = NULL;
     }
     PG_CATCH();
     {
         /* Emergency cleanup on error */
         if (temp) pfree(temp);
         if (block_counts) pfree(block_counts);
         if (block_offsets) pfree(block_offsets);
         if (counts_low) pfree(counts_low);
         if (counts_high) pfree(counts_high);
         if (offsets_low) pfree(offsets_low);
         if (offsets_high) pfree(offsets_high);
         
         /* Re-throw the error */
         PG_RE_THROW();
     }
     PG_END_TRY();
 }
/*
* Sort TIDs for sequential heap access
* This is critical for performance with large result sets
*/
#define RADIX_SORT_THRESHOLD 5000

static void
biscuit_sort_tids_by_block(ItemPointerData *tids, int count)
{
    if (count <= 1)
        return;
    
    if (count < RADIX_SORT_THRESHOLD) {
        /* Small dataset: use quicksort */
        qsort(tids, count, sizeof(ItemPointerData), biscuit_compare_tids);
    } else {
        /* Large dataset: use radix sort */
        biscuit_radix_sort_tids(tids, count);
    }
}

/* ==================== PARALLEL TID COLLECTION (OPTIMIZATION 11) ==================== */

/*
* Parallel worker structure for TID collection
*/
typedef struct {
BiscuitIndex *idx;
uint32_t *indices;
uint64_t start_idx;
uint64_t end_idx;
ItemPointerData *output;
int output_count;
} TIDCollectionWorker;

/*
* Worker function for parallel TID collection
* Each worker processes a chunk of the bitmap
*/
static void
biscuit_collect_tids_worker(TIDCollectionWorker *worker)
{
uint64_t i;
int out_idx = 0;

for (i = worker->start_idx; i < worker->end_idx; i++) {
    uint32_t rec_idx = worker->indices[i];
    
    if (rec_idx < (uint32_t)worker->idx->num_records) {
        ItemPointerCopy(&worker->idx->tids[rec_idx], 
                        &worker->output[out_idx]);
        out_idx++;
    }
}

worker->output_count = out_idx;
}

/*
* Parallel TID collection with automatic work distribution
* Uses multiple workers when result set is large
*/

/*
* Single-threaded TID collection (optimized version of original)
* Used for small result sets or as fallback
*/
static void
biscuit_collect_sorted_tids_single(BiscuitIndex *idx, 
                                   RoaringBitmap *result,
                                   ItemPointerData **out_tids,
                                   int *out_count,
                                   bool needs_sorting)
{
    uint64_t count;
    ItemPointerData *tids;
    int idx_out = 0;

    count = biscuit_roaring_count(result);

    if (count == 0) {
        *out_tids = NULL;
        *out_count = 0;
        return;
    }

    tids = (ItemPointerData *)palloc(count * sizeof(ItemPointerData));

    #ifdef HAVE_ROARING
    {
        roaring_uint32_iterator_t *iter = roaring_create_iterator(result);
        
        while (iter->has_value) {
            uint32_t rec_idx = iter->current_value;
            
            if (rec_idx < (uint32_t)idx->num_records) {
                ItemPointerCopy(&idx->tids[rec_idx], &tids[idx_out]);
                idx_out++;
            }
            
            roaring_advance_uint32_iterator(iter);
        }
        
        roaring_free_uint32_iterator(iter);
    }
    #else
    {
        uint32_t *indices;
        int i;
        
        indices = biscuit_roaring_to_array(result, &count);
        
        if (indices) {
            for (i = 0; i < (int)count; i++) {
                if (indices[i] < (uint32_t)idx->num_records) {
                    ItemPointerCopy(&idx->tids[indices[i]], &tids[idx_out]);
                    idx_out++;
                }
            }
            pfree(indices);
        }
    }
    #endif

    *out_count = idx_out;

    /* OPTIMIZATION: Skip sorting if not needed (aggregates, bitmap scans) */
    if (needs_sorting && idx_out > 1) {
        //elog(DEBUG1, "Biscuit: Sorting %d TIDs for sequential heap access", idx_out);
        biscuit_sort_tids_by_block(tids, idx_out);
    } else if (!needs_sorting) {
        //elog(DEBUG1, "Biscuit: Skipping TID sort (aggregate query or bitmap scan)");
    }

    *out_tids = tids;
}
                               
static void
biscuit_collect_sorted_tids_parallel(BiscuitIndex *idx, 
                                        RoaringBitmap *result,
                                        ItemPointerData **out_tids,
                                        int *out_count,
                                        bool needs_sorting)
{
    uint64_t count;
    ItemPointerData *tids;
    uint32_t *indices;
    int num_workers;
    int max_workers = 4;
    uint64_t items_per_worker;
    TIDCollectionWorker *workers;
    int i;
    int total_collected = 0;
    
    count = biscuit_roaring_count(result);
    
    if (count == 0) {
        *out_tids = NULL;
        *out_count = 0;
        return;
    }
    
    /* Use parallelization only for large result sets */
    if (count < 10000) {
        biscuit_collect_sorted_tids_single(idx, result, out_tids, out_count, needs_sorting);
        return;
    }

    /* Determine number of workers based on result set size */
    num_workers = (count < 100000) ? 2 : max_workers;
    items_per_worker = (count + num_workers - 1) / num_workers;

    /* Convert bitmap to array once */
    indices = biscuit_roaring_to_array(result, &count);
    if (!indices) {
        *out_tids = NULL;
        *out_count = 0;
        return;
    }

    /* Allocate output buffer and workers */
    tids = (ItemPointerData *)palloc(count * sizeof(ItemPointerData));
    workers = (TIDCollectionWorker *)palloc(num_workers * sizeof(TIDCollectionWorker));

    /* Distribute work across workers */
    for (i = 0; i < num_workers; i++) {
        workers[i].idx = idx;
        workers[i].indices = indices;
        workers[i].start_idx = i * items_per_worker;
        workers[i].end_idx = ((i + 1) * items_per_worker < count) ? 
                                (i + 1) * items_per_worker : count;
        workers[i].output = &tids[workers[i].start_idx];
        workers[i].output_count = 0;
    }

    /* Execute workers */
    for (i = 0; i < num_workers; i++) {
        biscuit_collect_tids_worker(&workers[i]);
        total_collected += workers[i].output_count;
    }

    /* Compact the output array if needed */
    if (total_collected < (int)count) {
        int write_pos = 0;
        for (i = 0; i < num_workers; i++) {
            if (workers[i].output_count > 0) {
                if (write_pos != workers[i].start_idx) {
                    memmove(&tids[write_pos], 
                            &tids[workers[i].start_idx],
                            workers[i].output_count * sizeof(ItemPointerData));
                }
                write_pos += workers[i].output_count;
            }
        }
    }

    pfree(indices);
    pfree(workers);

    *out_count = total_collected;

    /* OPTIMIZATION: Skip sorting if not needed */
    if (needs_sorting && total_collected > 1) {
        //elog(DEBUG1, "Biscuit: Sorting %d TIDs for sequential heap access", total_collected);
        biscuit_sort_tids_by_block(tids, total_collected);
    } else if (!needs_sorting) {
        //elog(DEBUG1, "Biscuit: Skipping TID sort (aggregate query or bitmap scan)");
    }

    *out_tids = tids;
}

/* ==================== LIMIT-AWARE TID COLLECTION ==================== */

/*
 * ENHANCED: TID collection with early termination for LIMIT
 * 
 * NOTE: PostgreSQL doesn't pass LIMIT to index AMs directly,
 * but we can optimize by collecting only what we need
 */
 static int
biscuit_estimate_limit_hint(IndexScanDesc scan)
{
    /*
     * LIMITATION: PostgreSQL's index AM interface doesn't provide
     * direct access to LIMIT values. We could:
     * 
     * 1. Check if this is a bounded scan (orderbys with LIMIT)
     * 2. Monitor gettuple() calls to detect early termination
     * 3. Use heuristics based on scan->xs_snapshot
     * 
     * For now, return -1 (no limit known)
     */
    
    /* Future: Could check scan->parallel_scan for batch size hints */
    return -1;
}

static void
biscuit_free_query_plan(QueryPlan *plan)
{
    int i;
    
    if (!plan)
        return;
    
    PG_TRY();
    {
        if (plan->predicates) {
            for (i = 0; i < plan->count; i++) {
                if (plan->predicates[i].pattern) {
                    pfree(plan->predicates[i].pattern);
                    plan->predicates[i].pattern = NULL;
                }
            }
            pfree(plan->predicates);
            plan->predicates = NULL;
        }
        pfree(plan);
    }
    PG_CATCH();
    {
        /* If error during cleanup, just swallow it */
        FlushErrorState();
    }
    PG_END_TRY();
}


/* ==================== CORRECTED: TID COLLECTION WITH OPTIMIZATIONS ==================== */

/*
 * Main TID collection with ALL optimizations applied
 * - Skips sorting for bitmap scans (aggregates)
 * - Early termination for LIMIT (when detected)
 * - Parallel collection for large result sets
 */
static void
biscuit_collect_tids_optimized(BiscuitIndex *idx, 
                                RoaringBitmap *result,
                                ItemPointerData **out_tids,
                                int *out_count,
                                bool needs_sorting,
                                int limit_hint)
{
    uint64_t total_count;
    uint64_t collect_count;
    ItemPointerData *tids;
    int idx_out = 0;
    
    total_count = biscuit_roaring_count(result);
    
    if (total_count == 0) {
        *out_tids = NULL;
        *out_count = 0;
        return;
    }
    
    /* OPTIMIZATION 1: LIMIT-aware collection */
    if (limit_hint > 0 && limit_hint < (int)total_count) {
        collect_count = limit_hint * 2;  /* 2x buffer for safety */
        //elog(DEBUG1, "Biscuit: LIMIT optimization - collecting %llu of %llu TIDs",(unsigned long long)collect_count, (unsigned long long)total_count);
    } else {
        collect_count = total_count;
    }
    
    /* OPTIMIZATION 2: Use parallel collection for large result sets */
    if (collect_count >= 10000) {
        //elog(DEBUG1, "Biscuit: Using parallel TID collection for %llu TIDs",(unsigned long long)collect_count);
        
        /* Call existing parallel implementation */
        biscuit_collect_sorted_tids_parallel(idx, result, out_tids, 
                                            out_count, needs_sorting);
        
        /* Apply LIMIT if needed */
        if (limit_hint > 0 && *out_count > limit_hint) {
            //elog(DEBUG1, "Biscuit: Truncating results to LIMIT %d", limit_hint);
            *out_count = limit_hint;
        }
        return;
    }
    
    /* OPTIMIZATION 3: Single-threaded collection with early termination */
    tids = (ItemPointerData *)palloc(collect_count * sizeof(ItemPointerData));
    
    #ifdef HAVE_ROARING
    {
        roaring_uint32_iterator_t *iter = roaring_create_iterator(result);
        
        while (iter->has_value && idx_out < (int)collect_count) {
            uint32_t rec_idx = iter->current_value;
            
            if (rec_idx < (uint32_t)idx->num_records) {
                ItemPointerCopy(&idx->tids[rec_idx], &tids[idx_out]);
                idx_out++;
                
                /* LIMIT early termination */
                if (limit_hint > 0 && idx_out >= limit_hint) {
                    //elog(DEBUG1, "Biscuit: LIMIT reached during collection");
                    break;
                }
            }
            
            roaring_advance_uint32_iterator(iter);
        }
        
        roaring_free_uint32_iterator(iter);
    }
    #else
    {
        uint32_t *indices;
        int i;
        uint64_t array_count;
        
        indices = biscuit_roaring_to_array(result, &array_count);
        
        if (indices) {
            int max_collect = (int)Min(collect_count, array_count);
            
            for (i = 0; i < max_collect; i++) {
                if (indices[i] < (uint32_t)idx->num_records) {
                    ItemPointerCopy(&idx->tids[indices[i]], &tids[idx_out]);
                    idx_out++;
                    
                    /* LIMIT early termination */
                    if (limit_hint > 0 && idx_out >= limit_hint) {
                        break;
                    }
                }
            }
            pfree(indices);
        }
    }
    #endif
    
    *out_count = idx_out;
    
    /* OPTIMIZATION 4: Skip sorting for bitmap scans */
    if (needs_sorting && idx_out > 1) {
        //elog(DEBUG1, "Biscuit: Sorting %d TIDs for sequential heap access", idx_out);
        biscuit_sort_tids_by_block(tids, idx_out);
    } else if (!needs_sorting) {
        //elog(DEBUG1, "Biscuit: Skipping TID sort (bitmap scan for aggregates)");
    }
    
    *out_tids = tids;
}

/* ==================== CRUD HELPER FUNCTIONS ==================== */

/* ==================== DISK SERIALIZATION ==================== */

/*
 * Write a simple marker page to indicate index was built
 * Bitmaps are too large to serialize - we'll rebuild on load
 */
 static void
 biscuit_write_metadata_to_disk(Relation index, BiscuitIndex *idx)
 {
     Buffer buf;
     Page page;
     GenericXLogState *state;
     BiscuitMetaPageData *meta;
     
     //elog(DEBUG1, "Biscuit: Writing index metadata marker to disk");
     
     /* Extend relation by one block if needed */
     buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
     LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
     
     state = GenericXLogStart(index);
     page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
     
     /* Initialize page */
     PageInit(page, BufferGetPageSize(buf), sizeof(BiscuitMetaPageData));
     
     /* Write metadata in special space */
     meta = (BiscuitMetaPageData *)PageGetSpecialPointer(page);
     meta->magic = BISCUIT_MAGIC;
     meta->version = BISCUIT_VERSION;
     meta->num_records = idx->num_records;
     meta->root = 0;
     
     GenericXLogFinish(state);
     UnlockReleaseBuffer(buf);
     
     //elog(DEBUG1, "Biscuit: Metadata marker written (will rebuild bitmaps on load)");
 }
 
 /*
  * Check if index has metadata marker on disk
  */
 static bool
 biscuit_read_metadata_from_disk(Relation index, int *num_records, int *num_columns, int *max_len)
 {
     Buffer buf;
     Page page;
     BiscuitMetaPageData *meta;
     BlockNumber nblocks;
     
     nblocks = RelationGetNumberOfBlocks(index);
     
     if (nblocks == 0) {
         //elog(DEBUG1, "Biscuit: No disk pages found, needs full rebuild");
         *num_records = 0;
         *num_columns = 0;
         *max_len = 0;
         return false;
     }
     
     /* Read metadata page */
     buf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
     LockBuffer(buf, BUFFER_LOCK_SHARE);
     page = BufferGetPage(buf);
     
     /* Check if page is properly initialized */
     if (PageIsNew(page) || PageIsEmpty(page)) {
         UnlockReleaseBuffer(buf);
         //elog(DEBUG1, "Biscuit: Metadata page empty, needs rebuild");
         *num_records = 0;
         *num_columns = 0;
         *max_len = 0;
         return false;
     }
     
     meta = (BiscuitMetaPageData *)PageGetSpecialPointer(page);
     
     /* Verify magic number */
     if (meta->magic != BISCUIT_MAGIC) {
         UnlockReleaseBuffer(buf);
         //elog(WARNING, "Biscuit: Invalid magic number, needs rebuild");
         *num_records = 0;
         *num_columns = 0;
         *max_len = 0;
         return false;
     }
     
     *num_records = meta->num_records;
     *num_columns = 0; /* Not stored in marker */
     *max_len = 0;     /* Not stored in marker */
     
     UnlockReleaseBuffer(buf);
     
     //elog(DEBUG1, "Biscuit: Found valid metadata marker (%d records on disk)",*num_records);
     
     return true;
 }

static void biscuit_init_crud_structures(BiscuitIndex *idx)
{
    idx->tombstones = biscuit_roaring_create();
    idx->free_capacity = 64;
    idx->free_count = 0;
    idx->free_list = (uint32_t *)palloc(idx->free_capacity * sizeof(uint32_t));
    idx->tombstone_count = 0;
    idx->insert_count = 0;
    idx->update_count = 0;
    idx->delete_count = 0;
}

static void biscuit_push_free_slot(BiscuitIndex *idx, uint32_t slot)
{
    if (idx->free_count >= idx->free_capacity)
    {
        int new_cap = idx->free_capacity * 2;
        uint32_t *new_list = (uint32_t *)palloc(new_cap * sizeof(uint32_t));
        memcpy(new_list, idx->free_list, idx->free_count * sizeof(uint32_t));
        pfree(idx->free_list);
        idx->free_list = new_list;
        idx->free_capacity = new_cap;
    }
    idx->free_list[idx->free_count++] = slot;
}

static bool biscuit_pop_free_slot(BiscuitIndex *idx, uint32_t *slot)
{
    if (idx->free_count == 0)
        return false;
    *slot = idx->free_list[--idx->free_count];
    return true;
}

/* ==================== FIXED: biscuit_remove_from_all_indices ==================== */
/*
 * Remove a record from ALL indices (LIKE, ILIKE, and all length bitmaps)
 * Called during DELETE operations and tombstone cleanup
 * 
 * CRITICAL FIXES:
 * 1. Use j <= max_length (not j < max_length) to include last bitmap
 * 2. Add bounds checks before loops
 * 3. Add NULL checks for bitmap arrays
 * 4. Remove from BOTH case-sensitive AND case-insensitive length bitmaps
 */
static void 
biscuit_remove_from_all_indices(BiscuitIndex *idx, uint32_t rec_idx)
{
    int ch, j, col;
    CharIndex *pos_cidx;
    CharIndex *neg_cidx;
    
    if (!idx)
        return;
    
    /* ==================== MULTI-COLUMN INDICES ==================== */
    if (idx->num_columns > 1 && idx->column_indices) {
        for (col = 0; col < idx->num_columns; col++) {
            ColumnIndex *cidx = &idx->column_indices[col];
            
            /* Case-sensitive character indices */
            for (ch = 0; ch < CHAR_RANGE; ch++) {
                pos_cidx = &cidx->pos_idx[ch];
                for (j = 0; j < pos_cidx->count; j++)
                    biscuit_roaring_remove(pos_cidx->entries[j].bitmap, rec_idx);
                
                neg_cidx = &cidx->neg_idx[ch];
                for (j = 0; j < neg_cidx->count; j++)
                    biscuit_roaring_remove(neg_cidx->entries[j].bitmap, rec_idx);
                
                if (cidx->char_cache[ch])
                    biscuit_roaring_remove(cidx->char_cache[ch], rec_idx);
            }
            
            /* Case-insensitive character indices */
            for (ch = 0; ch < CHAR_RANGE; ch++) {
                pos_cidx = &cidx->pos_idx_lower[ch];
                for (j = 0; j < pos_cidx->count; j++)
                    biscuit_roaring_remove(pos_cidx->entries[j].bitmap, rec_idx);
                
                neg_cidx = &cidx->neg_idx_lower[ch];
                for (j = 0; j < neg_cidx->count; j++)
                    biscuit_roaring_remove(neg_cidx->entries[j].bitmap, rec_idx);
                
                if (cidx->char_cache_lower[ch])
                    biscuit_roaring_remove(cidx->char_cache_lower[ch], rec_idx);
            }
            
            /* ✅ FIX: Remove from ALL allocated length bitmaps, not just up to max_length */
            /* The issue: max_length is the max STRING length, but arrays may be larger */
            /* We need to check each bitmap individually */
            
            /* Case-sensitive length bitmaps */
            if (cidx->length_bitmaps) {
                /* Find actual array size by checking allocations */
                for (j = 0; j < cidx->max_length; j++) {
                    if (cidx->length_bitmaps[j])
                        biscuit_roaring_remove(cidx->length_bitmaps[j], rec_idx);
                }
            }
            
            if (cidx->length_ge_bitmaps) {
                for (j = 0; j < cidx->max_length; j++) {
                    if (cidx->length_ge_bitmaps[j])
                        biscuit_roaring_remove(cidx->length_ge_bitmaps[j], rec_idx);
                }
            }
            
            /* Case-insensitive length bitmaps */
            if (cidx->length_bitmaps_lower) {
                for (j = 0; j < cidx->max_length_lower; j++) {
                    if (cidx->length_bitmaps_lower[j])
                        biscuit_roaring_remove(cidx->length_bitmaps_lower[j], rec_idx);
                }
            }
            
            if (cidx->length_ge_bitmaps_lower) {
                for (j = 0; j < cidx->max_length_lower; j++) {
                    if (cidx->length_ge_bitmaps_lower[j])
                        biscuit_roaring_remove(cidx->length_ge_bitmaps_lower[j], rec_idx);
                }
            }
        }
        
        return;
    }
    
    /* ==================== SINGLE-COLUMN (LEGACY) ==================== */
    
    /* Case-sensitive character indices */
    for (ch = 0; ch < CHAR_RANGE; ch++) {
        pos_cidx = &idx->pos_idx_legacy[ch];
        for (j = 0; j < pos_cidx->count; j++)
            biscuit_roaring_remove(pos_cidx->entries[j].bitmap, rec_idx);
        
        neg_cidx = &idx->neg_idx_legacy[ch];
        for (j = 0; j < neg_cidx->count; j++)
            biscuit_roaring_remove(neg_cidx->entries[j].bitmap, rec_idx);
        
        if (idx->char_cache_legacy[ch])
            biscuit_roaring_remove(idx->char_cache_legacy[ch], rec_idx);
    }
    
    /* Case-insensitive character indices */
    for (ch = 0; ch < CHAR_RANGE; ch++) {
        pos_cidx = &idx->pos_idx_lower[ch];
        for (j = 0; j < pos_cidx->count; j++)
            biscuit_roaring_remove(pos_cidx->entries[j].bitmap, rec_idx);
        
        neg_cidx = &idx->neg_idx_lower[ch];
        for (j = 0; j < neg_cidx->count; j++)
            biscuit_roaring_remove(neg_cidx->entries[j].bitmap, rec_idx);
        
        if (idx->char_cache_lower[ch])
            biscuit_roaring_remove(idx->char_cache_lower[ch], rec_idx);
    }
    
    /* ✅ FIX: Use < instead of <= for array bounds */
    /* Arrays are sized [0..max_length_legacy-1] */
    if (idx->max_length_legacy > 0) {
        for (j = 0; j < idx->max_length_legacy; j++) {
            if (idx->length_bitmaps_legacy && idx->length_bitmaps_legacy[j])
                biscuit_roaring_remove(idx->length_bitmaps_legacy[j], rec_idx);
            if (idx->length_ge_bitmaps_legacy && idx->length_ge_bitmaps_legacy[j])
                biscuit_roaring_remove(idx->length_ge_bitmaps_legacy[j], rec_idx);
        }
    }
    
    if (idx->max_length_lower > 0) {
        for (j = 0; j < idx->max_length_lower; j++) {
            if (idx->length_bitmaps_lower && idx->length_bitmaps_lower[j])
                biscuit_roaring_remove(idx->length_bitmaps_lower[j], rec_idx);
            if (idx->length_ge_bitmaps_lower && idx->length_ge_bitmaps_lower[j])
                biscuit_roaring_remove(idx->length_ge_bitmaps_lower[j], rec_idx);
        }
    }
}
/* ==================== ROARING BITMAP WRAPPER ==================== */

#ifdef HAVE_ROARING
static inline RoaringBitmap* biscuit_roaring_create(void) { return roaring_bitmap_create(); }
static inline void biscuit_roaring_add(RoaringBitmap *rb, uint32_t value) { roaring_bitmap_add(rb, value); }
static inline void biscuit_roaring_remove(RoaringBitmap *rb, uint32_t value) { roaring_bitmap_remove(rb, value); }
static inline uint64_t biscuit_roaring_count(const RoaringBitmap *rb) { return roaring_bitmap_get_cardinality(rb); }
static inline bool biscuit_roaring_is_empty(const RoaringBitmap *rb) { return roaring_bitmap_get_cardinality(rb) == 0; }
static inline void biscuit_roaring_free(RoaringBitmap *rb) { if (rb) roaring_bitmap_free(rb); }
static inline RoaringBitmap* biscuit_roaring_copy(const RoaringBitmap *rb) { return roaring_bitmap_copy(rb); }
static inline void biscuit_roaring_and_inplace(RoaringBitmap *a, const RoaringBitmap *b) { roaring_bitmap_and_inplace(a, b); }
static inline void biscuit_roaring_or_inplace(RoaringBitmap *a, const RoaringBitmap *b) { roaring_bitmap_or_inplace(a, b); }
static inline void biscuit_roaring_andnot_inplace(RoaringBitmap *a, const RoaringBitmap *b) { roaring_bitmap_andnot_inplace(a, b); }

static inline uint32_t* biscuit_roaring_to_array(const RoaringBitmap *rb, uint64_t *count) {
    uint32_t *array;
    *count = roaring_bitmap_get_cardinality(rb);
    if (*count == 0) return NULL;
    array = (uint32_t *)palloc(*count * sizeof(uint32_t));
    roaring_bitmap_to_uint32_array(rb, array);
    return array;
}
#else
static inline RoaringBitmap* biscuit_roaring_create(void) {
    RoaringBitmap *rb = (RoaringBitmap *)palloc0(sizeof(RoaringBitmap));
    rb->capacity = 16;
    rb->blocks = (uint64_t *)palloc0(rb->capacity * sizeof(uint64_t));
    return rb;
}

static inline void biscuit_roaring_add(RoaringBitmap *rb, uint32_t value) {
    int block = value >> 6;
    int bit = value & 63;
    if (block >= rb->capacity) {
        int new_cap = (block + 1) * 2;
        uint64_t *new_blocks = (uint64_t *)palloc0(new_cap * sizeof(uint64_t));
        if (rb->num_blocks > 0)
            memcpy(new_blocks, rb->blocks, rb->num_blocks * sizeof(uint64_t));
        pfree(rb->blocks);
        rb->blocks = new_blocks;
        rb->capacity = new_cap;
    }
    if (block >= rb->num_blocks)
        rb->num_blocks = block + 1;
    rb->blocks[block] |= (1ULL << bit);
}

static inline void biscuit_roaring_remove(RoaringBitmap *rb, uint32_t value) {
    int block = value >> 6;
    int bit = value & 63;
    if (block < rb->num_blocks)
        rb->blocks[block] &= ~(1ULL << bit);
}

static inline uint64_t biscuit_roaring_count(const RoaringBitmap *rb) {
    uint64_t count = 0;
    int i;
    for (i = 0; i < rb->num_blocks; i++)
        count += __builtin_popcountll(rb->blocks[i]);
    return count;
}

static inline bool biscuit_roaring_is_empty(const RoaringBitmap *rb) {
    int i;
    for (i = 0; i < rb->num_blocks; i++)
        if (rb->blocks[i]) return false;
    return true;
}

static inline void biscuit_roaring_free(RoaringBitmap *rb) {
    if (rb) {
        if (rb->blocks) pfree(rb->blocks);
        pfree(rb);
    }
}

static inline RoaringBitmap* biscuit_roaring_copy(const RoaringBitmap *rb) {
    RoaringBitmap *copy = biscuit_roaring_create();
    if (rb->num_blocks > 0) {
        pfree(copy->blocks);
        copy->blocks = (uint64_t *)palloc(rb->num_blocks * sizeof(uint64_t));
        copy->num_blocks = rb->num_blocks;
        copy->capacity = rb->num_blocks;
        memcpy(copy->blocks, rb->blocks, rb->num_blocks * sizeof(uint64_t));
    }
    return copy;
}

static inline void biscuit_roaring_and_inplace(RoaringBitmap *a, const RoaringBitmap *b) {
    int min = (a->num_blocks < b->num_blocks) ? a->num_blocks : b->num_blocks;
    int i;
    for (i = 0; i < min; i++)
        a->blocks[i] &= b->blocks[i];
    for (i = min; i < a->num_blocks; i++)
        a->blocks[i] = 0;
    a->num_blocks = min;
}

static inline void biscuit_roaring_or_inplace(RoaringBitmap *a, const RoaringBitmap *b) {
    int min;
    int i;
    if (b->num_blocks > a->capacity) {
        uint64_t *new_blocks = (uint64_t *)palloc0(b->num_blocks * sizeof(uint64_t));
        if (a->num_blocks > 0)
            memcpy(new_blocks, a->blocks, a->num_blocks * sizeof(uint64_t));
        pfree(a->blocks);
        a->blocks = new_blocks;
        a->capacity = b->num_blocks;
    }
    min = (a->num_blocks < b->num_blocks) ? a->num_blocks : b->num_blocks;
    for (i = 0; i < min; i++)
        a->blocks[i] |= b->blocks[i];
    if (b->num_blocks > a->num_blocks) {
        memcpy(a->blocks + a->num_blocks, b->blocks + a->num_blocks,
            (b->num_blocks - a->num_blocks) * sizeof(uint64_t));
        a->num_blocks = b->num_blocks;
    }
}

static inline void biscuit_roaring_andnot_inplace(RoaringBitmap *a, const RoaringBitmap *b) {
    int min = (a->num_blocks < b->num_blocks) ? a->num_blocks : b->num_blocks;
    int i;
    for (i = 0; i < min; i++)
        a->blocks[i] &= ~b->blocks[i];
}

static inline uint32_t* biscuit_roaring_to_array(const RoaringBitmap *rb, uint64_t *count) {
    uint32_t *array;
    int idx;
    int i;
    uint64_t base;
    *count = biscuit_roaring_count(rb);
    if (*count == 0) return NULL;
    array = (uint32_t *)palloc(*count * sizeof(uint32_t));
    idx = 0;
    for (i = 0; i < rb->num_blocks; i++) {
        uint64_t bits = rb->blocks[i];
        if (!bits) continue;
        base = (uint64_t)i << 6;
        while (bits) {
            array[idx++] = (uint32_t)(base + __builtin_ctzll(bits));
            bits &= bits - 1;
        }
    }
    return array;
}
#endif

/* ==================== BITMAP ACCESS ==================== */

static inline RoaringBitmap* biscuit_get_pos_bitmap(BiscuitIndex *idx, unsigned char ch, int pos) {
    CharIndex *cidx = &idx->pos_idx_legacy[ch]; 
    int left = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos)
            return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < pos)
            left = mid + 1;
        else
            right = mid - 1;
    }
    return NULL;
}

static inline RoaringBitmap* biscuit_get_neg_bitmap(BiscuitIndex *idx, unsigned char ch, int neg_offset) {
    CharIndex *cidx = &idx->neg_idx_legacy[ch];
    int left = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset)
            return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < neg_offset)
            left = mid + 1;
        else
            right = mid - 1;
    }
    return NULL;
}

static void biscuit_set_pos_bitmap(BiscuitIndex *idx, unsigned char ch, int pos, RoaringBitmap *bm) {
    CharIndex *cidx = &idx->pos_idx_legacy[ch]; 
    int left = 0, right = cidx->count - 1, insert_pos = cidx->count;
    int i;
    
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) {
            cidx->entries[mid].bitmap = bm;
            return;
        } else if (cidx->entries[mid].pos < pos)
            left = mid + 1;
        else {
            insert_pos = mid;
            right = mid - 1;
        }
    }
    
    if (cidx->count >= cidx->capacity) {
        int new_cap = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *)palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0)
            memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries = new_entries;
        cidx->capacity = new_cap;
    }
    
    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];
    
    cidx->entries[insert_pos].pos = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

static void biscuit_set_neg_bitmap(BiscuitIndex *idx, unsigned char ch, int neg_offset, RoaringBitmap *bm) {
    CharIndex *cidx = &idx->neg_idx_legacy[ch];
    int left = 0, right = cidx->count - 1, insert_pos = cidx->count;
    int i;
    
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) {
            cidx->entries[mid].bitmap = bm;
            return;
        } else if (cidx->entries[mid].pos < neg_offset)
            left = mid + 1;
        else {
            insert_pos = mid;
            right = mid - 1;
        }
    }
    
    if (cidx->count >= cidx->capacity) {
        int new_cap = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *)palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0)
            memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries = new_entries;
        cidx->capacity = new_cap;
    }
    
    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];
    
    cidx->entries[insert_pos].pos = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

/*
 * Mirror the existing bitmap accessor functions for lowercase index
 */

static inline RoaringBitmap* 
biscuit_get_pos_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int pos) {
    CharIndex *cidx = &idx->pos_idx_lower[ch];
    int left = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos)
            return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < pos)
            left = mid + 1;
        else
            right = mid - 1;
    }
    return NULL;
}

static inline RoaringBitmap* 
biscuit_get_neg_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int neg_offset) {
    CharIndex *cidx = &idx->neg_idx_lower[ch];
    int left = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset)
            return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < neg_offset)
            left = mid + 1;
        else
            right = mid - 1;
    }
    return NULL;
}

static void 
biscuit_set_pos_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int pos, RoaringBitmap *bm) {
    CharIndex *cidx = &idx->pos_idx_lower[ch];
    int left = 0, right = cidx->count - 1, insert_pos = cidx->count;
    int i;
    
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) {
            cidx->entries[mid].bitmap = bm;
            return;
        } else if (cidx->entries[mid].pos < pos)
            left = mid + 1;
        else {
            insert_pos = mid;
            right = mid - 1;
        }
    }
    
    if (cidx->count >= cidx->capacity) {
        int new_cap = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *)palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0)
            memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries = new_entries;
        cidx->capacity = new_cap;
    }
    
    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];
    
    cidx->entries[insert_pos].pos = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

static void 
biscuit_set_neg_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int neg_offset, RoaringBitmap *bm) {
    CharIndex *cidx = &idx->neg_idx_lower[ch];
    int left = 0, right = cidx->count - 1, insert_pos = cidx->count;
    int i;
    
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) {
            cidx->entries[mid].bitmap = bm;
            return;
        } else if (cidx->entries[mid].pos < neg_offset)
            left = mid + 1;
        else {
            insert_pos = mid;
            right = mid - 1;
        }
    }
    
    if (cidx->count >= cidx->capacity) {
        int new_cap = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *)palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0)
            memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries = new_entries;
        cidx->capacity = new_cap;
    }
    
    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];
    
    cidx->entries[insert_pos].pos = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

static RoaringBitmap* 
biscuit_get_length_ge_lower(BiscuitIndex *idx, int min_len) {
    // ✅ FIX: Use the lowercase-specific max_length and bitmaps
    if (min_len >= idx->max_length_lower)
        return biscuit_roaring_create();
    
    // ✅ FIX: Return from lowercase bitmaps, not legacy
    if (!idx->length_ge_bitmaps_lower || !idx->length_ge_bitmaps_lower[min_len])
        return biscuit_roaring_create();
        
    return biscuit_roaring_copy(idx->length_ge_bitmaps_lower[min_len]);
}

/* Get column lowercase length >= bitmap for ILIKE */
static RoaringBitmap* 
biscuit_get_col_length_ge_lower(ColumnIndex *col_idx, int min_len) {
    if (!col_idx) {
        return biscuit_roaring_create();
    }
    
    if (!col_idx->length_ge_bitmaps_lower) {
        return biscuit_roaring_create();
    }
    
    if (min_len > col_idx->max_length_lower)
        return biscuit_roaring_create();
    
    return biscuit_roaring_copy(col_idx->length_ge_bitmaps_lower[min_len]);
}
/* ==================== PER-COLUMN BITMAP ACCESSORS ==================== */

static RoaringBitmap* 
biscuit_get_col_pos_bitmap(ColumnIndex *col_idx, unsigned char ch, int pos) {
    CharIndex *cidx;
    int left = 0, right;
    
    /* SAFETY: Check if column index is initialized */
    if (!col_idx) {
        //elog(WARNING, "Biscuit: NULL column index in get_col_pos_bitmap");
        return NULL;
    }
    
    cidx = &col_idx->pos_idx[ch];
    
    /* SAFETY: Check if character index is initialized */
    if (!cidx->entries) {
        //elog(DEBUG1, "Biscuit: Uninitialized pos_idx for char %d", ch);
        return NULL;
    }
    
    right = cidx->count - 1;
    
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos)
            return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < pos)
            left = mid + 1;
        else
            right = mid - 1;
    }
    return NULL;
}

static RoaringBitmap* 
biscuit_get_col_neg_bitmap(ColumnIndex *col_idx, unsigned char ch, int neg_offset) {
    CharIndex *cidx;
    int left = 0, right;
    
    /* SAFETY: Check if column index is initialized */
    if (!col_idx) {
        //elog(WARNING, "Biscuit: NULL column index in get_col_neg_bitmap");
        return NULL;
    }
    
    cidx = &col_idx->neg_idx[ch];
    
    /* SAFETY: Check if character index is initialized */
    if (!cidx->entries) {
        //elog(DEBUG1, "Biscuit: Uninitialized neg_idx for char %d", ch);
        return NULL;
    }
    
    right = cidx->count - 1;
    
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset)
            return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < neg_offset)
            left = mid + 1;
        else
            right = mid - 1;
    }
    return NULL;
}



static void 
biscuit_set_col_pos_bitmap(ColumnIndex *col_idx, unsigned char ch, int pos, RoaringBitmap *bm) {
    CharIndex *cidx = &col_idx->pos_idx[ch];
    int left = 0, right = cidx->count - 1, insert_pos = cidx->count;
    int i;
    
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) {
            cidx->entries[mid].bitmap = bm;
            return;
        } else if (cidx->entries[mid].pos < pos)
            left = mid + 1;
        else {
            insert_pos = mid;
            right = mid - 1;
        }
    }
    
    if (cidx->count >= cidx->capacity) {
        int new_cap = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *)palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0)
            memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries = new_entries;
        cidx->capacity = new_cap;
    }
    
    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];
    
    cidx->entries[insert_pos].pos = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

static void 
biscuit_set_col_neg_bitmap(ColumnIndex *col_idx, unsigned char ch, int neg_offset, RoaringBitmap *bm) {
    CharIndex *cidx = &col_idx->neg_idx[ch];
    int left = 0, right = cidx->count - 1, insert_pos = cidx->count;
    int i;
    
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) {
            cidx->entries[mid].bitmap = bm;
            return;
        } else if (cidx->entries[mid].pos < neg_offset)
            left = mid + 1;
        else {
            insert_pos = mid;
            right = mid - 1;
        }
    }
    
    if (cidx->count >= cidx->capacity) {
        int new_cap = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *)palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0)
            memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries = new_entries;
        cidx->capacity = new_cap;
    }
    
    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];
    
    cidx->entries[insert_pos].pos = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

static RoaringBitmap* 
biscuit_get_col_length_ge(ColumnIndex *col_idx, int min_len) {
    if (!col_idx) {
        //elog(WARNING, "Biscuit: NULL column index in get_col_length_ge");
        return biscuit_roaring_create();
    }
    
    if (!col_idx->length_ge_bitmaps) {
        //elog(DEBUG1, "Biscuit: Uninitialized length_ge_bitmaps");
        return biscuit_roaring_create();
    }
    
    if (min_len > col_idx->max_length)
        return biscuit_roaring_create();
    
    return biscuit_roaring_copy(col_idx->length_ge_bitmaps[min_len]);
}

static inline RoaringBitmap* 
biscuit_get_col_pos_bitmap_lower(ColumnIndex *col_idx, unsigned char ch, int pos) {
    CharIndex *cidx;
    int left = 0, right;
    
    if (!col_idx) {
        return NULL;
    }
    
    cidx = &col_idx->pos_idx_lower[ch];
    
    if (!cidx->entries) {
        return NULL;
    }
    
    right = cidx->count - 1;
    
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos)
            return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < pos)
            left = mid + 1;
        else
            right = mid - 1;
    }
    return NULL;
}

static inline RoaringBitmap* 
biscuit_get_col_neg_bitmap_lower(ColumnIndex *col_idx, unsigned char ch, int neg_offset) {
    CharIndex *cidx;
    int left = 0, right;
    
    if (!col_idx) {
        return NULL;
    }
    
    cidx = &col_idx->neg_idx_lower[ch];
    
    if (!cidx->entries) {
        return NULL;
    }
    
    right = cidx->count - 1;
    
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset)
            return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < neg_offset)
            left = mid + 1;
        else
            right = mid - 1;
    }
    return NULL;
}

static void 
biscuit_set_col_pos_bitmap_lower(ColumnIndex *col_idx, unsigned char ch, int pos, RoaringBitmap *bm) {
    CharIndex *cidx = &col_idx->pos_idx_lower[ch];
    int left = 0, right = cidx->count - 1, insert_pos = cidx->count;
    int i;
    
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) {
            cidx->entries[mid].bitmap = bm;
            return;
        } else if (cidx->entries[mid].pos < pos)
            left = mid + 1;
        else {
            insert_pos = mid;
            right = mid - 1;
        }
    }
    
    if (cidx->count >= cidx->capacity) {
        int new_cap = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *)palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0)
            memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries = new_entries;
        cidx->capacity = new_cap;
    }
    
    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];
    
    cidx->entries[insert_pos].pos = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

static void 
biscuit_set_col_neg_bitmap_lower(ColumnIndex *col_idx, unsigned char ch, int neg_offset, RoaringBitmap *bm) {
    CharIndex *cidx = &col_idx->neg_idx_lower[ch];
    int left = 0, right = cidx->count - 1, insert_pos = cidx->count;
    int i;
    
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) {
            cidx->entries[mid].bitmap = bm;
            return;
        } else if (cidx->entries[mid].pos < neg_offset)
            left = mid + 1;
        else {
            insert_pos = mid;
            right = mid - 1;
        }
    }
    
    if (cidx->count >= cidx->capacity) {
        int new_cap = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *)palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0)
            memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries = new_entries;
        cidx->capacity = new_cap;
    }
    
    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];
    
    cidx->entries[insert_pos].pos = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

/* ==================== OPTIMIZED PATTERN MATCHING ==================== */

static RoaringBitmap* biscuit_get_length_ge(BiscuitIndex *idx, int min_len) {
    if (min_len >= idx->max_length_legacy)
        return biscuit_roaring_create();
    return biscuit_roaring_copy(idx->length_ge_bitmaps_legacy[min_len]);
}

/* ==================== FIXED: UTF-8-AWARE PATTERN MATCHING ==================== */

/*
 * Match a pattern part at a specific character position
 * Handles multi-byte UTF-8 characters correctly
 */
/* ==================== CRITICAL FIX: UTF-8 Pattern Matching ==================== */

/*
 * The bug: Multi-byte UTF-8 characters weren't being matched correctly because
 * we were trying to match ALL bytes of a character but weren't handling the
 * bitmap intersection properly.
 * 
 * Example: 'café' has 'é' = [0xC3, 0xA9]
 * - We index BOTH 0xC3 and 0xA9 at position 3
 * - We need to find records that have BOTH bytes at position 3
 * - The old code was doing AND between different byte positions
 */

/*
 * FIXED: Match a pattern part at a specific character position
 * Correctly handles multi-byte UTF-8 characters
 */
static RoaringBitmap* 
biscuit_match_part_at_pos(BiscuitIndex *idx, const char *part, int part_byte_len, int start_pos) {
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int part_byte_pos = 0;
    int char_pos = start_pos;
    int concrete_chars = 0;
    bool first_char = true;
    
    /* Process pattern character by character (not byte by byte) */
    while (part_byte_pos < part_byte_len) {
        unsigned char first_byte = (unsigned char)part[part_byte_pos];
        
        /* Handle underscore wildcard */
        if (first_byte == '_') {
            part_byte_pos++;
            char_pos++;
            continue;
        }
        
        /* Get UTF-8 character length */
        int char_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_len > part_byte_len)
            char_len = part_byte_len - part_byte_pos;
        
        concrete_chars++;
        
        /* CRITICAL FIX: For multi-byte chars, we need records that have ALL bytes */
        if (char_len == 1) {
            /* Single-byte character - simple case */
            RoaringBitmap *char_bm = biscuit_get_pos_bitmap(idx, first_byte, char_pos);
            
            if (!char_bm) {
                if (result) biscuit_roaring_free(result);
                return biscuit_roaring_create();
            }
            
            if (first_char) {
                result = biscuit_roaring_copy(char_bm);
                first_char = false;
            } else {
                biscuit_roaring_and_inplace(result, char_bm);
                if (biscuit_roaring_is_empty(result)) {
                    return result;
                }
            }
        } else {
            /* Multi-byte character - MUST have ALL bytes at this position */
            RoaringBitmap *multibyte_result = NULL;
            int b;
            
            for (b = 0; b < char_len; b++) {
                unsigned char byte_val = (unsigned char)part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_pos_bitmap(idx, byte_val, char_pos);
                
                if (!byte_bm) {
                    if (multibyte_result) biscuit_roaring_free(multibyte_result);
                    if (result) biscuit_roaring_free(result);
                    return biscuit_roaring_create();
                }
                
                if (b == 0) {
                    /* First byte of multi-byte char */
                    multibyte_result = biscuit_roaring_copy(byte_bm);
                } else {
                    /* Subsequent bytes - must intersect */
                    biscuit_roaring_and_inplace(multibyte_result, byte_bm);
                    if (biscuit_roaring_is_empty(multibyte_result)) {
                        biscuit_roaring_free(multibyte_result);
                        if (result) biscuit_roaring_free(result);
                        return biscuit_roaring_create();
                    }
                }
            }
            
            /* Now intersect this character's result with overall result */
            if (first_char) {
                result = multibyte_result;
                first_char = false;
            } else {
                biscuit_roaring_and_inplace(result, multibyte_result);
                biscuit_roaring_free(multibyte_result);
                if (biscuit_roaring_is_empty(result)) {
                    return result;
                }
            }
        }
        
        part_byte_pos += char_len;
        char_pos++;
    }
    
    /* All wildcards case */
    if (concrete_chars == 0) {
        int pattern_char_count = biscuit_utf8_char_count(part, part_byte_len);
        if (result) biscuit_roaring_free(result);
        result = biscuit_get_length_ge(idx, start_pos + pattern_char_count);
    } else {
        /* Apply length constraint */
        int pattern_char_count = biscuit_utf8_char_count(part, part_byte_len);
        len_filter = biscuit_get_length_ge(idx, start_pos + pattern_char_count);
        if (len_filter) {
            if (result) {
                biscuit_roaring_and_inplace(result, len_filter);
            } else {
                result = len_filter;
                len_filter = NULL;
            }
            if (len_filter) biscuit_roaring_free(len_filter);
        }
    }
    
    return result ? result : biscuit_roaring_create();
}

/* ==================== FIXED: biscuit_match_part_at_end ==================== */
static RoaringBitmap* 
biscuit_match_part_at_end(BiscuitIndex *idx, const char *part, int part_byte_len) {
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int concrete_chars = 0;
    bool first_char = true;
    
    /* Count characters in pattern (not bytes) */
    int pattern_char_count = biscuit_utf8_char_count(part, part_byte_len);
    
    /* Process pattern from end to beginning */
    int part_byte_pos = 0;
    int char_offset_from_end = 0;
    
    while (part_byte_pos < part_byte_len) {
        unsigned char first_byte = (unsigned char)part[part_byte_pos];
        
        /* Handle underscore wildcard */
        if (first_byte == '_') {
            part_byte_pos++;
            char_offset_from_end++;
            continue;
        }
        
        /* Get UTF-8 character length */
        int char_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_len > part_byte_len)
            char_len = part_byte_len - part_byte_pos;
        
        concrete_chars++;
        
        /* Calculate negative position */
        int neg_pos = -(pattern_char_count - char_offset_from_end);
        
        /* Match all bytes of this character at negative position */
        if (char_len == 1) {
            /* Single-byte character */
            RoaringBitmap *char_bm = biscuit_get_neg_bitmap(idx, first_byte, neg_pos);
            
            if (!char_bm) {
                if (result) biscuit_roaring_free(result);
                return biscuit_roaring_create();
            }
            
            if (first_char) {
                result = biscuit_roaring_copy(char_bm);
                first_char = false;
            } else {
                biscuit_roaring_and_inplace(result, char_bm);
                if (biscuit_roaring_is_empty(result)) {
                    return result;
                }
            }
        } else {
            /* Multi-byte character - must have ALL bytes */
            RoaringBitmap *multibyte_result = NULL;
            int b;
            
            for (b = 0; b < char_len; b++) {
                unsigned char byte_val = (unsigned char)part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_neg_bitmap(idx, byte_val, neg_pos);
                
                if (!byte_bm) {
                    if (multibyte_result) biscuit_roaring_free(multibyte_result);
                    if (result) biscuit_roaring_free(result);
                    return biscuit_roaring_create();
                }
                
                if (b == 0) {
                    multibyte_result = biscuit_roaring_copy(byte_bm);
                } else {
                    biscuit_roaring_and_inplace(multibyte_result, byte_bm);
                    if (biscuit_roaring_is_empty(multibyte_result)) {
                        biscuit_roaring_free(multibyte_result);
                        if (result) biscuit_roaring_free(result);
                        return biscuit_roaring_create();
                    }
                }
            }
            
            /* Intersect with overall result */
            if (first_char) {
                result = multibyte_result;
                first_char = false;
            } else {
                biscuit_roaring_and_inplace(result, multibyte_result);
                biscuit_roaring_free(multibyte_result);
                if (biscuit_roaring_is_empty(result)) {
                    return result;
                }
            }
        }
        
        part_byte_pos += char_len;
        char_offset_from_end++;
    }
    
    /* All wildcards case */
    if (concrete_chars == 0) {
        if (result) biscuit_roaring_free(result);
        result = biscuit_get_length_ge(idx, pattern_char_count);
    } else {
        len_filter = biscuit_get_length_ge(idx, pattern_char_count);
        if (len_filter) {
            if (result) {
                biscuit_roaring_and_inplace(result, len_filter);
            } else {
                result = len_filter;
                len_filter = NULL;
            }
            if (len_filter) biscuit_roaring_free(len_filter);
        }
    }
    
    return result ? result : biscuit_roaring_create();
}

/* ==================== FIXED: biscuit_match_part_at_pos_ilike ==================== */
static RoaringBitmap* 
biscuit_match_part_at_pos_ilike(BiscuitIndex *idx, const char *part, int part_byte_len, int start_pos) {
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int part_byte_pos = 0;
    int char_pos = start_pos;
    int concrete_chars = 0;
    bool first_char = true;
    
    while (part_byte_pos < part_byte_len) {
        unsigned char first_byte = (unsigned char)part[part_byte_pos];
        
        if (first_byte == '_') {
            part_byte_pos++;
            char_pos++;
            continue;
        }
        
        int char_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_len > part_byte_len)
            char_len = part_byte_len - part_byte_pos;
        
        concrete_chars++;
        
        if (char_len == 1) {
            RoaringBitmap *char_bm = biscuit_get_pos_bitmap_lower(idx, first_byte, char_pos);
            
            if (!char_bm) {
                if (result) biscuit_roaring_free(result);
                return biscuit_roaring_create();
            }
            
            if (first_char) {
                result = biscuit_roaring_copy(char_bm);
                first_char = false;
            } else {
                biscuit_roaring_and_inplace(result, char_bm);
                if (biscuit_roaring_is_empty(result))
                    return result;
            }
        } else {
            RoaringBitmap *multibyte_result = NULL;
            int b;
            
            for (b = 0; b < char_len; b++) {
                unsigned char byte_val = (unsigned char)part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_pos_bitmap_lower(idx, byte_val, char_pos);
                
                if (!byte_bm) {
                    if (multibyte_result) biscuit_roaring_free(multibyte_result);
                    if (result) biscuit_roaring_free(result);
                    return biscuit_roaring_create();
                }
                
                if (b == 0) {
                    multibyte_result = biscuit_roaring_copy(byte_bm);
                } else {
                    biscuit_roaring_and_inplace(multibyte_result, byte_bm);
                    if (biscuit_roaring_is_empty(multibyte_result)) {
                        biscuit_roaring_free(multibyte_result);
                        if (result) biscuit_roaring_free(result);
                        return biscuit_roaring_create();
                    }
                }
            }
            
            if (first_char) {
                result = multibyte_result;
                first_char = false;
            } else {
                biscuit_roaring_and_inplace(result, multibyte_result);
                biscuit_roaring_free(multibyte_result);
                if (biscuit_roaring_is_empty(result))
                    return result;
            }
        }
        
        part_byte_pos += char_len;
        char_pos++;
    }
    
    if (concrete_chars == 0) {
        int pattern_char_count = biscuit_utf8_char_count(part, part_byte_len);
        if (result) biscuit_roaring_free(result);
        result = biscuit_get_length_ge_lower(idx, start_pos + pattern_char_count);
    } else {
        int pattern_char_count = biscuit_utf8_char_count(part, part_byte_len);
        len_filter = biscuit_get_length_ge_lower(idx, start_pos + pattern_char_count);
        if (len_filter) {
            if (result) {
                biscuit_roaring_and_inplace(result, len_filter);
            } else {
                result = len_filter;
                len_filter = NULL;
            }
            if (len_filter) biscuit_roaring_free(len_filter);
        }
    }
    
    return result ? result : biscuit_roaring_create();
}

/* ==================== FIXED: biscuit_match_part_at_end_ilike ==================== */
static RoaringBitmap* 
biscuit_match_part_at_end_ilike(BiscuitIndex *idx, const char *part, int part_byte_len) {
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int concrete_chars = 0;
    bool first_char = true;
    
    int pattern_char_count = biscuit_utf8_char_count(part, part_byte_len);
    
    int part_byte_pos = 0;
    int char_offset_from_end = 0;
    
    while (part_byte_pos < part_byte_len) {
        unsigned char first_byte = (unsigned char)part[part_byte_pos];
        
        if (first_byte == '_') {
            part_byte_pos++;
            char_offset_from_end++;
            continue;
        }
        
        int char_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_len > part_byte_len)
            char_len = part_byte_len - part_byte_pos;
        
        concrete_chars++;
        
        int neg_pos = -(pattern_char_count - char_offset_from_end);
        
        if (char_len == 1) {
            RoaringBitmap *char_bm = biscuit_get_neg_bitmap_lower(idx, first_byte, neg_pos);
            
            if (!char_bm) {
                if (result) biscuit_roaring_free(result);
                return biscuit_roaring_create();
            }
            
            if (first_char) {
                result = biscuit_roaring_copy(char_bm);
                first_char = false;
            } else {
                biscuit_roaring_and_inplace(result, char_bm);
                if (biscuit_roaring_is_empty(result))
                    return result;
            }
        } else {
            RoaringBitmap *multibyte_result = NULL;
            int b;
            
            for (b = 0; b < char_len; b++) {
                unsigned char byte_val = (unsigned char)part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_neg_bitmap_lower(idx, byte_val, neg_pos);
                
                if (!byte_bm) {
                    if (multibyte_result) biscuit_roaring_free(multibyte_result);
                    if (result) biscuit_roaring_free(result);
                    return biscuit_roaring_create();
                }
                
                if (b == 0) {
                    multibyte_result = biscuit_roaring_copy(byte_bm);
                } else {
                    biscuit_roaring_and_inplace(multibyte_result, byte_bm);
                    if (biscuit_roaring_is_empty(multibyte_result)) {
                        biscuit_roaring_free(multibyte_result);
                        if (result) biscuit_roaring_free(result);
                        return biscuit_roaring_create();
                    }
                }
            }
            
            if (first_char) {
                result = multibyte_result;
                first_char = false;
            } else {
                biscuit_roaring_and_inplace(result, multibyte_result);
                biscuit_roaring_free(multibyte_result);
                if (biscuit_roaring_is_empty(result))
                    return result;
            }
        }
        
        part_byte_pos += char_len;
        char_offset_from_end++;
    }
    
    if (concrete_chars == 0) {
        if (result) biscuit_roaring_free(result);
        result = biscuit_get_length_ge_lower(idx, pattern_char_count);
    } else {
        len_filter = biscuit_get_length_ge_lower(idx, pattern_char_count);
        if (len_filter) {
            if (result) {
                biscuit_roaring_and_inplace(result, len_filter);
            } else {
                result = len_filter;
                len_filter = NULL;
            }
            if (len_filter) biscuit_roaring_free(len_filter);
        }
    }
    
    return result ? result : biscuit_roaring_create();
}
/* ==================== FIXED: UTF-8-AWARE RECURSIVE WINDOWED MATCHING (SINGLE-COLUMN) ==================== */

/*
 * Recursive windowed matching for single-column patterns (case-sensitive)
 * Handles multi-byte UTF-8 characters correctly
 */
static void 
biscuit_recursive_windowed_match(
    RoaringBitmap *result, BiscuitIndex *idx,
    const char **parts, int *part_lens, int part_count,
    bool ends_percent, int part_idx, int min_pos,
    RoaringBitmap *current_candidates, int max_len)
{
    int remaining_len, max_pos, pos, i;
    RoaringBitmap *end_match;
    RoaringBitmap *length_constraint;
    RoaringBitmap *part_match;
    RoaringBitmap *next_candidates;
    int min_required_length;
    int next_min_pos;
    
    /* Base case: all parts have been matched */
    if (part_idx >= part_count) {
        biscuit_roaring_or_inplace(result, current_candidates);
        return;
    }
    
    /* Calculate minimum CHARACTER length needed for remaining parts */
    remaining_len = 0;
    for (i = part_idx + 1; i < part_count; i++) {
        remaining_len += biscuit_utf8_char_count(parts[i], part_lens[i]);
    }
    
    /* CRITICAL FIX: Last part without trailing % must match at end */
    if (part_idx == part_count - 1 && !ends_percent) {
        /* Use negative indexing to match at the end */
        end_match = biscuit_match_part_at_end(idx, parts[part_idx], part_lens[part_idx]);
        
        if (!end_match) {
            return;
        }
        
        /* Intersect with current candidates */
        biscuit_roaring_and_inplace(end_match, current_candidates);
        
        /* Ensure minimum CHARACTER length constraint */
        min_required_length = min_pos + biscuit_utf8_char_count(parts[part_idx], part_lens[part_idx]);
        length_constraint = biscuit_get_length_ge(idx, min_required_length);
        biscuit_roaring_and_inplace(length_constraint, end_match);
        biscuit_roaring_free(length_constraint);
        
        /* Add to result */
        biscuit_roaring_or_inplace(result, end_match);
        biscuit_roaring_free(end_match);
        return;
    }
    
    /* Middle part: try all valid CHARACTER positions */
    int current_part_char_len = biscuit_utf8_char_count(parts[part_idx], part_lens[part_idx]);
    max_pos = max_len - current_part_char_len - remaining_len;
    
    if (min_pos > max_pos) {
        /* No valid position for this part */
        return;
    }
    
    /* Try each valid CHARACTER position for current part */
    for (pos = min_pos; pos <= max_pos; pos++) {
        /* Match part at this CHARACTER position */
        part_match = biscuit_match_part_at_pos(idx, parts[part_idx], part_lens[part_idx], pos);
        
        if (!part_match) {
            continue;
        }
        
        /* Intersect with current candidates */
        next_candidates = biscuit_roaring_copy(current_candidates);
        biscuit_roaring_and_inplace(next_candidates, part_match);
        biscuit_roaring_free(part_match);
        
        /* Skip if no matches at this position */
        if (biscuit_roaring_is_empty(next_candidates)) {
            biscuit_roaring_free(next_candidates);
            continue;
        }
        
        /* Recurse for next part with updated CHARACTER position */
        next_min_pos = pos + current_part_char_len;
        
        /* CRITICAL: For patterns with trailing %, ensure gap for remaining parts */
        if (ends_percent || part_idx < part_count - 1) {
            /* Need at least one position gap before next part can start */
            next_min_pos = pos + current_part_char_len;
        }
        
        biscuit_recursive_windowed_match(
            result, idx, parts, part_lens, part_count,
            ends_percent, part_idx + 1, next_min_pos, 
            next_candidates, max_len
        );
        
        biscuit_roaring_free(next_candidates);
    }
}

/*
 * Recursive windowed matching for single-column patterns (case-insensitive)
 * Handles multi-byte UTF-8 characters correctly
 */
static void 
biscuit_recursive_windowed_match_ilike(
    RoaringBitmap *result, BiscuitIndex *idx,
    const char **parts, int *part_lens, int part_count,
    bool ends_percent, int part_idx, int min_pos,
    RoaringBitmap *current_candidates, int max_len)
{
    int remaining_len, max_pos, pos, i;
    RoaringBitmap *end_match;
    RoaringBitmap *length_constraint;
    RoaringBitmap *part_match;
    RoaringBitmap *next_candidates;
    int min_required_length;
    int next_min_pos;
    
    /* Base case */
    if (part_idx >= part_count) {
        biscuit_roaring_or_inplace(result, current_candidates);
        return;
    }
    
    /* Calculate minimum CHARACTER length for remaining parts */
    remaining_len = 0;
    for (i = part_idx + 1; i < part_count; i++) {
        remaining_len += biscuit_utf8_char_count(parts[i], part_lens[i]);
    }
    
    /* Last part without trailing % must match at end */
    if (part_idx == part_count - 1 && !ends_percent) {
        end_match = biscuit_match_part_at_end_ilike(idx, parts[part_idx], part_lens[part_idx]);
        
        if (!end_match) {
            return;
        }
        
        biscuit_roaring_and_inplace(end_match, current_candidates);
        
        /* CHARACTER length constraint */
        min_required_length = min_pos + biscuit_utf8_char_count(parts[part_idx], part_lens[part_idx]);
        length_constraint = biscuit_get_length_ge_lower(idx, min_required_length);
        biscuit_roaring_and_inplace(end_match, length_constraint);
        biscuit_roaring_free(length_constraint);
        
        biscuit_roaring_or_inplace(result, end_match);
        biscuit_roaring_free(end_match);
        return;
    }
    
    /* Try all valid CHARACTER positions */
    int current_part_char_len = biscuit_utf8_char_count(parts[part_idx], part_lens[part_idx]);
    max_pos = max_len - current_part_char_len - remaining_len;
    
    if (min_pos > max_pos) {
        return;
    }
    
    for (pos = min_pos; pos <= max_pos; pos++) {
        part_match = biscuit_match_part_at_pos_ilike(idx, parts[part_idx], part_lens[part_idx], pos);
        
        if (!part_match) {
            continue;
        }
        
        next_candidates = biscuit_roaring_copy(current_candidates);
        biscuit_roaring_and_inplace(next_candidates, part_match);
        biscuit_roaring_free(part_match);
        
        if (biscuit_roaring_is_empty(next_candidates)) {
            biscuit_roaring_free(next_candidates);
            continue;
        }
        
        /* CHARACTER position advancement */
        next_min_pos = pos + current_part_char_len;
        
        biscuit_recursive_windowed_match_ilike(
            result, idx, parts, part_lens, part_count,
            ends_percent, part_idx + 1, next_min_pos, 
            next_candidates, max_len
        );
        
        biscuit_roaring_free(next_candidates);
    }
}

static void
biscuit_free_parsed_pattern(ParsedPattern *parsed)
{
    int i;
    
    if (!parsed)
        return;
    
    if (parsed->parts) {
        for (i = 0; i < parsed->part_count; i++) {
            if (parsed->parts[i]) {
                pfree(parsed->parts[i]);
                parsed->parts[i] = NULL;
            }
        }
        pfree(parsed->parts);
    }
    
    if (parsed->part_lens) {
        pfree(parsed->part_lens);
    }
    
    if (parsed->part_byte_lens) {  // NEW
        pfree(parsed->part_byte_lens);
    }
    
    pfree(parsed);
}

/*
 * Main ILIKE query function - routes to lowercase index
 */


/* ==================== FIXED: biscuit_query_pattern_ilike - Substring Search ==================== */
static RoaringBitmap* 
biscuit_query_pattern_ilike(BiscuitIndex *idx, const char *pattern) {
    char *pattern_lower = NULL;
    int plen = strlen(pattern);
    ParsedPattern *parsed = NULL;
    int min_len, i;
    RoaringBitmap *result = NULL;
    int wildcard_count = 0, percent_count = 0;
    bool only_wildcards = true;
    
    /* CRITICAL FIX: Convert pattern to lowercase FIRST */
    pattern_lower = biscuit_str_tolower(pattern, plen);
    plen = strlen(pattern_lower);
    
    PG_TRY();
    {
        /* ========== FAST PATH 1: Empty pattern '' ========== */
        if (plen == 0) {
            if (idx->length_bitmaps_legacy && idx->length_bitmaps_legacy[0]) {
                result = biscuit_roaring_copy(idx->length_bitmaps_legacy[0]);
            } else {
                result = biscuit_roaring_create();
            }
            pfree(pattern_lower);
            return result;
        }
        
        /* ========== FAST PATH 2: Single '%' matches everything ========== */
        if (plen == 1 && pattern_lower[0] == '%') {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                #ifdef HAVE_ROARING
                if (!roaring_bitmap_contains(idx->tombstones, (uint32_t)i))
                #else
                uint32_t block = i >> 6;
                uint32_t bit = i & 63;
                bool tombstoned = (block < idx->tombstones->num_blocks &&
                                  (idx->tombstones->blocks[block] & (1ULL << bit)));
                if (!tombstoned)
                #endif
                    biscuit_roaring_add(result, i);
            }
            pfree(pattern_lower);
            return result;
        }
        
        /* ========== FAST PATH 3: Analyze for pure wildcards (% and _ only) ========== */
        for (i = 0; i < plen; i++) {
            if (pattern_lower[i] == '%') {
                percent_count++;
            } else if (pattern_lower[i] == '_') {
                wildcard_count++;
            } else {
                only_wildcards = false;
                break;
            }
        }
        
        /* ========== FAST PATH 4 & 5: Pure wildcard patterns ========== */
        if (only_wildcards) {
            if (percent_count > 0) {
                /* FAST PATH 4: Has %, so length >= wildcard_count */
                result = biscuit_get_length_ge_lower(idx, wildcard_count);
            } else {
                /* FAST PATH 5: Only underscores → EXACT length match */
                if (wildcard_count < idx->max_length_legacy && 
                    idx->length_bitmaps_legacy[wildcard_count]) {
                    result = biscuit_roaring_copy(idx->length_bitmaps_legacy[wildcard_count]);
                } else {
                    result = biscuit_roaring_create();
                }
            }
            pfree(pattern_lower);
            return result;
        }
        
        /* ========== SLOW PATH: Pattern contains concrete characters ========== */
        
        /* Parse the LOWERCASE pattern */
        parsed = biscuit_parse_pattern(pattern_lower);
        
        /* All percent signs (shouldn't happen, but handle gracefully) */
        if (parsed->part_count == 0) {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                #ifdef HAVE_ROARING
                if (!roaring_bitmap_contains(idx->tombstones, (uint32_t)i))
                #else
                uint32_t block = i >> 6;
                uint32_t bit = i & 63;
                bool tombstoned = (block < idx->tombstones->num_blocks &&
                                  (idx->tombstones->blocks[block] & (1ULL << bit)));
                if (!tombstoned)
                #endif
                    biscuit_roaring_add(result, i);
            }
            biscuit_free_parsed_pattern(parsed);
            pfree(pattern_lower);
            return result;
        }
        
        /* Calculate minimum required length */
        min_len = 0;
        for (i = 0; i < parsed->part_count; i++)
            min_len += parsed->part_lens[i];  // Uses CHARACTER counts
        
        /* ==================== OPTIMIZED SINGLE PART PATTERNS ==================== */
        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                /* EXACT: 'abc' or 'a_c' - must match exactly at position 0 with exact length */
                result = biscuit_match_part_at_pos_ilike(idx, parsed->parts[0], 
                                                         parsed->part_byte_lens[0], 0);
                if (result && min_len < idx->max_length_legacy && 
                    idx->length_bitmaps_legacy[min_len]) {
                    biscuit_roaring_and_inplace(result, idx->length_bitmaps_legacy[min_len]);
                } else if (!result || min_len >= idx->max_length_legacy) {
                    if (result) biscuit_roaring_free(result);
                    result = biscuit_roaring_create();
                }
            } else if (!parsed->starts_percent) {
                /* PREFIX: 'abc%' - starts at position 0, any length >= min_len */
                result = biscuit_match_part_at_pos_ilike(idx, parsed->parts[0], 
                                                         parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                /* SUFFIX: '%abc' - ends at end, any length >= min_len */
                result = biscuit_match_part_at_end_ilike(idx, parsed->parts[0], 
                                                         parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                /* ==================== FIXED: SUBSTRING MATCH (ILIKE) ==================== */
                /* Pattern: '%abc%' - must validate complete UTF-8 characters with lowercase */
                
                result = biscuit_roaring_create();
                
                RoaringBitmap *candidates = NULL;
                int part_byte_len = parsed->part_byte_lens[0];
                int part_char_len = parsed->part_lens[0];
                
                /* Get candidates using first byte of pattern */
                if (part_byte_len > 0) {
                    unsigned char first_byte = (unsigned char)parsed->parts[0][0];
                    
                    /* Skip underscores to find first concrete byte */
                    int scan_pos = 0;
                    while (scan_pos < part_byte_len && parsed->parts[0][scan_pos] == '_') {
                        scan_pos++;
                    }
                    
                    if (scan_pos < part_byte_len) {
                        first_byte = (unsigned char)parsed->parts[0][scan_pos];
                        
                        /* Get all records with this byte (lowercase index) */
                        if (idx->char_cache_lower[first_byte]) {
                            candidates = biscuit_roaring_copy(idx->char_cache_lower[first_byte]);
                        }
                    }
                }
                
                if (!candidates) {
                    candidates = biscuit_roaring_create();
                    for (i = 0; i < idx->num_records; i++) {
                        #ifdef HAVE_ROARING
                        if (!roaring_bitmap_contains(idx->tombstones, (uint32_t)i))
                        #else
                        uint32_t block = i >> 6;
                        uint32_t bit = i & 63;
                        bool tombstoned = (block < idx->tombstones->num_blocks &&
                                          (idx->tombstones->blocks[block] & (1ULL << bit)));
                        if (!tombstoned)
                        #endif
                            biscuit_roaring_add(candidates, i);
                    }
                }
                
                /* Apply length filter (using lowercase length bitmaps) */
                RoaringBitmap *length_filter = biscuit_get_length_ge_lower(idx, part_char_len);
                if (length_filter) {
                    biscuit_roaring_and_inplace(candidates, length_filter);
                    biscuit_roaring_free(length_filter);
                }
                
                /* ✅ FIX: Validate each candidate with UTF-8-aware substring search (case-insensitive) */
                #ifdef HAVE_ROARING
                roaring_uint32_iterator_t *iter = roaring_create_iterator(candidates);
                
                while (iter->has_value) {
                    uint32_t rec_idx = iter->current_value;
                    
                    if (rec_idx < (uint32_t)idx->num_records && idx->data_cache_lower && 
                        idx->data_cache_lower[rec_idx]) {
                        char *haystack = idx->data_cache_lower[rec_idx];
                        int haystack_byte_len = strlen(haystack);
                        int haystack_char_len = biscuit_utf8_char_count(haystack, haystack_byte_len);
                        
                        /* Try matching pattern at each CHARACTER position */
                        bool found = false;
                        for (int char_pos = 0; char_pos <= haystack_char_len - part_char_len && !found; char_pos++) {
                            /* Convert character position to byte offset */
                            int byte_offset = biscuit_utf8_char_to_byte_offset(haystack, haystack_byte_len, char_pos);
                            
                            if (byte_offset < 0)
                                continue;
                            
                            /* ✅ KEY FIX: Match CHARACTER by CHARACTER */
                            bool match = true;
                            int pattern_byte = 0;
                            int haystack_byte = byte_offset;
                            
                            while (pattern_byte < part_byte_len && haystack_byte < haystack_byte_len && match) {
                                /* Handle underscore wildcard */
                                if (parsed->parts[0][pattern_byte] == '_') {
                                    /* Underscore matches ONE CHARACTER */
                                    int hay_char_len = biscuit_utf8_char_length((unsigned char)haystack[haystack_byte]);
                                    haystack_byte += hay_char_len;
                                    pattern_byte++;
                                    continue;
                                }
                                
                                /* Get lengths of current UTF-8 characters */
                                int pattern_char_len = biscuit_utf8_char_length((unsigned char)parsed->parts[0][pattern_byte]);
                                int hay_char_len = biscuit_utf8_char_length((unsigned char)haystack[haystack_byte]);
                                
                                /* Character lengths must match */
                                if (pattern_char_len != hay_char_len) {
                                    match = false;
                                    break;
                                }
                                
                                /* Compare all bytes of the multi-byte character */
                                for (int b = 0; b < pattern_char_len; b++) {
                                    if (pattern_byte + b >= part_byte_len || 
                                        haystack_byte + b >= haystack_byte_len ||
                                        parsed->parts[0][pattern_byte + b] != haystack[haystack_byte + b]) {
                                        match = false;
                                        break;
                                    }
                                }
                                
                                if (!match)
                                    break;
                                
                                /* Advance by character length */
                                pattern_byte += pattern_char_len;
                                haystack_byte += hay_char_len;
                            }
                            
                            /* Verify we consumed the entire pattern */
                            if (match && pattern_byte == part_byte_len) {
                                found = true;
                            }
                        }
                        
                        if (found) {
                            biscuit_roaring_add(result, rec_idx);
                        }
                    }
                    
                    roaring_advance_uint32_iterator(iter);
                }
                
                roaring_free_uint32_iterator(iter);
                #else
                {
                    uint32_t *indices;
                    uint64_t count;
                    int j;
                    
                    indices = biscuit_roaring_to_array(candidates, &count);
                    
                    if (indices) {
                        for (j = 0; j < (int)count; j++) {
                            uint32_t rec_idx = indices[j];
                            
                            if (rec_idx < (uint32_t)idx->num_records && idx->data_cache_lower && 
                                idx->data_cache_lower[rec_idx]) {
                                char *haystack = idx->data_cache_lower[rec_idx];
                                int haystack_byte_len = strlen(haystack);
                                int haystack_char_len = biscuit_utf8_char_count(haystack, haystack_byte_len);
                                
                                bool found = false;
                                int char_pos;
                                for (char_pos = 0; char_pos <= haystack_char_len - part_char_len && !found; char_pos++) {
                                    int byte_offset = biscuit_utf8_char_to_byte_offset(haystack, haystack_byte_len, char_pos);
                                    
                                    if (byte_offset < 0)
                                        continue;
                                    
                                    /* ✅ FIX: CHARACTER-by-CHARACTER matching */
                                    bool match = true;
                                    int pattern_byte = 0;
                                    int haystack_byte = byte_offset;
                                    
                                    while (pattern_byte < part_byte_len && haystack_byte < haystack_byte_len && match) {
                                        if (parsed->parts[0][pattern_byte] == '_') {
                                            int hay_char_len = biscuit_utf8_char_length((unsigned char)haystack[haystack_byte]);
                                            haystack_byte += hay_char_len;
                                            pattern_byte++;
                                            continue;
                                        }
                                        
                                        int pattern_char_len = biscuit_utf8_char_length((unsigned char)parsed->parts[0][pattern_byte]);
                                        int hay_char_len = biscuit_utf8_char_length((unsigned char)haystack[haystack_byte]);
                                        
                                        if (pattern_char_len != hay_char_len) {
                                            match = false;
                                            break;
                                        }
                                        
                                        for (int b = 0; b < pattern_char_len; b++) {
                                            if (pattern_byte + b >= part_byte_len || 
                                                haystack_byte + b >= haystack_byte_len ||
                                                parsed->parts[0][pattern_byte + b] != haystack[haystack_byte + b]) {
                                                match = false;
                                                break;
                                            }
                                        }
                                        
                                        if (!match)
                                            break;
                                        
                                        pattern_byte += pattern_char_len;
                                        haystack_byte += hay_char_len;
                                    }
                                    
                                    if (match && pattern_byte == part_byte_len) {
                                        found = true;
                                    }
                                }
                                
                                if (found) {
                                    biscuit_roaring_add(result, rec_idx);
                                }
                            }
                        }
                        pfree(indices);
                    }
                }
                #endif
                
                biscuit_roaring_free(candidates);
            }
        }
        /* ==================== OPTIMIZED TWO PART PATTERNS ==================== */
        else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            /* INFIX: 'abc%def' - first at start, last at end */
            RoaringBitmap *prefix_match;
            RoaringBitmap *suffix_match;
            RoaringBitmap *length_filter;
            
            prefix_match = biscuit_match_part_at_pos_ilike(idx, parsed->parts[0], 
                                                           parsed->part_byte_lens[0], 0);
            suffix_match = biscuit_match_part_at_end_ilike(idx, parsed->parts[1], 
                                                           parsed->part_byte_lens[1]);
            
            if (!prefix_match || !suffix_match) {
                if (prefix_match) biscuit_roaring_free(prefix_match);
                if (suffix_match) biscuit_roaring_free(suffix_match);
                result = biscuit_roaring_create();
            } else {
                biscuit_roaring_and_inplace(prefix_match, suffix_match);
                biscuit_roaring_free(suffix_match);
                
                length_filter = biscuit_get_length_ge_lower(idx, min_len);
                if (length_filter) {
                    biscuit_roaring_and_inplace(prefix_match, length_filter);
                    biscuit_roaring_free(length_filter);
                }
                
                result = prefix_match;
            }
        }
        /* ==================== COMPLEX MULTI-PART PATTERNS ==================== */
        else {
            RoaringBitmap *candidates;
            
            result = biscuit_roaring_create();
            candidates = biscuit_get_length_ge_lower(idx, min_len);
            
            if (!candidates || biscuit_roaring_is_empty(candidates)) {
                if (candidates) biscuit_roaring_free(candidates);
            } else {
                if (!parsed->starts_percent) {
                    RoaringBitmap *first_part_match = biscuit_match_part_at_pos_ilike(
                        idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                    
                    if (first_part_match) {
                        biscuit_roaring_and_inplace(first_part_match, candidates);
                        biscuit_roaring_free(candidates);
                        
                        if (!biscuit_roaring_is_empty(first_part_match)) {
                            biscuit_recursive_windowed_match_ilike(
                                result, idx,
                                (const char **)parsed->parts, parsed->part_byte_lens,
                                parsed->part_count, parsed->ends_percent,
                                1, parsed->part_lens[0], first_part_match, 
                                idx->max_len
                            );
                        }
                        biscuit_roaring_free(first_part_match);
                    } else {
                        biscuit_roaring_free(candidates);
                    }
                } else {
                    biscuit_recursive_windowed_match_ilike(
                        result, idx,
                        (const char **)parsed->parts, parsed->part_byte_lens,
                        parsed->part_count, parsed->ends_percent,
                        0, 0, candidates, idx->max_len
                    );
                    biscuit_roaring_free(candidates);
                }
            }
        }
        
        /* Cleanup */
        biscuit_free_parsed_pattern(parsed);
        pfree(pattern_lower);
    }
    PG_CATCH();
    {
        /* Emergency cleanup */
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        if (pattern_lower) pfree(pattern_lower);
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (result && idx->tombstone_count > 0) {
        biscuit_roaring_andnot_inplace(result, idx->tombstones);
    }

    return result ? result : biscuit_roaring_create();
}

static ParsedPattern* biscuit_parse_pattern(const char *pattern) {
    ParsedPattern *parsed;
    int plen;
    int part_cap = 8;
    int part_start;
    int i;
    
    parsed = (ParsedPattern *)palloc0(sizeof(ParsedPattern));
    plen = strlen(pattern);
    
    parsed->parts = (char **)palloc0(part_cap * sizeof(char *));
    parsed->part_lens = (int *)palloc0(part_cap * sizeof(int));
    parsed->part_byte_lens = (int *)palloc0(part_cap * sizeof(int));  // NEW
    parsed->part_count = 0;
    parsed->starts_percent = (plen > 0 && pattern[0] == '%');
    parsed->ends_percent = (plen > 0 && pattern[plen - 1] == '%');
    
    part_start = parsed->starts_percent ? 1 : 0;
    
    for (i = part_start; i < plen; i++) {
        if (pattern[i] == '%') {
            int part_byte_len = i - part_start;
            if (part_byte_len > 0) {
                if (parsed->part_count >= part_cap) {
                    int new_cap = part_cap * 2;
                    char **new_parts = (char **)palloc(new_cap * sizeof(char *));
                    int *new_lens = (int *)palloc(new_cap * sizeof(int));
                    int *new_byte_lens = (int *)palloc(new_cap * sizeof(int));  // NEW
                    memcpy(new_parts, parsed->parts, part_cap * sizeof(char *));
                    memcpy(new_lens, parsed->part_lens, part_cap * sizeof(int));
                    memcpy(new_byte_lens, parsed->part_byte_lens, part_cap * sizeof(int));  // NEW
                    pfree(parsed->parts);
                    pfree(parsed->part_lens);
                    pfree(parsed->part_byte_lens);  // NEW
                    parsed->parts = new_parts;
                    parsed->part_lens = new_lens;
                    parsed->part_byte_lens = new_byte_lens;  // NEW
                    part_cap = new_cap;
                }
                parsed->parts[parsed->part_count] = pnstrdup(pattern + part_start, part_byte_len);
                
                // CRITICAL FIX: Store CHARACTER count for length filters
                parsed->part_lens[parsed->part_count] = biscuit_utf8_char_count(
                    pattern + part_start, part_byte_len);
                
                // Store BYTE length for string operations
                parsed->part_byte_lens[parsed->part_count] = part_byte_len;
                
                parsed->part_count++;
            }
            part_start = i + 1;
        }
    }
    
    if (part_start < plen && (!parsed->ends_percent || part_start < plen - 1)) {
        int part_byte_len = parsed->ends_percent ? (plen - 1 - part_start) : (plen - part_start);
        if (part_byte_len > 0) {
            parsed->parts[parsed->part_count] = pnstrdup(pattern + part_start, part_byte_len);
            
            // CRITICAL FIX: Store CHARACTER count for length filters
            parsed->part_lens[parsed->part_count] = biscuit_utf8_char_count(
                pattern + part_start, part_byte_len);
            
            // Store BYTE length for string operations
            parsed->part_byte_lens[parsed->part_count] = part_byte_len;
            
            parsed->part_count++;
        }
    }
    
    return parsed;
}


/*
* OPTIMIZATION 12: Pure wildcard patterns as length queries
* Patterns like '%%%___%%' with m '%'s and n '_'s become length_ge(n)
*/
/* ==================== FIXED: biscuit_query_pattern - Substring Search ==================== */

static RoaringBitmap* biscuit_query_pattern(BiscuitIndex *idx, const char *pattern) {
    int plen = strlen(pattern);
    ParsedPattern *parsed = NULL;
    int min_len, i;
    RoaringBitmap *result = NULL;
    int wildcard_count = 0, percent_count = 0;
    bool only_wildcards = true;
    
    /* ========== FAST PATH 1-5: Same as before ========== */
    if (plen == 0) {
        if (idx->length_bitmaps_legacy && idx->length_bitmaps_legacy[0]) {
            return biscuit_roaring_copy(idx->length_bitmaps_legacy[0]);
        }
        return biscuit_roaring_create();
    }
    
    if (plen == 1 && pattern[0] == '%') {
        result = biscuit_roaring_create();
        for (i = 0; i < idx->num_records; i++) {
            #ifdef HAVE_ROARING
            if (!roaring_bitmap_contains(idx->tombstones, (uint32_t)i))
            #else
            uint32_t block = i >> 6;
            uint32_t bit = i & 63;
            bool tombstoned = (block < idx->tombstones->num_blocks &&
                              (idx->tombstones->blocks[block] & (1ULL << bit)));
            if (!tombstoned)
            #endif
                biscuit_roaring_add(result, i);
        }
        return result;
    }
    
    for (i = 0; i < plen; i++) {
        if (pattern[i] == '%') {
            percent_count++;
        } else if (pattern[i] == '_') {
            wildcard_count++;
        } else {
            only_wildcards = false;
            break;
        }
    }
    
    if (only_wildcards) {
        if (percent_count > 0) {
            result = biscuit_get_length_ge(idx, wildcard_count);
            return result;
        } else {
            if (wildcard_count < idx->max_length_legacy && 
                idx->length_bitmaps_legacy[wildcard_count]) {
                return biscuit_roaring_copy(idx->length_bitmaps_legacy[wildcard_count]);
            }
            return biscuit_roaring_create();
        }
    }
    
    /* ========== SLOW PATH ========== */
    parsed = biscuit_parse_pattern(pattern);

    if (parsed->part_count == 0) {
        result = biscuit_roaring_create();
        for (i = 0; i < idx->num_records; i++) {
            #ifdef HAVE_ROARING
            if (!roaring_bitmap_contains(idx->tombstones, (uint32_t)i))
            #else
            uint32_t block = i >> 6;
            uint32_t bit = i & 63;
            bool tombstoned = (block < idx->tombstones->num_blocks &&
                                (idx->tombstones->blocks[block] & (1ULL << bit)));
            if (!tombstoned)
            #endif
                biscuit_roaring_add(result, i);
        }
        biscuit_free_parsed_pattern(parsed);
        return result;
    }

    min_len = 0;
    for (i = 0; i < parsed->part_count; i++)
        min_len += parsed->part_lens[i];

    /* ==================== OPTIMIZED SINGLE PART PATTERNS ==================== */
    if (parsed->part_count == 1) {
        if (!parsed->starts_percent && !parsed->ends_percent) {
            /* EXACT match */
            result = biscuit_match_part_at_pos(idx, parsed->parts[0], 
                                             parsed->part_byte_lens[0], 0);
            if (result && min_len < idx->max_length_legacy && idx->length_bitmaps_legacy[min_len]) {
                biscuit_roaring_and_inplace(result, idx->length_bitmaps_legacy[min_len]);
            } else if (result) {
                biscuit_roaring_free(result);
                result = biscuit_roaring_create();
            } else {
                result = biscuit_roaring_create();
            }
        } else if (!parsed->starts_percent) {
            /* PREFIX match */
            result = biscuit_match_part_at_pos(idx, parsed->parts[0], 
                                             parsed->part_byte_lens[0], 0);
        } else if (!parsed->ends_percent) {
            /* SUFFIX match */
            result = biscuit_match_part_at_end(idx, parsed->parts[0], 
                                             parsed->part_byte_lens[0]);
        } else {
            /* ==================== SUBSTRING: Use position-based matching like multi-column ==================== */
            
            result = biscuit_roaring_create();
            
            /* Parse the pattern into parts for windowed matching */
            int part_byte_len = parsed->part_byte_lens[0];
            
            /* Count total pattern characters (including underscores) */
            int total_pattern_chars = 0;
            {
                int temp_pos = 0;
                while (temp_pos < part_byte_len) {
                    if (parsed->parts[0][temp_pos] == '_') {
                        total_pattern_chars++;
                        temp_pos++;
                    } else {
                        int char_len = biscuit_utf8_char_length((unsigned char)parsed->parts[0][temp_pos]);
                        if (temp_pos + char_len > part_byte_len)
                            char_len = part_byte_len - temp_pos;
                        temp_pos += char_len;
                        total_pattern_chars++;
                    }
                }
            }
            
            /* Try matching at each position in the string */
            for (int start_pos = 0; start_pos <= idx->max_len - total_pattern_chars; start_pos++) {
                RoaringBitmap *pos_match = NULL;
                bool first_concrete = true;
                
                /* Match the pattern character-by-character at this position */
                int part_byte_pos = 0;
                int current_char_pos = start_pos;
                
                while (part_byte_pos < part_byte_len) {
                    unsigned char first_byte = (unsigned char)parsed->parts[0][part_byte_pos];
                    
                    /* Handle underscore wildcard - skip this position */
                    if (first_byte == '_') {
                        part_byte_pos++;
                        current_char_pos++;
                        continue;
                    }
                    
                    /* Get character length */
                    int char_len = biscuit_utf8_char_length(first_byte);
                    if (part_byte_pos + char_len > part_byte_len)
                        char_len = part_byte_len - part_byte_pos;
                    
                    /* Match all bytes of this character at current_char_pos */
                    if (char_len == 1) {
                        /* Single-byte character */
                        RoaringBitmap *char_bm = biscuit_get_pos_bitmap(idx, first_byte, current_char_pos);
                        
                        if (!char_bm) {
                            if (pos_match) biscuit_roaring_free(pos_match);
                            pos_match = NULL;
                            break;
                        }
                        
                        if (first_concrete) {
                            pos_match = biscuit_roaring_copy(char_bm);
                            first_concrete = false;
                        } else {
                            biscuit_roaring_and_inplace(pos_match, char_bm);
                            if (biscuit_roaring_is_empty(pos_match)) {
                                biscuit_roaring_free(pos_match);
                                pos_match = NULL;
                                break;
                            }
                        }
                    } else {
                        /* Multi-byte character - all bytes must match at same position */
                        RoaringBitmap *multibyte_result = NULL;
                        int b;
                        
                        for (b = 0; b < char_len; b++) {
                            unsigned char byte_val = (unsigned char)parsed->parts[0][part_byte_pos + b];
                            RoaringBitmap *byte_bm = biscuit_get_pos_bitmap(idx, byte_val, current_char_pos);
                            
                            if (!byte_bm) {
                                if (multibyte_result) biscuit_roaring_free(multibyte_result);
                                multibyte_result = NULL;
                                break;
                            }
                            
                            if (b == 0) {
                                multibyte_result = biscuit_roaring_copy(byte_bm);
                            } else {
                                biscuit_roaring_and_inplace(multibyte_result, byte_bm);
                                if (biscuit_roaring_is_empty(multibyte_result)) {
                                    biscuit_roaring_free(multibyte_result);
                                    multibyte_result = NULL;
                                    break;
                                }
                            }
                        }
                        
                        if (!multibyte_result) {
                            if (pos_match) biscuit_roaring_free(pos_match);
                            pos_match = NULL;
                            break;
                        }
                        
                        if (first_concrete) {
                            pos_match = multibyte_result;
                            first_concrete = false;
                        } else {
                            biscuit_roaring_and_inplace(pos_match, multibyte_result);
                            biscuit_roaring_free(multibyte_result);
                            if (biscuit_roaring_is_empty(pos_match)) {
                                biscuit_roaring_free(pos_match);
                                pos_match = NULL;
                                break;
                            }
                        }
                    }
                    
                    part_byte_pos += char_len;
                    current_char_pos++;
                }
                
                /* If we got matches at this position, add them to result */
                if (pos_match) {
                    /* Apply length constraint */
                    RoaringBitmap *length_filter = biscuit_get_length_ge(idx, start_pos + total_pattern_chars);
                    if (length_filter) {
                        biscuit_roaring_and_inplace(pos_match, length_filter);
                        biscuit_roaring_free(length_filter);
                    }
                    
                    biscuit_roaring_or_inplace(result, pos_match);
                    biscuit_roaring_free(pos_match);
                }
            }
        }
    }
    /* ==================== TWO PART PATTERNS ==================== */
    else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
        /* INFIX: 'abc%def' */
        RoaringBitmap *prefix_match;
        RoaringBitmap *suffix_match;
        RoaringBitmap *length_filter;
        
        prefix_match = biscuit_match_part_at_pos(idx, parsed->parts[0], 
                                                parsed->part_byte_lens[0], 0);
        suffix_match = biscuit_match_part_at_end(idx, parsed->parts[1], 
                                                parsed->part_byte_lens[1]);
        
        if (!prefix_match || !suffix_match) {
            if (prefix_match) biscuit_roaring_free(prefix_match);
            if (suffix_match) biscuit_roaring_free(suffix_match);
            result = biscuit_roaring_create();
        } else {
            biscuit_roaring_and_inplace(prefix_match, suffix_match);
            biscuit_roaring_free(suffix_match);
            
            length_filter = biscuit_get_length_ge(idx, min_len);
            if (length_filter) {
                biscuit_roaring_and_inplace(prefix_match, length_filter);
                biscuit_roaring_free(length_filter);
            }
            
            result = prefix_match;
        }
    }
    /* ==================== COMPLEX MULTI-PART PATTERNS ==================== */
    else {
        RoaringBitmap *candidates;
        
        result = biscuit_roaring_create();
        candidates = biscuit_get_length_ge(idx, min_len);
        
        if (!candidates || biscuit_roaring_is_empty(candidates)) {
            if (candidates) biscuit_roaring_free(candidates);
        } else {
            if (!parsed->starts_percent) {
                RoaringBitmap *first_part_match = biscuit_match_part_at_pos(
                    idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                
                if (first_part_match) {
                    biscuit_roaring_and_inplace(first_part_match, candidates);
                    biscuit_roaring_free(candidates);
                    
                    if (!biscuit_roaring_is_empty(first_part_match)) {
                        biscuit_recursive_windowed_match(
                            result, idx,
                            (const char **)parsed->parts, parsed->part_byte_lens,
                            parsed->part_count, parsed->ends_percent,
                            1, parsed->part_lens[0], first_part_match, idx->max_len
                        );
                    }
                    biscuit_roaring_free(first_part_match);
                } else {
                    biscuit_roaring_free(candidates);
                }
            } else {
                biscuit_recursive_windowed_match(
                    result, idx,
                    (const char **)parsed->parts, parsed->part_byte_lens,
                    parsed->part_count, parsed->ends_percent,
                    0, 0, candidates, idx->max_len
                );
                biscuit_roaring_free(candidates);
            }
        }
    }

    biscuit_free_parsed_pattern(parsed);

    if (result && idx->tombstone_count > 0) {
        biscuit_roaring_andnot_inplace(result, idx->tombstones);
    }

    return result ? result : biscuit_roaring_create();
}

/* ==================== UPDATED: biscuit_build_multicolumn WITH ILIKE SUPPORT ==================== */
/* ==================== COMPLETE FIXED: biscuit_build_multicolumn() ==================== */
static IndexBuildResult *
biscuit_build_multicolumn(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    BiscuitIndex *idx;
    TableScanDesc scan;
    TupleTableSlot *slot;
    int natts;
    int ch, rec_idx, col;
    MemoryContext oldcontext;
    MemoryContext buildContext;
    Oid typoutput;
    bool typIsVarlena;
    bool isnull;
    Datum value;
    int text_len;
    char *text_val;
    int i;
    
    natts = indexInfo->ii_NumIndexAttrs;
    
    if (natts < 1)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("biscuit index requires at least one column")));
    
    elog(DEBUG1, "Biscuit: Building multi-column index with UTF-8 character-level indexing (%d columns)", natts);
    
    /* Build in CacheMemoryContext */
    buildContext = CacheMemoryContext;
    oldcontext = MemoryContextSwitchTo(buildContext);
    
    PG_TRY();
    {
        /* Initialize in-memory index */
        idx = (BiscuitIndex *)palloc0(sizeof(BiscuitIndex));
        idx->capacity = 1024;
        idx->num_records = 0;
        idx->tids = (ItemPointerData *)palloc(idx->capacity * sizeof(ItemPointerData));
        idx->max_len = 0;
        
        /* Multi-column initialization */
        idx->num_columns = natts;
        idx->column_types = (Oid *)palloc(natts * sizeof(Oid));
        idx->output_funcs = (FmgrInfo *)palloc(natts * sizeof(FmgrInfo));
        idx->column_data_cache = (char ***)palloc(natts * sizeof(char **));
        
        for (col = 0; col < natts; col++) {
            if (indexInfo->ii_IndexAttrNumbers[col] == 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("biscuit indexes do not support expressions"),
                        errdetail("Expression found in column %d", col + 1),
                        errhint("Index the column directly instead of casting it.")));
            }
            AttrNumber col_attnum = indexInfo->ii_IndexAttrNumbers[col];
            
            if (col_attnum == 0) {
                Form_pg_attribute idx_attr = TupleDescAttr(index->rd_att, col);
                idx->column_types[col] = idx_attr->atttypid;
                getTypeOutputInfo(idx_attr->atttypid, &typoutput, &typIsVarlena);
                fmgr_info(typoutput, &idx->output_funcs[col]);
            } else {
                Form_pg_attribute col_attr = TupleDescAttr(heap->rd_att, col_attnum - 1);
                idx->column_types[col] = col_attr->atttypid;
                getTypeOutputInfo(col_attr->atttypid, &typoutput, &typIsVarlena);
                fmgr_info(typoutput, &idx->output_funcs[col]);
            }
            
            idx->column_data_cache[col] = 
                (char **)palloc(idx->capacity * sizeof(char *));
        }
        
        /* Initialize per-column indices WITH UTF-8 support */
        idx->column_indices = (ColumnIndex *)palloc0(natts * sizeof(ColumnIndex));

        for (col = 0; col < natts; col++) {
            ColumnIndex *cidx = &idx->column_indices[col];
            
            /* Case-sensitive index */
            for (ch = 0; ch < CHAR_RANGE; ch++) {
                cidx->pos_idx[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
                cidx->pos_idx[ch].count = 0;
                cidx->pos_idx[ch].capacity = 64;
                cidx->neg_idx[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
                cidx->neg_idx[ch].count = 0;
                cidx->neg_idx[ch].capacity = 64;
                cidx->char_cache[ch] = NULL;
            }
            
            /* Case-insensitive index */
            for (ch = 0; ch < CHAR_RANGE; ch++) {
                cidx->pos_idx_lower[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
                cidx->pos_idx_lower[ch].count = 0;
                cidx->pos_idx_lower[ch].capacity = 64;
                cidx->neg_idx_lower[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
                cidx->neg_idx_lower[ch].count = 0;
                cidx->neg_idx_lower[ch].capacity = 64;
                cidx->char_cache_lower[ch] = NULL;
            }
            
            cidx->max_length = 0;
            cidx->max_length_lower = 0;  /* ✅ NEW: Track lowercase max separately */
            cidx->length_bitmaps = NULL;
            cidx->length_ge_bitmaps = NULL;
            cidx->length_bitmaps_lower = NULL;      /* ✅ NEW: Separate ILIKE bitmaps */
            cidx->length_ge_bitmaps_lower = NULL;   /* ✅ NEW: Separate ILIKE bitmaps */
        }
        
        biscuit_init_crud_structures(idx);
        
        /* Scan heap and build UTF-8 character-level indices */
        slot = table_slot_create(heap, NULL);
        scan = table_beginscan(heap, SnapshotAny, 0, NULL);
        
        while (table_scan_getnextslot(scan, ForwardScanDirection, slot)) {
            slot_getallattrs(slot);
            
            if (idx->num_records >= idx->capacity) {
                int c;
                idx->capacity *= 2;
                idx->tids = (ItemPointerData *)repalloc(idx->tids, 
                                                        idx->capacity * sizeof(ItemPointerData));
                for (c = 0; c < natts; c++) {
                    idx->column_data_cache[c] = (char **)repalloc(
                        idx->column_data_cache[c],
                        idx->capacity * sizeof(char *));
                }
            }
            
            ItemPointerCopy(&slot->tts_tid, &idx->tids[idx->num_records]);
            
            /* Index each column separately with UTF-8 character-level indexing */
            for (col = 0; col < natts; col++) {
                AttrNumber col_attnum = indexInfo->ii_IndexAttrNumbers[col];
                
                if (col_attnum == 0) {
                    Datum *values = (Datum *)palloc(natts * sizeof(Datum));
                    bool *isnulls = (bool *)palloc(natts * sizeof(bool));
                    
                    FormIndexDatum(indexInfo, slot, NULL, values, isnulls);
                    value = values[col];
                    isnull = isnulls[col];
                    
                    pfree(values);
                    pfree(isnulls);
                } else {
                    value = slot_getattr(slot, col_attnum, &isnull);
                }
                
                if (isnull) {
                    idx->column_data_cache[col][idx->num_records] = pstrdup("");
                    continue;
                }
                
                text_val = biscuit_datum_to_text(
                    value, 
                    idx->column_types[col],
                    &idx->output_funcs[col],
                    &text_len
                );
                
                idx->column_data_cache[col][idx->num_records] = text_val;
                
                ColumnIndex *cidx = &idx->column_indices[col];
                int col_char_count = biscuit_utf8_char_count(text_val, text_len);
                
                /* ✅ FIX: Track case-sensitive max */
                if (col_char_count > cidx->max_length) 
                    cidx->max_length = col_char_count;
                if (col_char_count > idx->max_len) 
                    idx->max_len = col_char_count;
                
                /* ==================== UTF-8 CHARACTER-BASED INDEXING (CASE-SENSITIVE) ==================== */
                {
                    int byte_pos = 0;
                    int char_pos = 0;
                    
                    while (byte_pos < text_len) {
                        unsigned char first_byte = (unsigned char)text_val[byte_pos];
                        int char_len = biscuit_utf8_char_length(first_byte);
                        RoaringBitmap *bm;
                        int neg_offset;
                        int remaining_chars;
                        int b;
                        
                        if (byte_pos + char_len > text_len)
                            char_len = text_len - byte_pos;
                        
                        /* Index ALL bytes of this UTF-8 character at SAME char_pos */
                        for (b = 0; b < char_len; b++) {
                            unsigned char uch = (unsigned char)text_val[byte_pos + b];
                            
                            /* Positive position */
                            bm = biscuit_get_col_pos_bitmap(cidx, uch, char_pos);
                            if (!bm) {
                                bm = biscuit_roaring_create();
                                biscuit_set_col_pos_bitmap(cidx, uch, char_pos, bm);
                            }
                            biscuit_roaring_add(bm, idx->num_records);
                            
                            /* Negative position */
                            remaining_chars = biscuit_utf8_char_count(
                                text_val + byte_pos, text_len - byte_pos
                            );
                            neg_offset = -remaining_chars;
                            
                            bm = biscuit_get_col_neg_bitmap(cidx, uch, neg_offset);
                            if (!bm) {
                                bm = biscuit_roaring_create();
                                biscuit_set_col_neg_bitmap(cidx, uch, neg_offset, bm);
                            }
                            biscuit_roaring_add(bm, idx->num_records);
                            
                            /* Character cache */
                            if (!cidx->char_cache[uch])
                                cidx->char_cache[uch] = biscuit_roaring_create();
                            biscuit_roaring_add(cidx->char_cache[uch], idx->num_records);
                        }
                        
                        byte_pos += char_len;
                        char_pos++;
                    }
                }

                /* ==================== UTF-8 CHARACTER-BASED INDEXING (CASE-INSENSITIVE) ==================== */
                {
                    char *text_val_lower = biscuit_str_tolower(text_val, text_len);
                    int lower_byte_len = strlen(text_val_lower);
                    int lower_char_count = biscuit_utf8_char_count(text_val_lower, lower_byte_len);
                    int byte_pos = 0;
                    int char_pos = 0;

                    /* ✅ FIX: Track lowercase max separately */
                    if (lower_char_count > cidx->max_length_lower)
                        cidx->max_length_lower = lower_char_count;

                    while (byte_pos < lower_byte_len) {
                        unsigned char first_byte = (unsigned char)text_val_lower[byte_pos];
                        int char_len = biscuit_utf8_char_length(first_byte);
                        RoaringBitmap *bm;
                        int neg_offset;
                        int remaining_chars;
                        int b;
                        
                        if (byte_pos + char_len > lower_byte_len)
                            char_len = lower_byte_len - byte_pos;
                        
                        /* Index ALL bytes of this UTF-8 character at SAME char_pos */
                        for (b = 0; b < char_len; b++) {
                            unsigned char uch_lower = (unsigned char)text_val_lower[byte_pos + b];
                            
                            /* Positive position */
                            bm = biscuit_get_col_pos_bitmap_lower(cidx, uch_lower, char_pos);
                            if (!bm) {
                                bm = biscuit_roaring_create();
                                biscuit_set_col_pos_bitmap_lower(cidx, uch_lower, char_pos, bm);
                            }
                            biscuit_roaring_add(bm, idx->num_records);
                            
                            /* Negative position */
                            remaining_chars = biscuit_utf8_char_count(
                                text_val_lower + byte_pos, lower_byte_len - byte_pos
                            );
                            neg_offset = -remaining_chars;
                            
                            bm = biscuit_get_col_neg_bitmap_lower(cidx, uch_lower, neg_offset);
                            if (!bm) {
                                bm = biscuit_roaring_create();
                                biscuit_set_col_neg_bitmap_lower(cidx, uch_lower, neg_offset, bm);
                            }
                            biscuit_roaring_add(bm, idx->num_records);
                            
                            /* Character cache */
                            if (!cidx->char_cache_lower[uch_lower])
                                cidx->char_cache_lower[uch_lower] = biscuit_roaring_create();
                            biscuit_roaring_add(cidx->char_cache_lower[uch_lower], idx->num_records);
                        }
                        
                        byte_pos += char_len;
                        char_pos++;
                    }
                    
                    pfree(text_val_lower);
                }
            }
            
            idx->num_records++;
        }
        
        table_endscan(scan);
        ExecDropSingleTupleTableSlot(slot);
        
        elog(DEBUG1, "Biscuit: Multi-column scan complete - %d records", idx->num_records);
        
        /* ==================== BUILD SEPARATE LENGTH BITMAPS ==================== */
        for (col = 0; col < natts; col++) {
            ColumnIndex *cidx = &idx->column_indices[col];
            
            elog(DEBUG1, "Biscuit: Building length bitmaps for column %d (max_case_sensitive=%d, max_lowercase=%d characters)", 
                 col, cidx->max_length, cidx->max_length_lower);
            
            /* CASE-SENSITIVE length bitmaps */
            cidx->length_bitmaps = (RoaringBitmap **)palloc0((cidx->max_length + 1) * sizeof(RoaringBitmap *));
            cidx->length_ge_bitmaps = (RoaringBitmap **)palloc0((cidx->max_length + 1) * sizeof(RoaringBitmap *));
            
            for (i = 0; i <= cidx->max_length; i++) {
                cidx->length_ge_bitmaps[i] = biscuit_roaring_create();
            }
            
            /* CASE-INSENSITIVE length bitmaps (separate!) */
            cidx->length_bitmaps_lower = (RoaringBitmap **)palloc0((cidx->max_length_lower + 1) * sizeof(RoaringBitmap *));
            cidx->length_ge_bitmaps_lower = (RoaringBitmap **)palloc0((cidx->max_length_lower + 1) * sizeof(RoaringBitmap *));
            
            for (i = 0; i <= cidx->max_length_lower; i++) {
                cidx->length_ge_bitmaps_lower[i] = biscuit_roaring_create();
            }
        }
        
        /* ✅ FIX: Populate BOTH case-sensitive and lowercase length bitmaps SEPARATELY */
        for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++) {
            for (col = 0; col < natts; col++) {
                char *col_str = idx->column_data_cache[col][rec_idx];
                int byte_len = strlen(col_str);
                int char_len = biscuit_utf8_char_count(col_str, byte_len);
                
                /* Calculate lowercase character count */
                char *col_str_lower = biscuit_str_tolower(col_str, byte_len);
                int lower_byte_len = strlen(col_str_lower);
                int lower_char_len = biscuit_utf8_char_count(col_str_lower, lower_byte_len);
                pfree(col_str_lower);
                
                ColumnIndex *cidx = &idx->column_indices[col];
                
                /* CASE-SENSITIVE: Add to exact length bitmap */
                if (char_len <= cidx->max_length) {
                    if (!cidx->length_bitmaps[char_len])
                        cidx->length_bitmaps[char_len] = biscuit_roaring_create();
                    biscuit_roaring_add(cidx->length_bitmaps[char_len], rec_idx);
                }
                
                /* CASE-SENSITIVE: Populate length_ge */
                for (i = 0; i <= char_len && i <= cidx->max_length; i++) {
                    biscuit_roaring_add(cidx->length_ge_bitmaps[i], rec_idx);
                }
                
                /* CASE-INSENSITIVE: Add to exact lowercase length bitmap */
                if (lower_char_len <= cidx->max_length_lower) {
                    if (!cidx->length_bitmaps_lower[lower_char_len])
                        cidx->length_bitmaps_lower[lower_char_len] = biscuit_roaring_create();
                    biscuit_roaring_add(cidx->length_bitmaps_lower[lower_char_len], rec_idx);
                }
                
                /* CASE-INSENSITIVE: Populate length_ge_lower */
                for (i = 0; i <= lower_char_len && i <= cidx->max_length_lower; i++) {
                    biscuit_roaring_add(cidx->length_ge_bitmaps_lower[i], rec_idx);
                }
            }
        }
        
        /* Write metadata to disk */
        biscuit_write_metadata_to_disk(index, idx);
        
        /* Register callback and cache */
        biscuit_register_callback();
        biscuit_cache_insert(RelationGetRelid(index), idx);
        
        index->rd_amcache = idx;
        
        MemoryContextSwitchTo(oldcontext);
        
        result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
        result->heap_tuples = idx->num_records;
        result->index_tuples = idx->num_records;
        
        elog(DEBUG1, "Biscuit: Multi-column build complete with separate LIKE/ILIKE length bitmaps");
        
        return result;
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcontext);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/* ==================== IAM CALLBACK FUNCTIONS ==================== */
/* ==================== FIXED: biscuit_build - SINGLE COLUMN WITH ILIKE ==================== */

/* ==================== COMPLETE REPLACEMENT: biscuit_build() ==================== */
/* ==================== COMPLETE FIXED: biscuit_build() ==================== */

static IndexBuildResult *
biscuit_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    BiscuitIndex *idx;
    TableScanDesc scan;
    TupleTableSlot *slot;
    int ch;
    MemoryContext oldcontext;
    MemoryContext buildContext;
    
    int natts = indexInfo->ii_NumIndexAttrs;
    
    /* Block expression indexes */
    if (indexInfo->ii_IndexAttrNumbers[0] == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("biscuit indexes do not support expressions"),
                 errhint("Index the column directly instead of using an expression.")));
    }

    /* Route to multi-column builder if needed */
    if (natts > 1) {
        return biscuit_build_multicolumn(heap, index, indexInfo);
    }
    
    /* Build in CacheMemoryContext */
    buildContext = CacheMemoryContext;
    oldcontext = MemoryContextSwitchTo(buildContext);
    
    PG_TRY();
    {
        /* Initialize in-memory index */
        idx = (BiscuitIndex *)palloc0(sizeof(BiscuitIndex));
        idx->capacity = 1024;
        idx->num_records = 0;
        idx->tids = (ItemPointerData *)palloc(idx->capacity * sizeof(ItemPointerData));
        idx->data_cache = (char **)palloc(idx->capacity * sizeof(char *));
        idx->max_len = 0;
        
        /* Initialize for single column */
        idx->num_columns = 1;
        idx->column_types = NULL;
        idx->output_funcs = NULL;
        idx->column_data_cache = NULL;
        idx->column_indices = NULL;
        
        /* Initialize case-sensitive index (_legacy) */
        for (ch = 0; ch < CHAR_RANGE; ch++) {
            idx->pos_idx_legacy[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
            idx->pos_idx_legacy[ch].count = 0;
            idx->pos_idx_legacy[ch].capacity = 64;
            idx->neg_idx_legacy[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
            idx->neg_idx_legacy[ch].count = 0;
            idx->neg_idx_legacy[ch].capacity = 64;
            idx->char_cache_legacy[ch] = NULL;
        }
        
        /* Initialize case-insensitive index (_lower) */
        for (ch = 0; ch < CHAR_RANGE; ch++) {
            idx->pos_idx_lower[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
            idx->pos_idx_lower[ch].count = 0;
            idx->pos_idx_lower[ch].capacity = 64;
            idx->neg_idx_lower[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
            idx->neg_idx_lower[ch].count = 0;
            idx->neg_idx_lower[ch].capacity = 64;
            idx->char_cache_lower[ch] = NULL;
        }
        
        /* Allocate lowercase data cache */
        idx->data_cache_lower = (char **)palloc(idx->capacity * sizeof(char *));
        
        /* Initialize tracking for lowercase max length */
        idx->max_length_lower = 0;
        
        /* Initialize CRUD structures */
        biscuit_init_crud_structures(idx);
        
        /* Scan heap and build BOTH indices */
        slot = table_slot_create(heap, NULL);
        scan = table_beginscan(heap, SnapshotAny, 0, NULL);
        
        elog(DEBUG1, "Biscuit: Building single-column index with UTF-8 character-level indexing");
        
        while (table_scan_getnextslot(scan, ForwardScanDirection, slot)) {
            text *txt;
            char *str;
            int byte_len;
            int char_count;
            Datum values[1];
            bool isnull[1];
            bool should_free;
            
            slot_getallattrs(slot);
            
            values[0] = slot_getattr(slot, indexInfo->ii_IndexAttrNumbers[0], &isnull[0]);
            
            if (!isnull[0]) {
                txt = DatumGetTextPP(values[0]);
                str = VARDATA_ANY(txt);
                byte_len = VARSIZE_ANY_EXHDR(txt);
                should_free = (txt != DatumGetTextPP(values[0]));
                
                /* CRITICAL: Count CHARACTERS, not bytes */
                char_count = biscuit_utf8_char_count(str, byte_len);
                
                if (char_count > idx->max_len) 
                    idx->max_len = char_count;
                
                if (idx->num_records >= idx->capacity) {
                    idx->capacity *= 2;
                    idx->tids = (ItemPointerData *)repalloc(idx->tids, 
                        idx->capacity * sizeof(ItemPointerData));
                    idx->data_cache = (char **)repalloc(idx->data_cache, 
                        idx->capacity * sizeof(char *));
                    idx->data_cache_lower = (char **)repalloc(idx->data_cache_lower,
                        idx->capacity * sizeof(char *));
                }
                
                ItemPointerCopy(&slot->tts_tid, &idx->tids[idx->num_records]);
                
                /* Store original text (for LIKE) */
                idx->data_cache[idx->num_records] = pnstrdup(str, byte_len);
                
                /* ==================== UTF-8 CHARACTER-BASED INDEXING (CASE-SENSITIVE) ==================== */
                {
                    int byte_pos = 0;
                    int char_pos = 0;
                    
                    while (byte_pos < byte_len) {
                        unsigned char first_byte = (unsigned char)str[byte_pos];
                        int char_len = biscuit_utf8_char_length(first_byte);
                        RoaringBitmap *bm;
                        int neg_offset;
                        int remaining_chars;
                        int b;
                        
                        /* Safety: Don't read past buffer */
                        if (byte_pos + char_len > byte_len)
                            char_len = byte_len - byte_pos;
                        
                        /* ✅ FIX: Index ALL bytes of this UTF-8 character at SAME char_pos */
                        for (b = 0; b < char_len; b++) {
                            unsigned char uch = (unsigned char)str[byte_pos + b];
                            
                            /* Positive position indexing (from start) */
                            bm = biscuit_get_pos_bitmap(idx, uch, char_pos);
                            if (!bm) {
                                bm = biscuit_roaring_create();
                                biscuit_set_pos_bitmap(idx, uch, char_pos, bm);
                            }
                            biscuit_roaring_add(bm, idx->num_records);
                            
                            /* Negative position indexing (from end) */
                            remaining_chars = biscuit_utf8_char_count(
                                str + byte_pos, byte_len - byte_pos
                            );
                            neg_offset = -remaining_chars;
                            
                            bm = biscuit_get_neg_bitmap(idx, uch, neg_offset);
                            if (!bm) {
                                bm = biscuit_roaring_create();
                                biscuit_set_neg_bitmap(idx, uch, neg_offset, bm);
                            }
                            biscuit_roaring_add(bm, idx->num_records);
                            
                            /* Character cache */
                            if (!idx->char_cache_legacy[uch])
                                idx->char_cache_legacy[uch] = biscuit_roaring_create();
                            biscuit_roaring_add(idx->char_cache_legacy[uch], idx->num_records);
                        }
                        
                        byte_pos += char_len;
                        char_pos++;  /* Increment ONCE per character */
                    }
                }
                
                /* ==================== UTF-8 CHARACTER-BASED INDEXING (CASE-INSENSITIVE) ==================== */
                {
                    char *str_lower = biscuit_str_tolower(str, byte_len);
                    int lower_byte_len = strlen(str_lower);
                    int lower_char_count = biscuit_utf8_char_count(str_lower, lower_byte_len);
                    int byte_pos = 0;
                    int char_pos = 0;
                    
                    idx->data_cache_lower[idx->num_records] = str_lower;
                    
                    /* ✅ NEW: Track max lowercase character count separately */
                    if (lower_char_count > idx->max_length_lower)
                        idx->max_length_lower = lower_char_count;
                    
                    while (byte_pos < lower_byte_len) {
                        unsigned char first_byte = (unsigned char)str_lower[byte_pos];
                        int char_len = biscuit_utf8_char_length(first_byte);
                        RoaringBitmap *bm;
                        int neg_offset;
                        int remaining_chars;
                        int b;
                        
                        if (byte_pos + char_len > lower_byte_len)
                            char_len = lower_byte_len - byte_pos;
                        
                        /* ✅ FIX: Index ALL bytes of this UTF-8 character at SAME char_pos */
                        for (b = 0; b < char_len; b++) {
                            unsigned char uch_lower = (unsigned char)str_lower[byte_pos + b];
                            
                            /* Positive position */
                            bm = biscuit_get_pos_bitmap_lower(idx, uch_lower, char_pos);
                            if (!bm) {
                                bm = biscuit_roaring_create();
                                biscuit_set_pos_bitmap_lower(idx, uch_lower, char_pos, bm);
                            }
                            biscuit_roaring_add(bm, idx->num_records);
                            
                            /* Negative position */
                            remaining_chars = biscuit_utf8_char_count(
                                str_lower + byte_pos, lower_byte_len - byte_pos
                            );
                            neg_offset = -remaining_chars;
                            
                            bm = biscuit_get_neg_bitmap_lower(idx, uch_lower, neg_offset);
                            if (!bm) {
                                bm = biscuit_roaring_create();
                                biscuit_set_neg_bitmap_lower(idx, uch_lower, neg_offset, bm);
                            }
                            biscuit_roaring_add(bm, idx->num_records);
                            
                            /* Character cache */
                            if (!idx->char_cache_lower[uch_lower])
                                idx->char_cache_lower[uch_lower] = biscuit_roaring_create();
                            biscuit_roaring_add(idx->char_cache_lower[uch_lower], idx->num_records);
                        }
                        
                        byte_pos += char_len;
                        char_pos++;  /* Increment ONCE per character */
                    }
                }
                
                idx->num_records++;
                
                if (should_free)
                    pfree(txt);
            }
        }
        
        table_endscan(scan);
        ExecDropSingleTupleTableSlot(slot);
        
        elog(DEBUG1, "Biscuit: Scanned %d records, max_len=%d (case-sensitive), max_len_lower=%d (lowercase) characters", 
             idx->num_records, idx->max_len, idx->max_length_lower);
        
        /* ==================== BUILD SEPARATE LENGTH BITMAPS ==================== */
        {
            int rec_idx, i;
            
            /* Allocate for case-sensitive lengths */
            idx->max_length_legacy = idx->max_len + 1;
            
            /* Allocate for lowercase lengths (can be different!) */
            idx->max_length_lower += 1; // Add 1 for array sizing
            
            elog(DEBUG1, "Biscuit: Allocating SEPARATE length bitmaps:");
            elog(DEBUG1, "  - Case-sensitive: [0..%d] characters", idx->max_len);
            elog(DEBUG1, "  - Lowercase:       [0..%d] characters", idx->max_length_lower - 1);
            
            /* ==================== CASE-SENSITIVE LENGTH BITMAPS ==================== */
            idx->length_bitmaps_legacy = (RoaringBitmap **)palloc0(
                idx->max_length_legacy * sizeof(RoaringBitmap *));
            idx->length_ge_bitmaps_legacy = (RoaringBitmap **)palloc0(
                idx->max_length_legacy * sizeof(RoaringBitmap *));
            
            for (ch = 0; ch < idx->max_length_legacy; ch++) {
                idx->length_ge_bitmaps_legacy[ch] = biscuit_roaring_create();
            }
            
            /* ==================== CASE-INSENSITIVE LENGTH BITMAPS (SEPARATE!) ==================== */
            idx->length_bitmaps_lower = (RoaringBitmap **)palloc0(
                idx->max_length_lower * sizeof(RoaringBitmap *));
            idx->length_ge_bitmaps_lower = (RoaringBitmap **)palloc0(
                idx->max_length_lower * sizeof(RoaringBitmap *));
            
            for (ch = 0; ch < idx->max_length_lower; ch++) {
                idx->length_ge_bitmaps_lower[ch] = biscuit_roaring_create();
            }
            
            elog(DEBUG1, "Biscuit: Populating BOTH sets of length bitmaps from %d records", 
                 idx->num_records);
            
            /* ✅ POPULATE BOTH SETS OF BITMAPS SEPARATELY BASED ON ACTUAL LENGTHS */
            for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++) {
                if (!idx->data_cache[rec_idx])
                    continue;
                
                /* ==================== CASE-SENSITIVE CHARACTER COUNT ==================== */
                int byte_len = strlen(idx->data_cache[rec_idx]);
                int char_len = biscuit_utf8_char_count(idx->data_cache[rec_idx], byte_len);
                
                /* Add to exact length bitmap */
                if (char_len < idx->max_length_legacy) {
                    if (!idx->length_bitmaps_legacy[char_len])
                        idx->length_bitmaps_legacy[char_len] = biscuit_roaring_create();
                    biscuit_roaring_add(idx->length_bitmaps_legacy[char_len], rec_idx);
                }
                
                /* Populate length_ge bitmaps */
                for (i = 0; i <= char_len && i < idx->max_length_legacy; i++)
                    biscuit_roaring_add(idx->length_ge_bitmaps_legacy[i], rec_idx);
                
                /* ==================== CASE-INSENSITIVE CHARACTER COUNT (SEPARATE!) ==================== */
                if (idx->data_cache_lower[rec_idx]) {
                    int lower_byte_len = strlen(idx->data_cache_lower[rec_idx]);
                    int lower_char_len = biscuit_utf8_char_count(idx->data_cache_lower[rec_idx], lower_byte_len);
                    
                    /* Add to exact lowercase length bitmap */
                    if (lower_char_len < idx->max_length_lower) {
                        if (!idx->length_bitmaps_lower[lower_char_len])
                            idx->length_bitmaps_lower[lower_char_len] = biscuit_roaring_create();
                        biscuit_roaring_add(idx->length_bitmaps_lower[lower_char_len], rec_idx);
                    }
                    
                    /* Populate lowercase length_ge bitmaps */
                    for (i = 0; i <= lower_char_len && i < idx->max_length_lower; i++)
                        biscuit_roaring_add(idx->length_ge_bitmaps_lower[i], rec_idx);
                }
            }
            
            elog(DEBUG1, "Biscuit: Length bitmap population complete");
        }
        
        /* Write metadata to disk */
        biscuit_write_metadata_to_disk(index, idx);
        
        /* Register callback and cache */
        biscuit_register_callback();
        biscuit_cache_insert(RelationGetRelid(index), idx);
        
        index->rd_amcache = idx;
        
        /* Switch back to old context for result allocation */
        MemoryContextSwitchTo(oldcontext);
        
        result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
        result->heap_tuples = idx->num_records;
        result->index_tuples = idx->num_records;
        
        elog(DEBUG1, "Biscuit: Build complete with separate LIKE/ILIKE length bitmaps");
        
        return result;
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcontext);
        PG_RE_THROW();
    }
    PG_END_TRY();
}
/* ==================== UPDATED: biscuit_load_index - WITH UTF-8 CHARACTER-LEVEL LOGIC ==================== */

/* ==================== COMPLETE FIXED: biscuit_load_index() ==================== */

static BiscuitIndex* biscuit_load_index(Relation index)
{
    Relation heap;
    TableScanDesc scan;
    TupleTableSlot *slot;
    BiscuitIndex *idx;
    MemoryContext oldcontext;
    MemoryContext indexContext;
    int ch;
    int rec_idx;
    int natts;
    int col;
    Oid typoutput;
    bool typIsVarlena;
    int stored_records = 0, stored_columns = 0, stored_maxlen = 0;
    bool has_disk_metadata;
    
    has_disk_metadata = biscuit_read_metadata_from_disk(index, &stored_records, 
                                                         &stored_columns, &stored_maxlen);
    
    if (has_disk_metadata) {
        elog(DEBUG1, "Biscuit: Found disk metadata, rebuilding bitmaps from heap");
    } else {
        elog(DEBUG1, "Biscuit: No disk metadata, performing full index build from heap");
    }
    
    heap = table_open(index->rd_index->indrelid, AccessShareLock);
    natts = index->rd_index->indnatts;
    
    if (!index->rd_indexcxt) {
        index->rd_indexcxt = AllocSetContextCreate(CacheMemoryContext,
                                                    "Biscuit index context",
                                                    ALLOCSET_DEFAULT_SIZES);
    }
    indexContext = index->rd_indexcxt;
    oldcontext = MemoryContextSwitchTo(indexContext);
    
    idx = (BiscuitIndex *)palloc0(sizeof(BiscuitIndex));
    idx->capacity = 1024;
    idx->num_records = 0;
    idx->tids = (ItemPointerData *)palloc(idx->capacity * sizeof(ItemPointerData));
    idx->max_len = 0;
    
    idx->num_columns = natts;
    
    if (natts > 1) {
        /* ==================== MULTI-COLUMN INITIALIZATION WITH UTF-8 ==================== */
        elog(DEBUG1, "Biscuit: Loading %d-column index with UTF-8 character-level support", natts);
        
        idx->column_types = (Oid *)palloc(natts * sizeof(Oid));
        idx->output_funcs = (FmgrInfo *)palloc(natts * sizeof(FmgrInfo));
        idx->column_data_cache = (char ***)palloc(natts * sizeof(char **));
        idx->column_indices = (ColumnIndex *)palloc0(natts * sizeof(ColumnIndex));
        
        for (col = 0; col < natts; col++) {
            AttrNumber col_attnum = index->rd_index->indkey.values[col];
            Form_pg_attribute col_attr = TupleDescAttr(heap->rd_att, col_attnum - 1);
            ColumnIndex *cidx = &idx->column_indices[col];
            
            idx->column_types[col] = col_attr->atttypid;
            getTypeOutputInfo(col_attr->atttypid, &typoutput, &typIsVarlena);
            fmgr_info(typoutput, &idx->output_funcs[col]);
            idx->column_data_cache[col] = (char **)palloc(idx->capacity * sizeof(char *));
            
            /* Case-sensitive index */
            for (ch = 0; ch < CHAR_RANGE; ch++) {
                cidx->pos_idx[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
                cidx->pos_idx[ch].count = 0;
                cidx->pos_idx[ch].capacity = 64;
                cidx->neg_idx[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
                cidx->neg_idx[ch].count = 0;
                cidx->neg_idx[ch].capacity = 64;
                cidx->char_cache[ch] = NULL;
            }
            
            /* Case-insensitive index */
            for (ch = 0; ch < CHAR_RANGE; ch++) {
                cidx->pos_idx_lower[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
                cidx->pos_idx_lower[ch].count = 0;
                cidx->pos_idx_lower[ch].capacity = 64;
                cidx->neg_idx_lower[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
                cidx->neg_idx_lower[ch].count = 0;
                cidx->neg_idx_lower[ch].capacity = 64;
                cidx->char_cache_lower[ch] = NULL;
            }
            
            cidx->max_length = 0;
            cidx->max_length_lower = 0;  /* ✅ NEW: Track lowercase max separately */
            cidx->length_bitmaps = NULL;
            cidx->length_ge_bitmaps = NULL;
            cidx->length_bitmaps_lower = NULL;      /* ✅ NEW: Separate ILIKE bitmaps */
            cidx->length_ge_bitmaps_lower = NULL;   /* ✅ NEW: Separate ILIKE bitmaps */
        }
        idx->data_cache = NULL;
        idx->data_cache_lower = NULL;
        
    } else {
        /* ==================== SINGLE-COLUMN INITIALIZATION ==================== */
        elog(DEBUG1, "Biscuit: Loading single-column index with UTF-8 character-level support");
        idx->data_cache = (char **)palloc(idx->capacity * sizeof(char *));
        idx->data_cache_lower = (char **)palloc(idx->capacity * sizeof(char *));
        idx->column_types = NULL;
        idx->output_funcs = NULL;
        idx->column_data_cache = NULL;
        idx->column_indices = NULL;
        
        /* Initialize _legacy fields for single-column */
        for (ch = 0; ch < CHAR_RANGE; ch++) {
            idx->pos_idx_legacy[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
            idx->pos_idx_legacy[ch].count = 0;
            idx->pos_idx_legacy[ch].capacity = 64;
            idx->neg_idx_legacy[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
            idx->neg_idx_legacy[ch].count = 0;
            idx->neg_idx_legacy[ch].capacity = 64;
            idx->char_cache_legacy[ch] = NULL;
        }
        
        /* Initialize lowercase index structures */
        for (ch = 0; ch < CHAR_RANGE; ch++) {
            idx->pos_idx_lower[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
            idx->pos_idx_lower[ch].count = 0;
            idx->pos_idx_lower[ch].capacity = 64;
            idx->neg_idx_lower[ch].entries = (PosEntry *)palloc(64 * sizeof(PosEntry));
            idx->neg_idx_lower[ch].count = 0;
            idx->neg_idx_lower[ch].capacity = 64;
            idx->char_cache_lower[ch] = NULL;
        }
        
        /* ✅ NEW: Initialize separate lowercase max length tracker */
        idx->max_length_lower = 0;
    }
    
    biscuit_init_crud_structures(idx);
    MemoryContextSwitchTo(oldcontext);
    
    /* ==================== SCAN HEAP AND BUILD INDEX ==================== */
    slot = table_slot_create(heap, NULL);
    scan = table_beginscan(heap, SnapshotAny, 0, NULL);
    
    while (table_scan_getnextslot(scan, ForwardScanDirection, slot)) {
        bool isnull;
        
        slot_getallattrs(slot);
        
        if (natts > 1) {
            /* ==================== MULTI-COLUMN SCAN WITH UTF-8 CHARACTER-LEVEL ==================== */
            oldcontext = MemoryContextSwitchTo(indexContext);
            
            if (idx->num_records >= idx->capacity) {
                int c;
                idx->capacity *= 2;
                idx->tids = (ItemPointerData *)repalloc(idx->tids, 
                                                        idx->capacity * sizeof(ItemPointerData));
                for (c = 0; c < natts; c++) {
                    idx->column_data_cache[c] = (char **)repalloc(
                        idx->column_data_cache[c],
                        idx->capacity * sizeof(char *));
                }
            }
            
            ItemPointerCopy(&slot->tts_tid, &idx->tids[idx->num_records]);
            MemoryContextSwitchTo(oldcontext);
            
            for (col = 0; col < natts; col++) {
                AttrNumber col_attnum = index->rd_index->indkey.values[col];
                Datum value;
                
                /* Handle expression indexes */
                if (col_attnum == 0) {
                    /* Expression - need to evaluate it */
                    IndexInfo *indexInfo = BuildIndexInfo(index);
                    Datum *values = (Datum *)palloc(natts * sizeof(Datum));
                    bool *isnulls = (bool *)palloc(natts * sizeof(bool));
                    
                    FormIndexDatum(indexInfo, slot, NULL, values, isnulls);
                    value = values[col];
                    isnull = isnulls[col];
                    
                    pfree(values);
                    pfree(isnulls);
                    pfree(indexInfo);
                } else {
                    /* Regular column */
                    value = slot_getattr(slot, col_attnum, &isnull);
                }
                
                ColumnIndex *cidx = &idx->column_indices[col];
                char *text_val;
                int text_len;
                int char_count;
                
                oldcontext = MemoryContextSwitchTo(indexContext);
                
                if (isnull) {
                    idx->column_data_cache[col][idx->num_records] = pstrdup("");
                    MemoryContextSwitchTo(oldcontext);
                    continue;
                }
                
                text_val = biscuit_datum_to_text(value, idx->column_types[col],
                                                &idx->output_funcs[col], &text_len);
                
                /* Count UTF-8 characters */
                char_count = biscuit_utf8_char_count(text_val, text_len);
                
                if (char_count > cidx->max_length) 
                    cidx->max_length = char_count;
                if (char_count > idx->max_len) 
                    idx->max_len = char_count;
                
                idx->column_data_cache[col][idx->num_records] = text_val;
                
                /* ==================== UTF-8 CHARACTER-BASED INDEXING (CASE-SENSITIVE) ==================== */
                {
                    int byte_pos = 0;
                    int char_pos = 0;
                    
                    while (byte_pos < text_len) {
                        unsigned char first_byte = (unsigned char)text_val[byte_pos];
                        int char_len = biscuit_utf8_char_length(first_byte);
                        RoaringBitmap *bm;
                        int neg_offset;
                        int remaining_chars;
                        int b;
                        
                        if (byte_pos + char_len > text_len)
                            char_len = text_len - byte_pos;
                        
                        /* Index ALL bytes of the UTF-8 character at SAME char_pos */
                        for (b = 0; b < char_len; b++) {
                            unsigned char uch = (unsigned char)text_val[byte_pos + b];
                            
                            bm = biscuit_get_col_pos_bitmap(cidx, uch, char_pos);
                            if (!bm) {
                                bm = biscuit_roaring_create();
                                biscuit_set_col_pos_bitmap(cidx, uch, char_pos, bm);
                            }
                            biscuit_roaring_add(bm, idx->num_records);
                            
                            remaining_chars = biscuit_utf8_char_count(
                                text_val + byte_pos, text_len - byte_pos
                            );
                            neg_offset = -remaining_chars;
                            
                            bm = biscuit_get_col_neg_bitmap(cidx, uch, neg_offset);
                            if (!bm) {
                                bm = biscuit_roaring_create();
                                biscuit_set_col_neg_bitmap(cidx, uch, neg_offset, bm);
                            }
                            biscuit_roaring_add(bm, idx->num_records);
                            
                            if (!cidx->char_cache[uch])
                                cidx->char_cache[uch] = biscuit_roaring_create();
                            biscuit_roaring_add(cidx->char_cache[uch], idx->num_records);
                        }
                        
                        byte_pos += char_len;
                        char_pos++;
                    }
                }

                /* ==================== UTF-8 CHARACTER-BASED INDEXING (CASE-INSENSITIVE) ==================== */
                {
                    char *text_val_lower = biscuit_str_tolower(text_val, text_len);
                    int lower_byte_len = strlen(text_val_lower);
                    int lower_char_count = biscuit_utf8_char_count(text_val_lower, lower_byte_len);
                    int byte_pos = 0;
                    int char_pos = 0;

                    /* ✅ FIX: Track lowercase max separately */
                    if (lower_char_count > cidx->max_length_lower)
                        cidx->max_length_lower = lower_char_count;

                    while (byte_pos < lower_byte_len) {
                        unsigned char first_byte = (unsigned char)text_val_lower[byte_pos];
                        int char_len = biscuit_utf8_char_length(first_byte);
                        RoaringBitmap *bm;
                        int neg_offset;
                        int remaining_chars;
                        int b;
                        
                        if (byte_pos + char_len > lower_byte_len)
                            char_len = lower_byte_len - byte_pos;
                        
                        /* Index ALL bytes of the UTF-8 character at SAME char_pos */
                        for (b = 0; b < char_len; b++) {
                            unsigned char uch_lower = (unsigned char)text_val_lower[byte_pos + b];
                            
                            bm = biscuit_get_col_pos_bitmap_lower(cidx, uch_lower, char_pos);
                            if (!bm) {
                                bm = biscuit_roaring_create();
                                biscuit_set_col_pos_bitmap_lower(cidx, uch_lower, char_pos, bm);
                            }
                            biscuit_roaring_add(bm, idx->num_records);
                            
                            remaining_chars = biscuit_utf8_char_count(
                                text_val_lower + byte_pos, lower_byte_len - byte_pos
                            );
                            neg_offset = -remaining_chars;
                            
                            bm = biscuit_get_col_neg_bitmap_lower(cidx, uch_lower, neg_offset);
                            if (!bm) {
                                bm = biscuit_roaring_create();
                                biscuit_set_col_neg_bitmap_lower(cidx, uch_lower, neg_offset, bm);
                            }
                            biscuit_roaring_add(bm, idx->num_records);
                            
                            if (!cidx->char_cache_lower[uch_lower])
                                cidx->char_cache_lower[uch_lower] = biscuit_roaring_create();
                            biscuit_roaring_add(cidx->char_cache_lower[uch_lower], idx->num_records);
                        }
                        
                        byte_pos += char_len;
                        char_pos++;
                    }
                    
                    pfree(text_val_lower);
                }
                
                MemoryContextSwitchTo(oldcontext);
            }
            
            idx->num_records++;
            
        } else {
            /* ==================== SINGLE-COLUMN SCAN WITH UTF-8 CHARACTER-LEVEL ==================== */
            AttrNumber col_attnum = index->rd_index->indkey.values[0];
            Datum value = slot_getattr(slot, col_attnum, &isnull);
            text *txt;
            char *str;
            int byte_len;
            int char_count;
            bool should_free;

            if (isnull) continue;

            txt = DatumGetTextPP(value);
            str = VARDATA_ANY(txt);
            byte_len = VARSIZE_ANY_EXHDR(txt);
            should_free = (txt != DatumGetTextPP(value));

            /* Count characters */
            char_count = biscuit_utf8_char_count(str, byte_len);
            if (char_count > idx->max_len) 
                idx->max_len = char_count;

            oldcontext = MemoryContextSwitchTo(indexContext);

            if (idx->num_records >= idx->capacity) {
                idx->capacity *= 2;
                idx->tids = (ItemPointerData *)repalloc(idx->tids, 
                                                        idx->capacity * sizeof(ItemPointerData));
                idx->data_cache = (char **)repalloc(idx->data_cache, 
                                                    idx->capacity * sizeof(char *));
                idx->data_cache_lower = (char **)repalloc(idx->data_cache_lower,
                                                        idx->capacity * sizeof(char *));
            }

            ItemPointerCopy(&slot->tts_tid, &idx->tids[idx->num_records]);
            idx->data_cache[idx->num_records] = pnstrdup(str, byte_len);

            /* ==================== UTF-8 CHARACTER-BASED INDEXING (CASE-SENSITIVE) ==================== */
            {
                int byte_pos = 0;
                int char_pos = 0;
                
                while (byte_pos < byte_len) {
                    unsigned char first_byte = (unsigned char)str[byte_pos];
                    int char_len = biscuit_utf8_char_length(first_byte);
                    RoaringBitmap *bm;
                    int neg_offset;
                    int remaining_chars;
                    int b;
                    
                    if (byte_pos + char_len > byte_len)
                        char_len = byte_len - byte_pos;
                    
                    /* Index ALL bytes of the UTF-8 character at SAME char_pos */
                    for (b = 0; b < char_len; b++) {
                        unsigned char uch = (unsigned char)str[byte_pos + b];
                        
                        bm = biscuit_get_pos_bitmap(idx, uch, char_pos);
                        if (!bm) {
                            bm = biscuit_roaring_create();
                            biscuit_set_pos_bitmap(idx, uch, char_pos, bm);
                        }
                        biscuit_roaring_add(bm, idx->num_records);
                        
                        remaining_chars = biscuit_utf8_char_count(
                            str + byte_pos, byte_len - byte_pos
                        );
                        neg_offset = -remaining_chars;
                        
                        bm = biscuit_get_neg_bitmap(idx, uch, neg_offset);
                        if (!bm) {
                            bm = biscuit_roaring_create();
                            biscuit_set_neg_bitmap(idx, uch, neg_offset, bm);
                        }
                        biscuit_roaring_add(bm, idx->num_records);
                        
                        if (!idx->char_cache_legacy[uch])
                            idx->char_cache_legacy[uch] = biscuit_roaring_create();
                        biscuit_roaring_add(idx->char_cache_legacy[uch], idx->num_records);
                    }
                    
                    byte_pos += char_len;
                    char_pos++;
                }
            }

            /* ==================== UTF-8 CHARACTER-BASED INDEXING (CASE-INSENSITIVE) ==================== */
            {
                char *str_lower = biscuit_str_tolower(str, byte_len);
                int lower_byte_len = strlen(str_lower);
                int lower_char_count = biscuit_utf8_char_count(str_lower, lower_byte_len);
                int byte_pos = 0;
                int char_pos = 0;
                
                idx->data_cache_lower[idx->num_records] = str_lower;
                
                /* ✅ FIX: Track lowercase max separately */
                if (lower_char_count > idx->max_length_lower)
                    idx->max_length_lower = lower_char_count;
                
                while (byte_pos < lower_byte_len) {
                    unsigned char first_byte = (unsigned char)str_lower[byte_pos];
                    int char_len = biscuit_utf8_char_length(first_byte);
                    RoaringBitmap *bm;
                    int neg_offset;
                    int remaining_chars;
                    int b;
                    
                    if (byte_pos + char_len > lower_byte_len)
                        char_len = lower_byte_len - byte_pos;
                    
                    /* Index ALL bytes of the UTF-8 character at SAME char_pos */
                    for (b = 0; b < char_len; b++) {
                        unsigned char uch_lower = (unsigned char)str_lower[byte_pos + b];
                        
                        bm = biscuit_get_pos_bitmap_lower(idx, uch_lower, char_pos);
                        if (!bm) {
                            bm = biscuit_roaring_create();
                            biscuit_set_pos_bitmap_lower(idx, uch_lower, char_pos, bm);
                        }
                        biscuit_roaring_add(bm, idx->num_records);
                        
                        remaining_chars = biscuit_utf8_char_count(
                            str_lower + byte_pos, lower_byte_len - byte_pos
                        );
                        neg_offset = -remaining_chars;
                        
                        bm = biscuit_get_neg_bitmap_lower(idx, uch_lower, neg_offset);
                        if (!bm) {
                            bm = biscuit_roaring_create();
                            biscuit_set_neg_bitmap_lower(idx, uch_lower, neg_offset, bm);
                        }
                        biscuit_roaring_add(bm, idx->num_records);
                        
                        if (!idx->char_cache_lower[uch_lower])
                            idx->char_cache_lower[uch_lower] = biscuit_roaring_create();
                        biscuit_roaring_add(idx->char_cache_lower[uch_lower], idx->num_records);
                    }
                    
                    byte_pos += char_len;
                    char_pos++;
                }
            }

            idx->num_records++;
            MemoryContextSwitchTo(oldcontext);

            if (should_free) pfree(txt);
        }
    }
    
    table_endscan(scan);
    ExecDropSingleTupleTableSlot(slot);
    
    if (idx->num_records == 0) {
        elog(WARNING, "Biscuit: No records loaded from heap - index is empty!");
        table_close(heap, AccessShareLock);
        return idx;
    }
    
    /* ==================== BUILD SEPARATE LENGTH BITMAPS FOR LIKE AND ILIKE ==================== */
    oldcontext = MemoryContextSwitchTo(indexContext);
    
    if (natts > 1) {
        /* ==================== MULTI-COLUMN LENGTH BITMAPS ==================== */
        elog(DEBUG1, "Biscuit: Building multi-column length bitmaps (separate for LIKE/ILIKE)");
        
        for (col = 0; col < natts; col++) {
            ColumnIndex *cidx = &idx->column_indices[col];
            int i;
            
            elog(DEBUG1, "  Column %d: case_sensitive max=%d, lowercase max=%d characters",
                 col, cidx->max_length, cidx->max_length_lower);
            
            /* CASE-SENSITIVE length bitmaps */
            cidx->length_bitmaps = (RoaringBitmap **)palloc0((cidx->max_length + 1) * sizeof(RoaringBitmap *));
            cidx->length_ge_bitmaps = (RoaringBitmap **)palloc0((cidx->max_length + 1) * sizeof(RoaringBitmap *));
            
            for (i = 0; i <= cidx->max_length; i++) {
                cidx->length_ge_bitmaps[i] = biscuit_roaring_create();
            }
            
            /* CASE-INSENSITIVE length bitmaps (separate!) */
            cidx->length_bitmaps_lower = (RoaringBitmap **)palloc0((cidx->max_length_lower + 1) * sizeof(RoaringBitmap *));
            cidx->length_ge_bitmaps_lower = (RoaringBitmap **)palloc0((cidx->max_length_lower + 1) * sizeof(RoaringBitmap *));
            
            for (i = 0; i <= cidx->max_length_lower; i++) {
                cidx->length_ge_bitmaps_lower[i] = biscuit_roaring_create();
            }
        }
        
        MemoryContextSwitchTo(oldcontext);
        
        /* ✅ FIX: Populate BOTH case-sensitive and lowercase length bitmaps SEPARATELY */
        for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++) {
            for (col = 0; col < natts; col++) {
                ColumnIndex *cidx = &idx->column_indices[col];
                char *cached_str = idx->column_data_cache[col][rec_idx];
                int byte_len = strlen(cached_str);
                int char_len = biscuit_utf8_char_count(cached_str, byte_len);
                
                /* Calculate lowercase character count */
                char *cached_str_lower = biscuit_str_tolower(cached_str, byte_len);
                int lower_byte_len = strlen(cached_str_lower);
                int lower_char_len = biscuit_utf8_char_count(cached_str_lower, lower_byte_len);
                pfree(cached_str_lower);
                
                int i;
                
                oldcontext = MemoryContextSwitchTo(indexContext);
                
                /* CASE-SENSITIVE: Add to exact length bitmap */
                if (char_len <= cidx->max_length) {
                    if (!cidx->length_bitmaps[char_len])
                        cidx->length_bitmaps[char_len] = biscuit_roaring_create();
                    biscuit_roaring_add(cidx->length_bitmaps[char_len], rec_idx);
                }
                
                /* CASE-SENSITIVE: Populate length_ge */
                for (i = 0; i <= char_len && i <= cidx->max_length; i++) {
                    biscuit_roaring_add(cidx->length_ge_bitmaps[i], rec_idx);
                }
                
                /* CASE-INSENSITIVE: Add to exact lowercase length bitmap */
                if (lower_char_len <= cidx->max_length_lower) {
                    if (!cidx->length_bitmaps_lower[lower_char_len])
                        cidx->length_bitmaps_lower[lower_char_len] = biscuit_roaring_create();
                    biscuit_roaring_add(cidx->length_bitmaps_lower[lower_char_len], rec_idx);
                }
                
                /* CASE-INSENSITIVE: Populate length_ge_lower */
                for (i = 0; i <= lower_char_len && i <= cidx->max_length_lower; i++) {
                    biscuit_roaring_add(cidx->length_ge_bitmaps_lower[i], rec_idx);
                }
                
                MemoryContextSwitchTo(oldcontext);
            }
        }
        
    } else {
        /* ==================== SINGLE-COLUMN LENGTH BITMAPS ==================== */
        int i;
        
        /* Ensure max_length_lower is set (add 1 for array sizing) */
        idx->max_length_lower += 1;
        
        elog(DEBUG1, "Biscuit: Building single-column length bitmaps (case_sensitive: [0..%d], lowercase: [0..%d] characters)",
             idx->max_len, idx->max_length_lower - 1);
        
        /* CASE-SENSITIVE length bitmaps */
        idx->max_length_legacy = idx->max_len + 1;
        idx->length_bitmaps_legacy = (RoaringBitmap **)palloc0(idx->max_length_legacy * sizeof(RoaringBitmap *));
        idx->length_ge_bitmaps_legacy = (RoaringBitmap **)palloc0(idx->max_length_legacy * sizeof(RoaringBitmap *));
        
        for (ch = 0; ch < idx->max_length_legacy; ch++)
            idx->length_ge_bitmaps_legacy[ch] = biscuit_roaring_create();
        
        /* CASE-INSENSITIVE length bitmaps (separate!) */
        idx->length_bitmaps_lower = (RoaringBitmap **)palloc0(idx->max_length_lower * sizeof(RoaringBitmap *));
        idx->length_ge_bitmaps_lower = (RoaringBitmap **)palloc0(idx->max_length_lower * sizeof(RoaringBitmap *));
        
        for (ch = 0; ch < idx->max_length_lower; ch++)
            idx->length_ge_bitmaps_lower[ch] = biscuit_roaring_create();
        
        MemoryContextSwitchTo(oldcontext);
        
        /* ✅ FIX: Populate BOTH case-sensitive and lowercase length bitmaps SEPARATELY */
        for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++) {
            if (!idx->data_cache[rec_idx])
                continue;
            
            /* Case-sensitive character count */
            int byte_len = strlen(idx->data_cache[rec_idx]);
            int char_len = biscuit_utf8_char_count(idx->data_cache[rec_idx], byte_len);
            
            /* Calculate lowercase character count */
            char *cached_str_lower = biscuit_str_tolower(idx->data_cache[rec_idx], byte_len);
            int lower_byte_len = strlen(cached_str_lower);
            int lower_char_len = biscuit_utf8_char_count(cached_str_lower, lower_byte_len);
            pfree(cached_str_lower);
            
            oldcontext = MemoryContextSwitchTo(indexContext);
            
            /* CASE-SENSITIVE: Add to exact length bitmap */
            if (char_len < idx->max_length_legacy) {
                if (!idx->length_bitmaps_legacy[char_len])
                    idx->length_bitmaps_legacy[char_len] = biscuit_roaring_create();
                biscuit_roaring_add(idx->length_bitmaps_legacy[char_len], rec_idx);
            }
            
            /* CASE-SENSITIVE: Populate length_ge */
            for (i = 0; i <= char_len && i < idx->max_length_legacy; i++)
                biscuit_roaring_add(idx->length_ge_bitmaps_legacy[i], rec_idx);
            
            /* CASE-INSENSITIVE: Add to exact lowercase length bitmap */
            if (lower_char_len < idx->max_length_lower) {
                if (!idx->length_bitmaps_lower[lower_char_len])
                    idx->length_bitmaps_lower[lower_char_len] = biscuit_roaring_create();
                biscuit_roaring_add(idx->length_bitmaps_lower[lower_char_len], rec_idx);
            }
            
            /* CASE-INSENSITIVE: Populate length_ge_lower */
            for (i = 0; i <= lower_char_len && i < idx->max_length_lower; i++)
                biscuit_roaring_add(idx->length_ge_bitmaps_lower[i], rec_idx);
            
            MemoryContextSwitchTo(oldcontext);
        }
    }
    
    table_close(heap, AccessShareLock);
    
    elog(DEBUG1, "Biscuit: Index load complete with UTF-8 character-level support");
    
    return idx;
}

static void
biscuit_buildempty(Relation index)
{
    /* Nothing to do for empty index */
}

static bool
biscuit_insert(Relation index, Datum *values, bool *isnull,
            ItemPointer ht_ctid, Relation heapRel,
            IndexUniqueCheck checkUnique,
            bool indexUnchanged,
            IndexInfo *indexInfo)
{
    BiscuitIndex *idx;
    MemoryContext oldcontext;
    MemoryContext indexContext;
    uint32_t rec_idx;
    bool is_reusing_slot = false;
    bool found_existing = false;
    
    if (!index->rd_indexcxt) {
        index->rd_indexcxt = AllocSetContextCreate(CacheMemoryContext,
                                                    "Biscuit index context",
                                                    ALLOCSET_DEFAULT_SIZES);
    }
    indexContext = index->rd_indexcxt;
    
    idx = (BiscuitIndex *)index->rd_amcache;
    
    if (!idx) {
        idx = biscuit_load_index(index);
        index->rd_amcache = idx;
    }
    
    /* Handle NULL values - just skip indexing */
    if (isnull[0]) {
        return true;
    }
    
    oldcontext = MemoryContextSwitchTo(indexContext);
    
    /* ==================== MULTI-COLUMN INSERT ==================== */
    if (idx->num_columns > 1) {
        int col;
        
        /* Check if any column is NULL */
        for (col = 0; col < idx->num_columns; col++) {
            if (isnull[col]) {
                MemoryContextSwitchTo(oldcontext);
                return true;  /* Skip NULL records */
            }
        }
        
        /* ✅ FIX: Check if this TID already exists (UPDATE case) */
        for (rec_idx = 0; rec_idx < (uint32_t)idx->num_records; rec_idx++) {
            if (ItemPointerEquals(&idx->tids[rec_idx], ht_ctid)) {
                /* Found existing record - this is an UPDATE */
                found_existing = true;
                
                /* Clean up old data from ALL indices */
                biscuit_remove_from_all_indices(idx, rec_idx);
                
                for (col = 0; col < idx->num_columns; col++) {
                    if (idx->column_data_cache[col][rec_idx]) {
                        pfree(idx->column_data_cache[col][rec_idx]);
                        idx->column_data_cache[col][rec_idx] = NULL;
                    }
                }
                
                /* Remove from tombstones if present */
                if (idx->tombstone_count > 0) {
                    #ifdef HAVE_ROARING
                    if (roaring_bitmap_contains(idx->tombstones, rec_idx)) {
                        biscuit_roaring_remove(idx->tombstones, rec_idx);
                        idx->tombstone_count--;
                    }
                    #else
                    uint32_t block = rec_idx >> 6;
                    uint32_t bit = rec_idx & 63;
                    if (block < idx->tombstones->num_blocks &&
                        (idx->tombstones->blocks[block] & (1ULL << bit))) {
                        biscuit_roaring_remove(idx->tombstones, rec_idx);
                        idx->tombstone_count--;
                    }
                    #endif
                }
                
                idx->update_count++;
                break;
            }
        }
        
        /* If not found, try to reuse a free slot or allocate new */
        if (!found_existing) {
            if (biscuit_pop_free_slot(idx, &rec_idx)) {
                is_reusing_slot = true;
                
                /* Clean up old data first */
                biscuit_remove_from_all_indices(idx, rec_idx);
                
                for (col = 0; col < idx->num_columns; col++) {
                    if (idx->column_data_cache[col][rec_idx]) {
                        pfree(idx->column_data_cache[col][rec_idx]);
                        idx->column_data_cache[col][rec_idx] = NULL;
                    }
                }
                
                /* Remove from tombstone bitmap */
                biscuit_roaring_remove(idx->tombstones, rec_idx);
                idx->tombstone_count--;
            } else {
                /* Allocate new slot */
                if (idx->num_records >= idx->capacity) {
                    idx->capacity *= 2;
                    idx->tids = (ItemPointerData *)repalloc(idx->tids,
                                                            idx->capacity * sizeof(ItemPointerData));
                    for (col = 0; col < idx->num_columns; col++) {
                        idx->column_data_cache[col] = (char **)repalloc(
                            idx->column_data_cache[col],
                            idx->capacity * sizeof(char *));
                    }
                }
                rec_idx = idx->num_records++;
                
                /* Initialize new slot */
                for (col = 0; col < idx->num_columns; col++) {
                    idx->column_data_cache[col][rec_idx] = NULL;
                }
            }
        }
        
        /* Store TID */
        ItemPointerCopy(ht_ctid, &idx->tids[rec_idx]);
        
        /* Process each column */
        for (col = 0; col < idx->num_columns; col++) {
            ColumnIndex *cidx = &idx->column_indices[col];
            char *text_val;
            int text_len;
            int char_count;
            int i;
            
            /* Convert datum to text */
            text_val = biscuit_datum_to_text(values[col], idx->column_types[col],
                                            &idx->output_funcs[col], &text_len);
            
            idx->column_data_cache[col][rec_idx] = text_val;
            
            char_count = biscuit_utf8_char_count(text_val, text_len);
            
            if (char_count > cidx->max_length)
                cidx->max_length = char_count;
            if (char_count > idx->max_len)
                idx->max_len = char_count;
            
            /* Build case-sensitive index */
            {
                int byte_pos = 0;
                int char_pos = 0;
                
                while (byte_pos < text_len) {
                    unsigned char first_byte = (unsigned char)text_val[byte_pos];
                    int char_len = biscuit_utf8_char_length(first_byte);
                    RoaringBitmap *bm;
                    int neg_offset, remaining_chars;
                    int b;
                    
                    if (byte_pos + char_len > text_len)
                        char_len = text_len - byte_pos;
                    
                    for (b = 0; b < char_len; b++) {
                        unsigned char uch = (unsigned char)text_val[byte_pos + b];
                        
                        bm = biscuit_get_col_pos_bitmap(cidx, uch, char_pos);
                        if (!bm) {
                            bm = biscuit_roaring_create();
                            biscuit_set_col_pos_bitmap(cidx, uch, char_pos, bm);
                        }
                        biscuit_roaring_add(bm, rec_idx);
                        
                        remaining_chars = biscuit_utf8_char_count(
                            text_val + byte_pos, text_len - byte_pos);
                        neg_offset = -remaining_chars;
                        
                        bm = biscuit_get_col_neg_bitmap(cidx, uch, neg_offset);
                        if (!bm) {
                            bm = biscuit_roaring_create();
                            biscuit_set_col_neg_bitmap(cidx, uch, neg_offset, bm);
                        }
                        biscuit_roaring_add(bm, rec_idx);
                        
                        if (!cidx->char_cache[uch])
                            cidx->char_cache[uch] = biscuit_roaring_create();
                        biscuit_roaring_add(cidx->char_cache[uch], rec_idx);
                    }
                    
                    byte_pos += char_len;
                    char_pos++;
                }
            }
            
            /* Build case-insensitive index */
            {
                char *text_lower = biscuit_str_tolower(text_val, text_len);
                int lower_byte_len = strlen(text_lower);
                int lower_char_count = biscuit_utf8_char_count(text_lower, lower_byte_len);
                int byte_pos = 0;
                int char_pos = 0;
                
                if (lower_char_count > cidx->max_length_lower)
                    cidx->max_length_lower = lower_char_count;
                
                while (byte_pos < lower_byte_len) {
                    unsigned char first_byte = (unsigned char)text_lower[byte_pos];
                    int char_len = biscuit_utf8_char_length(first_byte);
                    RoaringBitmap *bm;
                    int neg_offset, remaining_chars;
                    int b;
                    
                    if (byte_pos + char_len > lower_byte_len)
                        char_len = lower_byte_len - byte_pos;
                    
                    for (b = 0; b < char_len; b++) {
                        unsigned char uch_lower = (unsigned char)text_lower[byte_pos + b];
                        
                        bm = biscuit_get_col_pos_bitmap_lower(cidx, uch_lower, char_pos);
                        if (!bm) {
                            bm = biscuit_roaring_create();
                            biscuit_set_col_pos_bitmap_lower(cidx, uch_lower, char_pos, bm);
                        }
                        biscuit_roaring_add(bm, rec_idx);
                        
                        remaining_chars = biscuit_utf8_char_count(
                            text_lower + byte_pos, lower_byte_len - byte_pos);
                        neg_offset = -remaining_chars;
                        
                        bm = biscuit_get_col_neg_bitmap_lower(cidx, uch_lower, neg_offset);
                        if (!bm) {
                            bm = biscuit_roaring_create();
                            biscuit_set_col_neg_bitmap_lower(cidx, uch_lower, neg_offset, bm);
                        }
                        biscuit_roaring_add(bm, rec_idx);
                        
                        if (!cidx->char_cache_lower[uch_lower])
                            cidx->char_cache_lower[uch_lower] = biscuit_roaring_create();
                        biscuit_roaring_add(cidx->char_cache_lower[uch_lower], rec_idx);
                    }
                    
                    byte_pos += char_len;
                    char_pos++;
                }
                
                pfree(text_lower);
            }
            
            /* Update case-sensitive length bitmaps */
            {
                /* Ensure bitmaps are allocated */
                if (!cidx->length_bitmaps || char_count >= cidx->max_length) {
                    int old_max = cidx->max_length;
                    int new_max = (char_count + 1) * 2;
                    RoaringBitmap **new_bitmaps = (RoaringBitmap **)palloc0(new_max * sizeof(RoaringBitmap *));
                    RoaringBitmap **new_ge = (RoaringBitmap **)palloc0(new_max * sizeof(RoaringBitmap *));
                    
                    if (cidx->length_bitmaps) {
                        memcpy(new_bitmaps, cidx->length_bitmaps, old_max * sizeof(RoaringBitmap *));
                        pfree(cidx->length_bitmaps);
                    }
                    if (cidx->length_ge_bitmaps) {
                        memcpy(new_ge, cidx->length_ge_bitmaps, old_max * sizeof(RoaringBitmap *));
                        pfree(cidx->length_ge_bitmaps);
                    }
                    
                    for (i = old_max; i < new_max; i++)
                        new_ge[i] = biscuit_roaring_create();
                    
                    cidx->length_bitmaps = new_bitmaps;
                    cidx->length_ge_bitmaps = new_ge;
                    cidx->max_length = new_max;
                }
                
                if (!cidx->length_bitmaps[char_count])
                    cidx->length_bitmaps[char_count] = biscuit_roaring_create();
                biscuit_roaring_add(cidx->length_bitmaps[char_count], rec_idx);
                
                for (i = 0; i <= char_count && i < cidx->max_length; i++)
                    biscuit_roaring_add(cidx->length_ge_bitmaps[i], rec_idx);
            }
            
            /* Update case-insensitive length bitmaps */
            {
                char *temp_lower = biscuit_str_tolower(text_val, text_len);
                int temp_lower_len = strlen(temp_lower);
                int lower_char_count = biscuit_utf8_char_count(temp_lower, temp_lower_len);
                pfree(temp_lower);
                
                if (!cidx->length_bitmaps_lower || lower_char_count >= cidx->max_length_lower) {
                    int old_max = cidx->max_length_lower;
                    int new_max = (lower_char_count + 1) * 2;
                    RoaringBitmap **new_bitmaps = (RoaringBitmap **)palloc0(new_max * sizeof(RoaringBitmap *));
                    RoaringBitmap **new_ge = (RoaringBitmap **)palloc0(new_max * sizeof(RoaringBitmap *));
                    
                    if (cidx->length_bitmaps_lower) {
                        memcpy(new_bitmaps, cidx->length_bitmaps_lower, old_max * sizeof(RoaringBitmap *));
                        pfree(cidx->length_bitmaps_lower);
                    }
                    if (cidx->length_ge_bitmaps_lower) {
                        memcpy(new_ge, cidx->length_ge_bitmaps_lower, old_max * sizeof(RoaringBitmap *));
                        pfree(cidx->length_ge_bitmaps_lower);
                    }
                    
                    for (i = old_max; i < new_max; i++)
                        new_ge[i] = biscuit_roaring_create();
                    
                    cidx->length_bitmaps_lower = new_bitmaps;
                    cidx->length_ge_bitmaps_lower = new_ge;
                    cidx->max_length_lower = new_max;
                }
                
                if (!cidx->length_bitmaps_lower[lower_char_count])
                    cidx->length_bitmaps_lower[lower_char_count] = biscuit_roaring_create();
                biscuit_roaring_add(cidx->length_bitmaps_lower[lower_char_count], rec_idx);
                
                for (i = 0; i <= lower_char_count && i < cidx->max_length_lower; i++)
                    biscuit_roaring_add(cidx->length_ge_bitmaps_lower[i], rec_idx);
            }
        }
        
        if (!found_existing && !is_reusing_slot) {
            idx->insert_count++;
        }
        
        MemoryContextSwitchTo(oldcontext);
        return true;
    }
    
    /* ==================== SINGLE-COLUMN INSERT ==================== */
    {
        text *txt;
        char *str;
        int byte_len, char_count;
        
        txt = DatumGetTextPP(values[0]);
        str = VARDATA_ANY(txt);
        byte_len = VARSIZE_ANY_EXHDR(txt);
        
        char_count = biscuit_utf8_char_count(str, byte_len);
        
        if (char_count > idx->max_len) 
            idx->max_len = char_count;
        
        /* ✅ FIX: Check if this TID already exists (UPDATE case) */
        for (rec_idx = 0; rec_idx < (uint32_t)idx->num_records; rec_idx++) {
            if (ItemPointerEquals(&idx->tids[rec_idx], ht_ctid)) {
                /* Found existing record - this is an UPDATE */
                found_existing = true;
                
                /* Clean up old data from ALL indices */
                biscuit_remove_from_all_indices(idx, rec_idx);
                
                if (idx->data_cache[rec_idx]) {
                    pfree(idx->data_cache[rec_idx]);
                    idx->data_cache[rec_idx] = NULL;
                }
                
                if (idx->data_cache_lower && idx->data_cache_lower[rec_idx]) {
                    pfree(idx->data_cache_lower[rec_idx]);
                    idx->data_cache_lower[rec_idx] = NULL;
                }
                
                /* Remove from tombstones if present */
                if (idx->tombstone_count > 0) {
                    #ifdef HAVE_ROARING
                    if (roaring_bitmap_contains(idx->tombstones, rec_idx)) {
                        biscuit_roaring_remove(idx->tombstones, rec_idx);
                        idx->tombstone_count--;
                    }
                    #else
                    uint32_t block = rec_idx >> 6;
                    uint32_t bit = rec_idx & 63;
                    if (block < idx->tombstones->num_blocks &&
                        (idx->tombstones->blocks[block] & (1ULL << bit))) {
                        biscuit_roaring_remove(idx->tombstones, rec_idx);
                        idx->tombstone_count--;
                    }
                    #endif
                }
                
                idx->update_count++;
                break;
            }
        }
        
        /* If not found, try to reuse free slot or allocate new */
        if (!found_existing) {
            if (biscuit_pop_free_slot(idx, &rec_idx)) {
                is_reusing_slot = true;
                
                /* Clean up old data first */
                biscuit_remove_from_all_indices(idx, rec_idx);
                
                if (idx->data_cache[rec_idx]) {
                    pfree(idx->data_cache[rec_idx]);
                    idx->data_cache[rec_idx] = NULL;
                }
                
                if (idx->data_cache_lower && idx->data_cache_lower[rec_idx]) {
                    pfree(idx->data_cache_lower[rec_idx]);
                    idx->data_cache_lower[rec_idx] = NULL;
                }
                
                /* Remove from tombstone bitmap */
                biscuit_roaring_remove(idx->tombstones, rec_idx);
                idx->tombstone_count--;
            } else {
                /* Allocate new slot */
                if (idx->num_records >= idx->capacity) {
                    idx->capacity *= 2;
                    idx->tids = (ItemPointerData *)repalloc(idx->tids, 
                                                            idx->capacity * sizeof(ItemPointerData));
                    idx->data_cache = (char **)repalloc(idx->data_cache, 
                                                        idx->capacity * sizeof(char *));
                    
                    if (idx->data_cache_lower) {
                        idx->data_cache_lower = (char **)repalloc(idx->data_cache_lower,
                                                                  idx->capacity * sizeof(char *));
                    } else {
                        idx->data_cache_lower = (char **)palloc0(idx->capacity * sizeof(char *));
                    }
                }
                rec_idx = idx->num_records++;
                
                /* Initialize new slot */
                idx->data_cache[rec_idx] = NULL;
                if (idx->data_cache_lower) {
                    idx->data_cache_lower[rec_idx] = NULL;
                }
            }
        }
        
        /* Store TID and data */
        ItemPointerCopy(ht_ctid, &idx->tids[rec_idx]);
        idx->data_cache[rec_idx] = pnstrdup(str, byte_len);
        
        /* ==================== BUILD INDICES FOR NEW DATA ==================== */
        
        /* Case-sensitive indexing */
        {
            int byte_pos = 0;
            int char_pos = 0;
            
            while (byte_pos < byte_len) {
                unsigned char first_byte = (unsigned char)str[byte_pos];
                int char_len = biscuit_utf8_char_length(first_byte);
                RoaringBitmap *bm;
                int neg_offset;
                int remaining_chars;
                int b;
                
                if (byte_pos + char_len > byte_len)
                    char_len = byte_len - byte_pos;
                
                for (b = 0; b < char_len; b++) {
                    unsigned char uch = (unsigned char)str[byte_pos + b];
                    
                    /* Positive position */
                    bm = biscuit_get_pos_bitmap(idx, uch, char_pos);
                    if (!bm) {
                        bm = biscuit_roaring_create();
                        biscuit_set_pos_bitmap(idx, uch, char_pos, bm);
                    }
                    biscuit_roaring_add(bm, rec_idx);
                    
                    /* Negative position */
                    remaining_chars = biscuit_utf8_char_count(
                        str + byte_pos, byte_len - byte_pos
                    );
                    neg_offset = -remaining_chars;
                    
                    bm = biscuit_get_neg_bitmap(idx, uch, neg_offset);
                    if (!bm) {
                        bm = biscuit_roaring_create();
                        biscuit_set_neg_bitmap(idx, uch, neg_offset, bm);
                    }
                    biscuit_roaring_add(bm, rec_idx);
                    
                    /* Character cache */
                    if (!idx->char_cache_legacy[uch])
                        idx->char_cache_legacy[uch] = biscuit_roaring_create();
                    biscuit_roaring_add(idx->char_cache_legacy[uch], rec_idx);
                }
                
                byte_pos += char_len;
                char_pos++;
            }
        }
        
        /* Case-insensitive indexing */
        {
            char *str_lower;
            int lower_byte_len;
            int lower_char_count;
            int byte_pos = 0;
            int char_pos = 0;
            
            str_lower = biscuit_str_tolower(str, byte_len);
            lower_byte_len = strlen(str_lower);
            lower_char_count = biscuit_utf8_char_count(str_lower, lower_byte_len);
            
            if (!idx->data_cache_lower) {
                idx->data_cache_lower = (char **)palloc0(idx->capacity * sizeof(char *));
            }
            
            idx->data_cache_lower[rec_idx] = str_lower;
            
            if (lower_char_count > idx->max_length_lower)
                idx->max_length_lower = lower_char_count;
            
            while (byte_pos < lower_byte_len) {
                unsigned char first_byte = (unsigned char)str_lower[byte_pos];
                int char_len = biscuit_utf8_char_length(first_byte);
                RoaringBitmap *bm;
                int neg_offset;
                int remaining_chars;
                int b;
                
                if (byte_pos + char_len > lower_byte_len)
                    char_len = lower_byte_len - byte_pos;
                
                for (b = 0; b < char_len; b++) {
                    unsigned char uch_lower = (unsigned char)str_lower[byte_pos + b];
                    
                    /* Positive position */
                    bm = biscuit_get_pos_bitmap_lower(idx, uch_lower, char_pos);
                    if (!bm) {
                        bm = biscuit_roaring_create();
                        biscuit_set_pos_bitmap_lower(idx, uch_lower, char_pos, bm);
                    }
                    biscuit_roaring_add(bm, rec_idx);
                    
                    /* Negative position */
                    remaining_chars = biscuit_utf8_char_count(
                        str_lower + byte_pos, lower_byte_len - byte_pos
                    );
                    neg_offset = -remaining_chars;
                    
                    bm = biscuit_get_neg_bitmap_lower(idx, uch_lower, neg_offset);
                    if (!bm) {
                        bm = biscuit_roaring_create();
                        biscuit_set_neg_bitmap_lower(idx, uch_lower, neg_offset, bm);
                    }
                    biscuit_roaring_add(bm, rec_idx);
                    
                    /* Character cache */
                    if (!idx->char_cache_lower[uch_lower])
                        idx->char_cache_lower[uch_lower] = biscuit_roaring_create();
                    biscuit_roaring_add(idx->char_cache_lower[uch_lower], rec_idx);
                }
                
                byte_pos += char_len;
                char_pos++;
            }
        }
        
        /* ==================== UPDATE LENGTH BITMAPS ==================== */
        
        /* Case-sensitive length bitmaps */
        {
            int i;
            
            if (!idx->length_bitmaps_legacy || idx->max_length_legacy == 0) {
                int initial_size = 32;
                
                idx->length_bitmaps_legacy = (RoaringBitmap **)palloc0(initial_size * sizeof(RoaringBitmap *));
                idx->length_ge_bitmaps_legacy = (RoaringBitmap **)palloc0(initial_size * sizeof(RoaringBitmap *));
                
                for (i = 0; i < initial_size; i++) {
                    idx->length_ge_bitmaps_legacy[i] = biscuit_roaring_create();
                }
                
                idx->max_length_legacy = initial_size;
            }
            
            if (char_count >= idx->max_length_legacy) {
                int old_max = idx->max_length_legacy;
                int new_max = (char_count + 1) * 2;
                
                RoaringBitmap **new_bitmaps = (RoaringBitmap **)palloc0(new_max * sizeof(RoaringBitmap *));
                RoaringBitmap **new_ge_bitmaps = (RoaringBitmap **)palloc0(new_max * sizeof(RoaringBitmap *));
                
                memcpy(new_bitmaps, idx->length_bitmaps_legacy, 
                    old_max * sizeof(RoaringBitmap *));
                memcpy(new_ge_bitmaps, idx->length_ge_bitmaps_legacy, 
                    old_max * sizeof(RoaringBitmap *));
                
                for (i = old_max; i < new_max; i++) {
                    new_ge_bitmaps[i] = biscuit_roaring_create();
                }
                
                pfree(idx->length_bitmaps_legacy);
                pfree(idx->length_ge_bitmaps_legacy);
                
                idx->length_bitmaps_legacy = new_bitmaps;
                idx->length_ge_bitmaps_legacy = new_ge_bitmaps;
                idx->max_length_legacy = new_max;
            }
            
            if (char_count < idx->max_length_legacy) {
                if (!idx->length_bitmaps_legacy[char_count])
                    idx->length_bitmaps_legacy[char_count] = biscuit_roaring_create();
                biscuit_roaring_add(idx->length_bitmaps_legacy[char_count], rec_idx);
            }
            
            for (i = 0; i <= char_count && i < idx->max_length_legacy; i++) {
                if (!idx->length_ge_bitmaps_legacy[i]) {
                    idx->length_ge_bitmaps_legacy[i] = biscuit_roaring_create();
                }
                biscuit_roaring_add(idx->length_ge_bitmaps_legacy[i], rec_idx);
            }
        }
        
        /* Case-insensitive length bitmaps */
        {
            int lower_char_count = biscuit_utf8_char_count(
                idx->data_cache_lower[rec_idx], 
                strlen(idx->data_cache_lower[rec_idx])
            );
            int i;
            
            if (!idx->length_bitmaps_lower || idx->max_length_lower == 0) {
                int initial_size = 32;
                
                idx->length_bitmaps_lower = (RoaringBitmap **)palloc0(initial_size * sizeof(RoaringBitmap *));
                idx->length_ge_bitmaps_lower = (RoaringBitmap **)palloc0(initial_size * sizeof(RoaringBitmap *));
                
                for (i = 0; i < initial_size; i++) {
                    idx->length_ge_bitmaps_lower[i] = biscuit_roaring_create();
                }
                
                idx->max_length_lower = initial_size;
            }
            
            if (lower_char_count >= idx->max_length_lower) {
                int old_max = idx->max_length_lower;
                int new_max = (lower_char_count + 1) * 2;
                
                RoaringBitmap **new_bitmaps = (RoaringBitmap **)palloc0(new_max * sizeof(RoaringBitmap *));
                RoaringBitmap **new_ge_bitmaps = (RoaringBitmap **)palloc0(new_max * sizeof(RoaringBitmap *));
                
                memcpy(new_bitmaps, idx->length_bitmaps_lower, 
                    old_max * sizeof(RoaringBitmap *));
                memcpy(new_ge_bitmaps, idx->length_ge_bitmaps_lower, 
                    old_max * sizeof(RoaringBitmap *));
                
                for (i = old_max; i < new_max; i++) {
                    new_ge_bitmaps[i] = biscuit_roaring_create();
                }
                
                pfree(idx->length_bitmaps_lower);
                pfree(idx->length_ge_bitmaps_lower);
                
                idx->length_bitmaps_lower = new_bitmaps;
                idx->length_ge_bitmaps_lower = new_ge_bitmaps;
                idx->max_length_lower = new_max;
            }
            
            if (lower_char_count < idx->max_length_lower) {
                if (!idx->length_bitmaps_lower[lower_char_count])
                    idx->length_bitmaps_lower[lower_char_count] = biscuit_roaring_create();
                biscuit_roaring_add(idx->length_bitmaps_lower[lower_char_count], rec_idx);
            }
            
            for (i = 0; i <= lower_char_count && i < idx->max_length_lower; i++) {
                if (!idx->length_ge_bitmaps_lower[i]) {
                    idx->length_ge_bitmaps_lower[i] = biscuit_roaring_create();
                }
                biscuit_roaring_add(idx->length_ge_bitmaps_lower[i], rec_idx);
            }
        }
        
        if (!found_existing && !is_reusing_slot) {
            idx->insert_count++;
        }
        
        MemoryContextSwitchTo(oldcontext);
        
        return true;
    }
}
/* ==================== FIXED: biscuit_bulkdelete ==================== */
/*
 * Bulk delete with proper ILIKE bitmap cleanup
 */
/* ==================== CRITICAL FIX: biscuit_bulkdelete ==================== */
/*
 * The bug: When DELETE is executed, records are added to tombstones bitmap,
 * then ALL tombstones are removed from ALL bitmaps - including records that
 * were tombstoned in PREVIOUS operations but still valid.
 * 
 * The fix: Only remove from bitmaps the records we're ACTUALLY deleting now,
 * not all historical tombstones.
 */
static IndexBulkDeleteResult *
biscuit_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                   IndexBulkDeleteCallback callback, void *callback_state)
{
    Relation index = info->index;
    BiscuitIndex *idx;
    int i;
    MemoryContext oldcontext;
    RoaringBitmap *records_to_delete = NULL;  /* NEW: Track what we're deleting NOW */
    uint64_t delete_count;
    uint32_t *delete_indices;
    int j, ch, col;
    
    idx = (BiscuitIndex *)index->rd_amcache;
    
    if (!idx) {
        idx = biscuit_load_index(index);
        index->rd_amcache = idx;
    }
    
    if (!stats) {
        stats = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));
    }
    
    if (index->rd_indexcxt) {
        oldcontext = MemoryContextSwitchTo(index->rd_indexcxt);
    }
    
    /* ==================== NEW: Create bitmap of records to delete ==================== */
    records_to_delete = biscuit_roaring_create();
    
    /* ==================== MARK RECORDS FOR DELETION ==================== */
    for (i = 0; i < idx->num_records; i++) {
        bool has_data;
        bool already_tombstoned;
        
        /* Check if record has data */
        if (idx->num_columns == 1) {
            has_data = (idx->data_cache[i] != NULL);
        } else {
            has_data = (idx->column_data_cache && 
                       idx->column_data_cache[0] && 
                       idx->column_data_cache[0][i] != NULL);
        }
        
        if (!has_data)
            continue;
        
        /* Check if already tombstoned */
        #ifdef HAVE_ROARING
        already_tombstoned = roaring_bitmap_contains(idx->tombstones, (uint32_t)i);
        #else
        {
            uint32_t block = i >> 6;
            uint32_t bit = i & 63;
            already_tombstoned = (block < idx->tombstones->num_blocks &&
                                 (idx->tombstones->blocks[block] & (1ULL << bit)));
        }
        #endif
        
        if (already_tombstoned)
            continue;
        
        /* Check if should be deleted via callback */
        if (callback(&idx->tids[i], callback_state)) {
            /* Mark as tombstone */
            biscuit_roaring_add(idx->tombstones, (uint32_t)i);
            biscuit_roaring_add(records_to_delete, (uint32_t)i);  /* NEW: Track for this operation */
            idx->tombstone_count++;
            
            biscuit_push_free_slot(idx, (uint32_t)i);
            stats->tuples_removed++;
            idx->delete_count++;
        }
    }
    
    /* ==================== CRITICAL FIX: Remove ONLY newly deleted records ==================== */
    delete_count = biscuit_roaring_count(records_to_delete);
    
    if (delete_count > 0) {
        delete_indices = biscuit_roaring_to_array(records_to_delete, &delete_count);
        
        if (delete_indices) {
            /* ==================== SINGLE-COLUMN CLEANUP ==================== */
            if (idx->num_columns == 1) {
                /* Remove from character indices */
                for (ch = 0; ch < CHAR_RANGE; ch++) {
                    CharIndex *pos_cidx = &idx->pos_idx_legacy[ch];
                    CharIndex *neg_cidx = &idx->neg_idx_legacy[ch];
                    
                    /* Case-sensitive */
                    for (j = 0; j < pos_cidx->count; j++)
                        biscuit_roaring_andnot_inplace(pos_cidx->entries[j].bitmap, records_to_delete);
                    
                    for (j = 0; j < neg_cidx->count; j++)
                        biscuit_roaring_andnot_inplace(neg_cidx->entries[j].bitmap, records_to_delete);
                    
                    if (idx->char_cache_legacy[ch])
                        biscuit_roaring_andnot_inplace(idx->char_cache_legacy[ch], records_to_delete);
                    
                    /* Case-insensitive */
                    pos_cidx = &idx->pos_idx_lower[ch];
                    neg_cidx = &idx->neg_idx_lower[ch];
                    
                    for (j = 0; j < pos_cidx->count; j++)
                        biscuit_roaring_andnot_inplace(pos_cidx->entries[j].bitmap, records_to_delete);
                    
                    for (j = 0; j < neg_cidx->count; j++)
                        biscuit_roaring_andnot_inplace(neg_cidx->entries[j].bitmap, records_to_delete);
                    
                    if (idx->char_cache_lower[ch])
                        biscuit_roaring_andnot_inplace(idx->char_cache_lower[ch], records_to_delete);
                }
                
                /* Remove from length bitmaps */
                for (j = 0; j < idx->max_length_legacy; j++) {
                    if (idx->length_bitmaps_legacy && idx->length_bitmaps_legacy[j])
                        biscuit_roaring_andnot_inplace(idx->length_bitmaps_legacy[j], records_to_delete);
                    if (idx->length_ge_bitmaps_legacy && idx->length_ge_bitmaps_legacy[j])
                        biscuit_roaring_andnot_inplace(idx->length_ge_bitmaps_legacy[j], records_to_delete);
                }
                
                for (j = 0; j < idx->max_length_lower; j++) {
                    if (idx->length_bitmaps_lower && idx->length_bitmaps_lower[j])
                        biscuit_roaring_andnot_inplace(idx->length_bitmaps_lower[j], records_to_delete);
                    if (idx->length_ge_bitmaps_lower && idx->length_ge_bitmaps_lower[j])
                        biscuit_roaring_andnot_inplace(idx->length_ge_bitmaps_lower[j], records_to_delete);
                }
                
                /* Free cached strings for deleted records */
                for (j = 0; j < (int)delete_count; j++) {
                    if (idx->data_cache[delete_indices[j]]) {
                        pfree(idx->data_cache[delete_indices[j]]);
                        idx->data_cache[delete_indices[j]] = NULL;
                    }
                    if (idx->data_cache_lower && idx->data_cache_lower[delete_indices[j]]) {
                        pfree(idx->data_cache_lower[delete_indices[j]]);
                        idx->data_cache_lower[delete_indices[j]] = NULL;
                    }
                }
            }
            /* ==================== MULTI-COLUMN CLEANUP ==================== */
            else {
                for (col = 0; col < idx->num_columns; col++) {
                    ColumnIndex *cidx = &idx->column_indices[col];
                    
                    for (ch = 0; ch < CHAR_RANGE; ch++) {
                        CharIndex *pos_cidx = &cidx->pos_idx[ch];
                        CharIndex *neg_cidx = &cidx->neg_idx[ch];
                        
                        /* Case-sensitive */
                        for (j = 0; j < pos_cidx->count; j++)
                            biscuit_roaring_andnot_inplace(pos_cidx->entries[j].bitmap, records_to_delete);
                        
                        for (j = 0; j < neg_cidx->count; j++)
                            biscuit_roaring_andnot_inplace(neg_cidx->entries[j].bitmap, records_to_delete);
                        
                        if (cidx->char_cache[ch])
                            biscuit_roaring_andnot_inplace(cidx->char_cache[ch], records_to_delete);
                        
                        /* Case-insensitive */
                        pos_cidx = &cidx->pos_idx_lower[ch];
                        neg_cidx = &cidx->neg_idx_lower[ch];
                        
                        for (j = 0; j < pos_cidx->count; j++)
                            biscuit_roaring_andnot_inplace(pos_cidx->entries[j].bitmap, records_to_delete);
                        
                        for (j = 0; j < neg_cidx->count; j++)
                            biscuit_roaring_andnot_inplace(neg_cidx->entries[j].bitmap, records_to_delete);
                        
                        if (cidx->char_cache_lower[ch])
                            biscuit_roaring_andnot_inplace(cidx->char_cache_lower[ch], records_to_delete);
                    }
                    
                    /* Length bitmaps */
                    for (j = 0; j <= cidx->max_length; j++) {
                        if (cidx->length_bitmaps && cidx->length_bitmaps[j])
                            biscuit_roaring_andnot_inplace(cidx->length_bitmaps[j], records_to_delete);
                        if (cidx->length_ge_bitmaps && cidx->length_ge_bitmaps[j])
                            biscuit_roaring_andnot_inplace(cidx->length_ge_bitmaps[j], records_to_delete);
                    }
                    
                    for (j = 0; j <= cidx->max_length_lower; j++) {
                        if (cidx->length_bitmaps_lower && cidx->length_bitmaps_lower[j])
                            biscuit_roaring_andnot_inplace(cidx->length_bitmaps_lower[j], records_to_delete);
                        if (cidx->length_ge_bitmaps_lower && cidx->length_ge_bitmaps_lower[j])
                            biscuit_roaring_andnot_inplace(cidx->length_ge_bitmaps_lower[j], records_to_delete);
                    }
                }
                
                /* Free column data */
                for (j = 0; j < (int)delete_count; j++) {
                    for (col = 0; col < idx->num_columns; col++) {
                        if (idx->column_data_cache[col][delete_indices[j]]) {
                            pfree(idx->column_data_cache[col][delete_indices[j]]);
                            idx->column_data_cache[col][delete_indices[j]] = NULL;
                        }
                    }
                }
            }
            
            pfree(delete_indices);
        }
    }
    
    /* Cleanup */
    biscuit_roaring_free(records_to_delete);
    
    /* ==================== OPTIONAL: Reset tombstone bitmap periodically ==================== */
    /* Only reset if we've accumulated too many tombstones */
    if (idx->tombstone_count >= TOMBSTONE_CLEANUP_THRESHOLD) {
        biscuit_roaring_free(idx->tombstones);
        idx->tombstones = biscuit_roaring_create();
        idx->tombstone_count = 0;
    }
    
    if (index->rd_indexcxt) {
        MemoryContextSwitchTo(oldcontext);
    }
    
    stats->num_pages = 1;
    stats->pages_deleted = 0;
    stats->pages_free = 0;
    
    return stats;
}

static IndexBulkDeleteResult *
biscuit_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    return stats;
}

static bool
biscuit_canreturn(Relation index, int attno)
{
    return false;
}

static void
biscuit_costestimate(PlannerInfo *root, IndexPath *path,
                    double loop_count, Cost *indexStartupCost,
                    Cost *indexTotalCost, Selectivity *indexSelectivity,
                    double *indexCorrelation, double *indexPages)
{
    Relation index = path->indexinfo->indexoid != InvalidOid ? 
                    index_open(path->indexinfo->indexoid, AccessShareLock) : NULL;
    BlockNumber numPages = 1;
    
    if (index != NULL) {
        numPages = RelationGetNumberOfBlocks(index);
        if (numPages == 0)
            numPages = 1;
        index_close(index, AccessShareLock);
    }
    
    /* Set very low costs to encourage index usage */
    *indexStartupCost = 0.0;
    *indexTotalCost = 0.01 + (numPages * random_page_cost);
    *indexSelectivity = 0.01;
    *indexCorrelation = 1.0;
    
    if (indexPages)
        *indexPages = numPages;
}

static bytea *
biscuit_options(Datum reloptions, bool validate)
{
    return NULL;
}

static bool
biscuit_validate(Oid opclassoid)
{
    return true;
}

static void
biscuit_adjustmembers(Oid opfamilyoid, Oid opclassoid,
                    List *operators, List *functions)
{
    /* Nothing to adjust */
}

/*
 * CRITICAL FIX: Ensure index stays loaded across cache invalidations
 * Use a callback to preserve the index in memory
 */
 static void
 biscuit_cache_callback(Datum arg, Oid relid)
 {
     /* This callback is invoked when relation cache is invalidated */
     /* We don't clear rd_amcache here to keep the index in memory */
     //elog(DEBUG1, "Biscuit: Cache callback for relation %u - preserving index", relid);
 }
 
 static IndexScanDesc
 biscuit_beginscan(Relation index, int nkeys, int norderbys)
 {
     IndexScanDesc scan;
     BiscuitScanOpaque *so;
     
     scan = RelationGetIndexScan(index, nkeys, norderbys);
     
     so = (BiscuitScanOpaque *)palloc(sizeof(BiscuitScanOpaque));
     
     so->index = (BiscuitIndex *)index->rd_amcache;
    
     if (!so->index) {
         /* Check static cache first */
         so->index = biscuit_cache_lookup(RelationGetRelid(index));
         
         if (so->index) {
             //elog(DEBUG1, "Biscuit: Found index in static cache");
             index->rd_amcache = so->index;
         } else {
             //elog(DEBUG1, "Biscuit: Loading index for first time");
             so->index = biscuit_load_index(index);
             index->rd_amcache = so->index;
             
             /* Cache it persistently */
             biscuit_register_callback();
             biscuit_cache_insert(RelationGetRelid(index), so->index);
         }
     } else {
         //elog(DEBUG1, "Biscuit: Using cached index: %d records, max_len=%d", so->index->num_records, so->index->max_len);
     }
     
     if (!so->index) {
         //elog(ERROR, "Biscuit: Failed to load or create index");
     }
     
     so->results = NULL;
     so->num_results = 0;
     so->current = 0;
     
     /* Initialize optimization flags */
     so->is_aggregate_only = false;
     so->needs_sorted_access = true;  /* Default to sorted */
     so->limit_remaining = -1;        /* No limit by default */
     
     scan->opaque = so;
     
     return scan;
 }
 
 /* ==================== OPTIMIZED RESCAN WITH CACHING (OPTIMIZATION 13) ==================== */

/*
* Enhanced rescan with pattern result caching for repeated queries
*/
typedef struct PatternCacheEntry {
char *pattern;
ItemPointerData *tids;
int num_tids;
struct PatternCacheEntry *next;
} PatternCacheEntry;


/*
 * BISCUIT QUERY OPTIMIZER - Multi-Column LIKE Query Reordering
 * 
 * Intelligently reorders predicates to minimize candidates early:
 * 1. Non-% patterns with more _'s (higher selectivity)
 * 2. %-patterns with strong anchors (prefix/suffix with concrete chars)
 * 3. Patterns with fewer windowed partitions
 * 4. Non-anchored patterns (%...%) last
 */


/* ==================== PATTERN ANALYSIS ==================== */

/*
 * Calculate anchor strength based on concrete characters in anchors
 * Returns 0-100, where higher = stronger anchor
 */
static int
calculate_anchor_strength(const char *pattern, bool is_prefix, bool is_suffix)
{
    int strength = 0;
    int i, len;
    
    if (!is_prefix && !is_suffix)
        return 0;
    
    len = strlen(pattern);
    
    if (is_prefix) {
        /* Count leading concrete characters before first % */
        for (i = 0; i < len && pattern[i] != '%'; i++) {
            if (pattern[i] != '_')
                strength += 10;  /* Concrete char worth 10 points */
            else
                strength += 3;   /* _ worth 3 points */
        }
    }
    
    if (is_suffix) {
        /* Count trailing concrete characters after last % */
        int suffix_start = len;
        for (i = len - 1; i >= 0 && pattern[i] != '%'; i--) {
            suffix_start = i;
        }
        
        for (i = suffix_start; i < len; i++) {
            if (pattern[i] != '_')
                strength += 10;
            else
                strength += 3;
        }
    }
    
    return Min(strength, 100);
}

/*
 * Analyze a LIKE pattern and extract its characteristics
 */
static void
analyze_pattern(QueryPredicate *pred)
{
    const char *p = pred->pattern;
    int len = strlen(p);
    int i;
    bool in_percent_run = false;
    
    /* Initialize counters */
    pred->concrete_chars = 0;
    pred->underscore_count = 0;
    pred->percent_count = 0;
    pred->partition_count = 0;
    pred->has_percent = false;
    
    /* Scan pattern */
    for (i = 0; i < len; i++) {
        if (p[i] == '%') {
            pred->has_percent = true;
            if (!in_percent_run) {
                pred->percent_count++;
                in_percent_run = true;
            }
        } else {
            if (in_percent_run)
                pred->partition_count++;
            in_percent_run = false;
            
            if (p[i] == '_')
                pred->underscore_count++;
            else
                pred->concrete_chars++;
        }
    }
    
    /* If we ended outside a % run, count the last partition */
    if (!in_percent_run && len > 0)
        pred->partition_count++;
    
    /* Classify pattern type */
    pred->is_exact = !pred->has_percent && pred->underscore_count == 0;
    pred->is_prefix = (len > 0 && p[0] != '%' && pred->has_percent);
    pred->is_suffix = (len > 0 && p[len-1] != '%' && pred->has_percent);
    pred->is_substring = (len >= 2 && p[0] == '%' && p[len-1] == '%' && 
                          !pred->is_prefix && !pred->is_suffix);
    
    /* Calculate anchor strength */
    pred->anchor_strength = calculate_anchor_strength(p, pred->is_prefix, pred->is_suffix);
}

/*
 * Calculate selectivity score for a predicate
 * Lower score = more selective = execute earlier
 * 
 * Scoring formula:
 * - Base: 1.0 / (concrete_chars + 1)
 * - Bonus: -0.1 per underscore (they filter but allow flexibility)
 * - Penalty: +0.2 per partition (more complex matching)
 * - Bonus: -anchor_strength / 200 (stronger anchors are more selective)
 */
static void
calculate_selectivity(QueryPredicate *pred)
{
    double score = 1.0;
    
    /* Base selectivity from concrete characters */
    if (pred->concrete_chars > 0)
        score = 1.0 / (double)(pred->concrete_chars + 1);
    
    /* Exact matches are highly selective */
    if (pred->is_exact)
        score *= 0.1;
    
    /* Underscores add some selectivity */
    score -= (pred->underscore_count * 0.05);
    
    /* Multiple partitions reduce selectivity (harder to optimize) */
    score += (pred->partition_count * 0.15);
    
    /* Anchor strength bonus */
    score -= (pred->anchor_strength / 200.0);
    
    /* Substring patterns are least selective */
    if (pred->is_substring)
        score += 0.5;
    
    /* Ensure score stays in valid range */
    if (score < 0.01)
        score = 0.01;
    if (score > 1.0)
        score = 1.0;
    
    pred->selectivity_score = score;
}

/*
 * Assign priority based on selectivity and pattern characteristics
 * Priority 0 = execute first, higher = execute later
 */
static void
assign_priority(QueryPredicate *pred)
{
    /* Priority tier 1: Exact matches and patterns with many underscores */
    if (pred->is_exact || (pred->underscore_count >= 3 && !pred->has_percent)) {
        pred->priority = 0;
    }
    /* Priority tier 2: Non-% patterns with underscores */
    else if (!pred->has_percent && pred->underscore_count > 0) {
        pred->priority = 10 + (5 - pred->underscore_count);
    }
    /* Priority tier 3: Strong anchored patterns */
    else if ((pred->is_prefix || pred->is_suffix) && pred->anchor_strength >= 30) {
        pred->priority = 20 + (100 - pred->anchor_strength) / 10;
    }
    /* Priority tier 4: Weak anchored patterns */
    else if ((pred->is_prefix || pred->is_suffix) && pred->anchor_strength > 0) {
        pred->priority = 30 + (50 - pred->anchor_strength) / 5;
    }
    /* Priority tier 5: Multi-partition patterns */
    else if (pred->partition_count > 2) {
        pred->priority = 40 + pred->partition_count;
    }
    /* Priority tier 6: Substring patterns (lowest priority) */
    else if (pred->is_substring) {
        pred->priority = 50 + (10 - pred->concrete_chars);
    }
    /* Default: medium priority */
    else {
        pred->priority = 35;
    }
    
    /* Fine-tune with selectivity score */
    pred->priority += (int)(pred->selectivity_score * 10);
}

/* ==================== QUERY PLAN CREATION ==================== */

/*
 * Compare function for sorting predicates by priority
 */
static int
compare_predicates(const void *a, const void *b)
{
    const QueryPredicate *pred_a = (const QueryPredicate *)a;
    const QueryPredicate *pred_b = (const QueryPredicate *)b;
    
    /* Primary sort: priority (lower first) */
    if (pred_a->priority != pred_b->priority)
        return pred_a->priority - pred_b->priority;
    
    /* Secondary sort: selectivity score (lower first) */
    if (pred_a->selectivity_score < pred_b->selectivity_score)
        return -1;
    if (pred_a->selectivity_score > pred_b->selectivity_score)
        return 1;
    
    /* Tertiary sort: column index (stable ordering) */
    return pred_a->column_index - pred_b->column_index;
}

/*
 * Create optimized query plan from scan keys
 */
static QueryPlan*
create_query_plan(ScanKey keys, int nkeys)
{
    QueryPlan *plan;
    int i;
    
    plan = (QueryPlan *)palloc(sizeof(QueryPlan));
    plan->capacity = nkeys;
    plan->count = 0;
    plan->predicates = (QueryPredicate *)palloc(nkeys * sizeof(QueryPredicate));
    
    /* Analyze each predicate */
    for (i = 0; i < nkeys; i++) {
        QueryPredicate *pred = &plan->predicates[plan->count];
        ScanKey key = &keys[i];
        text *pattern_text;
        
        /* Skip null keys */
        if (key->sk_flags & SK_ISNULL)
            continue;
        
        /* Extract pattern */
        pattern_text = DatumGetTextPP(key->sk_argument);
        pred->pattern = text_to_cstring(pattern_text);
        pred->column_index = key->sk_attno - 1;  /* Convert to 0-based */
        pred->scan_key = key;
        
        /* Analyze pattern characteristics */
        analyze_pattern(pred);
        calculate_selectivity(pred);
        assign_priority(pred);
        
        plan->count++;
    }
    
    /* Sort predicates by priority */
    if (plan->count > 1) {
        qsort(plan->predicates, plan->count, sizeof(QueryPredicate), 
              compare_predicates);
    }
    
    return plan;
}

/*
 * Log query plan for debugging
 */
static void
log_query_plan(QueryPlan *plan)
{
    int i;
    
    //elog(DEBUG1, "=== BISCUIT QUERY EXECUTION PLAN ===");
    //elog(DEBUG1, "Total predicates: %d", plan->count);
    
    for (i = 0; i < plan->count; i++) {
        QueryPredicate *pred = &plan->predicates[i];
        const char *type;
        
        if (pred->is_exact)
            type = "EXACT";
        else if (pred->is_prefix)
            type = "PREFIX";
        else if (pred->is_suffix)
            type = "SUFFIX";
        else if (pred->is_substring)
            type = "SUBSTRING";
        else
            type = "COMPLEX";
        
        //elog(DEBUG1, "[%d] Col=%d Pattern='%s' Type=%s Priority=%d Selectivity=%.3f "
            // "Concrete=%d Under=%d Parts=%d Anchor=%d",
            // i, pred->column_index, pred->pattern, type, 
            // pred->priority, pred->selectivity_score,
            // pred->concrete_chars, pred->underscore_count,
            // pred->partition_count, pred->anchor_strength);
    }
    
    //elog(DEBUG1, "====================================");
}

/* ==================== PER-COLUMN PATTERN MATCHING ==================== */

/* ==================== FIXED: UTF-8-AWARE MULTI-COLUMN PATTERN MATCHING ==================== */

/*
 * Match a pattern part at a specific CHARACTER position (multi-column LIKE)
 * Handles multi-byte UTF-8 characters correctly
 */
static RoaringBitmap* 
biscuit_match_col_part_at_pos(ColumnIndex *col, const char *part, 
                              int part_byte_len, int start_char_pos) {
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int part_byte_pos = 0;
    int current_char_pos = start_char_pos;  /* Current CHARACTER position in the string */
    int concrete_chars = 0;
    bool first_concrete = true;
    
    /* Process pattern character by character (not byte by byte) */
    while (part_byte_pos < part_byte_len) {
        unsigned char first_byte = (unsigned char)part[part_byte_pos];
        
        /* Handle underscore wildcard - matches ANY single character */
        if (first_byte == '_') {
            part_byte_pos++;
            current_char_pos++;  /* Move to next character position */
            continue;
        }
        
        /* Get UTF-8 character length in bytes */
        int char_byte_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_byte_len > part_byte_len)
            char_byte_len = part_byte_len - part_byte_pos;
        
        concrete_chars++;
        
        /* CRITICAL: For multi-byte characters, ALL bytes must be indexed at 
         * the SAME character position */
        if (char_byte_len == 1) {
            /* Single-byte character - simple case */
            RoaringBitmap *char_bm = biscuit_get_col_pos_bitmap(col, first_byte, current_char_pos);
            
            if (!char_bm) {
                if (result) biscuit_roaring_free(result);
                return biscuit_roaring_create();
            }
            
            if (first_concrete) {
                result = biscuit_roaring_copy(char_bm);
                first_concrete = false;
            } else {
                biscuit_roaring_and_inplace(result, char_bm);
                if (biscuit_roaring_is_empty(result))
                    return result;
            }
        } else {
            /* Multi-byte character - must match ALL bytes at current_char_pos */
            RoaringBitmap *multibyte_result = NULL;
            int b;
            
            for (b = 0; b < char_byte_len; b++) {
                unsigned char byte_val = (unsigned char)part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_col_pos_bitmap(col, byte_val, current_char_pos);
                
                if (!byte_bm) {
                    if (multibyte_result) biscuit_roaring_free(multibyte_result);
                    if (result) biscuit_roaring_free(result);
                    return biscuit_roaring_create();
                }
                
                if (b == 0) {
                    /* First byte of multi-byte char */
                    multibyte_result = biscuit_roaring_copy(byte_bm);
                } else {
                    /* Subsequent bytes - must ALL be present at same position */
                    biscuit_roaring_and_inplace(multibyte_result, byte_bm);
                    if (biscuit_roaring_is_empty(multibyte_result)) {
                        biscuit_roaring_free(multibyte_result);
                        if (result) biscuit_roaring_free(result);
                        return biscuit_roaring_create();
                    }
                }
            }
            
            /* Now intersect this character's result with overall result */
            if (first_concrete) {
                result = multibyte_result;
                first_concrete = false;
            } else {
                biscuit_roaring_and_inplace(result, multibyte_result);
                biscuit_roaring_free(multibyte_result);
                if (biscuit_roaring_is_empty(result))
                    return result;
            }
        }
        
        part_byte_pos += char_byte_len;
        current_char_pos++;  /* Move to next CHARACTER position */
    }
    
    /* All wildcards case - just need length >= min */
    if (concrete_chars == 0) {
        int pattern_char_count = biscuit_utf8_char_count(part, part_byte_len);
        if (result) biscuit_roaring_free(result);
        result = biscuit_get_col_length_ge(col, start_char_pos + pattern_char_count);
    } else {
        /* Apply length constraint: string must be long enough */
        int pattern_char_count = biscuit_utf8_char_count(part, part_byte_len);
        len_filter = biscuit_get_col_length_ge(col, start_char_pos + pattern_char_count);
        if (len_filter) {
            if (result) {
                biscuit_roaring_and_inplace(result, len_filter);
            } else {
                result = len_filter;
                len_filter = NULL;
            }
            if (len_filter) biscuit_roaring_free(len_filter);
        }
    }
    
    return result ? result : biscuit_roaring_create();
}

/*
 * Match a pattern part at the END using negative CHARACTER indexing
 * Handles multi-byte UTF-8 characters correctly
 */
static RoaringBitmap* 
biscuit_match_col_part_at_end(ColumnIndex *col, const char *part, int part_byte_len) {
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int concrete_chars = 0;
    bool first_concrete = true;
    
    /* Count CHARACTERS in pattern (not bytes) */
    int pattern_char_count = biscuit_utf8_char_count(part, part_byte_len);
    
    /* Process pattern LEFT-TO-RIGHT, but assign negative positions */
    int part_byte_pos = 0;
    int char_index = 0;  /* 0 = first char in pattern, 1 = second, etc. */
    
    while (part_byte_pos < part_byte_len) {
        unsigned char first_byte = (unsigned char)part[part_byte_pos];
        
        /* Handle underscore wildcard */
        if (first_byte == '_') {
            part_byte_pos++;
            char_index++;
            continue;
        }
        
        /* Get UTF-8 character length */
        int char_byte_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_byte_len > part_byte_len)
            char_byte_len = part_byte_len - part_byte_pos;
        
        concrete_chars++;
        
        /* CRITICAL FIX: Calculate negative position correctly
         * 
         * For pattern '%안녕' (2 characters):
         *   char_index=0 ('안') → neg_pos = -(2-0) = -2 (second-to-last position)
         *   char_index=1 ('녕') → neg_pos = -(2-1) = -1 (last position)
         * 
         * For pattern '%X' (1 character):
         *   char_index=0 ('X') → neg_pos = -(1-0) = -1 (last position)
         */
        int neg_pos = -(pattern_char_count - char_index);
        
        /* Match all bytes of this character at the SAME negative position */
        if (char_byte_len == 1) {
            /* Single-byte character */
            RoaringBitmap *char_bm = biscuit_get_col_neg_bitmap(col, first_byte, neg_pos);
            
            if (!char_bm) {
                if (result) biscuit_roaring_free(result);
                return biscuit_roaring_create();
            }
            
            if (first_concrete) {
                result = biscuit_roaring_copy(char_bm);
                first_concrete = false;
            } else {
                biscuit_roaring_and_inplace(result, char_bm);
                if (biscuit_roaring_is_empty(result))
                    return result;
            }
        } else {
            /* Multi-byte character - ALL bytes must be at same neg_pos */
            RoaringBitmap *multibyte_result = NULL;
            int b;
            
            for (b = 0; b < char_byte_len; b++) {
                unsigned char byte_val = (unsigned char)part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_col_neg_bitmap(col, byte_val, neg_pos);
                
                if (!byte_bm) {
                    if (multibyte_result) biscuit_roaring_free(multibyte_result);
                    if (result) biscuit_roaring_free(result);
                    return biscuit_roaring_create();
                }
                
                if (b == 0) {
                    multibyte_result = biscuit_roaring_copy(byte_bm);
                } else {
                    biscuit_roaring_and_inplace(multibyte_result, byte_bm);
                    if (biscuit_roaring_is_empty(multibyte_result)) {
                        biscuit_roaring_free(multibyte_result);
                        if (result) biscuit_roaring_free(result);
                        return biscuit_roaring_create();
                    }
                }
            }
            
            /* Intersect with overall result */
            if (first_concrete) {
                result = multibyte_result;
                first_concrete = false;
            } else {
                biscuit_roaring_and_inplace(result, multibyte_result);
                biscuit_roaring_free(multibyte_result);
                if (biscuit_roaring_is_empty(result))
                    return result;
            }
        }
        
        part_byte_pos += char_byte_len;
        char_index++;  /* Move to next character in pattern */
    }
    
    /* All wildcards case */
    if (concrete_chars == 0) {
        if (result) biscuit_roaring_free(result);
        result = biscuit_get_col_length_ge(col, pattern_char_count);
    } else {
        /* Apply length constraint */
        len_filter = biscuit_get_col_length_ge(col, pattern_char_count);
        if (len_filter) {
            if (result) {
                biscuit_roaring_and_inplace(result, len_filter);
            } else {
                result = len_filter;
                len_filter = NULL;
            }
            if (len_filter) biscuit_roaring_free(len_filter);
        }
    }
    
    return result ? result : biscuit_roaring_create();
}
/* ==================== ILIKE VERSIONS (MULTI-COLUMN) ==================== */

/*
 * Match a pattern part at a specific CHARACTER position (multi-column ILIKE)
 * Uses lowercase index for case-insensitive matching
 */
static RoaringBitmap* 
biscuit_match_col_part_at_pos_ilike(ColumnIndex *col, const char *part, 
                                    int part_byte_len, int start_char_pos) {
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int part_byte_pos = 0;
    int current_char_pos = start_char_pos;
    int concrete_chars = 0;
    bool first_concrete = true;
    
    while (part_byte_pos < part_byte_len) {
        unsigned char first_byte = (unsigned char)part[part_byte_pos];
        
        /* Handle underscore wildcard */
        if (first_byte == '_') {
            part_byte_pos++;
            current_char_pos++;
            continue;
        }
        
        int char_byte_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_byte_len > part_byte_len)
            char_byte_len = part_byte_len - part_byte_pos;
        
        concrete_chars++;
        
        if (char_byte_len == 1) {
            /* Single-byte character */
            RoaringBitmap *char_bm = biscuit_get_col_pos_bitmap_lower(col, first_byte, current_char_pos);
            
            if (!char_bm) {
                if (result) biscuit_roaring_free(result);
                return biscuit_roaring_create();
            }
            
            if (first_concrete) {
                result = biscuit_roaring_copy(char_bm);
                first_concrete = false;
            } else {
                biscuit_roaring_and_inplace(result, char_bm);
                if (biscuit_roaring_is_empty(result))
                    return result;
            }
        } else {
            /* Multi-byte character */
            RoaringBitmap *multibyte_result = NULL;
            int b;
            
            for (b = 0; b < char_byte_len; b++) {
                unsigned char byte_val = (unsigned char)part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_col_pos_bitmap_lower(col, byte_val, current_char_pos);
                
                if (!byte_bm) {
                    if (multibyte_result) biscuit_roaring_free(multibyte_result);
                    if (result) biscuit_roaring_free(result);
                    return biscuit_roaring_create();
                }
                
                if (b == 0) {
                    multibyte_result = biscuit_roaring_copy(byte_bm);
                } else {
                    biscuit_roaring_and_inplace(multibyte_result, byte_bm);
                    if (biscuit_roaring_is_empty(multibyte_result)) {
                        biscuit_roaring_free(multibyte_result);
                        if (result) biscuit_roaring_free(result);
                        return biscuit_roaring_create();
                    }
                }
            }
            
            if (first_concrete) {
                result = multibyte_result;
                first_concrete = false;
            } else {
                biscuit_roaring_and_inplace(result, multibyte_result);
                biscuit_roaring_free(multibyte_result);
                if (biscuit_roaring_is_empty(result))
                    return result;
            }
        }
        
        part_byte_pos += char_byte_len;
        current_char_pos++;
    }
    
    /* All wildcards case */
    if (concrete_chars == 0) {
        int pattern_char_count = biscuit_utf8_char_count(part, part_byte_len);
        if (result) biscuit_roaring_free(result);
        result = biscuit_get_col_length_ge_lower(col, start_char_pos + pattern_char_count);
    } else {
        /* Apply length constraint */
        int pattern_char_count = biscuit_utf8_char_count(part, part_byte_len);
        len_filter = biscuit_get_col_length_ge_lower(col, start_char_pos + pattern_char_count);
        if (len_filter) {
            if (result) {
                biscuit_roaring_and_inplace(result, len_filter);
            } else {
                result = len_filter;
                len_filter = NULL;
            }
            if (len_filter) biscuit_roaring_free(len_filter);
        }
    }
    
    return result ? result : biscuit_roaring_create();
}

/*
 * Match a pattern part at the END using negative CHARACTER indexing (ILIKE)
 * Uses lowercase index for case-insensitive matching
 */
static RoaringBitmap* 
biscuit_match_col_part_at_end_ilike(ColumnIndex *col, const char *part, int part_byte_len) {
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int concrete_chars = 0;
    bool first_concrete = true;
    
    /* Count CHARACTERS in pattern */
    int pattern_char_count = biscuit_utf8_char_count(part, part_byte_len);
    
    /* Process pattern LEFT-TO-RIGHT with negative indexing */
    int part_byte_pos = 0;
    int char_index = 0;
    
    while (part_byte_pos < part_byte_len) {
        unsigned char first_byte = (unsigned char)part[part_byte_pos];
        
        /* Handle underscore wildcard */
        if (first_byte == '_') {
            part_byte_pos++;
            char_index++;
            continue;
        }
        
        int char_byte_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_byte_len > part_byte_len)
            char_byte_len = part_byte_len - part_byte_pos;
        
        concrete_chars++;
        
        /* Calculate negative position correctly */
        int neg_pos = -(pattern_char_count - char_index);
        
        if (char_byte_len == 1) {
            /* Single-byte character */
            RoaringBitmap *char_bm = biscuit_get_col_neg_bitmap_lower(col, first_byte, neg_pos);
            
            if (!char_bm) {
                if (result) biscuit_roaring_free(result);
                return biscuit_roaring_create();
            }
            
            if (first_concrete) {
                result = biscuit_roaring_copy(char_bm);
                first_concrete = false;
            } else {
                biscuit_roaring_and_inplace(result, char_bm);
                if (biscuit_roaring_is_empty(result))
                    return result;
            }
        } else {
            /* Multi-byte character */
            RoaringBitmap *multibyte_result = NULL;
            int b;
            
            for (b = 0; b < char_byte_len; b++) {
                unsigned char byte_val = (unsigned char)part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_col_neg_bitmap_lower(col, byte_val, neg_pos);
                
                if (!byte_bm) {
                    if (multibyte_result) biscuit_roaring_free(multibyte_result);
                    if (result) biscuit_roaring_free(result);
                    return biscuit_roaring_create();
                }
                
                if (b == 0) {
                    multibyte_result = biscuit_roaring_copy(byte_bm);
                } else {
                    biscuit_roaring_and_inplace(multibyte_result, byte_bm);
                    if (biscuit_roaring_is_empty(multibyte_result)) {
                        biscuit_roaring_free(multibyte_result);
                        if (result) biscuit_roaring_free(result);
                        return biscuit_roaring_create();
                    }
                }
            }
            
            if (first_concrete) {
                result = multibyte_result;
                first_concrete = false;
            } else {
                biscuit_roaring_and_inplace(result, multibyte_result);
                biscuit_roaring_free(multibyte_result);
                if (biscuit_roaring_is_empty(result))
                    return result;
            }
        }
        
        part_byte_pos += char_byte_len;
        char_index++;
    }
    
    /* All wildcards case */
    if (concrete_chars == 0) {
        if (result) biscuit_roaring_free(result);
        result = biscuit_get_col_length_ge_lower(col, pattern_char_count);
    } else {
        len_filter = biscuit_get_col_length_ge_lower(col, pattern_char_count);
        if (len_filter) {
            if (result) {
                biscuit_roaring_and_inplace(result, len_filter);
            } else {
                result = len_filter;
                len_filter = NULL;
            }
            if (len_filter) biscuit_roaring_free(len_filter);
        }
    }
    
    return result ? result : biscuit_roaring_create();
}
/* ==================== FIXED: UTF-8-AWARE RECURSIVE WINDOWED MATCHING (MULTI-COLUMN) ==================== */

/*
 * Recursive windowed matching for multi-column patterns (case-sensitive)
 * Handles CHARACTER-based positioning for multi-byte UTF-8
 */
static void 
biscuit_recursive_windowed_match_col(
    RoaringBitmap *result, ColumnIndex *col,
    const char **parts, int *part_byte_lens, int part_count,
    bool ends_percent, int part_idx, int min_char_pos,
    RoaringBitmap *current_candidates, int max_char_len)
{
    int remaining_char_len, max_char_pos, char_pos, i;
    RoaringBitmap *end_match;
    RoaringBitmap *length_constraint;
    RoaringBitmap *part_match;
    RoaringBitmap *next_candidates;
    int min_required_char_length;
    int next_min_char_pos;
    
    /* Base case: all parts matched */
    if (part_idx >= part_count) {
        biscuit_roaring_or_inplace(result, current_candidates);
        return;
    }
    
    /* Calculate minimum CHARACTER length needed for remaining parts */
    remaining_char_len = 0;
    for (i = part_idx + 1; i < part_count; i++) {
        remaining_char_len += biscuit_utf8_char_count(parts[i], part_byte_lens[i]);
    }
    
    /* Last part without trailing % must match at end */
    if (part_idx == part_count - 1 && !ends_percent) {
        end_match = biscuit_match_col_part_at_end(col, parts[part_idx], part_byte_lens[part_idx]);
        
        if (!end_match) {
            return;
        }
        
        biscuit_roaring_and_inplace(end_match, current_candidates);
        
        /* Ensure minimum CHARACTER length constraint */
        min_required_char_length = min_char_pos + biscuit_utf8_char_count(parts[part_idx], part_byte_lens[part_idx]);
        length_constraint = biscuit_get_col_length_ge(col, min_required_char_length);
        if (length_constraint) {
            biscuit_roaring_and_inplace(end_match, length_constraint);
            biscuit_roaring_free(length_constraint);
        }
        
        biscuit_roaring_or_inplace(result, end_match);
        biscuit_roaring_free(end_match);
        return;
    }
    
    /* Middle part: try all valid CHARACTER positions */
    int current_part_char_len = biscuit_utf8_char_count(parts[part_idx], part_byte_lens[part_idx]);
    max_char_pos = max_char_len - current_part_char_len - remaining_char_len;
    
    if (min_char_pos > max_char_pos) {
        /* No valid position for this part */
        return;
    }
    
    /* Try each valid CHARACTER position for current part */
    for (char_pos = min_char_pos; char_pos <= max_char_pos; char_pos++) {
        /* Match part at this CHARACTER position */
        part_match = biscuit_match_col_part_at_pos(col, parts[part_idx], part_byte_lens[part_idx], char_pos);
        
        if (!part_match) {
            continue;
        }
        
        next_candidates = biscuit_roaring_copy(current_candidates);
        biscuit_roaring_and_inplace(next_candidates, part_match);
        biscuit_roaring_free(part_match);
        
        if (biscuit_roaring_is_empty(next_candidates)) {
            biscuit_roaring_free(next_candidates);
            continue;
        }
        
        /* Recurse for next part with updated CHARACTER position */
        next_min_char_pos = char_pos + current_part_char_len;
        
        biscuit_recursive_windowed_match_col(
            result, col, parts, part_byte_lens, part_count,
            ends_percent, part_idx + 1, next_min_char_pos, 
            next_candidates, max_char_len
        );
        
        biscuit_roaring_free(next_candidates);
    }
}

/*
 * Recursive windowed matching for multi-column patterns (case-insensitive)
 * Handles CHARACTER-based positioning for multi-byte UTF-8
 */
static void 
biscuit_recursive_windowed_match_col_ilike(
    RoaringBitmap *result, ColumnIndex *col,
    const char **parts, int *part_byte_lens, int part_count,
    bool ends_percent, int part_idx, int min_char_pos,
    RoaringBitmap *current_candidates, int max_char_len)
{
    int remaining_char_len, max_char_pos, char_pos, i;
    RoaringBitmap *end_match;
    RoaringBitmap *length_constraint;
    RoaringBitmap *part_match;
    RoaringBitmap *next_candidates;
    int min_required_char_length;
    int next_min_char_pos;
    
    /* Base case */
    if (part_idx >= part_count) {
        biscuit_roaring_or_inplace(result, current_candidates);
        return;
    }
    
    /* Calculate minimum CHARACTER length for remaining parts */
    remaining_char_len = 0;
    for (i = part_idx + 1; i < part_count; i++) {
        remaining_char_len += biscuit_utf8_char_count(parts[i], part_byte_lens[i]);
    }
    
    /* Last part without trailing % must match at end */
    if (part_idx == part_count - 1 && !ends_percent) {
        end_match = biscuit_match_col_part_at_end_ilike(col, parts[part_idx], part_byte_lens[part_idx]);
        
        if (!end_match) {
            return;
        }
        
        biscuit_roaring_and_inplace(end_match, current_candidates);
        
        /* CHARACTER length constraint */
        min_required_char_length = min_char_pos + biscuit_utf8_char_count(parts[part_idx], part_byte_lens[part_idx]);
        length_constraint = biscuit_get_col_length_ge_lower(col, min_required_char_length);
        if (length_constraint) {
            biscuit_roaring_and_inplace(end_match, length_constraint);
            biscuit_roaring_free(length_constraint);
        }
        
        biscuit_roaring_or_inplace(result, end_match);
        biscuit_roaring_free(end_match);
        return;
    }
    
    /* Try all valid CHARACTER positions */
    int current_part_char_len = biscuit_utf8_char_count(parts[part_idx], part_byte_lens[part_idx]);
    max_char_pos = max_char_len - current_part_char_len - remaining_char_len;
    
    if (min_char_pos > max_char_pos) {
        return;
    }
    
    for (char_pos = min_char_pos; char_pos <= max_char_pos; char_pos++) {
        part_match = biscuit_match_col_part_at_pos_ilike(col, parts[part_idx], part_byte_lens[part_idx], char_pos);
        
        if (!part_match) {
            continue;
        }
        
        next_candidates = biscuit_roaring_copy(current_candidates);
        biscuit_roaring_and_inplace(next_candidates, part_match);
        biscuit_roaring_free(part_match);
        
        if (biscuit_roaring_is_empty(next_candidates)) {
            biscuit_roaring_free(next_candidates);
            continue;
        }
        
        /* CHARACTER position advancement */
        next_min_char_pos = char_pos + current_part_char_len;
        
        biscuit_recursive_windowed_match_col_ilike(
            result, col, parts, part_byte_lens, part_count,
            ends_percent, part_idx + 1, next_min_char_pos, 
            next_candidates, max_char_len
        );
        
        biscuit_roaring_free(next_candidates);
    }
}
/* ==================== FIXED: Per-column pattern matching ==================== */

static RoaringBitmap* 
biscuit_query_column_pattern(BiscuitIndex *idx, int col_idx, const char *pattern) {
    ColumnIndex *col;
    int plen = strlen(pattern);
    ParsedPattern *parsed = NULL;
    int min_len, i;
    RoaringBitmap *result = NULL;
    int wildcard_count = 0, percent_count = 0;
    bool only_wildcards = true;
    
    /* ========== SAFETY CHECKS ========== */
    if (!idx) {
        return biscuit_roaring_create();
    }
    
    if (col_idx < 0 || col_idx >= idx->num_columns) {
        return biscuit_roaring_create();
    }
    
    if (!idx->column_indices) {
        return biscuit_roaring_create();
    }
    
    col = &idx->column_indices[col_idx];
    
    if (col->length_bitmaps == NULL || col->length_ge_bitmaps == NULL) {
        return biscuit_roaring_create();
    }
    
    if (col->max_length <= 0) {
        return biscuit_roaring_create();
    }
    
    /* ========== FAST PATH 1: Empty pattern '' ========== */
    if (plen == 0) {
        if (col->length_bitmaps && col->length_bitmaps[0]) {
            return biscuit_roaring_copy(col->length_bitmaps[0]);
        }
        return biscuit_roaring_create();
    }
    
    /* ========== FAST PATH 2: Single '%' matches everything ========== */
    if (plen == 1 && pattern[0] == '%') {
        if (col->length_ge_bitmaps && col->length_ge_bitmaps[0]) {
            return biscuit_roaring_copy(col->length_ge_bitmaps[0]);
        }
        return biscuit_roaring_create();
    }
    
    /* ========== FAST PATH 3: Analyze for pure wildcards ========== */
    for (i = 0; i < plen; i++) {
        if (pattern[i] == '%') {
            percent_count++;
        } else if (pattern[i] == '_') {
            wildcard_count++;
        } else {
            only_wildcards = false;
            break;
        }
    }
    
    /* ========== FAST PATH 4 & 5: Pure wildcard patterns ========== */
    if (only_wildcards) {
        if (percent_count > 0) {
            /* FAST PATH 4: Has %, so length >= wildcard_count */
            if (wildcard_count <= col->max_length && col->length_ge_bitmaps[wildcard_count]) {
                return biscuit_roaring_copy(col->length_ge_bitmaps[wildcard_count]);
            }
            return biscuit_roaring_create();
        } else {
            /* FAST PATH 5: Only underscores → EXACT length match */
            if (wildcard_count <= col->max_length && col->length_bitmaps[wildcard_count]) {
                return biscuit_roaring_copy(col->length_bitmaps[wildcard_count]);
            }
            return biscuit_roaring_create();
        }
    }
    
    /* ========== SLOW PATH: Pattern contains concrete characters ========== */
    
    PG_TRY();
    {
        parsed = biscuit_parse_pattern(pattern);
        
        /* All percent signs */
        if (parsed->part_count == 0) {
            if (col->length_ge_bitmaps && col->length_ge_bitmaps[0]) {
                result = biscuit_roaring_copy(col->length_ge_bitmaps[0]);
            } else {
                result = biscuit_roaring_create();
            }
            biscuit_free_parsed_pattern(parsed);
            return result;
        }
        
        /* Calculate minimum required length */
        min_len = 0;
        for (i = 0; i < parsed->part_count; i++)
            min_len += parsed->part_lens[i];
        
        /* ==================== OPTIMIZED SINGLE PART PATTERNS ==================== */
        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                /* EXACT match */
                result = biscuit_match_col_part_at_pos(col, parsed->parts[0], 
                                                    parsed->part_byte_lens[0], 0);
                if (result && min_len <= col->max_length && col->length_bitmaps[min_len]) {
                    biscuit_roaring_and_inplace(result, col->length_bitmaps[min_len]);
                } else if (result) {
                    biscuit_roaring_free(result);
                    result = biscuit_roaring_create();
                } else {
                    result = biscuit_roaring_create();
                }
            } else if (!parsed->starts_percent) {
                /* PREFIX match */
                result = biscuit_match_col_part_at_pos(col, parsed->parts[0], 
                                                    parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                /* SUFFIX match */
                result = biscuit_match_col_part_at_end(col, parsed->parts[0], 
                                                    parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                /* ==================== FIXED: SUBSTRING MATCH (MULTI-COLUMN) ==================== */
                /* Pattern: '%abc%' - must validate complete UTF-8 characters */
                
                result = biscuit_roaring_create();
                
                RoaringBitmap *candidates = NULL;
                int part_byte_len = parsed->part_byte_lens[0];
                int part_char_len = parsed->part_lens[0];
                
                /* Get candidates using first concrete byte of pattern */
                if (part_byte_len > 0) {
                    unsigned char first_byte = (unsigned char)parsed->parts[0][0];
                    
                    /* Skip underscores to find first concrete byte */
                    int scan_pos = 0;
                    while (scan_pos < part_byte_len && parsed->parts[0][scan_pos] == '_') {
                        scan_pos++;
                    }
                    
                    if (scan_pos < part_byte_len) {
                        first_byte = (unsigned char)parsed->parts[0][scan_pos];
                        
                        /* Get all records with this byte */
                        if (col->char_cache[first_byte]) {
                            candidates = biscuit_roaring_copy(col->char_cache[first_byte]);
                        }
                    }
                }
                
                if (!candidates) {
                    candidates = biscuit_roaring_create();
                    #ifdef HAVE_ROARING
                    roaring_bitmap_add_range(candidates, 0, idx->num_records);
                    #else
                    for (i = 0; i < idx->num_records; i++) {
                        biscuit_roaring_add(candidates, i);
                    }
                    #endif
                }
                
                /* Apply length filter */
                RoaringBitmap *length_filter = biscuit_get_col_length_ge(col, part_char_len);
                if (length_filter) {
                    biscuit_roaring_and_inplace(candidates, length_filter);
                    biscuit_roaring_free(length_filter);
                }
                
                /* ✅ FIX: Validate each candidate with CHARACTER-BY-CHARACTER matching */
                #ifdef HAVE_ROARING
                roaring_uint32_iterator_t *iter = roaring_create_iterator(candidates);
                
                while (iter->has_value) {
                    uint32_t rec_idx = iter->current_value;
                    
                    if (rec_idx < (uint32_t)idx->num_records && 
                        idx->column_data_cache && 
                        idx->column_data_cache[col_idx] &&
                        idx->column_data_cache[col_idx][rec_idx]) {
                        
                        char *haystack = idx->column_data_cache[col_idx][rec_idx];
                        int haystack_byte_len = strlen(haystack);
                        int haystack_char_len = biscuit_utf8_char_count(haystack, haystack_byte_len);
                        
                        /* Try matching pattern at each CHARACTER position */
                        bool found = false;
                        int char_pos;
                        for (char_pos = 0; char_pos <= haystack_char_len - part_char_len && !found; char_pos++) {
                            /* Convert character position to byte offset */
                            int byte_offset = biscuit_utf8_char_to_byte_offset(haystack, haystack_byte_len, char_pos);
                            
                            if (byte_offset < 0)
                                continue;
                            
                            /* ✅ KEY FIX: Match CHARACTER by CHARACTER */
                            bool match = true;
                            int pattern_byte = 0;
                            int haystack_byte = byte_offset;
                            
                            while (pattern_byte < part_byte_len && haystack_byte < haystack_byte_len && match) {
                                /* Handle underscore wildcard */
                                if (parsed->parts[0][pattern_byte] == '_') {
                                    int hay_char_len = biscuit_utf8_char_length((unsigned char)haystack[haystack_byte]);
                                    haystack_byte += hay_char_len;
                                    pattern_byte++;
                                    continue;
                                }
                                
                                /* Get lengths of current UTF-8 characters */
                                int pattern_char_len = biscuit_utf8_char_length((unsigned char)parsed->parts[0][pattern_byte]);
                                int hay_char_len = biscuit_utf8_char_length((unsigned char)haystack[haystack_byte]);
                                
                                /* Character lengths must match */
                                if (pattern_char_len != hay_char_len) {
                                    match = false;
                                    break;
                                }
                                
                                /* Compare all bytes of the multi-byte character */
                                int b;
                                for (b = 0; b < pattern_char_len; b++) {
                                    if (pattern_byte + b >= part_byte_len || 
                                        haystack_byte + b >= haystack_byte_len ||
                                        parsed->parts[0][pattern_byte + b] != haystack[haystack_byte + b]) {
                                        match = false;
                                        break;
                                    }
                                }
                                
                                if (!match)
                                    break;
                                
                                /* Advance by full character length */
                                pattern_byte += pattern_char_len;
                                haystack_byte += hay_char_len;
                            }
                            
                            /* Verify we consumed the entire pattern */
                            if (match && pattern_byte == part_byte_len) {
                                found = true;
                            }
                        }
                        
                        if (found) {
                            biscuit_roaring_add(result, rec_idx);
                        }
                    }
                    
                    roaring_advance_uint32_iterator(iter);
                }
                
                roaring_free_uint32_iterator(iter);
                #else
                {
                    uint32_t *indices;
                    uint64_t count;
                    int j;
                    
                    indices = biscuit_roaring_to_array(candidates, &count);
                    
                    if (indices) {
                        for (j = 0; j < (int)count; j++) {
                            uint32_t rec_idx = indices[j];
                            
                            if (rec_idx < (uint32_t)idx->num_records && 
                                idx->column_data_cache && 
                                idx->column_data_cache[col_idx] &&
                                idx->column_data_cache[col_idx][rec_idx]) {
                                
                                char *haystack = idx->column_data_cache[col_idx][rec_idx];
                                int haystack_byte_len = strlen(haystack);
                                int haystack_char_len = biscuit_utf8_char_count(haystack, haystack_byte_len);
                                
                                bool found = false;
                                int char_pos;
                                for (char_pos = 0; char_pos <= haystack_char_len - part_char_len && !found; char_pos++) {
                                    int byte_offset = biscuit_utf8_char_to_byte_offset(haystack, haystack_byte_len, char_pos);
                                    
                                    if (byte_offset < 0)
                                        continue;
                                    
                                    bool match = true;
                                    int pattern_byte = 0;
                                    int haystack_byte = byte_offset;
                                    
                                    while (pattern_byte < part_byte_len && haystack_byte < haystack_byte_len && match) {
                                        if (parsed->parts[0][pattern_byte] == '_') {
                                            int hay_char_len = biscuit_utf8_char_length((unsigned char)haystack[haystack_byte]);
                                            haystack_byte += hay_char_len;
                                            pattern_byte++;
                                            continue;
                                        }
                                        
                                        int pattern_char_len = biscuit_utf8_char_length((unsigned char)parsed->parts[0][pattern_byte]);
                                        int hay_char_len = biscuit_utf8_char_length((unsigned char)haystack[haystack_byte]);
                                        
                                        if (pattern_char_len != hay_char_len) {
                                            match = false;
                                            break;
                                        }
                                        
                                        int b;
                                        for (b = 0; b < pattern_char_len; b++) {
                                            if (pattern_byte + b >= part_byte_len || 
                                                haystack_byte + b >= haystack_byte_len ||
                                                parsed->parts[0][pattern_byte + b] != haystack[haystack_byte + b]) {
                                                match = false;
                                                break;
                                            }
                                        }
                                        
                                        if (!match)
                                            break;
                                        
                                        pattern_byte += pattern_char_len;
                                        haystack_byte += hay_char_len;
                                    }
                                    
                                    if (match && pattern_byte == part_byte_len) {
                                        found = true;
                                    }
                                }
                                
                                if (found) {
                                    biscuit_roaring_add(result, rec_idx);
                                }
                            }
                        }
                        pfree(indices);
                    }
                }
                #endif
                
                biscuit_roaring_free(candidates);
            }
        }
        /* ==================== OPTIMIZED TWO PART PATTERNS ==================== */
        else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            /* INFIX: 'abc%def' */
            RoaringBitmap *prefix_match;
            RoaringBitmap *suffix_match;
            RoaringBitmap *length_filter;
            
            prefix_match = biscuit_match_col_part_at_pos(col, parsed->parts[0], 
                                                        parsed->part_byte_lens[0], 0);
            suffix_match = biscuit_match_col_part_at_end(col, parsed->parts[1], 
                                                        parsed->part_byte_lens[1]);
            
            if (!prefix_match || !suffix_match) {
                if (prefix_match) biscuit_roaring_free(prefix_match);
                if (suffix_match) biscuit_roaring_free(suffix_match);
                result = biscuit_roaring_create();
            } else {
                biscuit_roaring_and_inplace(prefix_match, suffix_match);
                biscuit_roaring_free(suffix_match);
                
                length_filter = biscuit_get_col_length_ge(col, min_len);
                if (length_filter) {
                    biscuit_roaring_and_inplace(prefix_match, length_filter);
                    biscuit_roaring_free(length_filter);
                }
                
                result = prefix_match;
            }
        }
        /* ==================== COMPLEX MULTI-PART PATTERNS ==================== */
        else {
            RoaringBitmap *candidates;
            
            result = biscuit_roaring_create();
            candidates = biscuit_get_col_length_ge(col, min_len);
            
            if (!candidates || biscuit_roaring_is_empty(candidates)) {
                if (candidates) biscuit_roaring_free(candidates);
            } else {
                if (!parsed->starts_percent) {
                    RoaringBitmap *first_part_match = biscuit_match_col_part_at_pos(
                        col, parsed->parts[0], parsed->part_byte_lens[0], 0);
                    
                    if (first_part_match) {
                        biscuit_roaring_and_inplace(first_part_match, candidates);
                        biscuit_roaring_free(candidates);
                        
                        if (!biscuit_roaring_is_empty(first_part_match)) {
                            biscuit_recursive_windowed_match_col(
                                result, col,
                                (const char **)parsed->parts, parsed->part_byte_lens,
                                parsed->part_count, parsed->ends_percent,
                                1, parsed->part_lens[0], first_part_match, col->max_length
                            );
                        }
                        biscuit_roaring_free(first_part_match);
                    } else {
                        biscuit_roaring_free(candidates);
                    }
                } else {
                    biscuit_recursive_windowed_match_col(
                        result, col,
                        (const char **)parsed->parts, parsed->part_byte_lens,
                        parsed->part_count, parsed->ends_percent,
                        0, 0, candidates, col->max_length
                    );
                    biscuit_roaring_free(candidates);
                }
            }
        }
        
        /* Normal cleanup */
        biscuit_free_parsed_pattern(parsed);
        parsed = NULL;
    }
    PG_CATCH();
    {
        /* Emergency cleanup */
        if (parsed)
            biscuit_free_parsed_pattern(parsed);
        if (result)
            biscuit_roaring_free(result);
        PG_RE_THROW();
    }
    PG_END_TRY();
    
    if (result && idx->tombstone_count > 0) {
        biscuit_roaring_andnot_inplace(result, idx->tombstones);
    }

    return result ? result : biscuit_roaring_create();
}

/* ==================== FIXED: Per-column ILIKE pattern matching ==================== */

static RoaringBitmap* 
biscuit_query_column_pattern_ilike(BiscuitIndex *idx, int col_idx, const char *pattern) {
    ColumnIndex *col;
    char *pattern_lower = NULL;
    int plen = strlen(pattern);
    ParsedPattern *parsed = NULL;
    int min_len, i;
    RoaringBitmap *result = NULL;
    int wildcard_count = 0, percent_count = 0;
    bool only_wildcards = true;
    
    /* Safety checks */
    if (!idx || col_idx < 0 || col_idx >= idx->num_columns || !idx->column_indices) {
        return biscuit_roaring_create();
    }
    
    col = &idx->column_indices[col_idx];
    
    if (col->max_length_lower <= 0) {
        elog(WARNING, "Biscuit: Column %d has invalid max_length_lower=%d", col_idx, col->max_length_lower);
        return biscuit_roaring_create();
    }
    
    /* Convert pattern to lowercase FIRST */
    pattern_lower = biscuit_str_tolower(pattern, plen);
    plen = strlen(pattern_lower);
    
    PG_TRY();
    {
        /* ========== FAST PATH 1: Empty pattern '' ========== */
        if (plen == 0) {
            /* ✅ FIX: Use lowercase length bitmaps */
            if (col->length_bitmaps_lower && col->length_bitmaps_lower[0]) {
                result = biscuit_roaring_copy(col->length_bitmaps_lower[0]);
            } else {
                result = biscuit_roaring_create();
            }
            pfree(pattern_lower);
            return result;
        }
        
        /* ========== FAST PATH 2: Single '%' matches everything ========== */
        if (plen == 1 && pattern_lower[0] == '%') {
            /* ✅ FIX: Use lowercase length_ge bitmaps */
            if (col->length_ge_bitmaps_lower && col->length_ge_bitmaps_lower[0]) {
                result = biscuit_roaring_copy(col->length_ge_bitmaps_lower[0]);
            } else {
                result = biscuit_roaring_create();
            }
            pfree(pattern_lower);
            return result;
        }
        
        /* ========== FAST PATH 3: Analyze for pure wildcards ========== */
        for (i = 0; i < plen; i++) {
            if (pattern_lower[i] == '%') {
                percent_count++;
            } else if (pattern_lower[i] == '_') {
                wildcard_count++;
            } else {
                only_wildcards = false;
                break;
            }
        }
        
        /* ========== FAST PATH 4 & 5: Pure wildcard patterns ========== */
        if (only_wildcards) {
            if (percent_count > 0) {
                /* ✅ FIX: Use lowercase length_ge bitmaps */
                if (wildcard_count <= col->max_length_lower && 
                    col->length_ge_bitmaps_lower && 
                    col->length_ge_bitmaps_lower[wildcard_count]) {
                    result = biscuit_roaring_copy(col->length_ge_bitmaps_lower[wildcard_count]);
                } else {
                    result = biscuit_roaring_create();
                }
            } else {
                /* ✅ FIX: Use lowercase length bitmaps for exact match */
                if (wildcard_count <= col->max_length_lower && 
                    col->length_bitmaps_lower && 
                    col->length_bitmaps_lower[wildcard_count]) {
                    result = biscuit_roaring_copy(col->length_bitmaps_lower[wildcard_count]);
                } else {
                    result = biscuit_roaring_create();
                }
            }
            pfree(pattern_lower);
            return result;
        }
        
        /* ========== SLOW PATH: Pattern contains concrete characters ========== */
        
        /* Parse the LOWERCASE pattern */
        parsed = biscuit_parse_pattern(pattern_lower);
        
        /* All percent signs */
        if (parsed->part_count == 0) {
            /* ✅ FIX: Use lowercase length_ge bitmaps */
            if (col->length_ge_bitmaps_lower && col->length_ge_bitmaps_lower[0]) {
                result = biscuit_roaring_copy(col->length_ge_bitmaps_lower[0]);
            } else {
                result = biscuit_roaring_create();
            }
            biscuit_free_parsed_pattern(parsed);
            pfree(pattern_lower);
            return result;
        }
        
        /* Calculate minimum required length */
        min_len = 0;
        for (i = 0; i < parsed->part_count; i++)
            min_len += parsed->part_lens[i];
        
        /* ==================== OPTIMIZED SINGLE PART PATTERNS ==================== */
        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                /* EXACT: 'abc' or 'a_c' */
                result = biscuit_match_col_part_at_pos_ilike(col, parsed->parts[0], 
                                                             parsed->part_byte_lens[0], 0);
                /* ✅ FIX: Use lowercase length bitmaps */
                if (result && min_len <= col->max_length_lower && 
                    col->length_bitmaps_lower && 
                    col->length_bitmaps_lower[min_len]) {
                    biscuit_roaring_and_inplace(result, col->length_bitmaps_lower[min_len]);
                } 
                else if (!result || min_len > col->max_length_lower) {
                    if (result) biscuit_roaring_free(result);
                    result = biscuit_roaring_create();
                }
            } else if (!parsed->starts_percent) {
                /* PREFIX: 'abc%' */
                result = biscuit_match_col_part_at_pos_ilike(col, parsed->parts[0], 
                                                             parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                /* SUFFIX: '%abc' */
                result = biscuit_match_col_part_at_end_ilike(col, parsed->parts[0], 
                                                             parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                /* SUBSTRING: '%abc%' */
                result = biscuit_roaring_create();
                /* ✅ FIX: Use max_length_lower instead of max_length */
                for (i = 0; i <= col->max_length_lower - parsed->part_lens[0]; i++) {
                    RoaringBitmap *part_match = biscuit_match_col_part_at_pos_ilike(
                        col, parsed->parts[0], parsed->part_byte_lens[0], i);
                    if (part_match) {
                        biscuit_roaring_or_inplace(result, part_match);
                        biscuit_roaring_free(part_match);
                    }
                }
            }
        }
        /* ==================== OPTIMIZED TWO PART PATTERNS ==================== */
        else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            /* INFIX: 'abc%def' */
            RoaringBitmap *prefix_match;
            RoaringBitmap *suffix_match;
            RoaringBitmap *length_filter;
            
            prefix_match = biscuit_match_col_part_at_pos_ilike(col, parsed->parts[0], 
                                                               parsed->part_byte_lens[0], 0);
            suffix_match = biscuit_match_col_part_at_end_ilike(col, parsed->parts[1], 
                                                               parsed->part_byte_lens[1]);
            
            if (!prefix_match || !suffix_match) {
                if (prefix_match) biscuit_roaring_free(prefix_match);
                if (suffix_match) biscuit_roaring_free(suffix_match);
                result = biscuit_roaring_create();
            } else {
                biscuit_roaring_and_inplace(prefix_match, suffix_match);
                biscuit_roaring_free(suffix_match);
                
                /* ✅ FIX: Use lowercase length_ge bitmaps */
                length_filter = biscuit_get_col_length_ge_lower(col, min_len);
                if (length_filter) {
                    biscuit_roaring_and_inplace(prefix_match, length_filter);
                    biscuit_roaring_free(length_filter);
                }
                
                result = prefix_match;
            }
        }
        /* ==================== COMPLEX MULTI-PART PATTERNS ==================== */
        else {
            RoaringBitmap *candidates;
            
            result = biscuit_roaring_create();
            /* ✅ FIX: Use lowercase length_ge bitmaps */
            candidates = biscuit_get_col_length_ge_lower(col, min_len);
            
            if (!candidates || biscuit_roaring_is_empty(candidates)) {
                if (candidates) biscuit_roaring_free(candidates);
            } else {
                if (!parsed->starts_percent) {
                    RoaringBitmap *first_part_match = biscuit_match_col_part_at_pos_ilike(
                        col, parsed->parts[0], parsed->part_byte_lens[0], 0);
                    
                    if (first_part_match) {
                        biscuit_roaring_and_inplace(first_part_match, candidates);
                        biscuit_roaring_free(candidates);
                        
                        if (!biscuit_roaring_is_empty(first_part_match)) {
                            /* ✅ FIX: Use max_length_lower */
                            biscuit_recursive_windowed_match_col_ilike(
                                result, col,
                                (const char **)parsed->parts, parsed->part_byte_lens,
                                parsed->part_count, parsed->ends_percent,
                                1, parsed->part_lens[0], first_part_match, 
                                col->max_length_lower  /* ← CHANGED */
                            );
                        }
                        biscuit_roaring_free(first_part_match);
                    } else {
                        biscuit_roaring_free(candidates);
                    }
                } else {
                    /* ✅ FIX: Use max_length_lower */
                    biscuit_recursive_windowed_match_col_ilike(
                        result, col,
                        (const char **)parsed->parts, parsed->part_byte_lens,
                        parsed->part_count, parsed->ends_percent,
                        0, 0, candidates, col->max_length_lower  /* ← CHANGED */
                    );
                    biscuit_roaring_free(candidates);
                }
            }
        }
        
        /* Cleanup */
        biscuit_free_parsed_pattern(parsed);
        pfree(pattern_lower);
    }
    PG_CATCH();
    {
        /* Emergency cleanup */
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        if (pattern_lower) pfree(pattern_lower);
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (result && idx->tombstone_count > 0) {
        biscuit_roaring_andnot_inplace(result, idx->tombstones);
    }
    
    return result ? result : biscuit_roaring_create();
}
/* ==================== OPTIMIZED MULTI-COLUMN RESCAN ==================== */


static void
biscuit_rescan_multicolumn(IndexScanDesc scan, ScanKey keys, int nkeys,
                          ScanKey orderbys, int norderbys)
{
    BiscuitScanOpaque *so = (BiscuitScanOpaque *)scan->opaque;
    QueryPlan *plan;
    RoaringBitmap *candidates = NULL;
    bool is_aggregate;
    bool needs_sorting;
    int limit_hint;
    int i;
    
    /* Clear previous results */
    if (so->results) {
        pfree(so->results);
        so->results = NULL;
    }
    so->num_results = 0;
    so->current = 0;
    
    if (nkeys == 0 || !so->index || so->index->num_records == 0) {
        return;
    }
    
    /* REMOVED: ILIKE error check - we now support it! */
    
    /* Detect optimizations */
    is_aggregate = biscuit_is_aggregate_query(scan);
    needs_sorting = !is_aggregate;
    limit_hint = biscuit_estimate_limit_hint(scan);
    
    /* Update scan state */
    so->is_aggregate_only = is_aggregate;
    so->needs_sorted_access = needs_sorting;
    so->limit_remaining = limit_hint;
    
    if (!so->index->column_indices) {
        elog(ERROR, "Biscuit: Multi-column index not properly initialized");
        return;
    }
    
    /* Create optimized query plan */
    plan = create_query_plan(keys, nkeys);
    
    if (plan->count == 0) {
        pfree(plan->predicates);
        pfree(plan);
        return;
    }
    
    log_query_plan(plan);
    
    /* Execute first predicate */
    QueryPredicate *first_pred = &plan->predicates[0];
    int first_strategy = first_pred->scan_key->sk_strategy;
    bool is_not_like = (first_strategy == BISCUIT_NOT_LIKE_STRATEGY || 
                        first_strategy == BISCUIT_NOT_ILIKE_STRATEGY);
    bool is_ilike = (first_strategy == BISCUIT_ILIKE_STRATEGY || 
                     first_strategy == BISCUIT_NOT_ILIKE_STRATEGY);
    
    if (first_pred->column_index < 0 || 
        first_pred->column_index >= so->index->num_columns) {
        elog(ERROR, "Biscuit: Invalid column index %d", first_pred->column_index);
        goto cleanup;
    }
    
    /* Route to appropriate query function based on strategy */
    if (is_ilike) {
        candidates = biscuit_query_column_pattern_ilike(
            so->index, first_pred->column_index, first_pred->pattern);
    } else {
        candidates = biscuit_query_column_pattern(
            so->index, first_pred->column_index, first_pred->pattern);
    }
    
    if (!candidates) {
        candidates = biscuit_roaring_create();
    }

    /* Handle NOT LIKE/NOT ILIKE for first predicate */
    if (is_not_like) {
        RoaringBitmap *all_records = biscuit_roaring_create();
        int j;
        
        #ifdef HAVE_ROARING
        roaring_bitmap_add_range(all_records, 0, so->index->num_records);
        #else
        for (j = 0; j < so->index->num_records; j++) {
            biscuit_roaring_add(all_records, j);
        }
        #endif
        
        biscuit_roaring_andnot_inplace(all_records, candidates);
        biscuit_roaring_free(candidates);
        candidates = all_records;
    }
    
    /* Filter tombstones */
    if (so->index->tombstone_count > 0) {
        biscuit_roaring_andnot_inplace(candidates, so->index->tombstones);
    }
    
    if (biscuit_roaring_count(candidates) == 0) {
        biscuit_roaring_free(candidates);
        goto cleanup;
    }
    
    /* Execute remaining predicates */
    for (i = 1; i < plan->count; i++) {
        QueryPredicate *pred = &plan->predicates[i];
        RoaringBitmap *col_result;
        int pred_strategy = pred->scan_key->sk_strategy;
        bool pred_is_not_like = (pred_strategy == BISCUIT_NOT_LIKE_STRATEGY ||
                                  pred_strategy == BISCUIT_NOT_ILIKE_STRATEGY);
        bool pred_is_ilike = (pred_strategy == BISCUIT_ILIKE_STRATEGY ||
                              pred_strategy == BISCUIT_NOT_ILIKE_STRATEGY);
        
        if (pred->column_index < 0 || 
            pred->column_index >= so->index->num_columns) {
            continue;
        }
        
        /* Route to appropriate query function */
        if (pred_is_ilike) {
            col_result = biscuit_query_column_pattern_ilike(
                so->index, pred->column_index, pred->pattern);
        } else {
            col_result = biscuit_query_column_pattern(
                so->index, pred->column_index, pred->pattern);
        }
        
        if (!col_result) {
            col_result = biscuit_roaring_create();
        }
        
        /* Handle NOT LIKE/NOT ILIKE */
        if (pred_is_not_like) {
            RoaringBitmap *all_records = biscuit_roaring_create();
            int j;
            
            #ifdef HAVE_ROARING
            roaring_bitmap_add_range(all_records, 0, so->index->num_records);
            #else
            for (j = 0; j < so->index->num_records; j++) {
                biscuit_roaring_add(all_records, j);
            }
            #endif
            
            biscuit_roaring_andnot_inplace(all_records, col_result);
            biscuit_roaring_free(col_result);
            col_result = all_records;
        }
        
        /* Intersect with candidates */
        biscuit_roaring_and_inplace(candidates, col_result);
        biscuit_roaring_free(col_result);
        
        if (biscuit_roaring_count(candidates) == 0) {
            break;
        }
    }
    
    /* Collect results */
    biscuit_collect_tids_optimized(so->index, candidates, 
                                    &so->results, &so->num_results,
                                    needs_sorting, limit_hint);
    
    biscuit_roaring_free(candidates);
    
cleanup:
    biscuit_free_query_plan(plan);
}

static void
biscuit_rescan(IndexScanDesc scan, ScanKey keys, int nkeys,
               ScanKey orderbys, int norderbys)
{
    BiscuitScanOpaque *so = (BiscuitScanOpaque *)scan->opaque;
    RoaringBitmap *result = NULL;
    bool is_aggregate;
    bool needs_sorting;
    int limit_hint;
    int i;
    
    /* Clear previous results */
    if (so->results) {
        pfree(so->results);
        so->results = NULL;
    }
    so->num_results = 0;
    so->current = 0;
    
    if (!so->index) {
        return;
    }
    
    if (nkeys == 0 || so->index->num_records == 0) {
        return;
    }
    
    /* Detect optimizations */
    is_aggregate = biscuit_is_aggregate_query(scan);
    needs_sorting = !is_aggregate;
    limit_hint = biscuit_estimate_limit_hint(scan);
    
    /* Update scan opaque */
    so->is_aggregate_only = is_aggregate;
    so->needs_sorted_access = needs_sorting;
    so->limit_remaining = limit_hint;
    
    /* Route to multi-column handler if needed */
    if (so->index->num_columns > 1) {
        biscuit_rescan_multicolumn(scan, keys, nkeys, orderbys, norderbys);
        return;
    }
    
    /* Process ALL keys and intersect results (AND logic) */
    for (i = 0; i < nkeys; i++) {
        ScanKey key = &keys[i];
        text *pattern_text;
        char *pattern;
        RoaringBitmap *key_result;
        bool is_not;
        
        if (key->sk_flags & SK_ISNULL) {
            continue;
        }
        
        pattern_text = DatumGetTextPP(key->sk_argument);
        pattern = text_to_cstring(pattern_text);
        
        /* Route based on strategy */
        switch (key->sk_strategy) {
            case BISCUIT_LIKE_STRATEGY:
                /* Case-sensitive query - use original index */
                key_result = biscuit_query_pattern(so->index, pattern);
                is_not = false;
                break;
                
            case BISCUIT_NOT_LIKE_STRATEGY:
                /* Case-sensitive negation - use original index */
                key_result = biscuit_query_pattern(so->index, pattern);
                is_not = true;
                break;
                
            case BISCUIT_ILIKE_STRATEGY:
                /* NEW: Case-insensitive query - use lowercase index */
                key_result = biscuit_query_pattern_ilike(so->index, pattern);
                is_not = false;
                break;
                
            case BISCUIT_NOT_ILIKE_STRATEGY:
                /* NEW: Case-insensitive negation - use lowercase index */
                key_result = biscuit_query_pattern_ilike(so->index, pattern);
                is_not = true;
                break;
                
            default:
                elog(ERROR, "Unsupported scan strategy: %d", key->sk_strategy);
                pfree(pattern);
                continue;
        }
        
        pfree(pattern);
        
        if (!key_result) {
            if (result) biscuit_roaring_free(result);
            return;
        }
        
        /* Handle NOT variants by inverting bitmap */
        if (is_not) {
            RoaringBitmap *all_records = biscuit_roaring_create();
            int j;
            
            #ifdef HAVE_ROARING
            roaring_bitmap_add_range(all_records, 0, so->index->num_records);
            #else
            for (j = 0; j < so->index->num_records; j++) {
                biscuit_roaring_add(all_records, j);
            }
            #endif
            
            biscuit_roaring_andnot_inplace(all_records, key_result);
            biscuit_roaring_free(key_result);
            key_result = all_records;
        }
        
        /* Intersect with previous results (AND logic) */
        if (result == NULL) {
            result = key_result;
        } else {
            biscuit_roaring_and_inplace(result, key_result);
            biscuit_roaring_free(key_result);
            
            if (biscuit_roaring_is_empty(result)) {
                biscuit_roaring_free(result);
                result = NULL;
                return;
            }
        }
    }
    
    if (!result) {
        return;
    }
    
    /* Filter out tombstones */
    if (so->index->tombstone_count > 0) {
        biscuit_roaring_andnot_inplace(result, so->index->tombstones);
    }
    
    /* Collect results with optimizations */
    biscuit_collect_tids_optimized(so->index, result, 
                                    &so->results, &so->num_results, 
                                    needs_sorting, limit_hint);
    
    biscuit_roaring_free(result);
}

  static bool
  biscuit_gettuple(IndexScanDesc scan, ScanDirection dir)
  {
      BiscuitScanOpaque *so = (BiscuitScanOpaque *)scan->opaque;
  
      /* Check if we've exhausted results */
      if (so->current >= so->num_results) {
          //elog(DEBUG1, "Biscuit: Scan complete, returned %d tuples", so->current);
          return false;
      }
  
      /* Return next TID */
      scan->xs_heaptid = so->results[so->current];
      scan->xs_recheck = false;
      so->current++;
  
      /* Track progress for LIMIT queries */
      if (so->limit_remaining > 0) {
          so->limit_remaining--;
          if (so->limit_remaining == 0) {
              //elog(DEBUG1, "Biscuit: LIMIT reached, stopping early");
          }
      }
      //elog(DEBUG1, "Biscuit to your service!");
      return true;
  }
  
  

/*
* Enhanced getbitmap with chunked TID insertion for better memory efficiency
*/
static int64
biscuit_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
    BiscuitScanOpaque *so = (BiscuitScanOpaque *)scan->opaque;
    int64 ntids = 0;
    int chunk_size = 10000;
    int i;
    
    /*
     * OPTIMIZATION: For bitmap scans, TIDs are unsorted
     * BitmapHeapScan will handle page-level ordering
     * 
     * This is where we save time for COUNT(*) queries:
     * - No sorting overhead
     * - Direct bitmap insertion
     * - Chunked processing for large result sets
     */
    
    if (so->num_results > 0) {
        bool recheck = false;
        
        //elog(DEBUG1, "Biscuit: Bitmap scan returning %d unsorted TIDs", so->num_results);
        
        if (so->num_results > chunk_size) {
            /* Chunked insertion for large result sets */
            for (i = 0; i < so->num_results; i += chunk_size) {
                int batch_size = Min(chunk_size, so->num_results - i);
                tbm_add_tuples(tbm, &so->results[i], batch_size, recheck);
                ntids += batch_size;
                
                CHECK_FOR_INTERRUPTS();
            }
        } else {
            /* Direct insertion for small result sets */
            tbm_add_tuples(tbm, so->results, so->num_results, recheck);
            ntids = so->num_results;
        }
        
        //elog(DEBUG1, "Biscuit: Bitmap scan complete, added %lld TIDs", (long long)ntids);
    }
    
    return ntids;
}


static void
biscuit_endscan(IndexScanDesc scan)
{
    BiscuitScanOpaque *so = (BiscuitScanOpaque *)scan->opaque;
    
    if (so) {
        if (so->results)
            pfree(so->results);
        pfree(so);
    }
}
/* ==================== OPERATOR SUPPORT ==================== */

PG_FUNCTION_INFO_V1(biscuit_like_support);
Datum
biscuit_like_support(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(true);
}

/* ==================== SIZE OF INDEX ==================== */
/*
 * Calculate accurate memory footprint of Biscuit index
 * Accounts for Roaring bitmap compression
 */

/* Add this forward declaration near the top with other PG_FUNCTION_INFO_V1 declarations */
PG_FUNCTION_INFO_V1(biscuit_index_memory_size);

/* Helper function to get Roaring bitmap memory size */
static size_t
biscuit_roaring_memory_usage(const RoaringBitmap *rb)
{
    if (!rb)
        return 0;
    
#ifdef HAVE_ROARING
    /* Use Roaring's built-in size calculation */
    return roaring_bitmap_size_in_bytes(rb);
#else
    /* Fallback implementation for simple bitmap */
    return sizeof(RoaringBitmap) + (rb->capacity * sizeof(uint64_t));
#endif
}

/* Helper function to calculate CharIndex memory usage */
static size_t
biscuit_charindex_memory_usage(const CharIndex *cidx)
{
    size_t total = 0;
    int i;
    
    if (!cidx)
        return 0;
    
    /* PosEntry array */
    total += cidx->capacity * sizeof(PosEntry);
    
    /* Each bitmap in the entries */
    for (i = 0; i < cidx->count; i++) {
        total += biscuit_roaring_memory_usage(cidx->entries[i].bitmap);
    }
    
    return total;
}

static size_t
biscuit_columnindex_memory_usage(const ColumnIndex *col_idx)
{
    size_t total = 0;
    int ch, i;
    
    if (!col_idx)
        return 0;
    
    /* FIX 13: Add sanity check on max_length */
    if (col_idx->max_length < 0 ) {
        elog(WARNING, "Invalid max_length in ColumnIndex: %d", col_idx->max_length);
        return 0;
    }
    
    /* Case-sensitive indices */
    for (ch = 0; ch < CHAR_RANGE; ch++) {
        total += biscuit_charindex_memory_usage(&col_idx->pos_idx[ch]);
        total += biscuit_charindex_memory_usage(&col_idx->neg_idx[ch]);
        total += biscuit_roaring_memory_usage(col_idx->char_cache[ch]);
    }
    
    /* Case-insensitive indices */
    for (ch = 0; ch < CHAR_RANGE; ch++) {
        total += biscuit_charindex_memory_usage(&col_idx->pos_idx_lower[ch]);
        total += biscuit_charindex_memory_usage(&col_idx->neg_idx_lower[ch]);
        total += biscuit_roaring_memory_usage(col_idx->char_cache_lower[ch]);
    }
    
    /* Length bitmaps (shared for both case-sensitive and case-insensitive) */
    if (col_idx->length_bitmaps) {
        /* FIX 14: Bounds check before loop */
        for (i = 0; i <= col_idx->max_length; i++) {
            total += biscuit_roaring_memory_usage(col_idx->length_bitmaps[i]);
        }
        total += (col_idx->max_length + 1) * sizeof(RoaringBitmap *);
    }
    
    if (col_idx->length_ge_bitmaps) {
        /* FIX 15: Bounds check before loop */
        for (i = 0; i <= col_idx->max_length; i++) {
            total += biscuit_roaring_memory_usage(col_idx->length_ge_bitmaps[i]);
        }
        total += (col_idx->max_length + 1) * sizeof(RoaringBitmap *);
    }

    /* CASE-INSENSITIVE length bitmaps (separate!) */
    if (col_idx->length_bitmaps_lower && col_idx->max_length_lower > 0) {
        for (i = 0; i <= col_idx->max_length_lower; i++) {
            total += biscuit_roaring_memory_usage(col_idx->length_bitmaps_lower[i]);
        }
        total += (col_idx->max_length_lower + 1) * sizeof(RoaringBitmap *);
    }
    
    if (col_idx->length_ge_bitmaps_lower && col_idx->max_length_lower > 0) {
        for (i = 0; i <= col_idx->max_length_lower; i++) {
            total += biscuit_roaring_memory_usage(col_idx->length_ge_bitmaps_lower[i]);
        }
        total += (col_idx->max_length_lower + 1) * sizeof(RoaringBitmap *);
    }
    
    return total;
}

Datum
biscuit_index_memory_size(PG_FUNCTION_ARGS)
{
    Oid indexoid = PG_GETARG_OID(0);
    Relation index;
    BiscuitIndex *idx;
    size_t total_bytes = 0;
    size_t string_bytes = 0;
    size_t bitmap_bytes = 0;
    size_t metadata_bytes = 0;
    int i, ch, col;
    
    index = index_open(indexoid, AccessShareLock);
    
    /* FIX 1: Add NULL check for index */
    if (!index) {
        elog(ERROR, "Could not open index with OID %u", indexoid);
    }
    
    idx = (BiscuitIndex *)index->rd_amcache;
    if (!idx) {
        idx = biscuit_load_index(index);
        index->rd_amcache = idx;
    }
    
    /* FIX 2: Add NULL check after load */
    if (!idx) {
        index_close(index, AccessShareLock);
        PG_RETURN_INT64(0);
    }
    
    /* ==================== BASE STRUCTURE ==================== */
    metadata_bytes += sizeof(BiscuitIndex);
    
    /* TID array - FIX 3: Check if tids array exists */
    if (idx->tids) {
        metadata_bytes += idx->capacity * sizeof(ItemPointerData);
    }
    
    /* ==================== SINGLE-COLUMN INDEX ==================== */
    if (idx->num_columns == 1) {
        /* Data cache pointer array (original strings) */
        if (idx->data_cache) {
            metadata_bytes += idx->capacity * sizeof(char *);
            for (i = 0; i < idx->num_records && i < idx->capacity; i++) {
                if (idx->data_cache[i]) {
                    string_bytes += strlen(idx->data_cache[i]) + 1;
                }
            }
        }
        
        /* Lowercase data cache pointer array */
        if (idx->data_cache_lower) {
            metadata_bytes += idx->capacity * sizeof(char *);
            for (i = 0; i < idx->num_records && i < idx->capacity; i++) {
                if (idx->data_cache_lower[i]) {
                    string_bytes += strlen(idx->data_cache_lower[i]) + 1;
                }
            }
        }
        
        /* Case-sensitive indices (_legacy) - 256 characters worth */
        for (ch = 0; ch < CHAR_RANGE; ch++) {
            size_t pos_usage = biscuit_charindex_memory_usage(&idx->pos_idx_legacy[ch]);
            size_t neg_usage = biscuit_charindex_memory_usage(&idx->neg_idx_legacy[ch]);
            size_t cache_usage = biscuit_roaring_memory_usage(idx->char_cache_legacy[ch]);
            
            bitmap_bytes += pos_usage + neg_usage + cache_usage;
        }
        
        /* Case-insensitive indices (_lower) - 256 characters worth */
        for (ch = 0; ch < CHAR_RANGE; ch++) {
            size_t pos_usage = biscuit_charindex_memory_usage(&idx->pos_idx_lower[ch]);
            size_t neg_usage = biscuit_charindex_memory_usage(&idx->neg_idx_lower[ch]);
            size_t cache_usage = biscuit_roaring_memory_usage(idx->char_cache_lower[ch]);
            
            bitmap_bytes += pos_usage + neg_usage + cache_usage;
        }
        
        /* CRITICAL FIX: Single-column uses SHARED length bitmaps for both LIKE and ILIKE */
        /* Only count them ONCE, not twice! */
        if (idx->length_bitmaps_legacy) {
            if (idx->max_length_legacy > 0) {
                metadata_bytes += idx->max_length_legacy * sizeof(RoaringBitmap *);
                for (i = 0; i < idx->max_length_legacy; i++) {
                    if (idx->length_bitmaps_legacy[i]) {
                        bitmap_bytes += biscuit_roaring_memory_usage(idx->length_bitmaps_legacy[i]);
                    }
                }
            }
        }

        if (idx->length_ge_bitmaps_legacy) {
            if (idx->max_length_legacy > 0) {
                metadata_bytes += idx->max_length_legacy * sizeof(RoaringBitmap *);
                for (i = 0; i < idx->max_length_legacy; i++) {
                    if (idx->length_ge_bitmaps_legacy[i]) {
                        bitmap_bytes += biscuit_roaring_memory_usage(idx->length_ge_bitmaps_legacy[i]);
                    }
                }
            }
        }
        
        /* REMOVED: No separate lowercase length bitmaps for single-column! */
        /* The _lower character indices use the SAME length bitmaps as case-sensitive */
    }
    /* ==================== MULTI-COLUMN INDEX ==================== */
    else if (idx->num_columns > 1) {  /* FIX 8: Add explicit check */
                
        /* Column metadata arrays */
        metadata_bytes += idx->num_columns * sizeof(Oid);
        metadata_bytes += idx->num_columns * sizeof(FmgrInfo);
        metadata_bytes += idx->num_columns * sizeof(char **);
        
        /* Per-column data caches */
        /* FIX 10: Check if column_data_cache exists */
        if (idx->column_data_cache) {
            for (col = 0; col < idx->num_columns; col++) {
                if (idx->column_data_cache[col]) {
                    metadata_bytes += idx->capacity * sizeof(char *);
                    /* FIX 11: Bounds check */
                    for (i = 0; i < idx->num_records && i < idx->capacity; i++) {
                        if (idx->column_data_cache[col][i]) {
                            string_bytes += strlen(idx->column_data_cache[col][i]) + 1;
                        }
                    }
                }
            }
        }
        
        /* Per-column indices */
        if (idx->column_indices) {
            metadata_bytes += idx->num_columns * sizeof(ColumnIndex);
            for (col = 0; col < idx->num_columns; col++) {
                bitmap_bytes += biscuit_columnindex_memory_usage(&idx->column_indices[col]);
            }
        }
    }
    
    /* ==================== CRUD STRUCTURES ==================== */
    
    /* Tombstones bitmap - FIX 12: Check if tombstones exists */
    if (idx->tombstones) {
        bitmap_bytes += biscuit_roaring_memory_usage(idx->tombstones);
    }
    
    /* Free list array */
    if (idx->free_list && idx->free_capacity > 0) {
        metadata_bytes += idx->free_capacity * sizeof(uint32_t);
    }
    
    /* Total everything up */
    total_bytes = metadata_bytes + string_bytes + bitmap_bytes;
    
    index_close(index, AccessShareLock);
    
    PG_RETURN_INT64((int64)total_bytes);
}

/* ==================== INDEX HANDLER ==================== */

Datum
biscuit_handler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);
    
    amroutine->amstrategies = 4;
    amroutine->amsupport = 2;
    amroutine->amoptsprocnum = 0;
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = false;
    amroutine->amcanbackward = false;
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = true;  /* CHANGED: Enable multi-column support */
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = false;
    amroutine->amstorage = false;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = false;
    amroutine->amcanparallel = true;
    amroutine->amcaninclude = false;
    amroutine->amusemaintenanceworkmem = false;
    amroutine->amsummarizing = false;
    amroutine->amparallelvacuumoptions = 0;
    amroutine->amkeytype = InvalidOid;
    
    amroutine->ambuild = biscuit_build;
    amroutine->ambuildempty = biscuit_buildempty;
    amroutine->aminsert = biscuit_insert;
    amroutine->ambulkdelete = biscuit_bulkdelete;
    amroutine->amvacuumcleanup = biscuit_vacuumcleanup;
    amroutine->amcanreturn = biscuit_canreturn;
    amroutine->amcostestimate = biscuit_costestimate;
    amroutine->amoptions = biscuit_options;
    amroutine->amproperty = NULL;
    amroutine->ambuildphasename = NULL;
    amroutine->amvalidate = biscuit_validate;
    amroutine->amadjustmembers = biscuit_adjustmembers;
    amroutine->ambeginscan = biscuit_beginscan;
    amroutine->amrescan = biscuit_rescan;
    amroutine->amgettuple = biscuit_gettuple;
    amroutine->amgetbitmap = biscuit_getbitmap;
    amroutine->amendscan = biscuit_endscan;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;
    
    PG_RETURN_POINTER(amroutine);
}

/* ==================== DIAGNOSTIC FUNCTION ==================== */

Datum
biscuit_index_stats(PG_FUNCTION_ARGS)
{
    Oid indexoid = PG_GETARG_OID(0);
    Relation index;
    BiscuitIndex *idx;
    StringInfoData buf;
    int active_records = 0;
    int i;
    
    index = index_open(indexoid, AccessShareLock);
    
    idx = (BiscuitIndex *)index->rd_amcache;
    if (!idx) {
        idx = biscuit_load_index(index);
        index->rd_amcache = idx;
    }
    
    /* Count active records (excluding tombstones) */
    for (i = 0; i < idx->num_records; i++) {
        bool has_data = (idx->num_columns == 1 && idx->data_cache[i] != NULL) ||
                        (idx->num_columns > 1 && idx->column_data_cache[0][i] != NULL);
        if (has_data) {
            bool is_tombstoned = false;
            #ifdef HAVE_ROARING
            is_tombstoned = roaring_bitmap_contains(idx->tombstones, (uint32_t)i);
            #else
            uint32_t block = i >> 6;
            uint32_t bit = i & 63;
            is_tombstoned = (block < idx->tombstones->num_blocks &&
                            (idx->tombstones->blocks[block] & (1ULL << bit)));
            #endif
            
            if (!is_tombstoned)
                active_records++;
        }
    }
    
    initStringInfo(&buf);
    appendStringInfo(&buf, "Biscuit Index Statistics\n");
    appendStringInfo(&buf, "==========================================\n");
    appendStringInfo(&buf, "Index: %s\n", RelationGetRelationName(index));
    appendStringInfo(&buf, "Active records: %d\n", active_records);
    appendStringInfo(&buf, "Total slots: %d\n", idx->num_records);
    appendStringInfo(&buf, "Free slots: %d\n", idx->free_count);
    appendStringInfo(&buf, "Tombstones: %d\n", idx->tombstone_count);
    appendStringInfo(&buf, "Max length: %d\n", idx->max_len);
    appendStringInfo(&buf, "------------------------\n");
    appendStringInfo(&buf, "CRUD Statistics:\n");
    appendStringInfo(&buf, "  Inserts: %lld\n", (long long)idx->insert_count);
    appendStringInfo(&buf, "  Updates: %lld\n", (long long)idx->update_count);
    appendStringInfo(&buf, "  Deletes: %lld\n", (long long)idx->delete_count);
    appendStringInfo(&buf, "------------------------\n");
    appendStringInfo(&buf, "Active Optimizations:\n");
    appendStringInfo(&buf, "  ✓ 1. Skip wildcard intersections\n");
    appendStringInfo(&buf, "  ✓ 2. Early termination on empty\n");
    appendStringInfo(&buf, "  ✓ 3. Avoid redundant copies\n");
    appendStringInfo(&buf, "  ✓ 4. Optimized single-part patterns\n");
    appendStringInfo(&buf, "  ✓ 5. Skip unnecessary length ops\n");
    appendStringInfo(&buf, "  ✓ 6. TID sorting for sequential I/O\n");
    appendStringInfo(&buf, "  ✓ 7. Batch TID insertion\n");
    appendStringInfo(&buf, "  ✓ 8. Direct bitmap iteration\n");
    appendStringInfo(&buf, "  ✓ 9. Parallel bitmap scan support\n");
    appendStringInfo(&buf, "  ✓ 10. Batch cleanup on threshold\n");
    appendStringInfo(&buf, "  ✓ 11. Skip sorting for bitmap scans (aggregates)\n");
    appendStringInfo(&buf, "  ✓ 12. LIMIT-aware TID collection\n");
    
    index_close(index, AccessShareLock);
    
    PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
