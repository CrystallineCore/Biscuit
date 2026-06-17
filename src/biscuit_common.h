/*
 * biscuit_common.h
 * Shared types, constants, macros, and forward declarations
 * for the Biscuit PostgreSQL Index Access Method.
 */

#ifndef BISCUIT_COMMON_H
#define BISCUIT_COMMON_H

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
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/date.h"
#include "fmgr.h"
#include "utils/inval.h"
#include "storage/ipc.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "utils/formatting.h"
#include "utils/pg_locale.h"
#include "mb/pg_wchar.h"
#include "storage/itemptr.h"
#include "access/parallel.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "port/atomics.h"
#include "nodes/execnodes.h"

/* ==================== ROARING BITMAP TYPES ==================== */

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

/* ==================== STRATEGY NUMBERS ==================== */

/* BTree strategy numbers for reference
#define BTLessStrategyNumber            1
#define BTLessEqualStrategyNumber       2
#define BTEqualStrategyNumber           3
#define BTGreaterEqualStrategyNumber    4
#define BTGreaterStrategyNumber         5
*/

#define BISCUIT_LIKE_STRATEGY           1
#define BISCUIT_NOT_LIKE_STRATEGY       2
#define BISCUIT_ILIKE_STRATEGY          3
#define BISCUIT_NOT_ILIKE_STRATEGY      4

/* ==================== CONSTANTS ==================== */

#define BISCUIT_MAGIC                   0x42495343  /* "BISC" */
#define BISCUIT_VERSION                 1
#define BISCUIT_METAPAGE_BLKNO          0
#define CHAR_RANGE                      256
#define TOMBSTONE_CLEANUP_THRESHOLD     1000
#define RADIX_SORT_THRESHOLD            5000
#define BISCUIT_LIBRARY_VERSION         "2.3.0 - Croissant"

/* ==================== MEMORY MANAGEMENT MACROS ==================== */

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

/* ==================== CORE DATA STRUCTURES ==================== */

/* Position entry for character position indices */
typedef struct {
    int pos;
    RoaringBitmap *bitmap;
} PosEntry;

/* Dynamic array of position entries per character */
typedef struct {
    PosEntry *entries;
    int count;
    int capacity;
} CharIndex;

/* Disk meta-page */
typedef struct BiscuitMetaPageData {
    uint32 magic;
    uint32 version;
    BlockNumber root;
    uint32 num_records;
} BiscuitMetaPageData;

typedef BiscuitMetaPageData *BiscuitMetaPage;

/* Per-column bitmap index (case-sensitive + case-insensitive) */
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
    RoaringBitmap **length_bitmaps_lower;
    RoaringBitmap **length_ge_bitmaps_lower;
    int max_length_lower;
} ColumnIndex;

/* Main in-memory index structure */
typedef struct BiscuitIndex {
    int num_columns;
    Oid *column_types;
    FmgrInfo *output_funcs;
    char ***column_data_cache;      /* [column][record] */

    /* Per-column indices for multi-column indexes */
    ColumnIndex *column_indices;

    /*
     * Pre-lowercased string cache for multi-column indexes.
     * column_data_cache_lower[col][rec] mirrors column_data_cache[col][rec]
     * but with every string run through biscuit_str_tolower() at build /
     * load time.  This lets biscuit_fallback_scan() use a direct pointer
     * for ILIKE queries instead of allocating a new lowercased copy on
     * every record on every scan call.
     *
     * Layout and lifecycle are identical to column_data_cache:
     *   • Allocated as char**  per column, palloc0'd to idx->capacity slots.
     *   • Grown with repalloc whenever column_data_cache is grown.
     *   • NULL entries mirror NULL entries in column_data_cache.
     *   • Freed / NULLed in the vacuum bulkdelete path alongside
     *     column_data_cache entries.
     * Only allocated when num_columns > 1; NULL otherwise.
     */
    char ***column_data_cache_lower;

    /* Single-column (legacy) fields */
    CharIndex pos_idx_legacy[CHAR_RANGE];
    CharIndex neg_idx_legacy[CHAR_RANGE];
    RoaringBitmap *char_cache_legacy[CHAR_RANGE];
    RoaringBitmap **length_bitmaps_legacy;
    RoaringBitmap **length_ge_bitmaps_legacy;
    int max_length_legacy;
    int max_len;

    /* Case-insensitive single-column fields */
    CharIndex pos_idx_lower[CHAR_RANGE];
    CharIndex neg_idx_lower[CHAR_RANGE];
    RoaringBitmap *char_cache_lower[CHAR_RANGE];
    RoaringBitmap **length_bitmaps_lower;
    RoaringBitmap **length_ge_bitmaps_lower;
    int max_length_lower;

    char **data_cache_lower;

    /* Record data */
    ItemPointerData *tids;
    char **data_cache;
    int num_records;
    int capacity;

    /* CRUD state */
    RoaringBitmap *tombstones;
    uint32_t *free_list;
    int free_count;
    int free_capacity;
    int tombstone_count;

    /* Statistics */
    int64 insert_count;
    int64 update_count;
    int64 delete_count;

    /*
     * Lazy-load / preload state.
     *
     * Set to one of the BISCUIT_PRELOAD_* constants defined in
     * biscuit_preload.h.  The field is written only by the background
     * worker (via biscuit_complete_preload) or by beginscan when it
     * creates a skeleton.  The rescan path reads it to decide whether
     * to use the fast bitmap path or the fallback sequential scan.
     *
     * Using a plain uint32 is safe because the worker and the query
     * backend are separate OS processes; the worker writes the fully-
     * warmed index into its own CacheMemoryContext copy and signals
     * the owning session, which re-fetches from the cache.  There is
     * no cross-process pointer sharing.
     */
    uint32 preload_state;
} BiscuitIndex;

/* Scan opaque state */
typedef struct {
    BiscuitIndex *index;
    ItemPointerData *results;
    int num_results;
    int current;

    bool is_aggregate_only;
    bool needs_sorted_access;
    int limit_remaining;
} BiscuitScanOpaque;

/* Parsed LIKE pattern */
typedef struct {
    char **parts;
    int *part_lens;         /* CHARACTER counts */
    int *part_byte_lens;    /* byte lengths */
    int part_count;
    bool starts_percent;
    bool ends_percent;
} ParsedPattern;

/* Query plan predicate */
typedef struct {
    int column_index;
    char *pattern;
    ScanKey scan_key;

    bool has_percent;
    bool starts_percent;
    bool ends_percent;
    bool is_prefix;
    bool is_suffix;
    bool is_exact;
    bool is_substring;

    int concrete_chars;
    int underscore_count;
    int percent_count;
    int partition_count;
    int anchor_strength;

    double selectivity_score;
    int priority;
} QueryPredicate;

typedef struct QueryPlan {
    QueryPredicate *predicates;
    int count;
    int capacity;
} QueryPlan;

/* Parallel TID collection worker */
typedef struct {
    BiscuitIndex *idx;
    uint32_t *indices;
    uint64_t start_idx;
    uint64_t end_idx;
    ItemPointerData *output;
    int output_count;
} TIDCollectionWorker;

/* Pattern result cache entry */
typedef struct PatternCacheEntry {
    char *pattern;
    ItemPointerData *tids;
    int num_tids;
    struct PatternCacheEntry *next;
} PatternCacheEntry;

#endif /* BISCUIT_COMMON_H */
