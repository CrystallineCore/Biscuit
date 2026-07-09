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
#include "executor/executor.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/snapmgr.h"
#include "access/xact.h"
#include "postmaster/interrupt.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "utils/syscache.h"

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
#define BISCUIT_LIBRARY_VERSION         "2.5.0 - Disk"

/*
 * BISCUIT_SNAPSHOT_GEN_THRESHOLD
 *
 * Maximum number of generation bumps (inserts/vacuums) we let accumulate
 * in memory before forcing an eager on-disk snapshot re-save from
 * biscuit_insert()/biscuit_bulkdelete(). Keeping the snapshot within this
 * many generations of "live" bounds how much from-heap rebuild work a
 * cold load has to redo when it finds the snapshot stale -- without
 * this, a long-running backend that never triggers a natural resave
 * could drift arbitrarily far ahead of the on-disk copy.
 *
 * The comparison that uses this constant is done with unsigned
 * subtraction on idx->gen and idx->gen_at_last_snapshot (both uint64):
 *
 *     if (idx->gen - idx->gen_at_last_snapshot >= BISCUIT_SNAPSHOT_GEN_THRESHOLD)
 *
 * This is intentional and must NOT be "fixed" into a signed comparison.
 * idx->gen is monotonically non-decreasing and gen_at_last_snapshot is
 * always some generation that was live at an earlier point, so in
 * normal operation idx->gen >= idx->gen_at_last_snapshot and the
 * subtraction is just their true difference. In the astronomically
 * unlikely event that idx->gen wraps around UINT64_MAX, unsigned
 * subtraction still yields the correct modular distance between the two
 * counters -- exactly the "how far behind is the snapshot" quantity we
 * want -- whereas a signed comparison would misbehave right at the wrap
 * boundary. There is no plan to special-case wraparound; at 2^64
 * generations this is not a practical concern, and the eventual
 * wraparound behavior of the unsigned subtraction is relied upon, not
 * something to be "corrected" later.
 */
#define BISCUIT_SNAPSHOT_GEN_THRESHOLD   64

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

    /*
     * Monotonic generation counter.  Mirrors idx->gen at the time of the
     * last biscuit_write_metadata_to_disk() call, so a cold-loaded
     * snapshot can be recognized as stale relative to the live
     * heap-backed state.  See the comment on idx->gen in the
     * BiscuitIndex struct for the (intentionally non-transactional)
     * semantics.
     */
    uint64 gen;

    /*
     * Reserved for future metadata.  Writers must zero-fill this; readers
     * must ignore its contents (not rely on any value found here), so
     * old readers stay forward-compatible with newer writers that start
     * using a slot here.
     */
    uint32 reserved[4];
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
     * Monotonic generation counter.
     *
     * Incremented in biscuit_insert() and biscuit_bulkdelete() immediately
     * after the in-memory bitmap mutation has completed successfully, and
     * persisted to the metapage (BiscuitMetaPageData.gen) right away via
     * biscuit_write_metadata_to_disk().  This lets a consumer of the
     * on-disk snapshot (biscuit_persist.c) detect that a snapshot is
     * stale relative to the live in-memory index.
     *
     * INTENTIONALLY NON-TRANSACTIONAL: this counter is bumped as soon as
     * the mutation lands in memory, without regard for whether the
     * enclosing transaction ultimately commits or rolls back. A rolled
     * back INSERT/VACUUM will still have bumped idx->gen. This means the
     * counter can over-invalidate (mark a snapshot stale when nothing
     * durable actually changed) but must never under-invalidate (fail to
     * bump when a durable change occurred). Over-invalidation just costs
     * an extra rebuild/re-snapshot; under-invalidation would let a stale
     * snapshot silently mask real data, which is the bug this field
     * exists to fix. Do NOT try to make this transactional (e.g. by
     * deferring the bump to commit via a callback) -- that would
     * reintroduce a window where a crash/cache-evict between the durable
     * in-memory mutation and the deferred bump leaves gen unmodified while
     * data changed, i.e. exactly the under-invalidation this exists to
     * prevent.
     */
    uint64 gen;

    /*
     * Generation value as of the last successful on-disk snapshot
     * (biscuit_persist_save()).  Purely in-memory bookkeeping used to
     * decide whether a snapshot needs to be re-taken -- it must NEVER be
     * serialized to disk (biscuit_persist_save()/biscuit_persist_load()
     * must not read or write this field; it is meaningless outside the
     * process that set it, since a freshly loaded/built BiscuitIndex has
     * no snapshot yet).
     */
    uint64 gen_at_last_snapshot;

    /* Reserved for future in-memory bookkeeping fields. */
    uint64 reserved[4];
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

/* ==================== CROSS-VERSION COMPATIBILITY ==================== */

/*
 * ParallelIndexScanDescData::ps_offset_am
 *
 * In PG18+ the AM-private offset field was renamed from ps_offset to
 * ps_offset_am to clarify its purpose.  Use this macro everywhere so a
 * single version check covers all call sites.
 */
#if PG_VERSION_NUM >= 180000
#define BISCUIT_PARALLEL_AM_OFFSET(ps)  ((ps)->ps_offset_am)
#else
#define BISCUIT_PARALLEL_AM_OFFSET(ps)  ((ps)->ps_offset)
#endif

/*
 * Index search counter
 *
 * xs_numIndexSearches was added to IndexScanDescData in PG17 and then
 * replaced by scan->instrument->nsearches in PG18.  Use this macro to
 * increment the counter in a version-safe way; it expands to nothing on
 * PG16 and earlier where neither field exists.
 */
#if PG_VERSION_NUM >= 180000
#define BISCUIT_COUNT_INDEX_SEARCH(scan) \
    do { if ((scan)->instrument) (scan)->instrument->nsearches++; } while(0)
#elif PG_VERSION_NUM >= 170000
#define BISCUIT_COUNT_INDEX_SEARCH(scan) \
    do { (scan)->xs_numIndexSearches++; } while(0)
#else
#define BISCUIT_COUNT_INDEX_SEARCH(scan) \
    do { } while(0)
#endif

#endif /* BISCUIT_COMMON_H */
