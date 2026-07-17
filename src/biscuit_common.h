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

/* ==================== OPCLASS CASE-MODE GATING ==================== */
/*
 * Per-column bit flags recording which structure set(s) a given index
 * column was actually built with, derived from the opclass/opfamily the
 * user chose for that column (biscuit_ops, biscuit_like_ops, or
 * biscuit_ilike_ops -- see biscuit.sql).
 *
 *   BISCUIT_MODE_LIKE  -- build/maintain the case-sensitive structures
 *                         (pos_idx/neg_idx/char_cache/length_bitmaps and
 *                         their non-"_lower" counterparts). Needed to
 *                         serve LIKE / NOT LIKE.
 *   BISCUIT_MODE_ILIKE -- build/maintain the case-insensitive "_lower"
 *                         structures. Needed to serve ILIKE / NOT ILIKE.
 *
 * biscuit_ops builds both (BISCUIT_MODE_BOTH); biscuit_like_ops builds
 * only BISCUIT_MODE_LIKE; biscuit_ilike_ops builds only
 * BISCUIT_MODE_ILIKE. These flags are derived at build/load time from
 * the index relation's opfamily (see biscuit_get_column_case_mode() in
 * biscuit_index.c) and are never themselves persisted to disk -- the
 * on-disk snapshot only ever contains whichever structures were built,
 * and the mode is always recomputed fresh from the live Relation.
 */
#define BISCUIT_MODE_LIKE               0x1
#define BISCUIT_MODE_ILIKE              0x2
#define BISCUIT_MODE_BOTH               (BISCUIT_MODE_LIKE | BISCUIT_MODE_ILIKE)

/* ==================== CONSTANTS ==================== */

#define BISCUIT_MAGIC                   0x42495343  /* "BISC" */
#define BISCUIT_VERSION                 2           /* on-disk format cutover: WAL-logged
                                                       * pending-list storage replaces the old
                                                       * external-file snapshot mechanism.
                                                       * No dual-path reader -- old indexes
                                                       * (version 1) must be REINDEXed. */
#define BISCUIT_METAPAGE_BLKNO          0

/*
 * BISCUIT_PAGE_FORMAT_VERSION
 *
 * Guards the *binary layout* of the individual page structs below
 * (BiscuitBlobChunkHeader, BiscuitPendingPageHeader, BiscuitPendingRecord,
 * BiscuitDirPageHeader, BiscuitDirEntry, BiscuitPageOpaqueData) rather than
 * the overall extension/catalog-visible format that BISCUIT_VERSION guards.
 * Kept separate so a future change that only touches one page struct's
 * layout (e.g. widening BiscuitPendingRecord) doesn't have to be bundled
 * with an unrelated BISCUIT_VERSION bump, and so page-level tools
 * (pg_filedump-style inspection, amcheck) can validate a page in isolation
 * against this field without needing to know anything about the rest of
 * the extension's format.
 */
#define BISCUIT_PAGE_FORMAT_VERSION     1

/*
 * BISCUIT_MAX_DIR_COLUMNS
 *
 * Upper bound on the number of per-column directory chain roots the
 * metapage can hold directly (BiscuitMetaPageData.dir_roots[]). Chosen
 * generously above any realistic multi-column index width; the
 * single-column (legacy) case always uses slot 0. This is a metapage
 * sizing constant only -- it does not bound how many *entries* a given
 * column's directory chain can hold, since each chain grows across as
 * many BISCUIT_PAGE_DIR pages as needed.
 */
#define BISCUIT_MAX_DIR_COLUMNS         32

/*
 * BISCUIT_DEFAULT_PENDING_LIST_LIMIT
 *
 * Default value for BiscuitMetaPageData.pending_list_limit: the
 * per-structure pending-chain byte size that triggers an opportunistic
 * drain (design doc §3). Mirrors gin_pending_list_limit but scoped much
 * smaller, since Biscuit has thousands of independent small structures
 * rather than one shared index-wide list.
 */
#define BISCUIT_DEFAULT_PENDING_LIST_LIMIT   (64 * 1024)
#define CHAR_RANGE                      256
#define TOMBSTONE_CLEANUP_THRESHOLD     1000
#define RADIX_SORT_THRESHOLD            5000
#define BISCUIT_LIBRARY_VERSION         "2.6.0 - Testing"

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

/*
 * Disk meta-page.
 *
 * BISCUIT_VERSION 2: this is a clean format cutover (see design doc) --
 * the old `root` field (dead weight, never pointed at anything under the
 * legacy file-snapshot design) is dropped outright rather than kept
 * unused for compatibility, since old (version-1) indexes must be
 * REINDEXed anyway and there is no dual-path reader.
 */
typedef struct BiscuitMetaPageData {
    uint32 magic;
    uint32 version;              /* extension/catalog format version,
                                   * BISCUIT_VERSION */
    uint32 page_format_version;  /* binary page-struct layout version,
                                   * BISCUIT_PAGE_FORMAT_VERSION -- see its
                                   * comment above for why this is tracked
                                   * separately from `version` */
    uint32 num_records;

    /*
     * Monotonic generation counter.  Bumped in lockstep with idx->gen on
     * every durable mutation.  See the comment on idx->gen in the
     * BiscuitIndex struct for the (intentionally non-transactional)
     * semantics.
     */
    uint64 gen;

    /* ---------------- Directory: per-column chain roots ----------------
     *
     * Each column's directory (a chain of BISCUIT_PAGE_DIR pages, each
     * holding a packed BiscuitDirEntry[] -- see BiscuitDirPageHeader
     * below) is rooted independently, one entry per indexed column, so
     * that concurrent build/scan/drain activity against different
     * columns never has to walk or lock a shared, index-wide directory
     * chain to find its own entries. The single-column (legacy) case
     * always uses dir_roots[0].
     */
    int32       num_dir_columns;                     /* number of populated
                                                        * entries in dir_roots[]
                                                        * below (1 for the
                                                        * legacy single-column
                                                        * case) */
    BlockNumber dir_roots[BISCUIT_MAX_DIR_COLUMNS];   /* dir_roots[col] ==
                                                        * InvalidBlockNumber
                                                        * until that column's
                                                        * directory chain has
                                                        * been allocated */

    /* ---------------- FSM bootstrap ----------------
     *
     * Biscuit pages retired by a drain are not immediately handed back to
     * the ordinary Postgres index FSM (storage/indexfsm.h): they are
     * deferred-recycle-gated on BiscuitPageOpaqueData.recycle_xid (see its
     * comment) and must not be reused until no concurrent scan could
     * still be walking them. fsm_root is the head of Biscuit's own
     * recycle_xid-gated freelist chain (plain BISCUIT_PAGE_PENDING-shaped
     * link list of retired-but-not-yet-recyclable pages, reusing
     * opaque.next for chaining); only once biscuit_vacuumcleanup()'s
     * horizon check clears a page does it get pushed to the standard
     * index FSM for actual reuse. InvalidBlockNumber until the first page
     * is ever retired -- this list is allocated lazily, not at build
     * time, since a fresh index retires nothing.
     */
    BlockNumber fsm_root;
    uint32      fsm_page_count;   /* pages currently linked into fsm_root's
                                    * chain, awaiting their recycle_xid
                                    * horizon; observability only, not
                                    * load-bearing for correctness */

    /* ---------------- Pending-list tuning / stats ----------------
     * See design doc §3 and Round 5, finding 1.
     */
    uint32 pending_list_limit;    /* per-structure byte threshold that
                                    * triggers an opportunistic drain;
                                    * GUC-overridable, defaults to
                                    * BISCUIT_DEFAULT_PENDING_LIST_LIMIT */
    uint64 total_pending_bytes;   /* approximate sum of pending_bytes across
                                    * all directory entries. NOT updated on
                                    * the append path (that would reintroduce
                                    * the cross-structure contention point
                                    * fixed in design doc Round 5) --
                                    * recomputed from scratch only by
                                    * biscuit_vacuumcleanup()'s existing full
                                    * directory walk. Stale by up to one
                                    * vacuum cycle; observability only. */
    uint64 total_drains;          /* lifetime count of drains performed
                                    * (size-threshold trigger or
                                    * vacuumcleanup's unconditional pass);
                                    * observability counter for tuning
                                    * pending_list_limit and autovacuum
                                    * cadence */

    /*
     * Reserved for future metadata. No backward-compat constraint on this
     * cutover (clean format bump), so headroom is cheap. Writers must
     * zero-fill this; readers must ignore its contents (not rely on any
     * value found here), so old readers stay forward-compatible with
     * newer writers that start using a slot here.
     */
    uint32 reserved[6];
} BiscuitMetaPageData;

typedef BiscuitMetaPageData *BiscuitMetaPage;

/* ==================== WAL-LOGGED PAGE STORAGE ====================
 *
 * Replaces biscuit_persist.c's external-file snapshot mechanism.
 * Every bitmap/CharIndex/length-array structure ("structure" below)
 * now lives entirely inside the index relation's own pages, split
 * into two chains:
 *
 *   (a) COMPACTED CHAIN  -- CRoaring's serialized bytes, chunked
 *       across ordinary data pages (a plain blob store, no
 *       structure-specific logic).
 *   (b) PENDING CHAIN    -- an append-only list of raw (TID, op)
 *       delta records not yet folded into the compacted blob,
 *       mirroring GIN's pending list design.
 *
 * See the design doc at the bottom of this section for the
 * merge/drain trigger, read-time reconciliation strategy, and
 * locking rules that go with these structs.
 */

/* ---- shared chunk/page "kind" tag, stored in the opaque area ---- */
#define BISCUIT_PAGE_BLOB     1     /* compacted-blob chunk page   */
#define BISCUIT_PAGE_PENDING  2     /* pending-delta list page     */
#define BISCUIT_PAGE_DIR      3     /* directory page              */

/*
 * BiscuitPageOpaqueData
 * Standard opaque footer (PageAddSpecial) on every non-meta Biscuit
 * page, so any page can be identified/validated in isolation (crash
 * recovery, pg_filedump-style inspection, amcheck) without consulting
 * the directory.
 */
typedef struct BiscuitPageOpaqueData
{
    BlockNumber next;      /* next page in this chain, or InvalidBlockNumber */
    uint16      page_kind; /* one of BISCUIT_PAGE_* above */
    uint16      flags;     /* page_kind-specific, see below */

    /*
     * recycle_xid
     * Set when a page is unlinked from its chain by a drain (old
     * compacted-chain pages, drained pending-chain pages). InvalidXid on
     * a live, in-chain page. A page with recycle_xid set must not be
     * handed out by the FSM to a new chain until no scan could still
     * hold a pointer to it -- i.e. until recycle_xid precedes the
     * oldest xmin any concurrent backend could still be running with,
     * the same "deferred recycle" rule GIN uses for its own deleted
     * pages and btree uses via RecentGlobalXmin-gated recycling.
     * See design doc, "Addressing review feedback", point 1.
     */
    TransactionId recycle_xid;
} BiscuitPageOpaqueData;

typedef BiscuitPageOpaqueData *BiscuitPageOpaque;

/* flags for BISCUIT_PAGE_PENDING pages */
#define BISCUIT_PENDING_FLAG_TAIL   0x0001  /* this is the current append target */

/* ---- (a) COMPACTED-BLOB CHUNK CHAIN ---- */

/*
 * BiscuitBlobChunkHeader
 * Header at the start of each page's data area in a compacted-blob
 * chunk chain. One structure's serialized roaring_bitmap_t bytes are
 * split across as many chunk pages as needed (same primitive as a
 * plain TOAST-like blob store -- no per-structure semantics here).
 *
 * total_len/total_chunks are duplicated on every chunk (not just the
 * head) purely so a reader landing mid-chain via a stale directory
 * entry can sanity-check itself; they are not required for correct
 * sequential reassembly, which relies solely on opaque.next.
 */
typedef struct BiscuitBlobChunkHeader
{
    uint32  total_len;      /* total byte length of the reassembled blob */
    uint32  total_chunks;   /* total number of chunks in this chain      */
    uint32  chunk_seq;      /* 0-based sequence number of this chunk     */
    uint32  chunk_len;      /* bytes of blob payload stored on this page */
    /* chunk_len bytes of raw CRoaring-serialized payload follow */
} BiscuitBlobChunkHeader;

/* ---- (b) PENDING-DELTA LIST CHAIN ---- */

#define BISCUIT_PENDING_OP_ADD     1
#define BISCUIT_PENDING_OP_REMOVE  2

/*
 * BiscuitPendingRecord
 * One raw delta against a structure's compacted bitmap. `value` is
 * the roaring-bitmap uint32 element -- for record-position bitmaps
 * this is the record slot index (not a raw ItemPointerData; Biscuit
 * already maps TIDs to dense uint32 slots elsewhere, so the pending
 * record reuses that same domain to stay a fixed-size 8-byte record
 * and pack densely on a page).
 */
typedef struct BiscuitPendingRecord
{
    uint32  value;   /* roaring-bitmap element (record slot) */
    uint8   op;      /* BISCUIT_PENDING_OP_ADD / _REMOVE */
    uint8   reserved[3];
} BiscuitPendingRecord;

/*
 * BiscuitPendingPageHeader
 * Header at the start of each page's data area in a pending-list
 * chain. Records are appended packed and in order; a page fills up
 * to num_records * sizeof(BiscuitPendingRecord) and a new tail page
 * is allocated when it can't fit the next record (GIN-style: no
 * in-place compaction of a pending page, only whole-chain drain).
 */
typedef struct BiscuitPendingPageHeader
{
    uint32  num_records;    /* records currently stored on this page */
    uint32  max_records;    /* capacity for this page, set at alloc  */
    /* num_records * sizeof(BiscuitPendingRecord) records follow */
} BiscuitPendingPageHeader;

/* ---- DIRECTORY ---- */

/*
 * BiscuitDirEntry
 * Whole-bitmap-granularity directory entry: (col, is_lower, kind,
 * char, position) -> this pair of chain heads. Container-level
 * (per-block-of-the-bitmap) addressing is explicitly out of scope
 * for this design -- see design doc point 5. This is a further
 * write-amplification reduction for a *later* iteration, not a fix
 * for any correctness or performance problem this design has.
 *
 * pending_count/pending_bytes are maintained incrementally on every
 * append and reset on drain, so the size-threshold check in point 3
 * is an O(1) field read rather than a chain walk.
 */
typedef struct BiscuitDirEntry
{
    /* identity -- see biscuit_pattern.c accessor naming for kind values */
    int16   col;            /* column index, or -1 for legacy single-column */
    bool    is_lower;        /* case-insensitive structure set?             */
    uint8   kind;            /* BISCUIT_DIR_KIND_* -- pos/neg/cache/len/len_ge */
    int32   ch;               /* character (unsigned char), or -1 if n/a     */
    int32   position;         /* pos/neg_offset/length value, or -1 if n/a   */

    /* compacted-blob chain */
    BlockNumber blob_head;    /* InvalidBlockNumber if structure is empty/absent */

    /* pending-delta chain */
    BlockNumber pending_head; /* InvalidBlockNumber if none ever allocated   */
    BlockNumber pending_tail; /* == pending_head if single page; cached to
                                * make append O(1) instead of a chain walk   */
    uint32      pending_count;  /* total undained records across the chain  */
    uint32      pending_bytes;  /* total undrained bytes, for the size trigger */
} BiscuitDirEntry;

#define BISCUIT_DIR_KIND_POS      1
#define BISCUIT_DIR_KIND_NEG      2
#define BISCUIT_DIR_KIND_CACHE    3
#define BISCUIT_DIR_KIND_LEN      4
#define BISCUIT_DIR_KIND_LEN_GE   5

/*
 * Additional BISCUIT_DIR_KIND_* values, added for the biscuit_persist.c
 * rewrite (Phase after the pending-list design doc). The design doc's
 * directory (§5) only ever specified `(col, is_lower, kind, ch, position)`
 * for the per-character/per-length RoaringBitmap-shaped structures
 * (POS/NEG/CACHE/LEN/LEN_GE above) -- it explicitly did not cover the rest
 * of BiscuitIndex's persistent state (the tid array, the tombstone
 * bitmap, the free-slot list, the per-record string caches), because
 * biscuit_blob.c's chunk-chain primitive is bitmap-agnostic ("just bytes
 * in, bytes out", §1) and works equally well for any of these once they're
 * serialized to a flat byte buffer -- so the same directory+blob
 * machinery is reused here rather than inventing a second, parallel
 * addressing scheme just for non-bitmap state.
 *
 *   BISCUIT_DIR_KIND_TIDS        -- idx->tids (ItemPointerData[num_records]),
 *                                    a flat array dump, no bitmap involved.
 *   BISCUIT_DIR_KIND_TOMBSTONES  -- idx->tombstones (RoaringBitmap*) --
 *                                    this one *is* bitmap-shaped, so it
 *                                    could in principle grow a pending
 *                                    chain like POS/NEG/CACHE do; not done
 *                                    in this phase since nothing yet
 *                                    appends to it incrementally (CRUD
 *                                    call-site wiring is a later phase).
 *   BISCUIT_DIR_KIND_FREELIST    -- idx->free_list (uint32_t[free_count]),
 *                                    a flat array dump.
 *   BISCUIT_DIR_KIND_STRCACHE    -- one column's data_cache or
 *                                    data_cache_lower (or, for the legacy
 *                                    single-column case, idx->data_cache /
 *                                    idx->data_cache_lower): the whole
 *                                    per-record C-string array for that
 *                                    (col, is_lower) pair, length-prefixed
 *                                    and concatenated into one blob --
 *                                    ch/position are unused (-1) since
 *                                    this isn't addressed per-character.
 *
 * ch and position are meaningless for all four of these (always -1);
 * `is_lower` is meaningless for TIDS/TOMBSTONES/FREELIST (always false)
 * since there's exactly one of each per index, not a case-sensitive/
 * case-insensitive pair.
 */
#define BISCUIT_DIR_KIND_TIDS        6
#define BISCUIT_DIR_KIND_TOMBSTONES  7
#define BISCUIT_DIR_KIND_FREELIST    8
#define BISCUIT_DIR_KIND_STRCACHE    9

/*
 * BISCUIT_DIR_KIND_HEADER -- one more addition, for biscuit_persist.c's
 * rewrite: a single raw blob (col=BISCUIT_DIR_COL_SINGLETON) holding the
 * scalar bookkeeping fields that don't belong to any one structure
 * (capacity, max_len, max_length_legacy/_lower, insert/update/delete/
 * tombstone counts, num_columns, and, for multi-column indexes,
 * column_types[]/per-column max_length/max_length_lower). num_records and
 * gen are deliberately NOT duplicated here -- they already live in
 * BiscuitMetaPageData and stay authoritative there (see
 * biscuit_write_metadata_to_disk()/biscuit_read_metadata_from_disk() in
 * biscuit_index.c).
 */
#define BISCUIT_DIR_KIND_HEADER      10

/*
 * BiscuitDirEntry.col sentinels, beyond real column indices (0..N-1):
 *
 *   BISCUIT_DIR_COL_LEGACY    (-1) -- already used by the design doc for
 *                                     the single-column (legacy) field
 *                                     set's POS/NEG/CACHE/LEN/LEN_GE and,
 *                                     as of this phase, its STRCACHE too.
 *   BISCUIT_DIR_COL_SINGLETON (-2) -- new in this phase: TIDS/TOMBSTONES/
 *                                     FREELIST, which exist exactly once
 *                                     per index regardless of column
 *                                     count, so they don't belong to any
 *                                     particular column's chain. Always
 *                                     stored in the array slot 0 chain
 *                                     (see biscuit_dir.c's
 *                                     biscuit_dir_slot_for_col()) alongside
 *                                     whatever column-0 or legacy entries
 *                                     also live there -- entries are
 *                                     disambiguated by their own `col`
 *                                     tag during lookup, not by which
 *                                     chain they happen to be linked
 *                                     into, so sharing a chain is safe.
 */
#define BISCUIT_DIR_COL_LEGACY     (-1)
#define BISCUIT_DIR_COL_SINGLETON  (-2)

/*
 * BiscuitDirPageHeader
 * Header at the start of each page's data area in a per-column
 * directory chain (BISCUIT_PAGE_DIR, one chain per BiscuitMetaPageData
 * .dir_roots[col] -- see metapage comment). Entries are packed and
 * appended in the order their structures are first referenced during
 * build/insert; a page fills up to num_entries * sizeof(BiscuitDirEntry)
 * and a new tail page is linked via opaque.next when it can't fit the
 * next entry. Unlike the pending-list chain, directory entries are
 * mutated in place (pending_count/pending_bytes/blob_head are updated on
 * their existing entry, not re-appended), so a directory page never
 * shrinks and entries are never relocated once written -- an entry's
 * (page, offset) is stable for the life of the structure it describes.
 */
typedef struct BiscuitDirPageHeader
{
    uint32  num_entries;    /* entries currently stored on this page */
    uint32  max_entries;    /* capacity for this page, set at alloc  */
    /* num_entries * sizeof(BiscuitDirEntry) entries follow */
} BiscuitDirPageHeader;

/*
 * Page-capacity helpers.
 *
 * All three chained page types share the same page layout: standard
 * PostgreSQL page header, a variable-length array of fixed-size records
 * immediately following a small fixed header, and a BiscuitPageOpaqueData
 * special area (PageAddSpecial'd, per BISCUIT_PAGE_* kind) at the end.
 * These macros compute how many records of each kind fit on a page of a
 * given size, for use when a chain allocates a new page and needs to set
 * max_records/max_entries in that page's header.
 */
#define BiscuitPendingPageMaxRecords(pagesize) \
    (((pagesize) - MAXALIGN(SizeOfPageHeaderData) \
                  - MAXALIGN(sizeof(BiscuitPendingPageHeader)) \
                  - MAXALIGN(sizeof(BiscuitPageOpaqueData))) \
     / sizeof(BiscuitPendingRecord))

#define BiscuitDirPageMaxEntries(pagesize) \
    (((pagesize) - MAXALIGN(SizeOfPageHeaderData) \
                  - MAXALIGN(sizeof(BiscuitDirPageHeader)) \
                  - MAXALIGN(sizeof(BiscuitPageOpaqueData))) \
     / sizeof(BiscuitDirEntry))

/*
 * Usable payload bytes for one BiscuitBlobChunkHeader page (the
 * compacted-blob chain has no separate "max records" notion since each
 * chunk simply stores as many raw serialized bytes as fit).
 */
#define BiscuitBlobChunkMaxPayload(pagesize) \
    ((pagesize) - MAXALIGN(SizeOfPageHeaderData) \
                - MAXALIGN(sizeof(BiscuitBlobChunkHeader)) \
                - MAXALIGN(sizeof(BiscuitPageOpaqueData)))

/* ---- METAPAGE EXTENSION ----
 *
 * Implemented above (BiscuitMetaPageData): this is a clean format cutover
 * (BISCUIT_VERSION bumped to 2, REINDEX required, no dual-path reader --
 * see design doc), so the old `root` field and the old reserved[4] slots
 * were dropped/promoted outright rather than kept behind a version check.
 *
 * One deliberate deviation from the design doc's original single
 * `dir_root`: the metapage now holds one directory-chain root per
 * indexed column (`dir_roots[BISCUIT_MAX_DIR_COLUMNS]`,
 * `num_dir_columns`) instead of a single shared root with `col` as just
 * another field inside each BiscuitDirEntry. This keeps each column's
 * directory chain, and therefore its locking (§6) and drain traffic,
 * fully independent at the chain-root level, not just at the
 * individual-entry level -- a multi-column index's columns never have to
 * walk or contend on a shared root page to find their own entries. The
 * per-entry `col` field on BiscuitDirEntry is kept regardless, since a
 * single column's chain can still hold entries for multiple `kind`s and
 * multiple case-mode sets and having the field is cheap self-description
 * for page-level tooling; it should just always agree with the chain's
 * dir_roots[] index it was reached through.
 *
 * FSM bootstrap (`fsm_root`, `fsm_page_count`) is new relative to the
 * design doc's text but implements the deferred-recycle mechanism the
 * doc already specifies (Addressing review feedback, point 1): a page
 * with recycle_xid set is not immediately FSM-eligible, so something has
 * to hold onto "pages retired but not yet recyclable" until
 * biscuit_vacuumcleanup()'s horizon check clears them for the ordinary
 * index FSM. fsm_root is that holding chain's head.
 *
 * `page_format_version` is new relative to the design doc's text too --
 * see its own comment above (BISCUIT_PAGE_FORMAT_VERSION) for why it's
 * tracked separately from `version`.
 *
 * total_pending_bytes is deliberately NOT maintained synchronously on the
 * append path (an earlier version of the design doc described it as
 * bumped on every append "same spirit as BiscuitIndex.gen" -- that was
 * wrong, and was corrected; see design doc Round 5, finding 1, for why a
 * per-append write to a single shared metapage field is a global
 * serialization point that contradicts §6's no-cross-structure-contention
 * guarantee). The only synchronously-maintained pending-byte counters are
 * the per-structure `BiscuitDirEntry.pending_bytes` fields, which is also
 * all the §3 size-threshold trigger actually needs -- it was never
 * necessary for the append path to touch anything index-wide.
 *
 * Scope note: this header only defines the metapage layout and the page
 * structs it points at (BiscuitDirPageHeader alongside the existing
 * BiscuitBlobChunkHeader/BiscuitPendingPageHeader). Directory
 * lookup/insert, compacted-blob chunk read/write, and pending-list
 * append/drain logic are all still unimplemented -- see biscuit_index.c's
 * biscuit_write_metadata_to_disk()/biscuit_read_metadata_from_disk() for
 * the metapage read/write that *is* wired up in this phase, and the
 * design doc for what's still pending.
 */

/* ==================== DESIGN NOTES (see accompanying doc) ==================== */

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
     * Per-column case-mode gating (BISCUIT_MODE_LIKE / BISCUIT_MODE_ILIKE
     * / BISCUIT_MODE_BOTH), one entry per column, indexed in lockstep with
     * column_indices[]. Derived from each column's opclass at build/load
     * time via biscuit_get_column_case_mode() -- see biscuit_index.c.
     * Only allocated when num_columns > 1; NULL otherwise (the
     * single-column/legacy case uses legacy_case_mode below instead).
     */
    uint8 *column_case_mode;

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

    /*
     * Case-mode gating for the single-column (legacy) fields above,
     * mirroring column_case_mode[] for the multi-column case. Derived
     * from the sole column's opclass via biscuit_get_column_case_mode().
     */
    uint8 legacy_case_mode;

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
