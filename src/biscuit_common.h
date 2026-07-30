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
#define BISCUIT_VERSION                 3           /* on-disk format cutover:
                                                       * the old best-effort/
                                                       * proc-exit-flush
                                                       * snapshot machinery
                                                       * (BISCUIT_SNAPSHOT_GEN_THRESHOLD,
                                                       * biscuit_cache.c's
                                                       * proc-exit save) is
                                                       * deleted outright, and
                                                       * biscuit_persist_save()/
                                                       * biscuit_persist_load()
                                                       * failures now
                                                       * propagate instead of
                                                       * being swallowed.
                                                       * No dual-path reader --
                                                       * version-1 (old
                                                       * external-file) and
                                                       * version-2 indexes
                                                       * must be REINDEXed.
                                                       *
                                                       * DELIBERATELY NOT
                                                       * bumped for the
                                                       * row-identity in-place
                                                       * storage rewrite
                                                       * (biscuit_rowstore.c),
                                                       * which changes the
                                                       * on-disk layout of
                                                       * TIDS/STRCACHE/HEADER
                                                       * incompatibly: version 3
                                                       * (BISCUIT_LIBRARY_VERSION
                                                       * "3.0.0 - Player") was
                                                       * never shipped, so there
                                                       * is no deployed
                                                       * version-3 index
                                                       * anywhere for a bump to
                                                       * distinguish this format
                                                       * from. That is also
                                                       * exactly why this
                                                       * rewrite carries no
                                                       * backward-compatibility
                                                       * obligation and needs no
                                                       * dual-format reader --
                                                       * the only readers that
                                                       * ever existed for the
                                                       * old TIDS/STRCACHE blob
                                                       * layout are in this same
                                                       * unreleased tree, and
                                                       * they are replaced, not
                                                       * kept alongside. Any
                                                       * index built from an
                                                       * intermediate
                                                       * development checkout
                                                       * must be REINDEXed, same
                                                       * as always. */
#define BISCUIT_METAPAGE_BLKNO          0

/*
 * BISCUIT_PAGE_FORMAT_VERSION
 *
 * Guards the *binary layout* of the individual page structs below
 * (BiscuitBlobChunkHeader, BiscuitPendLogPageHeader, BiscuitPendLogRecord,
 * BiscuitDirPageHeader, BiscuitDirEntry, BiscuitPageOpaqueData) rather than
 * the overall extension/catalog-visible format that BISCUIT_VERSION guards.
 * Kept separate so a future change that only touches one page struct's
 * layout (e.g. widening BiscuitPendLogRecord) doesn't have to be bundled
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
#define BISCUIT_LIBRARY_VERSION         "3.0.0 - Cookie"

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
     * recycle_xid-gated freelist chain (a plain singly-linked
     * list of retired-but-not-yet-recyclable pages, reusing
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

    /* ---------------- Shared pending log (biscuit_pendlog.c) ----------
     *
     * The single index-wide append-only log that replaced the old
     * per-structure pending chains. Every steady-state bitmap mutation
     * now appends one self-describing BiscuitPendLogRecord here instead
     * of appending to (and updating the directory entry of) its own
     * structure's private chain.
     *
     * Why: a row touching K structures used to dirty ~K distinct pages,
     * and PostgreSQL charges a full-page image per distinct page per
     * checkpoint interval. Measured on one insert of a 9-character
     * string: 82 pages, 84 FPIs, 60KB of WAL. With one shared log those
     * K records (20 bytes each) land on the *same* tail page, so the
     * page count -- and therefore the FPI bill -- stops scaling with the
     * number of structures a row touches.
     *
     * BiscuitDirEntry's per-structure pending fields were consequently
     * removed outright: a bitmap-kind entry now carries only blob_head.
     * See BiscuitDirEntry's per-kind field table below.
     */
    BlockNumber pendlog_head;     /* first page of the shared log chain,
                                    * InvalidBlockNumber when the log is
                                    * empty (nothing appended since the
                                    * last drain) */
    BlockNumber pendlog_tail;     /* current append target; the O(1) tail
                                    * pointer, same role the old
                                    * per-structure tail pointer served */
    uint32      pendlog_npages;   /* pages currently in the log chain.
                                    *
                                    * Deliberately a PAGE count, not a record
                                    * count: it is only touched when the log
                                    * grows a page, so the metapage stays out
                                    * of the per-append WAL record entirely.
                                    * An earlier version kept an exact record
                                    * count here and paid for it twice --
                                    * every append carried the metapage into
                                    * its WAL record (doubling pages-per-append,
                                    * the very quantity this design minimizes)
                                    * and every writer serialized on one
                                    * exclusive metapage lock.
                                    *
                                    * npages * BLCKSZ is the log's size for
                                    * drain-trigger purposes, which is all the
                                    * trigger ever needed. Exact record counts
                                    * are available per page in
                                    * BiscuitPendLogPageHeader.num_records for
                                    * anything that genuinely needs them. */

    BlockNumber pendlog_draining;  /* Head of a chain that has been detached
                                     * for draining but whose merge has not
                                     * completed. InvalidBlockNumber
                                     * normally.
                                     *
                                     * Exists because a drain detaches the
                                     * log from pendlog_head *before*
                                     * merging (so appenders never block and
                                     * can never lose a record into a chain
                                     * being drained), which leaves a window
                                     * where the only reference to those
                                     * records is the drainer's local
                                     * variable. GenericXLog page changes
                                     * survive transaction abort, so an
                                     * ereport(ERROR) mid-merge -- or a
                                     * crash -- would otherwise strand every
                                     * not-yet-merged delta permanently.
                                     *
                                     * Recovery is replay: the next drain
                                     * ingests this chain before the live
                                     * one. Re-applying deltas that were
                                     * already folded into a blob is safe
                                     * because the last op per rec_idx wins,
                                     * so replaying a structure's full
                                     * sequence lands on the same state
                                     * whether or not part of it was already
                                     * applied. */

    /*
     * Reserved for future metadata. No backward-compat constraint on this
     * cutover (clean format bump), so headroom is cheap. Writers must
     * zero-fill this; readers must ignore its contents (not rely on any
     * value found here), so old readers stay forward-compatible with
     * newer writers that start using a slot here.
     */
    uint32 reserved[5];
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
/*
 * Kind 2 is RETIRED: it tagged the old per-structure pending-delta pages,
 * a format nothing writes or reads any more (replaced by
 * BISCUIT_PAGE_PENDLOG). Deliberately left defined-but-unused rather than
 * deleted, and deliberately not recycled for a new page kind, so that any
 * page still carrying this tag in a development-era relation file is
 * recognizable as stale rather than being silently misread as whatever
 * structure claimed the number next.
 */
#define BISCUIT_PAGE_PENDING_RETIRED  2
#define BISCUIT_PAGE_DIR      3     /* directory page              */

/*
 * ---- row-identity in-place storage (biscuit_rowstore.c) ----
 *
 * Added for the TIDS/STRCACHE/HEADER in-place rewrite (biscuit_rowstore.c;
 * no BISCUIT_VERSION bump -- version 3 was never shipped, see that
 * constant's comment): unlike the compacted-blob/pending-list chains
 * above, these are mutated with single-page in-place GenericXLog writes,
 * not rewritten wholesale on every commit. See biscuit_rowstore.c's file
 * header for the full design.
 *
 *   BISCUIT_PAGE_PAGEDIR  -- append-only chain mapping a dense logical
 *                            page number (0, 1, 2, ...) to the physical
 *                            BlockNumber holding that logical page's fixed
 *                            slot array. Shared by both TIDS (slot =
 *                            ItemPointerData[]) and STRCACHE's pointer
 *                            array (slot = BiscuitStrPtr[]); entries are
 *                            appended once, when a new logical page is
 *                            first allocated, and never change afterward,
 *                            so (unlike BISCUIT_PAGE_DIR) there is no
 *                            in-place update case at all here.
 *   BISCUIT_PAGE_TIDSLOT  -- fixed ItemPointerData[BiscuitTidSlotsPerPage]
 *                            array, one slot per record index landing in
 *                            this logical page's range. No header: the
 *                            slot array starts right after the standard
 *                            page header.
 *   BISCUIT_PAGE_STRPTR   -- fixed BiscuitStrPtr[BiscuitStrPtrSlotsPerPage]
 *                            array, same addressing shape as TIDSLOT.
 *   BISCUIT_PAGE_STRHEAP  -- append-only, bump-allocated chain of raw
 *                            string bytes that BiscuitStrPtr entries point
 *                            into. A write either fits in the remaining
 *                            space of the current tail page (in-place
 *                            append, pointer written elsewhere) or spills
 *                            to a fresh tail page -- same tail-page
 *                            pattern as biscuit_pendlog_append(). Bytes
 *                            already written are never moved or reclaimed
 *                            in this phase (freelist-slot reuse orphans
 *                            the old bytes; see biscuit_rowstore.c's file
 *                            header for the deferred compaction note).
 *   BISCUIT_PAGE_HEADER   -- single fixed page (never chained) holding the
 *                            HEADER scalar-bookkeeping blob, overwritten
 *                            in place every commit instead of being
 *                            reallocated.
 */
/*
 * BISCUIT_PAGE_PENDLOG -- a page of the single index-wide shared pending
 * log (biscuit_pendlog.c). Layout is BiscuitPendLogPageHeader followed by
 * a packed BiscuitPendLogRecord[] array; chained via opaque.next, with the
 * tail marked BISCUIT_PENDING_FLAG_TAIL.
 *
 * This replaced the per-structure pending page (retired kind 2, see
 * BISCUIT_PAGE_PENDING_RETIRED above); nothing writes or reads that
 * format any more.
 */
#define BISCUIT_PAGE_PENDLOG  9     /* shared index-wide pending log page  */

/*
 * BISCUIT_PENDLOG_DRAIN_PAGES
 *
 * Log size (in pages) at which the append path drains opportunistically.
 * VACUUM drains unconditionally regardless of this.
 *
 * Sized for an OLAP-first workload: a drain costs O(index size) because it
 * rewrites every touched structure's compacted blob, so frequent draining
 * makes bulk loading quadratic. Letting the log grow to a few MB keeps
 * drains rare during a load while bounding what a cold reader has to
 * materialize into a snapshot before its first query (and bounding that
 * snapshot's memory). 512 pages = 4MB at the default BLCKSZ, which is the
 * same order as GIN's gin_pending_list_limit default, arrived at for the
 * same reasons.
 */
#define BISCUIT_PENDLOG_DRAIN_PAGES  512

#define BISCUIT_PAGE_PAGEDIR  4     /* logical-page -> BlockNumber directory */
#define BISCUIT_PAGE_TIDSLOT  5     /* fixed ItemPointerData[] slot page     */
#define BISCUIT_PAGE_STRPTR   6     /* fixed BiscuitStrPtr[] slot page       */
#define BISCUIT_PAGE_STRHEAP  7     /* append-only string value-heap page    */
#define BISCUIT_PAGE_HEADER   8     /* single in-place HEADER blob page      */

/*
 * BISCUIT_ROWSTORE_LOCK_BLKNO
 *
 * Lock tag for the per-index row-identity allocation lock, taken as
 * LockPage(index, BISCUIT_ROWSTORE_LOCK_BLKNO, ExclusiveLock) -- see
 * biscuit_persist.c's biscuit_rowstore_alloc_lock().
 *
 * Deliberately a block number that can never name a real page, because
 * this is a pure lock tag and not a page reference. It must also differ
 * from BISCUIT_METAPAGE_BLKNO, which biscuit_pendlog_drain_all() already
 * uses as its own heavyweight serialization tag: coupling row-identity
 * writes to the drain lock would make every INSERT queue behind an
 * O(index size) merge, which is precisely the thing that drain's
 * ConditionalLockPage(wait = false) path exists to avoid.
 *
 * Why the lock exists at all: writing a row's identity is a
 * read-modify-write of shared directory-entry state
 * (biscuit_dir_find() -> allocate pages -> biscuit_dir_update()), and
 * every step of it raced. Two backends could both miss the same
 * directory entry and both insert it (biscuit_dir_insert() does not
 * check for duplicates -- by design, callers were supposed to do the
 * find-then-act split under something); both allocate a page-directory
 * root and orphan one of them; both allocate the same logical slot page
 * and hit the dense-in-order check; or both propagate a stale
 * strheap_tail back into the entry and rewind the value heap's tail
 * pointer. Serializing the whole sequence per index fixes all four at
 * once, and does so where it costs least: allocation-shaped work already
 * serializes on the shared pendlog tail page for the bulk of an insert's
 * cost, so this adds queueing to an already-queued path rather than
 * introducing a new bottleneck.
 */
#define BISCUIT_ROWSTORE_LOCK_BLKNO  ((BlockNumber) 0xFFFFFFF0)

/*
 * BiscuitSlotWriteMode
 *
 * Expected prior state of a row-identity slot, threaded from the CRUD
 * call site (biscuit_index.c) down through biscuit_persist.c into
 * biscuit_rowstore_tid_write()'s in-place branch, where it becomes an
 * assertion about what that slot must currently contain on disk.
 *
 * This exists as defense-in-depth for the slot-allocation race fixed in
 * biscuit_index.c's biscuit_claim_new_slot(). Slot numbers used to be
 * handed out from a process-local counter, so two backends routinely
 * computed the same slot_idx for different rows; the TIDSLOT page's
 * buffer lock serialized the two writes without preventing the
 * collision, and the second writer silently clobbered the first row.
 * biscuit_pagedir_append()'s dense-in-order check was the only guard in
 * this area and it only fires when a whole new *logical page* is
 * allocated, which is why the collision showed up as a loud error in a
 * minority of runs and as silent, total row loss in all of them.
 *
 * With a mode passed down, a fresh claim landing on an already-occupied
 * slot raises an ERROR at the moment of the overwrite instead. The point
 * is not to catch today's bug -- that is fixed at the allocator -- but to
 * make any future regression in slot-allocation atomicity fail loudly and
 * immediately rather than corrupting data invisibly.
 */
typedef enum BiscuitSlotWriteMode
{
    /*
     * The caller just claimed this slot number and believes nothing has
     * ever occupied it. The TIDS slot on disk must therefore be an invalid
     * ItemPointer (a freshly PageInit'd TIDSLOT page is all zeroes, and
     * ItemPointerIsValid() is false for a zeroed pointer). Anything else
     * means two writers were handed the same slot.
     */
    BISCUIT_SLOT_WRITE_FRESH = 0,

    /*
     * The caller already owns this slot and is rewriting it: the UPDATE
     * path in biscuit_insert() (duplicate TID found), the delete-clear
     * loop in biscuit_bulkdelete(), and biscuit_persist_save()'s
     * whole-snapshot bulk rewrite. Whatever the slot currently holds is
     * expected, so no check is performed.
     */
    BISCUIT_SLOT_WRITE_INPLACE = 1
} BiscuitSlotWriteMode;

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

/* flags for tail-page-tracking chains (PENDLOG, STRHEAP) */
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

/* ---- (b) PENDING DELTAS ----
 *
 * There is no per-structure pending chain any more, and no
 * BISCUIT_PAGE_PENDING page format. Both were replaced by the single
 * index-wide append-only log in biscuit_pendlog.c: see
 * BiscuitPendLogRecord / BiscuitPendLogPageHeader below and
 * BiscuitMetaPageData.pendlog_* above. BISCUIT_PENDING_OP_ADD/_REMOVE
 * survive because the shared log's records still use them.
 */

#define BISCUIT_PENDING_OP_ADD     1
#define BISCUIT_PENDING_OP_REMOVE  2

/* ---- DIRECTORY ---- */

/*
 * BiscuitDirEntry
 * Whole-structure-granularity directory entry: (col, is_lower, kind,
 * ch, position) -> the chain head(s) that structure owns. Container-level
 * (per-block-of-the-bitmap) addressing is explicitly out of scope
 * for this design -- see design doc point 5. This is a further
 * write-amplification reduction for a *later* iteration, not a fix
 * for any correctness or performance problem this design has.
 *
 * Per-kind field meanings
 * -----------------------
 * blob_head is the only field every kind uses, but what it heads differs:
 *
 *   POS/NEG/CACHE/LEN/LEN_GE/TOMBSTONES (the bitmap kinds)
 *     blob_head     -- compacted-blob chunk chain (biscuit_blob.c), i.e.
 *                      the structure's serialized RoaringBitmap.
 *     strheap_*     -- unused, always InvalidBlockNumber.
 *
 *     These kinds have NO per-structure pending chain. Undrained deltas
 *     live in the index-wide shared log (biscuit_pendlog.c) and are keyed
 *     by structure identity there, so nothing per-entry tracks them. The
 *     old pending_head/pending_tail/pending_count/pending_bytes fields
 *     that used to serve that purpose are gone; see the note below.
 *
 *   FREELIST
 *     blob_head     -- compacted-blob chain holding a flat uint32_t[] dump.
 *     strheap_*     -- unused.
 *
 *   TIDS
 *     blob_head     -- root of the TIDS logical-page directory chain
 *                      (BISCUIT_PAGE_PAGEDIR), mapping
 *                      slot_idx / BiscuitTidSlotsPerPage to a
 *                      BISCUIT_PAGE_TIDSLOT block. InvalidBlockNumber
 *                      until the first TID is ever written.
 *     strheap_*     -- unused.
 *
 *   STRCACHE (one entry per (col, is_lower))
 *     blob_head     -- root of the pointer-array logical-page directory
 *                      chain (BISCUIT_PAGE_PAGEDIR) over
 *                      BISCUIT_PAGE_STRPTR pages of BiscuitStrPtr[].
 *     strheap_head  -- head of this column's append-only value-heap chain
 *                      (BISCUIT_PAGE_STRHEAP).
 *     strheap_tail  -- current value-heap tail (bump-allocation target).
 *
 *   HEADER
 *     blob_head     -- the single BISCUIT_PAGE_HEADER block, overwritten
 *                      in place every save (never chained, never
 *                      reallocated once the first write creates it).
 *     strheap_*     -- unused.
 *
 * Why strheap_head/strheap_tail rather than reused pending fields
 * ---------------------------------------------------------------
 * These two fields used to be pending_head/pending_tail, silently
 * reinterpreted as the STRCACHE value heap for that one kind while
 * meaning "pending-delta chain" for the bitmap kinds. That was a
 * comment-level rename only, and it was a live hazard: biscuit_persist.c's
 * drop walk branches on kind to decide how to free them, and one missing
 * or mis-ordered case would have freed a STRHEAP chain as if it were a
 * pending chain (or walked a pending chain as a value heap). With the
 * bitmap kinds' pending chains gone entirely there is no longer anything
 * to share these fields *with*, so they are named for their only real
 * user. Nothing forced the old shape to be preserved -- no format is
 * shipped -- so the honest names win.
 *
 * pending_count/pending_bytes are also gone: they existed for the
 * per-structure size-threshold drain trigger, and the shared log's
 * trigger is a page count on the metapage (pendlog_npages) instead.
 * Dropping all four fields shrinks the entry from 32 to 24 bytes, which
 * directly raises BiscuitDirPageMaxEntries().
 *
 * ALWAYS initialize with BiscuitDirEntryInit() rather than
 * memset()-then-assign: a zeroed BlockNumber is block 0 (the metapage),
 * NOT InvalidBlockNumber, so a field left at its memset value is a
 * pointer at the metapage that teardown code will happily try to free.
 */
typedef struct BiscuitDirEntry
{
    /* identity -- see biscuit_pattern.c accessor naming for kind values */
    int16   col;            /* column index, or -1 for legacy single-column */
    bool    is_lower;        /* case-insensitive structure set?             */
    uint8   kind;            /* BISCUIT_DIR_KIND_*                          */
    int32   ch;               /* character (unsigned char), or -1 if n/a     */
    int32   position;         /* pos/neg_offset/length value, or -1 if n/a   */

    /* primary chain head -- meaning is kind-dependent, see above */
    BlockNumber blob_head;    /* InvalidBlockNumber if structure is empty/absent */

    /* STRCACHE value heap only; InvalidBlockNumber for every other kind */
    BlockNumber strheap_head;
    BlockNumber strheap_tail;
} BiscuitDirEntry;

/*
 * BiscuitDirEntryInit
 *
 * Set an entry's identity and put every chain head at InvalidBlockNumber.
 * This exists because the natural-looking memset(&e, 0, sizeof e) leaves
 * all three BlockNumbers pointing at block 0 -- the metapage -- and every
 * call site then had to remember to overwrite each one. Callers set
 * whichever chain heads they actually own after calling this.
 */
static inline void
BiscuitDirEntryInit(BiscuitDirEntry *e, int32 col, bool is_lower,
                     uint8 kind, int32 ch, int32 position)
{
    memset(e, 0, sizeof(*e));
    e->col          = (int16) col;
    e->is_lower     = is_lower;
    e->kind         = kind;
    e->ch           = ch;
    e->position     = position;
    e->blob_head    = InvalidBlockNumber;
    e->strheap_head = InvalidBlockNumber;
    e->strheap_tail = InvalidBlockNumber;
}

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
 * next entry. Unlike the shared log, directory entries are
 * mutated in place (blob_head/strheap_* are updated on
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

/* ==================== SHARED PENDING LOG ====================
 * See BISCUIT_PAGE_PENDLOG above, BiscuitMetaPageData.pendlog_* and
 * biscuit_pendlog.c for the full design.
 */

/*
 * BiscuitPendLogRecord
 *
 * One delta against one structure's compacted bitmap, in the shared log.
 *
 * The difference from the retired per-structure pending record is that
 * this record is *self-describing*: it carries the full structure identity
 * (col, is_lower, kind, ch, position) that the old per-structure chain
 * conveyed implicitly by which chain the record was sitting in. That
 * costs 12 extra bytes per record (20 vs 8) and buys the collapse from
 * K pages per row down to 1 -- a trade that is overwhelmingly worth it,
 * because page count drives full-page-image volume while record width
 * only drives the (already cheap) delta.
 *
 * rec_idx is the dense record slot index, the same uint32 domain the
 * retired per-structure pending record's `value` field used.
 */
typedef struct BiscuitPendLogRecord
{
    int16   col;            /* BiscuitDirEntry.col, incl. the sentinels */
    uint8   is_lower;
    uint8   kind;           /* BISCUIT_DIR_KIND_* */
    int32   ch;
    int32   position;
    uint32  rec_idx;        /* roaring element to add/remove */
    uint8   op;             /* BISCUIT_PENDING_OP_ADD / _REMOVE */
    uint8   reserved[3];    /* zero-filled by writers, ignored by readers */
} BiscuitPendLogRecord;

typedef struct BiscuitPendLogPageHeader
{
    uint32  num_records;
    uint32  max_records;
} BiscuitPendLogPageHeader;

#define BiscuitPendLogMaxRecords(pagesize) \
    (((pagesize) - MAXALIGN(SizeOfPageHeaderData) \
                  - MAXALIGN(sizeof(BiscuitPendLogPageHeader)) \
                  - MAXALIGN(sizeof(BiscuitPageOpaqueData))) \
     / sizeof(BiscuitPendLogRecord))

#define BiscuitPendLogUsedBytes(n) \
    (MAXALIGN(sizeof(BiscuitPendLogPageHeader)) + (Size)(n) * sizeof(BiscuitPendLogRecord))

/* ==================== ROW-IDENTITY IN-PLACE STORAGE ====================
 * See BISCUIT_PAGE_PAGEDIR/_TIDSLOT/_STRPTR/_STRHEAP/_HEADER above and
 * biscuit_rowstore.c for the full design.
 */

/*
 * BiscuitPageDirHeader
 * Header for a BISCUIT_PAGE_PAGEDIR page: a packed, append-only
 * BlockNumber[] array mapping a contiguous run of logical page numbers to
 * physical blocks. Logical page L lives on pagedir chain page
 * (L / max_entries), at offset (L % max_entries) once the right chain
 * page is reached by walking opaque.next -- entries are only ever
 * appended (when a new logical TIDSLOT/STRPTR page is first allocated),
 * never updated, so unlike BISCUIT_PAGE_DIR there is no in-place-update
 * case here at all.
 */
typedef struct BiscuitPageDirHeader
{
    uint32  num_entries;    /* entries currently stored on this page */
    uint32  max_entries;    /* capacity for this page, set at alloc  */
    /* num_entries * sizeof(BlockNumber) entries follow */
} BiscuitPageDirHeader;

#define BiscuitPageDirMaxEntries(pagesize) \
    (((pagesize) - MAXALIGN(SizeOfPageHeaderData) \
                  - MAXALIGN(sizeof(BiscuitPageDirHeader)) \
                  - MAXALIGN(sizeof(BiscuitPageOpaqueData))) \
     / sizeof(BlockNumber))

/*
 * A BISCUIT_PAGE_TIDSLOT page has no header at all: it is simply
 * ItemPointerData[BiscuitTidSlotsPerPage] starting right after the
 * standard page header, ending before the BiscuitPageOpaqueData special
 * area. An all-zero (never-written) slot decodes as an invalid
 * ItemPointer (ip_posid == 0), which is exactly PageInit()'s zero-filled
 * starting state, so unwritten slots need no explicit initialization.
 */
#define BiscuitTidSlotsPerPage(pagesize) \
    (((pagesize) - MAXALIGN(SizeOfPageHeaderData) \
                  - MAXALIGN(sizeof(BiscuitPageOpaqueData))) \
     / sizeof(ItemPointerData))

/*
 * BiscuitStrPtr
 * One STRCACHE pointer-array slot: where in the value heap (or, for an
 * oversized value, which dedicated blob chain) the string for this record
 * slot lives.
 *
 *   blkno == InvalidBlockNumber              -> NULL (absent) string.
 *   offset == BISCUIT_STRPTR_OVERSIZE_SENTINEL
 *                                              -> blkno is a
 *                                                 biscuit_page_write_blob()
 *                                                 chain head (the value
 *                                                 didn't fit in one heap
 *                                                 page); length is the
 *                                                 blob's total byte length.
 *   otherwise                                 -> blkno/offset/length
 *                                                 address `length` bytes
 *                                                 starting at byte `offset`
 *                                                 of that BISCUIT_PAGE_STRHEAP
 *                                                 page's payload area
 *                                                 (length == 0 is a legal
 *                                                 non-NULL empty string,
 *                                                 distinguished from the
 *                                                 NULL case above by blkno).
 */
typedef struct BiscuitStrPtr
{
    BlockNumber blkno;
    uint32      offset;
    uint32      length;
} BiscuitStrPtr;

#define BISCUIT_STRPTR_OVERSIZE_SENTINEL   PG_UINT32_MAX

#define BiscuitStrPtrSlotsPerPage(pagesize) \
    (((pagesize) - MAXALIGN(SizeOfPageHeaderData) \
                  - MAXALIGN(sizeof(BiscuitPageOpaqueData))) \
     / sizeof(BiscuitStrPtr))

/*
 * BiscuitStrHeapHeader
 * Header for a BISCUIT_PAGE_STRHEAP page: a simple bump allocator.
 * `used` bytes of the `avail`-byte payload area (immediately following
 * this header) are occupied; a write either fits in (avail - used) and is
 * appended in place, or the page is full and a new tail page is
 * allocated -- identical shape to BiscuitPendLogPageHeader's
 * num_records/max_records, just byte-granular instead of record-granular.
 * Bytes already written are never moved or reclaimed in this phase
 * (freelist-slot reuse orphans the old bytes -- deferred to the future
 * STRCACHE value-heap compaction work, not required for v1 correctness).
 */
typedef struct BiscuitStrHeapHeader
{
    uint32  used;
    uint32  avail;
    /* `used` bytes of raw string payload follow, up to `avail` total */
} BiscuitStrHeapHeader;

#define BiscuitStrHeapMaxPayload(pagesize) \
    ((pagesize) - MAXALIGN(SizeOfPageHeaderData) \
                - MAXALIGN(sizeof(BiscuitStrHeapHeader)) \
                - MAXALIGN(sizeof(BiscuitPageOpaqueData)))

/*
 * A BISCUIT_PAGE_HEADER page has just a 4-byte length prefix (MAXALIGN'd)
 * followed by that many bytes of the HEADER blob -- never chained, always
 * overwritten in place. biscuit_rowstore_header_write() ERRORs rather
 * than silently truncating if the caller's blob ever exceeds this (only
 * realistically reachable with a huge number of indexed columns).
 */
#define BiscuitHeaderMaxPayload(pagesize) \
    ((pagesize) - MAXALIGN(SizeOfPageHeaderData) \
                - MAXALIGN(sizeof(uint32)) \
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
 * guarantee). The shared log's drain trigger reads pendlog_npages, which
 * the append path only writes when the log grows a page -- so the common
 * append still touches nothing index-wide.
 *
 * Scope note: this header only defines the metapage layout and the page
 * structs it points at (BiscuitDirPageHeader alongside the existing
 * BiscuitBlobChunkHeader/BiscuitPendLogPageHeader). Directory
 * lookup/insert, compacted-blob chunk read/write, and shared-log
 * append/drain logic live in their own files -- see biscuit_index.c's
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
