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

/*
 * Regex strategies (~ !~ ~* !~*).
 *
 * These are NOT a second matching engine. A regex key is rewritten into an
 * equivalent LIKE/ILIKE glob by biscuit_regex_to_glob() (biscuit_regex.c)
 * during query planning, and from that point on it is indistinguishable
 * from a LIKE key -- QueryPredicate.effective_strategy carries the
 * rewritten strategy and every evaluation site switches on that rather
 * than on ScanKey.sk_strategy. Only the decomposable subset is rewritten;
 * see biscuit_regex.h for what that subset is and
 * biscuit_build_query_plan() for what happens to the rest.
 *
 * The ordering deliberately mirrors 1..4 (positive, negated, insensitive,
 * negated-insensitive) so biscuit_regex_effective_strategy() is a
 * straight-line mapping.
 */
#define BISCUIT_REGEX_STRATEGY          5
#define BISCUIT_NOT_REGEX_STRATEGY      6
#define BISCUIT_IREGEX_STRATEGY         7
#define BISCUIT_NOT_IREGEX_STRATEGY     8

#define BISCUIT_MAX_STRATEGY            8

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
/*
 * BISCUIT_VERSION -- on-disk format version.
 *
 * There is no dual-path reader: an index written by any earlier format must
 * be REINDEXed. biscuit_persist_save() and biscuit_persist_load() failures
 * propagate rather than being swallowed.
 *
 * Deliberately NOT bumped for the row-identity in-place storage layout
 * (biscuit_rowstore.c), which changes the on-disk layout of
 * TIDS/STRCACHE/HEADER incompatibly: version 3 (BISCUIT_LIBRARY_VERSION
 * "3.0.0 - Player") was never shipped, so there is no deployed version-3
 * index anywhere for a bump to distinguish that format from, and nothing for
 * a compatibility path to read. An index built from an intermediate
 * development checkout must be REINDEXed.
 */
#define BISCUIT_VERSION                 3
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
#define BISCUIT_LIBRARY_VERSION         "3.1.0"

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
 * The format is a clean cutover (see design doc): there is no dual-path
 * reader and older indexes must be REINDEXed, so fields that carry no
 * meaning under the current design are dropped outright rather than kept
 * unused for compatibility.
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
     * A single index-wide append-only log. Every steady-state bitmap
     * mutation appends one self-describing BiscuitPendLogRecord here,
     * rather than appending to (and updating the directory entry of)
     * its own structure's private chain.
     *
     * Why: a row touching K structures would otherwise dirty ~K
     * distinct pages, and PostgreSQL charges a full-page image per
     * distinct page per checkpoint interval. Measured on one insert
     * of a 9-character string, that shape costs 82 pages, 84 FPIs and
     * 60KB of WAL. With one shared log those K records (20 bytes
     * each) land on the *same* tail page, so the page count -- and
     * therefore the FPI bill -- stops scaling with the number of
     * structures a row touches.
     *
     * BiscuitDirEntry consequently carries no per-structure pending
     * fields: a bitmap-kind entry holds only blob_head. See
     * BiscuitDirEntry's per-kind field table below.
     */
    BlockNumber pendlog_head;     /* first page of the shared log chain,
                                    * InvalidBlockNumber when the log is
                                    * empty (nothing appended since the
                                    * last drain) */
    BlockNumber pendlog_tail;     /* current append target; the O(1) tail pointer */
    uint32      pendlog_npages;   /*
                                   * pages currently in the log chain.
                                   *
                                   * Deliberately a PAGE count, not a record count: it is only touched when
                                   * the log grows a page, so the metapage stays out of the per-append WAL
                                   * record entirely. An exact record count here would cost twice -- every
                                   * append would carry the metapage into its WAL record, doubling
                                   * pages-per-append, the very quantity this design minimizes, and every
                                   * writer would serialize on one exclusive metapage lock.
                                   *
                                   * npages * BLCKSZ is the log's size for drain-trigger purposes, which is
                                   * all the trigger needs. Exact record counts are available per page in
                                   * BiscuitPendLogPageHeader.num_records for anything that genuinely needs
                                   * them.
                                   */

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
 * Unlike the compacted-blob/pending-list chains above, these pages are
 * mutated with single-page in-place GenericXLog writes rather than
 * rewritten wholesale on every commit. They carry no BISCUIT_VERSION
 * bump -- version 3 was never shipped, see that constant's comment. See
 * biscuit_rowstore.c's file header for the full design.
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
 * This is the only pending format in use; the per-structure pending page
 * (retired kind 2, see BISCUIT_PAGE_PENDING_RETIRED above) is neither
 * written nor read.
 */
#define BISCUIT_PAGE_PENDLOG  9     /* shared index-wide pending log page  */

/*
 * BISCUIT_PENDLOG_DRAIN_PAGES
 *
 * Hard ceiling (in pages) on log size, at which the append path drains
 * opportunistically. VACUUM drains unconditionally regardless of this.
 *
 * THIS IS NOT THE PRIMARY TRIGGER. Read biscuit.delta_compaction_slots
 * (biscuit_delta.c) first; this constant is the backstop beneath it.
 *
 * A byte-denominated threshold is nearly uncorrelated with the cost it is
 * meant to bound. What a large log costs is delta rebuild time, and
 * rebuild time scales with the number of ROWS in the log (one STRCACHE
 * materialization plus one fan-out each), not with how few bytes each
 * row's record occupied. At 8 bytes per row, 512 pages of log is on the
 * order of half a million rows.
 *
 * The row-denominated threshold lives in a GUC because the right value
 * follows from a measurement that has not been taken yet: whether delta
 * rebuild is dominated by the STRCACHE walk or by bitmap construction
 * (design §11). Keeping this page ceiling as well costs nothing and
 * bounds the pathological case where the GUC is set absurdly high.
 */
#define BISCUIT_PENDLOG_DRAIN_PAGES  512

/*
 * BISCUIT_PENDLOG_COMPACT_PAGES
 *
 * How much of the log one opportunistic compaction ships into base.
 *
 * Compaction exists so the delta does not grow unbounded between vacuums
 * (design §7.2), and it must ship a strict PREFIX of the log -- never a
 * selection of slots. For any slot, either every record up to the
 * compaction point is applied to base, or none is. Shipping "the
 * interesting slots" reorders that slot's history, and with slot recycling
 * that is concretely wrong: a slot may hold ADD, REMOVE, ADD for three
 * different rows, and applying the first ADD without the REMOVE leaves
 * base claiming a row that no longer matches. Nothing downstream catches
 * that -- xs_recheck is false, so the predicate is never re-tested, and
 * MVCC filters dead rows, not wrong ones.
 *
 * The value is a compromise between two costs that pull opposite ways. A
 * large prefix amortises the fixed cost of a compaction (detach, ingest,
 * the directory work) over more rows, but each one blocks longer against
 * other drainers. A small prefix keeps every individual compaction short
 * but fires more often. 64 pages is ~65k rows' worth of records at 8 bytes
 * each, which is comfortably more than any sensible value of
 * biscuit.delta_compaction_slots -- so in practice one compaction ships
 * everything that has accumulated, and the constant acts as a ceiling on
 * how much a single trigger can be made to do rather than as a routine
 * limit.
 */
#define BISCUIT_PENDLOG_COMPACT_PAGES  64

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
 * This is defense-in-depth for slot-allocation atomicity, which is
 * enforced at the allocator in biscuit_index.c's
 * biscuit_claim_new_slot(). If slot numbers were ever handed out from a
 * process-local counter again, two backends would compute the same
 * slot_idx for different rows; the TIDSLOT page's buffer lock serializes
 * the two writes without preventing the collision, and the second writer
 * would silently clobber the first row. biscuit_pagedir_append()'s
 * dense-in-order check is the only other guard in this area and it fires
 * only when a whole new *logical page* is allocated, so such a collision
 * would be loud in a minority of runs and silent in the rest.
 *
 * With a mode passed down, a fresh claim landing on an already-occupied
 * slot raises an ERROR at the moment of the overwrite instead, making any
 * future regression fail loudly and immediately rather than corrupting
 * data invisibly.
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
 * BiscuitXlogBatch
 *
 * Lets a caller that needs to durably write several pages for what is
 * logically one row -- biscuit_persist_row_identity_write_record()'s one
 * TIDS slot plus up to two STRCACHE slots per indexed column -- pay for
 * one GenericXLogStart/Finish pair per up-to-MAX_GENERIC_XLOG_PAGES pages
 * instead of one pair per page. This is the same combining
 * biscuit_blob.c's chunk-chain linking and biscuit_dir.c's rollover
 * branch already do for two buffers at once, just threaded through more
 * call layers. See biscuit_rowstore.c's biscuit_xlog_batch_register()
 * (static, internal) and biscuit_xlog_batch_flush() for the mechanics,
 * and biscuit_persist.c's biscuit_persist_row_identity_write_record() for
 * the driving caller.
 *
 * A batch holds at most MAX_GENERIC_XLOG_PAGES buffers -- the hard
 * ceiling GenericXLogRegisterBuffer() itself enforces -- pinned and
 * exclusive-locked between registrations. Registering a buffer beyond
 * that count flushes (finishes the WAL record for, and releases) the
 * buffers already held before opening a new transaction for the rest, so
 * a row wider than the limit still gets batched, just split across two
 * (or more) transactions instead of one per page.
 *
 * This covers only the steady-state "page already exists, update it in
 * place" writes (biscuit_rowstore_tid_write()'s slot write,
 * biscuit_strptr_write(), biscuit_strheap_append()'s in-place-append
 * branch). A write that must allocate and link a brand new page (a fresh
 * TIDSLOT/STRPTR logical page via biscuit_pagedir_ensure(), a fresh
 * STRHEAP tail via biscuit_strheap_append()'s rollover branch) keeps
 * doing that in its own self-contained GenericXLogStart/Finish exactly as
 * before -- that PageInit + link is its own atomic unit, mirroring
 * biscuit_blob.c's and biscuit_dir.c's existing rollover transactions,
 * and folding it into an in-progress batch would make the batch's scope
 * depend on how full an unrelated page happened to be. When a rollover
 * is hit mid-batch, biscuit_rowstore.c flushes the batch first so the two
 * transactions stay disjoint rather than interleaved.
 *
 * A caller not interested in batching (biscuit_persist_save()'s
 * whole-snapshot bulk rewrite, biscuit_persist_write_header_blob(), any
 * read path) simply passes a NULL BiscuitXlogBatch pointer down; every
 * batch-aware function falls back to its original one-transaction-per-page
 * behavior in that case.
 */
typedef struct BiscuitXlogBatch
{
    Relation          index;
    GenericXLogState *state;
    Buffer            bufs[MAX_GENERIC_XLOG_PAGES];
    int               nbufs;
} BiscuitXlogBatch;

extern void biscuit_xlog_batch_init(BiscuitXlogBatch *batch, Relation index);
extern void biscuit_xlog_batch_flush(BiscuitXlogBatch *batch);

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

/*
 * Direction of one pending-log record.
 *
 * Encoded as the ASCII characters rather than 1 and 2. This costs nothing:
 * BiscuitPendLogRecord is 8 bytes either way, and `op` occupies a byte
 * that alignment padding would otherwise waste, so the choice of value is
 * free. What it buys is that a raw dump of a pendlog page is readable
 * without a decoder -- a run of inserts shows up as a column of 2b, an
 * UPDATE as the 2d/2b pair it actually is. On a structure whose whole job
 * is to be reconstructed by hand when something has gone wrong, that is
 * worth more than the tidiness of small integers.
 *
 * Not folded into the sign of BiscuitPendLogRecord.slot, which would be
 * the genuinely compact encoding. slot is uint32; signing it would cost a
 * bit of slot space, leave slot 0 with no representable REMOVE, and save
 * no bytes at all, since the record is padded to 8 regardless.
 *
 * The values are free to change because nothing has shipped: 3.0.0 is the
 * first GA release built on this log format, so there is no on-disk or
 * WAL representation in the field to stay compatible with.
 */
#define BISCUIT_PENDING_OP_ADD     '+'
#define BISCUIT_PENDING_OP_REMOVE  '-'

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
 *     by structure identity there, so nothing per-entry tracks them.
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
 * strheap_head/strheap_tail are named for their only user and must not
 * be repurposed as general-purpose per-kind fields. Overloading one pair
 * of fields to mean "value heap" for STRCACHE and something else for the
 * bitmap kinds is a live hazard rather than a naming preference:
 * biscuit_persist.c's drop walk branches on kind to decide how to free
 * them, and one missing or mis-ordered case frees a STRHEAP chain as if
 * it were a pending chain, or walks a pending chain as a value heap.
 *
 * The entry carries no pending_count/pending_bytes either: those would
 * serve a per-structure size-threshold drain trigger, and the shared
 * log's trigger is a page count on the metapage (pendlog_npages)
 * instead. Keeping the entry at 24 bytes rather than 32 directly raises
 * BiscuitDirPageMaxEntries().
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
 * Additional BISCUIT_DIR_KIND_* values, covering BiscuitIndex persistent
 * state that is not per-character/per-length bitmap-shaped.
 *
 * The design doc's directory (§5) specifies
 * `(col, is_lower, kind, ch, position)` for the RoaringBitmap-shaped
 * structures (POS/NEG/CACHE/LEN/LEN_GE above) and does not cover the tid
 * array, the tombstone bitmap, the free-slot list or the per-record
 * string caches. biscuit_blob.c's chunk-chain primitive is
 * bitmap-agnostic ("just bytes in, bytes out", §1) and works equally well
 * for any of these once they are serialized to a flat byte buffer, so the
 * same directory+blob machinery is reused rather than inventing a second,
 * parallel addressing scheme for non-bitmap state.
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
 *   BISCUIT_DIR_COL_LEGACY    (-1) -- the single-column (legacy) field
 *                                     set's POS/NEG/CACHE/LEN/LEN_GE and
 *                                     its STRCACHE.
 *   BISCUIT_DIR_COL_SINGLETON (-2) -- TIDS/TOMBSTONES/FREELIST, which
 *                                     exist exactly once per index
 *                                     regardless of column count, so they
 *                                     don't belong to any particular
 *                                     column's chain. Always stored in the
 *                                     array slot 0 chain (see
 *                                     biscuit_dir.c's
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
 * THE FACT OF A ROW WRITE. Eight bytes, fixed size, no payload.
 *
 * History, because the shape of this struct is the whole design:
 *
 *   1. A per-structure pending chain. A row touching K structures dirtied
 *      ~K distinct pages, and PostgreSQL charges a full-page image per
 *      distinct page per checkpoint interval.
 *   2. A shared, index-wide log of *self-describing derived* records: the
 *      full structure identity (col, is_lower, kind, ch, position) plus
 *      rec_idx, 20 bytes. That collapsed K pages per row to 1, which is
 *      what the FPI bill actually scales with.
 *   3. This. Stop logging derived records altogether.
 *
 * Step 2 fixed page count but left record count alone, and record count is
 * where the remaining write amplification lived: indexing one N-character
 * string emits roughly 8N+4 derived identities (POS, NEG and CACHE per
 * character per case mode, plus LEN and the LEN_GE ladder). At N=24 that
 * is ~196 records, ~3.9 kB of derived data per row, and the accumulated
 * page delta covering it is what lands in WAL. Measured on a 100k-row
 * fixture: 5131 B of WAL per row inserted into a live index against 158 B
 * for the heap alone -- ~32x, with pg_waldump attributing 95.9% of it to
 * this extension's own Generic records.
 *
 * None of that fan-out is information. It is all recomputable from the
 * row's text, which is ALREADY durable and ALREADY WAL-logged, once, by
 * biscuit_persist_row_identity_write_record(): that writes TIDS[slot] and
 * every (column, is_lower) STRCACHE entry for the slot, in place, O(1).
 * Putting the text in this record too would write the same bytes to WAL
 * twice. So the record carries neither the text nor the fan-out -- only
 * which slot changed and in which direction -- and biscuit_delta.c
 * reconstructs the identities on demand by reading STRCACHE back.
 *
 * Consequences of the fixed-size form, all of them simplifications:
 *   - No variable-length encoding, no page-spanning records, no oversize
 *     spill path. BiscuitPendLogMaxRecords stays a plain constant.
 *   - Multi-column atomicity comes free: one row's write is ONE record
 *     regardless of column count, because the per-column text lives in
 *     STRCACHE under that slot.
 *
 * Why there is no per-record generation counter: idx->gen is bumped
 * non-transactionally (see biscuit_insert()), so a record written by a
 * transaction that later aborts would carry a generation that "happened".
 * Generation tracking belongs in the three places that already do it
 * correctly -- the metapage-vs-cache comparison in
 * biscuit_get_current_index(), the snapshot's resume_blk/resume_off, and
 * total_drains -- not in a field on every record.
 *
 * Why `op` is mandatory: slots are recycled, so a log may legitimately
 * hold ADD@42, REMOVE@42, ADD@42 for three different rows. DELETE and the
 * delete-half of UPDATE have no other representation at all.
 *
 * NOTE ON NAMING (deviation from the design doc): the doc calls the first
 * field `position`. It is renamed `slot` here because this codebase
 * already uses `position` throughout for a character's position within a
 * structure identity (BiscuitDirEntry.position, BiscuitPendLogKey.position),
 * and a second, unrelated meaning for the same word in the same subsystem
 * is a bug waiting to be written.
 */
typedef struct BiscuitPendLogRecord
{
    uint32  slot;           /* record slot (rec_idx) this record concerns */
    uint8   op;             /* BISCUIT_PENDING_OP_ADD / _REMOVE */
    uint8   flags;          /* reserved; zero-filled by writers, ignored
                             * by readers */
    uint16  pad;            /* reserved; zero-filled by writers */
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
 * Implemented above (BiscuitMetaPageData). The format is a clean cutover
 * (REINDEX required, no dual-path reader -- see design doc), so fields
 * that carry no meaning under the current design are dropped rather than
 * kept behind a version check.
 *
 * One deliberate deviation from the design doc's single `dir_root`: the
 * metapage holds one directory-chain root per indexed column
 * (`dir_roots[BISCUIT_MAX_DIR_COLUMNS]`, `num_dir_columns`) instead of a
 * single shared root with `col` as just another field inside each
 * BiscuitDirEntry. This keeps each column's directory chain, and
 * therefore its locking (§6) and drain traffic, fully independent at the
 * chain-root level, not just at the individual-entry level -- a
 * multi-column index's columns never have to walk or contend on a shared
 * root page to find their own entries. The per-entry `col` field on
 * BiscuitDirEntry is kept regardless, since a single column's chain can
 * still hold entries for multiple `kind`s and multiple case-mode sets and
 * having the field is cheap self-description for page-level tooling; it
 * should just always agree with the dir_roots[] index it was reached
 * through.
 *
 * FSM bootstrap (`fsm_root`, `fsm_page_count`) implements the
 * deferred-recycle mechanism the design doc specifies: a page with
 * recycle_xid set is not immediately FSM-eligible, so something has to
 * hold onto "pages retired but not yet recyclable" until
 * biscuit_vacuumcleanup()'s horizon check clears them for the ordinary
 * index FSM. fsm_root is that holding chain's head.
 *
 * See BISCUIT_PAGE_FORMAT_VERSION's own comment above for why
 * `page_format_version` is tracked separately from `version`.
 *
 * total_pending_bytes is deliberately NOT maintained synchronously on the
 * append path. A per-append write to a single shared metapage field is a
 * global serialization point that contradicts §6's
 * no-cross-structure-contention guarantee. The shared log's drain trigger
 * reads pendlog_npages, which the append path only writes when the log
 * grows a page -- so the common append still touches nothing index-wide.
 *
 * Scope note: this header only defines the metapage layout and the page
 * structs it points at (BiscuitDirPageHeader alongside the existing
 * BiscuitBlobChunkHeader/BiscuitPendLogPageHeader). Directory
 * lookup/insert, compacted-blob chunk read/write, and shared-log
 * append/drain logic live in their own files -- see biscuit_index.c's
 * biscuit_write_metadata_to_disk()/biscuit_read_metadata_from_disk() for
 * the metapage read/write wired up here.
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

    /*
     * SCRATCH CONTEXT for pending-list reconciliation's fresh bitmap copies
     * (biscuit_reconcile_pending()/biscuit_reconcile_register_cleanup(),
     * biscuit_pattern.c).
     *
     * Created once in biscuit_beginscan(), MemoryContextReset() at the start
     * of every biscuit_rescan(), and MemoryContextDelete()'d in
     * biscuit_endscan(). The reset is what bounds growth across repeated
     * rescans of the SAME scan object -- e.g. a prepared statement's index
     * scan reused across many pgbench transactions, an access pattern that
     * outlives the ambient CurrentMemoryContext this would otherwise rely on.
     * Explicit, scan-owned teardown, independent of whatever the executor's
     * own ambient context happens to be or how long it happens to live -- see
     * biscuit_scan.c's biscuit_reconcile_scratch_cxt for how this gets
     * threaded down to biscuit_pattern.c without changing every call
     * signature in between.
     */
    MemoryContext scratch_cxt;

    /*
     * Set when this scan's candidate set is a SUPERSET of the true result
     * rather than exactly it -- currently only when a regex key fell
     * outside the glob-decomposable subset and was skipped (see
     * QueryPredicate.is_lossy). Reported to the executor via
     * scan->xs_recheck / the tbm_add_tuples() recheck flag, which makes it
     * re-evaluate the original qual and discard the extra rows.
     *
     * Biscuit is otherwise an exact-match AM and hard-codes recheck to
     * false; several invariants elsewhere in the tree are stated in terms
     * of that (biscuit_index.c, biscuit_pendlog.c, biscuit_pattern.c).
     * Those remain true -- this flag can only ever turn recheck ON for a
     * scan that has already given up exactness, and is recomputed from
     * scratch on every biscuit_rescan().
     */
    bool needs_recheck;
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

    /*
     * The strategy this predicate should actually be EVALUATED as, which
     * is not always scan_key->sk_strategy.
     *
     * For LIKE/ILIKE keys the two are identical. For the regex strategies
     * (5..8) biscuit_build_query_plan() rewrites `pattern` from a regex
     * into the equivalent glob and sets this to the matching LIKE/ILIKE
     * strategy, so that every downstream switch -- in biscuit_rescan(),
     * biscuit_build_candidates_multicolumn(), and analyze_pattern() --
     * needs no regex awareness at all. Read this, never sk_strategy, when
     * deciding how to evaluate a predicate; read sk_strategy only when you
     * genuinely mean "what did the user write".
     */
    int effective_strategy;

    /*
     * True when this predicate could NOT be rewritten exactly -- a regex
     * outside the decomposable subset.
     *
     * Such a predicate contributes NOTHING to the candidate set: it is
     * skipped rather than approximated, and the scan sets xs_recheck so
     * the executor re-evaluates the original qual against the heap tuple.
     * Skipping is what keeps the result a superset, which is the only
     * thing recheck can repair -- recheck removes rows, it cannot add
     * back a row an over-tight glob wrongly excluded.
     *
     * This is also why a lossy predicate must never be evaluated as a
     * negated strategy. Inverting a superset yields a SUBSET, and a
     * subset is exactly the shape recheck cannot fix.
     */
    bool is_lossy;

    /*
     * True when this predicate IS evaluated (unlike is_lossy) but its
     * result is only a superset, so the executor must still recheck.
     *
     * Currently set only for case-insensitive regex (~*). That rewrite
     * targets the ILIKE machinery, whose lower()-based folding is not the
     * same relation as the regex engine's case folding: ILIKE over-matches
     * on characters like 'İ' and the ǅ/ǈ/ǋ titlecase family. The glob
     * still does almost all the filtering, so this is much better than
     * is_lossy -- it just cannot be trusted as exact.
     *
     * The opposite direction ('I' ~* 'ı' is true where ILIKE is false)
     * would be unfixable by recheck, and is excluded by only decomposing
     * case-insensitive patterns that are pure ASCII. See
     * biscuit_regex_glob_is_ascii().
     */
    bool needs_recheck;

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
