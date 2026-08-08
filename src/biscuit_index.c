/*
 * biscuit_index.c
 * Index construction (build / load), disk metadata I/O, CRUD record
 * management, and AM maintenance callbacks (insert, bulkdelete,
 * vacuumcleanup, costestimate, options, validate, adjustmembers).
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_utf8.h"
#include "biscuit_cache.h"
#include "biscuit_index.h"
#include "utils/spccache.h"   /* get_tablespace_page_costs() -- cost model */
#include "biscuit_blob.h"   /* biscuit_page_write_blob() -- the per-structure
                              * pending-chain primitives this used to need are
                              * gone; see biscuit_pendlog.h */
#include "biscuit_dir.h"    /* biscuit_dir_find/_insert/_update, BiscuitDirEntry */
#include "biscuit_fanout.h" /* biscuit_fanout_string() -- the single answer to
                             * "what structures does this string belong to",
                             * shared with the delta builder and the drain */
#include "biscuit_pendlog.h" /* shared index-wide pending log: replaced the
                              * per-structure pending chains on the write path */
#include "access/xact.h"    /* RegisterXactCallback, XACT_EVENT_* */

/* --- cost model (biscuit_costestimate, SECTION 6a) --- */
#include "optimizer/cost.h"    /* cpu_tuple_cost, cpu_operator_cost */
#include "optimizer/optimizer.h"
#include "utils/selfuncs.h"    /* clauselist_selectivity(),
                                * get_quals_from_indexclauses(),
                                * add_predicate_to_index_quals() */
#include "catalog/pg_type.h"   /* TEXTOID */
#include "utils/builtins.h"    /* TextDatumGetCString() */
#include "nodes/pathnodes.h"   /* IndexClause, IndexOptInfo, RestrictInfo */
#include "nodes/nodeFuncs.h"
#include "utils/lsyscache.h"  /* get_attavgwidth() */
#include "parser/parsetree.h"  /* planner_rt_fetch() */

/* ================================================================
 * SECTION 0b – Deferred header/tombstone/freelist flush
 * ================================================================
 *
 * A steady-state INSERT/DELETE must eventually re-persist its index's
 * HEADER blob (num_records, capacity, the insert/update/delete counters),
 * its tombstone bitmap and its free list. Those are index-wide scalars
 * rather than per-row data: every row in a statement would write the same
 * content, so each mutating call just marks its index OID dirty here and a
 * single pre-commit xact callback flushes every dirty index exactly once.
 *
 * Scope note: TIDS and STRCACHE used to go through this path too, and it
 * was load-bearing then -- they were whole-array blob rewrites, so doing
 * one per row would have been O(n^2) for a bulk insert. That is no longer
 * the case: both are now written per-row, in place, at the mutation site
 * (biscuit_persist_row_identity_write_record(), called from
 * biscuit_insert()), and biscuit_persist_save_row_identity() no longer
 * touches them at all. What remains here is O(1) per flush, so this
 * machinery is now a redundancy-avoidance measure rather than a
 * complexity-class fix -- worth keeping (writing the same header once per
 * row is pure waste) but no longer a correctness-critical amortization.
 *
 * The dirty set is a tiny process-local array of OIDs (an index can only
 * appear once). It lives in TopTransactionContext-independent static
 * storage and is cleared at every transaction end. The BiscuitIndex to
 * re-save is fetched from the session cache (biscuit_cache_lookup), which
 * biscuit_insert() always keeps current for the touched index.
 */
#define BISCUIT_MAX_DIRTY_INDEXES 64
static Oid   biscuit_dirty_oids[BISCUIT_MAX_DIRTY_INDEXES];
static int   biscuit_dirty_count       = 0;
static bool  biscuit_xact_cb_registered = false;

static void biscuit_row_identity_xact_callback(XactEvent event, void *arg);

static void
biscuit_mark_row_identity_dirty(Oid indexoid)
{
    int i;

    for (i = 0; i < biscuit_dirty_count; i++)
        if (biscuit_dirty_oids[i] == indexoid)
            return;   /* already marked this transaction */

    if (!biscuit_xact_cb_registered)
    {
        RegisterXactCallback(biscuit_row_identity_xact_callback, NULL);
        biscuit_xact_cb_registered = true;
    }

    if (biscuit_dirty_count < BISCUIT_MAX_DIRTY_INDEXES)
        biscuit_dirty_oids[biscuit_dirty_count++] = indexoid;
    else
    {
        /*
         * Overflow (a single transaction touched > 64 distinct biscuit
         * indexes): fall back to flushing this one inline right now so we
         * never silently drop a required row-identity save. Rare enough
         * that the O(num_records) cost here is irrelevant.
         */
        BiscuitIndex *idx = biscuit_cache_lookup(indexoid);
        if (idx)
        {
            Relation rel = index_open(indexoid, RowExclusiveLock);
            biscuit_persist_save_row_identity(rel, idx);
            index_close(rel, RowExclusiveLock);
        }
    }
}

static void
biscuit_flush_dirty_row_identity(void)
{
    int i;

    for (i = 0; i < biscuit_dirty_count; i++)
    {
        Oid            indexoid = biscuit_dirty_oids[i];
        BiscuitIndex  *idx      = biscuit_cache_lookup(indexoid);
        Relation       rel;

        if (!idx)
        {
            /*
             * Evicted (e.g. DROP, or a sinval-driven relcache callback) --
             * there is no in-memory state left to serialize, so skipping is
             * the only option.
             *
             * This used to be silently destructive: the skipped write left
             * the HEADER's num_records at whatever older value it held,
             * while biscuit_claim_new_slot() had already advanced the
             * metapage, and a later cold load trusted the header. It no
             * longer is -- biscuit_persist_load() reconciles against the
             * metapage, and biscuit_persist_save_row_identity() clamps up to
             * it rather than overwriting with a possibly-lower value. What
             * is still lost here is the counters/tombstones/freelist delta
             * for this transaction, which the next flush re-derives.
             */
            elog(DEBUG1,
                 "biscuit: skipping row-identity flush for index %u (cache entry evicted)",
                 indexoid);
            continue;
        }

        rel = index_open(indexoid, RowExclusiveLock);
        biscuit_persist_save_row_identity(rel, idx);
        index_close(rel, RowExclusiveLock);
    }
    biscuit_dirty_count = 0;
}

static void
biscuit_row_identity_xact_callback(XactEvent event, void *arg)
{
    (void) arg;

    switch (event)
    {
        case XACT_EVENT_PRE_COMMIT:
        case XACT_EVENT_PARALLEL_PRE_COMMIT:
            /* Flush while the transaction is still live (we can still open
             * relations and write WAL-logged pages here). */
            biscuit_flush_dirty_row_identity();
            break;

        case XACT_EVENT_ABORT:
        case XACT_EVENT_PARALLEL_ABORT:
            /* Rolled back: the in-memory idx and any pending appends are
             * discarded/undone with the transaction; just drop the marks.
             * (Over-invalidation of idx->gen is harmless, as documented at
             * the gen++ site.) */
            biscuit_dirty_count = 0;
            break;

        default:
            break;
    }
}

/* ================================================================
 * SECTION 0 – Opclass case-mode gating
 * ================================================================
 *
 * Three opclasses share the biscuit access method: biscuit_ops (LIKE +
 * ILIKE, default), biscuit_like_ops (LIKE only), and biscuit_ilike_ops
 * (ILIKE only) -- see biscuit.sql. Each declares its own opfamily, and
 * PostgreSQL's relcache resolves the opfamily actually chosen for each
 * index column into index->rd_opfamily[col] regardless of access method.
 * That's exactly the signal we need: it reflects whatever opclass the
 * user wrote in CREATE INDEX (explicitly, or implicitly via DEFAULT),
 * with no dependency on catalog lookups beyond what the relcache has
 * already done for us.
 *
 * biscuit_build()/biscuit_insert()/biscuit_persist_load() call this once
 * per column and gate structure population accordingly, so a
 * biscuit_like_ops column never spends memory/build time on the
 * case-insensitive ("_lower") structures it can never be queried with,
 * and vice versa for biscuit_ilike_ops.
 */
#if PG_VERSION_NUM < 180000
#include "catalog/pg_opfamily.h"
#include "utils/syscache.h"
/*
 * get_opfamily_name() was only added to lsyscache.h in PG18. For PG16/17
 * we look the name up ourselves via the syscache, mirroring its
 * missing_ok semantics (returns NULL rather than erroring).
 */
static char *
biscuit_get_opfamily_name_compat(Oid opfamily, bool missing_ok)
{
    HeapTuple tp;
    char     *result;

    tp = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfamily));
    if (!HeapTupleIsValid(tp))
    {
        if (missing_ok)
            return NULL;
        elog(ERROR, "cache lookup failed for opfamily %u", opfamily);
    }

    result = pstrdup(NameStr(((Form_pg_opfamily) GETSTRUCT(tp))->opfname));
    ReleaseSysCache(tp);
    return result;
}
#define get_opfamily_name(opfamily, missing_ok) \
    biscuit_get_opfamily_name_compat((opfamily), (missing_ok))
#endif

uint8
biscuit_get_column_case_mode(Relation index, int col)
{
    Oid   opfamily;
    char *famname;
    uint8 mode;

    if (!index || !index->rd_index || !index->rd_opfamily ||
        col < 0 || col >= index->rd_index->indnatts)
        return BISCUIT_MODE_BOTH;   /* safe default: build everything */

    opfamily = index->rd_opfamily[col];
    famname  = get_opfamily_name(opfamily, true);

    if (!famname)
        return BISCUIT_MODE_BOTH;

    if (strcmp(famname, "biscuit_like_ops") == 0)
        mode = BISCUIT_MODE_LIKE;
    else if (strcmp(famname, "biscuit_ilike_ops") == 0)
        mode = BISCUIT_MODE_ILIKE;
    else
        /* biscuit_ops, or any opfamily we don't specifically recognize. */
        mode = BISCUIT_MODE_BOTH;

    pfree(famname);
    return mode;
}

/* ================================================================
 * SECTION 1 – Disk metadata I/O
 * ================================================================ */

/*
 * biscuit_metapage_data
 *
 * Validate a buffer's contents as a Biscuit metapage and return the
 * special-area struct, or NULL if it isn't one.
 *
 * WHY THIS EXISTS (BLOCKER-1). The metapage readers below used to gate on
 *
 *     if (PageIsNew(page) || PageIsEmpty(page))
 *         return false;
 *
 * PageIsEmpty() is *always true* for a valid Biscuit metapage. The page
 * keeps every byte of its data in the special area
 * (BiscuitMetaPageData via PageGetSpecialPointer) and never places an
 * item in the page body, so PageInit() leaves pd_lower at
 * SizeOfPageHeaderData and it stays there forever -- which is precisely
 * what PageIsEmpty() tests. The gate therefore fired on every
 * successfully written metapage, and every caller took the "no readable
 * metapage" branch unconditionally.
 *
 * biscuit_write_metadata_to_disk() already documents this trap and had
 * PageIsEmpty() removed from its own carry-forward gate; the two readers
 * were missed, and that omission is what silently disabled the
 * cross-backend staleness check. See biscuit_get_current_index().
 *
 * The correct validity test for a special-area-only format is the one
 * used here: confirm the special area is structurally sane, then confirm
 * it actually holds a Biscuit metapage of a layout we understand.
 * PageIsNew() is still screened because PageGetSpecialPointer() on a
 * never-initialized (all-zero) page would index past a zero-sized
 * special area.
 *
 * Callers hold at least a share lock on the buffer.
 */
static BiscuitMetaPageData *
biscuit_metapage_data(Page page)
{
    PageHeader           ph = (PageHeader) page;
    BiscuitMetaPageData *meta;

    if (PageIsNew(page))
        return NULL;

    /*
     * Structural sanity before dereferencing the special area: a page
     * whose header survived but whose pd_special is garbage must not be
     * turned into a wild pointer.
     */
    if (PageGetPageSize(page) != BLCKSZ)
        return NULL;
    if (ph->pd_special > BLCKSZ || ph->pd_special < ph->pd_upper)
        return NULL;
    if (PageGetSpecialSize(page) < MAXALIGN(sizeof(BiscuitMetaPageData)))
        return NULL;

    meta = (BiscuitMetaPageData *) PageGetSpecialPointer(page);

    /*
     * magic identifies this as a Biscuit metapage at all; version /
     * page_format_version identify which layout it was written with.
     * BISCUIT_VERSION 2's metapage layout (dir_roots/fsm_root/pending-list
     * tuning) is not compatible with a version-1 page (which had `root`
     * and a differently-sized reserved area at the same offsets), so a
     * mismatch is treated exactly like "no metapage", which
     * biscuit_load_index() turns into a REINDEX-required ERROR rather
     * than interpreting old-layout bytes as if they were the new one.
     */
    if (meta->magic != BISCUIT_MAGIC ||
        meta->version != BISCUIT_VERSION ||
        meta->page_format_version != BISCUIT_PAGE_FORMAT_VERSION)
        return NULL;

    return meta;
}

void
biscuit_write_metadata_to_disk(Relation index, BiscuitIndex *idx)
{
    Buffer               buf;
    Page                 page;
    GenericXLogState    *state;
    BiscuitMetaPageData *meta;
    bool                 is_new_page;
    BlockNumber          nblocks = RelationGetNumberOfBlocks(index);

    /*
     * This function is called repeatedly over an index's lifetime
     * (unconditionally from every biscuit_insert()/biscuit_bulkdelete()
     * call, to keep gen current -- see callers).
     */
    is_new_page = (nblocks == 0);

    if (is_new_page)
        buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
    else
        buf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);

    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    /*
     * FAST PATH -- existing, current-format metapage. This is what every
     * ordinary biscuit_insert()/biscuit_bulkdelete() call hits. Only
     * num_records/gen ever change here (num_records is already advanced
     * under this same buffer's lock by biscuit_claim_new_slot(); this just
     * republishes Max(page's value, caller's value) -- see the
     * CONCURRENCY FIX comment below), so update them IN PLACE with a
     * plain (non-FULL_IMAGE) registration instead of PageInit()-ing and
     * rewriting the whole page.
     *
     * This function used to run PageInit() + GENERIC_XLOG_FULL_IMAGE
     * unconditionally on every call: an unconditional full BLCKSZ WAL
     * image (forced every time, unlike ordinary full-page-write
     * suppression which only fires once per checkpoint) to persist what
     * is, on this path, a one-or-two-field counter bump -- dwarfing by
     * roughly three orders of magnitude the entire point of
     * biscuit_pendlog.c's row-batching redesign, and holding the
     * metapage's one global exclusive lock far longer than the update
     * needs. biscuit_claim_new_slot(), a few dozen lines up, already does
     * the cheap version of this; this now matches it.
     *
     * biscuit_metapage_data() is the correct validity test here (not
     * PageIsEmpty()/PageIsNew() -- see that function's own comment for
     * why PageIsEmpty() is always true for a valid Biscuit metapage), and
     * it also gives us the structural bounds checks it does before
     * dereferencing the special area, which a bare magic-field peek would
     * not.
     */
    if (!is_new_page)
    {
        page = BufferGetPage(buf);
        meta = biscuit_metapage_data(page);

        if (meta != NULL)
        {
            uint32 new_num_records;

            state = GenericXLogStart(index);
            page  = GenericXLogRegisterBuffer(state, buf, 0);
            meta  = (BiscuitMetaPageData *) PageGetSpecialPointer(page);

            /*
             * CONCURRENCY FIX (slot-allocation race).
             *
             * num_records is NOT written from idx->num_records
             * unconditionally: slot numbers are claimed from
             * meta->num_records under the metapage's exclusive buffer
             * lock (see biscuit_claim_new_slot()), so the metapage is the
             * authoritative counter and any backend's idx->num_records is
             * a process-local copy that goes stale the moment another
             * backend claims a slot. This function runs well after that
             * claim and its lock have been released, so blindly writing
             * meta->num_records = idx->num_records here would let a
             * backend that claimed slot 5 (leaving the counter at 6)
             * stamp 6 back over a 7 that a concurrent backend had since
             * committed -- handing the *same* slot number out twice and
             * silently clobbering a row. Max() instead, since both
             * counters are monotonically non-decreasing over an index's
             * lifetime (bulkdelete tombstones slots, it never shrinks
             * num_records). BiscuitMetaPageData.num_records is uint32
             * while BiscuitIndex.num_records is int, so clamp before
             * comparing to keep Max() away from a signed/unsigned
             * promotion.
             */
            new_num_records = Max(meta->num_records, (uint32) Max(idx->num_records, 0));
            meta->num_records = new_num_records;

            /*
             * GEN FIX (cross-backend staleness race).
             *
             * gen used to be published as Max(meta->gen, idx->gen). Unlike
             * num_records, idx->gen is NOT a cached copy of the one shared
             * counter -- it is seeded from disk_gen at load time and then
             * bumped once per row *this backend itself* writes (see
             * biscuit_insert()/biscuit_bulkdelete()). It counts this
             * backend's own mutations since it loaded, not mutations
             * index-wide.
             *
             * That made the Max() lossy. Two backends A and B loading at
             * the same disk_gen = G and then each inserting rows can each
             * publish new_gen = Max(meta->gen, idx_X.gen); whichever of
             * them is running behind the other's already-published value
             * contributes nothing -- its own, genuinely new row leaves
             * meta->gen completely unchanged. A backend whose local idx->gen
             * later happens to reach that same plateau then passes
             * biscuit_get_current_index()'s staleness check (disk_gen <=
             * idx->gen) and skips the reload that would have refreshed the
             * "hole" slots biscuit_claim_new_slot() deliberately leaves
             * behind for rows other backends claimed concurrently (see its
             * comment). biscuit_reconcile_pending() applies the durable
             * pending log unconditionally on the read path, so a hole slot
             * can still surface as a bitmap match at scan time -- and
             * biscuit_collect_sorted_tids_single() then finds no valid TID
             * behind it. Not on-disk damage: a missed reload, caused by a
             * gen counter that failed to advance on a genuine commit.
             *
             * Fix: gen becomes a genuinely shared, atomically-advanced
             * counter -- the same pattern biscuit_claim_new_slot() already
             * uses for num_records, and pendlog_clear_draining() already
             * uses for its own bump. Increment the durable counter by
             * exactly one per call, under this buffer's exclusive lock, and
             * do not let idx->gen influence the published value at all.
             * Then pull this backend's local copy forward to match, exactly
             * as biscuit_claim_new_slot() pulls idx->num_records forward
             * after claiming a slot.
             */
            meta->gen++;
            idx->gen = meta->gen;

            GenericXLogFinish(state);
            UnlockReleaseBuffer(buf);
            return;
        }
    }

    /*
     * SLOW / INIT PATH -- a brand-new page (biscuit_build()'s one-time
     * write against a fresh relfilenode), or an existing block 0 that
     * biscuit_metapage_data() didn't recognize as a current-format
     * Biscuit metapage (unrecognized/foreign/corrupt page, or an old
     * version-1 layout). Both are rare and not a per-row cost, so paying
     * for a full PageInit() + FULL_IMAGE rewrite here is the right trade:
     * there is no prior state worth preserving (or safe to trust) in
     * either case, so everything but idx's own values resets to
     * defaults -- same defaults the old carry-forward fallback used.
     */
    state = GenericXLogStart(index);
    page  = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

    PageInit(page, BufferGetPageSize(buf), sizeof(BiscuitMetaPageData));

    meta          = (BiscuitMetaPageData *) PageGetSpecialPointer(page);
    meta->magic   = BISCUIT_MAGIC;
    meta->version = BISCUIT_VERSION;
    meta->page_format_version = BISCUIT_PAGE_FORMAT_VERSION;
    meta->num_records = (uint32) Max(idx->num_records, 0);

    /*
     * Direct assignment, not the fast path's increment-and-pull-forward:
     * this branch only runs for a brand-new page (biscuit_build()'s
     * one-time write, where idx->gen is still its initial 0) or a
     * corrupt/unrecognized block 0 being forcibly reset. Both discard
     * whatever was on disk and establish a fresh baseline rather than
     * publishing one more increment against other backends' concurrent
     * state, so there is no shared counter to race against here.
     */
    meta->gen         = idx->gen;

    meta->num_dir_columns = 0;
    for (int i = 0; i < BISCUIT_MAX_DIR_COLUMNS; i++)
        meta->dir_roots[i] = InvalidBlockNumber;
    meta->fsm_root            = InvalidBlockNumber;
    meta->fsm_page_count      = 0;
    meta->pending_list_limit  = BISCUIT_DEFAULT_PENDING_LIST_LIMIT;
    meta->total_pending_bytes = 0;
    meta->total_drains        = 0;
    /*
     * MUST be InvalidBlockNumber, not left at 0 by a zeroed struct. A
     * zero-filled metapage special area would leave these at block 0, the
     * metapage itself -- and the shared-log append path reads the
     * metapage as if it were a log page, decides the "tail" is full, and
     * tries to lock block 0 while already holding it, self-deadlocking on
     * the buffer content lock on an index's very first insert.
     */
    meta->pendlog_head        = InvalidBlockNumber;
    meta->pendlog_tail        = InvalidBlockNumber;
    meta->pendlog_npages      = 0;
    meta->pendlog_draining    = InvalidBlockNumber;
    memset(meta->reserved, 0, sizeof(meta->reserved));

    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

bool
biscuit_read_metadata_from_disk(Relation index,
                                int *num_records,
                                int *num_columns,
                                int *max_len,
                                uint64 *gen)
{
    Buffer             buf;
    Page               page;
    BiscuitMetaPageData *meta;
    BlockNumber        nblocks = RelationGetNumberOfBlocks(index);

    if (nblocks == 0)
    {
        *num_records = *num_columns = *max_len = 0;
        if (gen) *gen = 0;
        return false;
    }

    buf  = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);

    /*
     * BLOCKER-1 FIX. This used to be
     *
     *     if (PageIsNew(page) || PageIsEmpty(page))
     *
     * followed by an inline magic/version check. PageIsEmpty() is always
     * true for a Biscuit metapage (all its data lives in the special
     * area, so pd_lower never moves off SizeOfPageHeaderData), so this
     * function returned false for *every* index, always -- including
     * perfectly healthy ones. See biscuit_metapage_data().
     *
     * The two consequences were exactly the reported symptom:
     *
     *  1. biscuit_get_current_index() treats a false return as "no
     *     readable metapage, nothing to compare against, trust the
     *     cache" and returns the cached copy unconditionally. The
     *     cross-backend staleness check was dead code: a backend that
     *     primed its cache before another backend's INSERT committed
     *     served the pre-commit snapshot forever.
     *
     *  2. biscuit_load_index() read back gen == 0 for every index, so a
     *     writer's own idx->gen restarted at 0 on each (re)load. Because
     *     biscuit_write_metadata_to_disk() at the time published
     *     Max(prev_gen, idx->gen), the on-disk counter then stopped
     *     advancing altogether once it exceeded the number of mutations
     *     any single backend performed since its last load -- so even a
     *     working comparison would have had nothing to compare.
     *
     * Both are repaired by reading the metapage correctly here. (Note for
     * future readers: biscuit_write_metadata_to_disk() no longer publishes
     * Max(prev_gen, idx->gen) at all -- see the GEN FIX comment there for
     * a second, independent failure mode that formula had, and its
     * replacement.)
     */
    meta = biscuit_metapage_data(page);
    if (meta == NULL)
    {
        UnlockReleaseBuffer(buf);
        *num_records = *num_columns = *max_len = 0;
        if (gen) *gen = 0;
        return false;
    }

    *num_records = meta->num_records;
    *num_columns = 0;
    *max_len     = 0;
    /* meta->reserved, dir_roots[], fsm_root, and pending-list tuning/stats
     * are intentionally not surfaced through this call's signature --
     * nothing in this phase consumes them yet (no directory/blob/pending
     * read path exists). A later phase that needs them should read the
     * metapage directly rather than growing this function's out-param
     * list further. */
    if (gen) *gen = meta->gen;

    UnlockReleaseBuffer(buf);
    return true;
}

/* ================================================================
 * SECTION 2 – CRUD helpers
 * ================================================================ */

void
biscuit_init_crud_structures(BiscuitIndex *idx)
{
    idx->tombstones     = biscuit_roaring_create();
    idx->free_capacity  = 64;
    idx->free_count     = 0;
    idx->free_list      = (uint32_t *) palloc(idx->free_capacity * sizeof(uint32_t));
    idx->tombstone_count = 0;
    idx->insert_count   = 0;
    idx->update_count   = 0;
    idx->delete_count   = 0;
}

void
biscuit_push_free_slot(BiscuitIndex *idx, uint32_t slot)
{
    if (idx->free_count >= idx->free_capacity)
    {
        int       new_cap  = idx->free_capacity * 2;
        uint32_t *new_list = (uint32_t *) palloc(new_cap * sizeof(uint32_t));
        memcpy(new_list, idx->free_list, idx->free_count * sizeof(uint32_t));
        pfree(idx->free_list);
        idx->free_list     = new_list;
        idx->free_capacity = new_cap;
    }
    idx->free_list[idx->free_count++] = slot;
}

bool
biscuit_pop_free_slot(BiscuitIndex *idx, uint32_t *slot)
{
    if (idx->free_count == 0)
        return false;
    *slot = idx->free_list[--idx->free_count];
    return true;
}

/* ================================================================
 * SECTION 2a – Cross-backend slot allocation
 * ================================================================
 *
 * Slot numbers are an index-wide shared resource, and until this fix they
 * were handed out from a purely process-local counter (idx->num_records on
 * a BiscuitIndex cached in CacheMemoryContext, which biscuit_cache.c keeps
 * per backend, not in shared memory). Two backends that had each loaded
 * their own copy at the same baseline would compute the *same* slot_idx
 * for two different rows. The per-page buffer lock down in
 * biscuit_rowstore_tid_write() then serialized the two writes in time
 * without preventing the collision, so the second writer simply overwrote
 * the first row's slot -- silent data loss, invisible to any number of
 * single-client runs because a single client never has a competing
 * baseline to collide against.
 *
 * The fix is to make "read the current count, claim the next slot, publish
 * the new count" a single atomic operation against a genuinely shared
 * object. The metapage is already a shared, WAL-logged on-disk buffer, and
 * its exclusive content lock is already the serialization point this
 * module uses for the other index-wide allocations (biscuit_dir.c's
 * dir_roots, biscuit_page_alloc()'s fsm freelist), so it is the natural
 * home: same pattern PostgreSQL itself uses with
 * LockRelationForExtension() for physical page allocation. Slot allocation
 * is simply the sibling shared resource that never got the same treatment.
 *
 * NON-TRANSACTIONAL, deliberately, for the same reason idx->gen is (see
 * the comment at the end of biscuit_insert()): the counter advances the
 * moment the slot is claimed, whether or not the surrounding transaction
 * commits. An aborted insert therefore leaks its slot number. That is
 * over-allocation, which is harmless -- the slot is simply never marked
 * live, reads skip it, and VACUUM reclaims it. The alternative, releasing
 * the claim on abort, would mean the counter can move backwards, which is
 * exactly how two rows end up sharing a slot again.
 *
 * LOCK ORDERING NOTE. The metapage lock is taken and released entirely
 * within this function, before the caller touches any other buffer. Do not
 * hoist it to span the row write: biscuit_pending_mutate_structure(),
 * biscuit_page_alloc() and biscuit_dir_ensure_root() all acquire the
 * metapage lock themselves, so holding it across them self-deadlocks (the
 * same hazard biscuit_dir_ensure_root() documents for its own P_NEW call).
 */
static uint32
biscuit_claim_new_slot(Relation index, BiscuitIndex *idx)
{
    Buffer               buf;
    Page                 page;
    GenericXLogState    *state;
    BiscuitMetaPageData *meta;
    uint32               slot;

    if (RelationGetNumberOfBlocks(index) == 0)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("biscuit: cannot claim a row slot in index \"%s\": no metapage",
                        RelationGetRelationName(index)),
                 errhint("The index may be corrupt; consider running REINDEX.")));

    biscuit_ensure_synchronous_commit();

    buf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(buf);
    if (PageIsNew(page))
    {
        UnlockReleaseBuffer(buf);
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("biscuit: cannot claim a row slot in index \"%s\": metapage uninitialized",
                        RelationGetRelationName(index)),
                 errhint("The index may be corrupt; consider running REINDEX.")));
    }

    meta = (BiscuitMetaPageData *) PageGetSpecialPointer(page);
    if (meta->magic != BISCUIT_MAGIC ||
        meta->version != BISCUIT_VERSION ||
        meta->page_format_version != BISCUIT_PAGE_FORMAT_VERSION)
    {
        UnlockReleaseBuffer(buf);
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("biscuit: cannot claim a row slot in index \"%s\": unrecognized metapage",
                        RelationGetRelationName(index)),
                 errhint("The index may be corrupt; consider running REINDEX.")));
    }

    state = GenericXLogStart(index);
    page  = GenericXLogRegisterBuffer(state, buf, 0);
    meta  = (BiscuitMetaPageData *) PageGetSpecialPointer(page);

    /*
     * Re-read the authoritative value from the page under the lock. We
     * deliberately do NOT consult idx->num_records here: that is this
     * backend's own possibly-stale copy, and trusting it is precisely the
     * defect being fixed.
     */
    slot = meta->num_records;
    meta->num_records = slot + 1;

    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);

    /*
     * Pull this backend's copy forward to the value we just published. It
     * may jump by more than one if other backends claimed slots since we
     * last looked; that is expected and correct. The gap slots belong to
     * other backends' rows, and this backend's view of them is a hole
     * (NULL data cache / invalid TID) until it next reloads from disk --
     * which is the same "cold reader must reload" contract that already
     * governs every other cross-backend mutation here.
     */
    if ((int) (slot + 1) > idx->num_records)
        idx->num_records = (int) (slot + 1);

    return slot;
}

/*
 * biscuit_ensure_slot_capacity
 * Grow the per-slot arrays so that `slot` is addressable, zero-filling the
 * newly allocated tail.
 *
 * This replaces the single "if (num_records >= capacity) capacity *= 2"
 * step that used to live inline in biscuit_insert(). It must loop now: a
 * claimed slot can be arbitrarily far past this backend's last-known
 * num_records (another backend may have claimed many slots since), so one
 * doubling is no longer guaranteed to be enough.
 *
 * Zero-filling is not optional -- see FIX A/B/C in the original inline
 * version. repalloc leaves the tail uninitialized, and both the NULL guard
 * in biscuit_insert() and the has_data test in biscuit_bulkdelete() treat
 * garbage bytes as a live pointer.
 */
static void
biscuit_ensure_slot_capacity(BiscuitIndex *idx, uint32 slot)
{
    int old_capacity = idx->capacity;
    int col;

    if ((int) slot < idx->capacity)
        return;

    while ((int) slot >= idx->capacity)
        idx->capacity *= 2;

    idx->tids = (ItemPointerData *) repalloc(idx->tids,
                                             idx->capacity * sizeof(ItemPointerData));
    /*
     * The TID tail must be explicitly invalidated, not merely left
     * uninitialized. biscuit_insert()'s duplicate-TID scan walks
     * [0, num_records) comparing idx->tids[i] against the incoming ctid,
     * and with cross-backend slot claiming that range can now contain
     * holes this backend never wrote. Garbage there could spuriously
     * compare equal and send the insert down the UPDATE path, overwriting
     * an unrelated slot.
     */
    for (int i = old_capacity; i < idx->capacity; i++)
        ItemPointerSetInvalid(&idx->tids[i]);

    if (idx->num_columns == 1)
    {
        idx->data_cache       = (char **) repalloc(idx->data_cache,
                                                   idx->capacity * sizeof(char *));
        idx->data_cache_lower = (char **) repalloc(idx->data_cache_lower,
                                                   idx->capacity * sizeof(char *));
        memset(idx->data_cache       + old_capacity, 0,
               (idx->capacity - old_capacity) * sizeof(char *));
        memset(idx->data_cache_lower + old_capacity, 0,
               (idx->capacity - old_capacity) * sizeof(char *));
    }
    else
    {
        for (col = 0; col < idx->num_columns; col++)
        {
            idx->column_data_cache[col] = (char **) repalloc(
                idx->column_data_cache[col], idx->capacity * sizeof(char *));
            memset(idx->column_data_cache[col] + old_capacity, 0,
                   (idx->capacity - old_capacity) * sizeof(char *));

            if (idx->column_data_cache_lower)
            {
                idx->column_data_cache_lower[col] = (char **) repalloc(
                    idx->column_data_cache_lower[col], idx->capacity * sizeof(char *));
                memset(idx->column_data_cache_lower[col] + old_capacity, 0,
                       (idx->capacity - old_capacity) * sizeof(char *));
            }
        }
    }
}

/* ================================================================
 * SECTION 2b – Shared-log mutation contract
 * ================================================================
 *
 * Every steady-state (post-build) CRUD mutation against a bitmap-shaped
 * structure goes through biscuit_pending_mutate_structure() below,
 * which is the *only* place outside biscuit_build()/biscuit_load_index()
 * and biscuit_pendlog_drain_all() itself that a structure's durable state
 * changes. It never decodes/re-encodes the compacted blob except when the
 * shared log has grown past BISCUIT_PENDLOG_DRAIN_PAGES, in which case it
 * drains the whole log through biscuit_pendlog_drain_all() exactly as
 * VACUUM's unconditional pass does (biscuit_vacuumcleanup()).
 *
 * This does NOT touch the in-memory RoaringBitmap* cache on
 * BiscuitIndex/ColumnIndex/CharIndex -- callers (biscuit_index_single_record,
 * biscuit_index_column_record, biscuit_remove_from_all_indices) keep
 * mutating that themselves, exactly as before, immediately before or
 * after calling this helper. The in-memory bitmap is this backend's own
 * read cache and stays correct for its own subsequent reads without
 * needing read-time reconciliation against its own not-yet-drained
 * pending records; that reconciliation (Phase 1 Contract §3) only
 * matters for a *different* backend reading the same structure.
 */

/*
 * biscuit_read_pending_stats
 * Share-locked read of the metapage's pending-list observability fields
 * (design doc §3 / Round 5): the configured per-structure drain
 * threshold, the last-known total undrained bytes across every
 * structure's pending chain (refreshed once per VACUUM by
 * biscuit_vacuumcleanup(), so this can be stale by up to one vacuum
 * cycle -- see the field comment on BiscuitMetaPageData.total_pending_bytes),
 * and the lifetime count of drains performed. Exposed to SQL via
 * biscuit_index_stats() (biscuit.c) so an operator can see "how much
 * unmerged write volume is sitting in this index right now" without
 * needing to walk the directory by hand.
 */
bool
biscuit_read_pending_stats(Relation index,
                            uint32 *pending_list_limit,
                            uint64 *total_pending_bytes,
                            uint64 *total_drains)
{
    Buffer               buf;
    Page                 page;
    BiscuitMetaPageData *meta;
    BlockNumber          nblocks = RelationGetNumberOfBlocks(index);

    if (nblocks == 0)
    {
        *pending_list_limit  = BISCUIT_DEFAULT_PENDING_LIST_LIMIT;
        *total_pending_bytes = 0;
        *total_drains        = 0;
        return false;
    }

    buf  = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);

    /*
     * Same always-true PageIsEmpty() gate as biscuit_read_metadata_from_disk()
     * had -- see biscuit_metapage_data(). Here the blast radius was
     * observability rather than correctness: biscuit_index_stats() reported
     * the default pending-list limit and zero pending bytes / zero drains
     * for every index, healthy or not, which is also why the undrained
     * write volume this counter exists to expose never showed up in the
     * GA report's pending-list figures.
     */
    meta = biscuit_metapage_data(page);
    if (meta == NULL)
    {
        UnlockReleaseBuffer(buf);
        *pending_list_limit  = BISCUIT_DEFAULT_PENDING_LIST_LIMIT;
        *total_pending_bytes = 0;
        *total_drains        = 0;
        return false;
    }

    *pending_list_limit  = meta->pending_list_limit ? meta->pending_list_limit
                                                     : BISCUIT_DEFAULT_PENDING_LIST_LIMIT;
    *total_pending_bytes = meta->total_pending_bytes;
    *total_drains        = meta->total_drains;

    UnlockReleaseBuffer(buf);
    return true;
}

/*
 * biscuit_read_pending_list_limit
 * Share-locked read of BiscuitMetaPageData.pending_list_limit, falling
 * back to BISCUIT_DEFAULT_PENDING_LIST_LIMIT for a metapage that
 * predates this field (0 = "unset"). Callers fetch this once per
 * statement (biscuit_insert/biscuit_bulkdelete each call it exactly
 * once) rather than once per structure mutated -- the GUC can't change
 * mid-statement, so there's no correctness reason to re-read it on
 * every one of the ~2*strlen(value) structures a single row touches.
 */
static uint32
biscuit_read_pending_list_limit(Relation index)
{
    Buffer               buf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    Page                 page;
    BiscuitMetaPageData *meta;
    uint32               limit;

    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page  = BufferGetPage(buf);
    meta  = (BiscuitMetaPageData *) PageGetSpecialPointer(page);
    limit = meta->pending_list_limit ? meta->pending_list_limit
                                      : BISCUIT_DEFAULT_PENDING_LIST_LIMIT;
    UnlockReleaseBuffer(buf);

    return limit;
}

/*
 * biscuit_load_blob_bitmap
 * Deserialize a structure's compacted-blob chain into a fresh in-memory
 * RoaringBitmap, or an empty bitmap if the structure has no compacted
 * blob yet (InvalidBlockNumber -- e.g. a structure whose only durable
 * state so far lives in its pending chain). Thin wrapper so drain call
 * sites (here and biscuit_vacuumcleanup()) don't repeat the
 * read-then-deserialize pair.
 */
/*
 * biscuit_load_blob_bitmap was removed: its only callers were the
 * per-structure pending-drain paths, which the shared pending log
 * replaced. biscuit_pendlog_drain_all() does the equivalent
 * read-blob-and-deserialize inline, once per structure per drain.
 */

/*
 * biscuit_pending_mutate_row
 *
 * The durable half of a row write: ONE fixed-size record into the
 * index-wide shared log saying "slot N was added" or "slot N was retired",
 * plus an opportunistic compaction if the log has outgrown its rebuild-time
 * budget.
 *
 * This replaces biscuit_pending_mutate_structure(), which was called once
 * per structure the row touched -- roughly 8N+4 times for an N-character
 * string, since every character contributes POS, NEG and CACHE per case
 * mode on top of LEN and the LEN_GE ladder. Every one of those records was
 * pure derivation: the set of structures a row belongs to is a function of
 * that row's text, and the text is already durable, already WAL-logged,
 * written in place and O(1) by
 * biscuit_persist_row_identity_write_record() a few hundred lines below.
 * Logging the fan-out as well was writing the same information to WAL a
 * second time in expanded form, and it accounted for the overwhelming
 * majority of this extension's write volume: 5131 B per row inserted into
 * a live index against 158 B for the heap alone, with pg_waldump
 * attributing 95.9% of an UPDATE window to our own Generic records.
 *
 * What consumes the resulting record: biscuit_delta.c, which reads the
 * row's text back out of STRCACHE and runs biscuit_fanout_string() over it
 * to reconstruct exactly the identities this function used to enumerate.
 * The fan-out has not disappeared -- it left the write path and became
 * delta-build work, paid on the first read after a write burst rather than
 * on every write. For load-then-query it is never paid at all, because the
 * delta is empty by query time.
 *
 * The multi-column case collapses for free: one row is one record no
 * matter how many columns are indexed, because each column's text lives in
 * STRCACHE under the same slot.
 */
/*
 * Deferred post-self-drain reload.
 *
 * A tiny process-local set of index OIDs whose in-memory copy is known to
 * have been invalidated by a drain THIS backend performed. Armed by
 * biscuit_resync_gen_after_self_drain(), consumed by
 * biscuit_get_current_index() at the next scan entry.
 *
 * A fixed array rather than a list: the count is bounded by the number of
 * distinct biscuit indexes one backend drains between two scans, which is
 * small, and overflow is handled by falling back to "reload everything"
 * rather than by growing. Over-reloading costs time; under-reloading
 * returns wrong answers.
 */
#define BISCUIT_MAX_DRAIN_RELOADS 16
static Oid  biscuit_drain_reload_oids[BISCUIT_MAX_DRAIN_RELOADS];
static int  biscuit_drain_reload_count = 0;
static bool biscuit_drain_reload_all   = false;

static void
biscuit_arm_self_drain_reload(Oid indexoid)
{
    int i;

    if (!OidIsValid(indexoid) || biscuit_drain_reload_all)
        return;

    for (i = 0; i < biscuit_drain_reload_count; i++)
        if (biscuit_drain_reload_oids[i] == indexoid)
            return;

    if (biscuit_drain_reload_count < BISCUIT_MAX_DRAIN_RELOADS)
        biscuit_drain_reload_oids[biscuit_drain_reload_count++] = indexoid;
    else
        biscuit_drain_reload_all = true;
}

/*
 * Consume the flag: true means this backend drained this index since the
 * last time it was asked, so its cached copy predates the drain.
 */
static bool
biscuit_consume_self_drain_reload(Oid indexoid)
{
    int i;

    if (biscuit_drain_reload_all)
    {
        biscuit_drain_reload_all   = false;
        biscuit_drain_reload_count = 0;
        return true;
    }

    for (i = 0; i < biscuit_drain_reload_count; i++)
    {
        if (biscuit_drain_reload_oids[i] == indexoid)
        {
            biscuit_drain_reload_oids[i] =
                biscuit_drain_reload_oids[--biscuit_drain_reload_count];
            return true;
        }
    }
    return false;
}

/*
 * biscuit_resync_gen_after_self_drain
 *
 * SELF-DRAIN CACHE-INVALIDATION FIX -- shared by every call site that may
 * have just driven a pendlog compaction/drain to completion itself.
 *
 * pendlog_detach_prefix() / pendlog_clear_draining() (biscuit_pendlog.c)
 * bump meta->gen unconditionally whenever a drain runs, on the (correct,
 * for every OTHER backend) assumption that a merge just changed durable
 * state that any cached BiscuitIndex must notice -- see
 * biscuit_get_current_index()'s comment. That reload is right for a
 * backend that did NOT do the merge. It is wrong for THIS backend, which
 * just performed the merge itself: every row it has written so far,
 * including the one that triggered the drain, is already reflected
 * directly in idx's in-memory bitmaps (the write path's fan-out mutates
 * them unconditionally, independent of what gets drained).
 * biscuit_persist_load() does NOT replay the pending log into a freshly
 * loaded idx -- that is what biscuit_reconcile_pending() is for, on the
 * READ path only -- so a self-triggered reload here would swap this
 * backend's complete, correct idx for one rebuilt from the durable HEADER
 * blob, which is flushed only at pre-commit (SECTION 0b above), not per
 * row. Any row written since the last flush -- including everything
 * written earlier in an in-flight, uncommitted statement -- would then be
 * silently missing from idx->tids/idx->num_records, even though its TID
 * was already durably written by biscuit_persist_row_identity_write_
 * record() and its bitmap membership is still sitting in the (still-
 * durable) pending log tail the write path never replays on load. That
 * combination -- a slot the reconciled read-time bitmap matches, with no
 * corresponding live entry in idx->tids -- is exactly what
 * biscuit_collect_sorted_tids_single() detects and reports as "scan
 * result includes slot N with no valid TID".
 *
 * The fix is not to skip the invalidation (other backends still need it)
 * but to keep THIS backend's bookkeeping in step with the merge it just
 * performed, so its own next staleness check in biscuit_get_current_index()
 * sees "nothing has changed that I don't already know about" rather than
 * "something changed -- discard everything".
 *
 * self_gen, NOT A FRESH DISK READ.
 *
 * This used to re-read meta->gen here via a second, independent call to
 * biscuit_read_metadata_from_disk() -- a plain BUFFER_LOCK_SHARE read,
 * taken after the drain that triggered this call had already released
 * its own serializing lock (LockPage(BISCUIT_METAPAGE_BLKNO,
 * ExclusiveLock) in pendlog_drain_internal()). That left a genuine race:
 * meta->gen is a single counter bumped by every backend's drain,
 * including autovacuum's biscuit_bulkdelete(), and nothing stops a
 * concurrent drain from running -- and bumping gen further -- in the gap
 * between this backend's own drain releasing its lock and this function's
 * separate probe. Trusting that later, inflated value as "the gen my own
 * drain produced" made biscuit_get_current_index()'s staleness check
 * (disk_gen <= idx->gen) permanently pass, even though idx's in-memory
 * bitmaps and idx->tids[] had incorporated only this backend's own
 * writes, not the concurrent drain's. The result: a stale bitmap match
 * for a slot a concurrent DELETE had already durably killed and folded
 * out of the live pending log (so read-time reconciliation had nothing
 * left to subtract), surfacing later as "scan result includes slot N
 * with no valid TID" -- non-deterministically, timed by autovacuum.
 *
 * The caller now passes the exact meta->gen value ITS OWN drain produced,
 * captured by pendlog_clear_draining() while still holding that drain's
 * serializing lock (see biscuit_pendlog_compact()/biscuit_pendlog_
 * drain_all()'s out_gen parameter). That value cannot be contaminated by
 * a concurrent drain -- the serializing lock rules that out -- so
 * advancing idx->gen to it is safe regardless of what else lands on disk
 * afterward. self_gen == 0 means the caller's drain call found nothing to
 * drain (pendlog_drain_internal()'s early-return sentinel); there is
 * nothing to resync in that case.
 *
 * Callers: the opportunistic bounded-prefix compact triggered inline from
 * biscuit_pending_mutate_row() below, and the deferred whole-log drain
 * biscuit_pendlog_batch_end() runs after a batched append armed
 * want_drain (biscuit_pendlog.c) -- the latter is the path steady-state
 * single-row INSERT/UPDATE actually takes, since biscuit_insert() keeps a
 * batch open across the whole row and biscuit_pendlog_append() cannot
 * compact synchronously while holding the tail page's content lock.
 */
void
biscuit_resync_gen_after_self_drain(Relation index, BiscuitIndex *idx,
                                    uint64 self_gen)
{
    if (idx == NULL || index == NULL)
        return;

    if (self_gen == 0)
        return;   /* the caller's drain found nothing to drain */

    if (self_gen > idx->gen)
    {
        idx->gen                  = self_gen;
        idx->gen_at_last_snapshot = self_gen;
    }

    /*
     * THE RESYNC IS NOT SUFFICIENT ON ITS OWN -- arm a deferred reload.
     *
     * Advancing idx->gen here suppresses biscuit_get_current_index()'s
     * staleness check, which is correct for its stated purpose (not
     * rebuilding a complete in-memory index mid-statement from a HEADER blob
     * that only gets flushed at pre-commit). But suppressing that check also
     * suppresses the ONLY mechanism this backend has for noticing that its
     * in-memory base bitmaps just went stale -- and the drain that just ran
     * is exactly what made them stale.
     *
     * A drain folds the pending records into the on-disk compacted blobs and
     * clears the log. It operates directly against the on-disk directory
     * with no in-memory BiscuitIndex involved (see biscuit_cache.c's
     * module-unload comment, which states this explicitly). So immediately
     * after a self-drain this backend holds:
     *
     *   - on-disk blobs that DO contain the drained slots;
     *   - in-memory base bitmaps that do NOT, because nothing updated them;
     *   - a pending log that no longer mentions them, because the drain
     *     consumed it;
     *   - an idx->gen resynced forward, so no reload will be triggered.
     *
     * The drained slots are then in neither half of the read path. Read-time
     * reconciliation finds a snapshot (pend=hit), finds nothing to add for
     * those identities, and returns the stale base unchanged -- the
     * "merged == from while pend=hit" signature. Every property of the
     * resulting failure follows from this: backend-local (another backend
     * reloads and is fine), post-build rows only (build rows were already in
     * base), omission-only and never extra (a missing add, not a bad one),
     * silent (nothing is inconsistent enough to trip a guard), and
     * correlated with drains.
     *
     * Reloading synchronously here is not safe -- the caller may be inside
     * biscuit_insert() holding `idx`, and evicting the cache would not
     * invalidate that local pointer. So arm a flag instead and let
     * biscuit_get_current_index() act on it at the next scan entry, which is
     * past the end of the mutating statement.
     *
     * That deferred reload is safe now in a way it was not when this resync
     * was written. The objection recorded above -- that a reload rebuilds
     * from a HEADER blob lagging the in-flight statement -- was really an
     * objection about num_records, and biscuit_persist_load() now reconciles
     * that against the metapage's authoritative slot watermark rather than
     * trusting the header. TIDs and STRCACHE are per-row durable, and the
     * base bitmaps come from the directory blobs the drain just wrote, so a
     * post-drain reload reads strictly better state than the copy it
     * replaces.
     */
    biscuit_arm_self_drain_reload(RelationGetRelid(index));
}

static void
biscuit_pending_mutate_row(Relation index, BiscuitIndex *idx, uint32 slot, uint8 op,
                            uint32 pending_list_limit)
{
    uint64 pendlog_bytes;

    if (index == NULL)
        return;   /* build/load: in-memory only; biscuit_build() persists
                    * everything in one bulk pass at the end instead */

    pendlog_bytes = biscuit_pendlog_append(index, slot, op);

    /*
     * Compaction trigger.
     *
     * Two things changed here, and they are separable.
     *
     * The threshold is now denominated in ROWS
     * (biscuit.delta_compaction_slots), not bytes, because rows are what
     * set delta rebuild time and therefore read latency. At ~3.9 kB of
     * derived records per row the old 4 MB byte threshold worked out to a
     * few thousand rows; at 8 bytes per row the identical byte figure
     * admits on the order of 500x more, which is a rebuild budget nobody
     * chose. biscuit_pendlog_drain_trigger_bytes() converts.
     *
     * And this compacts a PREFIX rather than draining the whole log. A
     * full drain rewrites every structure the log touched, so firing one
     * from the append path makes a bulk load quadratic -- (N/limit) drains
     * times O(index size) each. Shipping a bounded prefix bounds the work
     * per trigger while still keeping the delta small enough to rebuild
     * quickly. VACUUM still drains unconditionally and completely.
     *
     * The batch check is now vestigial: biscuit_insert() no longer opens a
     * row batch, because a row is one record and there is nothing left to
     * batch. It is kept because biscuit_pendlog_batch_active() is cheap and
     * because compacting while holding the tail page's content lock would
     * deadlock if a batch ever returns.
     */
    if (pendlog_bytes > biscuit_pendlog_drain_trigger_bytes() &&
        !biscuit_pendlog_batch_active())
    {
        uint64 self_gen;

        biscuit_pendlog_compact(index, false,   /* opportunistic: skip if
                                                 * another backend is
                                                 * already draining */
                                 BISCUIT_PENDLOG_COMPACT_PAGES,
                                 &self_gen);

        /*
         * SELF-COMPACTION CACHE-INVALIDATION FIX.
         *
         * biscuit_pendlog_compact() -> pendlog_detach_prefix() /
         * pendlog_clear_draining() bump meta->gen unconditionally, on the
         * (correct, for every OTHER backend) assumption that a merge just
         * changed durable state that any cached BiscuitIndex must notice.
         * biscuit_get_current_index() enforces that by discarding and
         * reloading from disk on the next call whose idx->gen is behind
         * meta->gen -- see its comment.
         *
         * That reload is right for a backend that did NOT do the merge:
         * its cached bitmaps are stale relative to what just landed on
         * disk. It is wrong for THIS backend, which just performed the
         * merge itself. Every row this backend has written so far --
         * including the one that triggered this compaction -- is already
         * reflected directly in idx's in-memory bitmaps (biscuit_index.c's
         * write-time in-memory fan-out mutates them unconditionally,
         * independent of what's drained). biscuit_persist_load() does NOT
         * replay the pending log into a freshly loaded idx (that's what
         * biscuit_reconcile_pending() is for, on the READ path only -- the
         * write path deliberately bypasses it, see inmem_fanout_emit()'s
         * index=NULL calls and biscuit_pattern.h's contract note, because
         * it assumes idx already IS the complete, current truth). A
         * self-triggered reload here would swap this backend's complete,
         * correct idx for a base-only copy that is missing every row
         * written since the last full drain, INCLUDING rows this same
         * backend already durably wrote and whose only record of
         * membership is now the (still-pending, still-durable) log tail
         * that the write path will never reconcile against. Every
         * subsequent write in this session then silently omits those
         * rows' identities from the bitmaps it builds on top of the
         * reloaded base -- a backend-local, read-visible undercount that
         * nothing downstream catches, because xs_recheck is false and the
         * rows are not wrong, just silently absent.
         *
         * The fix is not to skip the invalidation (other backends still
         * need it) but to keep THIS backend's bookkeeping in step with the
         * merge it just performed, so its own next staleness check sees
         * "nothing has changed that I don't already know about" rather
         * than "something changed -- discard everything".
         *
         * self_gen is the exact meta->gen this compaction produced,
         * captured under its own serializing lock by pendlog_clear_
         * draining() -- not a second, independently-locked read of the
         * metapage taken after that lock is released. A concurrent
         * drain by another backend (autovacuum's biscuit_bulkdelete(),
         * most plausibly) can advance meta->gen further in exactly that
         * gap; trusting a fresh read there as "what my own compaction
         * produced" would silently adopt a gen value idx's in-memory
         * bitmaps never actually caught up to. See
         * biscuit_resync_gen_after_self_drain()'s header comment for the
         * full history of that failure mode.
         */
        biscuit_resync_gen_after_self_drain(index, idx, self_gen);
    }

    (void) pending_list_limit;   /* retained in the signature for the
                                   * statement-cached-read contract in
                                   * biscuit_index.h; it was never the
                                   * trigger for the shared log, and is even
                                   * less meaningful now that the threshold
                                   * is counted in rows */
}

/*
 * Remove a record from every character and length bitmap.
 * Handles both single-column (legacy) and multi-column layouts.
 *
 * TWO HALVES THAT NO LONGER MIRROR EACH OTHER.
 *
 * The in-memory half is unchanged: walk every structure this backend has
 * cached and biscuit_roaring_remove() the slot from each. That still has
 * to enumerate, because a cached bitmap is a concrete object with the slot
 * concretely in it.
 *
 * The durable half is now a single record. It used to be one
 * biscuit_pending_mutate_structure() call per structure, appended inside
 * the loop below, so deleting one N-character row wrote ~8N+4 records
 * naming every structure it belonged to. That made DELETE the most
 * expensive operation in the extension, and all of it was derivation.
 *
 * The reconciler does NOT recover that list by re-deriving it. It cannot:
 * by the time anything reads this record, an UPDATE may already have
 * overwritten STRCACHE with the replacement row's text, so the identities
 * this slot used to have are unrecoverable. Instead the slot joins the
 * kill set, and every base bitmap a reader touches has the kill set
 * subtracted from it before the delta's additions are applied (see
 * biscuit_pendlog_apply()). That is order-independent and survives slot
 * recycling, neither of which a list of per-structure removals would.
 *
 * The append happens ONCE, up front, and is deliberately not conditional
 * on the loop below finding anything: a slot with no cached structures in
 * THIS backend may still be present in the base blobs, and this record is
 * what retires it there.
 *
 * `index`/`pending_list_limit` are always required -- every call site is
 * steady-state CRUD (biscuit_insert's UPDATE-as-delete-then-insert path,
 * biscuit_bulkdelete's tombstone purge), never the one-time build path.
 */
void
biscuit_remove_from_all_indices(Relation index, BiscuitIndex *idx,
                                 uint32_t rec_idx, uint32 pending_list_limit)
{
    int ch, j, col;

    if (!idx)
        return;

    biscuit_pending_mutate_row(index, idx, (uint32) rec_idx,
                                BISCUIT_PENDING_OP_REMOVE, pending_list_limit);

    /* -------- Multi-column -------- */
    if (idx->num_columns > 1 && idx->column_indices)
    {
        for (col = 0; col < idx->num_columns; col++)
        {
            ColumnIndex *cidx = &idx->column_indices[col];

            for (ch = 0; ch < CHAR_RANGE; ch++)
            {
                /* case-sensitive */
                for (j = 0; j < cidx->pos_idx[ch].count; j++)
                {
                    biscuit_roaring_remove(cidx->pos_idx[ch].entries[j].bitmap, rec_idx);
                }
                for (j = 0; j < cidx->neg_idx[ch].count; j++)
                {
                    biscuit_roaring_remove(cidx->neg_idx[ch].entries[j].bitmap, rec_idx);
                }
                if (cidx->char_cache[ch])
                {
                    biscuit_roaring_remove(cidx->char_cache[ch], rec_idx);
                }

                /* case-insensitive */
                for (j = 0; j < cidx->pos_idx_lower[ch].count; j++)
                {
                    biscuit_roaring_remove(cidx->pos_idx_lower[ch].entries[j].bitmap, rec_idx);
                }
                for (j = 0; j < cidx->neg_idx_lower[ch].count; j++)
                {
                    biscuit_roaring_remove(cidx->neg_idx_lower[ch].entries[j].bitmap, rec_idx);
                }
                if (cidx->char_cache_lower[ch])
                {
                    biscuit_roaring_remove(cidx->char_cache_lower[ch], rec_idx);
                }
            }

            if (cidx->length_bitmaps)
                for (j = 0; j < cidx->max_length; j++)
                    if (cidx->length_bitmaps[j])
                    {
                        biscuit_roaring_remove(cidx->length_bitmaps[j], rec_idx);
                    }

            if (cidx->length_ge_bitmaps)
                for (j = 0; j < cidx->max_length; j++)
                    if (cidx->length_ge_bitmaps[j])
                    {
                        biscuit_roaring_remove(cidx->length_ge_bitmaps[j], rec_idx);
                    }

            if (cidx->length_bitmaps_lower)
                for (j = 0; j < cidx->max_length_lower; j++)
                    if (cidx->length_bitmaps_lower[j])
                    {
                        biscuit_roaring_remove(cidx->length_bitmaps_lower[j], rec_idx);
                    }

            if (cidx->length_ge_bitmaps_lower)
                for (j = 0; j < cidx->max_length_lower; j++)
                    if (cidx->length_ge_bitmaps_lower[j])
                    {
                        biscuit_roaring_remove(cidx->length_ge_bitmaps_lower[j], rec_idx);
                    }
        }
        return;
    }

    /* -------- Single-column (legacy) -------- */
    /* Legacy single-column structures address as col = -1, per
     * biscuit_dir_slot_for_col()'s convention (biscuit_dir.c). */
    for (ch = 0; ch < CHAR_RANGE; ch++)
    {
        /* case-sensitive */
        for (j = 0; j < idx->pos_idx_legacy[ch].count; j++)
        {
            biscuit_roaring_remove(idx->pos_idx_legacy[ch].entries[j].bitmap, rec_idx);
        }
        for (j = 0; j < idx->neg_idx_legacy[ch].count; j++)
        {
            biscuit_roaring_remove(idx->neg_idx_legacy[ch].entries[j].bitmap, rec_idx);
        }
        if (idx->char_cache_legacy[ch])
        {
            biscuit_roaring_remove(idx->char_cache_legacy[ch], rec_idx);
        }

        /* case-insensitive */
        for (j = 0; j < idx->pos_idx_lower[ch].count; j++)
        {
            biscuit_roaring_remove(idx->pos_idx_lower[ch].entries[j].bitmap, rec_idx);
        }
        for (j = 0; j < idx->neg_idx_lower[ch].count; j++)
        {
            biscuit_roaring_remove(idx->neg_idx_lower[ch].entries[j].bitmap, rec_idx);
        }
        if (idx->char_cache_lower[ch])
        {
            biscuit_roaring_remove(idx->char_cache_lower[ch], rec_idx);
        }
    }

    if (idx->max_length_legacy > 0)
    {
        for (j = 0; j < idx->max_length_legacy; j++)
        {
            if (idx->length_bitmaps_legacy && idx->length_bitmaps_legacy[j])
            {
                biscuit_roaring_remove(idx->length_bitmaps_legacy[j], rec_idx);
            }
            if (idx->length_ge_bitmaps_legacy && idx->length_ge_bitmaps_legacy[j])
            {
                biscuit_roaring_remove(idx->length_ge_bitmaps_legacy[j], rec_idx);
            }
        }
    }

    if (idx->max_length_lower > 0)
    {
        for (j = 0; j < idx->max_length_lower; j++)
        {
            if (idx->length_bitmaps_lower && idx->length_bitmaps_lower[j])
            {
                biscuit_roaring_remove(idx->length_bitmaps_lower[j], rec_idx);
            }
            if (idx->length_ge_bitmaps_lower && idx->length_ge_bitmaps_lower[j])
            {
                biscuit_roaring_remove(idx->length_ge_bitmaps_lower[j], rec_idx);
            }
        }
    }
}

/* ================================================================
 * SECTION 3 – Index build
 * ================================================================
 *
 * The build functions scan the heap and populate all in-memory
 * structures.  Multi-column logic mirrors single-column but fans out
 * across ColumnIndex instances.  For brevity the CharIndex insertion
 * helpers are imported from biscuit_pattern.c via the static linkage
 * within the same translation unit; they are re-declared here as
 * forward references through biscuit_pattern.h.
 */

#include "biscuit_pattern.h"   /* for set_pos/neg_bitmap helpers etc */

/* ================================================================
 * IN-MEMORY FAN-OUT SINK
 * ================================================================
 *
 * The write path's half of "one fan-out, three callers".
 *
 * biscuit_index_single_record() and biscuit_index_column_record() used to
 * contain their own copy of the rule for which structures a string belongs
 * to -- the same rule biscuit_fanout_string() now owns and the delta
 * builder depends on. Two implementations of that rule is the risk the
 * design lists as "delta and base disagree on fan-out", and it is a
 * particularly unpleasant one: the two would not disagree loudly, they
 * would disagree on some strings and not others, and the in-memory copy is
 * what serves reads for the writing backend until its next reload.
 *
 * So these functions now drive the shared fan-out and only supply the sink.
 * All this callback does is resolve an identity to the right in-memory
 * bitmap, creating it if absent -- no decisions about what the identities
 * ARE.
 *
 * Only ADD is ever emitted here. Removal is biscuit_remove_from_all_indices()'s
 * job, and it enumerates cached structures rather than fanning out, because
 * a cached bitmap is a concrete object with the slot concretely in it.
 */

typedef struct InMemFanoutCtx
{
    BiscuitIndex *idx;
    ColumnIndex  *cidx;         /* NULL for the legacy single-column layout */
    bool          do_lengths;   /* maintain LEN / LEN_GE arrays here? */
} InMemFanoutCtx;

/*
 * Grow a (length_bitmaps, length_ge_bitmaps, max_length) triple so that
 * `need` is a valid index, preserving the exact growth policy the four
 * open-coded copies in biscuit_insert() used: new capacity (need + 1) * 2,
 * LEN slots left NULL and created on demand, LEN_GE slots pre-created.
 *
 * max_length is the ALLOCATED CAPACITY of these arrays, not a
 * "longest string seen" counter, and treating it as the latter is a bug
 * with a history here: biscuit_index_single_record() used to bump it,
 * which made biscuit_insert() read an already-bumped value as the old
 * capacity, leaving a gap of uninitialized RoaringBitmap* entries that
 * crashed inside libroaring on the next longer insert. Only this function
 * moves it.
 */
static void
inmem_grow_lengths(RoaringBitmap ***len_arr, RoaringBitmap ***len_ge_arr,
                    int *max_length, int need)
{
    int old_ml = *max_length;
    int new_ml;
    int i;

    if (need < old_ml)
        return;

    new_ml = (need + 1) * 2;

    if (*len_arr)
        *len_arr = (RoaringBitmap **) repalloc(*len_arr, new_ml * sizeof(RoaringBitmap *));
    else
        *len_arr = (RoaringBitmap **) palloc0(new_ml * sizeof(RoaringBitmap *));

    if (*len_ge_arr)
        *len_ge_arr = (RoaringBitmap **) repalloc(*len_ge_arr, new_ml * sizeof(RoaringBitmap *));
    else
        *len_ge_arr = (RoaringBitmap **) palloc0(new_ml * sizeof(RoaringBitmap *));

    for (i = old_ml; i < new_ml; i++)
    {
        (*len_arr)[i]    = NULL;
        (*len_ge_arr)[i] = biscuit_roaring_create();
    }

    *max_length = new_ml;
}

static void
inmem_fanout_emit(void *ctxp,
                  int32 col, bool is_lower, uint8 kind,
                  int32 ch, int32 position, uint32 slot, uint8 op)
{
    InMemFanoutCtx *fc  = (InMemFanoutCtx *) ctxp;
    BiscuitIndex   *idx = fc->idx;
    ColumnIndex    *cidx = fc->cidx;
    unsigned char   uch = (unsigned char) ch;
    RoaringBitmap  *bm;

    Assert(op == BISCUIT_PENDING_OP_ADD);
    (void) op;
    (void) col;   /* the ColumnIndex to write into is already resolved in ctx */

    switch (kind)
    {
        case BISCUIT_DIR_KIND_POS:
            if (cidx == NULL)
            {
                /*
                 * index == NULL to the getter, deliberately: the write path
                 * needs the raw live pointer to mutate in place, not a
                 * reconciled copy. See biscuit_pattern.h's contract note.
                 */
                if (is_lower)
                {
                    bm = biscuit_get_pos_bitmap_lower(NULL, idx, uch, position);
                    if (!bm)
                    {
                        bm = biscuit_roaring_create();
                        biscuit_set_pos_bitmap_lower(idx, uch, position, bm);
                    }
                }
                else
                {
                    bm = biscuit_get_pos_bitmap(NULL, idx, uch, position);
                    if (!bm)
                    {
                        bm = biscuit_roaring_create();
                        biscuit_set_pos_bitmap(idx, uch, position, bm);
                    }
                }
            }
            else
            {
                if (is_lower)
                {
                    bm = biscuit_get_col_pos_bitmap_lower(NULL, cidx, (int) col, uch, position);
                    if (!bm)
                    {
                        bm = biscuit_roaring_create();
                        biscuit_set_col_pos_bitmap_lower(cidx, uch, position, bm);
                    }
                }
                else
                {
                    bm = biscuit_get_col_pos_bitmap(NULL, cidx, (int) col, uch, position);
                    if (!bm)
                    {
                        bm = biscuit_roaring_create();
                        biscuit_set_col_pos_bitmap(cidx, uch, position, bm);
                    }
                }
            }
            biscuit_roaring_add(bm, slot);
            break;

        case BISCUIT_DIR_KIND_NEG:
            if (cidx == NULL)
            {
                if (is_lower)
                {
                    bm = biscuit_get_neg_bitmap_lower(NULL, idx, uch, position);
                    if (!bm)
                    {
                        bm = biscuit_roaring_create();
                        biscuit_set_neg_bitmap_lower(idx, uch, position, bm);
                    }
                }
                else
                {
                    bm = biscuit_get_neg_bitmap(NULL, idx, uch, position);
                    if (!bm)
                    {
                        bm = biscuit_roaring_create();
                        biscuit_set_neg_bitmap(idx, uch, position, bm);
                    }
                }
            }
            else
            {
                if (is_lower)
                {
                    bm = biscuit_get_col_neg_bitmap_lower(NULL, cidx, (int) col, uch, position);
                    if (!bm)
                    {
                        bm = biscuit_roaring_create();
                        biscuit_set_col_neg_bitmap_lower(cidx, uch, position, bm);
                    }
                }
                else
                {
                    bm = biscuit_get_col_neg_bitmap(NULL, cidx, (int) col, uch, position);
                    if (!bm)
                    {
                        bm = biscuit_roaring_create();
                        biscuit_set_col_neg_bitmap(cidx, uch, position, bm);
                    }
                }
            }
            biscuit_roaring_add(bm, slot);
            break;

        case BISCUIT_DIR_KIND_CACHE:
        {
            RoaringBitmap **cache;

            if (cidx == NULL)
                cache = is_lower ? &idx->char_cache_lower[uch]
                                 : &idx->char_cache_legacy[uch];
            else
                cache = is_lower ? &cidx->char_cache_lower[uch]
                                 : &cidx->char_cache[uch];

            if (*cache == NULL)
                *cache = biscuit_roaring_create();
            biscuit_roaring_add(*cache, slot);
            break;
        }

        case BISCUIT_DIR_KIND_LEN:
        case BISCUIT_DIR_KIND_LEN_GE:
        {
            RoaringBitmap ***len_arr;
            RoaringBitmap ***len_ge_arr;
            int             *max_length;

            if (!fc->do_lengths)
                break;

            if (cidx == NULL)
            {
                if (is_lower)
                {
                    len_arr    = &idx->length_bitmaps_lower;
                    len_ge_arr = &idx->length_ge_bitmaps_lower;
                    max_length = &idx->max_length_lower;
                }
                else
                {
                    len_arr    = &idx->length_bitmaps_legacy;
                    len_ge_arr = &idx->length_ge_bitmaps_legacy;
                    max_length = &idx->max_length_legacy;
                }
            }
            else
            {
                if (is_lower)
                {
                    len_arr    = &cidx->length_bitmaps_lower;
                    len_ge_arr = &cidx->length_ge_bitmaps_lower;
                    max_length = &cidx->max_length_lower;
                }
                else
                {
                    len_arr    = &cidx->length_bitmaps;
                    len_ge_arr = &cidx->length_ge_bitmaps;
                    max_length = &cidx->max_length;
                }
            }

            inmem_grow_lengths(len_arr, len_ge_arr, max_length, (int) position);

            if (kind == BISCUIT_DIR_KIND_LEN)
            {
                if (!(*len_arr)[position])
                    (*len_arr)[position] = biscuit_roaring_create();
                biscuit_roaring_add((*len_arr)[position], slot);
            }
            else
            {
                if (!(*len_ge_arr)[position])
                    (*len_ge_arr)[position] = biscuit_roaring_create();
                biscuit_roaring_add((*len_ge_arr)[position], slot);
            }
            break;
        }

        default:
            /* No other kind is a bitmap structure the fan-out can produce. */
            break;
    }
}

/*
 * Helper: add a single text record to the single-column (legacy) index.
 * Called both from biscuit_build()/biscuit_load_index() (one-time bulk
 * build/load) and from biscuit_insert() (steady-state CRUD).
 *
 * index == NULL means the build/load path. Two things follow from it:
 * no durable pending-log append (biscuit_build() persists everything in
 * one bulk pass at the end), and no LEN/LEN_GE maintenance here, because
 * biscuit_build() has a dedicated length-bitmap pass that recomputes those
 * arrays from scratch after every record is in. The steady-state insert
 * path has no such pass, so it maintains them here -- which is where the
 * four open-coded copies that used to live in biscuit_insert() went.
 */
static void
biscuit_index_single_record(Relation      index,
                             BiscuitIndex *idx,
                             const char   *str,
                             int           byte_len,
                             int           rec_idx,
                             uint32        pending_list_limit)
{
    uint8          mode       = idx->legacy_case_mode;
    int            char_count = biscuit_utf8_char_count(str, byte_len);
    char          *str_lower  = NULL;
    int            lower_len  = 0;
    InMemFanoutCtx fc;

    (void) pending_list_limit;   /* the row's single log record is appended
                                   * by biscuit_insert(), after the row's
                                   * text is durable -- not here, once per
                                   * structure, as it used to be */

    /*
     * The lowercased copy is computed ONCE, here, and handed to the
     * fan-out. It is never recomputed downstream: biscuit_str_tolower()
     * calls PostgreSQL's collation-dependent lower(), so a second
     * derivation of the same bytes -- in another backend, or on a standby
     * running a different ICU/libc -- could disagree. The delta builder
     * reads these bytes back from STRCACHE for the same reason.
     */
    if (mode & BISCUIT_MODE_ILIKE)
    {
        str_lower = biscuit_str_tolower(str, byte_len);
        lower_len = (int) strlen(str_lower);
        idx->data_cache_lower[rec_idx] = str_lower;
    }
    else
    {
        /*
         * This column's opclass (biscuit_like_ops) never needs the
         * case-insensitive structures -- leave the lowercased cache slot
         * NULL and skip building any "_lower" bitmaps for this record.
         */
        idx->data_cache_lower[rec_idx] = NULL;
    }

    fc.idx        = idx;
    fc.cidx       = NULL;
    fc.do_lengths = (index != NULL);

    biscuit_fanout_string(str, byte_len, str_lower, lower_len,
                          BISCUIT_DIR_COL_LEGACY, mode, (uint32) rec_idx,
                          -1, BISCUIT_PENDING_OP_ADD,
                          inmem_fanout_emit, &fc);

    /* Track max case-sensitive character length */
    if (char_count > idx->max_len)
        idx->max_len = char_count;
}

/*
 * biscuit_index_column_record
 * ----------------------------
 * Multi-column analogue of biscuit_index_single_record(): populate every
 * in-memory structure this string implies into column `col`'s ColumnIndex.
 *
 * Same index == NULL contract, same do_lengths reasoning, same
 * compute-lower-once-and-pass-it rule.
 */
static void
biscuit_index_column_record(Relation      index,
                             BiscuitIndex *idx,
                             int           col,
                             const char   *str,
                             int           byte_len,
                             int           rec_idx,
                             uint32        pending_list_limit)
{
    ColumnIndex   *cidx      = &idx->column_indices[col];
    uint8          mode      = idx->column_case_mode ? idx->column_case_mode[col]
                                                     : BISCUIT_MODE_BOTH;
    char          *str_lower = NULL;
    int            lower_len = 0;
    InMemFanoutCtx fc;

    (void) pending_list_limit;   /* see biscuit_index_single_record() */

    /*
     * Prefer the lowercased copy the caller already computed and stored in
     * column_data_cache_lower -- biscuit_insert() populates it before
     * calling here, so recomputing would both waste a lower() call and
     * risk two derivations of the same bytes. Fall back to computing it
     * only on the build path, which does not pre-populate that array.
     */
    if (mode & BISCUIT_MODE_ILIKE)
    {
        if (idx->column_data_cache_lower &&
            idx->column_data_cache_lower[col] &&
            idx->column_data_cache_lower[col][rec_idx])
        {
            str_lower = idx->column_data_cache_lower[col][rec_idx];
            lower_len = (int) strlen(str_lower);
        }
        else
        {
            str_lower = biscuit_str_tolower(str, byte_len);
            lower_len = (int) strlen(str_lower);
        }
    }

    fc.idx        = idx;
    fc.cidx       = cidx;
    fc.do_lengths = (index != NULL);

    biscuit_fanout_string(str, byte_len, str_lower, lower_len,
                          (int32) col, mode, (uint32) rec_idx,
                          -1, BISCUIT_PENDING_OP_ADD,
                          inmem_fanout_emit, &fc);

    /*
     * Only free a copy this function made. When it came from
     * column_data_cache_lower it belongs to idx and outlives us.
     */
    if (str_lower != NULL &&
        !(idx->column_data_cache_lower &&
          idx->column_data_cache_lower[col] &&
          idx->column_data_cache_lower[col][rec_idx] == str_lower))
        pfree(str_lower);
}

/*
 * Build a brand-new index from the heap.  Returns an IndexBuildResult.
 * Handles both single-column and multi-column cases.
 */
IndexBuildResult *
biscuit_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    BiscuitIndex     *idx;
    TupleTableSlot   *slot;
    TableScanDesc     scan;
    MemoryContext     oldcontext;
    int               ch, natts, col, rec_idx;
    EState           *estate;
    ExprContext      *econtext;
    Datum             index_values[INDEX_MAX_KEYS];
    bool              index_isnull[INDEX_MAX_KEYS];

    /*
     * FIX #10 — expression index columns (e.g. USING biscuit((col::text))
     * or USING biscuit(lower(col2))) failed with "cache lookup failed for
     * type 0" / "... type 4294967295".
     *
     * Root cause: the type and value derivation below previously assumed
     * every index column is a plain attribute reference. It read
     * index->rd_index->indkey.values[col] and looked that attnum up
     * directly in the *heap's* tuple descriptor (heap->rd_att) to get the
     * type, then used slot_getattr() with that same attnum to fetch the
     * value. For a plain column this attnum is the real 1-based heap
     * attribute number, so it happened to work. For an expression column,
     * PostgreSQL stores indkey.values[col] as 0 (InvalidAttrNumber) --
     * there is no underlying heap attribute for an expression -- so
     * TupleDescAttr(heap->rd_att, 0 - 1) read attribute -1 (garbage,
     * explaining the bogus type OIDs 0 / 4294967295), and the matching
     * slot_getattr(slot, 0, ...) call was equally invalid (valid attnums
     * are 1-based). Expressions were never evaluated anywhere in this
     * file.
     *
     * Fix: get the type from the index's own tuple descriptor
     * (RelationGetDescr(index)) instead of the heap's -- this is correct
     * for both plain columns and expressions, since PostgreSQL always
     * populates the index tuple descriptor with the actual result type of
     * each key. Get the value via FormIndexDatum(), which evaluates
     * whatever the key actually is (plain Var or arbitrary expression)
     * against the current heap tuple slot, exactly like every built-in AM
     * (btree, gin, gist, ...) does. This requires a per-tuple ExprContext,
     * set up once via CreateExecutorState() below and reset per row.
     */
    estate   = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);

    /*
     * All BiscuitIndex data must live in CacheMemoryContext, not in
     * rd_indexcxt.  PostgreSQL calls MemoryContextDelete(rd_indexcxt) inside
     * RelationClearRelation on any relcache invalidation (ANALYZE, DDL, cache
     * sweeps), which would free all our data while the cache entry still holds
     * the pointer.  CacheMemoryContext is never reset by PostgreSQL and is the
     * correct long-lived home for session-scoped index structures.
     */
    oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

    PG_TRY();
    {
        natts = index->rd_index->indnatts;

        idx               = (BiscuitIndex *) palloc0(sizeof(BiscuitIndex));
        idx->capacity     = 1024;
        idx->num_records  = 0;
        idx->num_columns  = natts;
        idx->max_len      = 0;
        idx->tids         = (ItemPointerData *) palloc(idx->capacity * sizeof(ItemPointerData));

        /*
         * A freshly built index starts life at generation 0.  gen is
         * bumped from here on by biscuit_insert()/biscuit_bulkdelete();
         * gen_at_last_snapshot is set to match once the snapshot below is
         * actually taken, so it starts "in sync" rather than falsely
         * looking stale.
         */
        idx->gen                 = 0;
        idx->gen_at_last_snapshot = 0;

        if (natts == 1)
        {
            /* ---- Single-column initialisation ---- */
            Oid      typoutput;
            bool     typIsVarlena;
            Oid      coltypid = TupleDescAttr(RelationGetDescr(index), 0)->atttypid;
            FmgrInfo single_output_func;

            getTypeOutputInfo(coltypid, &typoutput, &typIsVarlena);
            fmgr_info(typoutput, &single_output_func);

            /*
             * Determine which structure set(s) this column's opclass
             * actually needs -- biscuit_like_ops skips the "_lower"
             * (ILIKE) structures below, biscuit_ilike_ops skips the
             * case-sensitive (LIKE) ones, and biscuit_ops (or an
             * unrecognized opfamily) builds both.
             */
            idx->legacy_case_mode = biscuit_get_column_case_mode(index, 0);

            idx->data_cache = (char **) palloc0(idx->capacity * sizeof(char *));
            idx->data_cache_lower = (char **) palloc0(idx->capacity * sizeof(char *));

            for (ch = 0; ch < CHAR_RANGE; ch++)
            {
                if (idx->legacy_case_mode & BISCUIT_MODE_LIKE)
                {
                    idx->pos_idx_legacy[ch].entries  = (PosEntry *) palloc(64 * sizeof(PosEntry));
                    idx->pos_idx_legacy[ch].count    = 0;
                    idx->pos_idx_legacy[ch].capacity = 64;
                    idx->neg_idx_legacy[ch].entries  = (PosEntry *) palloc(64 * sizeof(PosEntry));
                    idx->neg_idx_legacy[ch].count    = 0;
                    idx->neg_idx_legacy[ch].capacity = 64;
                    idx->char_cache_legacy[ch]       = NULL;
                }

                if (idx->legacy_case_mode & BISCUIT_MODE_ILIKE)
                {
                    idx->pos_idx_lower[ch].entries   = (PosEntry *) palloc(64 * sizeof(PosEntry));
                    idx->pos_idx_lower[ch].count     = 0;
                    idx->pos_idx_lower[ch].capacity  = 64;
                    idx->neg_idx_lower[ch].entries   = (PosEntry *) palloc(64 * sizeof(PosEntry));
                    idx->neg_idx_lower[ch].count     = 0;
                    idx->neg_idx_lower[ch].capacity  = 64;
                    idx->char_cache_lower[ch]        = NULL;
                }
            }

            biscuit_init_crud_structures(idx);

            slot = table_slot_create(heap, NULL);
            #if PG_VERSION_NUM >= 190000
                scan = table_beginscan(heap, SnapshotAny, 0, NULL, 0);
            #else
                scan = table_beginscan(heap, SnapshotAny, 0, NULL);
            #endif
            while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
            {
                char  *str;
                int    out_len;

                ResetExprContext(econtext);
                econtext->ecxt_scantuple = slot;
                FormIndexDatum(indexInfo, slot, estate, index_values, index_isnull);

                if (!index_isnull[0])
                {
                    str = biscuit_datum_to_text(index_values[0], coltypid,
                                                 &single_output_func, &out_len);

                    if (idx->num_records >= idx->capacity)
                    {
                        idx->capacity *= 2;
                        idx->tids = (ItemPointerData *) repalloc(
                            idx->tids, idx->capacity * sizeof(ItemPointerData));
                        idx->data_cache = (char **) repalloc(
                            idx->data_cache, idx->capacity * sizeof(char *));
                        idx->data_cache_lower = (char **) repalloc(
                            idx->data_cache_lower, idx->capacity * sizeof(char *));
                    }

                    ItemPointerCopy(&slot->tts_tid, &idx->tids[idx->num_records]);
                    idx->data_cache[idx->num_records] = str;

                    biscuit_index_single_record(NULL, idx, str, out_len, idx->num_records, 0);

                    idx->num_records++;
                }
            }

            table_endscan(scan);
            ExecDropSingleTupleTableSlot(slot);

            /* Build length bitmaps */
            idx->max_length_legacy = idx->max_len + 1;

            /*
             * idx->max_length_lower must be computed independently here,
             * via a dedicated scan over idx->data_cache_lower, rather than
             * relying on any per-record side-effect performed inside
             * biscuit_index_single_record(). That field is the live
             * allocated capacity of idx->length_bitmaps_lower /
             * idx->length_ge_bitmaps_lower, and biscuit_insert()'s growth
             * block depends on it accurately reflecting the *current*
             * array size at all times — so biscuit_index_single_record()
             * must never touch it (see the note in that function). Build
             * therefore has to derive the correct value itself, the same
             * way the multi-column build path already does.
             *
             * When ILIKE mode isn't built for this column,
             * data_cache_lower[*] is NULL for every record (see the
             * ILIKE-gated pass in biscuit_index_single_record()), so this
             * naturally computes max_lower == 0 and the arrays below stay
             * essentially empty -- but we still skip the population pass
             * explicitly below to avoid doing needless work.
             */
            if (idx->legacy_case_mode & BISCUIT_MODE_ILIKE)
            {
                int max_lower = 0;
                for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
                {
                    int lbl, lcl;
                    if (!idx->data_cache_lower[rec_idx]) continue;
                    lbl = strlen(idx->data_cache_lower[rec_idx]);
                    lcl = biscuit_utf8_char_count(idx->data_cache_lower[rec_idx], lbl);
                    if (lcl > max_lower) max_lower = lcl;
                }
                idx->max_length_lower = max_lower + 1;
            }
            else
            {
                idx->max_length_lower = 0;
            }

            if (idx->legacy_case_mode & BISCUIT_MODE_LIKE)
            {
                idx->length_bitmaps_legacy    = (RoaringBitmap **) palloc0(idx->max_length_legacy * sizeof(RoaringBitmap *));
                idx->length_ge_bitmaps_legacy = (RoaringBitmap **) palloc0(idx->max_length_legacy * sizeof(RoaringBitmap *));
                for (ch = 0; ch < idx->max_length_legacy; ch++)
                    idx->length_ge_bitmaps_legacy[ch] = biscuit_roaring_create();
            }

            if (idx->legacy_case_mode & BISCUIT_MODE_ILIKE)
            {
                idx->length_bitmaps_lower     = (RoaringBitmap **) palloc0(idx->max_length_lower   * sizeof(RoaringBitmap *));
                idx->length_ge_bitmaps_lower  = (RoaringBitmap **) palloc0(idx->max_length_lower   * sizeof(RoaringBitmap *));
                for (ch = 0; ch < idx->max_length_lower; ch++)
                    idx->length_ge_bitmaps_lower[ch] = biscuit_roaring_create();
            }

            for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
            {
                int bl;
                int cl;
                if (!idx->data_cache[rec_idx]) continue;

                if (idx->legacy_case_mode & BISCUIT_MODE_LIKE)
                {
                    bl = strlen(idx->data_cache[rec_idx]);
                    cl = biscuit_utf8_char_count(idx->data_cache[rec_idx], bl);

                    if (cl < idx->max_length_legacy)
                    {
                        if (!idx->length_bitmaps_legacy[cl])
                            idx->length_bitmaps_legacy[cl] = biscuit_roaring_create();
                        biscuit_roaring_add(idx->length_bitmaps_legacy[cl], rec_idx);
                    }
                    for (int i = 0; i <= cl && i < idx->max_length_legacy; i++)
                        biscuit_roaring_add(idx->length_ge_bitmaps_legacy[i], rec_idx);
                }

                if ((idx->legacy_case_mode & BISCUIT_MODE_ILIKE) && idx->data_cache_lower[rec_idx])
                {
                    int lbl = strlen(idx->data_cache_lower[rec_idx]);
                    int lcl = biscuit_utf8_char_count(idx->data_cache_lower[rec_idx], lbl);

                    if (lcl < idx->max_length_lower)
                    {
                        if (!idx->length_bitmaps_lower[lcl])
                            idx->length_bitmaps_lower[lcl] = biscuit_roaring_create();
                        biscuit_roaring_add(idx->length_bitmaps_lower[lcl], rec_idx);
                    }
                    for (int i = 0; i <= lcl && i < idx->max_length_lower; i++)
                        biscuit_roaring_add(idx->length_ge_bitmaps_lower[i], rec_idx);
                }
            }
        }
        else
        {
            /*
             * Multi-column: allocate a ColumnIndex per column and populate
             * each with the same character-level logic.
             * (Abbreviated here — mirrors single-column per column.)
             */
            idx->column_types            = (Oid *)        palloc(natts * sizeof(Oid));
            idx->output_funcs            = (FmgrInfo *)   palloc(natts * sizeof(FmgrInfo));
            idx->column_data_cache       = (char ***)     palloc(natts * sizeof(char **));
            idx->column_data_cache_lower = (char ***)     palloc(natts * sizeof(char **));
            idx->column_indices          = (ColumnIndex *) palloc0(natts * sizeof(ColumnIndex));
            idx->column_case_mode        = (uint8 *)      palloc(natts * sizeof(uint8));

            for (col = 0; col < natts; col++)
            {
                /*
                 * FIX #10 (multi-column case): previously looked up the type
                 * via index->rd_index->indkey.values[col] against the heap's
                 * tuple descriptor, which is InvalidAttrNumber (0) for an
                 * expression column and therefore reads garbage. Use the
                 * index's own tuple descriptor instead -- PostgreSQL always
                 * populates it with the correct result type for every key,
                 * whether that key is a plain Var or an arbitrary expression.
                 */
                Form_pg_attribute col_attr = TupleDescAttr(RelationGetDescr(index), col);
                Oid               typoutput;
                bool              typIsVarlena;
                ColumnIndex       *cidx = &idx->column_indices[col];

                idx->column_types[col] = col_attr->atttypid;
                getTypeOutputInfo(col_attr->atttypid, &typoutput, &typIsVarlena);
                fmgr_info(typoutput, &idx->output_funcs[col]);
                idx->column_data_cache[col]       = (char **) palloc0(idx->capacity * sizeof(char *));
                idx->column_data_cache_lower[col] = (char **) palloc0(idx->capacity * sizeof(char *));

                /* Per-column opclass gating -- see biscuit_get_column_case_mode(). */
                idx->column_case_mode[col] = biscuit_get_column_case_mode(index, col);

                for (ch = 0; ch < CHAR_RANGE; ch++)
                {
                    if (idx->column_case_mode[col] & BISCUIT_MODE_LIKE)
                    {
                        cidx->pos_idx[ch].entries  = (PosEntry *) palloc(64 * sizeof(PosEntry));
                        cidx->pos_idx[ch].count    = 0; cidx->pos_idx[ch].capacity = 64;
                        cidx->neg_idx[ch].entries  = (PosEntry *) palloc(64 * sizeof(PosEntry));
                        cidx->neg_idx[ch].count    = 0; cidx->neg_idx[ch].capacity = 64;
                        cidx->char_cache[ch]       = NULL;
                    }

                    if (idx->column_case_mode[col] & BISCUIT_MODE_ILIKE)
                    {
                        cidx->pos_idx_lower[ch].entries  = (PosEntry *) palloc(64 * sizeof(PosEntry));
                        cidx->pos_idx_lower[ch].count    = 0; cidx->pos_idx_lower[ch].capacity = 64;
                        cidx->neg_idx_lower[ch].entries  = (PosEntry *) palloc(64 * sizeof(PosEntry));
                        cidx->neg_idx_lower[ch].count    = 0; cidx->neg_idx_lower[ch].capacity = 64;
                        cidx->char_cache_lower[ch]       = NULL;
                    }
                }
            }

            biscuit_init_crud_structures(idx);

            slot = table_slot_create(heap, NULL);
            #if PG_VERSION_NUM >= 190000
                scan = table_beginscan(heap, SnapshotAny, 0, NULL, 0);
            #else
                scan = table_beginscan(heap, SnapshotAny, 0, NULL);
            #endif
            while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
            {
                /*
                 * FIX #10 (multi-column case): previously called
                 * slot_getattr(slot, index->rd_index->indkey.values[col], ...)
                 * to fetch each column's value, which is only valid for plain
                 * attribute references -- indkey.values[col] is 0 for an
                 * expression column, and slot_getattr() has no way to
                 * evaluate an expression in the first place. FormIndexDatum()
                 * evaluates every index key (Var or arbitrary expression)
                 * against the current heap tuple slot and fills
                 * index_values[]/index_isnull[], exactly like every built-in
                 * AM does for expression-index support.
                 */
                ResetExprContext(econtext);
                econtext->ecxt_scantuple = slot;
                FormIndexDatum(indexInfo, slot, estate, index_values, index_isnull);

                /*
                 * FIX #11: previously skipped the ENTIRE row (continue) if
                 * any single indexed column was NULL, via an all_non_null
                 * flag computed here from index_isnull[]. That dropped rows
                 * from idx->tids entirely -- including their other,
                 * non-NULL columns -- making them invisible to every
                 * subsequent query regardless of which column it filtered
                 * on. The per-column NULL handling below (storing NULL into
                 * column_data_cache[col] and skipping only that column's
                 * indexing) already exists and is sufficient; there is no
                 * need for an all-or-nothing skip. This mirrors what
                 * biscuit_insert() already does correctly per-row.
                 */

                if (idx->num_records >= idx->capacity)
                {
                    idx->capacity *= 2;
                    idx->tids = (ItemPointerData *) repalloc(idx->tids, idx->capacity * sizeof(ItemPointerData));
                    for (col = 0; col < natts; col++)
                    {
                        idx->column_data_cache[col] = (char **) repalloc(
                            idx->column_data_cache[col], idx->capacity * sizeof(char *));
                        idx->column_data_cache_lower[col] = (char **) repalloc(
                            idx->column_data_cache_lower[col], idx->capacity * sizeof(char *));
                    }
                }

                ItemPointerCopy(&slot->tts_tid, &idx->tids[idx->num_records]);

                for (col = 0; col < natts; col++)
                {
                    int        out_len;
                    char      *str;

                    if (index_isnull[col])
                    {
                        idx->column_data_cache[col][idx->num_records]       = NULL;
                        idx->column_data_cache_lower[col][idx->num_records] = NULL;
                        continue;
                    }

                    str = biscuit_datum_to_text(index_values[col], idx->column_types[col], &idx->output_funcs[col], &out_len);
                    idx->column_data_cache[col][idx->num_records] = str;

                    /*
                     * Only precompute the lowercased copy when this
                     * column's opclass actually needs ILIKE support;
                     * biscuit_like_ops columns leave this NULL.
                     */
                    idx->column_data_cache_lower[col][idx->num_records] =
                        (idx->column_case_mode[col] & BISCUIT_MODE_ILIKE)
                            ? biscuit_str_tolower(str, out_len)
                            : NULL;

                    /*
                     * Populate all character-level and case-insensitive bitmaps
                     * for this column.  Previously this was a stub comment; the
                     * missing call was the root cause of multi-column indexes
                     * returning 0 rows for every query.
                     */
                    biscuit_index_column_record(NULL, idx, col, str, out_len, idx->num_records, 0);
                }

                idx->num_records++;
            }

            table_endscan(scan);
            ExecDropSingleTupleTableSlot(slot);

            /* Build per-column length bitmaps */
            for (col = 0; col < natts; col++)
            {
                ColumnIndex *cidx = &idx->column_indices[col];
                uint8        col_mode = idx->column_case_mode[col];
                int max_cl = 0;

                /* ---- Case-sensitive length bitmaps (LIKE-gated) ---- */
                if (col_mode & BISCUIT_MODE_LIKE)
                {
                    for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
                    {
                        int bl;
                        int cl;
                        if (!idx->column_data_cache[col][rec_idx]) continue;
                        bl = strlen(idx->column_data_cache[col][rec_idx]);
                        cl = biscuit_utf8_char_count(idx->column_data_cache[col][rec_idx], bl);
                        if (cl > max_cl) max_cl = cl;
                    }

                    cidx->max_length = max_cl + 1;
                    cidx->length_bitmaps    = (RoaringBitmap **) palloc0(cidx->max_length * sizeof(RoaringBitmap *));
                    cidx->length_ge_bitmaps = (RoaringBitmap **) palloc0(cidx->max_length * sizeof(RoaringBitmap *));
                    for (int i = 0; i < cidx->max_length; i++)
                        cidx->length_ge_bitmaps[i] = biscuit_roaring_create();

                    for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
                    {
                        int bl;
                        int cl;
                        if (!idx->column_data_cache[col][rec_idx]) continue;
                        bl = strlen(idx->column_data_cache[col][rec_idx]);
                        cl = biscuit_utf8_char_count(idx->column_data_cache[col][rec_idx], bl);
                        if (cl < cidx->max_length)
                        {
                            if (!cidx->length_bitmaps[cl]) cidx->length_bitmaps[cl] = biscuit_roaring_create();
                            biscuit_roaring_add(cidx->length_bitmaps[cl], rec_idx);
                        }
                        for (int i = 0; i <= cl && i < cidx->max_length; i++)
                            biscuit_roaring_add(cidx->length_ge_bitmaps[i], rec_idx);
                    }
                }

                /* Case-insensitive length bitmaps (ILIKE-gated) — compute
                 * max_length_lower independently. Cannot simply copy
                 * max_length: lowercasing can change character count
                 * (e.g. German ß → ss doubles that character). */
                if (col_mode & BISCUIT_MODE_ILIKE)
                {
                    int max_cl_lower = 0;

                    for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
                    {
                        int bl, cl;
                        const char *s = idx->column_data_cache[col][rec_idx];
                        char       *sl;
                        if (!s) continue;
                        bl = strlen(s);
                        sl = biscuit_str_tolower(s, bl);
                        cl = biscuit_utf8_char_count(sl, strlen(sl));
                        pfree(sl);
                        if (cl > max_cl_lower) max_cl_lower = cl;
                    }

                    cidx->max_length_lower = max_cl_lower + 1;
                    cidx->length_bitmaps_lower    = (RoaringBitmap **) palloc0(cidx->max_length_lower * sizeof(RoaringBitmap *));
                    cidx->length_ge_bitmaps_lower = (RoaringBitmap **) palloc0(cidx->max_length_lower * sizeof(RoaringBitmap *));
                    for (int i = 0; i < cidx->max_length_lower; i++)
                        cidx->length_ge_bitmaps_lower[i] = biscuit_roaring_create();

                    for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
                    {
                        int         bl, cl;
                        const char *s = idx->column_data_cache[col][rec_idx];
                        char       *sl;
                        if (!s) continue;
                        bl = strlen(s);
                        sl = biscuit_str_tolower(s, bl);
                        cl = biscuit_utf8_char_count(sl, strlen(sl));
                        pfree(sl);
                        if (cl < cidx->max_length_lower)
                        {
                            if (!cidx->length_bitmaps_lower[cl])
                                cidx->length_bitmaps_lower[cl] = biscuit_roaring_create();
                            biscuit_roaring_add(cidx->length_bitmaps_lower[cl], rec_idx);
                        }
                        for (int i = 0; i <= cl && i < cidx->max_length_lower; i++)
                            biscuit_roaring_add(cidx->length_ge_bitmaps_lower[i], rec_idx);
                    }
                }
            }
        }

        biscuit_write_metadata_to_disk(index, idx);

        /*
         * Bulk-load persistence path (Phase 1 Contract: "biscuit_build()
         * rewritten as a bulk loader"). Every biscuit_index_single_record()/
         * biscuit_index_column_record() call above was invoked with
         * index == NULL, so the full in-memory structure set built above
         * never touched the shared log at all -- only biscuit_persist_save()
         * below writes anything durable, and it writes straight to each
         * structure's compacted-blob chain (biscuit_persist_write_raw() ->
         * biscuit_page_write_blob(), see biscuit_persist.c) rather than
         * going through the shared log and its drain.
         * This is deliberate and safe specifically because build runs with
         * no concurrent readers of this not-yet-visible index to reconcile
         * against -- the read-time merge machinery in biscuit_pattern.c
         * (Phase 1 Contract §3) exists to let a *different* backend see a
         * structure's undrained log records; a brand-new index has no
         * "different backend" that could have observed a half-built state,
         * so there is nothing to reconcile and the log would be
         * pure overhead here. Separate from the num_records-only metapage
         * write above. This write is no longer best-effort: biscuit_load_index()
         * has no from-heap rebuild fallback, so a failed/skipped snapshot
         * here would leave every future cold load of this index with
         * nothing to read. Any failure propagates and fails the build.
         */
        biscuit_persist_save(RelationGetRelid(index), idx);
        idx->gen_at_last_snapshot = idx->gen;

        biscuit_register_callback();
        /*
         * NOTE: idx lives permanently in CacheMemoryContext and is owned
         * exclusively by biscuit_cache (keyed by relid).  Do NOT also
         * assign it to index->rd_amcache: PostgreSQL pfree()s rd_amcache
         * on relcache invalidation, which under load (VACUUM/ANALYZE/many
         * transactions) happens far more often than our own cache gets
         * evicted, and pfree()ing this shared object out from under the
         * global cache produces a dangling pointer / use-after-free the
         * next time biscuit_cache_lookup() hands it back out.
         */
        biscuit_cache_insert(RelationGetRelid(index), idx);

        MemoryContextSwitchTo(oldcontext);
        FreeExecutorState(estate);

        result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
        result->heap_tuples  = idx->num_records;
        result->index_tuples = idx->num_records;

        return result;
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcontext);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void
biscuit_buildempty(Relation index)
{
    /* Nothing to write for an empty index */
    (void) index;
}

/*
 * Load the index from its on-disk page directory (compacted blobs) on a
 * cache miss. biscuit_build() always persists a complete snapshot via
 * biscuit_persist_save() before returning, so any existing biscuit index
 * relation has a saved snapshot by construction -- there is no from-heap
 * rebuild path here, and no external-file fallback of any kind. This is
 * the only way a BiscuitIndex is ever (re)materialized outside of build.
 */
BiscuitIndex *
biscuit_load_index(Relation index)
{
    BiscuitIndex *idx = biscuit_persist_load(index);

    if (!idx)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("biscuit: no on-disk snapshot found for index \"%s\"",
                        RelationGetRelationName(index)),
                 errhint("The index may be corrupt; consider running REINDEX.")));

    /*
     * biscuit_persist_load() never touches idx->gen /
     * idx->gen_at_last_snapshot (the snapshot format deliberately doesn't
     * carry them -- see the field comments in BiscuitIndex). The
     * authoritative generation counter lives in the metapage, so pull it
     * from there now; a missing/unreadable metapage just leaves us at
     * generation 0, which is safe (it only means the next mutation's bump
     * is the first one this process observes).
     */
    {
        int    disk_records = 0;
        int    unused_columns, unused_max_len;
        uint64 disk_gen = 0;

        biscuit_read_metadata_from_disk(index, &disk_records,
                                        &unused_columns, &unused_max_len,
                                        &disk_gen);
        idx->gen                  = disk_gen;
        idx->gen_at_last_snapshot = disk_gen;

        /*
         * The record count used to be discarded here (into a variable
         * literally named unused_records) even though the metapage is the
         * authoritative high-water mark for slot allocation and the HEADER
         * blob it was silently overriding is not. That was one half of the
         * lost-post-build-rows defect; the reconciliation itself now lives
         * in biscuit_persist_load(), which is the only place that can widen
         * idx->capacity before the per-slot arrays are allocated.
         *
         * This is left as a belt-and-braces check rather than a second fix:
         * if it ever fires, biscuit_persist_load() failed to reconcile and
         * every slot in [idx->num_records, disk_records) is unreachable to
         * this backend. Warn rather than error -- the index is still usable
         * for the rows it does know about, and erroring here would take out
         * every read of a merely-truncated index.
         */
        if (disk_records > idx->num_records)
            elog(WARNING,
                 "biscuit: index \"%s\" loaded %d records but metapage reports %d; "
                 "%d slot(s) will not be visible to this backend",
                 RelationGetRelationName(index), idx->num_records, disk_records,
                 disk_records - idx->num_records);
    }

    biscuit_register_callback();
    biscuit_cache_insert(RelationGetRelid(index), idx);
    return idx;
}

/*
 * biscuit_get_current_index
 *
 * BLOCKER-1 FIX -- cross-backend visibility of committed inserts.
 *
 * biscuit_cache_lookup() returns this backend's own process-local copy of
 * the index. Despite comments elsewhere calling biscuit_cache a "global"
 * cache, biscuit_cache_head (biscuit_cache.c) is a per-backend static, not
 * shared memory. That copy's idx->num_records, idx->tids[], data caches,
 * and every bitmap are a snapshot taken when it was built
 * (biscuit_load_index()) or last mutated *by this backend*
 * (biscuit_insert()/biscuit_bulkdelete()). It is never touched by another
 * backend's commit: ordinary DML does not send a relcache invalidation for
 * the target relation (that machinery fires for DDL-shaped catalog
 * changes, not row inserts), so biscuit_relcache_callback() -- the index's
 * only cache-eviction path -- never runs because of a concurrent INSERT. A
 * backend that primed this cache before another backend committed
 * therefore keeps serving the pre-commit snapshot indefinitely, across
 * any number of later transactions in that backend.
 *
 * This is not just a "missing row" problem. Every read path (the
 * candidate-set construction in biscuit_scan.c, the negation base set in
 * biscuit_pattern.c) builds its universe as the range [0, idx->num_records),
 * so slots another backend has since claimed sit entirely outside the
 * candidate range and are structurally unreachable no matter what any
 * individual bitmap contains. The shared pending log (biscuit_pendlog.c)
 * reconciles bitmap *membership* for slots this backend already knows
 * about; it cannot manufacture idx->tids[]/idx->num_records entries for
 * slots it has never loaded, so it cannot close this gap by itself.
 *
 * Fix: before trusting a cached BiscuitIndex for a read, compare it
 * against the metapage's authoritative, monotonically non-decreasing
 * generation counter (meta->gen -- bumped and durably persisted at the end
 * of every biscuit_insert()/biscuit_bulkdelete(), by whichever backend
 * performed it). A single share-locked metapage read is cheap enough to
 * do on every beginscan/rescan. On a mismatch the cached copy is stale by
 * construction: evict it and reload the complete index synchronously from
 * disk via biscuit_load_index() -- the same path an ordinary cold cache
 * miss already takes, which is correct because it consults the real
 * on-disk state directly rather than trying to patch a partial in-memory
 * object.
 */
BiscuitIndex *
biscuit_get_current_index(Relation index)
{
    Oid           indexoid = RelationGetRelid(index);
    BiscuitIndex *idx      = biscuit_cache_lookup(indexoid);
    int           disk_num_records, disk_num_columns, disk_max_len;
    uint64        disk_gen = 0;

    if (!idx)
    {
        (void) biscuit_consume_self_drain_reload(indexoid);
        return biscuit_load_index(index);
    }

    /*
     * A drain THIS backend performed invalidates the cached copy without
     * ever tripping the gen comparison below, because
     * biscuit_resync_gen_after_self_drain() deliberately advanced idx->gen
     * past the drain's own bump. Check that first: the gen test cannot see
     * this case by construction, so ordering it after would be checking a
     * condition that is guaranteed false.
     *
     * See the long comment on biscuit_resync_gen_after_self_drain() for why
     * the drain leaves the in-memory bitmaps stale and why deferring the
     * reload to here -- past the end of the mutating statement -- is both
     * necessary and now safe.
     */
    if (biscuit_consume_self_drain_reload(indexoid))
    {
        elog(DEBUG1,
             "biscuit: reloading index %u after this backend's own drain",
             indexoid);
        biscuit_cache_remove(indexoid);
        return biscuit_load_index(index);
    }

    if (!biscuit_read_metadata_from_disk(index, &disk_num_records,
                                          &disk_num_columns, &disk_max_len,
                                          &disk_gen))
        return idx;   /* no readable on-disk metapage; nothing to compare
                        * against, so trust the cache rather than error */

    if (disk_gen <= idx->gen)
        return idx;   /* nothing has mutated this index since we cached it */

    /*
     * Stale: another backend committed a mutation (insert, update, or
     * delete) since this backend last built or refreshed its copy. Discard
     * it and reload synchronously -- biscuit_load_index() reads the
     * complete index (TIDs, data caches, every bitmap) from the on-disk
     * directory and re-installs the fresh copy in the cache itself.
     */
    biscuit_cache_remove(indexoid);
    return biscuit_load_index(index);
}

bool
biscuit_insert(Relation index,
               Datum *values,
               bool *isnull,
               ItemPointer ht_ctid,
               Relation heapRelation,
               IndexUniqueCheck checkUnique,
               bool indexUnchanged,
               IndexInfo *indexInfo)
{
    BiscuitIndex  *idx;
    MemoryContext  oldcontext;
    uint32_t       slot;
    bool           found_existing  = false;
    int            col;
    uint32         pending_list_limit;

    (void) heapRelation;
    (void) checkUnique;
    (void) indexUnchanged;
    (void) indexInfo;

    /*
     * Read once per statement (per Phase 1 Contract §2a) rather than once
     * per structure mutated below -- the GUC/metapage value can't change
     * mid-statement, so there's no correctness reason to re-fetch it on
     * every one of the many structures a single row touches.
     */
    pending_list_limit = biscuit_read_pending_list_limit(index);

    /*
     * Always resolve through the global, relid-keyed biscuit_cache rather
     * than index->rd_amcache.  rd_amcache is pfree()d by PostgreSQL on
     * relcache invalidation (e.g. the catalog access triggered by INSERT's
     * own executor setup) — if we shared the same pointer with rd_amcache,
     * that pfree would free the object the global cache still references,
     * leading to a use-after-free on a later lookup.
     */
    idx = biscuit_get_current_index(index);

    /*
     * biscuit_load_index() (called by biscuit_get_current_index() above,
     * on a cold miss or a stale-cache reload) always builds a
     * complete BiscuitIndex — heap scan, data caches, and every bitmap —
     * before returning it, so the length-bitmap arrays below are always
     * allocated and non-NULL by the time we get here.
     */
    oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

    /*
     * BATCH THE UPDATE-IN-PLACE RECORD PAIR.
     *
     * "A row is now one record" (see the comment below on the removed row
     * batch) is true for a plain INSERT or a bulkdelete-driven REMOVE, but
     * it is NOT true here: the found-existing branch below emits a REMOVE
     * (from biscuit_remove_from_all_indices(), appended before the new
     * text overwrites the old) and, much later in this function, an ADD
     * for the row's new content. That is two pending-log records for one
     * logical write, and nothing was protecting the window between them --
     * biscuit_pending_mutate_row()'s opportunistic-compaction check
     * (biscuit_pendlog_drain_trigger_bytes()/biscuit_pendlog_compact()) is
     * only suppressed while a batch is open. Without the batch, a
     * compaction landing on the REMOVE mid-row runs while idx->data_cache
     * for this slot still holds the old text and STRCACHE hasn't been
     * overwritten yet -- exactly the kind of straddled-mutation window
     * biscuit_pendlog_batch_begin()/_end() exists to close (see
     * biscuit_pendlog_append()'s deferred-drain comment). Opening the
     * batch here, before the duplicate-TID scan, covers both the REMOVE
     * (if found_existing) and the ADD (always) under one drain-suppression
     * window; biscuit_pendlog_batch_end() runs any compaction that got
     * deferred only after the row is fully durable.
     */
    biscuit_pendlog_batch_begin(index);

    /* Check for duplicate TID (UPDATE path) */
    for (int i = 0; i < idx->num_records; i++)
    {
        /*
         * Skip slots this backend has no TID for.
         *
         * [0, num_records) is not densely populated in this backend's copy:
         * cross-backend slot claiming leaves holes (documented on
         * biscuit_claim_new_slot()), aborted inserts leak slot numbers, and
         * a slot deleted by biscuit_bulkdelete() is explicitly reset with
         * ItemPointerSetInvalid(). Two distinct "no TID here" encodings
         * reach this loop:
         *
         *   - ItemPointerSetInvalid(), i.e. block 0xFFFFFFFF, from
         *     biscuit_ensure_slot_capacity()'s tail fill and from
         *     biscuit_bulkdelete().
         *   - all-zero (block 0, offset 0), from the palloc0'd tail in
         *     biscuit_persist_load().
         *
         * ItemPointerIsValid() rejects both, because a valid heap TID has a
         * non-zero offset number: offset 0 is never a real line pointer.
         * Testing it matters -- an incoming ctid could otherwise compare
         * equal to a zeroed slot and send a fresh INSERT down the UPDATE
         * path, overwriting an unrelated (or not-yet-written) slot. That is
         * the same hazard the tail-invalidation comment in
         * biscuit_ensure_slot_capacity() describes, arriving here from the
         * load path instead of from repalloc.
         */
        if (!ItemPointerIsValid(&idx->tids[i]))
            continue;

        if (ItemPointerEquals(&idx->tids[i], ht_ctid))
        {
            found_existing = true;
            slot           = i;

            /* Remove old data from all bitmaps -- durably, via pending-list
             * appends (mutation contract §1), not a direct blob rewrite. */
            biscuit_remove_from_all_indices(index, idx, slot, pending_list_limit);

            if (idx->num_columns == 1)
            {
                if (idx->data_cache[slot])       { pfree(idx->data_cache[slot]);       idx->data_cache[slot]       = NULL; }
                if (idx->data_cache_lower[slot])  { pfree(idx->data_cache_lower[slot]); idx->data_cache_lower[slot] = NULL; }
            }
            else
            {
                for (col = 0; col < idx->num_columns; col++)
                {
                    if (idx->column_data_cache[col][slot])
                    {
                        pfree(idx->column_data_cache[col][slot]);
                        idx->column_data_cache[col][slot] = NULL;
                    }
                    if (idx->column_data_cache_lower &&
                        idx->column_data_cache_lower[col][slot])
                    {
                        pfree(idx->column_data_cache_lower[col][slot]);
                        idx->column_data_cache_lower[col][slot] = NULL;
                    }
                }
            }

            /* Un-tombstone if needed */
            biscuit_roaring_remove(idx->tombstones, slot);
            idx->update_count++;
            break;
        }
    }

    /*
     * CONCURRENCY FIX (slot-allocation race) -- freelist reuse.
     *
     * biscuit_pop_free_slot() is deliberately NOT called here any more.
     *
     * idx->free_list is process-local, exactly like idx->num_records was:
     * it is rebuilt from the durable HEADER blob on load and then mutated
     * only in this backend's own copy. Two backends that both have slot 42
     * on their free list will both pop it, for two different rows, and the
     * later writer silently clobbers the earlier one -- the same collision
     * as the num_records race, just sourced from the other allocator.
     * Pushing the pop under the metapage lock would not help on its own,
     * because the list being popped from is still private; a correct fix
     * needs the freelist itself to become shared durable state (a
     * metapage-anchored chain, claimed under the same lock as
     * biscuit_claim_new_slot() uses), which is an on-disk format change.
     *
     * Until that exists, this path takes the conservative option: always
     * claim a fresh slot. The cost is slot-space density -- deleted slots
     * are no longer recycled by INSERT, so the slot array grows with total
     * inserts rather than live rows, and reclamation waits for VACUUM's
     * compaction / REINDEX. That is a bounded, self-healing space cost.
     * The behaviour it replaces was unbounded, silent row loss.
     *
     * biscuit_push_free_slot() is left in place (biscuit_bulkdelete still
     * calls it) so the durable free list keeps accumulating and a future
     * shared-freelist implementation has the data it needs.
     */
    if (!found_existing)
    {
        /*
         * Claim the slot number atomically against every other backend,
         * from the metapage, before touching anything else. See
         * biscuit_claim_new_slot() for why this cannot read
         * idx->num_records and why the lock must not be held across the
         * row write below.
         */
        slot = biscuit_claim_new_slot(index, idx);
        biscuit_ensure_slot_capacity(idx, slot);
    }

    ItemPointerCopy(ht_ctid, &idx->tids[slot]);

    /*
     * NO ROW BATCH HERE ANY MORE.
     *
     * biscuit_pendlog_batch_begin()/_end() existed because indexing one
     * string emitted POS/NEG/CACHE per character per case mode plus LEN
     * and the LEN_GE ladder -- on the order of 8N+4 appends for an
     * N-character string, each originally its own GenericXLog transaction
     * paying a full record header and block reference for a ~24-byte
     * payload. Holding one transaction open across the row collapsed them
     * into a single WAL record carrying the accumulated page delta, and it
     * had to defer the drain trigger to batch_end(), because draining
     * under the tail page's content lock deadlocks.
     *
     * A row is now ONE record, so there is nothing left to batch: the
     * mechanism would open a GenericXLog transaction, write eight bytes,
     * and close it again. The append moved to the end of this function,
     * after the row's text is durable (see there for why the ordering
     * matters), and the compaction trigger fires from
     * biscuit_pending_mutate_row() with none of our locks held -- which is
     * precisely the condition batch_end() was arranging by hand.
     *
     * The machinery stays in biscuit_pendlog.c rather than being deleted:
     * it is correct, it costs nothing unused, and a change that
     * reintroduces multiple appends per row would want it back.
     */

    /* Insert record data */
    if (idx->num_columns == 1)
    {
        if (!isnull[0])
        {
            text *txt      = DatumGetTextPP(values[0]);
            char *str      = VARDATA_ANY(txt);
            int   byte_len = VARSIZE_ANY_EXHDR(txt);

            idx->data_cache[slot] = pnstrdup(str, byte_len);

            /*
             * biscuit_index_single_record writes idx->data_cache_lower[slot]
             * as a side-effect.  It must run BEFORE the length-bitmap block
             * below reads data_cache_lower[slot].
             */
            biscuit_index_single_record(index, idx, str, byte_len, slot, pending_list_limit);

            /*
             * The LEN / LEN_GE arrays used to be grown and populated here,
             * in four near-identical open-coded blocks (case-sensitive and
             * lowercase, times legacy and multi-column). They are now
             * maintained by inmem_fanout_emit(), driven from
             * biscuit_index_single_record() above via the same
             * biscuit_fanout_string() the delta builder uses.
             *
             * That is the point of the extraction: the length ladder is
             * part of what a string implies, so it has to come out of the
             * same function as the character structures, or the delta and
             * the base can disagree about a row's LEN_GE membership --
             * which is exactly the membership NOT LIKE inverts against.
             */
        }
        else
        {
            idx->data_cache[slot]       = NULL;
            idx->data_cache_lower[slot] = NULL;
        }
    }
    else
    {
        for (col = 0; col < idx->num_columns; col++)
        {
            if (!isnull[col])
            {
                int   out_len;
                char *str = biscuit_datum_to_text(values[col], idx->column_types[col],
                                                  &idx->output_funcs[col], &out_len);
                idx->column_data_cache[col][slot] = str;

                /*
                 * Pre-compute the lowercased copy so ILIKE queries can use
                 * column_data_cache_lower directly, matching the invariant
                 * established at build/load time.  Mirror NULL to NULL for
                 * the null-column case handled below, and also leave it
                 * NULL when this column's opclass doesn't need ILIKE
                 * support (biscuit_like_ops) -- matching the gating
                 * biscuit_index_column_record() already applies to the
                 * bitmap structures themselves.
                 */
                if (idx->column_data_cache_lower)
                    idx->column_data_cache_lower[col][slot] =
                        (idx->column_case_mode && (idx->column_case_mode[col] & BISCUIT_MODE_ILIKE))
                            ? biscuit_str_tolower(str, out_len)
                            : NULL;

                biscuit_index_column_record(index, idx, col, str, out_len, slot, pending_list_limit);

                /*
                 * FIX 5 — multi-column length bitmaps never updated on insert.
                 *
                 * biscuit_index_column_record() only maintains the per-character
                 * position/negative-position bitmaps and char_cache for this
                 * column's ColumnIndex.  It does NOT touch cidx->length_bitmaps /
                 * cidx->length_ge_bitmaps (or the _lower variants), which were
                 * only ever populated in the bulk-build pass (biscuit_build).
                 *
                 * biscuit_rescan() always uses the bitmap fast path
                 * (biscuit_rescan_multicolumn), which consults these length
                 * bitmaps for length-based predicates. Newly inserted rows
                 * were invisible there even though column_data_cache,
                 * char_cache, and the position bitmaps were all correctly
                 * updated above — hence "insert is on disk and in
                 * data_cache, but queries don't see it".
                 *
                 * Mirror the legacy single-column growth/insert logic here,
                 * operating on this column's ColumnIndex (cidx) instead of
                 * the top-level idx fields.
                 */
                /*
                 * Multi-column LEN / LEN_GE: likewise now maintained by
                 * inmem_fanout_emit() from biscuit_index_column_record(),
                 * not open-coded here.
                 *
                 * Worth noting what this block originally fixed, since the
                 * replacement must keep fixing it:
                 * biscuit_index_column_record() used to maintain only the
                 * per-character structures, so newly inserted rows were
                 * invisible to any length-based predicate on the
                 * multi-column scan path -- "insert is on disk and in
                 * data_cache, but queries don't see it". Routing lengths
                 * through the shared fan-out means that class of omission
                 * cannot recur: a caller either gets the whole fan-out or
                 * none of it.
                 */
            }
            else
            {
                idx->column_data_cache[col][slot] = NULL;
                if (idx->column_data_cache_lower)
                    idx->column_data_cache_lower[col][slot] = NULL;
            }
        }
    }

    if (!found_existing)
        idx->insert_count++;

    /*
     * Bump the generation counter now that the in-memory bitmap mutation
     * above has completed successfully, and persist it immediately.
     *
     * INTENTIONALLY NON-TRANSACTIONAL: this happens unconditionally, with
     * no regard for whether the surrounding transaction commits or rolls
     * back. If the transaction later aborts, idx->gen (and the on-disk
     * copy) stays bumped anyway -- that's over-invalidation, which is
     * harmless (worst case: an unnecessary future re-snapshot). The
     * alternative -- deferring the bump until commit -- would risk
     * under-invalidation (a durable mutation that isn't reflected in
     * gen), which is the actual correctness bug this counter exists to
     * prevent. Do not "fix" this by hooking commit/abort.
     *
     * This runs unconditionally for both the legacy single-column and
     * multi-column paths above -- it is not gated on
     * idx->num_columns == 1.
     *
     * The increment itself now happens inside biscuit_write_metadata_to_
     * disk(), under the metapage's exclusive lock, and idx->gen is pulled
     * forward to match there -- see the GEN FIX comment in that function
     * for why a local idx->gen++ before this call is no longer correct.
     */
    biscuit_write_metadata_to_disk(index, idx);

    /*
     * Durability for every *bitmap* mutation performed above already
     * landed on disk the moment each biscuit_pending_mutate_structure()
     * call returned -- each is its own GenericXLog-logged, WAL-replayed
     * pending-list append (plus, for any structure whose pending chain
     * crossed pending_list_limit mid-row, an already-completed
     * opportunistic drain per §2a).
     *
     * The *row-identity* structures split two ways:
     *
     *   - TIDS and STRCACHE are made durable right here, for this one row,
     *     in place: biscuit_persist_row_identity_write_record() writes
     *     tids[slot] and this row's string-cache entries and nothing else.
     *     This is O(1) per row (a handful of single-page writes), and it
     *     replaces the old "defer a full rewrite of both whole arrays to
     *     pre-commit" scheme, which was O(num_records) per *commit* --
     *     i.e. 70x the WAL per row for single-row transactions, the root
     *     cause this work exists to fix. Note this covers all three
     *     mutation shapes uniformly, since `slot` was resolved above
     *     identically for each: a fresh append (slot == num_records++), a
     *     reused freelist slot, and the sub-case where either lands on a
     *     logical page that doesn't exist on disk yet (biscuit_rowstore.c
     *     allocates it -- see biscuit_rowstore_tid_write()).
     *
     *   - The HEADER (num_records and the counters), tombstones and free
     *     list are still deferred to a pre-commit xact callback via
     *     biscuit_mark_row_identity_dirty(), because unlike TIDS/STRCACHE
     *     they are not per-row data at all: they'd be written with the
     *     same content by every row in the statement. Deferring is now a
     *     plain redundancy-avoidance measure rather than a workaround for
     *     an O(n^2) blowup (biscuit_persist_save_row_identity() is itself
     *     O(1) in num_records now). It also stays semantically correct for
     *     the same reason it always was: an uncommitted insert must not be
     *     visible to a cold reader in another backend anyway (MVCC), and a
     *     warm reader in this backend uses the in-memory idx directly.
     */
    biscuit_persist_row_identity_write_record(index, idx, (uint32) slot,
                                               found_existing
                                                   ? BISCUIT_SLOT_WRITE_INPLACE
                                                   : BISCUIT_SLOT_WRITE_FRESH);

    /*
     * NOW record the fact of the row in the pending log: eight bytes,
     * once, however many columns and characters it has.
     *
     * ORDER MATTERS, and it is why this sits here rather than up beside
     * the bitmap mutations where its per-structure predecessor lived. The
     * log record is a pointer to text -- whoever reads it goes to STRCACHE
     * for the bytes and fans them out. Writing the pointer before the text
     * opens a window in which the log names a slot that has no text yet,
     * and a crash inside that window leaves a durable record whose
     * expansion produces nothing.
     *
     * That window happens to be survivable: an expansion that finds no
     * text emits no identities, and a crash before this statement commits
     * means the heap tuple is not visible either, so an unindexed
     * invisible row is not a wrong answer. But it is survivable by
     * accident rather than by construction, and the other ordering costs
     * nothing. Text first, then the record that refers to it.
     *
     * On the UPDATE path this is the ADD half. The REMOVE half was already
     * appended by biscuit_remove_from_all_indices() above -- crucially,
     * before the new text overwrote the old in STRCACHE, though the
     * reconciler does not depend on that, since it withdraws the slot from
     * base wholesale rather than by re-deriving what it used to match.
     */
    biscuit_pending_mutate_row(index, idx, (uint32) slot,
                                BISCUIT_PENDING_OP_ADD, pending_list_limit);

    /*
     * Close the batch opened above. This is where any compaction that got
     * deferred because the batch was open (a REMOVE or this ADD crossing
     * biscuit.delta_compaction_slots) actually runs -- with the row fully
     * durable and none of biscuit_persist_row_identity_write_record()'s
     * locks held, matching biscuit_pendlog_batch_end()'s own contract.
     */
    biscuit_pendlog_batch_end();

    biscuit_mark_row_identity_dirty(RelationGetRelid(index));

    /*
     * FIX 2 — INSERT → SELECT returns 0.
     *
     * Write the updated index back into the global cache so the next
     * beginscan — in this session or any other — picks up the newly
     * inserted record via the bitmap path instead of a stale cached copy
     * that predates the insert.
     *
     * Without this, a SELECT immediately after a committed INSERT finds the
     * pre-insert cache entry and returns 0 rows.
     */
    biscuit_cache_insert(RelationGetRelid(index), idx);

    MemoryContextSwitchTo(oldcontext);

    return true;
}

/* ================================================================
 * SECTION 5 – BULKDELETE
 * ================================================================ */

IndexBulkDeleteResult *
biscuit_bulkdelete(IndexVacuumInfo *info,
                   IndexBulkDeleteResult *stats,
                   IndexBulkDeleteCallback callback,
                   void *callback_state)
{
    Relation       index = info->index;
    BiscuitIndex  *idx;
    int            i, j, col;
    MemoryContext  oldcontext;
    RoaringBitmap *records_to_delete;
    uint64_t       delete_count;
    uint32_t      *delete_indices;
    uint32         pending_list_limit;

    idx = biscuit_get_current_index(index);

    if (!stats)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

    pending_list_limit = biscuit_read_pending_list_limit(index);

    oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

    records_to_delete = biscuit_roaring_create();

    for (i = 0; i < idx->num_records; i++)
    {
        bool has_data;
        bool already_tombstoned;

        if (idx->num_columns == 1)
            has_data = (idx->data_cache[i] != NULL);
        else
            has_data = (idx->column_data_cache && idx->column_data_cache[0] && idx->column_data_cache[0][i] != NULL);

        if (!has_data) continue;

        /*
         * Defensive: never hand an unset TID to the vacuum callback. The
         * has_data test above already excludes almost every such slot (a
         * slot with no loaded text is not a row this backend knows about),
         * but [0, num_records) can contain holes -- other backends' claims,
         * leaked slots from aborted inserts, and the palloc0 tail of a
         * partially-covered load -- and a (0,0) or (0xFFFFFFFF,0) item
         * pointer reaching the heap-side callback is not something to leave
         * to a coincidence of two independent arrays agreeing.
         */
        if (!ItemPointerIsValid(&idx->tids[i])) continue;

#ifdef HAVE_ROARING
        already_tombstoned = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
#else
        { uint32_t bl = i >> 6, bt = i & 63; already_tombstoned = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
#endif
        if (already_tombstoned) continue;

        if (callback(&idx->tids[i], callback_state))
        {
            biscuit_roaring_add(idx->tombstones, (uint32_t) i);
            biscuit_roaring_add(records_to_delete, (uint32_t) i);
            idx->tombstone_count++;
            biscuit_push_free_slot(idx, (uint32_t) i);
            stats->tuples_removed++;
            idx->delete_count++;
        }
    }

    delete_count = biscuit_roaring_count(records_to_delete);

    if (delete_count > 0)
    {
        delete_indices = biscuit_roaring_to_array(records_to_delete, &delete_count);

        if (delete_indices)
        {
            /*
             * Per Phase 1 Contract §1/§4c: replace the old bulk
             * andnot_inplace(bitmap, records_to_delete) pass -- which
             * mutated every structure's in-memory bitmap in one shot,
             * relying on the (now-removed) eager whole-index resave to
             * eventually make that durable -- with one
             * biscuit_remove_from_all_indices() call per deleted slot.
             * This reuses the already-converted per-slot removal path
             * (in-memory biscuit_roaring_remove() plus a durable
             * pending-list append per touched structure per slot), at
             * the cost of walking every touched structure once per
             * deleted slot rather than once per bulkdelete call. A
             * batched multi-value pending append is a reasonable
             * follow-up optimization but out of scope for this
             * conversion -- per-value appends are still far cheaper
             * than the blob decode/re-encode they replace (design doc
             * §2's fan-out note).
             */
            for (j = 0; j < (int) delete_count; j++)
                biscuit_remove_from_all_indices(index, idx, delete_indices[j], pending_list_limit);

            if (idx->num_columns == 1)
            {
                for (j = 0; j < (int) delete_count; j++)
                {
                    if (idx->data_cache[delete_indices[j]])
                        { pfree(idx->data_cache[delete_indices[j]]); idx->data_cache[delete_indices[j]] = NULL; }
                    if (idx->data_cache_lower && idx->data_cache_lower[delete_indices[j]])
                        { pfree(idx->data_cache_lower[delete_indices[j]]); idx->data_cache_lower[delete_indices[j]] = NULL; }
                }
            }
            else
            {
                for (j = 0; j < (int) delete_count; j++)
                    for (col = 0; col < idx->num_columns; col++)
                    {
                        if (idx->column_data_cache[col][delete_indices[j]])
                        {
                            pfree(idx->column_data_cache[col][delete_indices[j]]);
                            idx->column_data_cache[col][delete_indices[j]] = NULL;
                        }
                        if (idx->column_data_cache_lower &&
                            idx->column_data_cache_lower[col][delete_indices[j]])
                        {
                            pfree(idx->column_data_cache_lower[col][delete_indices[j]]);
                            idx->column_data_cache_lower[col][delete_indices[j]] = NULL;
                        }
                    }
            }

            /*
             * Durably clear each deleted slot's TID and string-cache
             * entries, now that the in-memory arrays above have been
             * NULLed. This is a new, required step: before the in-place
             * rewrite, the pre-commit flush re-serialized both whole
             * arrays from memory, so a slot cleared in memory became
             * cleared on disk for free. Now that nothing rewrites them
             * wholesale, a delete that only clears memory would leave the
             * dead row's TID and string bytes on disk, and the next cold
             * load in another backend would read them straight back into
             * data_cache[slot] -- resurrecting a deleted row's payload for
             * any scan path that consults the string cache. The slot stays
             * within [0, num_records) after a delete (it goes on the free
             * list rather than shrinking the array), so the stale entry
             * really would be read.
             *
             * biscuit_persist_row_identity_write_record() reads the
             * (now-NULL) in-memory state, so this writes a NULL STRCACHE
             * pointer and the zeroed TID exactly as intended -- no
             * separate "clear" entry point is needed.
             */
            for (j = 0; j < (int) delete_count; j++)
            {
                ItemPointerSetInvalid(&idx->tids[delete_indices[j]]);
                biscuit_persist_row_identity_write_record(index, idx,
                                                           (uint32) delete_indices[j],
                                                           BISCUIT_SLOT_WRITE_INPLACE);
            }

            pfree(delete_indices);
        }
    }

    biscuit_roaring_free(records_to_delete);

    /* BEFORE clearing tombstones, purge stale bitmap data for
    * tombstoned slots that were never reused (still on free_list). */
    if (idx->tombstone_count >= TOMBSTONE_CLEANUP_THRESHOLD)
    {
        uint64_t  ts_count;
        uint32_t *ts_slots = biscuit_roaring_to_array(idx->tombstones, &ts_count);
        if (ts_slots)
        {
            uint64_t k;
            for (k = 0; k < ts_count; k++)
                biscuit_remove_from_all_indices(index, idx, ts_slots[k], pending_list_limit);
            pfree(ts_slots);
        }
        biscuit_roaring_free(idx->tombstones);
        idx->tombstones      = biscuit_roaring_create();
        idx->tombstone_count = 0;
    }

    /*
     * Bump the generation counter now that the in-memory bitmap mutation
     * above has completed successfully, and persist it immediately. See
     * the matching comment in biscuit_insert() -- this is intentionally
     * non-transactional (over-invalidation on rollback is acceptable,
     * under-invalidation is not) and runs unconditionally for both the
     * legacy single-column and multi-column deletion paths above, not
     * gated on idx->num_columns == 1.
     *
     * The increment itself now happens inside biscuit_write_metadata_to_
     * disk(), under the metapage's exclusive lock, and idx->gen is pulled
     * forward to match there -- see the GEN FIX comment in that function
     * for why a local idx->gen++ before this call is no longer correct.
     */
    biscuit_write_metadata_to_disk(index, idx);

    /*
     * No eager full-blob resave here -- see the matching comment at the
     * end of biscuit_insert(). Every removal above (both the main
     * delete_indices loop and the tombstone-purge loop) already went
     * through biscuit_remove_from_all_indices(), which durably appends to
     * each touched structure's pending list as it goes, and each deleted
     * slot's TID/STRCACHE was made durable in place, per-row, by the
     * biscuit_persist_row_identity_write_record() call above.
     *
     * MISSING-FLUSH FIX -- tombstones/free-list/HEADER counters.
     *
     * Those three are NOT per-row data (see biscuit_persist_save_row_
     * identity()'s header comment): they are index-wide scalars deferred
     * to a pre-commit xact callback via biscuit_mark_row_identity_dirty(),
     * exactly as biscuit_insert() does at the end of its own function.
     * This call was missing here, which meant idx->tombstones and
     * idx->free_list were durably correct only in the memory of whichever
     * backend ran this VACUUM -- never flushed to the on-disk HEADER blob.
     *
     * A backend that later reloads (biscuit_get_current_index() sees
     * meta->gen advanced past its own idx->gen, e.g. because THIS VACUUM
     * bumped it) calls biscuit_load_index(), which reads back a durably
     * correct TID array (invalid for every slot this VACUUM tombstoned --
     * that part was always per-row durable) alongside a STALE tombstones
     * bitmap that does not mark those same slots. biscuit_scan.c's
     * tombstone filter (biscuit_roaring_andnot_inplace(candidates,
     * so->index->tombstones)) then cannot subtract a slot the bitmap
     * doesn't know is dead, so the slot survives into the final matching
     * set with no valid TID behind it -- exactly the "scan result includes
     * slot N with no valid TID" ERROR biscuit_collect_sorted_tids_single()
     * raises. Marking dirty here, unconditionally whenever this function
     * ran (even with delete_count == 0, since the tombstone-purge branch
     * above can still have reset idx->tombstones), closes that gap the
     * same way biscuit_insert() already closes it for its own mutations.
     */
    biscuit_mark_row_identity_dirty(RelationGetRelid(index));

    MemoryContextSwitchTo(oldcontext);

    stats->num_pages   = 1;
    stats->pages_deleted = 0;
    stats->pages_free  = 0;

    return stats;
}



/*
 * biscuit_vacuum_drain_one has been removed.
 *
 * It walked every directory entry and drained each structure's own
 * pending chain. With those chains replaced by a single index-wide log
 * (biscuit_pendlog.c) there is nothing per-entry left to drain -- the
 * whole log is merged in one pass by biscuit_pendlog_drain_all(), which
 * groups records by structure itself and so visits each structure once
 * regardless of how many times it was touched.
 *
 * The kind guard that used to live here (refusing TIDS/STRCACHE/HEADER so
 * their repurposed value-heap fields were never read as pending-record
 * chains) is no longer needed for the same reason: nothing walks
 * directory entries looking for pending chains any more.
 */

/* ================================================================
 * SECTION 6 – Remaining AM callbacks
 * ================================================================ */

IndexBulkDeleteResult *
biscuit_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    Relation index = info->index;
    int      structures_drained;

    if (!stats)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

    /*
     * Unconditional full drain of every structure's pending list into
     * its compacted blob -- the VACUUM backstop from Phase 1 Contract
     * §2b. Unlike the old gen-threshold backstop this ran unconditionally
     * regardless of idx->gen -- there is no in-memory BiscuitIndex
     * involved at all here (contrast biscuit_insert()/biscuit_bulkdelete(),
     * which mutate idx and so need biscuit_cache_lookup()/biscuit_load_index()
     * first): draining is purely a durable-storage operation over the
     * on-disk directory, so it works correctly even for a cold index with
     * nothing cached for this backend.
     */
    /*
     * One pass over the shared log merges every structure. This replaced
     * a walk over every directory entry draining each structure's own
     * chain -- besides being simpler, it visits a structure once per
     * VACUUM regardless of how many times it was written, where the old
     * walk re-serialized a hot structure's blob once per threshold
     * crossing.
     */
    structures_drained = biscuit_pendlog_drain_all(index, true, NULL);   /* VACUUM must not skip.
                                                                          * No in-memory BiscuitIndex
                                                                          * is involved here (see the
                                                                          * comment above), so there is
                                                                          * no idx->gen to resync and
                                                                          * the drain's own gen value
                                                                          * is not needed. */

    /*
     * total_drains is already bumped by biscuit_pendlog_drain_all() (it
     * owns the metapage reset, so bumping the counter there keeps both in
     * one transaction). total_pending_bytes is now trivially 0 after a
     * drain: the log is the only place pending records live, and the
     * drain empties it -- no directory walk is needed to "recompute" it,
     * and pendlog_bytes in the metapage is the live figure anyway.
     */
    elog(DEBUG1, "biscuit: vacuumcleanup drained %d structure(s) from the shared pending log",
         structures_drained);

    return stats;
}

bool
biscuit_canreturn(Relation index, int attno)
{
    (void) index;
    (void) attno;
    return false;
}

/* ================================================================
 * SECTION 6a -- Cost model
 * ================================================================
 *
 * Replaces the "TEMPORARY DIAGNOSTIC" flat *indexTotalCost = 1.0 that
 * made Biscuit win every LIKE/ILIKE path unconditionally, including the
 * cases where it is one to three orders of magnitude slower than the
 * alternatives.
 *
 * Measured on PG 18.4, one core, warm steady state.  Each figure is the
 * median of 7 runs using a DIFFERENT literal per run, so repeated
 * identical lookups cannot inflate the result by re-reading the same
 * cached posting lists; the first two runs of each class are discarded.
 * (Repeating one literal understated prefix latency by ~7x, so this
 * matters.)  Cold (post-restart, page cache dropped) numbers are
 * deliberately NOT used here -- see the note on startup cost below.
 *
 * Baseline table, 500k rows (heap 40MB / 5150 pages, Biscuit 211MB,
 * pg_trgm GIN 45MB), all times in ms:
 *
 *   class                     Biscuit     GIN      Seq    fastest
 *   ------------------------------------------------------------
 *   prefix   'usr\_1111\_%'      0.60    10.07    36.61   Biscuit
 *   suffix   '%a1\_log'          3.24    16.45    69.54   Biscuit
 *   infix 1 concrete '%a%'     182.57   525.75    78.75   Seq
 *   infix 2 concrete '%ab%'    411.09   546.47    74.03   Seq
 *   infix 3 concrete '%abc%'   423.95     7.57    71.19   GIN
 *   infix 4 concrete '%abcd%'  386.59     1.44    84.94   GIN
 *   infix 6 concrete           353.11     0.46    82.06   GIN
 *   '%a_c%'   (run 1)          389.34   522.29    72.91   Seq
 *   '%a_c_e%' (run 1)          398.50   541.01    72.31   Seq
 *   '%abc%def%' (2 parts)       27.42     0.62    81.21   GIN
 *   '%ab%cd%ef%' (3 parts)     246.81   547.88    83.51   Seq
 *
 * Three structural facts drive the model:
 *
 * 1. Biscuit's infix cost is independent of selectivity.  Holding the
 *    pattern shape fixed at 1M rows and varying only the match fraction
 *    (uppercase markers, so marker frequency == selectivity exactly):
 *
 *      selectivity        50%     10%      1%    0.1%   0.01%
 *      Biscuit (run 2)  1243.4  1248.5  1260.7  1243.6  1234.3
 *      Seq               172.2   154.5   144.2   139.8   139.0
 *      GIN     (run 4)   137.0    26.7     2.8    0.65    0.12
 *
 *    Biscuit varies by 2% across a 5000x selectivity range; GIN varies by
 *    ~1100x.  An infix probe does fixed whole-structure work and cannot
 *    benefit from a selective predicate.  Anchored probes are 2-3 orders
 *    of magnitude cheaper, so the anchor -- not the selectivity -- is
 *    what makes Biscuit fast.
 *
 * 1a. Nor does the picture change with table size.  Biscuit's infix time
 *    and the seqscan both scale linearly, so their ratio is flat and the
 *    lines never cross:
 *
 *      N        Biscuit '%ab%'    Seq     ratio   Biscuit prefix / Seq
 *      125k          102.6       18.6      5.5x      0.07 /   6.3
 *      500k          418.9       72.1      5.8x      0.35 /  35.9
 *      2M           1834.3      361.5      5.1x      1.35 / 156.5
 *      4M           3430.1      713.2      4.8x     14.78 / 321.3
 *
 *    A 32x span in table size holds the infix ratio between 4.8x and
 *    5.8x with no trend toward 1.  Anchored patterns stay dominant at
 *    every size.  Note also that CREATE INDEX memory scales with N: an
 *    8M-row build was OOM-killed at 3.78GB RSS on a 4GB host, and at 4M
 *    the index is 1626MB against a 322MB heap (~5x the table).
 *
 * 2. pg_trgm needs THREE CONSECUTIVE concrete characters to extract a
 *    trigram.  '%ab%' and '%a_c%' yield no usable trigram, so GIN
 *    degrades to a full index scan (520-550ms) and PostgreSQL's own GIN
 *    cost estimate correctly balloons (69839 vs 216 for '%abcd%').  This
 *    is exactly the region the requirement reserves for Biscuit, and it
 *    is why an earlier revision discriminated on the longest RUN of
 *    consecutive concrete characters (a run of 3 is what pg_trgm can
 *    actually consume).  THAT DISCRIMINATOR IS DELIBERATELY GONE, and
 *    nothing below branches on run length.  Two reasons:
 *
 *      a) Biscuit's own infix cost does not vary with run length.
 *         Measured at L=48, 500k rows: '%ab%' 441ms, '%abc%' 459ms,
 *         '%abcd%' 449ms.  The position sweep dominates and the literal
 *         is almost free, so a single infixBase is the honest estimate
 *         for all of them.
 *      b) The run distinction is a fact about GIN, not about Biscuit,
 *         and GIN's own estimator already prices it correctly (69839 for
 *         '%ab%', which it cannot serve with a trigram, against 216 for
 *         '%abcd%', which it can).  Encoding a competitor's cost model
 *         here would duplicate knowledge that the planner already has
 *         and would go stale independently of it.
 *
 *    '_' still favours Biscuit, but that is expressed where it belongs:
 *    as a discount on the anchored branch, not as a run computation.
 *
 * Anchors take priority over the infix rules: the requirement discourages
 * "infix only" patterns, and 'usr\_1%abc%' (prefix + infix part) measures
 * 2.68ms for Biscuit against 8.26ms GIN / 41.16ms Seq, so an anchored
 * pattern stays cheap regardless of what its infix parts look like.
 *
 * Costs are expressed as multiples of a locally recomputed sequential-scan
 * baseline so the model scales with the table instead of hard-coding
 * absolute cost units.  On the benchmark table the baseline evaluates to
 * 5150*1.0 + 500000*(0.01+0.0025) = 11400, which matches the planner's own
 * Seq Scan estimate of 11400.00 exactly.
 */

/*
 * ABSOLUTE infix cost model.
 *
 * Earlier revisions priced infix as a fixed multiple of the sequential-scan
 * baseline.  That was wrong in kind, not merely in calibration: a controlled
 * experiment with the SAME strings and the SAME index over two heaps of 2703
 * and 8621 pages measured Biscuit at 28.300ms and 28.046ms (ratio 0.99) while
 * the seqscan went 37.355ms -> 67.368ms (ratio 1.80).  Biscuit's infix cost is
 * INDEPENDENT of heap width; the seqscan's is proportional to it.  Tying one to
 * the other guaranteed a wrong answer on any table whose row width and string
 * length were not in the ratio of the calibration set.
 *
 * What infix cost actually tracks is row count times the SQUARE of the string
 * length -- the position sweep is O(L) and each step's bitmap work grows with L
 * again.  Measured on 500k rows, pattern '%abc%', varying only string length:
 *
 *     L      time      ms/(N*L^2)
 *     12     28.3ms    3.93e-07
 *     24    122.9ms    4.27e-07
 *     48    459.5ms    3.99e-07
 *     96   1804.9ms    3.92e-07     <- constant to +-5% over an 8x range
 *
 * BISCUIT_INFIX_K converts that into planner cost units, anchored so that the
 * L=48 case (459ms Biscuit vs 107ms seqscan against a seqBaseline of 11405)
 * lands at the correct side of the comparison.  Validated against every dataset
 * measured, including a reported production table:
 *
 *     table          L     N      heapPg  bisCost  seqBase  picks  actual
 *     l12           12    500k     2703     3058     8953   bis    28 vs 37ms  OK
 *     l24           24    500k     3677    12231     9927   seq   123 vs 58ms  OK
 *     l48           48    500k     5155    48925    11405   seq   459 vs107ms  OK
 *     l96           96    500k     8197   195702    14447   seq  1805 vs171ms  OK
 *     w12           12    500k     8621     3058    14871   bis    28 vs 67ms  OK
 *     interactions  15    1M      16528     9556    29028   bis    21 vs 70ms  OK
 */
#define BISCUIT_INFIX_K                4.25e-5  /* cost units per tuple per L^2 */
#define BISCUIT_DEFAULT_STRLEN         32       /* when avgwidth is unavailable */
#define BISCUIT_MAX_STRLEN             4096     /* clamp, guards L^2 overflow   */

/*
 * Multi-part infix factors, relative to a single infix part.
 *
 * These correct an outright inversion in the previous model, which DISABLED
 * every pattern with two or more infix parts.  Extra parts are additional
 * constraints that prune the sweep, so two parts are an order of magnitude
 * CHEAPER than one, consistently across string lengths:
 *
 *     L     1 part   2 parts   ratio    3 parts   ratio
 *     12    30.4ms    1.7ms    0.056     5.1ms    0.17
 *     24   119.4ms   10.5ms    0.088    71.3ms    0.60
 *     48   467.7ms   48.3ms    0.103   691.6ms    1.48
 *
 * Three or more parts stop benefiting and scale worse than L^2 (about L^3.5),
 * so they are charged ABOVE a single part.  1.50 is the L=48 measurement and is
 * deliberately conservative at shorter lengths, where 3-part patterns are in
 * fact cheaper -- erring toward a seqscan there costs little in absolute terms.
 *
 * Note this factor applies WITHIN one pattern.  Separate scan keys
 * ('s LIKE a AND s LIKE b') remain additive and are summed by the caller:
 * that path shares no sweep and does not prune (measured: adding a 26-row
 * anchored key to an infix key saved nothing, 1543ms vs 1549ms).  Conflating
 * the two is what produced the disable rule this replaces.
 */
#define BISCUIT_PARTS2_FACTOR          0.10
#define BISCUIT_PARTS3_FACTOR          1.50

/*
 * Length-predicate patterns: no concrete characters at all, but at least one
 * '_'.  '______' is "length = 6"; '______%' is "length >= 6".  Biscuit answers
 * these from its length bitmaps with a SINGLE lookup -- no position sweep, no
 * character ANDs -- so they are cheaper than an anchored probe, not more
 * expensive.
 *
 * These were previously lumped in with '%' and hard-disabled at
 * BISCUIT_COST_DISABLED, which was simply wrong.  Measured on a 1M-row table
 * against a pg_trgm GIN + B-tree reference (which must seqscan these, since
 * neither a trigram nor a B-tree range can express "length = k"):
 *
 *     pattern                  underscores   Biscuit   reference   speedup
 *     '______'                      6         69.94ms    93.68ms     1.34x
 *     '______%'                     6        163.68ms   124.87ms     0.76x
 *     '________'                    8         70.08ms   128.95ms     1.84x
 *     '____________'               12         67.24ms   134.07ms     1.99x
 *     '_______________'            15         14.68ms    44.34ms     3.02x
 *     '_________________'          17          7.22ms    51.28ms     7.10x
 *     '___________________'        19          2.32ms    50.48ms    21.80x
 *     '___________________%'       19          1.98ms    47.72ms    24.11x
 *
 * Biscuit wins 8 of 10 and 1.62x on aggregate time (530ms vs 857ms), yet the
 * old rule refused to generate a path for any of them.
 *
 * Note the cost below deliberately does NOT try to model how many rows a given
 * underscore count selects.  The two losses above are the two least selective
 * patterns ('______%' matches 962,308 of 1,000,000 rows), and those are exactly
 * the cases PostgreSQL's own bitmap-heap costing rejects on its own once the
 * index cost stops being infinite: at 96% selectivity a bitmap heap scan prices
 * above a seqscan without any help from us.  Reporting a cheap, honest index
 * cost and letting the heap-access model gate selectivity is the correct
 * division of labour, and it is what makes the unselective cases fall back
 * while the selective ones (7x-24x) get the index.
 */
#define BISCUIT_COST_LENGTH_FRAC       0.002  /* single length-bitmap lookup */

/* Cost as a fraction/multiple of the sequential-scan baseline. */
/*
 * 0.005, not the 0.02 first calibrated at 500k rows.  GIN's own estimate
 * for an anchored pattern grows faster with N than this fraction does:
 * at 4M rows 0.02 puts Biscuit at 1824 against GIN's 1864, a 1.02x margin,
 * so a slightly larger table would flip prefix queries to GIN even though
 * Biscuit measures 14.8ms there against GIN's 75.7ms.  0.005 holds a ~4-5x
 * margin at both 500k and 4M and stays far below the measured ratio
 * (prefix Biscuit/Seq is 0.009-0.046 across the sizes tested), so the
 * anchored path cannot be lost to rounding as the table grows.
 */
/*
 * 0.002, lowered from 0.005 after a GA accuracy test on Costly Cookies-v3
 * (1M rows, biscuit + pg_trgm GIN + B-tree text_pattern_ops, planner free).
 *
 * At 0.005 every both-anchored and long-suffix query lost to GIN by a 10-15%
 * cost margin while being 6-12x FASTER in reality:
 *
 *     query                 bis cost  ref cost  margin   bis ms  ref ms
 *     'derek%1a'               484.1     434.9   11.3%     0.53    4.64
 *     'd__ek%1a'               414.2     374.9   10.5%     0.42    2.59
 *     ILIKE 'DEREK%1A'         484.1     434.9   11.3%     0.38    4.52
 *     '%grey1a'                484.1     422.0   14.7%     0.23    1.19
 *     ILIKE '%GREY1A'          484.1     422.0   14.7%     0.21    1.49
 *
 * 0.002 closes all of them.  It is safe against the B-tree because pure-prefix
 * queries lose on cost by 88%-427% -- two orders of magnitude more than this
 * delta moves -- so they stay with the B-tree, which is correct: it beat
 * Biscuit on 5 of 6 selective prefixes.  Measured effect over the 36-query GA
 * set: accuracy 69% -> 83%, mean penalty 2.46x -> 1.16x, worst case
 * 12.52x -> 2.30x.
 */
#define BISCUIT_COST_ANCHORED_FRAC     0.002  /* prefix and/or suffix */
#define BISCUIT_COST_USCORE_DISCOUNT   0.5    /* favour '_' patterns              */
#define BISCUIT_COST_UNKNOWN_FRAC      0.90   /* pattern not a Const at plan time */

/*
 * Effectively infinite, but finite: the planner does arithmetic on these
 * values (add_path comparisons, startup+total sums), and a true HUGE_VAL
 * risks Inf/NaN propagation.  1e18 is ~14 orders of magnitude above any
 * realistic seqscan cost, so the path is never chosen.
 */
#define BISCUIT_COST_DISABLED          1.0e18

/*
 * One index qual's contribution to a conjunction's cost: its own cost if
 * it ran first, and its selectivity (how much of the row set it leaves
 * for the keys evaluated after it).  See the aggregation comment in
 * biscuit_costestimate().
 */
typedef struct BiscuitKeyCost
{
    double cost;
    double sel;
} BiscuitKeyCost;

typedef struct BiscuitPatternShape
{
    bool has_prefix;        /* non-empty literal before the first '%'  */
    bool has_suffix;        /* non-empty literal after the last '%'    */
    int  n_infix;           /* non-empty literal segments in between   */
    int  n_wild_uscore;     /* count of unescaped '_' wildcards        */
    bool all_wild;          /* no literal content at all               */
} BiscuitPatternShape;

/*
 * Split a LIKE/ILIKE pattern on unescaped '%' and describe its shape.
 *
 * A backslash escapes the following character, which then counts as a
 * concrete character (so '\_' is a literal underscore and DOES extend a
 * trigram run, while a bare '_' is a single-character wildcard that
 * breaks one).  This mirrors PostgreSQL's default LIKE escape.
 */
static void
biscuit_classify_pattern(const char *pat, int len, BiscuitPatternShape *sh)
{
    int i;
    int seg = 0;            /* 0 == the segment before the first '%' */
    int seg_concrete = 0;

    memset(sh, 0, sizeof(*sh));

    for (i = 0; i < len; i++)
    {
        char c = pat[i];

        if (c == '\\' && i + 1 < len)
        {
            i++;                        /* escaped -> literal character */
            seg_concrete++;
            continue;
        }
        if (c == '%')
        {
            if (seg == 0)
            {
                if (seg_concrete > 0) sh->has_prefix = true;
            }
            else if (seg_concrete > 0)
            {
                /* a segment closed by '%' with another segment after it */
                sh->n_infix++;
            }
            seg++;
            seg_concrete = 0;
            continue;
        }
        if (c == '_')
        {
            sh->n_wild_uscore++;
            continue;
        }
        seg_concrete++;
    }

    /* close the final, still-open segment */
    if (seg == 0)
    {
        /* no unescaped '%' anywhere: anchored at both ends */
        if (seg_concrete > 0)
        {
            sh->has_prefix = true;
            sh->has_suffix = true;
        }
    }
    else if (seg_concrete > 0)
        sh->has_suffix = true;

    sh->all_wild = (!sh->has_prefix && !sh->has_suffix && sh->n_infix == 0);
}

/*
 * Map a pattern shape onto a cost, in the same currency as seqBaseline.
 */
static double
biscuit_cost_for_shape(const BiscuitPatternShape *sh,
                       double seqBaseline, double infixBase)
{
    double uscore = (sh->n_wild_uscore > 0) ? BISCUIT_COST_USCORE_DISCOUNT : 1.0;

    /*
     * No concrete characters anywhere.  Two very different cases:
     *
     *   '%' / '%%'      -- matches every row; the index can contribute
     *                      nothing, so refuse the path.
     *   '______', '__%' -- a LENGTH predicate, answered from the length
     *                      bitmaps in one lookup.  See BISCUIT_COST_LENGTH_FRAC:
     *                      these are among Biscuit's strongest queries (up to
     *                      24x faster than a GIN+B-tree reference, which has to
     *                      seqscan them) and were previously disabled outright.
     */
    if (sh->all_wild)
    {
        if (sh->n_wild_uscore > 0)
            return seqBaseline * BISCUIT_COST_LENGTH_FRAC;
        return BISCUIT_COST_DISABLED;
    }

    /*
     * Anchored at either end.  A prefix or suffix probe is a fixed number of
     * positional-bitmap ANDs -- it does not sweep positions, so it is cheap
     * regardless of string length (measured 0.09ms at L=12 and 0.80ms at
     * L=204, against seqscans of 37ms and 171ms).  Priced relative to the
     * seqscan baseline, which is what it actually competes against, and
     * validated at 43 wins out of 46 anchored queries.
     *
     * KNOWN LIMIT, deliberately not addressed here.  Against a B-tree with
     * text_pattern_ops, Biscuit loses the cost comparison on every selective
     * prefix (B-tree Index Scan totals 8.45 where Biscuit's Bitmap Heap Scan
     * totals 519) and yet is 2x better in AGGREGATE time over a measured
     * 18-query prefix set: 18.69ms against 37.19ms.  B-tree wins the count
     * (11 of 18) but only on queries already under a millisecond, while
     * Biscuit's bounded bitmap probe wins the broad prefixes that actually
     * cost something ('con%': 3.83ms vs 17.65ms).  Lowering
     * BISCUIT_COST_ANCHORED_FRAC cannot recover this: even at an index cost of
     * zero, the bitmap-heap path prices around 374 against the B-tree's 8.45,
     * so the gap is in the heap-access model, not in what this function
     * reports.  Index-only scan support (biscuit_canreturn) is what would
     * close it.
     */
    if (sh->has_prefix || sh->has_suffix)
        return seqBaseline * BISCUIT_COST_ANCHORED_FRAC * uscore;

    /*
     * Infix-only.  Cost is absolute (see BISCUIT_INFIX_K), NOT a multiple of
     * the seqscan baseline, because the position sweep is unaffected by how
     * wide the heap rows are.  The planner then compares this against the
     * seqscan and any pg_trgm GIN path on their own merits; no knowledge of
     * GIN's cost model is encoded here, and none is needed -- GIN prices its
     * own inability to extract a trigram correctly (69839 for '%ab%' against
     * 216 for '%abcd%').
     */
    if (sh->n_infix >= 3)
        return infixBase * BISCUIT_PARTS3_FACTOR;
    if (sh->n_infix == 2)
        return infixBase * BISCUIT_PARTS2_FACTOR;
    return infixBase;
}

/*
 * Pull the pattern constant out of an index qual, if it is a constant at
 * plan time.  Returns a palloc'd C string, or NULL.
 */
static char *
biscuit_pattern_from_clause(Expr *clause)
{
    OpExpr *op;
    Node   *rightop;
    Const  *con;

    if (!IsA(clause, OpExpr))
        return NULL;
    op = (OpExpr *) clause;
    if (list_length(op->args) != 2)
        return NULL;

    rightop = (Node *) lsecond(op->args);
    if (!IsA(rightop, Const))
        return NULL;

    con = (Const *) rightop;
    if (con->constisnull)
        return NULL;
    if (con->consttype != TEXTOID)
        return NULL;

    return TextDatumGetCString(con->constvalue);
}

void
biscuit_costestimate(PlannerInfo *root, IndexPath *path,
                    double loop_count,
                    Cost *indexStartupCost, Cost *indexTotalCost,
                    Selectivity *indexSelectivity,
                    double *indexCorrelation, double *indexPages)
{
    IndexOptInfo *index = path->indexinfo;
    List         *indexQuals;
    List         *selectivityQuals;
    ListCell     *lc;
    double        numPages;
    double        heapPages;
    double        heapTuples;
    double        seqBaseline;
    double        strLen;
    double        infixBase;
    double        spc_random_page_cost;
    double        spc_seq_page_cost;
    double        cost = -1.0;        /* summed cost over all quals */
    bool          sawPattern = false;
    bool          disabled = false;

    (void) loop_count;

    numPages = (index->pages > 0) ? (double) index->pages : 1.0;
    if (indexPages)
        *indexPages = numPages;

    /*
     * Biscuit returns rows in heap order at best; treat it like GIN and
     * claim no correlation rather than the old (wrong) 1.0, which told
     * the planner this scan was perfectly ordered.
     */
    *indexCorrelation = 0.0;

    if (path->indexclauses == NIL)
    {
        /*
         * No usable quals (e.g. plain SELECT * with no WHERE). Make this
         * path unattractive so the planner falls back to a seqscan instead
         * of using Biscuit with zero scan keys, which would return 0 rows.
         */
        *indexStartupCost = BISCUIT_COST_DISABLED;
        *indexTotalCost   = BISCUIT_COST_DISABLED;
        *indexSelectivity = 1.0;
        return;
    }

    /*
     * Real selectivity, instead of the previous hard-coded 0.01.  Without
     * this every Biscuit path claimed 1% of the table regardless of the
     * pattern, which corrupted the row estimates of everything above it
     * in the plan (joins especially), not merely the scan choice.
     */
    indexQuals       = get_quals_from_indexclauses(path->indexclauses);
    selectivityQuals = add_predicate_to_index_quals(index, indexQuals);
    *indexSelectivity = clauselist_selectivity(root, selectivityQuals,
                                               index->rel->relid,
                                               JOIN_INNER, NULL);

    get_tablespace_page_costs(index->reltablespace,
                              &spc_random_page_cost,
                              &spc_seq_page_cost);

    /*
     * Sequential-scan baseline for the underlying heap, recomputed here so
     * the model is relative to the table rather than absolute.  Matches
     * cost_seqscan()'s shape: pages * seq_page_cost plus per-tuple CPU.
     */
    heapPages  = (index->rel && index->rel->pages   > 0) ? (double) index->rel->pages  : 1.0;
    heapTuples = (index->rel && index->rel->tuples  > 0) ? index->rel->tuples          : 1.0;
    seqBaseline = heapPages * spc_seq_page_cost
                  + heapTuples * (cpu_tuple_cost + cpu_operator_cost);
    if (seqBaseline < 1.0)
        seqBaseline = 1.0;

    /*
     * Effective indexed string length L, used for the absolute infix model.
     *
     * Taken from pg_statistic via get_attavgwidth() rather than from the
     * column's declared width: a varchar(100) holding 12-character usernames
     * must cost like L=12, not L=100, and that distinction decides the plan
     * on real tables.  This is planner-native and needs no index I/O.
     *
     * ANALYZE DEPENDENCY.  get_attavgwidth() returns 0 outright when the
     * column has no pg_statistic row -- it does NOT fall back to a type
     * estimate itself (callers in costsize.c do that).  So on a freshly
     * loaded table, before autoanalyze catches up, L would otherwise be a
     * blind constant and every infix cost would be wrong by (real_L/32)^2.
     * We therefore mirror what the planner does and fall back to
     * get_typavgwidth(), which derives an estimate from the type and typmod
     * (for varchar(n) a sliding fraction of n).  That is still only a guess,
     * but it is a type-aware one, and it degrades toward the declared width
     * rather than to an unrelated number.
     *
     * The hardcoded default remains as a last resort for expression indexes
     * (indexkeys[0] == 0), where there is no attribute to consult at all.
     * Plans for infix patterns can therefore change after the first ANALYZE
     * on a new table; that is expected, not a bug.
     */
    {
        int32   avgwidth = 0;

        if (index->rel != NULL && index->nkeycolumns > 0 &&
            index->indexkeys[0] != 0 && root != NULL)
        {
            RangeTblEntry *rte = planner_rt_fetch(index->rel->relid, root);

            if (rte != NULL && rte->rtekind == RTE_RELATION)
            {
                AttrNumber attnum = (AttrNumber) index->indexkeys[0];

                avgwidth = get_attavgwidth(rte->relid, attnum);

                if (avgwidth <= 0)
                {
                    /* no ANALYZE yet -- derive from the declared type */
                    Oid     atttypid;
                    int32   atttypmod;
                    Oid     attcollation;

                    get_atttypetypmodcoll(rte->relid, attnum,
                                          &atttypid, &atttypmod, &attcollation);
                    avgwidth = get_typavgwidth(atttypid, atttypmod);
                }
            }
        }

        strLen = (avgwidth > VARHDRSZ) ? (double) (avgwidth - VARHDRSZ)
                                       : (double) BISCUIT_DEFAULT_STRLEN;
        if (strLen < 1.0)
            strLen = 1.0;
        if (strLen > BISCUIT_MAX_STRLEN)
            strLen = BISCUIT_MAX_STRLEN;
    }

    infixBase = BISCUIT_INFIX_K * heapTuples * strLen * strLen;
    if (infixBase < 1.0)
        infixBase = 1.0;

    /*
     * Conjunction cost: cheapest key at full price, every later key
     * scaled by the selectivity of the keys before it.
     *
     * THIS REPLACES A SUM, AND THE CHANGE IS FORCED BY AN EXECUTOR
     * CHANGE, NOT A RECALIBRATION.  Before candidate-mask threading
     * landed in biscuit_rescan(), each scan key was evaluated to
     * completion over the whole table and the results ANDed afterward,
     * so conjunction cost really was additive: an AND of two infix keys
     * measured 5146ms against ~2700ms for either alone.  Summing was
     * correct for that executor.
     *
     * The executor now sorts keys by selectivity, evaluates the cheapest
     * first, and threads the surviving row set into every later key as a
     * mask -- so a later key only examines rows that are still alive.
     * Re-measured on 500k rows, warm (A,B anchored; C,D,E infix, with
     * C leaving 2 rows and D leaving 250k):
     *
     *   conjunction        alone         together   vs cheapest
     *   A AND B          0.845 / 65.8     0.503        0.60x
     *   A AND C          0.845 / 1522     0.442        0.52x
     *   C AND D          1522  / 1590     1518         1.00x
     *   C AND E          1522  / 1621     1623         1.07x
     *   D AND E          1590  / 1621     2523         1.59x
     *   A AND B AND C         --          0.630        0.75x
     *   C AND D AND E         --          1530         1.01x
     *
     * That is neither SUM nor MAX nor MIN.  D AND E is the tell: it is
     * the only expensive pair, and the only one whose first key leaves a
     * large mask (250k rows) for the second to work through.  Cost
     * tracks how much the earlier keys PRUNE, so:
     *
     *     cost = sum over keys, cheapest first, of
     *              cost(key) * (product of sel of all earlier keys)
     *
     * which predicts 1.00x for every row above except D AND E, where it
     * predicts 1.50x against 1.59x measured.
     *
     * Practical effect: an anchored key next to an infix key now prices
     * at roughly the anchored key alone, which is what the executor
     * actually does (0.442ms measured, against 1543ms before the mask
     * fix).  Under the old SUM this conjunction was priced at the sum of
     * both and lost to GIN despite being ~9x faster.
     *
     * NEGATION deliberately needs no special case.  NOT LIKE / NOT ILIKE
     * are indexable strategies for this opclass (Biscuit produced an index
     * path for 275/275 queries in a combinatorial matrix; pg_trgm GIN
     * managed only 173/275).  Measured per position, negation does not
     * change which engine wins: anchored patterns still go to Biscuit
     * under negation (suffix 8/8 wins, prefix 6/8, both-anchored 3/4) and
     * infix patterns still lose under negation (0/10), which is what
     * pattern-shape classification already delivers.  Adding a negation
     * penalty would lose the anchored-negation cases, where Biscuit beats
     * the runner-up by up to 5531x (0.1ms vs 566ms for a negated anchored
     * ILIKE).  Note the executor separately inverts its OWN ordering score
     * for negated strategies (a strongly-anchored NOT pattern returns
     * ~99.99% of the table); that is an ordering concern, not a cost one.
     *
     * An ILIKE-specific baseline multiplier was also tested -- ILIKE
     * costs the ALTERNATIVES ~4x (seqscan 149ms -> 635ms) while costing
     * Biscuit almost nothing (52ms -> 97ms), so scaling the baseline for
     * case-insensitive clauses looked promising.  It is NOT implemented:
     * once the short-run infix class is priced correctly the factor
     * changes nothing at all (38.9s at every multiplier from 1.0 to 4.0).
     * It was only ever compensating for the mispriced infix branch.
     */
    {
        BiscuitKeyCost *kc = NULL;
        int             nkc = 0;
        int             kcmax = 0;
        int             a, b;
        double          running_sel = 1.0;

        /* upper bound on how many quals we might collect */
        foreach(lc, path->indexclauses)
            kcmax += list_length(((IndexClause *) lfirst(lc))->indexquals);
        if (kcmax > 0)
            kc = (BiscuitKeyCost *) palloc(kcmax * sizeof(BiscuitKeyCost));

        foreach(lc, path->indexclauses)
        {
            IndexClause *iclause = lfirst_node(IndexClause, lc);
            ListCell    *lc2;

            foreach(lc2, iclause->indexquals)
            {
                RestrictInfo        *rinfo = lfirst_node(RestrictInfo, lc2);
                char                *pat;
                BiscuitPatternShape  sh;
                double               c;

                pat = biscuit_pattern_from_clause(rinfo->clause);
                if (pat == NULL)
                    continue;           /* not a plan-time constant */

                biscuit_classify_pattern(pat, (int) strlen(pat), &sh);
                c = biscuit_cost_for_shape(&sh, seqBaseline, infixBase);
                pfree(pat);

                sawPattern = true;
                if (c >= BISCUIT_COST_DISABLED)
                {
                    /* one unusable pattern disables the whole path */
                    cost = BISCUIT_COST_DISABLED;
                    disabled = true;
                    break;
                }

                kc[nkc].cost = c;
                kc[nkc].sel  = clause_selectivity(root, (Node *) rinfo,
                                                  index->rel->relid,
                                                  JOIN_INNER, NULL);
                if (kc[nkc].sel < 0.0)  kc[nkc].sel = 0.0;
                if (kc[nkc].sel > 1.0)  kc[nkc].sel = 1.0;
                nkc++;
            }
            if (disabled)
                break;
        }

        if (!disabled && nkc > 0)
        {
            /*
             * Cheapest key first (insertion sort: nkc is the number of
             * index quals on one column, in practice 1-3).
             */
            for (a = 1; a < nkc; a++)
            {
                BiscuitKeyCost t = kc[a];

                for (b = a - 1; b >= 0 && kc[b].cost > t.cost; b--)
                    kc[b + 1] = kc[b];
                kc[b + 1] = t;
            }

            cost = 0.0;
            for (a = 0; a < nkc; a++)
            {
                cost += kc[a].cost * running_sel;
                running_sel *= kc[a].sel;
            }
        }

        if (kc)
            pfree(kc);
    }

    if (!sawPattern)
    {
        /*
         * Parameterised pattern ($1, or a non-Const expression): the shape
         * is unknown until execution, so neither favour nor forbid the
         * index.  Priced just under the seqscan baseline -- Biscuit is
         * preferred to a full scan, but loses to any GIN path, which is
         * cheaper still in precisely the cases GIN handles well.
         */
        cost = seqBaseline * BISCUIT_COST_UNKNOWN_FRAC;
    }

    *indexTotalCost = cost;

    /*
     * Startup cost.
     *
     * Biscuit's first probe after a cold start is dominated by loading the
     * index (measured 1181ms for a prefix probe against 0.60ms warm, and
     * 4529ms for an infix probe -- roughly a 10^3-10^4 factor).  That is
     * deliberately NOT charged here: it is paid once per backend, not per
     * scan, so folding it into a per-path startup cost would push every
     * Biscuit path out of contention on exactly the queries the index
     * exists to serve.  A small nominal startup keeps LIMIT-style plans
     * from treating the scan as entirely free.
     *
     * KNOWN RISK, not a settled tradeoff.  "Once per backend" is the
     * assumption, and it is not always true: the index reaches 3-8x the
     * heap (1626MB against a 322MB heap at 4M rows) and can be evicted
     * mid-session under memory pressure.  This was observed, not merely
     * postulated -- a warm 112ms infix probe reverted to 636ms after a
     * few intervening seqscans pushed the index out of cache.  When that
     * happens the planner has no signal at all that the next probe may be
     * orders of magnitude slower than costed, and nothing here lets it
     * hedge.  This is the largest single source of model error in
     * production and the first thing to revisit if plans look right but
     * latency does not.
     */
    if (cost >= BISCUIT_COST_DISABLED)
        *indexStartupCost = BISCUIT_COST_DISABLED;
    else
        *indexStartupCost = cost * 0.10;
}

bytea *
biscuit_options(Datum reloptions, bool validate)
{
    (void) reloptions;
    (void) validate;
    return NULL;
}

bool
biscuit_validate(Oid opclassoid)
{
    (void) opclassoid;
    return true;
}

void
biscuit_adjustmembers(Oid opfamilyoid, Oid opclassoid,
                      List *operators, List *functions)
{
    /* Nothing to adjust */
    (void) opfamilyoid;
    (void) opclassoid;
    (void) operators;
    (void) functions;
}
