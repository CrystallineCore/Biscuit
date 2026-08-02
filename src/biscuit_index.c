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
            continue;   /* evicted (e.g. DROP in same txn) -- nothing to save */

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

void
biscuit_write_metadata_to_disk(Relation index, BiscuitIndex *idx)
{
    Buffer             buf;
    Page               page;
    GenericXLogState  *state;
    BiscuitMetaPageData *meta;
    bool               is_new_page;

    /*
     * This function is called repeatedly over an index's lifetime
     * (unconditionally from every biscuit_insert()/biscuit_bulkdelete()
     * call, to keep gen current -- see callers). Only gen actually
     * changes on those calls (num_records is now advanced under lock by
     * biscuit_claim_new_slot() and is carried forward here rather than
     * written from the caller's copy -- see below); the directory
     * roots, FSM bootstrap state, and pending-list tuning/stats
     * (allocated/maintained by later phases, not yet by anything in this
     * one) must survive every such call, not be reset to "nothing
     * allocated yet" each time. So: read the existing page's values
     * first (if any), and carry them forward across the PageInit below
     * rather than reinitializing them.
     */
    BlockNumber nblocks = RelationGetNumberOfBlocks(index);
    is_new_page = (nblocks == 0);

    if (is_new_page)
        buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
    else
        buf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);

    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    state = GenericXLogStart(index);
    page  = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

    /*
     * Snapshot the carry-forward fields before PageInit() wipes the page
     * (PageInit() zeroes the whole page including the special area, so
     * this must happen while `page` still holds the pre-init contents;
     * on a freshly-extended P_NEW page there is nothing meaningful to
     * carry forward, so fall back to defaults instead).
     */
    BlockNumber prev_dir_roots[BISCUIT_MAX_DIR_COLUMNS];
    int32       prev_num_dir_columns;
    BlockNumber prev_fsm_root;
    BlockNumber prev_pendlog_head, prev_pendlog_tail;
    BlockNumber prev_pendlog_draining;
    uint32      prev_pendlog_npages;
    uint32      prev_fsm_page_count;
    uint32      prev_pending_list_limit;
    uint64      prev_total_pending_bytes;
    uint64      prev_total_drains;
    uint32      prev_reserved[5];
    /*
     * CONCURRENCY FIX (slot-allocation race).
     *
     * num_records and gen are now carry-forward fields too, exactly like
     * dir_roots/fsm_root above -- they are NOT written from the caller's
     * idx unconditionally any more.
     *
     * Why: slot numbers are claimed from meta->num_records under the
     * metapage's exclusive buffer lock (see biscuit_claim_new_slot()), so
     * the metapage is the authoritative counter and any backend's
     * idx->num_records is a process-local copy that goes stale the moment
     * another backend claims a slot. This function runs unconditionally at
     * the end of every biscuit_insert()/biscuit_bulkdelete(), well after
     * the claim and its lock have been released. Blindly writing
     * meta->num_records = idx->num_records here would let a backend that
     * claimed slot 5 (leaving the counter at 6) stamp 6 back over a 7 that
     * a concurrent backend had since committed -- handing the *same* slot
     * number out twice and silently clobbering a row. That is the exact
     * failure mode this fix exists to close, so re-introducing it here
     * would defeat the locked claim entirely.
     *
     * Max() rather than a plain carry-forward because both counters are
     * monotonically non-decreasing over an index's lifetime (bulkdelete
     * tombstones slots, it never shrinks num_records) and because the
     * caller's value legitimately leads the page's in one case: the very
     * first write after biscuit_build(). Build always runs against a fresh
     * relfilenode, i.e. nblocks == 0 / is_new_page, so it takes the
     * defaults branch below and Max() is never asked to reconcile a
     * populated page against a rebuilt-from-zero idx.
     */
    uint32      prev_num_records;
    uint64      prev_gen;

    /*
     * Decide whether the current page already holds a real biscuit
     * metapage whose carry-forward fields (dir_roots, fsm, pending stats)
     * must be preserved across the PageInit() below.
     *
     * We must NOT use PageIsEmpty()/PageIsNew() as that gate here: a
     * biscuit metapage keeps ALL of its data in the page's special area
     * (BiscuitMetaPageData via PageGetSpecialPointer), never in the main
     * body, so pd_lower stays at SizeOfPageHeaderData and PageIsEmpty()
     * is *always* true for a validly-written metapage. Gating on it made
     * this branch never taken on a populated metapage, so every rewrite
     * after build (every insert/bulkdelete) discarded dir_roots -- resetting
     * them to InvalidBlockNumber and orphaning the entire on-disk directory
     * (compacted blobs, pending lists, HEADER entry), which then made every
     * subsequent cold biscuit_persist_load() fail with "no on-disk snapshot
     * found". The magic check below is the correct, sufficient validity
     * test for a special-area format: it confirms the special area really
     * holds a biscuit metapage before we trust its contents. PageIsNew() is
     * still worth screening for, since PageGetSpecialPointer() on a
     * never-initialized (all-zero) page would index past a zero-sized
     * special area.
     */
    if (!is_new_page && !PageIsNew(page))
    {
        BiscuitMetaPageData *old = (BiscuitMetaPageData *) PageGetSpecialPointer(page);

        if (old->magic == BISCUIT_MAGIC)
        {
            prev_num_dir_columns = old->num_dir_columns;
            memcpy(prev_dir_roots, old->dir_roots, sizeof(prev_dir_roots));
            prev_fsm_root             = old->fsm_root;
            prev_fsm_page_count       = old->fsm_page_count;
            prev_pending_list_limit   = old->pending_list_limit;
            prev_total_pending_bytes  = old->total_pending_bytes;
            prev_total_drains         = old->total_drains;
            prev_pendlog_head         = old->pendlog_head;
            prev_pendlog_tail         = old->pendlog_tail;
            prev_pendlog_npages       = old->pendlog_npages;
            prev_pendlog_draining     = old->pendlog_draining;
            memcpy(prev_reserved, old->reserved, sizeof(prev_reserved));
            prev_num_records          = old->num_records;
            prev_gen                  = old->gen;
            goto have_prev_values;
        }
    }

    /* No usable prior page (new relation, or an unrecognized/foreign one): defaults. */
    prev_num_dir_columns = 0;
    for (int i = 0; i < BISCUIT_MAX_DIR_COLUMNS; i++)
        prev_dir_roots[i] = InvalidBlockNumber;
    prev_fsm_root            = InvalidBlockNumber;
    prev_fsm_page_count      = 0;
    prev_pending_list_limit  = BISCUIT_DEFAULT_PENDING_LIST_LIMIT;
    prev_total_pending_bytes = 0;
    prev_total_drains        = 0;
    /*
     * MUST be InvalidBlockNumber, not 0. A zero-filled metapage special
     * area leaves these at 0, which is a perfectly valid-looking block
     * number -- and block 0 is the metapage itself. The shared-log append
     * path then reads the metapage as if it were a log page, decides the
     * "tail" is full, and tries to lock block 0 while already holding it,
     * self-deadlocking on the buffer content lock on an index's very first
     * insert. Defaulting these explicitly is the fix.
     */
    prev_pendlog_head        = InvalidBlockNumber;
    prev_pendlog_tail        = InvalidBlockNumber;
    prev_pendlog_npages      = 0;
    /*
     * Same reasoning as pendlog_head/tail above: a zeroed BlockNumber is
     * block 0, the metapage itself, and a later drain would read it as an
     * abandoned log chain.
     */
    prev_pendlog_draining    = InvalidBlockNumber;
    memset(prev_reserved, 0, sizeof(prev_reserved));
    /*
     * No prior metapage to reconcile against: the caller's idx is the only
     * source of truth (this is the biscuit_build() path).
     */
    prev_num_records         = (uint32) Max(idx->num_records, 0);
    prev_gen                 = idx->gen;

have_prev_values:

    PageInit(page, BufferGetPageSize(buf), sizeof(BiscuitMetaPageData));

    meta          = (BiscuitMetaPageData *) PageGetSpecialPointer(page);
    meta->magic   = BISCUIT_MAGIC;
    meta->version = BISCUIT_VERSION;
    meta->page_format_version = BISCUIT_PAGE_FORMAT_VERSION;
    /* Carry-forward + monotonic max, NOT a blind overwrite -- see the
     * long comment on prev_num_records/prev_gen above. BiscuitMetaPageData
     * .num_records is uint32 while BiscuitIndex.num_records is int, so
     * clamp before comparing to keep Max() away from a signed/unsigned
     * promotion. */
    meta->num_records = Max(prev_num_records, (uint32) Max(idx->num_records, 0));
    meta->gen         = Max(prev_gen, idx->gen);

    /* Carried forward from the previous page contents (or defaults). */
    meta->num_dir_columns = prev_num_dir_columns;
    memcpy(meta->dir_roots, prev_dir_roots, sizeof(meta->dir_roots));
    meta->fsm_root            = prev_fsm_root;
    meta->fsm_page_count      = prev_fsm_page_count;
    meta->pending_list_limit  = prev_pending_list_limit;
    meta->total_pending_bytes = prev_total_pending_bytes;
    meta->total_drains        = prev_total_drains;
    meta->pendlog_head        = prev_pendlog_head;
    meta->pendlog_tail        = prev_pendlog_tail;
    meta->pendlog_npages      = prev_pendlog_npages;
    meta->pendlog_draining    = prev_pendlog_draining;
    memcpy(meta->reserved, prev_reserved, sizeof(meta->reserved));

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

    if (PageIsNew(page) || PageIsEmpty(page))
    {
        UnlockReleaseBuffer(buf);
        *num_records = *num_columns = *max_len = 0;
        if (gen) *gen = 0;
        return false;
    }

    meta = (BiscuitMetaPageData *) PageGetSpecialPointer(page);

    /*
     * magic identifies this as a Biscuit metapage at all; version/
     * page_format_version identify which layout it was written with.
     * BISCUIT_VERSION 2's metapage layout (dir_roots/fsm_root/pending-list
     * tuning) is not compatible with a version-1 page (which had `root`
     * and a differently-sized reserved area in the same offsets) -- this
     * is the clean cutover the design doc calls for, so a mismatch here
     * is treated exactly like "no snapshot", which biscuit_load_index()
     * turns into a REINDEX-required ERROR rather than trying to interpret
     * bytes written under the old layout as if they were the new one.
     */
    if (meta->magic != BISCUIT_MAGIC ||
        meta->version != BISCUIT_VERSION ||
        meta->page_format_version != BISCUIT_PAGE_FORMAT_VERSION)
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

    if (PageIsNew(page) || PageIsEmpty(page))
    {
        UnlockReleaseBuffer(buf);
        *pending_list_limit  = BISCUIT_DEFAULT_PENDING_LIST_LIMIT;
        *total_pending_bytes = 0;
        *total_drains        = 0;
        return false;
    }

    meta = (BiscuitMetaPageData *) PageGetSpecialPointer(page);

    if (meta->magic != BISCUIT_MAGIC ||
        meta->version != BISCUIT_VERSION ||
        meta->page_format_version != BISCUIT_PAGE_FORMAT_VERSION)
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
 * biscuit_pending_mutate_structure
 *
 * Durable half of one bitmap mutation for structure (col, is_lower,
 * kind, ch, position): appends one BiscuitPendLogRecord for
 * (rec_idx, op) into the index-wide shared log, and opportunistically
 * drains the whole log if it has grown past BISCUIT_PENDLOG_DRAIN_PAGES.
 * Does NOT touch the directory at all: the record is self-describing, so
 * a structure need not even have a directory entry yet. Entries are
 * created lazily at drain time (biscuit_pendlog_drain_all()).
 *
 * col uses the same addressing biscuit_dir_slot_for_col() expects
 * elsewhere: -1 for the legacy single-column layout, 0-based column
 * index for multi-column.
 */
static void
biscuit_pending_mutate_structure(Relation index,
                                  int32 col, bool is_lower, uint8 kind,
                                  int32 ch, int32 position,
                                  uint32 rec_idx, uint8 op,
                                  uint32 pending_list_limit)
{
    uint64 pendlog_bytes;

    /*
     * One self-describing append to the index-wide shared log
     * (biscuit_pendlog.c). No biscuit_dir_find(), no biscuit_dir_insert(),
     * no biscuit_dir_update() -- the record carries its own structure
     * identity, so this path never touches the directory at all. Directory
     * entries are created lazily at drain time for whichever structures
     * actually appear in the log.
     *
     * What this replaced: a per-structure pending chain, so a row touching
     * K structures dirtied ~K distinct pages, and PostgreSQL charges a
     * full-page image per distinct page per checkpoint interval. Now all K
     * records land on the same log tail page, so the FPI bill stops
     * scaling with K.
     */
    pendlog_bytes = biscuit_pendlog_append(index, col, is_lower, kind,
                                            ch, position, rec_idx, op);

    /*
     * Opportunistic drain trigger -- tuned for OLAP, kept sane for OLTP.
     *
     * A drain re-serializes the compacted blob of EVERY structure the log
     * touched, so its cost is proportional to index size, not to how much
     * is pending. Draining every pending_list_limit bytes therefore makes
     * a bulk load quadratic: (N/limit) drains x O(index size) each. That
     * is the same shape as the O(n)-per-commit bug the row-identity work
     * removed, and reintroducing it here through the back door would be a
     * poor trade.
     *
     * So the threshold is deliberately generous:
     *
     *   - OLAP (the priority): bulk loads and read-mostly workloads. A big
     *     log costs almost nothing here. Reads pay a single snapshot hash
     *     build per statement (biscuit_pendlog_snapshot()), amortized over
     *     a scan that touches many structures, and VACUUM does the real
     *     draining. Rare, large drains are exactly right.
     *
     *   - OLTP (kept reasonable, not optimized): the ceiling bounds how
     *     much a cold reader must replay before its first query, and how
     *     much memory one snapshot costs. BISCUIT_PENDLOG_DRAIN_PAGES caps
     *     it at a few MB, so worst-case snapshot build stays in the
     *     milliseconds rather than growing without limit until someone
     *     runs VACUUM.
     *
     * pending_list_limit is intentionally NOT consulted: it is a
     * per-structure figure (64KB-ish) from the old design, and applying a
     * per-structure number to a whole-index log is what made the first
     * version of this drain fire constantly.
     */
    if (pendlog_bytes > (uint64) BISCUIT_PENDLOG_DRAIN_PAGES * BLCKSZ)
        biscuit_pendlog_drain_all(index, false);   /* opportunistic: skip if
                                                    * another backend is
                                                    * already draining */

    (void) pending_list_limit;   /* retained in the signature for the
                                   * statement-cached-read contract in
                                   * biscuit_index.h; no longer the trigger */
}

/*
 * Remove a record from every character and length bitmap.
 * Handles both single-column (legacy) and multi-column layouts.
 *
 * Mutates the in-memory bitmaps exactly as before (biscuit_roaring_remove,
 * unchanged) and, per the mutation contract, also durably records each
 * removal via biscuit_pending_mutate_structure() against that structure's
 * directory entry. `index`/`pending_list_limit` are always required now --
 * every call site is steady-state CRUD (biscuit_insert's UPDATE-as-
 * delete-then-insert path, biscuit_bulkdelete's tombstone purge), never
 * the one-time build path, so there is no NULL/build-mode case to gate
 * here (contrast biscuit_index_single_record/biscuit_index_column_record
 * below, which are shared between build and insert).
 */
void
biscuit_remove_from_all_indices(Relation index, BiscuitIndex *idx,
                                 uint32_t rec_idx, uint32 pending_list_limit)
{
    int ch, j, col;

    if (!idx)
        return;

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
                    biscuit_pending_mutate_structure(index, col, false, BISCUIT_DIR_KIND_POS,
                                                      ch, cidx->pos_idx[ch].entries[j].pos,
                                                      rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                      pending_list_limit);
                }
                for (j = 0; j < cidx->neg_idx[ch].count; j++)
                {
                    biscuit_roaring_remove(cidx->neg_idx[ch].entries[j].bitmap, rec_idx);
                    biscuit_pending_mutate_structure(index, col, false, BISCUIT_DIR_KIND_NEG,
                                                      ch, cidx->neg_idx[ch].entries[j].pos,
                                                      rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                      pending_list_limit);
                }
                if (cidx->char_cache[ch])
                {
                    biscuit_roaring_remove(cidx->char_cache[ch], rec_idx);
                    biscuit_pending_mutate_structure(index, col, false, BISCUIT_DIR_KIND_CACHE,
                                                      ch, -1, rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                      pending_list_limit);
                }

                /* case-insensitive */
                for (j = 0; j < cidx->pos_idx_lower[ch].count; j++)
                {
                    biscuit_roaring_remove(cidx->pos_idx_lower[ch].entries[j].bitmap, rec_idx);
                    biscuit_pending_mutate_structure(index, col, true, BISCUIT_DIR_KIND_POS,
                                                      ch, cidx->pos_idx_lower[ch].entries[j].pos,
                                                      rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                      pending_list_limit);
                }
                for (j = 0; j < cidx->neg_idx_lower[ch].count; j++)
                {
                    biscuit_roaring_remove(cidx->neg_idx_lower[ch].entries[j].bitmap, rec_idx);
                    biscuit_pending_mutate_structure(index, col, true, BISCUIT_DIR_KIND_NEG,
                                                      ch, cidx->neg_idx_lower[ch].entries[j].pos,
                                                      rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                      pending_list_limit);
                }
                if (cidx->char_cache_lower[ch])
                {
                    biscuit_roaring_remove(cidx->char_cache_lower[ch], rec_idx);
                    biscuit_pending_mutate_structure(index, col, true, BISCUIT_DIR_KIND_CACHE,
                                                      ch, -1, rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                      pending_list_limit);
                }
            }

            if (cidx->length_bitmaps)
                for (j = 0; j < cidx->max_length; j++)
                    if (cidx->length_bitmaps[j])
                    {
                        biscuit_roaring_remove(cidx->length_bitmaps[j], rec_idx);
                        biscuit_pending_mutate_structure(index, col, false, BISCUIT_DIR_KIND_LEN,
                                                          -1, j, rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                          pending_list_limit);
                    }

            if (cidx->length_ge_bitmaps)
                for (j = 0; j < cidx->max_length; j++)
                    if (cidx->length_ge_bitmaps[j])
                    {
                        biscuit_roaring_remove(cidx->length_ge_bitmaps[j], rec_idx);
                        biscuit_pending_mutate_structure(index, col, false, BISCUIT_DIR_KIND_LEN_GE,
                                                          -1, j, rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                          pending_list_limit);
                    }

            if (cidx->length_bitmaps_lower)
                for (j = 0; j < cidx->max_length_lower; j++)
                    if (cidx->length_bitmaps_lower[j])
                    {
                        biscuit_roaring_remove(cidx->length_bitmaps_lower[j], rec_idx);
                        biscuit_pending_mutate_structure(index, col, true, BISCUIT_DIR_KIND_LEN,
                                                          -1, j, rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                          pending_list_limit);
                    }

            if (cidx->length_ge_bitmaps_lower)
                for (j = 0; j < cidx->max_length_lower; j++)
                    if (cidx->length_ge_bitmaps_lower[j])
                    {
                        biscuit_roaring_remove(cidx->length_ge_bitmaps_lower[j], rec_idx);
                        biscuit_pending_mutate_structure(index, col, true, BISCUIT_DIR_KIND_LEN_GE,
                                                          -1, j, rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                          pending_list_limit);
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
            biscuit_pending_mutate_structure(index, -1, false, BISCUIT_DIR_KIND_POS,
                                              ch, idx->pos_idx_legacy[ch].entries[j].pos,
                                              rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                              pending_list_limit);
        }
        for (j = 0; j < idx->neg_idx_legacy[ch].count; j++)
        {
            biscuit_roaring_remove(idx->neg_idx_legacy[ch].entries[j].bitmap, rec_idx);
            biscuit_pending_mutate_structure(index, -1, false, BISCUIT_DIR_KIND_NEG,
                                              ch, idx->neg_idx_legacy[ch].entries[j].pos,
                                              rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                              pending_list_limit);
        }
        if (idx->char_cache_legacy[ch])
        {
            biscuit_roaring_remove(idx->char_cache_legacy[ch], rec_idx);
            biscuit_pending_mutate_structure(index, -1, false, BISCUIT_DIR_KIND_CACHE,
                                              ch, -1, rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                              pending_list_limit);
        }

        /* case-insensitive */
        for (j = 0; j < idx->pos_idx_lower[ch].count; j++)
        {
            biscuit_roaring_remove(idx->pos_idx_lower[ch].entries[j].bitmap, rec_idx);
            biscuit_pending_mutate_structure(index, -1, true, BISCUIT_DIR_KIND_POS,
                                              ch, idx->pos_idx_lower[ch].entries[j].pos,
                                              rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                              pending_list_limit);
        }
        for (j = 0; j < idx->neg_idx_lower[ch].count; j++)
        {
            biscuit_roaring_remove(idx->neg_idx_lower[ch].entries[j].bitmap, rec_idx);
            biscuit_pending_mutate_structure(index, -1, true, BISCUIT_DIR_KIND_NEG,
                                              ch, idx->neg_idx_lower[ch].entries[j].pos,
                                              rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                              pending_list_limit);
        }
        if (idx->char_cache_lower[ch])
        {
            biscuit_roaring_remove(idx->char_cache_lower[ch], rec_idx);
            biscuit_pending_mutate_structure(index, -1, true, BISCUIT_DIR_KIND_CACHE,
                                              ch, -1, rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                              pending_list_limit);
        }
    }

    if (idx->max_length_legacy > 0)
    {
        for (j = 0; j < idx->max_length_legacy; j++)
        {
            if (idx->length_bitmaps_legacy && idx->length_bitmaps_legacy[j])
            {
                biscuit_roaring_remove(idx->length_bitmaps_legacy[j], rec_idx);
                biscuit_pending_mutate_structure(index, -1, false, BISCUIT_DIR_KIND_LEN,
                                                  -1, j, rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                  pending_list_limit);
            }
            if (idx->length_ge_bitmaps_legacy && idx->length_ge_bitmaps_legacy[j])
            {
                biscuit_roaring_remove(idx->length_ge_bitmaps_legacy[j], rec_idx);
                biscuit_pending_mutate_structure(index, -1, false, BISCUIT_DIR_KIND_LEN_GE,
                                                  -1, j, rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                  pending_list_limit);
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
                biscuit_pending_mutate_structure(index, -1, true, BISCUIT_DIR_KIND_LEN,
                                                  -1, j, rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                  pending_list_limit);
            }
            if (idx->length_ge_bitmaps_lower && idx->length_ge_bitmaps_lower[j])
            {
                biscuit_roaring_remove(idx->length_ge_bitmaps_lower[j], rec_idx);
                biscuit_pending_mutate_structure(index, -1, true, BISCUIT_DIR_KIND_LEN_GE,
                                                  -1, j, rec_idx, BISCUIT_PENDING_OP_REMOVE,
                                                  pending_list_limit);
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

/*
 * Helper: add a single text record to the single-column (legacy) index.
 * Called both from biscuit_build()/biscuit_load_index() (one-time bulk
 * build/load) and from biscuit_insert() (steady-state CRUD).
 *
 * str / byte_len  : original (UTF-8) string
 * rec_idx         : slot in the index arrays to write into
 *
 * index / pending_list_limit: per the mutation contract, build/load
 * pass index == NULL to mean "populate the in-memory bitmaps only, no
 * durable pending-list append" -- biscuit_build() persists everything
 * in one bulk pass at the end instead (see biscuit_build()'s rewrite).
 * biscuit_insert() passes its Relation and the statement's cached
 * pending_list_limit (biscuit_read_pending_list_limit()), so every
 * biscuit_roaring_add() below is paired with a durable
 * biscuit_pending_mutate_structure() append.
 */
static void
biscuit_index_single_record(Relation      index,
                             BiscuitIndex *idx,
                             const char   *str,
                             int           byte_len,
                             int           rec_idx,
                             uint32        pending_list_limit)
{
    int byte_pos  = 0;
    int char_pos  = 0;
    int char_count = biscuit_utf8_char_count(str, byte_len);
    uint8 mode = idx->legacy_case_mode;

    /* ---- Case-sensitive character indexing (LIKE-gated) ---- */
    byte_pos = char_pos = 0;
    while ((mode & BISCUIT_MODE_LIKE) && byte_pos < byte_len)
    {
        unsigned char first_byte = (unsigned char) str[byte_pos];
        int           char_len   = biscuit_utf8_char_length(first_byte);
        int           b;

        if (byte_pos + char_len > byte_len)
            char_len = byte_len - byte_pos;

        for (b = 0; b < char_len; b++)
        {
            unsigned char uch = (unsigned char) str[byte_pos + b];
            RoaringBitmap *bm;
            int remaining_chars;
            int neg_offset;

            /* positive position */
            bm = biscuit_get_pos_bitmap(NULL /* raw live pointer, write path -- see biscuit_reconcile_pending() */, idx, uch, char_pos);
            if (!bm) {
                bm = biscuit_roaring_create();
                biscuit_set_pos_bitmap(idx, uch, char_pos, bm);
            }
            biscuit_roaring_add(bm, rec_idx);
            if (index != NULL)
                biscuit_pending_mutate_structure(index, -1, false, BISCUIT_DIR_KIND_POS,
                                                  uch, char_pos, rec_idx,
                                                  BISCUIT_PENDING_OP_ADD, pending_list_limit);

            /* negative position */
            remaining_chars = biscuit_utf8_char_count(str + byte_pos, byte_len - byte_pos);
            neg_offset = -remaining_chars;
            bm = biscuit_get_neg_bitmap(NULL /* raw live pointer, write path -- see biscuit_reconcile_pending() */, idx, uch, neg_offset);
            if (!bm) {
                bm = biscuit_roaring_create();
                biscuit_set_neg_bitmap(idx, uch, neg_offset, bm);
            }
            biscuit_roaring_add(bm, rec_idx);
            if (index != NULL)
                biscuit_pending_mutate_structure(index, -1, false, BISCUIT_DIR_KIND_NEG,
                                                  uch, neg_offset, rec_idx,
                                                  BISCUIT_PENDING_OP_ADD, pending_list_limit);

            /* character cache */
            if (!idx->char_cache_legacy[uch])
                idx->char_cache_legacy[uch] = biscuit_roaring_create();
            biscuit_roaring_add(idx->char_cache_legacy[uch], rec_idx);
            if (index != NULL)
                biscuit_pending_mutate_structure(index, -1, false, BISCUIT_DIR_KIND_CACHE,
                                                  uch, -1, rec_idx,
                                                  BISCUIT_PENDING_OP_ADD, pending_list_limit);
        }

        byte_pos += char_len;
        char_pos++;
    }

    /* ---- Case-insensitive character indexing (ILIKE-gated) ---- */
    if (!(mode & BISCUIT_MODE_ILIKE))
    {
        /*
         * This column's opclass (biscuit_like_ops) never needs the
         * case-insensitive structures -- leave the lowercased cache slot
         * NULL and skip building any "_lower" bitmaps for this record.
         */
        idx->data_cache_lower[rec_idx] = NULL;
    }
    else
    {
        char *str_lower      = biscuit_str_tolower(str, byte_len);
        int   lower_byte_len = strlen(str_lower);
        int   lower_char_count = biscuit_utf8_char_count(str_lower, lower_byte_len);
        (void) lower_char_count;  /* no longer used to bump idx->max_length_lower here; see note below */

        idx->data_cache_lower[rec_idx] = str_lower;

        /*
         * NOTE: do NOT bump idx->max_length_lower here.  This field is not
         * a "largest string seen so far" scratch counter — it is the live
         * allocated capacity of idx->length_bitmaps_lower/length_ge_bitmaps_lower.
         * biscuit_build() recomputes it correctly from scratch after this
         * function returns (see the dedicated length-bitmap pass), and
         * biscuit_insert()'s growth block is the sole owner of keeping it
         * in lockstep with the actual array size. Mutating it here made
         * biscuit_insert() read an already-bumped value as "old capacity",
         * leaving a gap of uninitialized RoaringBitmap* entries and
         * crashing inside libroaring on the next insert of a longer string.
         */

        byte_pos = char_pos = 0;
        while (byte_pos < lower_byte_len)
        {
            unsigned char first_byte = (unsigned char) str_lower[byte_pos];
            int           char_len   = biscuit_utf8_char_length(first_byte);
            int           b;

            if (byte_pos + char_len > lower_byte_len)
                char_len = lower_byte_len - byte_pos;

            for (b = 0; b < char_len; b++)
            {
                unsigned char uch = (unsigned char) str_lower[byte_pos + b];
                RoaringBitmap *bm;
                int remaining_chars;
                int neg_offset;

                bm = biscuit_get_pos_bitmap_lower(NULL /* raw live pointer, write path -- see biscuit_reconcile_pending() */, idx, uch, char_pos);
                if (!bm) {
                    bm = biscuit_roaring_create();
                    biscuit_set_pos_bitmap_lower(idx, uch, char_pos, bm);
                }
                biscuit_roaring_add(bm, rec_idx);
                if (index != NULL)
                    biscuit_pending_mutate_structure(index, -1, true, BISCUIT_DIR_KIND_POS,
                                                      uch, char_pos, rec_idx,
                                                      BISCUIT_PENDING_OP_ADD, pending_list_limit);

                remaining_chars = biscuit_utf8_char_count(str_lower + byte_pos, lower_byte_len - byte_pos);
                neg_offset = -remaining_chars;
                bm = biscuit_get_neg_bitmap_lower(NULL /* raw live pointer, write path -- see biscuit_reconcile_pending() */, idx, uch, neg_offset);
                if (!bm) {
                    bm = biscuit_roaring_create();
                    biscuit_set_neg_bitmap_lower(idx, uch, neg_offset, bm);
                }
                biscuit_roaring_add(bm, rec_idx);
                if (index != NULL)
                    biscuit_pending_mutate_structure(index, -1, true, BISCUIT_DIR_KIND_NEG,
                                                      uch, neg_offset, rec_idx,
                                                      BISCUIT_PENDING_OP_ADD, pending_list_limit);

                if (!idx->char_cache_lower[uch])
                    idx->char_cache_lower[uch] = biscuit_roaring_create();
                biscuit_roaring_add(idx->char_cache_lower[uch], rec_idx);
                if (index != NULL)
                    biscuit_pending_mutate_structure(index, -1, true, BISCUIT_DIR_KIND_CACHE,
                                                      uch, -1, rec_idx,
                                                      BISCUIT_PENDING_OP_ADD, pending_list_limit);
            }

            byte_pos += char_len;
            char_pos++;
        }
    }

    /* Track max case-sensitive character length */
    if (char_count > idx->max_len)
        idx->max_len = char_count;
}

/*
 * biscuit_index_column_record
 * ----------------------------
 * Populate all character-level and case-insensitive bitmaps for a single
 * string value into the ColumnIndex for column `col`.
 *
 * This is the multi-column analogue of biscuit_index_single_record().
 * It uses the biscuit_set_col_*_bitmap helpers (static in biscuit_pattern.c
 * but inlined into this TU via the included header) which operate directly
 * on a ColumnIndex pointer rather than routing through the top-level
 * BiscuitIndex legacy fields.
 *
 * Parameters
 *   index    – Relation, or NULL for build/load's bulk in-memory-only
 *              mode (see biscuit_index_single_record()'s doc comment for
 *              the full index==NULL/pending_list_limit contract; identical
 *              here)
 *   idx      – the owning BiscuitIndex (needed for tolower utility)
 *   col      – column number (0-based) selecting column_indices[col]
 *   str      – original UTF-8 string (NOT NUL-terminated beyond byte_len)
 *   byte_len – byte length of str
 *   rec_idx  – record slot being indexed
 *   pending_list_limit – statement-cached BiscuitMetaPageData.pending_list_limit;
 *              ignored when index == NULL
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
    ColumnIndex   *cidx       = &idx->column_indices[col];
    int            byte_pos   = 0;
    int            char_pos   = 0;
    int            char_count = biscuit_utf8_char_count(str, byte_len);
    uint8          mode       = idx->column_case_mode ? idx->column_case_mode[col] : BISCUIT_MODE_BOTH;
    (void) char_count;  /* no longer used to bump cidx->max_length here; see note below */

    /* ----------------------------------------------------------------
     * Case-sensitive pass (LIKE-gated)
     * ---------------------------------------------------------------- */
    byte_pos = char_pos = 0;
    while ((mode & BISCUIT_MODE_LIKE) && byte_pos < byte_len)
    {
        unsigned char first_byte = (unsigned char) str[byte_pos];
        int           char_len   = biscuit_utf8_char_length(first_byte);
        int           b;

        if (byte_pos + char_len > byte_len)
            char_len = byte_len - byte_pos;

        for (b = 0; b < char_len; b++)
        {
            unsigned char  uch = (unsigned char) str[byte_pos + b];
            RoaringBitmap *bm;
            int            remaining_chars;
            int            neg_offset;

            /* positive-position bitmap */
            bm = biscuit_get_col_pos_bitmap(NULL /* raw live pointer, write path */, cidx, col, uch, char_pos);
            if (!bm)
            {
                bm = biscuit_roaring_create();
                biscuit_set_col_pos_bitmap(cidx, uch, char_pos, bm);
            }
            biscuit_roaring_add(bm, rec_idx);
            if (index != NULL)
                biscuit_pending_mutate_structure(index, col, false, BISCUIT_DIR_KIND_POS,
                                                  uch, char_pos, rec_idx,
                                                  BISCUIT_PENDING_OP_ADD, pending_list_limit);

            /* negative-position bitmap */
            remaining_chars = biscuit_utf8_char_count(str + byte_pos, byte_len - byte_pos);
            neg_offset      = -remaining_chars;
            bm = biscuit_get_col_neg_bitmap(NULL /* raw live pointer, write path */, cidx, col, uch, neg_offset);
            if (!bm)
            {
                bm = biscuit_roaring_create();
                biscuit_set_col_neg_bitmap(cidx, uch, neg_offset, bm);
            }
            biscuit_roaring_add(bm, rec_idx);
            if (index != NULL)
                biscuit_pending_mutate_structure(index, col, false, BISCUIT_DIR_KIND_NEG,
                                                  uch, neg_offset, rec_idx,
                                                  BISCUIT_PENDING_OP_ADD, pending_list_limit);

            /* character-presence cache */
            if (!cidx->char_cache[uch])
                cidx->char_cache[uch] = biscuit_roaring_create();
            biscuit_roaring_add(cidx->char_cache[uch], rec_idx);
            if (index != NULL)
                biscuit_pending_mutate_structure(index, col, false, BISCUIT_DIR_KIND_CACHE,
                                                  uch, -1, rec_idx,
                                                  BISCUIT_PENDING_OP_ADD, pending_list_limit);
        }

        byte_pos += char_len;
        char_pos++;
    }

    /*
     * NOTE: do NOT bump cidx->max_length here.  See the identical note in
     * biscuit_index_single_record() above: this field tracks the live
     * allocated capacity of cidx->length_bitmaps/length_ge_bitmaps, not a
     * scratch "longest string seen" counter. biscuit_build() recomputes it
     * from scratch after this function returns; biscuit_insert()'s growth
     * block is the sole owner of keeping it in lockstep with the actual
     * array size.
     */

    /* ----------------------------------------------------------------
     * Case-insensitive pass (ILIKE-gated)
     * ---------------------------------------------------------------- */
    if (mode & BISCUIT_MODE_ILIKE)
    {
        char *str_lower      = biscuit_str_tolower(str, byte_len);
        int   lower_byte_len = (int) strlen(str_lower);
        int   lower_char_count;

        lower_char_count = biscuit_utf8_char_count(str_lower, lower_byte_len);
        (void) lower_char_count;  /* no longer used to bump cidx->max_length_lower here; see note below */

        /*
         * NOTE: do NOT bump cidx->max_length_lower here, for the same
         * reason as cidx->max_length above.
         */

        byte_pos = char_pos = 0;
        while (byte_pos < lower_byte_len)
        {
            unsigned char first_byte = (unsigned char) str_lower[byte_pos];
            int           char_len   = biscuit_utf8_char_length(first_byte);
            int           b;

            if (byte_pos + char_len > lower_byte_len)
                char_len = lower_byte_len - byte_pos;

            for (b = 0; b < char_len; b++)
            {
                unsigned char  uch = (unsigned char) str_lower[byte_pos + b];
                RoaringBitmap *bm;
                int            remaining_chars;
                int            neg_offset;

                /* positive-position (lower) */
                bm = biscuit_get_col_pos_bitmap_lower(NULL /* raw live pointer, write path */, cidx, col, uch, char_pos);
                if (!bm)
                {
                    bm = biscuit_roaring_create();
                    biscuit_set_col_pos_bitmap_lower(cidx, uch, char_pos, bm);
                }
                biscuit_roaring_add(bm, rec_idx);
                if (index != NULL)
                    biscuit_pending_mutate_structure(index, col, true, BISCUIT_DIR_KIND_POS,
                                                      uch, char_pos, rec_idx,
                                                      BISCUIT_PENDING_OP_ADD, pending_list_limit);

                /* negative-position (lower) */
                remaining_chars = biscuit_utf8_char_count(str_lower + byte_pos,
                                                           lower_byte_len - byte_pos);
                neg_offset      = -remaining_chars;
                bm = biscuit_get_col_neg_bitmap_lower(NULL /* raw live pointer, write path */, cidx, col, uch, neg_offset);
                if (!bm)
                {
                    bm = biscuit_roaring_create();
                    biscuit_set_col_neg_bitmap_lower(cidx, uch, neg_offset, bm);
                }
                biscuit_roaring_add(bm, rec_idx);
                if (index != NULL)
                    biscuit_pending_mutate_structure(index, col, true, BISCUIT_DIR_KIND_NEG,
                                                      uch, neg_offset, rec_idx,
                                                      BISCUIT_PENDING_OP_ADD, pending_list_limit);

                /* character-presence cache (lower) */
                if (!cidx->char_cache_lower[uch])
                    cidx->char_cache_lower[uch] = biscuit_roaring_create();
                biscuit_roaring_add(cidx->char_cache_lower[uch], rec_idx);
                if (index != NULL)
                    biscuit_pending_mutate_structure(index, col, true, BISCUIT_DIR_KIND_CACHE,
                                                      uch, -1, rec_idx,
                                                      BISCUIT_PENDING_OP_ADD, pending_list_limit);
            }

            byte_pos += char_len;
            char_pos++;
        }

        pfree(str_lower);
    }
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
        int    unused_records, unused_columns, unused_max_len;
        uint64 disk_gen = 0;

        biscuit_read_metadata_from_disk(index, &unused_records,
                                        &unused_columns, &unused_max_len,
                                        &disk_gen);
        idx->gen                  = disk_gen;
        idx->gen_at_last_snapshot = disk_gen;
    }

    biscuit_register_callback();
    biscuit_cache_insert(RelationGetRelid(index), idx);
    return idx;
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
    idx = biscuit_cache_lookup(RelationGetRelid(index));
    if (!idx)
        idx = biscuit_load_index(index);

    /*
     * biscuit_load_index() (called above on a cache miss) always builds a
     * complete BiscuitIndex — heap scan, data caches, and every bitmap —
     * before returning it, so the length-bitmap arrays below are always
     * allocated and non-NULL by the time we get here.
     */
    oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

    /* Check for duplicate TID (UPDATE path) */
    for (int i = 0; i < idx->num_records; i++)
    {
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

            /* Grow length bitmaps if needed */
            {
                int cl = biscuit_utf8_char_count(str, byte_len);

                if (idx->legacy_case_mode & BISCUIT_MODE_LIKE)
                {
                if (cl >= idx->max_length_legacy)
                {
                    int old_ml = idx->max_length_legacy;
                    int new_ml = (cl + 1) * 2;

                    /*
                     * Belt-and-suspenders: these arrays are always allocated
                     * by biscuit_load_index()/biscuit_build() before we get
                     * here, but guard the repalloc/palloc0 choice anyway.
                     */
                    if (idx->length_bitmaps_legacy)
                        idx->length_bitmaps_legacy    = (RoaringBitmap **) repalloc(idx->length_bitmaps_legacy,    new_ml * sizeof(RoaringBitmap *));
                    else
                        idx->length_bitmaps_legacy    = (RoaringBitmap **) palloc0(new_ml * sizeof(RoaringBitmap *));

                    if (idx->length_ge_bitmaps_legacy)
                        idx->length_ge_bitmaps_legacy = (RoaringBitmap **) repalloc(idx->length_ge_bitmaps_legacy, new_ml * sizeof(RoaringBitmap *));
                    else
                        idx->length_ge_bitmaps_legacy = (RoaringBitmap **) palloc0(new_ml * sizeof(RoaringBitmap *));

                    for (int i = old_ml; i < new_ml; i++)
                    {
                        idx->length_bitmaps_legacy[i]    = NULL;
                        idx->length_ge_bitmaps_legacy[i] = biscuit_roaring_create();
                    }
                    idx->max_length_legacy = new_ml;
                }
                if (!idx->length_bitmaps_legacy[cl])
                    idx->length_bitmaps_legacy[cl] = biscuit_roaring_create();
                biscuit_roaring_add(idx->length_bitmaps_legacy[cl], slot);
                biscuit_pending_mutate_structure(index, -1, false, BISCUIT_DIR_KIND_LEN,
                                                  -1, cl, slot, BISCUIT_PENDING_OP_ADD,
                                                  pending_list_limit);
                for (int i = 0; i <= cl && i < idx->max_length_legacy; i++)
                {
                    if (!idx->length_ge_bitmaps_legacy[i])
                        idx->length_ge_bitmaps_legacy[i] = biscuit_roaring_create();
                    biscuit_roaring_add(idx->length_ge_bitmaps_legacy[i], slot);
                    biscuit_pending_mutate_structure(index, -1, false, BISCUIT_DIR_KIND_LEN_GE,
                                                      -1, i, slot, BISCUIT_PENDING_OP_ADD,
                                                      pending_list_limit);
                }
                }

                /*
                 * Lowercase length bitmaps (ILIKE-gated).
                 * data_cache_lower[slot] was populated by biscuit_index_single_record
                 * above -- which itself only fills it in when legacy_case_mode
                 * includes BISCUIT_MODE_ILIKE (NULL otherwise), so this guard
                 * already skips the block correctly for a LIKE-only column.
                 */
                if (idx->data_cache_lower[slot])
                {
                    int lbl = strlen(idx->data_cache_lower[slot]);
                    int lcl = biscuit_utf8_char_count(idx->data_cache_lower[slot], lbl);
                    if (lcl >= idx->max_length_lower)
                    {
                        int old_ml = idx->max_length_lower;
                        int new_ml = (lcl + 1) * 2;

                        if (idx->length_bitmaps_lower)
                            idx->length_bitmaps_lower    = (RoaringBitmap **) repalloc(idx->length_bitmaps_lower,    new_ml * sizeof(RoaringBitmap *));
                        else
                            idx->length_bitmaps_lower    = (RoaringBitmap **) palloc0(new_ml * sizeof(RoaringBitmap *));

                        if (idx->length_ge_bitmaps_lower)
                            idx->length_ge_bitmaps_lower = (RoaringBitmap **) repalloc(idx->length_ge_bitmaps_lower, new_ml * sizeof(RoaringBitmap *));
                        else
                            idx->length_ge_bitmaps_lower = (RoaringBitmap **) palloc0(new_ml * sizeof(RoaringBitmap *));

                        for (int i = old_ml; i < new_ml; i++)
                        {
                            idx->length_bitmaps_lower[i]    = NULL;
                            idx->length_ge_bitmaps_lower[i] = biscuit_roaring_create();
                        }
                        idx->max_length_lower = new_ml;
                    }
                    if (!idx->length_bitmaps_lower[lcl])
                        idx->length_bitmaps_lower[lcl] = biscuit_roaring_create();
                    biscuit_roaring_add(idx->length_bitmaps_lower[lcl], slot);
                    biscuit_pending_mutate_structure(index, -1, true, BISCUIT_DIR_KIND_LEN,
                                                      -1, lcl, slot, BISCUIT_PENDING_OP_ADD,
                                                      pending_list_limit);
                    for (int i = 0; i <= lcl && i < idx->max_length_lower; i++)
                    {
                        if (!idx->length_ge_bitmaps_lower[i])
                            idx->length_ge_bitmaps_lower[i] = biscuit_roaring_create();
                        biscuit_roaring_add(idx->length_ge_bitmaps_lower[i], slot);
                        biscuit_pending_mutate_structure(index, -1, true, BISCUIT_DIR_KIND_LEN_GE,
                                                          -1, i, slot, BISCUIT_PENDING_OP_ADD,
                                                          pending_list_limit);
                    }
                }
            } /* end cl block */
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
                {
                    ColumnIndex *cidx = &idx->column_indices[col];
                    int          cl   = biscuit_utf8_char_count(str, out_len);
                    uint8        col_mode = idx->column_case_mode ? idx->column_case_mode[col] : BISCUIT_MODE_BOTH;

                    if (col_mode & BISCUIT_MODE_LIKE)
                    {
                    if (cl >= cidx->max_length)
                    {
                        int old_ml = cidx->max_length;
                        int new_ml = (cl + 1) * 2;

                        if (cidx->length_bitmaps)
                            cidx->length_bitmaps    = (RoaringBitmap **) repalloc(cidx->length_bitmaps,    new_ml * sizeof(RoaringBitmap *));
                        else
                            cidx->length_bitmaps    = (RoaringBitmap **) palloc0(new_ml * sizeof(RoaringBitmap *));

                        if (cidx->length_ge_bitmaps)
                            cidx->length_ge_bitmaps = (RoaringBitmap **) repalloc(cidx->length_ge_bitmaps, new_ml * sizeof(RoaringBitmap *));
                        else
                            cidx->length_ge_bitmaps = (RoaringBitmap **) palloc0(new_ml * sizeof(RoaringBitmap *));

                        for (int i = old_ml; i < new_ml; i++)
                        {
                            cidx->length_bitmaps[i]    = NULL;
                            cidx->length_ge_bitmaps[i] = biscuit_roaring_create();
                        }
                        cidx->max_length = new_ml;
                    }
                    if (!cidx->length_bitmaps[cl])
                        cidx->length_bitmaps[cl] = biscuit_roaring_create();
                    biscuit_roaring_add(cidx->length_bitmaps[cl], slot);
                    biscuit_pending_mutate_structure(index, col, false, BISCUIT_DIR_KIND_LEN,
                                                      -1, cl, slot, BISCUIT_PENDING_OP_ADD,
                                                      pending_list_limit);
                    for (int i = 0; i <= cl && i < cidx->max_length; i++)
                    {
                        if (!cidx->length_ge_bitmaps[i])
                            cidx->length_ge_bitmaps[i] = biscuit_roaring_create();
                        biscuit_roaring_add(cidx->length_ge_bitmaps[i], slot);
                        biscuit_pending_mutate_structure(index, col, false, BISCUIT_DIR_KIND_LEN_GE,
                                                          -1, i, slot, BISCUIT_PENDING_OP_ADD,
                                                          pending_list_limit);
                    }
                    }

                    /* Lowercase length bitmaps (ILIKE-gated via column_data_cache_lower being NULL when disabled) */
                    if (idx->column_data_cache_lower)
                    {
                        const char *lstr = idx->column_data_cache_lower[col][slot];
                        if (lstr)
                        {
                            int lbl = strlen(lstr);
                            int lcl = biscuit_utf8_char_count(lstr, lbl);

                            if (lcl >= cidx->max_length_lower)
                            {
                                int old_ml = cidx->max_length_lower;
                                int new_ml = (lcl + 1) * 2;

                                if (cidx->length_bitmaps_lower)
                                    cidx->length_bitmaps_lower    = (RoaringBitmap **) repalloc(cidx->length_bitmaps_lower,    new_ml * sizeof(RoaringBitmap *));
                                else
                                    cidx->length_bitmaps_lower    = (RoaringBitmap **) palloc0(new_ml * sizeof(RoaringBitmap *));

                                if (cidx->length_ge_bitmaps_lower)
                                    cidx->length_ge_bitmaps_lower = (RoaringBitmap **) repalloc(cidx->length_ge_bitmaps_lower, new_ml * sizeof(RoaringBitmap *));
                                else
                                    cidx->length_ge_bitmaps_lower = (RoaringBitmap **) palloc0(new_ml * sizeof(RoaringBitmap *));

                                for (int i = old_ml; i < new_ml; i++)
                                {
                                    cidx->length_bitmaps_lower[i]    = NULL;
                                    cidx->length_ge_bitmaps_lower[i] = biscuit_roaring_create();
                                }
                                cidx->max_length_lower = new_ml;
                            }
                            if (!cidx->length_bitmaps_lower[lcl])
                                cidx->length_bitmaps_lower[lcl] = biscuit_roaring_create();
                            biscuit_roaring_add(cidx->length_bitmaps_lower[lcl], slot);
                            biscuit_pending_mutate_structure(index, col, true, BISCUIT_DIR_KIND_LEN,
                                                              -1, lcl, slot, BISCUIT_PENDING_OP_ADD,
                                                              pending_list_limit);
                            for (int i = 0; i <= lcl && i < cidx->max_length_lower; i++)
                            {
                                if (!cidx->length_ge_bitmaps_lower[i])
                                    cidx->length_ge_bitmaps_lower[i] = biscuit_roaring_create();
                                biscuit_roaring_add(cidx->length_ge_bitmaps_lower[i], slot);
                                biscuit_pending_mutate_structure(index, col, true, BISCUIT_DIR_KIND_LEN_GE,
                                                                  -1, i, slot, BISCUIT_PENDING_OP_ADD,
                                                                  pending_list_limit);
                            }
                        }
                    }
                } /* end multi-column length-bitmap block */
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
     */
    idx->gen++;
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

    idx = biscuit_cache_lookup(RelationGetRelid(index));
    if (!idx) { idx = biscuit_load_index(index); }

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
     */
    idx->gen++;
    biscuit_write_metadata_to_disk(index, idx);

    /*
     * No eager full-blob resave here anymore -- see the matching comment
     * at the end of biscuit_insert(). Every removal above (both the
     * main delete_indices loop and the tombstone-purge loop) already
     * went through biscuit_remove_from_all_indices(), which durably
     * appends to each touched structure's pending list as it goes.
     */

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
    structures_drained = biscuit_pendlog_drain_all(index, true);   /* VACUUM must not skip */

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
