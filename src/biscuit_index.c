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

/*
 * biscuit_costestimate
 *
 * The previous version returned a constant (indexTotalCost = 0.01,
 * selectivity = 0.01) with the real computation commented out. That made
 * every Biscuit path look free and identical: three structurally different
 * queries all planned at cost=0.00..10.56, and the planner had no way to
 * avoid a scan measured at 378ms against a 7ms seqscan on the same table.
 *
 * The model below comes from measurement, not first principles. Biscuit's
 * matcher is positional: an anchored pattern tests one window, while an
 * unanchored infix must test every possible start offset, so the work is
 *
 *      windows x pattern_chars   bitmap intersections
 *
 * with windows = 1 when anchored and ~max_length when not, doubled for
 * ILIKE because the lowercase structure set is maintained and scanned in
 * parallel. Observed on a 5,508-row table (seqscan 1-7ms):
 *
 *      email  (short) infix        0.5 - 2 ms
 *      long_text      infix LIKE   35 - 42 ms
 *      long_text      infix ILIKE  188 ms
 *      long_text      infix (0 rows, common chars)  378 - 595 ms
 *
 * Note the worst cases RETURN NOTHING: a non-matching pattern has no early
 * exit and pays the full sweep, so cost must be driven by the scan shape,
 * never by estimated selectivity.
 *
 * On anchoring to other index types: an earlier plan here was to peg infix
 * costs above pg_trgm's. pg_trgm has no cost estimator -- it is an opclass;
 * the planner calls gincostestimate()/gistcostestimate(), whose GIN-specific
 * inputs come from ginGetStats() on a GIN metapage that a Biscuit relation
 * does not have. Pegging to a competitor would also be the wrong target:
 * the path Biscuit actually loses to on long columns is the SEQSCAN, and a
 * blanket infix penalty would discard the cases where Biscuit legitimately
 * wins (short-column infix beats both seqscan and trigram). Costing the
 * real work puts long-column infix above seqscan and GIN on its own merits
 * while leaving short-column and anchored scans cheap.
 */
 void
 biscuit_costestimate(PlannerInfo *root, IndexPath *path,
                    double loop_count,
                    Cost *indexStartupCost, Cost *indexTotalCost,
                    Selectivity *indexSelectivity, double *indexCorrelation,
                    double *indexPages)
{
    IndexOptInfo *indexinfo = path->indexinfo;
    Relation      index;
    BlockNumber   numPages     = 1;
    int           num_records  = 0;
    int           num_columns  = 0;
    int           max_len      = 0;
    uint64        gen          = 0;
    bool          have_meta    = false;

    double        spc_random_page_cost;
    double        spc_seq_page_cost;

    double        total_windows   = 0.0;   /* summed over all clauses      */
    double        total_bitmapops = 0.0;
    double        selectivity     = 1.0;
    int           nclauses        = 0;
    bool          has_infix_clause = false;

    ListCell     *lc;

    (void) loop_count;

    /* ---- No usable quals: keep this path off the table entirely. ---- */
    if (path->indexclauses == NIL)
    {
        /*
         * Biscuit with zero scan keys returns 0 rows, so this is not merely
         * expensive, it is wrong. Price it out of consideration.
         */
        *indexStartupCost = 1.0e10;
        *indexTotalCost   = 1.0e10;
        *indexSelectivity = 1.0;
        *indexCorrelation = 0.0;
        if (indexPages) *indexPages = 1;
        return;
    }

    /* ---- Index-level facts. ---- */
    if (OidIsValid(indexinfo->indexoid))
    {
        index = index_open(indexinfo->indexoid, AccessShareLock);
        numPages = RelationGetNumberOfBlocks(index);
        if (numPages == 0)
            numPages = 1;
        have_meta = biscuit_read_metadata_from_disk(index, &num_records,
                                                     &num_columns, &max_len,
                                                     &gen);
        index_close(index, AccessShareLock);
    }

    if (!have_meta || num_records <= 0)
        num_records = (int) Max(1.0, indexinfo->tuples);
    if (max_len <= 0)
        max_len = 32;   /* unknown column width: assume something modest
                          * rather than something alarming, so a missing
                          * metapage doesn't silently disable the index */

    get_tablespace_page_costs(indexinfo->reltablespace,
                               &spc_random_page_cost, &spc_seq_page_cost);

    /* ---- Per-clause scan shape. ---- */
    foreach(lc, path->indexclauses)
    {
        IndexClause  *iclause = (IndexClause *) lfirst(lc);
        RestrictInfo *rinfo   = iclause->rinfo;
        Expr         *clause  = rinfo ? rinfo->clause : NULL;
        bool          is_ilike       = false;
        bool          anchored_start = false;
        bool          anchored_end   = false;
        int           pattern_chars  = 0;
        double        windows;
        double        clause_sel;

        nclauses++;

        if (clause && IsA(clause, OpExpr))
        {
            OpExpr *op    = (OpExpr *) clause;
            char   *opname = get_opname(op->opno);
            Node   *rnode;

            /*
             * "~~" is LIKE, "~~*" is ILIKE. Matching on the operator name
             * rather than a strategy number keeps this readable and avoids
             * depending on the opclass's strategy numbering, which is
             * private to biscuit.sql and has changed before.
             */
            if (opname != NULL)
                is_ilike = (strcmp(opname, "~~*") == 0 ||
                            strcmp(opname, "!~~*") == 0);

            rnode = (list_length(op->args) >= 2) ? (Node *) lsecond(op->args) : NULL;
            if (rnode && IsA(rnode, Const) && !((Const *) rnode)->constisnull)
            {
                Const *c = (Const *) rnode;
                Oid    ctype = c->consttype;

                if (ctype == TEXTOID || ctype == VARCHAROID || ctype == BPCHAROID)
                {
                    char *pat = TextDatumGetCString(c->constvalue);
                    int   len = (int) strlen(pat);

                    anchored_start = (len > 0 && pat[0] != '%');
                    anchored_end   = (len > 0 && pat[len - 1] != '%');

                    /*
                     * Count matchable characters: wildcards drive the window
                     * count, they are not themselves intersected. Counting
                     * UTF-8 characters (not bytes) matters -- a Tamil or
                     * kana pattern is far shorter in characters than bytes,
                     * and the matcher works in characters.
                     */
                    pattern_chars = biscuit_utf8_char_count(pat, len);
                    {
                        int i;
                        for (i = 0; i < len; i++)
                            if (pat[i] == '%' || pat[i] == '_')
                                pattern_chars--;
                    }
                    if (pattern_chars < 0)
                        pattern_chars = 0;

                    pfree(pat);
                }
            }
        }

        if (pattern_chars == 0)
        {
            /*
             * Couldn't read the pattern (non-Const, e.g. a parameter, or an
             * unexpected type). Assume the bad case rather than the good
             * one: an unanchored scan of a short pattern is exactly the
             * shape that blows up, and guessing "cheap" here is how the
             * constant-cost version caused the problem this function exists
             * to fix.
             */
            pattern_chars  = 2;
            anchored_start = false;
            anchored_end   = false;
        }

        /*
         * Windows to test. Anchored at either end pins the offset, so one
         * window suffices (prefix uses the positional bitmaps, suffix the
         * negative-offset ones). Unanchored means every start offset.
         */
        if (anchored_start || anchored_end)
            windows = 1.0;
        else
            windows = Max(1.0, (double) max_len - (double) pattern_chars + 1.0);

        /*
         * A clause anchored at neither end is a true infix match: there is
         * no pinned offset for the matcher to exploit, so it has no
         * locality to fall back on. Track this so the page-cost term below
         * can price it like a near-full scan instead of "one structure per
         * window" -- this is the lever that steers infix patterns toward a
         * trigram (GIN) index while leaving prefix/suffix patterns on
         * biscuit.
         */
        if (!anchored_start && !anchored_end)
            has_infix_clause = true;

        total_windows   += windows;
        total_bitmapops += windows * (double) pattern_chars * (is_ilike ? 2.0 : 1.0);

        /*
         * Selectivity. Deliberately crude and deliberately NOT used to
         * discount the scan cost above: the measured worst cases returned
         * zero rows. Longer and anchored patterns are more selective.
         */
        clause_sel = pow(0.30, (double) Max(1, pattern_chars));
        if (!anchored_start && !anchored_end)
            clause_sel *= 3.0;              /* infix matches more places */
        clause_sel = Min(1.0, Max(1.0e-6, clause_sel));
        selectivity *= clause_sel;
    }

    selectivity = Min(1.0, Max(1.0e-6, selectivity));

    /*
     * Cost the bitmap work. Each intersection touches a bitmap holding
     * num_records entries; Roaring processes these in word-sized chunks, so
     * scale per-op cost by the bitmap's word count rather than its bit
     * count. The 64.0 divisor is bits-per-word, not a fudge factor.
     */
    {
        double bitmap_words = Max(1.0, (double) num_records / 64.0);
        double cpu_bitmap   = total_bitmapops * bitmap_words * cpu_operator_cost;

        /*
         * Pages: an anchored scan (prefix or suffix) pins the match to a
         * known offset, so it only touches a handful of structures and
         * biscuit should win there. A true infix scan has no anchor at
         * all -- every window position can land on a different part of
         * the bitmap structures -- so there is no locality to exploit and
         * it is priced as touching essentially the whole index. This is
         * what steers the planner toward a trigram (GIN) index for infix
         * patterns specifically, while leaving anchored patterns on
         * biscuit. Cap at the index size -- it cannot read more than it
         * has.
         */
        double pages_touched = has_infix_clause
            ? (double) numPages
            : Min((double) numPages, Max(1.0, total_windows));

        /*
         * Startup: Biscuit materializes a column's structures on first
         * touch. Measured at 35-70ms per index on the benchmark table, which
         * the planner cannot amortize because it does not know whether this
         * backend has already paid it. Charge a fraction of the index size
         * so large indexes are not treated as free to open.
         */
        *indexStartupCost = (double) numPages * 0.05 * spc_random_page_cost;

        *indexTotalCost = *indexStartupCost
                        + pages_touched * spc_random_page_cost
                        + cpu_bitmap
                        + (double) num_records * selectivity * cpu_index_tuple_cost;

        /* Qual evaluation, as genericcostestimate does. */
        *indexTotalCost += (double) nclauses * cpu_operator_cost;

        if (indexPages)
            *indexPages = pages_touched;
    }

    *indexSelectivity = selectivity;

    /*
     * Biscuit returns TIDs sorted by block (biscuit_sort_tids_by_block), so
     * heap access is sequential-ish. This is genuinely favourable and is the
     * one place the old constant model was accidentally right.
     */
    *indexCorrelation = 1.0;
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
