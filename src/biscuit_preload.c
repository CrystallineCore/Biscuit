/*
 * biscuit_preload.c
 * Background index preloader.
 *
 * Design
 * ------
 *  1. biscuit_load_skeleton()      – fast path in beginscan:
 *       Scans the heap once, stores TIDs + raw string data, leaves ALL
 *       bitmap fields NULL.  Returns in O(n) heap-scan time but with
 *       zero bitmap overhead (~1-2 ms for 1 M rows vs ~500 ms full load).
 *
 *  2. biscuit_preload_request()    – still in beginscan:
 *       Pushes the index OID into a tiny shmem ring-buffer and pings
 *       the worker latch.  Returns immediately.
 *
 *  3. BiscuitPreloadWorker         – background process:
 *       Wakes on latch signal, dequeues an OID, opens the relation,
 *       calls biscuit_complete_preload() which rebuilds all bitmaps
 *       from the already-populated data_cache, then inserts the warm
 *       index into the session cache and sets preload_state = DONE.
 *
 *  4. biscuit_fallback_scan()      – used by rescan while state < DONE:
 *       Plain strstr / strcasestr walk of data_cache — no bitmaps.
 *       Correct but slower; only used during the warm-up window.
 *
 * Concurrency / safety
 * --------------------
 *  • The skeleton and the fully-loaded index are both allocated in
 *    CacheMemoryContext of their respective backend.  No cross-process
 *    pointer sharing.
 *  • The shared-memory ring-buffer is protected by a LWLock
 *    (BISCUIT_PRELOAD_LWLOCK).
 *  • preload_state is a pg_atomic_uint32 inside the shmem control
 *    block (indexed by OID % 64), not inside BiscuitIndex.  The
 *    foreground backend polls it cheaply before each scan.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_cache.h"
#include "biscuit_index.h"
#include "biscuit_pattern.h"
#include "biscuit_utf8.h"
#include "biscuit_preload.h"

#include "executor/executor.h"   /* for FormIndexDatum, CreateExecutorState,
                                   * GetPerTupleExprContext, ResetExprContext —
                                   * needed to evaluate expression index columns
                                   * (e.g. (col::text), lower(col2)) correctly in
                                   * biscuit_load_skeleton(), same fix as
                                   * biscuit_build() in biscuit_index.c. */

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "access/xact.h"

/* PG 14 moved SignalHandlerForConfigReload here */
#if PG_VERSION_NUM >= 140000
#include "postmaster/interrupt.h"
#endif


#ifndef WAIT_EVENT_BGWORKER_MAIN
#define WAIT_EVENT_BGWORKER_MAIN 0
#endif
/* ================================================================
 * Shared memory
 * ================================================================ */

static BiscuitPreloadShmem *biscuit_preload_shmem = NULL;

/* LWLock protecting the ring buffer (acquired from LWLockNewTrancheId) */
static LWLockPadded        *biscuit_lwlock_base    = NULL;

#define BiscuitPreloadLock() (&biscuit_lwlock_base[0].lock)

/* ================================================================
 * Shmem size / request
 * ================================================================ */

Size
biscuit_preload_shmem_size(void)
{
    return MAXALIGN(sizeof(BiscuitPreloadShmem))
         + MAXALIGN(sizeof(LWLockPadded));
}

/* ================================================================
 * _PG_init helpers
 * ================================================================ */

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void
biscuit_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();
    RequestAddinShmemSpace(biscuit_preload_shmem_size());
    RequestNamedLWLockTranche("biscuit_preload", 1);
}

static void
biscuit_shmem_startup(void)
{
    bool found;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    biscuit_preload_shmem = (BiscuitPreloadShmem *)
        ShmemInitStruct("biscuit_preload",
                        sizeof(BiscuitPreloadShmem),
                        &found);

    if (!found)
    {
        int i;
        memset(biscuit_preload_shmem, 0, sizeof(BiscuitPreloadShmem));
        for (i = 0; i < 64; i++)
        {
            pg_atomic_init_u32(&biscuit_preload_shmem->slot_state[i],
                               BISCUIT_PRELOAD_NONE);
            pg_atomic_init_u32(&biscuit_preload_shmem->slot_oid[i],
                               InvalidOid);
        }
        biscuit_preload_shmem->worker_latch = NULL;
    }

    biscuit_lwlock_base =
        GetNamedLWLockTranche("biscuit_preload");

    LWLockRelease(AddinShmemInitLock);
}

/* ================================================================
 * biscuit_preload_init  – called from _PG_init
 * ================================================================ */

void
biscuit_preload_init(void)
{
    BackgroundWorker worker;

    /* Hook shared-memory allocation */
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook      = biscuit_shmem_request;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook      = biscuit_shmem_startup;

    /* Register the persistent background worker */
    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags        = BGWORKER_SHMEM_ACCESS |
                              BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time   = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 5;   /* restart after 5 s on crash */
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "biscuit");
    snprintf(worker.bgw_function_name, BGW_MAXLEN,
             "biscuit_preload_worker_main");
    snprintf(worker.bgw_name, BGW_MAXLEN, "biscuit preload worker");
    snprintf(worker.bgw_type, BGW_MAXLEN, "biscuit preload");
    worker.bgw_main_arg     = (Datum) 0;
    worker.bgw_notify_pid   = 0;

    RegisterBackgroundWorker(&worker);
}

/* ================================================================
 * biscuit_preload_request  – enqueue an OID for the worker
 * ================================================================ */

void
biscuit_preload_request(Oid indexoid)
{
    BiscuitPreloadShmem *sh = biscuit_preload_shmem;
    int oid_slot = (int)(indexoid % 64);

    if (!sh)
        return;   /* shmem not yet initialised (e.g. bootstrap) */

    LWLockAcquire(BiscuitPreloadLock(), LW_EXCLUSIVE);

    if (sh->queue_size >= 64)
    {
        /* Queue full — skip; worker will catch it on the next cycle */
        LWLockRelease(BiscuitPreloadLock());
        elog(DEBUG1, "Biscuit preload: queue full, skipping OID %u", indexoid);
        return;
    }

    /* Check for duplicate OIDs already in the queue */
    {
        int i;
        for (i = 0; i < sh->queue_size; i++)
        {
            int pos = (sh->queue_head + i) % 64;
            if (sh->queue[pos] == indexoid)
            {
                LWLockRelease(BiscuitPreloadLock());
                return;   /* already queued */
            }
        }
    }

    sh->queue[sh->queue_tail] = indexoid;
    sh->queue_tail            = (sh->queue_tail + 1) % 64;
    sh->queue_size++;

    /*
     * Key the state on (indexoid % 64), not on the ring-buffer slot.
     * This way biscuit_preload_state() can find the state even after the
     * worker has dequeued the OID (queue_head advanced past the slot).
     */
    pg_atomic_write_u32(&sh->slot_oid[oid_slot],   (uint32) indexoid);
    pg_atomic_write_u32(&sh->slot_state[oid_slot], BISCUIT_PRELOAD_SKELETON);

    LWLockRelease(BiscuitPreloadLock());

    /* Wake the worker */
    if (sh->worker_latch)
        SetLatch(sh->worker_latch);

    elog(DEBUG1, "Biscuit preload: queued index %u for background load", indexoid);
}

/* ================================================================
 * Preload state query helper  (called from beginscan / rescan)
 * ================================================================ */

/*
 * Return BISCUIT_PRELOAD_* for the given OID, or BISCUIT_PRELOAD_NONE
 * if the OID is not tracked (or a different OID owns the same slot).
 *
 * O(1): reads two atomic uint32s, no lock, no ring-buffer scan.
 * Safe to call concurrently with the worker or biscuit_preload_request.
 */
uint32
biscuit_preload_state(Oid indexoid)
{
    BiscuitPreloadShmem *sh = biscuit_preload_shmem;
    int      oid_slot;
    uint32   stored_oid;

    if (!sh)
        return BISCUIT_PRELOAD_NONE;

    oid_slot   = (int)(indexoid % 64);
    stored_oid = pg_atomic_read_u32(&sh->slot_oid[oid_slot]);

    /* Collision: another OID owns this slot */
    if (stored_oid != (uint32) indexoid)
        return BISCUIT_PRELOAD_NONE;

    return pg_atomic_read_u32(&sh->slot_state[oid_slot]);
}

/* ================================================================
 * biscuit_load_skeleton
 * Fast first-query loader: heap scan only, no bitmaps.
 * ================================================================ */

BiscuitIndex *
biscuit_load_skeleton(Relation index)
{
    IndexInfo        *indexInfo;
    Relation          heap;
    BiscuitIndex     *idx;
    TupleTableSlot   *slot_tbl;
    TableScanDesc     scan;
    MemoryContext     oldcontext;
    int               natts, col, ch;
    EState           *estate;
    ExprContext      *econtext;
    Datum             index_values[INDEX_MAX_KEYS];
    bool              index_isnull[INDEX_MAX_KEYS];
    Oid               single_coltypid = InvalidOid;
    FmgrInfo          single_output_func;

    /*
     * FIX #11 — identical root cause to FIX #10 in biscuit_index.c's
     * biscuit_build(), but in a completely separate code path: this
     * function does its own heap scan rather than reusing biscuit_build(),
     * so it had never received that fix and crashed on expression index
     * columns (e.g. (col::text), lower(col2)) the first time a query ran
     * after CREATE INDEX -- i.e. the first time this skeleton loader fires.
     *
     * The single-column branch was actually missing the type lookup
     * entirely (it called DatumGetTextPP(val) directly, assuming text),
     * and fetched the value via
     *   slot_getattr(slot_tbl, indexInfo->ii_IndexAttrNumbers[0], &isnull)
     * which is InvalidAttrNumber (0) for an expression column -- not a
     * valid attribute number, and slot_getattr() cannot evaluate an
     * expression regardless. The multi-column branch additionally derived
     * each column's type via index->rd_index->indkey.values[col] looked up
     * against the *heap's* tuple descriptor, which is the same garbage-OID
     * bug already fixed in ambuild.
     *
     * Because this function builds the skeleton's data_cache /
     * column_data_cache, the garbage values it wrote here were stored
     * successfully (no immediate error) and only crashed later, when a
     * scan (biscuit_fallback_scan / pattern matching) dereferenced that
     * garbage -- explaining "index creation succeeds, query segfaults".
     *
     * Fix: same pattern as biscuit_build() -- derive each column's type
     * from the index's own tuple descriptor (always correct for both plain
     * columns and expressions), and fetch values via FormIndexDatum()
     * instead of slot_getattr(), since only FormIndexDatum() can evaluate
     * an arbitrary expression against the heap tuple.
     */
    estate   = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);

    /*
     * All skeleton data must live in CacheMemoryContext.  rd_indexcxt is
     * owned by PostgreSQL and deleted by RelationClearRelation on any
     * relcache invalidation, which would free our data while biscuit_cache
     * still holds the pointer.  CacheMemoryContext is never reset by PG.
     */
    oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

    heap      = table_open(index->rd_index->indrelid, AccessShareLock);
    indexInfo = BuildIndexInfo(index);
    natts     = index->rd_index->indnatts;

    idx              = (BiscuitIndex *) palloc0(sizeof(BiscuitIndex));
    idx->capacity    = 1024;
    idx->num_records = 0;
    idx->num_columns = natts;
    idx->max_len     = 0;
    idx->tids        = (ItemPointerData *) palloc(idx->capacity * sizeof(ItemPointerData));

    /* ---- Allocate data caches; leave ALL bitmap fields NULL ---- */
    if (natts == 1)
    {
        Oid  typoutput;
        bool typIsVarlena;

        single_coltypid = TupleDescAttr(RelationGetDescr(index), 0)->atttypid;
        getTypeOutputInfo(single_coltypid, &typoutput, &typIsVarlena);
        fmgr_info(typoutput, &single_output_func);

        idx->data_cache       = (char **) palloc0(idx->capacity * sizeof(char *));
        idx->data_cache_lower = (char **) palloc0(idx->capacity * sizeof(char *));

        /*
         * Allocate CharIndex entry arrays so that insert/remove paths
         * don't have to check for NULL, but leave .count = 0 and all
         * bitmap pointers NULL — the worker fills them in.
         */
        for (ch = 0; ch < CHAR_RANGE; ch++)
        {
            idx->pos_idx_legacy[ch].entries  = (PosEntry *) palloc(8 * sizeof(PosEntry));
            idx->pos_idx_legacy[ch].count    = 0;
            idx->pos_idx_legacy[ch].capacity = 8;
            idx->neg_idx_legacy[ch].entries  = (PosEntry *) palloc(8 * sizeof(PosEntry));
            idx->neg_idx_legacy[ch].count    = 0;
            idx->neg_idx_legacy[ch].capacity = 8;

            idx->pos_idx_lower[ch].entries   = (PosEntry *) palloc(8 * sizeof(PosEntry));
            idx->pos_idx_lower[ch].count     = 0;
            idx->pos_idx_lower[ch].capacity  = 8;
            idx->neg_idx_lower[ch].entries   = (PosEntry *) palloc(8 * sizeof(PosEntry));
            idx->neg_idx_lower[ch].count     = 0;
            idx->neg_idx_lower[ch].capacity  = 8;

            idx->char_cache_legacy[ch]  = NULL;
            idx->char_cache_lower[ch]   = NULL;
        }
    }
    else
    {
        idx->column_types            = (Oid *)        palloc(natts * sizeof(Oid));
        idx->output_funcs            = (FmgrInfo *)   palloc(natts * sizeof(FmgrInfo));
        idx->column_data_cache       = (char ***)     palloc(natts * sizeof(char **));
        idx->column_data_cache_lower = (char ***)     palloc(natts * sizeof(char **));
        idx->column_indices          = (ColumnIndex *) palloc0(natts * sizeof(ColumnIndex));

        for (col = 0; col < natts; col++)
        {
            /*
             * Use the index's own tuple descriptor, not the heap's --
             * see FIX #11 above. This is correct for both plain Var
             * columns and arbitrary expressions.
             */
            Form_pg_attribute col_attr = TupleDescAttr(RelationGetDescr(index), col);
            Oid               typoutput;
            bool              typIsVarlena;
            ColumnIndex      *cidx = &idx->column_indices[col];

            idx->column_types[col] = col_attr->atttypid;
            getTypeOutputInfo(col_attr->atttypid, &typoutput, &typIsVarlena);
            fmgr_info(typoutput, &idx->output_funcs[col]);
            idx->column_data_cache[col]       = (char **) palloc0(idx->capacity * sizeof(char *));
            idx->column_data_cache_lower[col] = (char **) palloc0(idx->capacity * sizeof(char *));

            for (ch = 0; ch < CHAR_RANGE; ch++)
            {
                cidx->pos_idx[ch].entries  = (PosEntry *) palloc(8 * sizeof(PosEntry));
                cidx->pos_idx[ch].count    = 0;
                cidx->pos_idx[ch].capacity = 8;
                cidx->neg_idx[ch].entries  = (PosEntry *) palloc(8 * sizeof(PosEntry));
                cidx->neg_idx[ch].count    = 0;
                cidx->neg_idx[ch].capacity = 8;

                cidx->pos_idx_lower[ch].entries  = (PosEntry *) palloc(8 * sizeof(PosEntry));
                cidx->pos_idx_lower[ch].count     = 0;
                cidx->pos_idx_lower[ch].capacity  = 8;
                cidx->neg_idx_lower[ch].entries   = (PosEntry *) palloc(8 * sizeof(PosEntry));
                cidx->neg_idx_lower[ch].count     = 0;
                cidx->neg_idx_lower[ch].capacity  = 8;

                cidx->char_cache[ch]       = NULL;
                cidx->char_cache_lower[ch] = NULL;
            }
        }
    }

    biscuit_init_crud_structures(idx);

    /* ---- Heap scan: store TIDs + raw strings only ---- */
    slot_tbl = table_slot_create(heap, NULL);
    
    #if PG_VERSION_NUM >= 190000
    scan = table_beginscan(heap, SnapshotAny, 0, NULL, 0);
    #else
        scan = table_beginscan(heap, SnapshotAny, 0, NULL);
    #endif
    
    while (table_scan_getnextslot(scan, ForwardScanDirection, slot_tbl))
    {
        /*
         * FIX #11: evaluate every index key (plain Var or expression)
         * via FormIndexDatum() instead of slot_getattr(), which can only
         * fetch a real heap attribute and has no way to run an expression.
         */
        ResetExprContext(econtext);
        econtext->ecxt_scantuple = slot_tbl;
        FormIndexDatum(indexInfo, slot_tbl, estate, index_values, index_isnull);

        if (idx->num_records >= idx->capacity)
        {
            idx->capacity *= 2;
            idx->tids = (ItemPointerData *) repalloc(
                idx->tids, idx->capacity * sizeof(ItemPointerData));

            if (natts == 1)
            {
                idx->data_cache = (char **) repalloc(
                    idx->data_cache, idx->capacity * sizeof(char *));
                idx->data_cache_lower = (char **) repalloc(
                    idx->data_cache_lower, idx->capacity * sizeof(char *));
            }
            else
            {
                for (col = 0; col < natts; col++)
                {
                    idx->column_data_cache[col] = (char **) repalloc(
                        idx->column_data_cache[col],
                        idx->capacity * sizeof(char *));
                    idx->column_data_cache_lower[col] = (char **) repalloc(
                        idx->column_data_cache_lower[col],
                        idx->capacity * sizeof(char *));
                }
            }
        }

        ItemPointerCopy(&slot_tbl->tts_tid, &idx->tids[idx->num_records]);

        if (natts == 1)
        {
            if (!index_isnull[0])
            {
                int   out_len;
                char *str = biscuit_datum_to_text(index_values[0], single_coltypid,
                                                   &single_output_func, &out_len);
                int   char_count = biscuit_utf8_char_count(str, out_len);

                idx->data_cache[idx->num_records]       = str;
                idx->data_cache_lower[idx->num_records] =
                    biscuit_str_tolower(str, out_len);

                if (char_count > idx->max_len)
                    idx->max_len = char_count;
            }
            else
            {
                idx->data_cache[idx->num_records]       = NULL;
                idx->data_cache_lower[idx->num_records] = NULL;
            }
        }
        else
        {
            for (col = 0; col < natts; col++)
            {
                int   out_len;

                if (!index_isnull[col])
                    idx->column_data_cache[col][idx->num_records] =
                        biscuit_datum_to_text(index_values[col],
                                              idx->column_types[col],
                                              &idx->output_funcs[col],
                                              &out_len);
                else
                    idx->column_data_cache[col][idx->num_records] = NULL;

                /*
                 * Pre-compute the lowercased copy for ILIKE fallback scans.
                 * biscuit_fallback_scan() reads column_data_cache_lower
                 * directly, avoiding a per-record palloc during query
                 * execution.  Mirror NULL entries exactly.
                 */
                if (idx->column_data_cache[col][idx->num_records])
                    idx->column_data_cache_lower[col][idx->num_records] =
                        biscuit_str_tolower(
                            idx->column_data_cache[col][idx->num_records],
                            strlen(idx->column_data_cache[col][idx->num_records]));
                else
                    idx->column_data_cache_lower[col][idx->num_records] = NULL;
            }
        }

        idx->num_records++;
    }

    table_endscan(scan);
    ExecDropSingleTupleTableSlot(slot_tbl);
    table_close(heap, AccessShareLock);
    FreeExecutorState(estate);

    /* Mark as skeleton only — bitmaps not yet built */
    idx->preload_state = BISCUIT_PRELOAD_SKELETON;
    
    /* If the worker already signalled DONE, build bitmaps now
     * so the returned index is immediately warm and consistent with shmem. */
    if (biscuit_preload_state(RelationGetRelid(index)) >= BISCUIT_PRELOAD_DONE)
        biscuit_complete_preload_local(idx, RelationGetRelid(index));

    MemoryContextSwitchTo(oldcontext);

    elog(DEBUG1,
         "Biscuit: skeleton loaded for index %u (%d records, no bitmaps)",
         RelationGetRelid(index), idx->num_records);

    return idx;
}

/* ================================================================
 * biscuit_complete_preload
 * Called from the background worker: builds all bitmaps from the
 * already-populated data_cache and updates the session cache entry.
 * ================================================================ */

/*
 * Internal: index a single string into pos/neg/char/length bitmaps
 * for the single-column (legacy) layout.  Mirrors biscuit_index_single_record
 * in biscuit_index.c.
 */
static void
preload_index_single_record(BiscuitIndex *idx,
                             const char   *str,
                             int           byte_len,
                             int           rec_idx)
{
    int byte_pos = 0;
    int char_pos = 0;
    int char_count = biscuit_utf8_char_count(str, byte_len);

    /* Case-sensitive */
    while (byte_pos < byte_len)
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

            bm = biscuit_get_pos_bitmap(idx, uch, char_pos);
            if (!bm) { bm = biscuit_roaring_create(); biscuit_set_pos_bitmap(idx, uch, char_pos, bm); }
            biscuit_roaring_add(bm, rec_idx);

            remaining_chars = biscuit_utf8_char_count(str + byte_pos, byte_len - byte_pos);
            neg_offset = -remaining_chars;
            bm = biscuit_get_neg_bitmap(idx, uch, neg_offset);
            if (!bm) { bm = biscuit_roaring_create(); biscuit_set_neg_bitmap(idx, uch, neg_offset, bm); }
            biscuit_roaring_add(bm, rec_idx);

            if (!idx->char_cache_legacy[uch])
                idx->char_cache_legacy[uch] = biscuit_roaring_create();
            biscuit_roaring_add(idx->char_cache_legacy[uch], rec_idx);
        }
        byte_pos += char_len;
        char_pos++;
    }

    /* Case-insensitive — data_cache_lower already populated by skeleton */
    if (idx->data_cache_lower[rec_idx])
    {
        const char *sl      = idx->data_cache_lower[rec_idx];
        int         sll     = strlen(sl);
        int         lcc     = biscuit_utf8_char_count(sl, sll);
        int         lbp     = 0;
        int         lcp     = 0;

        if (lcc > idx->max_length_lower)
            idx->max_length_lower = lcc;

        while (lbp < sll)
        {
            unsigned char fb  = (unsigned char) sl[lbp];
            int           cl2 = biscuit_utf8_char_length(fb);
            int           b;

            if (lbp + cl2 > sll) cl2 = sll - lbp;

            for (b = 0; b < cl2; b++)
            {
                unsigned char uch = (unsigned char) sl[lbp + b];
                RoaringBitmap *bm;
                int rc, no;

                bm = biscuit_get_pos_bitmap_lower(idx, uch, lcp);
                if (!bm) { bm = biscuit_roaring_create(); biscuit_set_pos_bitmap_lower(idx, uch, lcp, bm); }
                biscuit_roaring_add(bm, rec_idx);

                rc = biscuit_utf8_char_count(sl + lbp, sll - lbp);
                no = -rc;
                bm = biscuit_get_neg_bitmap_lower(idx, uch, no);
                if (!bm) { bm = biscuit_roaring_create(); biscuit_set_neg_bitmap_lower(idx, uch, no, bm); }
                biscuit_roaring_add(bm, rec_idx);

                if (!idx->char_cache_lower[uch])
                    idx->char_cache_lower[uch] = biscuit_roaring_create();
                biscuit_roaring_add(idx->char_cache_lower[uch], rec_idx);
            }
            lbp += cl2;
            lcp++;
        }
    }

    if (char_count > idx->max_len)
        idx->max_len = char_count;
}

void 
biscuit_complete_preload(Oid indexoid)
{
    int                  oid_slot = (int)(indexoid % 64);
    BiscuitPreloadShmem *sh       = biscuit_preload_shmem;

    /*
     * The worker's only job is to signal DONE in the shmem slot.
     *
     * Each foreground session has already called biscuit_load_skeleton()
     * during its first beginscan, so it holds TIDs and raw string data in
     * its own CacheMemoryContext.  When rescan() next checks shmem and
     * finds DONE it calls biscuit_complete_preload_local() on that
     * existing skeleton — pure CPU work, zero extra I/O.
     *
     * A previous implementation did a full heap scan here (biscuit_load_skeleton
     * + biscuit_complete_preload_local) and then discarded the result.  That
     * wasted one extra heap scan per index per server start while providing
     * no benefit: the worker's address space is separate from every session,
     * so nothing built here is reachable by foreground backends.
     *
     * Verifying the index is still valid (index_open) before signalling DONE
     * is a worthwhile safety check and is cheap — no heap scan, just a
     * syscache lookup.
     */
    Relation index = index_open(indexoid, AccessShareLock);
    if (!index)
    {
        elog(WARNING, "Biscuit preload: could not open index %u, skipping",
             indexoid);
        return;
    }
    index_close(index, AccessShareLock);

    if (sh && oid_slot >= 0)
        pg_atomic_write_u32(&sh->slot_state[oid_slot], BISCUIT_PRELOAD_DONE);

    elog(LOG, "Biscuit preload: signalled DONE for index %u "
              "(bitmaps built on demand per session)", indexoid);
}

/* ================================================================
 * biscuit_complete_preload_local
 *
 * Builds bitmaps from an already-populated skeleton that lives in the
 * *calling* backend's memory.  No relation open, no heap scan — pure
 * CPU work over the string arrays already in data_cache /
 * column_data_cache.
 *
 * This is the foreground-side alternative to calling biscuit_load_index()
 * (which would do a full heap scan + bitmap build from scratch).  Because
 * the skeleton is already in memory the only cost here is the bitmap
 * construction itself — identical to what biscuit_complete_preload() does
 * in the background worker, but without the second heap scan.
 *
 * On success:
 *   • idx->preload_state is set to BISCUIT_PRELOAD_DONE
 *   • The shmem slot_state for indexoid is written to DONE so that
 *     other sessions see the index as warm.
 * ================================================================ */
void
biscuit_complete_preload_local(BiscuitIndex *idx, Oid indexoid)
{
    int rec_idx, i, ch;

    Assert(idx != NULL);
    Assert(idx->preload_state < BISCUIT_PRELOAD_DONE);

    idx->preload_state = BISCUIT_PRELOAD_RUNNING;

    if (idx->num_columns == 1)
    {
        for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
        {
            if (idx->data_cache[rec_idx])
                preload_index_single_record(idx,
                                            idx->data_cache[rec_idx],
                                            strlen(idx->data_cache[rec_idx]),
                                            rec_idx);
            CHECK_FOR_INTERRUPTS();
        }

        /* Length bitmaps */
        idx->max_length_legacy = idx->max_len + 1;
        idx->max_length_lower += 1;

        idx->length_bitmaps_legacy    = (RoaringBitmap **) palloc0(idx->max_length_legacy * sizeof(RoaringBitmap *));
        idx->length_ge_bitmaps_legacy = (RoaringBitmap **) palloc0(idx->max_length_legacy * sizeof(RoaringBitmap *));
        idx->length_bitmaps_lower     = (RoaringBitmap **) palloc0(idx->max_length_lower   * sizeof(RoaringBitmap *));
        idx->length_ge_bitmaps_lower  = (RoaringBitmap **) palloc0(idx->max_length_lower   * sizeof(RoaringBitmap *));

        for (ch = 0; ch < idx->max_length_legacy; ch++)
            idx->length_ge_bitmaps_legacy[ch] = biscuit_roaring_create();
        for (ch = 0; ch < idx->max_length_lower; ch++)
            idx->length_ge_bitmaps_lower[ch] = biscuit_roaring_create();

        for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
        {
            int bl, cl;

            if (!idx->data_cache[rec_idx]) continue;

            bl = strlen(idx->data_cache[rec_idx]);
            cl = biscuit_utf8_char_count(idx->data_cache[rec_idx], bl);

            if (cl < idx->max_length_legacy)
            {
                if (!idx->length_bitmaps_legacy[cl])
                    idx->length_bitmaps_legacy[cl] = biscuit_roaring_create();
                biscuit_roaring_add(idx->length_bitmaps_legacy[cl], rec_idx);
            }
            for (i = 0; i <= cl && i < idx->max_length_legacy; i++)
                biscuit_roaring_add(idx->length_ge_bitmaps_legacy[i], rec_idx);

            if (idx->data_cache_lower[rec_idx])
            {
                int lbl = strlen(idx->data_cache_lower[rec_idx]);
                int lcl = biscuit_utf8_char_count(idx->data_cache_lower[rec_idx], lbl);

                if (lcl < idx->max_length_lower)
                {
                    if (!idx->length_bitmaps_lower[lcl])
                        idx->length_bitmaps_lower[lcl] = biscuit_roaring_create();
                    biscuit_roaring_add(idx->length_bitmaps_lower[lcl], rec_idx);
                }
                for (i = 0; i <= lcl && i < idx->max_length_lower; i++)
                    biscuit_roaring_add(idx->length_ge_bitmaps_lower[i], rec_idx);
            }
        }
    }
    else
    {
        int col;

        for (col = 0; col < idx->num_columns; col++)
        {
            ColumnIndex *cidx        = &idx->column_indices[col];
            int          max_cl      = 0;
            int          max_cl_lower = 0;

            for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
            {
                const char *str;
                int         bl, cl, byte_pos, char_pos;

                if (!idx->column_data_cache[col][rec_idx]) continue;

                str = idx->column_data_cache[col][rec_idx];
                bl  = strlen(str);
                cl  = biscuit_utf8_char_count(str, bl);

                if (cl > max_cl) max_cl = cl;

                /* ---- Case-sensitive pass ----
                 *
                 * FIX 1: Use biscuit_get_col_pos_bitmap /
                 * biscuit_set_col_pos_bitmap instead of direct array
                 * indexing.  The set_* functions do a binary-search
                 * insert with repalloc on capacity overflow; the old
                 * code did entries[count++] with no bounds check,
                 * overflowing the 8-slot skeleton allocation after the
                 * 9th distinct (char, position) pair.
                 */
                byte_pos = char_pos = 0;
                while (byte_pos < bl)
                {
                    unsigned char fb  = (unsigned char) str[byte_pos];
                    int           chl = biscuit_utf8_char_length(fb);
                    int           b;

                    if (byte_pos + chl > bl) chl = bl - byte_pos;

                    for (b = 0; b < chl; b++)
                    {
                        unsigned char  uch = (unsigned char) str[byte_pos + b];
                        RoaringBitmap *bm;
                        int            rch, no;

                        /* positive-position bitmap */
                        bm = biscuit_get_col_pos_bitmap(cidx, uch, char_pos);
                        if (!bm)
                        {
                            bm = biscuit_roaring_create();
                            biscuit_set_col_pos_bitmap(cidx, uch, char_pos, bm);
                        }
                        biscuit_roaring_add(bm, rec_idx);

                        /* negative-position bitmap */
                        rch = biscuit_utf8_char_count(str + byte_pos, bl - byte_pos);
                        no  = -rch;
                        bm  = biscuit_get_col_neg_bitmap(cidx, uch, no);
                        if (!bm)
                        {
                            bm = biscuit_roaring_create();
                            biscuit_set_col_neg_bitmap(cidx, uch, no, bm);
                        }
                        biscuit_roaring_add(bm, rec_idx);

                        /* character-presence cache */
                        if (!cidx->char_cache[uch])
                            cidx->char_cache[uch] = biscuit_roaring_create();
                        biscuit_roaring_add(cidx->char_cache[uch], rec_idx);
                    }
                    byte_pos += chl;
                    char_pos++;
                }

                /* ---- Case-insensitive pass ----
                 *
                 * FIX 2: This entire block was missing.  The old code
                 * never populated pos_idx_lower, neg_idx_lower, or
                 * char_cache_lower for multi-column indexes, so any
                 * ILIKE query after a preload would dereference NULL or
                 * stale pointers and segfault.
                 */
                {
                    char *sl  = biscuit_str_tolower(str, bl);
                    int   sll = (int) strlen(sl);
                    int   lcc = biscuit_utf8_char_count(sl, sll);
                    int   lbp = 0;
                    int   lcp = 0;

                    if (lcc > max_cl_lower) max_cl_lower = lcc;

                    while (lbp < sll)
                    {
                        unsigned char fb2 = (unsigned char) sl[lbp];
                        int           chl2 = biscuit_utf8_char_length(fb2);
                        int           b;

                        if (lbp + chl2 > sll) chl2 = sll - lbp;

                        for (b = 0; b < chl2; b++)
                        {
                            unsigned char  uch = (unsigned char) sl[lbp + b];
                            RoaringBitmap *bm;
                            int            rc, no;

                            bm = biscuit_get_col_pos_bitmap_lower(cidx, uch, lcp);
                            if (!bm)
                            {
                                bm = biscuit_roaring_create();
                                biscuit_set_col_pos_bitmap_lower(cidx, uch, lcp, bm);
                            }
                            biscuit_roaring_add(bm, rec_idx);

                            rc = biscuit_utf8_char_count(sl + lbp, sll - lbp);
                            no = -rc;
                            bm = biscuit_get_col_neg_bitmap_lower(cidx, uch, no);
                            if (!bm)
                            {
                                bm = biscuit_roaring_create();
                                biscuit_set_col_neg_bitmap_lower(cidx, uch, no, bm);
                            }
                            biscuit_roaring_add(bm, rec_idx);

                            if (!cidx->char_cache_lower[uch])
                                cidx->char_cache_lower[uch] = biscuit_roaring_create();
                            biscuit_roaring_add(cidx->char_cache_lower[uch], rec_idx);
                        }
                        lbp += chl2;
                        lcp++;
                    }

                    pfree(sl);
                }

                CHECK_FOR_INTERRUPTS();
            }

            /* ---- Case-sensitive length bitmaps ---- */
            cidx->max_length = max_cl + 1;
            cidx->length_bitmaps    = (RoaringBitmap **) palloc0(cidx->max_length * sizeof(RoaringBitmap *));
            cidx->length_ge_bitmaps = (RoaringBitmap **) palloc0(cidx->max_length * sizeof(RoaringBitmap *));
            for (i = 0; i < cidx->max_length; i++)
                cidx->length_ge_bitmaps[i] = biscuit_roaring_create();

            for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
            {
                int bl2, cl2;

                if (!idx->column_data_cache[col][rec_idx]) continue;
                bl2 = strlen(idx->column_data_cache[col][rec_idx]);
                cl2 = biscuit_utf8_char_count(idx->column_data_cache[col][rec_idx], bl2);
                if (cl2 < cidx->max_length)
                {
                    if (!cidx->length_bitmaps[cl2])
                        cidx->length_bitmaps[cl2] = biscuit_roaring_create();
                    biscuit_roaring_add(cidx->length_bitmaps[cl2], rec_idx);
                }
                for (i = 0; i <= cl2 && i < cidx->max_length; i++)
                    biscuit_roaring_add(cidx->length_ge_bitmaps[i], rec_idx);
            }

            /* ---- Case-insensitive length bitmaps ----
             *
             * FIX 2 (cont): Previously max_length_lower was simply
             * copied from max_length (wrong: lowercasing can change
             * character count, e.g. German ß→ss), and the bitmaps were
             * allocated but never filled.  Now we use the independently
             * tracked max_cl_lower and populate correctly.
             */
            cidx->max_length_lower = max_cl_lower + 1;
            cidx->length_bitmaps_lower    = (RoaringBitmap **) palloc0(cidx->max_length_lower * sizeof(RoaringBitmap *));
            cidx->length_ge_bitmaps_lower = (RoaringBitmap **) palloc0(cidx->max_length_lower * sizeof(RoaringBitmap *));
            for (i = 0; i < cidx->max_length_lower; i++)
                cidx->length_ge_bitmaps_lower[i] = biscuit_roaring_create();

            for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
            {
                int   bl2, lcl;
                char *sl;

                if (!idx->column_data_cache[col][rec_idx]) continue;
                bl2 = strlen(idx->column_data_cache[col][rec_idx]);
                sl  = biscuit_str_tolower(idx->column_data_cache[col][rec_idx], bl2);
                lcl = biscuit_utf8_char_count(sl, strlen(sl));
                pfree(sl);

                if (lcl < cidx->max_length_lower)
                {
                    if (!cidx->length_bitmaps_lower[lcl])
                        cidx->length_bitmaps_lower[lcl] = biscuit_roaring_create();
                    biscuit_roaring_add(cidx->length_bitmaps_lower[lcl], rec_idx);
                }
                for (i = 0; i <= lcl && i < cidx->max_length_lower; i++)
                    biscuit_roaring_add(cidx->length_ge_bitmaps_lower[i], rec_idx);
            }
        }
    }

    idx->preload_state = BISCUIT_PRELOAD_DONE;

    /* Signal other sessions via shmem */
    {
        BiscuitPreloadShmem *_sh = biscuit_preload_shmem;
        if (_sh)
        {
            int _oid_slot = (int)(indexoid % 64);
            pg_atomic_write_u32(&_sh->slot_state[_oid_slot], BISCUIT_PRELOAD_DONE);
        }
    }

    elog(DEBUG1,
         "Biscuit: local bitmap build complete for index %u (%d records)",
         indexoid, idx->num_records);
}

/* ================================================================
 * biscuit_fallback_scan
 * Sequential string scan used while bitmaps are not yet ready.
 * ================================================================ */

/*
 * biscuit_like_match
 * -------------------
 * Correct, UTF-8-aware backtracking LIKE/ILIKE matcher.  Honors the same
 * escape convention as the rest of this extension (backslash escapes the
 * following character, so '\%' / '\_' / '\\' are literals, not wildcards).
 *
 * '%' matches zero or more characters; '_' matches exactly one UTF-8
 * character.  Backtracks on '%' the standard way: remember the last '%'
 * position and the haystack position right after it, and on a mismatch
 * retry by having that '%' consume one more character.
 *
 * hay/hay_len and pat/pat_len are byte buffers; ilike compares using
 * lower-cased single bytes (callers pass already-lower-cased hay+pattern
 * for ILIKE, matching the convention used elsewhere in this file).
 */
static bool
biscuit_like_match(const char *hay, int hay_len, const char *pat, int pat_len)
{
    int hi = 0, pi = 0;
    int star_pi = -1, star_hi = -1;

    while (hi < hay_len)
    {
        if (pi < pat_len && pat[pi] == '\\' && pi + 1 < pat_len)
        {
            /* literal escaped char must match exactly (1 byte) */
            if (hay[hi] == pat[pi + 1])
            {
                hi++;
                pi += 2;
                continue;
            }
        }
        else if (pi < pat_len && pat[pi] == '_')
        {
            int cl = biscuit_utf8_char_length((unsigned char) hay[hi]);
            if (hi + cl > hay_len)
                cl = hay_len - hi;
            hi += cl;
            pi += 1;
            continue;
        }
        else if (pi < pat_len && pat[pi] == '%')
        {
            star_pi = pi++;
            star_hi = hi;
            continue;
        }
        else if (pi < pat_len && hay[hi] == pat[pi])
        {
            hi++;
            pi++;
            continue;
        }

        /* mismatch: backtrack to the most recent '%' if any */
        if (star_pi >= 0)
        {
            pi = star_pi + 1;
            star_hi += biscuit_utf8_char_length((unsigned char) hay[star_hi]);
            if (star_hi > hay_len)
                return false;
            hi = star_hi;
            continue;
        }
        return false;
    }

    /* consume any trailing '%' (and reject any other trailing pattern) */
    while (pi < pat_len && pat[pi] == '%')
        pi++;

    return pi == pat_len;
}

void
biscuit_fallback_scan(BiscuitIndex *idx,
                      const char   *pattern,
                      bool          ilike,
                      int           col_idx,
                      ItemPointerData **out_tids,
                      int          *out_count)
{
    int              i;
    int              capacity = 256;
    int              count    = 0;
    ItemPointerData *tids     = (ItemPointerData *) palloc(capacity * sizeof(ItemPointerData));
    const char      *match_pattern = pattern;
    char            *pattern_lower = NULL;
    int              pat_len;

    if (ilike)
    {
        pattern_lower = biscuit_str_tolower(pattern, strlen(pattern));
        match_pattern = pattern_lower;
    }
    pat_len = strlen(match_pattern);

    for (i = 0; i < idx->num_records; i++)
    {
        const char *str;

        /* skip tombstoned */
#ifdef HAVE_ROARING
        if (roaring_bitmap_contains(idx->tombstones, (uint32_t) i))
            continue;
#else
        { uint32_t bl = i >> 6, bt = i & 63;
          if (bl < idx->tombstones->num_blocks &&
              (idx->tombstones->blocks[bl] & (1ULL << bt)))
              continue; }
#endif

        if (idx->num_columns == 1)
        {
            str = ilike ? idx->data_cache_lower[i] : idx->data_cache[i];
        }
        else
        {
            if (col_idx < 0 || col_idx >= idx->num_columns) continue;
            if (ilike)
            {
                /*
                 * Use the pre-lowercased cache populated at build / skeleton
                 * load time.  This avoids a palloc + memcpy + pfree on every
                 * record for every ILIKE fallback scan.
                 *
                 * column_data_cache_lower is guaranteed non-NULL here: it is
                 * allocated alongside column_data_cache in both biscuit_build
                 * (biscuit_index.c) and biscuit_load_skeleton (this file).
                 * Per-slot entries are NULL iff the source value was NULL,
                 * which is caught by the "if (!str) continue" below.
                 */
                str = idx->column_data_cache_lower[col_idx][i];
            }
            else
            {
                str = idx->column_data_cache[col_idx][i];
            }
        }

        if (!str) continue;

        if (biscuit_like_match(str, strlen(str), match_pattern, pat_len))
        {
            if (count >= capacity)
            {
                capacity *= 2;
                tids = (ItemPointerData *) repalloc(
                    tids, capacity * sizeof(ItemPointerData));
            }
            ItemPointerCopy(&idx->tids[i], &tids[count++]);
        }
    }

    if (pattern_lower) pfree(pattern_lower);

    *out_tids  = tids;
    *out_count = count;
}

/* ================================================================
 * Background worker main loop
 * ================================================================ */

void
biscuit_preload_worker_main(Datum main_arg)
{
    BiscuitPreloadShmem *sh;

    (void) main_arg;   /* suppress unused-parameter warning */

    /* Allow signals */
    pqsignal(SIGTERM, die);
#if PG_VERSION_NUM >= 140000
    pqsignal(SIGHUP,  SignalHandlerForConfigReload);
#else
    pqsignal(SIGHUP,  PostgresSigHupHandler);
#endif
    BackgroundWorkerUnblockSignals();

    /* Connect to the default database (template1 or first DB in list) */
    BackgroundWorkerInitializeConnection(NULL, NULL, 0);

    /* Register our latch so requesters can wake us */
    sh = biscuit_preload_shmem;
    if (sh)
        sh->worker_latch = MyLatch;

    elog(LOG, "Biscuit preload worker started");

    for (;;)
    {
        Oid  indexoid = InvalidOid;
        int  oid_slot = -1;

        CHECK_FOR_INTERRUPTS();

        /* Dequeue one OID */
        if (sh)
        {
            LWLockAcquire(BiscuitPreloadLock(), LW_EXCLUSIVE);
            if (sh->queue_size > 0)
            {
                indexoid = sh->queue[sh->queue_head];
                sh->queue_head = (sh->queue_head + 1) % 64;
                sh->queue_size--;
                oid_slot = (int)(indexoid % 64);
                pg_atomic_write_u32(&sh->slot_state[oid_slot],
                                    BISCUIT_PRELOAD_RUNNING);
            }
            LWLockRelease(BiscuitPreloadLock());
        }

        if (OidIsValid(indexoid))
        {
            PG_TRY();
            {
                StartTransactionCommand();
                PushActiveSnapshot(GetTransactionSnapshot());

                biscuit_complete_preload(indexoid);

                PopActiveSnapshot();
                CommitTransactionCommand();

                /*
                 * biscuit_complete_preload already writes DONE into the
                 * shmem slot via its internal logic.  The write here is
                 * a belt-and-suspenders guarantee in case the function
                 * returns without doing so (e.g. "already DONE" early-exit).
                 */
                if (sh && oid_slot >= 0)
                    pg_atomic_write_u32(&sh->slot_state[oid_slot],
                                        BISCUIT_PRELOAD_DONE);
            }
            PG_CATCH();
            {
                if (sh && oid_slot >= 0)
                    pg_atomic_write_u32(&sh->slot_state[oid_slot],
                                        BISCUIT_PRELOAD_FAILED);

                HOLD_INTERRUPTS();
                EmitErrorReport();
                AbortCurrentTransaction();
                FlushErrorState();
                RESUME_INTERRUPTS();
            }
            PG_END_TRY();
        }
        else
        {
            /* Nothing to do — wait for a signal (up to 5 seconds) */
            (void) WaitLatch(MyLatch,
                             WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                             5000L,
                             WAIT_EVENT_BGWORKER_MAIN);
            ResetLatch(MyLatch);
        }
    }
}
