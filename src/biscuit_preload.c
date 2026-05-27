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

/* WAIT_EVENT_BGWORKER_MAIN was added in PG 17 */
#if PG_VERSION_NUM < 170000
#define WAIT_EVENT_BGWORKER_MAIN  WAIT_EVENT_BGWORKER_STARTUP
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

    if (!index->rd_indexcxt)
        index->rd_indexcxt = AllocSetContextCreate(
            CacheMemoryContext,
            "Biscuit index context",
            ALLOCSET_DEFAULT_SIZES);

    oldcontext = MemoryContextSwitchTo(index->rd_indexcxt);

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
        idx->column_types      = (Oid *)        palloc(natts * sizeof(Oid));
        idx->output_funcs      = (FmgrInfo *)   palloc(natts * sizeof(FmgrInfo));
        idx->column_data_cache = (char ***)     palloc(natts * sizeof(char **));
        idx->column_indices    = (ColumnIndex *) palloc0(natts * sizeof(ColumnIndex));

        for (col = 0; col < natts; col++)
        {
            AttrNumber        col_attnum = index->rd_index->indkey.values[col];
            Form_pg_attribute col_attr   = TupleDescAttr(heap->rd_att, col_attnum - 1);
            Oid               typoutput;
            bool              typIsVarlena;
            ColumnIndex      *cidx = &idx->column_indices[col];

            idx->column_types[col] = col_attr->atttypid;
            getTypeOutputInfo(col_attr->atttypid, &typoutput, &typIsVarlena);
            fmgr_info(typoutput, &idx->output_funcs[col]);
            idx->column_data_cache[col] = (char **) palloc0(idx->capacity * sizeof(char *));

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
    scan     = table_beginscan(heap, SnapshotAny, 0, NULL);

    while (table_scan_getnextslot(scan, ForwardScanDirection, slot_tbl))
    {
        slot_getallattrs(slot_tbl);

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
                    idx->column_data_cache[col] = (char **) repalloc(
                        idx->column_data_cache[col],
                        idx->capacity * sizeof(char *));
            }
        }

        ItemPointerCopy(&slot_tbl->tts_tid, &idx->tids[idx->num_records]);

        if (natts == 1)
        {
            Datum val;
            bool  isnull;

            val = slot_getattr(slot_tbl,
                               indexInfo->ii_IndexAttrNumbers[0],
                               &isnull);
            if (!isnull)
            {
                text *txt     = DatumGetTextPP(val);
                char *str     = VARDATA_ANY(txt);
                int   byte_len = VARSIZE_ANY_EXHDR(txt);
                int   char_count = biscuit_utf8_char_count(str, byte_len);

                idx->data_cache[idx->num_records]       = pnstrdup(str, byte_len);
                idx->data_cache_lower[idx->num_records] =
                    biscuit_str_tolower(str, byte_len);

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
                Datum val;
                bool  isnull;
                int   out_len;

                val = slot_getattr(slot_tbl,
                                   index->rd_index->indkey.values[col],
                                   &isnull);
                if (!isnull)
                    idx->column_data_cache[col][idx->num_records] =
                        biscuit_datum_to_text(val,
                                              idx->column_types[col],
                                              &idx->output_funcs[col],
                                              &out_len);
                else
                    idx->column_data_cache[col][idx->num_records] = NULL;
            }
        }

        idx->num_records++;
    }

    table_endscan(scan);
    ExecDropSingleTupleTableSlot(slot_tbl);
    table_close(heap, AccessShareLock);

    /* Mark as skeleton only — bitmaps not yet built */
    idx->preload_state = BISCUIT_PRELOAD_SKELETON;

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
    Relation      index;
    BiscuitIndex *idx;
    int           rec_idx, i, ch;
    MemoryContext oldcontext;

    elog(LOG, "Biscuit preload: starting bitmap build for index %u", indexoid);

    index = index_open(indexoid, AccessShareLock);
    if (!index)
    {
        elog(WARNING, "Biscuit preload: could not open index %u", indexoid);
        return;
    }

    /*
     * The worker runs in a separate backend process.  biscuit_cache_lookup
     * would search *this* backend's private cache, which never contains the
     * skeleton that the querying session built.  Always build a fresh
     * skeleton here so we have the raw string data to index from.
     */
    idx = biscuit_load_skeleton(index);
    if (!idx)
    {
        index_close(index, AccessShareLock);
        return;
    }

    /* Sanity: if somehow already fully loaded, skip */
    if (idx->preload_state == BISCUIT_PRELOAD_DONE)
    {
        index_close(index, AccessShareLock);
        return;
    }

    if (!index->rd_indexcxt)
        index->rd_indexcxt = AllocSetContextCreate(
            CacheMemoryContext, "Biscuit index context",
            ALLOCSET_DEFAULT_SIZES);

    oldcontext = MemoryContextSwitchTo(index->rd_indexcxt);

    idx->preload_state = BISCUIT_PRELOAD_RUNNING;

    PG_TRY();
    {
        if (idx->num_columns == 1)
        {
            /* ---- Build single-column bitmaps from data_cache ---- */
            for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
            {
                const char *str;
                int         byte_len;

                if (!idx->data_cache[rec_idx])
                    continue;

                str      = idx->data_cache[rec_idx];
                byte_len = strlen(str);

                preload_index_single_record(idx, str, byte_len, rec_idx);
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
                int bl;
                int cl;

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
            /* ---- Multi-column: per-column bitmap build ---- */
            int col;

            for (col = 0; col < idx->num_columns; col++)
            {
                ColumnIndex *cidx  = &idx->column_indices[col];
                int          max_cl = 0;

                for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
                {
                    const char *str;
                    int         bl, cl, byte_pos, char_pos;

                    if (!idx->column_data_cache[col][rec_idx]) continue;

                    str  = idx->column_data_cache[col][rec_idx];
                    bl   = strlen(str);
                    cl   = biscuit_utf8_char_count(str, bl);

                    if (cl > max_cl) max_cl = cl;

                    byte_pos = char_pos = 0;
                    while (byte_pos < bl)
                    {
                        unsigned char fb  = (unsigned char) str[byte_pos];
                        int           chl = biscuit_utf8_char_length(fb);
                        int           b;

                        if (byte_pos + chl > bl) chl = bl - byte_pos;

                        for (b = 0; b < chl; b++)
                        {
                            unsigned char uch = (unsigned char) str[byte_pos + b];
                            RoaringBitmap *bm;
                            int rch, no;

                            /* pos_idx */
                            if (char_pos >= cidx->pos_idx[uch].capacity)
                            {
                                /* grow CharIndex for this char */
                                int newcap = cidx->pos_idx[uch].capacity * 2;
                                cidx->pos_idx[uch].entries = (PosEntry *) repalloc(
                                    cidx->pos_idx[uch].entries,
                                    newcap * sizeof(PosEntry));
                                cidx->pos_idx[uch].capacity = newcap;
                            }
                            /* find or create pos entry */
                            bm = NULL;
                            {
                                int k;
                                for (k = 0; k < cidx->pos_idx[uch].count; k++)
                                    if (cidx->pos_idx[uch].entries[k].pos == char_pos)
                                    { bm = cidx->pos_idx[uch].entries[k].bitmap; break; }
                                if (!bm)
                                {
                                    PosEntry *pe = &cidx->pos_idx[uch].entries[cidx->pos_idx[uch].count++];
                                    pe->pos    = char_pos;
                                    pe->bitmap = biscuit_roaring_create();
                                    bm         = pe->bitmap;
                                }
                            }
                            biscuit_roaring_add(bm, rec_idx);

                            /* neg_idx */
                            rch = biscuit_utf8_char_count(str + byte_pos, bl - byte_pos);
                            no  = -rch;
                            bm  = NULL;
                            {
                                int k;
                                for (k = 0; k < cidx->neg_idx[uch].count; k++)
                                    if (cidx->neg_idx[uch].entries[k].pos == no)
                                    { bm = cidx->neg_idx[uch].entries[k].bitmap; break; }
                                if (!bm)
                                {
                                    PosEntry *pe = &cidx->neg_idx[uch].entries[cidx->neg_idx[uch].count++];
                                    pe->pos    = no;
                                    pe->bitmap = biscuit_roaring_create();
                                    bm         = pe->bitmap;
                                }
                            }
                            biscuit_roaring_add(bm, rec_idx);

                            if (!cidx->char_cache[uch])
                                cidx->char_cache[uch] = biscuit_roaring_create();
                            biscuit_roaring_add(cidx->char_cache[uch], rec_idx);
                        }
                        byte_pos += chl;
                        char_pos++;
                    }

                    CHECK_FOR_INTERRUPTS();
                }

                /* Length bitmaps for this column */
                cidx->max_length = max_cl + 1;
                cidx->length_bitmaps    = (RoaringBitmap **) palloc0(cidx->max_length * sizeof(RoaringBitmap *));
                cidx->length_ge_bitmaps = (RoaringBitmap **) palloc0(cidx->max_length * sizeof(RoaringBitmap *));
                for (i = 0; i < cidx->max_length; i++)
                    cidx->length_ge_bitmaps[i] = biscuit_roaring_create();

                for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
                {
                    int bl2;
                    int cl2;

                    if (!idx->column_data_cache[col][rec_idx]) continue;
                    bl2 = strlen(idx->column_data_cache[col][rec_idx]);
                    cl2 = biscuit_utf8_char_count(idx->column_data_cache[col][rec_idx], bl2);
                    if (cl2 < cidx->max_length)
                    {
                        if (!cidx->length_bitmaps[cl2]) cidx->length_bitmaps[cl2] = biscuit_roaring_create();
                        biscuit_roaring_add(cidx->length_bitmaps[cl2], rec_idx);
                    }
                    for (i = 0; i <= cl2 && i < cidx->max_length; i++)
                        biscuit_roaring_add(cidx->length_ge_bitmaps[i], rec_idx);
                }

                cidx->max_length_lower = cidx->max_length;
                cidx->length_bitmaps_lower    = (RoaringBitmap **) palloc0(cidx->max_length_lower * sizeof(RoaringBitmap *));
                cidx->length_ge_bitmaps_lower = (RoaringBitmap **) palloc0(cidx->max_length_lower * sizeof(RoaringBitmap *));
                for (i = 0; i < cidx->max_length_lower; i++)
                    cidx->length_ge_bitmaps_lower[i] = biscuit_roaring_create();
            }
        }

        /* Mark the local copy fully warm */
        idx->preload_state = BISCUIT_PRELOAD_DONE;

        /*
         * Write DONE into the shared-memory slot_state so that every
         * querying backend's rescan can detect completion via
         * biscuit_preload_state().  We scan the ring buffer for our OID
         * (same logic as biscuit_preload_state but with a write).
         *
         * Note: the worker's own biscuit_cache_insert below is harmless
         * but irrelevant — querying sessions have their own private caches.
         * The shmem flag is the only cross-process signal.
         */
        {
            BiscuitPreloadShmem *_sh = biscuit_preload_shmem;
            if (_sh)
            {
                int _oid_slot = (int)(indexoid % 64);
                pg_atomic_write_u32(&_sh->slot_state[_oid_slot],
                                    BISCUIT_PRELOAD_DONE);
            }
        }

        /* Cache in the worker's own backend (cheap, not relied upon). */
        biscuit_cache_insert(indexoid, idx);

        elog(LOG,
             "Biscuit preload: index %u fully loaded (%d records)",
             indexoid, idx->num_records);
    }
    PG_CATCH();
    {
        idx->preload_state = BISCUIT_PRELOAD_FAILED;
        elog(WARNING,
             "Biscuit preload: bitmap build failed for index %u; "
             "fallback scan will be used", indexoid);
        MemoryContextSwitchTo(oldcontext);
        PG_RE_THROW();
    }
    PG_END_TRY();

    MemoryContextSwitchTo(oldcontext);
    index_close(index, AccessShareLock);
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
            ColumnIndex *cidx  = &idx->column_indices[col];
            int          max_cl = 0;

            for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
            {
                const char *str;
                int         bl, cl, byte_pos, char_pos;

                if (!idx->column_data_cache[col][rec_idx]) continue;

                str  = idx->column_data_cache[col][rec_idx];
                bl   = strlen(str);
                cl   = biscuit_utf8_char_count(str, bl);

                if (cl > max_cl) max_cl = cl;

                byte_pos = char_pos = 0;
                while (byte_pos < bl)
                {
                    unsigned char fb  = (unsigned char) str[byte_pos];
                    int           chl = biscuit_utf8_char_length(fb);
                    int           b;

                    if (byte_pos + chl > bl) chl = bl - byte_pos;

                    for (b = 0; b < chl; b++)
                    {
                        unsigned char uch = (unsigned char) str[byte_pos + b];
                        RoaringBitmap *bm;
                        int rch, no;

                        bm = NULL;
                        {
                            int k;
                            for (k = 0; k < cidx->pos_idx[uch].count; k++)
                                if (cidx->pos_idx[uch].entries[k].pos == char_pos)
                                { bm = cidx->pos_idx[uch].entries[k].bitmap; break; }
                            if (!bm)
                            {
                                PosEntry *pe = &cidx->pos_idx[uch].entries[cidx->pos_idx[uch].count++];
                                pe->pos    = char_pos;
                                pe->bitmap = biscuit_roaring_create();
                                bm         = pe->bitmap;
                            }
                        }
                        biscuit_roaring_add(bm, rec_idx);

                        rch = biscuit_utf8_char_count(str + byte_pos, bl - byte_pos);
                        no  = -rch;
                        bm  = NULL;
                        {
                            int k;
                            for (k = 0; k < cidx->neg_idx[uch].count; k++)
                                if (cidx->neg_idx[uch].entries[k].pos == no)
                                { bm = cidx->neg_idx[uch].entries[k].bitmap; break; }
                            if (!bm)
                            {
                                PosEntry *pe = &cidx->neg_idx[uch].entries[cidx->neg_idx[uch].count++];
                                pe->pos    = no;
                                pe->bitmap = biscuit_roaring_create();
                                bm         = pe->bitmap;
                            }
                        }
                        biscuit_roaring_add(bm, rec_idx);

                        if (!cidx->char_cache[uch])
                            cidx->char_cache[uch] = biscuit_roaring_create();
                        biscuit_roaring_add(cidx->char_cache[uch], rec_idx);
                    }
                    byte_pos += chl;
                    char_pos++;
                }

                CHECK_FOR_INTERRUPTS();
            }

            /* Length bitmaps for this column */
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
                    if (!cidx->length_bitmaps[cl2]) cidx->length_bitmaps[cl2] = biscuit_roaring_create();
                    biscuit_roaring_add(cidx->length_bitmaps[cl2], rec_idx);
                }
                for (i = 0; i <= cl2 && i < cidx->max_length; i++)
                    biscuit_roaring_add(cidx->length_ge_bitmaps[i], rec_idx);
            }

            cidx->max_length_lower = cidx->max_length;
            cidx->length_bitmaps_lower    = (RoaringBitmap **) palloc0(cidx->max_length_lower * sizeof(RoaringBitmap *));
            cidx->length_ge_bitmaps_lower = (RoaringBitmap **) palloc0(cidx->max_length_lower * sizeof(RoaringBitmap *));
            for (i = 0; i < cidx->max_length_lower; i++)
                cidx->length_ge_bitmaps_lower[i] = biscuit_roaring_create();
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

    /*
     * Translate LIKE wildcards to a simple prefix+suffix+substring check.
     * For the warm-up window this does not need to be perfectly optimal —
     * just correct.  We use pg_strcasecmp for ILIKE.
     */
    bool   has_prefix  = false;
    bool   has_suffix  = false;
    bool   is_exact    = false;
    char  *prefix      = NULL;
    char  *suffix      = NULL;
    char  *substring   = NULL;
    int    prefix_len  = 0;
    int    suffix_len  = 0;
    int    sub_len     = 0;

    /* Simple pattern decomposition: find leading/trailing literal parts */
    {
        const char *p   = pattern;
        const char *end = pattern + strlen(pattern);
        const char *q;
        int         pct_count = 0;

        for (q = p; q < end; q++)
            if (*q == '%') pct_count++;

        if (pct_count == 0)
        {
            /* exact match */
            is_exact  = true;
            prefix     = pstrdup(pattern);
            prefix_len = strlen(prefix);
        }
        else
        {
            /* leading literal before first % */
            const char *first_pct = strchr(p, '%');
            const char *last_pct;
            if (first_pct > p)
            {
                prefix_len = first_pct - p;
                prefix     = pnstrdup(p, prefix_len);
                has_prefix = true;
            }

            /* trailing literal after last % */
            last_pct = strrchr(p, '%');
            if (last_pct < end - 1)
            {
                suffix     = pstrdup(last_pct + 1);
                suffix_len = strlen(suffix);
                has_suffix = true;
            }

            /* middle substring between first and last % */
            if (last_pct > first_pct + 1)
            {
                const char *mid_start = first_pct + 1;
                const char *mid_end   = last_pct;
                /* find first literal run (no %) */
                const char *s = mid_start;
                const char *e;
                while (s < mid_end && *s == '%') s++;
                e = s;
                while (e < mid_end && *e != '%') e++;
                if (e > s)
                {
                    substring = pnstrdup(s, e - s);
                    sub_len   = e - s;
                }
            }
        }
    }

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
            str = idx->column_data_cache[col_idx][i];
        }

        if (!str) continue;

        /* Match */
        {
            bool match = false;
            int  slen  = strlen(str);

            if (is_exact)
            {
                match = (ilike ? pg_strcasecmp(str, prefix) == 0
                               : strcmp(str, prefix) == 0);
            }
            else
            {
                bool ok = true;

                /* prefix check */
                if (has_prefix && prefix_len > 0)
                {
                    if (ilike)
                        ok = (pg_strncasecmp(str, prefix, prefix_len) == 0);
                    else
                        ok = (strncmp(str, prefix, prefix_len) == 0);
                }

                /* suffix check */
                if (ok && has_suffix && suffix_len > 0 && slen >= suffix_len)
                {
                    const char *str_end = str + slen - suffix_len;
                    if (ilike)
                        ok = (pg_strcasecmp(str_end, suffix) == 0);
                    else
                        ok = (strcmp(str_end, suffix) == 0);
                }
                else if (ok && has_suffix && slen < suffix_len)
                    ok = false;

                /* substring check */
                if (ok && substring && sub_len > 0)
                {
                    if (ilike)
                    {
                        /* case-insensitive substring: scan manually */
                        bool found = false;
                        int  j;
                        for (j = 0; j <= slen - sub_len; j++)
                        {
                            if (pg_strncasecmp(str + j, substring, sub_len) == 0)
                            { found = true; break; }
                        }
                        ok = found;
                    }
                    else
                        ok = (strstr(str, substring) != NULL);
                }

                match = ok;
            }

            if (match)
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
    }

    if (prefix)    pfree(prefix);
    if (suffix)    pfree(suffix);
    if (substring) pfree(substring);

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