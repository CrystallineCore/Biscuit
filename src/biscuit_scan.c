/*
 * biscuit_scan.c
 * Index scan lifecycle: beginscan, rescan (single- and multi-column),
 * gettuple, getbitmap, endscan.
 *
 * PARALLEL SCAN FIX (see also biscuit_tid.c)
 * ------------------------------------------
 * Root cause of the duplicate-row bug
 * ------------------------------------
 * biscuit_rescan() always called biscuit_collect_sorted_tids_single(),
 * which evaluates the full bitmap and returns ALL matching TIDs.  Because
 * every Gather participant (leader + N workers) executes an independent
 * amrescan, every participant collected the full result set and Gather
 * assembled N copies — producing rows=N×expected in EXPLAIN ANALYZE.
 *
 * Fix
 * ---
 * Both the single-column and multi-column fast paths now call
 * biscuit_collect_sorted_tids_parallel() when scan->parallel_scan != NULL.
 * That function handles all coordination internally:
 *
 *   • Each participant evaluates the bitmap locally (read-only, deterministic
 *     → identical result in every process, no DSM pointer sharing needed).
 *   • An atomic CAS on pdesc->next_chunk (UNINIT→INITING) elects exactly one
 *     initializer to write total_tids/total_chunks into the shared DSM
 *     descriptor.  Others spin-wait with CHECK_FOR_INTERRUPTS + 1µs sleep.
 *   • Every participant then claims a disjoint chunk range atomically and
 *     returns only those TIDs to Gather — assembling exactly one result copy.
 *
 * No changes to BiscuitScanOpaque or biscuit_common.h are required.
 * No IsParallelWorker() distinction is needed — every participant runs
 * the same code path.
 *
 *
 * Lazy-load strategy
 * ------------------
 * Previously beginscan called biscuit_load_index() synchronously, which
 * rebuilds every bitmap for the entire index before the first query can
 * return.  For a large index this can take hundreds of milliseconds and
 * consume gigabytes of RAM all at once.
 *
 * The new path:
 *
 *  1. beginscan   – loads only a lightweight skeleton (TIDs + raw string
 *                   data_cache, no bitmaps) via biscuit_load_skeleton(),
 *                   then immediately calls biscuit_preload_request() to
 *                   enqueue the index OID for the background worker.
 *                   Total cost: one heap scan, O(n) string copies.
 *
 *  2. rescan      – checks idx->preload_state:
 *       • BISCUIT_PRELOAD_DONE   → fast bitmap path (unchanged behaviour)
 *       • anything else          → biscuit_fallback_scan(), a plain
 *                                  strstr / strcasestr walk of data_cache.
 *                                  Correct but slower; used only during
 *                                  the warm-up window.
 *
 *  3. background worker – calls biscuit_complete_preload(), which
 *                          rebuilds all bitmaps from the already-cached
 *                          data and sets preload_state = DONE.  On the
 *                          next beginscan the session picks up the warm
 *                          index from the process cache and takes the
 *                          fast path from that point forward.
 *
 * The fallback scan result set is exact (no false positives) so
 * xs_recheck stays false.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_cache.h"
#include "biscuit_pattern.h"
#include "biscuit_tid.h"
#include "biscuit_index.h"
#include "biscuit_preload.h"   /* biscuit_load_skeleton, biscuit_preload_request,
                                   biscuit_preload_state, biscuit_fallback_scan */
#include "biscuit_scan.h"

/* ================================================================
 * SECTION 1 – beginscan
 *
 * Key change: on a cache miss we now call biscuit_load_skeleton()
 * instead of biscuit_load_index().  The skeleton is cheap (no bitmaps)
 * and is available immediately.  We then request background bitmap
 * construction via biscuit_preload_request().
 * ================================================================ */

IndexScanDesc
biscuit_beginscan(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc      scan;
    BiscuitScanOpaque *so;
    Oid                indexoid = RelationGetRelid(index);

    scan = RelationGetIndexScan(index, nkeys, norderbys);
    so   = (BiscuitScanOpaque *) palloc(sizeof(BiscuitScanOpaque));

    so->index = (BiscuitIndex *) index->rd_amcache;

    if (!so->index)
        so->index = biscuit_cache_lookup(indexoid);
    
    elog(DEBUG1, "Entered beginscan()");
    
    if (so->index)
    {
        /*
         * We have a cached index — but it may still be a skeleton.
         * Upgrade it synchronously if the worker has signalled DONE
         * and we haven't built the bitmaps yet in this session.
         *
         * This covers both the rd_amcache hit path and the
         * process-cache hit path.
         */
        if (so->index->preload_state < BISCUIT_PRELOAD_DONE)
        {
            uint32 shmem_state = biscuit_preload_state(indexoid);
            if (shmem_state >= BISCUIT_PRELOAD_DONE)
            {
                MemoryContext _oldctx = MemoryContextSwitchTo(CacheMemoryContext);
                biscuit_complete_preload_local(so->index, indexoid);
                MemoryContextSwitchTo(_oldctx);
            }
            /* else: still warming, rescan() will use fallback */
        }
        index->rd_amcache = so->index;
    }
    else
    {
        so->index = biscuit_load_skeleton(index);
        so->index->preload_state = BISCUIT_PRELOAD_SKELETON;
        index->rd_amcache = so->index;
        biscuit_register_callback();
        biscuit_cache_insert(indexoid, so->index);
        biscuit_preload_request(indexoid);

        elog(DEBUG1,
             "Biscuit: skeleton loaded for %u (%d records), bitmaps pending",
             indexoid, so->index->num_records);
    }

    so->results            = NULL;
    so->num_results        = 0;
    so->current            = 0;
    so->is_aggregate_only  = false;
    so->needs_sorted_access = true;
    so->limit_remaining    = -1;

    scan->opaque = so;
    return scan;
}

/* ================================================================
 * SECTION 2 – Multi-column rescan helper  (bitmap path)
 *
 * Only reached when preload_state == BISCUIT_PRELOAD_DONE.
 * ================================================================ */

/*
 * biscuit_rescan_multicolumn
 *
 * scan is passed through so the function can dispatch to the parallel
 * or single-threaded TID-collection path exactly as the single-column
 * fast path in biscuit_rescan() does.  See that path for the full
 * rationale.
 */
static void
biscuit_rescan_multicolumn(IndexScanDesc scan,
                           ScanKey keys, int nkeys,
                           ScanKey orderbys, int norderbys)
{
    BiscuitScanOpaque *so = (BiscuitScanOpaque *) scan->opaque;
    RoaringBitmap     *candidates;
    QueryPlan         *plan;
    bool               needs_sorting;
    int                i;

    needs_sorting = so->needs_sorted_access;

    /* Start with all records as candidates */
    candidates = biscuit_roaring_create();
#ifdef HAVE_ROARING
    roaring_bitmap_add_range(candidates, 0, so->index->num_records);
#else
    for (i = 0; i < so->index->num_records; i++)
        biscuit_roaring_add(candidates, i);
#endif

    /* Filter tombstones */
    if (so->index->tombstone_count > 0)
        biscuit_roaring_andnot_inplace(candidates, so->index->tombstones);

    /* Build optimized query plan */
    plan = biscuit_build_query_plan(so->index, keys, nkeys);
    if (!plan)
        goto cleanup;

    /* Apply each predicate in order of selectivity */
    for (i = 0; i < plan->count; i++)
    {
        QueryPredicate *pred            = &plan->predicates[i];
        int             pred_strategy   = pred->scan_key->sk_strategy;
        bool            pred_is_not_like = (pred_strategy == BISCUIT_NOT_LIKE_STRATEGY ||
                                            pred_strategy == BISCUIT_NOT_ILIKE_STRATEGY);
        bool            pred_is_ilike   = (pred_strategy == BISCUIT_ILIKE_STRATEGY ||
                                           pred_strategy == BISCUIT_NOT_ILIKE_STRATEGY);
        RoaringBitmap  *col_result;

        if (pred->column_index < 0 || pred->column_index >= so->index->num_columns)
            continue;

        col_result = pred_is_ilike
            ? biscuit_query_column_pattern_ilike(so->index, pred->column_index, pred->pattern)
            : biscuit_query_column_pattern(so->index, pred->column_index, pred->pattern);

        if (!col_result)
            col_result = biscuit_roaring_create();

        if (pred_is_not_like)
        {
            /*
             * NOT LIKE inversion: the "all" set must contain only non-null
             * indexed rows for this column.  Use the column's length_ge
             * bitmap at index 0 (all strings of length >= 0, i.e. non-null)
             * when available, otherwise fall back to the full record range.
             */
            ColumnIndex   *pred_col    = &so->index->column_indices[pred->column_index];
            RoaringBitmap *all;
            int j;
            if (pred_col->length_ge_bitmaps && pred_col->length_ge_bitmaps[0])
            {
                all = biscuit_roaring_copy(pred_col->length_ge_bitmaps[0]);
            }
            else
            {
                all = biscuit_roaring_create();
#ifdef HAVE_ROARING
                roaring_bitmap_add_range(all, 0, so->index->num_records);
#else
                for (j = 0; j < so->index->num_records; j++)
                    biscuit_roaring_add(all, j);
#endif
            }
            if (so->index->tombstone_count > 0 && so->index->tombstones)
                biscuit_roaring_andnot_inplace(all, so->index->tombstones);
            biscuit_roaring_andnot_inplace(all, col_result);
            biscuit_roaring_free(col_result);
            col_result = all;
        }

        biscuit_roaring_and_inplace(candidates, col_result);
        biscuit_roaring_free(col_result);

        if (biscuit_roaring_count(candidates) == 0)
            break;
    }

    /* Parallel-aware TID collection — same as single-column fast path. */
    {
        BiscuitParallelScanDesc *pdesc = NULL;

        if (scan->parallel_scan != NULL)
        {
            pdesc = (BiscuitParallelScanDesc *)
                        OffsetToPointer(scan->parallel_scan,
                                        scan->parallel_scan->ps_offset_am);
        }

        biscuit_collect_sorted_tids_parallel(
            so->index, candidates, pdesc,
            &so->results, &so->num_results,
            needs_sorting);
    }

    biscuit_roaring_free(candidates);

cleanup:
    biscuit_free_query_plan(plan);
}

/* ================================================================
 * SECTION 2b – Multi-column fallback rescan  (no bitmaps)
 *
 * Used while preload_state < BISCUIT_PRELOAD_DONE.  Runs one
 * biscuit_fallback_scan() per scan key and AND-reduces the TID sets.
 * ================================================================ */

static void
biscuit_rescan_multicolumn_fallback(IndexScanDesc scan,
                                    ScanKey keys, int nkeys,
                                    ScanKey orderbys, int norderbys)
{
    BiscuitScanOpaque *so = (BiscuitScanOpaque *) scan->opaque;
    bool               needs_sorting = so->needs_sorted_access;
    int                i;
    int                n             = so->index->num_records;
    uint32            *tid_map       = NULL;
    int                map_size      = 0;

    /*
     * Build a TID → record-index hash map once (O(n)) so the per-TID
     * reverse-lookup below is O(1) instead of O(num_records).
     */
    if (n > 0)
    {
        int k;
        map_size = 1;
        while (map_size < 2 * n)
            map_size <<= 1;
        tid_map = (uint32 *) palloc0(map_size * sizeof(uint32));

        for (k = 0; k < n; k++)
        {
            BlockNumber  blk    = ItemPointerGetBlockNumber(&so->index->tids[k]);
            OffsetNumber off    = ItemPointerGetOffsetNumber(&so->index->tids[k]);
            uint32       h      = ((uint32)blk * 2654435761u) ^ (uint32)off;
            int          bucket = (int)(h & (uint32)(map_size - 1));
            while (tid_map[bucket] != 0)
                bucket = (bucket + 1) & (map_size - 1);
            tid_map[bucket] = (uint32)(k + 1);
        }
    }

    /*
     * We need the intersection of per-key TID sets.  Use a RoaringBitmap
     * over record indices to compute the AND, then collect TIDs once.
     */
    {
        RoaringBitmap *candidates = biscuit_roaring_create();

#ifdef HAVE_ROARING
        roaring_bitmap_add_range(candidates, 0, (uint64_t) n);
#else
        for (i = 0; i < n; i++)
            biscuit_roaring_add(candidates, i);
#endif

        for (i = 0; i < nkeys; i++)
        {
            ScanKey          key = &keys[i];
            text            *pattern_text;
            char            *pattern;
            bool             is_ilike;
            bool             is_not;
            int              col_idx;
            ItemPointerData *key_tids   = NULL;
            int              key_count  = 0;
            RoaringBitmap   *key_bitmap;
            int              j;

            if (key->sk_flags & SK_ISNULL)
                continue;

            col_idx = (so->index->num_columns > 1)
                      ? (int)(key->sk_attno - 1)
                      : 0;

            if (col_idx < 0 || col_idx >= so->index->num_columns)
                continue;

            

            pattern_text = DatumGetTextPP(key->sk_argument);
            pattern      = text_to_cstring(pattern_text);

            biscuit_fallback_scan(so->index, pattern, is_ilike, col_idx,
                                  &key_tids, &key_count);
            pfree(pattern);

            /* O(1) TID → record-index lookup via hash map */
            key_bitmap = biscuit_roaring_create();
            for (j = 0; j < key_count; j++)
            {
                BlockNumber  blk    = ItemPointerGetBlockNumber(&key_tids[j]);
                OffsetNumber off    = ItemPointerGetOffsetNumber(&key_tids[j]);
                uint32       h      = ((uint32)blk * 2654435761u) ^ (uint32)off;
                int          bucket;

                if (!tid_map || map_size == 0)
                    break;

                bucket = (int)(h & (uint32)(map_size - 1));
                while (tid_map[bucket] != 0)
                {
                    uint32 recidx = tid_map[bucket] - 1;
                    ItemPointerData *t = &so->index->tids[recidx];
                    if (ItemPointerGetBlockNumber(t) == blk &&
                        ItemPointerGetOffsetNumber(t) == off)
                    {
                        biscuit_roaring_add(key_bitmap, recidx);
                        break;
                    }
                    bucket = (bucket + 1) & (map_size - 1);
                }
            }
            if (key_tids)
                pfree(key_tids);

            is_ilike = (key->sk_strategy == BISCUIT_ILIKE_STRATEGY ||
                        key->sk_strategy == BISCUIT_NOT_ILIKE_STRATEGY);
            is_not   = (key->sk_strategy == BISCUIT_NOT_LIKE_STRATEGY ||
                        key->sk_strategy == BISCUIT_NOT_ILIKE_STRATEGY);

            if (is_not)
            {
                RoaringBitmap *all = biscuit_roaring_create();
                int k;
                /* Only invert over non-null indexed records for this column */
                if (col_idx >= 0 && col_idx < so->index->num_columns &&
                    so->index->column_data_cache && so->index->column_data_cache[col_idx])
                {
                    for (k = 0; k < n; k++)
                    {
                        if (so->index->column_data_cache[col_idx][k])
                            biscuit_roaring_add(all, k);
                    }
                }
                else
                {
                    for (k = 0; k < n; k++)
                        biscuit_roaring_add(all, k);
                }
                /* Exclude tombstones before inverting. */
                if (so->index->tombstone_count > 0 && so->index->tombstones)
                    biscuit_roaring_andnot_inplace(all, so->index->tombstones);
                biscuit_roaring_andnot_inplace(all, key_bitmap);
                biscuit_roaring_free(key_bitmap);
                key_bitmap = all;
            }

            biscuit_roaring_and_inplace(candidates, key_bitmap);
            biscuit_roaring_free(key_bitmap);

            if (biscuit_roaring_count(candidates) == 0)
                break;
        }

        /* Filter tombstones */
        if (so->index->tombstone_count > 0 && so->index->tombstones)
            biscuit_roaring_andnot_inplace(candidates, so->index->tombstones);

        biscuit_collect_sorted_tids_single(so->index, candidates,
                                             &so->results, &so->num_results,
                                             needs_sorting);

        biscuit_roaring_free(candidates);
    }

    if (tid_map)
        pfree(tid_map);
}

/* ================================================================
 * SECTION 3 – rescan
 *
 * Key change: if preload_state < BISCUIT_PRELOAD_DONE (bitmaps not yet
 * built) we use biscuit_fallback_scan() instead of the bitmap query
 * functions.  Once the background worker finishes, subsequent scans
 * automatically use the fast bitmap path.
 * ================================================================ */

void
biscuit_rescan(IndexScanDesc scan,
               ScanKey keys, int nkeys,
               ScanKey orderbys, int norderbys)
{
    BiscuitScanOpaque *so = (BiscuitScanOpaque *) scan->opaque;
    bool               is_aggregate;
    bool               needs_sorting;
    bool               bitmaps_ready;

    if (so->results)
    {
        pfree(so->results);
        so->results = NULL;
    }
    so->num_results = 0;
    so->current     = 0;

    if (!so->index || nkeys == 0 || so->index->num_records == 0)
        return;

    /*
     * Count this as one index search.  PostgreSQL 17+ tracks this counter
     * in IndexScanDescData and displays it as "Index Searches" in EXPLAIN
     * ANALYSE.  The AM is responsible for incrementing it; genam.h does not
     * do so automatically.  Increment here, after the early-return guards,
     * so only real searches are counted.
     */
#if PG_VERSION_NUM >= 180000
    if (scan->instrument)
        scan->instrument->nsearches++;
#elif PG_VERSION_NUM >= 170000
    scan->xs_numIndexSearches++;
#endif

    is_aggregate   = biscuit_is_aggregate_query(scan);
    needs_sorting  = !is_aggregate;

    so->is_aggregate_only   = is_aggregate;
    so->needs_sorted_access = needs_sorting;
    so->limit_remaining     = -1;

    /*
     * Check whether the background worker has finished building the bitmaps.
     *
     * We first consult the in-process preload_state field which is the
     * cheapest check (no IPC).  If that still shows an incomplete state we
     * also probe the shared-memory ring buffer in case the worker finished
     * after this session's last cache lookup.
     */
    bitmaps_ready = (so->index->preload_state >= BISCUIT_PRELOAD_DONE);

    if (!bitmaps_ready)
    {
        uint32 shmem_state = biscuit_preload_state(RelationGetRelid(scan->indexRelation));

        if (shmem_state >= BISCUIT_PRELOAD_DONE)
        {
            /*
             * The worker has finished building bitmaps.  Rather than calling
             * biscuit_load_index() (which would do a full heap scan again),
             * we build the bitmaps directly from the skeleton that this
             * session already holds in so->index->data_cache.
             *
             * biscuit_complete_preload_local() is O(total-string-bytes) with
             * zero extra I/O.  It sets idx->preload_state = DONE and updates
             * the shmem slot so other sessions benefit too.
             *
             * This path executes at most once per session (the first query
             * after the background worker signals DONE).
             */
            {
                MemoryContext _oldctx = MemoryContextSwitchTo(CacheMemoryContext);
                biscuit_complete_preload_local(so->index,
                                               RelationGetRelid(scan->indexRelation));
                MemoryContextSwitchTo(_oldctx);
            }

            /* Refresh rd_amcache so subsequent beginscan hits this copy */
            scan->indexRelation->rd_amcache = so->index;
            biscuit_cache_insert(RelationGetRelid(scan->indexRelation), so->index);

            bitmaps_ready = true;

            elog(DEBUG1,
                 "Biscuit: local bitmap build done for %u after preload signal",
                 RelationGetRelid(scan->indexRelation));
        }
        else if (shmem_state == BISCUIT_PRELOAD_FAILED)
        {
            elog(DEBUG1,
                 "Biscuit: preload worker failed for index %u; "
                 "using sequential fallback scan (may be slow)",
                 RelationGetRelid(scan->indexRelation));
        }
    }

    /* ---------------------------------------------------------------
     * FAST PATH – bitmaps are available
     * --------------------------------------------------------------- */
    elog(DEBUG1, "Entering Checkpoint - 1: 559");
    if (bitmaps_ready)
    {
        if (so->index->num_columns > 1)
        {
            biscuit_rescan_multicolumn(scan, keys, nkeys, orderbys, norderbys);
        }
        else
        {
            /* ---- Single-column: AND all key results ---- */
            RoaringBitmap *result = NULL;
            int            i;

            for (i = 0; i < nkeys; i++)
            {
                ScanKey        key = &keys[i];
                text          *pattern_text;
                char          *pattern;
                RoaringBitmap *key_result;
                bool           is_not;

                if (key->sk_flags & SK_ISNULL)
                    continue;

                

                pattern_text = DatumGetTextPP(key->sk_argument);
                pattern      = text_to_cstring(pattern_text);

                switch (key->sk_strategy)
                {
                    case BISCUIT_LIKE_STRATEGY:
                        key_result = biscuit_query_column_pattern(so->index, 0, pattern);
                        break;
                    case BISCUIT_NOT_LIKE_STRATEGY:
                        key_result = biscuit_query_column_pattern(so->index, 0, pattern);
                        break;
                    case BISCUIT_ILIKE_STRATEGY:
                        key_result = biscuit_query_column_pattern_ilike(so->index, 0, pattern);
                        break;
                    case BISCUIT_NOT_ILIKE_STRATEGY:
                        key_result = biscuit_query_column_pattern_ilike(so->index, 0, pattern);
                        break;

                    default:
                        elog(ERROR, "Biscuit: unsupported scan strategy %d",
                             key->sk_strategy);
                        pfree(pattern);
                        continue;
                }

                pfree(pattern);

                if (!key_result)
                {
                    if (result) biscuit_roaring_free(result);
                    return;
                }
                
                is_not = (key->sk_strategy == BISCUIT_NOT_LIKE_STRATEGY ||
                          key->sk_strategy == BISCUIT_NOT_ILIKE_STRATEGY);
                          
                if (is_not)
                {
                    /*
                     * NOT LIKE inversion: build the live non-null set.
                     * We must NOT include records with a NULL data_cache
                     * entry — those rows have NULL column values and were
                     * never indexed, so they must not appear in NOT LIKE
                     * results (NULL LIKE x is NULL, not TRUE).
                     * Use length_ge_bitmaps_legacy[0] when available (it
                     * was built to contain exactly the non-null live rows),
                     * otherwise fall back to a record-by-record scan.
                     */
                    RoaringBitmap *all;
                    int j;
                    if (so->index->length_ge_bitmaps_legacy &&
                        so->index->length_ge_bitmaps_legacy[0])
                    {
                        all = biscuit_roaring_copy(
                            so->index->length_ge_bitmaps_legacy[0]);
                    }
                    else
                    {
                        all = biscuit_roaring_create();
                        for (j = 0; j < so->index->num_records; j++)
                        {
                            if (so->index->data_cache[j])
                                biscuit_roaring_add(all, j);
                        }
                    }
                    if (so->index->tombstone_count > 0 && so->index->tombstones)
                        biscuit_roaring_andnot_inplace(all, so->index->tombstones);
                    biscuit_roaring_andnot_inplace(all, key_result);
                    biscuit_roaring_free(key_result);
                    key_result = all;
                }
                if (!result)
                    result = key_result;
                else
                {
                    biscuit_roaring_and_inplace(result, key_result);
                    biscuit_roaring_free(key_result);

                    if (biscuit_roaring_is_empty(result))
                    {
                        biscuit_roaring_free(result);
                        return;
                    }
                }
            }

            if (!result)
                return;

            /* Filter tombstones (for non-NOT-LIKE keys that may remain) */
            if (so->index->tombstone_count > 0)
                biscuit_roaring_andnot_inplace(result, so->index->tombstones);

            /*
             * Parallel-aware TID collection.
             *
             * When scan->parallel_scan is set, the Gather node has launched
             * background workers that will each call biscuit_rescan()
             * independently on their own private IndexScanDesc.  Without
             * coordination every participant evaluates the full bitmap and
             * returns the full TID set, causing N× row duplication.
             *
             * Fix: biscuit_collect_sorted_tids_parallel() handles this
             * transparently.  Every participant (leader and workers alike)
             * evaluates the bitmap locally — the result is identical for all
             * because the bitmap and index data are read-only and deterministic.
             * An atomic CAS on pdesc->next_chunk elects exactly one initializer
             * which writes total_tids/total_chunks into the shared DSM
             * descriptor; the others spin-wait.  Then each participant atomically
             * claims a disjoint chunk range and returns only those TIDs to its
             * local Gather feeder — so the Gather node assembles exactly one
             * copy of the full result.
             *
             * When scan->parallel_scan is NULL the function is identical to
             * biscuit_collect_sorted_tids_single().
             */
             elog(DEBUG1, "Entering Checkpoint - 3: 700");
            {
                BiscuitParallelScanDesc *pdesc = NULL;
                elog(DEBUG1, "Entering Checkpoint - 2: 702");
                if (scan->parallel_scan != NULL)
                    pdesc = (BiscuitParallelScanDesc *)
                                OffsetToPointer(scan->parallel_scan,
                                                scan->parallel_scan->ps_offset_am);
              
                biscuit_collect_sorted_tids_parallel(
                    so->index, result, pdesc,
                    &so->results, &so->num_results,
                    needs_sorting);
            }

            biscuit_roaring_free(result);
        }

        return; /* fast path done */
    }

    /* ---------------------------------------------------------------
     * FALLBACK PATH – bitmaps not yet ready, use sequential scan
     *
     * biscuit_fallback_scan() walks data_cache (single-column) or
     * column_data_cache (multi-column) directly.  It is correct but
     * O(n) per scan key with no bitmap acceleration.  This path is
     * only active during the brief warm-up window after the first
     * query after a restart.
     * --------------------------------------------------------------- */
        /*
         * FALLBACK PATH – bitmaps not yet ready, use sequential scan
         *
         * biscuit_fallback_scan() walks data_cache (single-column) or
         * column_data_cache (multi-column) directly.  It is correct but
         * O(n) per scan key with no bitmap acceleration.  This path is
         * only active during the brief warm-up window after the first
         * query after a restart.
         *
         * Converting the TID list returned by biscuit_fallback_scan() into
         * a record-index bitmap was previously O(key_count * num_records)
         * per key — quadratic for large indexes.  We build a tid→record-index
         * hash map once before the key loop (O(n)) and use it for O(1)
         * lookups per matched TID.
         */
        elog(DEBUG1,
             "Biscuit: index %u not yet warm (state=%u); using fallback scan",
             RelationGetRelid(scan->indexRelation),
             so->index->preload_state);

        if (so->index->num_columns > 1)
        {
            /*
             * Multi-column fallback: handled by the dedicated helper which
             * computes the AND across all keys using the fallback scanner.
             */
            biscuit_rescan_multicolumn_fallback(scan, keys, nkeys,
                                                orderbys, norderbys);
        }
        else
        {
            /*
             * Single-column fallback: run one biscuit_fallback_scan() per
             * key and intersect the results.
             *
             * Build a TID → record-index hash map (open addressing, power-of-2
             * table) to avoid the O(num_records) linear search per matched TID.
             */
            int            n          = so->index->num_records;
            uint32        *tid_map    = NULL; /* tid_map[bucket] = recidx+1, 0=empty */
            int            map_size   = 0;

            /* Smallest power-of-two >= 2*n (load factor ~0.5) */
            if (n > 0)
            {
                map_size = 1;
                while (map_size < 2 * n)
                    map_size <<= 1;
                tid_map = (uint32 *) palloc0(map_size * sizeof(uint32));

                {
                    int k;
                    for (k = 0; k < n; k++)
                    {
                        BlockNumber blk  = ItemPointerGetBlockNumber(&so->index->tids[k]);
                        OffsetNumber off  = ItemPointerGetOffsetNumber(&so->index->tids[k]);
                        uint32       h    = ((uint32)blk * 2654435761u) ^ (uint32)off;
                        int          bucket = (int)(h & (uint32)(map_size - 1));
                        while (tid_map[bucket] != 0)
                            bucket = (bucket + 1) & (map_size - 1);
                        tid_map[bucket] = (uint32)(k + 1); /* store recidx+1 */
                    }
                }
            }

            {
                RoaringBitmap *candidates = biscuit_roaring_create();
                int            i;

#ifdef HAVE_ROARING
                roaring_bitmap_add_range(candidates, 0, (uint64_t) n);
#else
                for (i = 0; i < n; i++)
                    biscuit_roaring_add(candidates, i);
#endif

                for (i = 0; i < nkeys; i++)
                {
                    ScanKey          key = &keys[i];
                    text            *pattern_text;
                    char            *pattern;
                    bool             is_ilike;
                    bool             is_not;
                    ItemPointerData *key_tids  = NULL;
                    int              key_count = 0;
                    RoaringBitmap   *key_bitmap;
                    int              j;

                    if (key->sk_flags & SK_ISNULL)
                        continue;

                    is_ilike = (key->sk_strategy == BISCUIT_ILIKE_STRATEGY ||
                                key->sk_strategy == BISCUIT_NOT_ILIKE_STRATEGY);
                    is_not   = (key->sk_strategy == BISCUIT_NOT_LIKE_STRATEGY ||
                                key->sk_strategy == BISCUIT_NOT_ILIKE_STRATEGY);

                    pattern_text = DatumGetTextPP(key->sk_argument);
                    pattern      = text_to_cstring(pattern_text);

                    /* col_idx = 0 for single-column */
                    biscuit_fallback_scan(so->index, pattern, is_ilike, 0,
                                          &key_tids, &key_count);
                    pfree(pattern);

                    /* Convert TID list to a record-index bitmap via hash map */
                    key_bitmap = biscuit_roaring_create();
                    for (j = 0; j < key_count; j++)
                    {
                        BlockNumber blk  = ItemPointerGetBlockNumber(&key_tids[j]);
                        OffsetNumber off  = ItemPointerGetOffsetNumber(&key_tids[j]);
                        uint32       h    = ((uint32)blk * 2654435761u) ^ (uint32)off;
                        int          bucket;

                        if (!tid_map || map_size == 0)
                            break;  /* no records */

                        bucket = (int)(h & (uint32)(map_size - 1));
                        while (tid_map[bucket] != 0)
                        {
                            uint32 recidx = tid_map[bucket] - 1;
                            ItemPointerData *t = &so->index->tids[recidx];
                            if (ItemPointerGetBlockNumber(t) == blk &&
                                ItemPointerGetOffsetNumber(t) == off)
                            {
                                biscuit_roaring_add(key_bitmap, recidx);
                                break;
                            }
                            bucket = (bucket + 1) & (map_size - 1);
                        }
                    }
                    if (key_tids)
                        pfree(key_tids);

                    if (is_not)
                    {
                        RoaringBitmap *all = biscuit_roaring_create();
                        int k;
                        /* Only invert over non-null indexed records */
                        for (k = 0; k < n; k++)
                        {
                            if (so->index->data_cache[k])
                                biscuit_roaring_add(all, k);
                        }
                        if (so->index->tombstone_count > 0 && so->index->tombstones)
                            biscuit_roaring_andnot_inplace(all, so->index->tombstones);
                        biscuit_roaring_andnot_inplace(all, key_bitmap);
                        biscuit_roaring_free(key_bitmap);
                        key_bitmap = all;
                    }

                    biscuit_roaring_and_inplace(candidates, key_bitmap);
                    biscuit_roaring_free(key_bitmap);

                    if (biscuit_roaring_count(candidates) == 0)
                        break;
                }

                /* Filter tombstones (covers non-NOT-LIKE keys) */
                if (so->index->tombstone_count > 0 && so->index->tombstones)
                    biscuit_roaring_andnot_inplace(candidates, so->index->tombstones);

                /*biscuit_collect_sorted_tids_single(so->index, candidates,
                                                     &so->results, &so->num_results,
                                                     needs_sorting);*/
               
                {
                    BiscuitParallelScanDesc *pdesc = NULL;
                    elog(DEBUG1, "Entering Checkpoint - 2: 702");
                    if (scan->parallel_scan != NULL)
                        pdesc = (BiscuitParallelScanDesc *)
                                    OffsetToPointer(scan->parallel_scan,
                                                    scan->parallel_scan->ps_offset_am);
                  
                     biscuit_collect_sorted_tids_parallel(
                        so->index, candidates, pdesc,
                        &so->results, &so->num_results,
                        needs_sorting);
                }
                biscuit_roaring_free(candidates);
            }

            if (tid_map)
                pfree(tid_map);
        }
    }

/* ================================================================
 * SECTION 4 – gettuple
 * ================================================================ */

bool
biscuit_gettuple(IndexScanDesc scan, ScanDirection dir)
{
    BiscuitScanOpaque *so = (BiscuitScanOpaque *) scan->opaque;

    if (so->current >= so->num_results)
        return false;

    scan->xs_heaptid = so->results[so->current];
    scan->xs_recheck = false;
    so->current++;

    if (so->limit_remaining > 0)
        so->limit_remaining--;

    return true;
}

/* ================================================================
 * SECTION 5 – getbitmap  (aggregate / bitmap-heap scans)
 * ================================================================ */

int64
biscuit_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
    BiscuitScanOpaque *so        = (BiscuitScanOpaque *) scan->opaque;
    int64              ntids     = 0;
    const int          chunk_size = 10000;
    int                i;

    if (so->num_results > 0)
    {
        bool recheck = false;

        if (so->num_results > chunk_size)
        {
            for (i = 0; i < so->num_results; i += chunk_size)
            {
                int batch = Min(chunk_size, so->num_results - i);
                tbm_add_tuples(tbm, &so->results[i], batch, recheck);
                ntids += batch;
                CHECK_FOR_INTERRUPTS();
            }
        }
        else
        {
            tbm_add_tuples(tbm, so->results, so->num_results, recheck);
            ntids = so->num_results;
        }
    }

    return ntids;
}

/* ================================================================
 * SECTION 6 – endscan
 * ================================================================ */

void
biscuit_endscan(IndexScanDesc scan)
{
    BiscuitScanOpaque *so = (BiscuitScanOpaque *) scan->opaque;

    if (so)
    {
        if (so->results)
            pfree(so->results);
        pfree(so);
    }
    
    elog(DEBUG1, "Exited scan");
}
