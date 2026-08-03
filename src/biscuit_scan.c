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
 * Cache-based load strategy
 * -------------------------
 * beginscan() resolves the index purely through the session-scoped
 * biscuit_cache (biscuit_cache.c):
 *
 *  1. Cache hit  – the fully-built BiscuitIndex (TIDs, data caches, and
 *                  every bitmap) is reused immediately.
 *
 *  2. Cache miss – biscuit_load_index() reads the complete index (all
 *                  bitmaps included) directly from its on-disk page
 *                  directory (compacted blobs) in one synchronous pass,
 *                  inserts it into biscuit_cache, and returns it for
 *                  immediate use.
 *
 * Every scan therefore always has bitmaps available; there is no
 * warm-up window and no separate fallback scan path.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_cache.h"
#include "biscuit_pattern.h"
#include "biscuit_tid.h"
#include "biscuit_index.h"
#include "biscuit_scan.h"

/* ================================================================
 * SECTION 1 – beginscan
 *
 * On a cache miss, biscuit_load_index() reads the complete index
 * (data caches, all bitmaps) synchronously from its on-disk page
 * directory and inserts it into biscuit_cache.
 * ================================================================ */

IndexScanDesc
biscuit_beginscan(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc      scan;
    BiscuitScanOpaque *so;
    Oid                indexoid = RelationGetRelid(index);

    scan = RelationGetIndexScan(index, nkeys, norderbys);
    so   = (BiscuitScanOpaque *) palloc(sizeof(BiscuitScanOpaque));

    /*
     * Resolve through the (per-backend) biscuit_cache, but never trust a
     * cache hit blindly -- biscuit_get_current_index() compares the cached
     * copy's generation against the metapage's authoritative counter and
     * transparently reloads from disk if another backend has committed a
     * mutation since this backend's copy was built. See its header comment
     * and BLOCKER-1 in the GA report: without this check, a backend that
     * primed its cache before a concurrent INSERT committed would keep
     * returning stale (under-counted) results indefinitely.
     *
     * index->rd_amcache is intentionally never read or written here:
     * PostgreSQL pfree()s rd_amcache on relcache invalidation, and since
     * biscuit_cache holds the same object, that pfree would free memory
     * the cache still references — a use-after-free on the next lookup.
     * biscuit_cache (with its relcache callback in biscuit_cache.c) is the
     * single source of truth for this object's lifetime.
     */
    so->index = biscuit_get_current_index(index);

    elog(DEBUG1,
         "Entered beginscan() (index %u, %d records)",
         indexoid, so->index->num_records);

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

    (void) orderbys;
    (void) norderbys;

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

        /*
         * FIX 1: pass the running row-candidate set as a mask. Row
         * indices are shared across columns in this index, so a mask
         * built from an earlier predicate on a *different* column is
         * still a valid restriction here -- only the rows still alive
         * in `candidates` can possibly survive the final AND anyway.
         * (For NOT LIKE/NOT ILIKE this computes the *positive* match
         * restricted to the mask, which is inverted below; that's still
         * correct since a positive match outside the mask would only
         * ever get discarded, not added, by the subsequent AND.)
         */
        col_result = pred_is_ilike
            ? biscuit_query_column_pattern_ilike_masked(scan->indexRelation, so->index,
                                                          pred->column_index, pred->pattern, candidates)
            : biscuit_query_column_pattern_masked(scan->indexRelation, so->index,
                                                    pred->column_index, pred->pattern, candidates);

        if (!col_result)
            col_result = biscuit_roaring_create();

        if (pred_is_not_like)
        {
            /*
             * NOT LIKE / NOT ILIKE inversion: the "all" set must contain
             * only non-null indexed rows for this column. Use the
             * column's length_ge bitmap at index 0 (all strings of
             * length >= 0, i.e. non-null) when available, otherwise fall
             * back to the full record range.
             *
             * Which length_ge array to consult depends on the strategy:
             * NOT LIKE needs the case-sensitive set, NOT ILIKE needs the
             * case-insensitive ("_lower") set -- these are gated
             * independently per column by the column's opclass (see
             * biscuit_get_column_case_mode()), so a column built with
             * only one of the two structure sets only ever has the
             * matching array populated.
             */
            ColumnIndex   *pred_col = &so->index->column_indices[pred->column_index];
            RoaringBitmap *all;

            /*
             * Reconciled against the shared pending log -- reading
             * length_ge_bitmaps[0] raw here (as this did) silently drops
             * every row whose membership is still undrained, which after
             * freelist reuse meant every recycled row vanished from the
             * complement. See biscuit_get_negation_base_set().
             */
            all = biscuit_get_negation_base_set(scan->indexRelation, pred_col,
                                                 pred->column_index, pred_is_ilike,
                                                 so->index->num_records);

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
                                        BISCUIT_PARALLEL_AM_OFFSET(scan->parallel_scan));
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
 * SECTION 3 – rescan
 *
 * so->index always has every bitmap built (see beginscan), so rescan
 * always takes the bitmap query path below.
 * ================================================================ */

void
biscuit_rescan(IndexScanDesc scan,
               ScanKey keys, int nkeys,
               ScanKey orderbys, int norderbys)
{
    BiscuitScanOpaque *so = (BiscuitScanOpaque *) scan->opaque;
    bool               is_aggregate;
    bool               needs_sorting;

    if (so->results)
    {
        pfree(so->results);
        so->results = NULL;
    }
    so->num_results = 0;
    so->current     = 0;

    /*
     * Same staleness check as biscuit_beginscan() (BLOCKER-1): a scan node
     * can survive across many rescans (e.g. the inner side of a
     * parameterized nested loop), and a plain DML commit in another
     * backend never invalidates our cache entry, so re-check on every
     * rescan rather than only once at beginscan time.
     */
    so->index = biscuit_get_current_index(scan->indexRelation);

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
#endif

    is_aggregate   = biscuit_is_aggregate_query(scan);
    needs_sorting  = !is_aggregate;

    so->is_aggregate_only   = is_aggregate;
    so->needs_sorted_access = needs_sorting;
    so->limit_remaining     = -1;

    if (so->index->num_columns > 1)
    {
        biscuit_rescan_multicolumn(scan, keys, nkeys, orderbys, norderbys);
    }
    else
    {
            /*
             * ---- Single-column: AND all key results ----
             *
             * FIX 3: reuse the same cost-based ordering as the
             * multi-column path (biscuit_build_query_plan() sorts
             * predicates by selectivity_score, most selective first) so
             * an anchored key (usr\_1234\_%, ~26 rows) runs before an
             * infix key (%abcdef%) rather than in whatever order
             * Postgres happened to hand us the keys.
             *
             * FIX 1: thread the running candidate set into each
             * subsequent key evaluation via *_masked() instead of
             * computing every key's full-table result and ANDing
             * afterward. After the first (cheapest) key narrows the
             * candidate set to ~26 rows, every later key -- including an
             * infix key that would otherwise sweep/verify against the
             * full table -- only has to consider those 26 rows.
             */
            QueryPlan     *plan;
            RoaringBitmap *mask = NULL;   /* running candidate set; NULL == unrestricted */
            int            i;

            plan = biscuit_build_query_plan(so->index, keys, nkeys);
            if (!plan || plan->count == 0)
            {
                if (plan) biscuit_free_query_plan(plan);
                return;
            }

            for (i = 0; i < plan->count; i++)
            {
                QueryPredicate *pred = &plan->predicates[i];
                ScanKey         key  = pred->scan_key;
                RoaringBitmap  *key_result;
                bool            is_not;

                switch (key->sk_strategy)
                {
                    case BISCUIT_LIKE_STRATEGY:
                    case BISCUIT_NOT_LIKE_STRATEGY:
                        key_result = biscuit_query_pattern_masked(scan->indexRelation, so->index,
                                                                    pred->pattern, mask);
                        break;
                    case BISCUIT_ILIKE_STRATEGY:
                    case BISCUIT_NOT_ILIKE_STRATEGY:
                        key_result = biscuit_query_pattern_ilike_masked(scan->indexRelation, so->index,
                                                                          pred->pattern, mask);
                        break;

                    default:
                        elog(ERROR, "Biscuit: unsupported scan strategy %d",
                             key->sk_strategy);
                        continue;
                }

                if (!key_result)
                {
                    if (mask) biscuit_roaring_free(mask);
                    biscuit_free_query_plan(plan);
                    return;
                }

                is_not = (key->sk_strategy == BISCUIT_NOT_LIKE_STRATEGY ||
                          key->sk_strategy == BISCUIT_NOT_ILIKE_STRATEGY);

                if (is_not)
                {
                    /*
                     * NOT LIKE / NOT ILIKE inversion: build the live
                     * non-null set. We must NOT include records with a
                     * NULL data_cache entry — those rows have NULL
                     * column values and were never indexed, so they must
                     * not appear in NOT LIKE results (NULL LIKE x is
                     * NULL, not TRUE).
                     *
                     * Which length_ge array to consult depends on the
                     * strategy: NOT LIKE needs length_ge_bitmaps_legacy
                     * (case-sensitive), NOT ILIKE needs
                     * length_ge_bitmaps_lower (case-insensitive) -- these
                     * are gated independently by this column's opclass
                     * (see biscuit_get_column_case_mode()), so only the
                     * matching array is ever populated for a
                     * LIKE-only/ILIKE-only index. Use length_ge_bitmaps_*
                     * [0] when available (it was built to contain exactly
                     * the non-null live rows), otherwise fall back to a
                     * record-by-record scan over data_cache, which is
                     * always populated regardless of case mode.
                     *
                     * key_result above was computed restricted to `mask`
                     * (positive matches only need to be found within the
                     * current candidate set -- anything outside mask is
                     * discarded by the final AND regardless), so the base
                     * set is restricted to `mask` too before subtracting,
                     * keeping the two sides consistent.
                     */
                    bool           is_ilike_strategy = (key->sk_strategy == BISCUIT_NOT_ILIKE_STRATEGY);
                    RoaringBitmap *all;

                    /* Reconciled -- see biscuit_get_negation_base_set(). */
                    all = biscuit_get_negation_base_set_legacy(scan->indexRelation,
                                                                so->index,
                                                                is_ilike_strategy);

                    if (so->index->tombstone_count > 0 && so->index->tombstones)
                        biscuit_roaring_andnot_inplace(all, so->index->tombstones);
                    if (mask)
                        biscuit_roaring_and_inplace(all, mask);
                    biscuit_roaring_andnot_inplace(all, key_result);
                    biscuit_roaring_free(key_result);
                    key_result = all;
                }

                /*
                 * Defensive AND: *_masked() only guarantees the mask was
                 * applied in the two expensive branches (see
                 * biscuit_query_pattern_masked()'s header comment), so
                 * enforce it here regardless of which branch ran.
                 */
                if (mask)
                {
                    biscuit_roaring_and_inplace(key_result, mask);
                    biscuit_roaring_free(mask);
                }
                mask = key_result;

                if (biscuit_roaring_is_empty(mask))
                {
                    biscuit_roaring_free(mask);
                    biscuit_free_query_plan(plan);
                    return;
                }
            }

            biscuit_free_query_plan(plan);

            {
            RoaringBitmap *result = mask;

            /*
             * result is never NULL here: plan->count > 0 is guaranteed
             * above, every loop iteration either assigns mask (this
             * variable) or returns early, and the unsupported-strategy
             * default case elog(ERROR)s rather than falling through.
             */

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
                                                BISCUIT_PARALLEL_AM_OFFSET(scan->parallel_scan));
              
                biscuit_collect_sorted_tids_parallel(
                    so->index, result, pdesc,
                    &so->results, &so->num_results,
                    needs_sorting);
            }

            biscuit_roaring_free(result);
            }
        }
    }

/* ================================================================
 * SECTION 4 – gettuple
 * ================================================================ */

bool
biscuit_gettuple(IndexScanDesc scan, ScanDirection dir)
{
    BiscuitScanOpaque *so = (BiscuitScanOpaque *) scan->opaque;

    (void) dir;  /* Biscuit always returns results in build order */

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
