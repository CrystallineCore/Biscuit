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
#include "biscuit_pendlog.h"   /* biscuit_pendlog_scan_begin()/_moved()/_end()
                                 * -- the scan-lifetime drain guard the rescan
                                 * retry loop below is built around */
#include "biscuit_scan.h"

/*
 * biscuit.diag_scan_trace -- per-scan candidate accounting.
 *
 * Off by default and PGC_USERSET: it is read-only instrumentation with no
 * effect on results, so a session can turn it on for a reproducer without
 * touching anything another backend sees. Registered in _PG_init()
 * (biscuit.c).
 *
 * The one-shot reachability probe answered its question -- the tombstone
 * filter site is reached -- but it fires on the first scan in a backend,
 * which is always the healthy one. This reports every scan, so a failing
 * round can be diffed against the healthy round before it.
 */
bool biscuit_diag_scan_trace = false;

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

    /*
     * DIAGNOSTIC ONLY (biscuit.diag_scan_trace) -- reader-vs-durable probe.
     *
     * Logs so->index->num_records/gen (what biscuit_get_current_index()
     * decided to serve for this scan) side by side with a fresh,
     * uncached biscuit_read_metadata_from_disk() read taken right here,
     * right now. This distinguishes the two failure shapes a "stuck at
     * build count" report is consistent with, which nothing measured so
     * far tells apart:
     *
     *   - STALE CACHE, NOT RELOADING: this line shows a live num_records
     *     well above so->index->num_records on every single scan for the
     *     affected backend -- i.e. biscuit_get_current_index()'s gen
     *     check is comparing against a live value it never acts on. That
     *     points back at the gen-consistency chain (biscuit_persist_load()
     *     -> biscuit_load_index() -> biscuit_get_current_index()).
     *
     *   - ACTIVELY RELOADING TO A WRONG VALUE: this line shows the live
     *     read *also* at the low value at the moment of the probe (i.e.
     *     the durable metapage itself is briefly or persistently wrong,
     *     not just this backend's cached view of it). That points at the
     *     write path -- something durably regressing meta->num_records --
     *     not at caching/staleness at all.
     *
     * No effect on results either way; this is read-only and gated off by
     * default.
     */
    if (unlikely(biscuit_diag_scan_trace))
    {
        int    live_records = 0, live_columns = 0, live_max_len = 0;
        uint64 live_gen_probe = 0;
        bool   have_live = biscuit_read_metadata_from_disk(index, &live_records,
                                                             &live_columns, &live_max_len,
                                                             &live_gen_probe);

        elog(LOG,
             "biscuit diag_scan_trace: beginscan index %u backend %d -- "
             "cached num_records=%d cached gen=" UINT64_FORMAT
             " || live num_records=%d live gen=" UINT64_FORMAT " (live read %s)",
             indexoid, MyProcPid,
             so->index->num_records, so->index->gen,
             live_records, live_gen_probe,
             have_live ? "ok" : "FAILED");
    }

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
 * biscuit_build_candidates_multicolumn
 *
 * Returns the candidate slot set for this rescan, or NULL when there is
 * nothing to collect. The caller owns the result and must free it.
 *
 * TID COLLECTION DELIBERATELY DOES NOT HAPPEN HERE ANY MORE.
 *
 * This used to end by calling biscuit_collect_sorted_tids_parallel(). It
 * cannot, now that biscuit_rescan() may discard a candidate set and rebuild
 * it (see the drain-guard retry loop there): under a parallel scan, that
 * function atomically claims a disjoint chunk range from the shared
 * descriptor, and claiming twice from one participant would hand Gather a
 * torn result -- some chunks twice, others never. Splitting the build from
 * the collection keeps the retry confined to work that is pure and
 * repeatable, and leaves the one-shot, side-effecting step downstream of it.
 */
static RoaringBitmap *
biscuit_build_candidates_multicolumn(IndexScanDesc scan,
                                     ScanKey keys, int nkeys)
{
    BiscuitScanOpaque *so = (BiscuitScanOpaque *) scan->opaque;
    RoaringBitmap     *candidates;
    QueryPlan         *plan;
    int                i;

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

    biscuit_free_query_plan(plan);
    return candidates;

cleanup:
    biscuit_free_query_plan(plan);
    biscuit_roaring_free(candidates);
    return NULL;
}

/*
 * biscuit_build_candidates_singlecolumn
 *
 * Returns the candidate slot set for this rescan, or NULL when there is
 * nothing to collect. The caller owns the result and must free it.
 *
 * Split out of biscuit_rescan() for the same reason as its multi-column
 * sibling above: the drain-guard retry loop may have to discard a candidate
 * set and rebuild it, and TID collection is not repeatable. See that
 * function's header.
 */
static RoaringBitmap *
biscuit_build_candidates_singlecolumn(IndexScanDesc scan,
                                      ScanKey keys, int nkeys)
{
    BiscuitScanOpaque *so = (BiscuitScanOpaque *) scan->opaque;


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
        return NULL;
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
            return NULL;
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

        /*
         * Per-key candidate cardinality.
         *
         * This is the measurement the series has never taken. Both
         * ends of the scan are now instrumented and both have been
         * silent on failing runs: the tombstone filter reports
         * before == after, and the collection boundary reports
         * candidates in == TIDs out. Two equal counts at both ends
         * mean the candidate set was ALREADY short when it arrived,
         * so the loss is upstream of everything measured so far --
         * inside this loop, where each key's bitmap is built and
         * ANDed into the running mask.
         *
         * Reporting per key rather than per scan matters because a
         * single-key query (which the reproducer runs) collapses to
         * one line, and that line is directly comparable between the
         * healthy round and the failing one. If the number is short
         * here, biscuit_query_pattern_masked() and the reconcile
         * path beneath it own the defect; if it is correct here and
         * the final result is short, the loss is between this point
         * and the tombstone filter, which is a handful of lines.
         */
        if (unlikely(biscuit_diag_scan_trace))
            ereport(WARNING,
                    (errmsg("biscuit: diag key %d strategy %d -> " UINT64_FORMAT " candidate slot(s)",
                            i, (int) key->sk_strategy,
                            biscuit_roaring_count(mask)),
                     errdetail("num_records %d; tombstone_count %d.",
                               so->index->num_records,
                               so->index->tombstone_count)));

        if (biscuit_roaring_is_empty(mask))
        {
            biscuit_roaring_free(mask);
            biscuit_free_query_plan(plan);
            return NULL;
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

    /*
     * Filter tombstones (for non-NOT-LIKE keys that may remain).
     *
     * INSTRUMENTED -- this is the last mutation of the candidate set
     * before TID collection, and the only one on this path that can
     * remove slots without any of the delta-side or collection-side
     * guards seeing it.
     *
     * The guards on either side of it are now known silent while the
     * count still diverges: the pendlog live-subset-of-expanded
     * invariant and the zero-identity counter (biscuit_pendlog.c,
     * biscuit_delta.c) prove the delta contributed every slot it owed,
     * and the invalid-TID ereport plus the out-of-range counter
     * (biscuit_tid.c) prove nothing is discarded during collection.
     * A subtraction sitting between two clean checkpoints is where an
     * omission-only, never-extra loss would have to live.
     *
     * The specific suspicion is a tombstone bitmap that marks slots
     * which are actually live. Note the failure documented at the end
     * of biscuit_bulkdelete(): a STALE tombstone bitmap (marking too
     * few) produces a loud "slot N with no valid TID" error. The
     * opposite skew -- marking too many -- has no such backstop. It
     * subtracts live rows and returns a smaller, entirely plausible
     * answer, which is the shape being hunted.
     *
     * Two numbers are worth having together. `tombstone_count` is a
     * scalar carried in the HEADER blob; the bitmap is a separate
     * durable structure. They are written by the same call but
     * maintained independently -- biscuit_insert()'s UPDATE branch
     * calls biscuit_roaring_remove() on the bitmap without
     * decrementing the counter, for one -- so a divergence between
     * them is itself evidence about which of the two is wrong.
     *
     * This is a diagnostic, not a fix. It fires per scan; drop it once
     * the stage is identified.
     */
    {
        uint64_t card_before = biscuit_roaring_count(result);
        uint64_t card_after;
        uint64_t tomb_card;

        if (so->index->tombstone_count > 0)
            biscuit_roaring_andnot_inplace(result, so->index->tombstones);

        card_after = biscuit_roaring_count(result);
        tomb_card  = so->index->tombstones
                        ? biscuit_roaring_count(so->index->tombstones)
                        : 0;

        if (card_before != card_after || tomb_card != (uint64_t) so->index->tombstone_count)
            ereport(WARNING,
                    (errmsg("biscuit: tombstone filter removed " UINT64_FORMAT
                            " of " UINT64_FORMAT " candidate slot(s)",
                            card_before - card_after, card_before),
                     errdetail("tombstone bitmap holds " UINT64_FORMAT " slot(s); "
                               "tombstone_count scalar reads %d; num_records %d.",
                               tomb_card, so->index->tombstone_count,
                               so->index->num_records),
                     errhint("A bitmap marking more slots than are actually dead "
                             "subtracts live rows silently.")));
    }

    return result;
    }
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

    (void) orderbys;
    (void) norderbys;

    if (nkeys == 0)
        return;

    /*
     * The BLOCKER-1 staleness check (biscuit_get_current_index()) has MOVED
     * into the retry loop below, where it is preceded by
     * biscuit_pendlog_scan_begin(). It cannot stay here: arming the drain
     * guard after resolving the index reopens the exact window the guard
     * exists to close, and a retry that did not re-resolve the index would
     * rebuild the same wrong candidate set forever. It still runs on every
     * rescan, not just at beginscan -- a scan node survives many rescans
     * (the inner side of a parameterized nested loop) and a plain DML commit
     * in another backend never invalidates our cache entry.
     */

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

    /*
     * DRAIN-GUARD RETRY LOOP.
     *
     * Everything below the guard is one atomic-in-effect unit of work: build
     * a candidate set out of bitmaps that all describe the same moment, then
     * turn it into TIDs. The problem this closes is that the second half of
     * that sentence was never true.
     *
     * biscuit_get_current_index() is the only thing that notices another
     * backend has drained the pending log into the blobs, and it runs once,
     * here. biscuit_reconcile_pending() (biscuit_pattern.c) re-reads the log
     * on every bitmap fetch, and one ILIKE 'alpha%' makes six of them. A
     * drain landing between fetch 1 and fetch 2 leaves the remaining fetches
     * reconciling a base that predates the merge against a log that no
     * longer mentions the merged rows -- so those rows are in neither half,
     * every bitmap after the drain is short by exactly the drained set, and
     * the AND across positions collapses to the membership the base had when
     * this backend loaded it. That is why the reported failures land on
     * precisely the CREATE INDEX row count regardless of workload size, why
     * all concurrent readers hit it in the same narrow window, and why it
     * clears by itself: their next beginscan reloads.
     *
     * biscuit_pendlog_scan_begin() records (total_drains, pendlog_draining);
     * every fetch inside the scan compares; biscuit_pendlog_scan_moved()
     * reports. On a divergence the candidate set is not repairable -- it is a
     * mixture of two different indexes -- so it is discarded whole and the
     * loop reloads and rebuilds.
     *
     * ARMED BEFORE biscuit_get_current_index(), NOT AFTER. The two metapage
     * reads cannot be made atomic with respect to each other, and the
     * ordering decides which way the gap fails: arming first can cost one
     * unnecessary rebuild, arming second can miss the drain entirely. See
     * biscuit_pendlog_scan_begin()'s header.
     *
     * RETRY, NOT LOCK. A drain is rare relative to scans; excluding one for
     * the duration of every scan would cost far more than occasionally
     * redoing one. The loop is bounded because an unbounded optimistic retry
     * is a hang: BISCUIT_SCAN_DRAIN_RETRIES drains inside a single candidate
     * build is not a workload, it is a symptom, and a serialization failure
     * the client can retry is a better answer than either a hang or a
     * silently short count.
     */
#define BISCUIT_SCAN_DRAIN_RETRIES 10
    {
        RoaringBitmap *result   = NULL;
        int            attempt;

        for (attempt = 0; ; attempt++)
        {
            bool moved;

            biscuit_pendlog_scan_begin(scan->indexRelation);

            /*
             * Re-resolved on every attempt, not just the first: the whole
             * point of a retry is that the drain we just observed has made
             * so->index stale, and this is what reloads it. meta->gen was
             * bumped by that drain (pendlog_detach() /
             * pendlog_clear_draining(), biscuit_pendlog.c), so the staleness
             * check inside biscuit_get_current_index() fires and the reload
             * is not merely hoped for.
             */
            so->index = biscuit_get_current_index(scan->indexRelation);
            if (!so->index || so->index->num_records == 0)
            {
                biscuit_pendlog_scan_end();
                return;
            }

            /* DIAGNOSTIC ONLY (biscuit.diag_scan_trace) -- see the matching
             * probe in biscuit_beginscan() for what this checks and why. It
             * now fires once per ATTEMPT, which is the more useful shape: a
             * retry line followed by a second probe showing a higher cached
             * num_records is the fix working, end to end, in the log. */
            if (unlikely(biscuit_diag_scan_trace))
            {
                int    live_records = 0, live_columns = 0, live_max_len = 0;
                uint64 live_gen_probe = 0;
                bool   have_live = biscuit_read_metadata_from_disk(scan->indexRelation,
                                                                     &live_records, &live_columns,
                                                                     &live_max_len, &live_gen_probe);

                elog(LOG,
                     "biscuit diag_scan_trace: rescan index %u backend %d attempt %d -- "
                     "cached num_records=%d cached gen=" UINT64_FORMAT
                     " || live num_records=%d live gen=" UINT64_FORMAT " (live read %s)",
                     RelationGetRelid(scan->indexRelation), MyProcPid, attempt,
                     so->index->num_records, so->index->gen,
                     live_records, live_gen_probe,
                     have_live ? "ok" : "FAILED");
            }

            result = (so->index->num_columns > 1)
                ? biscuit_build_candidates_multicolumn(scan, keys, nkeys)
                : biscuit_build_candidates_singlecolumn(scan, keys, nkeys);

            moved = biscuit_pendlog_scan_moved();
            biscuit_pendlog_scan_end();

            if (!moved)
                break;

            /*
             * Discard unconditionally, including when the build returned
             * NULL. "No candidates" reached under a drain is exactly as
             * untrustworthy as a short count -- an AND against a
             * post-drain-but-pre-reload bitmap can empty the set outright --
             * and treating it as a legitimate zero would turn the loud
             * version of this bug back into the silent one.
             */
            if (result)
            {
                biscuit_roaring_free(result);
                result = NULL;
            }

            if (attempt >= BISCUIT_SCAN_DRAIN_RETRIES)
                ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                         errmsg("biscuit: index scan could not obtain a stable view of index \"%s\"",
                                RelationGetRelationName(scan->indexRelation)),
                         errdetail("The pending log was drained %d times while this scan "
                                   "was building its candidate set.",
                                   attempt + 1),
                         errhint("Retry the statement. Persistent failures suggest "
                                 "biscuit.delta_compaction_slots is set far too low for "
                                 "this write rate.")));

            if (unlikely(biscuit_diag_scan_trace))
                elog(LOG,
                     "biscuit diag_scan_trace: rescan index %u backend %d -- "
                     "rebuilding candidate set after concurrent drain (attempt %d)",
                     RelationGetRelid(scan->indexRelation), MyProcPid, attempt + 1);

            CHECK_FOR_INTERRUPTS();
        }

        if (result == NULL)
            return;

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
         *
         * DELIBERATELY OUTSIDE THE RETRY LOOP. The chunk claim is a
         * side-effecting, one-shot operation against shared DSM state;
         * calling it twice from one participant would hand Gather some chunks
         * twice and others never. Only the pure, repeatable half of the scan
         * is retried.
         *
         * pdesc is currently always NULL -- amcanparallel is false, because
         * the drain guard makes each participant self-consistent but cannot
         * make two participants agree with each other, and this scheme
         * partitions by offset into an array every participant recomputes.
         * See biscuit.c's amcanparallel comment. The split above is kept
         * regardless: it is what a re-enabled parallel path would need, and
         * it costs nothing now.
         */
        {
            BiscuitParallelScanDesc *pdesc = NULL;
            uint64_t                 card_in;

            if (scan->parallel_scan != NULL)
                pdesc = (BiscuitParallelScanDesc *)
                            OffsetToPointer(scan->parallel_scan,
                                            BISCUIT_PARALLEL_AM_OFFSET(scan->parallel_scan));

            card_in = biscuit_roaring_count(result);

            biscuit_collect_sorted_tids_parallel(
                so->index, result, pdesc,
                &so->results, &so->num_results,
                needs_sorting);

            /*
             * Closes the last gap in the chain of custody: candidate
             * slots in, TIDs out, and they must match. Suppressed for a
             * genuine parallel scan, where each participant deliberately
             * returns only its own disjoint slice of the total.
             *
             * Worth stating why this is not redundant with the guards
             * inside biscuit_collect_sorted_tids_single(): those count
             * slots that reached the collection loop and were rejected.
             * This counts everything that entered the function against
             * everything that left it, so it also catches a slot that
             * never reached the loop at all.
             */
            if (pdesc == NULL &&
                (biscuit_diag_scan_trace || card_in != (uint64_t) so->num_results))
                ereport(WARNING,
                        (errmsg("biscuit: TID collection returned %d row(s) for "
                                UINT64_FORMAT " candidate slot(s)",
                                so->num_results, card_in),
                         errhint("Every candidate slot should yield exactly one TID "
                                 "on a non-parallel scan.")));
        }

        biscuit_roaring_free(result);
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
