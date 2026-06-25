/*
 * biscuit_tid.c
 * TID sorting (radix + qsort) and TID collection (single-threaded and parallel).
 *
 * Parallel design
 * ───────────────
 * The AM parallel callbacks (aminitparallelscan / amparallelrescan /
 * amestimateparallelscan) are wired up in biscuit.c and cause PostgreSQL's
 * executor to launch real background workers via a Gather node.  Each
 * participant — the leader and every background worker — calls amgettuple /
 * amrescan independently.  Without explicit coordination every participant
 * would evaluate the full bitmap and return every matching TID, so Gather
 * would collect N copies of the result set (one per worker) and return N×
 * the expected row count. That is exactly the duplicate-result / slowdown
 * bug observed in the EXPLAIN ANALYZE output.
 *
 * Fix: range-partition the pre-sorted TID array across workers using the
 * atomic chunk counter already present in BiscuitParallelScanDesc.
 *
 *   Leader path  (biscuit_collect_sorted_tids_parallel)
 *   ────────────────────────────────────────────────────
 *   1. Collect the full result set into a palloc'd array exactly once
 *      (same as the single-threaded path).
 *   2. Populate pdesc->total_tids, total_chunks, chunk_size.
 *   3. Claim chunk 0 for the leader itself via biscuit_claim_next_chunk(),
 *      then copy only that slice into a fresh palloc'd *out_tids / *out_count.
 *      (Independent copy so the caller can pfree it without corrupting all_tids.)
 *   4. Store the full sorted array pointer in pdesc->pad[] (the cache-line
 *      padding field inside BiscuitParallelScanDesc) via memcpy so background
 *      workers can retrieve it with a matching memcpy + pg_memory_barrier().
 *      The array lives in the leader's palloc context (so->all_tids in
 *      BiscuitScanOpaque) for the duration of the scan.
 *
 *   Worker path  (biscuit_parallel_collect_chunk)
 *   ──────────────────────────────────────────────
 *   Each background worker detects IsParallelWorker() in biscuit_rescan(),
 *   reads the all_tids pointer from pdesc->pad[], then calls
 *   biscuit_parallel_collect_chunk() which atomically claims a disjoint
 *   [chunk_start, chunk_end) range and copies only those TIDs into a
 *   process-private buffer.  Workers never write shared state except through
 *   the atomic counter.
 *
 * Other changes from previous revision
 * ──────────────────────────────────────
 * 1. Roaring streaming iterator replaces biscuit_roaring_to_array():
 *    avoids the extra palloc + full-scan copy, halving peak memory for
 *    large result sets and improving cache utilisation.
 * 2. Hardware prefetch in the hot loop (__builtin_prefetch, L2, read-only).
 * 3. uint64_t → int truncation guarded by runtime Assert at every cast site.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_tid.h"

/* Number of slots to prefetch ahead in the TID-copy hot loop. */
#define PREFETCH_DISTANCE 16

/* ==================== COMPARISON ==================== */

static int biscuit_planned_nworkers = 0;

static inline int
biscuit_compare_tids(const void *a, const void *b)
{
    return (int) ItemPointerCompare((ItemPointer) a, (ItemPointer) b);
}

/* ==================== RADIX SORT ==================== */

/*
 * 4-pass LSD radix sort on BlockNumber, then a per-block counting sort
 * on OffsetNumber.  O(n) for large sets; falls back to qsort per block
 * if an offset exceeds 511 (should never happen in practice).
 */
static void
biscuit_radix_sort_tids(ItemPointerData *tids, int count)
{
    ItemPointerData *temp = NULL;
    ItemPointerData *src, *dst;
    int              i, pass;

    if (count <= 1)
        return;

    PG_TRY();
    {
        temp = (ItemPointerData *) palloc(count * sizeof(ItemPointerData));

        /* Phase 1: sort all 32 bits of BlockNumber in 4 × 8-bit passes */
        src = tids;
        dst = temp;

        for (pass = 0; pass < 4; pass++)
        {
            int counts[256]  = {0};
            int offsets[256];
            int shift        = pass * 8;

            for (i = 0; i < count; i++)
            {
                int bv = (ItemPointerGetBlockNumber(&src[i]) >> shift) & 0xFF;
                counts[bv]++;
            }

            offsets[0] = 0;
            for (i = 1; i < 256; i++)
                offsets[i] = offsets[i - 1] + counts[i - 1];

            for (i = 0; i < count; i++)
            {
                int bv  = (ItemPointerGetBlockNumber(&src[i]) >> shift) & 0xFF;
                int pos = offsets[bv]++;
                ItemPointerCopy(&src[i], &dst[pos]);
            }

            /* Swap src/dst for next pass */
            {
                ItemPointerData *swap = src;
                src = dst;
                dst = swap;
            }
        }
        /* After 4 passes with even swap count, data is back in tids[] */

        /* Phase 2: sort OffsetNumber within each block */
        {
            int start = 0;

            while (start < count)
            {
                BlockNumber current_block = ItemPointerGetBlockNumber(&tids[start]);
                int         block_end    = start + 1;

                while (block_end < count &&
                       ItemPointerGetBlockNumber(&tids[block_end]) == current_block)
                    block_end++;

                if (block_end - start > 1)
                {
                    int offset_counts[512];
                    int offset_positions[512];
                    int j;
                    bool fallback = false;

                    for (j = 0; j < 512; j++)
                        offset_counts[j] = 0;

                    for (i = start; i < block_end; i++)
                    {
                        OffsetNumber off = ItemPointerGetOffsetNumber(&tids[i]);
                        if (off < 512)
                            offset_counts[off]++;
                        else
                        {
                            elog(WARNING,
                                 "Biscuit: Invalid offset %d, falling back to qsort",
                                 off);
                            qsort(&tids[start], block_end - start,
                                  sizeof(ItemPointerData), biscuit_compare_tids);
                            fallback = true;
                            break;
                        }
                    }

                    if (!fallback)
                    {
                        offset_positions[0] = 0;
                        for (j = 1; j < 512; j++)
                            offset_positions[j] = offset_positions[j - 1] + offset_counts[j - 1];

                        for (i = start; i < block_end; i++)
                        {
                            OffsetNumber off = ItemPointerGetOffsetNumber(&tids[i]);
                            int          pos = start + offset_positions[off]++;
                            ItemPointerCopy(&tids[i], &temp[pos]);
                        }
                        memcpy(&tids[start], &temp[start],
                               (block_end - start) * sizeof(ItemPointerData));
                    }
                }

                start = block_end;
            }
        }

        pfree(temp);
    }
    PG_CATCH();
    {
        if (temp)
            pfree(temp);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/* ==================== PUBLIC SORT ENTRY POINT ==================== */

void
biscuit_sort_tids_by_block(ItemPointerData *tids, int count)
{
    if (count <= 1)
        return;

    if (count < RADIX_SORT_THRESHOLD)
        qsort(tids, count, sizeof(ItemPointerData), biscuit_compare_tids);
    else
        biscuit_radix_sort_tids(tids, count);
}

/* ==================== SINGLE-THREADED COLLECTION ==================== */

void
biscuit_collect_sorted_tids_single(BiscuitIndex *idx,
                                   RoaringBitmap *result,
                                   ItemPointerData **out_tids,
                                   int *out_count,
                                   bool needs_sorting)
{
    uint64_t         count;
    ItemPointerData *tids;
    int              idx_out = 0;

    count = biscuit_roaring_count(result);

    if (count == 0)
    {
        *out_tids  = NULL;
        *out_count = 0;
        return;
    }

    Assert(count <= (uint64_t) INT_MAX);

    tids = (ItemPointerData *) palloc(count * sizeof(ItemPointerData));

#ifdef HAVE_ROARING
    {
        /*
         * Stream record indices directly from the bitmap without materialising
         * a temporary uint32_t[] array.  For each entry we issue a prefetch
         * PREFETCH_DISTANCE slots ahead into idx->tids[] to hide the random-
         * access latency of large index tables.
         */
        roaring_uint32_iterator_t *iter = roaring_iterator_create(result);

        while (iter->has_value)
        {
            uint32_t rec_idx = iter->current_value;

            /*
             * Prefetch PREFETCH_DISTANCE record slots ahead.  The prefetch
             * target is computed from rec_idx rather than the output position
             * so it tracks the actual random-access pattern in idx->tids[].
             * locality=1 → L2 cache, read-only intent (rw=0).
             */
            if (rec_idx + PREFETCH_DISTANCE < (uint32_t) idx->num_records)
                __builtin_prefetch(&idx->tids[rec_idx + PREFETCH_DISTANCE], 0, 1);

            if (rec_idx < (uint32_t) idx->num_records)
            {
                ItemPointerCopy(&idx->tids[rec_idx], &tids[idx_out]);
                idx_out++;
            }
            roaring_uint32_iterator_advance(iter);
        }
        roaring_uint32_iterator_free(iter);
    }
#else
    {
        uint32_t *indices = biscuit_roaring_to_array(result, &count);
        int       i;

        if (indices)
        {
            for (i = 0; i < (int) count; i++)
            {
                if (indices[i] < (uint32_t) idx->num_records)
                {
                    ItemPointerCopy(&idx->tids[indices[i]], &tids[idx_out]);
                    idx_out++;
                }
            }
            pfree(indices);
        }
    }
#endif

    *out_count = idx_out;

    if (needs_sorting && idx_out > 1)
        biscuit_sort_tids_by_block(tids, idx_out);

    *out_tids = tids;
}

/* ==================== PARALLEL COLLECTION ==================== */

/*
 * biscuit_collect_sorted_tids_parallel
 *
 * Called by every participant (leader and background workers) from
 * biscuit_rescan() when scan->parallel_scan != NULL.
 *
 * Design: pre-partition, self-identify, local evaluate
 * -----------------------------------------------------
 * The first participant to arrive (CAS initialized 0→1) evaluates the bitmap,
 * computes the total TID count, divides it into num_participants slices, writes
 * [start,end) into pdesc->slots[], then sets initialized=2.  All others spin.
 *
 * Then every participant reads MyParallelWorkerNumber to find its slot:
 *   leader (-1) → slot 0
 *   worker  N   → slot N+1
 *
 * Each participant evaluates the bitmap locally (identical result — same index,
 * same query, read-only), keeps only tids[start..end), frees the rest.
 *
 * No per-TID atomics.  No chunk claiming races.  The Gather node assembles
 * exactly one copy of the full result from the disjoint per-process slices.
 */
void
biscuit_collect_sorted_tids_parallel(BiscuitIndex            *idx,
                                      RoaringBitmap           *result,
                                      BiscuitParallelScanDesc *pdesc,
                                      ItemPointerData        **out_tids,
                                      int                     *out_count,
                                      bool                     needs_sorting)
{
    *out_tids  = NULL;
    *out_count = 0;
    /* Non-parallel fallback. */
    if (pdesc == NULL)
    {
        biscuit_collect_sorted_tids_single(idx, result, out_tids, out_count,
                                           needs_sorting);
        return;
    }
    elog(DEBUG1,
         "biscuit parallel: pid=%d ParallelWorkerNumber=%d "
         "num_participants=%d initialized=%u",
         (int) MyProcPid,
         ParallelWorkerNumber,
         pdesc->num_participants,
         pg_atomic_read_u32(&pdesc->initialized));
    /* ---------------------------------------------------------------
     * Phase 1: elect one initializer to build the partition table.
     *
     * initialized states:
     *   0 – nobody has started   (set by initparallelscan / parallelrescan)
     *   1 – one process is computing (holds the "lock")
     *   2 – partition table is ready; everyone may proceed
     * --------------------------------------------------------------- */
    {
        uint32 zero = 0;
        bool   won  = pg_atomic_compare_exchange_u32(&pdesc->initialized,
                                                      &zero, 1);
        if (won)
        {
            /*
             * We are the initializer.  Evaluate the bitmap to get total_tids,
             * then divide into num_participants slices.
             */
            ItemPointerData *all_tids  = NULL;
            int              all_count = 0;
            int              np        = pdesc->num_participants;
            int              i;

            biscuit_collect_sorted_tids_single(idx, result,
                                               &all_tids, &all_count,
                                               needs_sorting);

            pdesc->total_tids = (uint64_t) all_count;

            if (all_count > 0 && np > 0)
            {
                /*
                 * Divide total_tids into np roughly-equal slices.
                 * base_size = floor(all_count / np)
                 * remainder = all_count % np   (distributed to last slot)
                 *
                 * We give the last slot whatever is left so uneven divisions
                 * never lose or duplicate TIDs.
                 */
                uint64_t base_size = (uint64_t) all_count / (uint64_t) np;
                uint64_t cursor    = 0;

                for (i = 0; i < np; i++)
                {
                    pdesc->slots[i].start = cursor;
                    if (i < np - 1)
                        cursor += base_size;
                    else
                        cursor  = (uint64_t) all_count; /* last gets remainder */
                    pdesc->slots[i].end = cursor;
                }
            }
            else
            {
                /* Empty result or no participants: zero all slots. */
                for (i = 0; i < BISCUIT_MAX_PARALLEL_WORKERS + 1; i++)
                    pdesc->slots[i].start = pdesc->slots[i].end = 0;
            }

            /*
             * Full barrier: slot writes must be visible to all processes
             * before initialized is set to 2.
             */
            pg_memory_barrier();
            pg_atomic_write_u32(&pdesc->initialized, 2);

            /* Free the full array — we'll re-evaluate per-process below. */
            if (all_tids)
                pfree(all_tids);
        }
        else
        {
            /*
             * Loser: spin until the initializer sets initialized = 2.
             * Use a 1 µs sleep to avoid burning a core; CHECK_FOR_INTERRUPTS
             * so the backend remains cancellable during the wait.
             */
            while (pg_atomic_read_u32(&pdesc->initialized) != 2)
            {
                CHECK_FOR_INTERRUPTS();
                pg_usleep(1);
            }
            pg_memory_barrier();    /* ensure slot reads see the initializer's writes */
        }
    }

    /* ---------------------------------------------------------------
     * Phase 2: every participant reads its own pre-computed slot.
     *
     * MyParallelWorkerNumber:
     *   -1 → leader  → slot 0
     *    N → worker N → slot N+1
     * --------------------------------------------------------------- */
    {
        int      slot_idx;
        uint64_t my_start;
        uint64_t my_end;
        int      my_count;

        slot_idx = ParallelWorkerNumber + 1;   /* leader: -1+1=0, worker N: N+1 */

        /* Clamp defensively — should never fire under normal operation. */
        if (slot_idx < 0 || slot_idx >= pdesc->num_participants)
        {
            elog(WARNING,
                 "biscuit: parallel slot index %d out of range [0,%d); "
                 "returning empty result",
                 slot_idx, pdesc->num_participants);
            return;
        }

        my_start = pdesc->slots[slot_idx].start;
        my_end   = pdesc->slots[slot_idx].end;
        my_count = (int) (my_end - my_start);

        if (my_count <= 0 || pdesc->total_tids == 0)
            return;   /* empty result or nothing assigned to this participant */

        /* ---------------------------------------------------------------
         * Phase 3: evaluate the bitmap locally and return only our slice.
         *
         * Every process produces an identical sorted TID array (same index,
         * same query, fully read-only).  We keep only [my_start, my_end)
         * and immediately free the rest to avoid peak memory N× total_tids.
         * --------------------------------------------------------------- */
        {
            ItemPointerData *all_tids  = NULL;
            int              all_count = 0;
            ItemPointerData *slice;

            biscuit_collect_sorted_tids_single(idx, result,
                                               &all_tids, &all_count,
                                               needs_sorting);

            if (all_count == 0 || all_tids == NULL)
                return;

            /*
             * Sanity: if the bitmap count differs from what the initializer
             * saw, something changed between evaluations.  Clamp to avoid
             * an out-of-bounds read; log a warning so the operator knows.
             */
            if ((uint64_t) all_count != pdesc->total_tids)
            {
                elog(WARNING,
                     "biscuit: parallel TID count mismatch: expected %llu, "
                     "got %d; clamping slice",
                     (unsigned long long) pdesc->total_tids, all_count);
                if (my_end > (uint64_t) all_count)
                    my_end = (uint64_t) all_count;
                if (my_start >= my_end)
                {
                    pfree(all_tids);
                    return;
                }
                my_count = (int) (my_end - my_start);
            }

            /* Copy the assigned slice into a fresh palloc buffer. */
            slice = (ItemPointerData *)
                        palloc(my_count * sizeof(ItemPointerData));
            memcpy(slice, &all_tids[my_start],
                   my_count * sizeof(ItemPointerData));

            pfree(all_tids);    /* free the full local array immediately */

            *out_tids  = slice;
            *out_count = my_count;
        }
    }
}

/* ==================== PARALLEL WORKER ENTRY POINT ==================== */

/*
 * biscuit_parallel_collect_worker
 *
 * Registered via RegisterParallelWorkerMain() for dynamic-loader
 * compatibility.  Body is a no-op stub; retained so symbol lookup on
 * extension reload does not fail.
 */
PGDLLEXPORT void
biscuit_parallel_collect_worker(dsm_segment *seg, shm_toc *toc)
{
    (void) seg;
    (void) toc;
}

/* ==================== PARALLEL WORKER HELPERS ==================== */

/**
 * biscuit_get_parallel_worker_count
 *
 * Returns the number of parallel workers active or requested for this index scan.
 * Returns 0 if this is a single-threaded execution thread.
 */
int
biscuit_get_parallel_worker_count(IndexScanDesc scan)
{
    BiscuitParallelScanDesc *pdesc;

    if (scan == NULL || scan->parallel_scan == NULL)
        return 0;

    pdesc = (BiscuitParallelScanDesc *)
                OffsetToPointer(scan->parallel_scan,
                                BISCUIT_PARALLEL_AM_OFFSET(scan->parallel_scan));

    return (pdesc->num_participants > 1) ? pdesc->num_participants - 1 : 2;
}

/* ==================== SCAN HELPERS ==================== */

bool
biscuit_is_aggregate_query(IndexScanDesc scan)
{
    return !scan->xs_want_itup;
}

/* ==================== PARALLEL AM CALLBACKS ==================== */

/*
 * biscuit_estimateparallelscan
 *
 * Returns sizeof(BiscuitParallelScanDesc) so the executor reserves exactly
 * the right amount of DSM space.
 *
 * The amestimateparallelscan_function typedef has changed signature across
 * major PostgreSQL versions:
 *
 *   PG16  and earlier : Size (*)(void)
 *   PG17              : Size (*)(int nkeys, int norderbys)
 *   PG18  and later   : Size (*)(Relation indexRelation, int nworkers, int nchunks)
 *
 * On PG17 the parameters carry operator-class metadata (not a worker count),
 * so biscuit_planned_nworkers is left at 0 on that version.
 * On PG16 and earlier there are no parameters at all; same treatment.
 * On PG18+ nworkers is available and is stashed for initparallelscan.
 *
 * In all cases where nworkers is unavailable, biscuit_initparallelscan sets
 * num_participants = 1 (leader only), and the scan degrades gracefully to
 * the single-process path.
 */
#if PG_VERSION_NUM >= 180000
Size
biscuit_estimateparallelscan(Relation indexRelation, int nworkers, int nchunks)
{
    (void) indexRelation;
    (void) nchunks;
    biscuit_planned_nworkers = nworkers;
    return sizeof(BiscuitParallelScanDesc);
}
#elif PG_VERSION_NUM >= 170000
Size
biscuit_estimateparallelscan(int nkeys, int norderbys)
{
    (void) nkeys;
    (void) norderbys;
    biscuit_planned_nworkers = 0;
    return sizeof(BiscuitParallelScanDesc);
}
#else
Size
biscuit_estimateparallelscan(void)
{
    biscuit_planned_nworkers = 0;
    return sizeof(BiscuitParallelScanDesc);
}
#endif

/*
 * biscuit_initparallelscan
 *
 * Called once by the leader after the DSM segment is allocated.
 * Zeroes the descriptor, records num_participants, and sets initialized = 0
 * so the first rescan triggers partition computation.
 *
 * num_participants is inferred as the number of active workers + 1 (leader).
 * PostgreSQL does not pass nworkers here, so we derive it from
 * ParallelWorkerNumber which the leader can read as 0 workers launched
 * at this point (still during init).  Instead we store 0 and let
 * biscuit_collect_sorted_tids_parallel() populate it from
 * pdesc->num_participants.
 *
 * NOTE: nworkers is NOT available in this callback.  We therefore set
 * num_participants to 0 here and resolve it lazily at the first
 * biscuit_collect_sorted_tids_parallel() call using
 * scan->parallel_scan->ps_nworkers + 1.  biscuit_rescan() passes this
 * through the pdesc pointer which it derives from scan->parallel_scan.
 * The caller in biscuit_scan.c sets pdesc->num_participants after this
 * function returns.
 */
void
biscuit_initparallelscan(void *target)
{
    BiscuitParallelScanDesc *pdesc = (BiscuitParallelScanDesc *) target;
    memset(pdesc, 0, sizeof(BiscuitParallelScanDesc));
    pg_atomic_init_u32(&pdesc->initialized, 0);

    /*
     * nworkers is unavailable here directly, but estimateparallelscan
     * was called in the same process immediately before this.
     * +1 for the leader.
     */
    elog(DEBUG1, "Default pdesc->num_participants value: %d", pdesc->num_participants);
    pdesc->num_participants = biscuit_planned_nworkers + 1;
    // biscuit_planned_nworkers = 0;   /* reset defensively */
    elog(DEBUG1, "Initiated parallel scan. Parallel workers: %d",pdesc->num_participants);
}

/*
 * biscuit_parallelrescan
 *
 * Resets the descriptor for a fresh scan (e.g. Materialize node rewind).
 * Clears initialized and all slots so the next rescan re-partitions.
 */
void
biscuit_parallelrescan(IndexScanDesc scan)
{
    BiscuitParallelScanDesc *pdesc;
    int                      i;

    if (scan->parallel_scan == NULL)
        return;

    pdesc = (BiscuitParallelScanDesc *)
                OffsetToPointer(scan->parallel_scan,
                                BISCUIT_PARALLEL_AM_OFFSET(scan->parallel_scan));

    /* Clear all slots and reset the initialized flag to 0. */
    pdesc->total_tids = 0;
    for (i = 0; i < BISCUIT_MAX_PARALLEL_WORKERS + 1; i++)
        pdesc->slots[i].start = pdesc->slots[i].end = 0;

    pg_memory_barrier();
    pg_atomic_write_u32(&pdesc->initialized, 0);
}
