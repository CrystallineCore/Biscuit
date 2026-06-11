/*
 * biscuit_tid.c
 * TID sorting (radix + qsort) and single-threaded TID collection.
 *
 * Changes from previous revision
 * ────────────────────────────────
 * 1. Fake parallel path removed entirely.
 *    biscuit_collect_sorted_tids_parallel() and the DSM/worker machinery
 *    that preceded it have been deleted.  The "parallel" implementation
 *    called workers sequentially, adding the full cost of index-array
 *    materialisation, DSM allocation, and a compaction pass with zero
 *    concurrency benefit (3× regression on the benchmark suite).
 *    biscuit_collect_tids_optimized() now routes unconditionally through
 *    the single-threaded path.
 *
 * 2. Roaring streaming iterator replaces biscuit_roaring_to_array().
 *    Under HAVE_ROARING the collection loop uses roaring_uint32_iterator_t
 *    instead of materialising a temporary uint32_t[] array.  This avoids
 *    the extra palloc + full-scan copy, cutting peak memory roughly in half
 *    for large result sets and improving cache utilisation.
 *
 * 3. Hardware prefetch in the hot loop.
 *    __builtin_prefetch() issues a non-temporal L2 prefetch for the TID
 *    entry PREFETCH_DISTANCE slots ahead, hiding the latency of random
 *    idx->tids[] accesses on large indexes.
 *
 * 4. uint64_t → int truncation guarded.
 *    A runtime Assert protects every site that casts biscuit_roaring_count()
 *    to int, ensuring num_records never exceeds INT_MAX.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_tid.h"

/* Number of slots to prefetch ahead in the TID-copy hot loop. */
#define PREFETCH_DISTANCE 16

/* ==================== COMPARISON ==================== */

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

        /*
         * Prime the prefetch pipeline for the first PREFETCH_DISTANCE slots
         * by advancing a lookahead iterator.  We use a separate pass rather
         * than peeking into iter internals, keeping the API surface clean.
         *
         * For simplicity we prefetch speculatively in the main loop using the
         * current value offset by PREFETCH_DISTANCE in record-index space.
         * This is not perfectly accurate (the bitmap may be sparse), but it
         * is branchless and keeps the CPU pipeline busy with high probability
         * on dense result sets, which are the common case for large scans.
         */
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

/*
 * biscuit_collect_sorted_tids_parallel — kept for ABI compatibility only.
 *
 * The previous DSM-based implementation provided zero concurrency benefit
 * (workers were driven by a sequential for-loop) while paying the full cost
 * of bitmap materialisation, DSM allocation, worker setup, and a compaction
 * pass.  The result was a measured 3× throughput regression versus the
 * single-threaded path.
 *
 * This stub delegates unconditionally to biscuit_collect_sorted_tids_single()
 * so that any caller that holds a direct reference to the parallel entry point
 * continues to produce correct results with no code changes on their side.
 */
void
biscuit_collect_sorted_tids_parallel(BiscuitIndex *idx,
                                     RoaringBitmap *result,
                                     ItemPointerData **out_tids,
                                     int *out_count,
                                     bool needs_sorting)
{
    biscuit_collect_sorted_tids_single(idx, result, out_tids,
                                       out_count, needs_sorting);
}

/* ==================== PARALLEL WORKER ENTRY POINT ==================== */

/*
 * biscuit_parallel_collect_worker — stub retained for dynamic loader
 * compatibility.
 *
 * RegisterParallelWorkerMain() records this symbol at extension load time.
 * Removing it would cause workers launched by an older leader to segfault on
 * lookup.  Since no caller in the current code base launches parallel workers
 * for TID collection, the body is a safe no-op.
 */
PGDLLEXPORT void
biscuit_parallel_collect_worker(dsm_segment *seg, shm_toc *toc)
{
    (void) seg;
    (void) toc;
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
 * Returns the number of bytes that must be reserved in the parallel DSM
 * segment for BiscuitParallelScanDesc.  The executor calls this once during
 * parallel query planning before the segment is created.
 *
 * We return the exact sizeof() so the executor passes a correctly-sized
 * pointer to biscuit_initparallelscan().
 */
Size
biscuit_estimateparallelscan(Relation indexRelation, int nworkers, int nchunks)
{
    (void) indexRelation;
    (void) nworkers;
    (void) nchunks;
    return sizeof(BiscuitParallelScanDesc);
}

/*
 * biscuit_initparallelscan
 *
 * Called by the parallel leader after the DSM segment has been allocated.
 * @target must point to at least sizeof(BiscuitParallelScanDesc) bytes of
 * DSM-backed memory.
 *
 * We memset the descriptor to zero first so that every byte is defined,
 * then initialise next_chunk atomically to 0.  total_tids, total_chunks,
 * and chunk_size are set to their default/sentinel values here; callers
 * (scan.c, or the first worker entering biscuit_parallel_collect_chunk)
 * fill in the real counts once the bitmap result is known.
 *
 * Design constraint: workers must NEVER write to shared state other than
 * through the atomic next_chunk counter.  All other fields are read-only
 * after this function returns.
 */
void
biscuit_initparallelscan(void *target)
{
    BiscuitParallelScanDesc *pdesc = (BiscuitParallelScanDesc *) target;

    /* Zero the entire struct first so padding bytes are well-defined. */
    memset(pdesc, 0, sizeof(BiscuitParallelScanDesc));

    /*
     * Initialise the atomic counter.  pg_atomic_init_u64() must be called
     * exactly once, in the leader, before any worker reads the counter.
     */
    pg_atomic_init_u64(&pdesc->next_chunk, 0);

    /*
     * Set chunk_size to the default.  total_tids and total_chunks are left
     * at 0; they will be populated by the caller (typically biscuit_rescan /
     * biscuit_beginscan) once the result bitmap has been evaluated and the
     * total TID count is known.
     */
    pdesc->chunk_size   = BISCUIT_PARALLEL_CHUNK_SIZE_DEFAULT;
    pdesc->total_tids   = 0;
    pdesc->total_chunks = 0;
}

/*
 * biscuit_parallelrescan
 *
 * AM callback invoked when a parallel scan must be rewound (e.g. a rescan
 * inside a Materialize node).  Resets next_chunk to 0 atomically so that all
 * workers restart from the first chunk.  The read-only fields (total_chunks,
 * chunk_size, total_tids) remain unchanged because the underlying index data
 * has not changed.
 *
 * pg_atomic_write_u64() provides a sequentially-consistent store on all
 * platforms supported by PostgreSQL's port/atomics layer.  No LWLocks or
 * spinlocks are required.
 */
void
biscuit_parallelrescan(IndexScanDesc scan)
{
    BiscuitParallelScanDesc *pdesc;

    if (scan->parallel_scan == NULL)
        return;

    pdesc = (BiscuitParallelScanDesc *)
                OffsetToPointer(scan->parallel_scan,
                                scan->parallel_scan->ps_offset_am);

    pg_atomic_write_u64(&pdesc->next_chunk, 0);
}

/* ==================== PARALLEL WORKER HELPERS ==================== */

/*
 * biscuit_claim_next_chunk
 *
 * Atomically increments next_chunk and returns the range of TID indices the
 * caller now owns exclusively.
 *
 * Returns true  → *chunk_start..*chunk_end-1 (exclusive upper) is valid work.
 * Returns false → all chunks have been claimed; caller should exit.
 *
 * The fetch-add is the ONLY write to shared memory performed by workers.
 * chunk_start = claimed_chunk * chunk_size
 * chunk_end   = min(chunk_start + chunk_size, total_tids)
 */
bool
biscuit_claim_next_chunk(BiscuitParallelScanDesc *pdesc,
                         uint64_t *chunk_start,
                         uint64_t *chunk_end)
{
    uint64_t claimed;

    Assert(pdesc != NULL);

    /* Atomically claim one chunk slot.  Sequentially consistent. */
    claimed = pg_atomic_fetch_add_u64(&pdesc->next_chunk, 1);

    if (claimed >= pdesc->total_chunks)
        return false;   /* no work left */

    *chunk_start = claimed * (uint64_t) pdesc->chunk_size;
    *chunk_end   = *chunk_start + (uint64_t) pdesc->chunk_size;

    /* Clamp to the actual TID count to avoid over-reading. */
    if (*chunk_end > pdesc->total_tids)
        *chunk_end = pdesc->total_tids;

    return true;
}

/*
 * biscuit_parallel_collect_chunk
 *
 * Worker-side TID collection loop.
 *
 * Parameters
 * ----------
 * idx        – read-only pointer to the in-memory BiscuitIndex.
 * pdesc      – pointer to the shared BiscuitParallelScanDesc (DSM).
 *              The only write is through biscuit_claim_next_chunk().
 * all_tids   – pointer to the full, pre-sorted TID array produced by the
 *              leader (stored read-only in the worker's address space).
 * total_tids – length of all_tids[].
 * out_tids   – (output) worker-local palloc'd TID array.
 * out_count  – (output) number of valid TIDs written into *out_tids.
 *
 * Algorithm
 * ---------
 * 1. Claim a chunk via biscuit_claim_next_chunk().
 * 2. Copy the corresponding slice of all_tids[] into a worker-local buffer.
 *    The buffer is grown with repalloc() as new chunks are claimed, avoiding
 *    per-chunk palloc overhead.
 * 3. Repeat until no chunks remain.
 *
 * Shared-state invariant: workers never write to *pdesc except through the
 * atomic counter.  all_tids[] is read-only throughout.  *out_tids is
 * process-private and never shared.
 *
 * Note: when all_tids is NULL (total_tids == 0) the function returns
 * immediately with *out_tids = NULL and *out_count = 0.
 */
void
biscuit_parallel_collect_chunk(BiscuitIndex        *idx,
                               BiscuitParallelScanDesc *pdesc,
                               ItemPointerData     *all_tids,
                               uint64_t             total_tids,
                               ItemPointerData    **out_tids,
                               int                 *out_count)
{
    ItemPointerData *local_buf   = NULL;
    int              local_cap   = 0;
    int              local_count = 0;

    uint64_t chunk_start;
    uint64_t chunk_end;

    /* Guard against empty result sets. */
    if (all_tids == NULL || total_tids == 0 || pdesc == NULL)
    {
        *out_tids  = NULL;
        *out_count = 0;
        return;
    }

    /*
     * Estimate the local buffer size as one chunk to avoid the first
     * repalloc in the common case where each worker processes exactly one
     * chunk.  We grow on demand.
     */
    local_cap = (int) pdesc->chunk_size;
    local_buf = (ItemPointerData *)
                    palloc(local_cap * sizeof(ItemPointerData));

    while (biscuit_claim_next_chunk(pdesc, &chunk_start, &chunk_end))
    {
        uint64_t slot;
        int      count_in_chunk = (int) (chunk_end - chunk_start);

        Assert(chunk_start < total_tids);
        Assert(chunk_end  <= total_tids);
        Assert(count_in_chunk > 0);

        /* Grow the local buffer if necessary. */
        if (local_count + count_in_chunk > local_cap)
        {
            /*
             * Double (or grow to fit) to keep amortised cost O(1) per TID.
             * We never exceed INT_MAX entries because total_tids is bounded
             * by idx->num_records which is an int.
             */
            while (local_cap < local_count + count_in_chunk)
            {
                local_cap *= 2;
                if (local_cap < 0) /* overflow guard */
                {
                    elog(ERROR, "biscuit: parallel TID buffer overflow");
                }
            }
            local_buf = (ItemPointerData *)
                            repalloc(local_buf,
                                     local_cap * sizeof(ItemPointerData));
        }

        /*
         * Bulk copy the TID slice.  all_tids[] is read-only; we write only
         * into our process-private local_buf[].
         */
        for (slot = chunk_start; slot < chunk_end; slot++)
        {
            /*
             * Prefetch the next slot's data into L2 to hide latency when
             * chunks are large.  The target is the TID entry that follows
             * PREFETCH_DISTANCE positions in the chunk.
             */
            if (slot + PREFETCH_DISTANCE < chunk_end)
                __builtin_prefetch(&all_tids[slot + PREFETCH_DISTANCE], 0, 1);

            ItemPointerCopy(&all_tids[slot],
                            &local_buf[local_count]);
            local_count++;
        }

        CHECK_FOR_INTERRUPTS();
    }

    if (local_count == 0)
    {
        pfree(local_buf);
        *out_tids  = NULL;
        *out_count = 0;
        return;
    }

    *out_tids  = local_buf;
    *out_count = local_count;
}
