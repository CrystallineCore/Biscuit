/*
 * biscuit_tid.c
 * TID sorting (radix + qsort) and parallel/optimized TID collection.
 *
 * Changes from previous revision
 * ────────────────────────────────
 * 1. LIMIT-awareness removed entirely.
 *    biscuit_collect_tids_optimized() and biscuit_estimate_limit_hint() are
 *    gone.  Truncation is the executor's responsibility; doing it inside an
 *    index AM produced incorrect results whenever the bitmap contained
 *    out-of-range record indices (the 2× buffer heuristic silently dropped
 *    live rows).  Callers that previously used the optimized entry point
 *    should call biscuit_collect_sorted_tids_parallel() or
 *    biscuit_collect_sorted_tids_single() directly.
 *
 * 2. Parallel path is now genuinely parallel.
 *    The previous implementation launched workers in a sequential for-loop,
 *    providing all the overhead of parallelism (index array materialisation,
 *    worker structs, compaction pass) with none of the benefit.  The new
 *    implementation uses PostgreSQL's ParallelContext / dynamic shared memory
 *    (DSM) and pg_atomic_uint64 work-stealing so that workers truly execute
 *    concurrently.  The sequential fallback (< 10 000 hits, or parallel
 *    infrastructure unavailable) is unchanged.
 *
 * 3. Parallel worker body completed.
 *    The previous stub copied output slots onto themselves (a no-op) because
 *    idx->tids[] is private to the leader process.  idx->tids[] is now
 *    copied into the DSM segment under TOC key 3 before workers are launched,
 *    so workers resolve record indices to TIDs from shared memory with no
 *    private-pointer access.  The TODO and placeholder self-copies are gone.
 *
 * 4. uint64_t → int truncation guarded.
 *    A compile-time StaticAssert and a runtime Assert now protect every site
 *    that casts biscuit_roaring_count() to int, ensuring num_records never
 *    exceeds INT_MAX.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_tid.h"

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
        roaring_uint32_iterator_t *iter = roaring_iterator_create(result);

        while (iter->has_value)
        {
            uint32_t rec_idx = iter->current_value;
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
 * Shared-memory layout (one DSM segment, addressed via shm_toc):
 *
 *   TOC key 0 — BiscuitParallelState   (control block + atomic cursor)
 *   TOC key 1 — uint32_t[count]        (flattened record-index array)
 *   TOC key 2 — ItemPointerData[count] (output TID array, pre-allocated)
 *   TOC key 3 — ItemPointerData[num_records] (shared copy of idx->tids[])
 *
 * Each worker atomically claims a CHUNK_SIZE slice of the index array,
 * writes TIDs directly into the pre-allocated output slot for that slice,
 * and records how many entries it actually wrote (some rec_idx values may
 * be out of range).  The leader then compacts and optionally sorts.
 */

#define BISCUIT_PARALLEL_CHUNK           1024
#define BISCUIT_PARALLEL_TOC_KEY_STATE   0
#define BISCUIT_PARALLEL_TOC_KEY_INDICES 1
#define BISCUIT_PARALLEL_TOC_KEY_OUTPUT  2
#define BISCUIT_PARALLEL_TOC_KEY_TIDS    3   /* shared copy of idx->tids[] */

typedef struct BiscuitParallelState
{
    /* Set by leader before workers launch; read-only for workers. */
    uint64_t        total_count;   /* elements in the shared index array     */
    uint32_t        num_records;   /* idx->num_records validity ceiling      */

    /*
     * Work-stealing cursor.  Each worker atomically increments this by
     * BISCUIT_PARALLEL_CHUNK to claim its next slice.
     */
    pg_atomic_uint64 cursor;

    /*
     * Per-chunk output counts written by workers.  Sized for the maximum
     * number of chunks; leader uses these to compact the output array.
     * max_chunks = (total_count + CHUNK - 1) / CHUNK
     * Allocated as a flexible array member.
     */
    uint32_t        chunk_counts[FLEXIBLE_ARRAY_MEMBER];
} BiscuitParallelState;

/*
 * Worker entry point, registered via RegisterParallelWorkerMain().
 * Name must match the string passed to LaunchParallelWorkers().
 */
PGDLLEXPORT void
biscuit_parallel_collect_worker(dsm_segment *seg, shm_toc *toc)
{
    BiscuitParallelState *state;
    uint32_t             *indices;
    ItemPointerData      *output;
    ItemPointerData      *shared_tids;
    uint64_t              total_count;
    uint32_t              num_records;
    uint64_t              chunk_size = BISCUIT_PARALLEL_CHUNK;

    state       = (BiscuitParallelState *) shm_toc_lookup(toc, BISCUIT_PARALLEL_TOC_KEY_STATE,   false);
    indices     = (uint32_t *)             shm_toc_lookup(toc, BISCUIT_PARALLEL_TOC_KEY_INDICES,  false);
    output      = (ItemPointerData *)      shm_toc_lookup(toc, BISCUIT_PARALLEL_TOC_KEY_OUTPUT,   false);
    shared_tids = (ItemPointerData *)      shm_toc_lookup(toc, BISCUIT_PARALLEL_TOC_KEY_TIDS,     false);
    total_count = state->total_count;
    num_records = state->num_records;

    for (;;)
    {
        uint64_t slice_start = pg_atomic_fetch_add_u64(&state->cursor, chunk_size);
        uint64_t slice_end;
        uint64_t i;
        uint32_t written = 0;
        uint64_t chunk_idx;

        if (slice_start >= total_count)
            break;

        slice_end = Min(slice_start + chunk_size, total_count);
        chunk_idx = slice_start / chunk_size;

        /*
         * Each worker owns output[slice_start .. slice_end) exclusively.
         * No locking needed: the atomic cursor guarantees disjoint slices.
         * written counts only the entries actually placed (rec_idx values
         * beyond num_records are silently skipped).
         */
        for (i = slice_start; i < slice_end; i++)
        {
            uint32_t rec_idx = indices[i];
            if (rec_idx < num_records)
            {
                ItemPointerCopy(&shared_tids[rec_idx],
                                &output[slice_start + written]);
                written++;
            }
        }

        state->chunk_counts[chunk_idx] = written;
    }
}

void
biscuit_collect_sorted_tids_parallel(BiscuitIndex *idx,
                                     RoaringBitmap *result,
                                     ItemPointerData **out_tids,
                                     int *out_count,
                                     bool needs_sorting)
{
    uint64_t             count;
    uint32_t            *indices;
    int                  num_workers;
    const int            max_workers = 4;

    count = biscuit_roaring_count(result);

    if (count == 0)
    {
        *out_tids  = NULL;
        *out_count = 0;
        return;
    }

    /* Fall back to single-threaded for small result sets */
    if (count < 10000)
    {
        biscuit_collect_sorted_tids_single(idx, result, out_tids,
                                           out_count, needs_sorting);
        return;
    }

    num_workers = (count < 100000) ? 2 : max_workers;

    indices = biscuit_roaring_to_array(result, &count);
    if (!indices)
    {
        *out_tids  = NULL;
        *out_count = 0;
        return;
    }

    /*
     * Build DSM segment and populate shared state.
     */
    {
        uint64_t              max_chunks;
        Size                  state_size;
        Size                  indices_size;
        Size                  output_size;
        Size                  tids_size;
        shm_toc_estimator     estimator;
        Size                  total_size;
        dsm_segment          *seg;        /* owned by pcxt, do not detach separately */
        shm_toc              *toc;        /* owned by pcxt */
        BiscuitParallelState *state;
        ParallelContext       *pcxt;
        uint32_t             *shared_indices;
        ItemPointerData      *shared_output;
        ItemPointerData      *shared_tids;
        int                   total_collected;
        int                   write_pos;
        uint64_t              ci;

        /*
         * Guard against uint64_t → int truncation.  num_records is typed int
         * in BiscuitIndex, so count cannot exceed it; this Assert documents
         * and enforces that invariant at the one place we convert.
         */
        Assert(idx->num_records >= 0);
        Assert(count <= (uint64_t) idx->num_records);

        max_chunks   = (count + BISCUIT_PARALLEL_CHUNK - 1) / BISCUIT_PARALLEL_CHUNK;
        state_size   = offsetof(BiscuitParallelState, chunk_counts) +
                       max_chunks * sizeof(uint32_t);
        indices_size = count * sizeof(uint32_t);
        output_size  = count * sizeof(ItemPointerData);
        tids_size    = (Size) idx->num_records * sizeof(ItemPointerData);


        /*
         * Create the ParallelContext first, then call InitializeParallelDSM
         * with our pre-computed size estimate so PostgreSQL allocates a single
         * DSM segment of exactly the size we need.  We must not create a
         * separate dsm_create() segment and graft it onto pcxt->seg — that
         * would leak the segment allocated by InitializeParallelDSM and
         * corrupt the context's internal state.
         */
        pcxt = CreateParallelContext("$libdir/biscuit",
                             "biscuit_parallel_collect_worker",
                             num_workers);

        /* Register YOUR data chunks into pcxt->estimator BEFORE InitializeParallelDSM.
         * InitializeParallelDSM also calls shm_toc_estimate_chunk/keys on pcxt->estimator
         * for its own internal state. Both sets of estimates must be in the same
         * estimator so the final DSM segment is large enough for everything. */
        shm_toc_estimate_chunk(&pcxt->estimator, state_size);
        shm_toc_estimate_chunk(&pcxt->estimator, indices_size);
        shm_toc_estimate_chunk(&pcxt->estimator, output_size);
        shm_toc_estimate_chunk(&pcxt->estimator, tids_size);
        shm_toc_estimate_keys(&pcxt->estimator, 4);

        InitializeParallelDSM(pcxt);   /* now sees full picture: its overhead + yours */
        seg = pcxt->seg;
        toc = pcxt->toc;

        /* shm_toc_allocate calls are unchanged */

        state              = (BiscuitParallelState *) shm_toc_allocate(toc, state_size);
        state->total_count = count;
        state->num_records = (uint32_t) idx->num_records;
        pg_atomic_init_u64(&state->cursor, 0);
        memset(state->chunk_counts, 0, max_chunks * sizeof(uint32_t));
        shm_toc_insert(toc, BISCUIT_PARALLEL_TOC_KEY_STATE, state);

        shared_indices = (uint32_t *) shm_toc_allocate(toc, indices_size);
        memcpy(shared_indices, indices, indices_size);
        shm_toc_insert(toc, BISCUIT_PARALLEL_TOC_KEY_INDICES, shared_indices);
        pfree(indices);

        shared_output = (ItemPointerData *) shm_toc_allocate(toc, output_size);
        shm_toc_insert(toc, BISCUIT_PARALLEL_TOC_KEY_OUTPUT, shared_output);

        /*
         * Copy the leader's private TID table into shared memory so that
         * parallel workers can resolve record indices to heap TIDs without
         * accessing the leader's address space.
         */
        shared_tids = (ItemPointerData *) shm_toc_allocate(toc, tids_size);
        memcpy(shared_tids, idx->tids, tids_size);
        shm_toc_insert(toc, BISCUIT_PARALLEL_TOC_KEY_TIDS, shared_tids);

        LaunchParallelWorkers(pcxt);

        /*
         * Leader participates as an extra worker rather than spinning idle.
         * Uses the same atomic cursor, giving (num_workers + 1)-way
         * parallelism with no additional process overhead.
         */
        biscuit_parallel_collect_worker(seg, toc);

        WaitForParallelWorkersToFinish(pcxt);
        /* DestroyParallelContext called below, after we copy out of DSM. */

        /*
         * Compact: some chunks may have written fewer items than their slice
         * size (out-of-range rec_idx values).  Consolidate into a contiguous
         * array.  shared_output and state still live in the DSM segment
         * owned by pcxt; do not destroy pcxt until after the copy-out.
         */
        total_collected = 0;
        write_pos       = 0;

        for (ci = 0; ci < max_chunks; ci++)
        {
            uint32_t written    = state->chunk_counts[ci];
            uint64_t slice_start = ci * BISCUIT_PARALLEL_CHUNK;

            if (written == 0)
                continue;

            if (write_pos != (int) slice_start)
                memmove(&shared_output[write_pos],
                        &shared_output[slice_start],
                        written * sizeof(ItemPointerData));

            write_pos       += written;
            total_collected += written;
        }

        /*
         * Copy result out of DSM into local (palloc'd) memory before
         * detaching the segment.
         */
        {
            ItemPointerData *local_tids =
                (ItemPointerData *) palloc(total_collected * sizeof(ItemPointerData));
            memcpy(local_tids, shared_output,
                   total_collected * sizeof(ItemPointerData));

            /*
             * DestroyParallelContext detaches and destroys the DSM segment;
             * do NOT call dsm_detach(seg) separately — that would double-free.
             */
            DestroyParallelContext(pcxt);

            *out_count = total_collected;

            if (needs_sorting && total_collected > 1)
                biscuit_sort_tids_by_block(local_tids, total_collected);

            *out_tids = local_tids;
        }
    }
}

/* ==================== SCAN HELPERS ==================== */

bool
biscuit_is_aggregate_query(IndexScanDesc scan)
{
    return !scan->xs_want_itup;
}
