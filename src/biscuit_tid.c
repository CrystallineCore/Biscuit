/*
 * biscuit_tid.c
 * TID sorting (radix + qsort) and parallel/optimized TID collection.
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
        roaring_uint32_iterator_t *iter = roaring_create_iterator(result);

        while (iter->has_value)
        {
            uint32_t rec_idx = iter->current_value;
            if (rec_idx < (uint32_t) idx->num_records)
            {
                ItemPointerCopy(&idx->tids[rec_idx], &tids[idx_out]);
                idx_out++;
            }
            roaring_advance_uint32_iterator(iter);
        }
        roaring_free_uint32_iterator(iter);
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

/* ==================== PARALLEL WORKER ==================== */

static void
biscuit_collect_tids_worker(TIDCollectionWorker *worker)
{
    uint64_t i;
    int      out_idx = 0;

    for (i = worker->start_idx; i < worker->end_idx; i++)
    {
        uint32_t rec_idx = worker->indices[i];

        if (rec_idx < (uint32_t) worker->idx->num_records)
        {
            ItemPointerCopy(&worker->idx->tids[rec_idx],
                            &worker->output[out_idx]);
            out_idx++;
        }
    }
    worker->output_count = out_idx;
}

/* ==================== PARALLEL COLLECTION ==================== */

void
biscuit_collect_sorted_tids_parallel(BiscuitIndex *idx,
                                     RoaringBitmap *result,
                                     ItemPointerData **out_tids,
                                     int *out_count,
                                     bool needs_sorting)
{
    uint64_t             count;
    ItemPointerData     *tids;
    uint32_t            *indices;
    int                  num_workers;
    const int            max_workers     = 4;
    uint64_t             items_per_worker;
    TIDCollectionWorker *workers;
    int                  i;
    int                  total_collected = 0;

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

    num_workers      = (count < 100000) ? 2 : max_workers;
    items_per_worker = (count + num_workers - 1) / num_workers;

    indices = biscuit_roaring_to_array(result, &count);
    if (!indices)
    {
        *out_tids  = NULL;
        *out_count = 0;
        return;
    }

    tids    = (ItemPointerData *) palloc(count * sizeof(ItemPointerData));
    workers = (TIDCollectionWorker *) palloc(num_workers * sizeof(TIDCollectionWorker));

    for (i = 0; i < num_workers; i++)
    {
        workers[i].idx        = idx;
        workers[i].indices    = indices;
        workers[i].start_idx  = (uint64_t) i * items_per_worker;
        workers[i].end_idx    = ((uint64_t)(i + 1) * items_per_worker < count)
                                    ? (uint64_t)(i + 1) * items_per_worker
                                    : count;
        workers[i].output     = &tids[workers[i].start_idx];
        workers[i].output_count = 0;
    }

    for (i = 0; i < num_workers; i++)
    {
        biscuit_collect_tids_worker(&workers[i]);
        total_collected += workers[i].output_count;
    }

    /* Compact if any slots were skipped (invalid rec indices) */
    if (total_collected < (int) count)
    {
        int write_pos = 0;

        for (i = 0; i < num_workers; i++)
        {
            if (workers[i].output_count > 0)
            {
                if (write_pos != (int) workers[i].start_idx)
                    memmove(&tids[write_pos],
                            &tids[workers[i].start_idx],
                            workers[i].output_count * sizeof(ItemPointerData));
                write_pos += workers[i].output_count;
            }
        }
    }

    pfree(indices);
    pfree(workers);

    *out_count = total_collected;

    if (needs_sorting && total_collected > 1)
        biscuit_sort_tids_by_block(tids, total_collected);

    *out_tids = tids;
}

/* ==================== UNIFIED ENTRY POINT ==================== */

void
biscuit_collect_tids_optimized(BiscuitIndex *idx,
                               RoaringBitmap *result,
                               ItemPointerData **out_tids,
                               int *out_count,
                               bool needs_sorting,
                               int limit_hint)
{
    uint64_t total_count;
    uint64_t collect_count;

    total_count = biscuit_roaring_count(result);

    if (total_count == 0)
    {
        *out_tids  = NULL;
        *out_count = 0;
        return;
    }

    /* LIMIT-aware: collect only what we need */
    collect_count = (limit_hint > 0 && (uint64_t) limit_hint < total_count)
                    ? (uint64_t) limit_hint * 2   /* 2× buffer for safety */
                    : total_count;

    /* Delegate to parallel for large sets */
    if (collect_count >= 10000)
    {
        biscuit_collect_sorted_tids_parallel(idx, result, out_tids,
                                             out_count, needs_sorting);
        if (limit_hint > 0 && *out_count > limit_hint)
            *out_count = limit_hint;
        return;
    }

    /* Single-threaded path with early LIMIT termination */
    {
        ItemPointerData *tids = (ItemPointerData *)
            palloc(collect_count * sizeof(ItemPointerData));
        int idx_out = 0;

#ifdef HAVE_ROARING
        {
            roaring_uint32_iterator_t *iter = roaring_create_iterator(result);

            while (iter->has_value && idx_out < (int) collect_count)
            {
                uint32_t rec_idx = iter->current_value;

                if (rec_idx < (uint32_t) idx->num_records)
                {
                    ItemPointerCopy(&idx->tids[rec_idx], &tids[idx_out]);
                    idx_out++;
                    if (limit_hint > 0 && idx_out >= limit_hint)
                        break;
                }
                roaring_advance_uint32_iterator(iter);
            }
            roaring_free_uint32_iterator(iter);
        }
#else
        {
            uint64_t  array_count;
            uint32_t *indices = biscuit_roaring_to_array(result, &array_count);
            int       max_collect = (int) Min(collect_count, array_count);
            int       i;

            if (indices)
            {
                for (i = 0; i < max_collect; i++)
                {
                    if (indices[i] < (uint32_t) idx->num_records)
                    {
                        ItemPointerCopy(&idx->tids[indices[i]], &tids[idx_out]);
                        idx_out++;
                        if (limit_hint > 0 && idx_out >= limit_hint)
                            break;
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
}

/* ==================== SCAN HELPERS ==================== */

bool
biscuit_is_aggregate_query(IndexScanDesc scan)
{
    return !scan->xs_want_itup;
}

int
biscuit_estimate_limit_hint(IndexScanDesc scan)
{
    /* PostgreSQL's index AM interface does not expose LIMIT values */
    return -1;
}
