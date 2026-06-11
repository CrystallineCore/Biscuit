/*
 * biscuit_tid.h
 * TID sorting (radix + qsort) and parallel TID-collection declarations.
 */

#ifndef BISCUIT_TID_H
#define BISCUIT_TID_H

#include "biscuit_common.h"
#include "biscuit_bitmap.h"

/* Sort an array of TIDs for sequential heap access. */
extern void biscuit_sort_tids_by_block(ItemPointerData *tids, int count);

/* Single-threaded TID collection from a result bitmap. */
extern void biscuit_collect_sorted_tids_single(BiscuitIndex *idx,
                                               RoaringBitmap *result,
                                               ItemPointerData **out_tids,
                                               int *out_count,
                                               bool needs_sorting);

/* Parallel TID collection for large result sets. */
extern void biscuit_collect_sorted_tids_parallel(BiscuitIndex *idx,
                                                 RoaringBitmap *result,
                                                 ItemPointerData **out_tids,
                                                 int *out_count,
                                                 bool needs_sorting);

/*
 * Parallel worker entry point — registered via RegisterParallelWorkerMain()
 * and also called directly by the leader process to participate as an extra
 * worker.  The name string must match the one passed to CreateParallelContext.
 */
PGDLLEXPORT extern void biscuit_parallel_collect_worker(dsm_segment *seg, shm_toc *toc);

/* Detect whether a scan is aggregate-only (no tuple fetch needed). */
extern bool biscuit_is_aggregate_query(IndexScanDesc scan);

#endif /* BISCUIT_TID_H */
