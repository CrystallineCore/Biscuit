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
 * Unified entry point: chooses parallel vs. single-threaded automatically
 * and supports an optional LIMIT hint (pass -1 for "no limit").
 */
extern void biscuit_collect_tids_optimized(BiscuitIndex *idx,
                                           RoaringBitmap *result,
                                           ItemPointerData **out_tids,
                                           int *out_count,
                                           bool needs_sorting,
                                           int limit_hint);

/* Detect whether a scan is aggregate-only (no tuple fetch needed). */
extern bool biscuit_is_aggregate_query(IndexScanDesc scan);

/* Estimate a LIMIT hint from a scan descriptor (-1 = unknown). */
extern int  biscuit_estimate_limit_hint(IndexScanDesc scan);

#endif /* BISCUIT_TID_H */
