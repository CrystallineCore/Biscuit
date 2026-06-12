/*
 * biscuit_tid.h
 * TID sorting (radix + qsort) and TID-collection declarations.
 *
 * TIDCollectionWorker and biscuit_collect_tids_worker() have been removed:
 * the parallel collection path that required them was replaced by a
 * single-threaded implementation that eliminated a 3× performance regression.
 *
 * Parallel index scan support (amparallelrescan infrastructure):
 *   BiscuitParallelScanDesc  – shared-memory descriptor placed in the
 *                              parallel query DSM segment by aminitparallelscan.
 *   biscuit_estimateparallelscan() – reports its size to the executor.
 *   biscuit_initparallelscan()     – initialises the atomic chunk counter.
 *   biscuit_parallelrescan()       – resets the counter for a new scan.
 *   biscuit_claim_next_chunk()     – atomically claims the next chunk slot.
 *   biscuit_parallel_collect_chunk() – worker loop; writes only into a
 *                                      worker-local palloc'd buffer.
 */

#ifndef BISCUIT_TID_H
#define BISCUIT_TID_H

#include "biscuit_common.h"
#include "biscuit_bitmap.h"

/* ==================== PARALLEL SCAN DESCRIPTOR ==================== */

/*
 * BiscuitParallelScanDesc
 *
 * Lives in the parallel query DSM segment.  Carries the pre-computed
 * per-participant TID partition table so each worker can self-identify and
 * read its own disjoint [start, end) range without any runtime coordination.
 *
 * Parallel design — pre-partition, self-identify, local evaluate
 * --------------------------------------------------------------
 * PostgreSQL parallel workers are separate OS processes with disjoint virtual
 * address spaces.  Only memory inside this DSM struct is shared.
 *
 * Key observations that drive the design:
 *
 *   A. The bitmap is read-only and deterministic: every process that evaluates
 *      it against the same index with the same query produces an identical
 *      sorted TID array.  So every participant can evaluate locally — no need
 *      to ship the TID array across process boundaries.
 *
 *   B. MyParallelWorkerNumber is a process-local int set by PostgreSQL before
 *      the first AM callback fires.  The leader has value -1; workers have
 *      0, 1, 2, … in launch order.  This gives each participant a stable,
 *      unique identity.
 *
 *   C. The executor tells us the worker count at estimateparallelscan /
 *      initparallelscan time, before any bitmap evaluation.
 *
 * Putting it together:
 *
 *   Init (biscuit_initparallelscan):
 *     • Record num_participants = 1 (leader) + nworkers.
 *     • Set initialized = 0.  All slot [start,end) pairs remain 0.
 *
 *   First rescan (any participant — whoever calls biscuit_rescan first):
 *     • CAS initialized: 0 → 1.  Winner evaluates the bitmap, computes the
 *       full sorted TID count, and fills slots[0..num_participants-1] with
 *       pre-computed [start, end) ranges.  The last slot absorbs any remainder
 *       so uneven divisions are handled correctly.  Then sets initialized = 2.
 *     • All other participants spin on initialized until it reaches 2.
 *       No per-TID coordination ever happens.
 *
 *   Each rescan:
 *     • Participant reads MyParallelWorkerNumber → slot index
 *         leader  (-1) → slot 0
 *         worker N     → slot N+1
 *     • Evaluates bitmap locally, returns only tids[slots[i].start .. slots[i].end).
 *
 *   Rescan (biscuit_parallelrescan):
 *     • Reset initialized = 0 and clear all slots so the next scan re-partitions.
 *
 * No atomic chunk counter.  No spin on chunk claims.  No false-sharing.
 * The only shared writes are the one-time partition computation.
 */

/* Maximum participants (1 leader + up to this many workers). */
#define BISCUIT_MAX_PARALLEL_WORKERS  64

/*
 * BiscuitWorkerSlot — the TID index range assigned to one participant.
 * start is inclusive, end is exclusive.  Both are indices into the local
 * sorted TID array (identical across all processes for a given scan).
 */
typedef struct BiscuitWorkerSlot
{
    uint64_t    start;      /* first TID index for this participant */
    uint64_t    end;        /* one past the last TID index          */
} BiscuitWorkerSlot;

/*
 * initialized state values
 *   0 – partition table not yet computed.
 *   1 – computation in progress (one participant holds the lock).
 *   2 – partition table ready; all participants may proceed.
 */
typedef struct BiscuitParallelScanDesc
{
    pg_atomic_uint32  initialized;      /* 0=uninit, 1=computing, 2=ready    */
    int32             num_participants; /* leader + workers                  */
    uint64_t          total_tids;       /* filled in by the initializer      */

    /*
     * Per-participant TID ranges.  Slot 0 is always the leader; slot i+1
     * belongs to background worker i (MyParallelWorkerNumber == i).
     * The last slot's end is clamped to total_tids to absorb any remainder.
     */
    BiscuitWorkerSlot slots[BISCUIT_MAX_PARALLEL_WORKERS + 1];
} BiscuitParallelScanDesc;

/* Sort an array of TIDs for sequential heap access. */
extern void biscuit_sort_tids_by_block(ItemPointerData *tids, int count);

/* Single-threaded TID collection from a result bitmap. */
extern void biscuit_collect_sorted_tids_single(BiscuitIndex *idx,
                                               RoaringBitmap *result,
                                               ItemPointerData **out_tids,
                                               int *out_count,
                                               bool needs_sorting);


/*
 * biscuit_collect_sorted_tids_parallel
 *
 * Parallel-aware TID collection.  Called by EVERY participant (leader and
 * background workers) from biscuit_rescan() whenever scan->parallel_scan
 * is non-NULL.
 *
 * Algorithm
 * ---------
 * 1. The first participant to arrive CASes pdesc->initialized from 0 → 1,
 *    evaluates the bitmap, computes the full sorted TID count, divides it
 *    evenly into pdesc->num_participants slices, writes each [start, end)
 *    into pdesc->slots[], then sets initialized = 2.  The last slot absorbs
 *    any remainder so uneven divisions are always correct.
 *
 * 2. All other participants spin on initialized until it reaches 2 (with
 *    CHECK_FOR_INTERRUPTS and a 1 µs sleep to avoid burning a core).
 *
 * 3. Every participant reads its own slot using its stable worker identity:
 *       leader  (MyParallelWorkerNumber == -1) → slot 0
 *       worker N (MyParallelWorkerNumber ==  N) → slot N+1
 *
 * 4. Each participant evaluates the bitmap locally (identical result for all
 *    — deterministic read-only operation) and returns only the TIDs in its
 *    assigned [start, end) range.
 *
 * No per-TID atomic operations.  No chunk claiming races.  The Gather node
 * assembles exactly one copy of the full result from the disjoint per-process
 * slices.
 *
 * When pdesc is NULL the function is identical to
 * biscuit_collect_sorted_tids_single() (non-parallel path).
 */
extern void biscuit_collect_sorted_tids_parallel(BiscuitIndex *idx,
                                                  RoaringBitmap *result,
                                                  BiscuitParallelScanDesc *pdesc,
                                                  ItemPointerData **out_tids,
                                                  int *out_count,
                                                  bool needs_sorting);

/*
 * Parallel worker entry point — registered via RegisterParallelWorkerMain()
 * for dynamic-loader compatibility.  Body is a no-op stub; retained so
 * symbol lookup on extension reload does not fail.
 */
PGDLLEXPORT extern void biscuit_parallel_collect_worker(dsm_segment *seg, shm_toc *toc);

PGDLLIMPORT extern int ParallelWorkerNumber;
/* Detect whether a scan is aggregate-only (no tuple fetch needed). */
extern bool biscuit_is_aggregate_query(IndexScanDesc scan);

/* ==================== PARALLEL AM CALLBACKS ==================== */

/*
 * biscuit_estimateparallelscan
 *
 * AM callback: returns sizeof(BiscuitParallelScanDesc) so the executor
 * reserves exactly the right amount of DSM space.  nworkers is also stored
 * so biscuit_initparallelscan can record num_participants.
 */
extern Size biscuit_estimateparallelscan(Relation indexRelation, int nworkers, int nchunks);

/*
 * biscuit_initparallelscan
 *
 * AM callback: zeroes the descriptor and records num_participants so the
 * partition table can be sized correctly at first rescan.  Sets
 * initialized = 0 (not yet computed).
 */
extern void biscuit_initparallelscan(void *target);

/*
 * biscuit_parallelrescan
 *
 * AM callback: resets initialized = 0 and clears all slots so the next
 * rescan re-partitions from scratch (handles Materialize node rewinds).
 */
extern void biscuit_parallelrescan(IndexScanDesc scan);

/**
 * biscuit_get_parallel_worker_count
 * * Returns the number of parallel workers planned for this index scan.
 * Returns 0 if this is a single-threaded execution or if parallel 
 * scan state is not initialized.
 */
extern int biscuit_get_parallel_worker_count(IndexScanDesc scan);

#endif /* BISCUIT_TID_H */
