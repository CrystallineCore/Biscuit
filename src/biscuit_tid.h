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
 * Lives in the parallel query DSM segment, written once by
 * biscuit_initparallelscan() and read by every worker.  Workers advance
 * next_chunk atomically; all other fields are read-only after init.
 *
 * Layout rules:
 *   • next_chunk must be naturally aligned for pg_atomic_uint64 (8 bytes).
 *   • pad[] ensures the hot atomic word and the read-only fields do not share
 *     a cache line, eliminating false-sharing between workers.
 *   • sizeof(BiscuitParallelScanDesc) is returned by
 *     biscuit_estimateparallelscan() so the executor reserves exactly the
 *     right amount of DSM space.
 */
#define BISCUIT_PARALLEL_CHUNK_SIZE_DEFAULT  1024

typedef struct BiscuitParallelScanDesc
{
    /*
     * Atomically incremented by each worker to claim the next chunk index.
     * A worker that reads a value >= total_chunks finds no work and exits.
     * Only pg_atomic_fetch_add_u64() is used — no spinlocks, no LWLocks.
     */
    pg_atomic_uint64  next_chunk;       /* next chunk index to claim         */

    uint64_t          total_chunks;     /* ceil(total_tids / chunk_size)     */
    uint32_t          chunk_size;       /* TIDs per chunk (default 1024)     */
    uint64_t          total_tids;       /* total live TIDs in this scan      */

    /*
     * Padding to a full cache line (64 bytes) so that next_chunk (written
     * by every worker) and the read-only fields above do not share a line.
     * Adjust if the fields above grow.
     */
    char              pad[64];
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
 * ABI-compatibility shim — delegates to biscuit_collect_sorted_tids_single().
 * The DSM-based parallel implementation has been removed; callers that
 * previously used this entry point continue to work without modification.
 */
extern void biscuit_collect_sorted_tids_parallel(BiscuitIndex *idx,
                                                 RoaringBitmap *result,
                                                 ItemPointerData **out_tids,
                                                 int *out_count,
                                                 bool needs_sorting);

/*
 * Parallel worker entry point — registered via RegisterParallelWorkerMain()
 * for dynamic-loader compatibility.  No longer launches real workers; body
 * is a no-op stub.  The name string must match the one used in any existing
 * CreateParallelContext calls to avoid lookup failures on extension reload.
 */
PGDLLEXPORT extern void biscuit_parallel_collect_worker(dsm_segment *seg, shm_toc *toc);

/* Detect whether a scan is aggregate-only (no tuple fetch needed). */
extern bool biscuit_is_aggregate_query(IndexScanDesc scan);

/* ==================== PARALLEL AM CALLBACKS ==================== */

/*
 * biscuit_estimateparallelscan
 *
 * AM callback: returns the number of bytes the executor must reserve in the
 * parallel DSM segment for the BiscuitParallelScanDesc.  Called once by the
 * leader before the segment is created.
 */
extern Size biscuit_estimateparallelscan(Relation indexRelation, int nworkers, int nchunks);

/*
 * biscuit_initparallelscan
 *
 * AM callback: called by the leader after the DSM segment has been created.
 * Initialises next_chunk to 0 and fills in the read-only fields.
 * @target  pointer to the BiscuitParallelScanDesc inside the DSM segment.
 *
 * Note: total_tids and chunk_size cannot be known at init time (the scan has
 * not yet been executed); they are set to sentinel values here and populated
 * by the first worker that enters biscuit_parallel_collect_chunk().  Only
 * next_chunk must be initialised to 0 at this stage so the executor can
 * safely call biscuit_parallelrescan() later.
 */
extern void biscuit_initparallelscan(void *target);

/*
 * biscuit_parallelrescan
 *
 * AM callback: resets next_chunk to 0 so the same DSM segment can be reused
 * for a rescan without re-allocating shared memory.
 */
extern void biscuit_parallelrescan(IndexScanDesc scan);

/* ==================== PARALLEL WORKER HELPERS ==================== */

/*
 * biscuit_claim_next_chunk
 *
 * Atomically claims the next available chunk from the shared descriptor.
 * Returns true and sets *chunk_start / *chunk_end to the TID range the
 * caller owns.  Returns false when all chunks have been claimed.
 *
 * Only pg_atomic_fetch_add_u64() is used; no locks of any kind.
 */
extern bool biscuit_claim_next_chunk(BiscuitParallelScanDesc *pdesc,
                                     uint64_t *chunk_start,
                                     uint64_t *chunk_end);

/*
 * biscuit_parallel_collect_chunk
 *
 * Worker loop.  Repeatedly calls biscuit_claim_next_chunk() and copies the
 * corresponding TID slice from all_tids (a worker-visible, read-only snapshot
 * of the full sorted TID array) into a worker-local palloc'd buffer pointed
 * to by *out_tids / *out_count.  The buffer is grown with repalloc() as
 * additional chunks are claimed.
 *
 * Workers never write to shared state other than through the atomic counter
 * inside BiscuitParallelScanDesc.
 */
extern void biscuit_parallel_collect_chunk(BiscuitIndex *idx,
                                           BiscuitParallelScanDesc *pdesc,
                                           ItemPointerData *all_tids,
                                           uint64_t total_tids,
                                           ItemPointerData **out_tids,
                                           int *out_count);

#endif /* BISCUIT_TID_H */
