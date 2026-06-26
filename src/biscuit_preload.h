/*
 * biscuit_preload.h
 * Background index preloader declarations.
 *
 * On the first query after a restart, biscuit_beginscan returns a
 * skeleton index immediately (TIDs + data cache loaded, bitmaps empty)
 * so the caller is unblocked.  Simultaneously, a background worker
 * (BiscuitPreloadWorker) is launched to rebuild all bitmaps into
 * CacheMemoryContext.  Subsequent queries find the index fully warm.
 *
 * Thread-safety contract
 * ----------------------
 * The foreground (query) path checks idx->preload_state.  While that
 * value is below BISCUIT_PRELOAD_DONE it falls back to a full
 * sequential scan of data_cache / column_data_cache for matching (slow
 * but correct).  Once the worker sets the flag the fast bitmap path is
 * used automatically.
 *
 * The worker and the foreground process live in *separate OS processes*
 * (PostgreSQL background worker model), so there is no shared memory
 * race: the worker writes into its own backend's CacheMemoryContext
 * copy of the index and then signals the owning session via a named
 * latch.  The owning session re-caches the fully-loaded index on next
 * access.
 */

#ifndef BISCUIT_PRELOAD_H
#define BISCUIT_PRELOAD_H

#include "biscuit_common.h"

/* ================================================================
 * Preload status values stored in BiscuitIndex.preload_state.
 * ================================================================ */

#define BISCUIT_PRELOAD_NONE       0   /* never started               */
#define BISCUIT_PRELOAD_SKELETON   1   /* skeleton loaded, worker pending */
#define BISCUIT_PRELOAD_RUNNING    2   /* worker is building bitmaps  */
#define BISCUIT_PRELOAD_DONE       3   /* fully warm, use bitmap path */
#define BISCUIT_PRELOAD_FAILED     4   /* worker crashed; use seq scan */

/* ================================================================
 * Shared-memory control block
 * Allocated once in _PG_init via ShmemInitStruct.
 * ================================================================ */
typedef struct BiscuitPreloadShmem
{
    /* Queue of index OIDs waiting to be preloaded (ring buffer). */
    Oid     queue[64];
    int     queue_head;         /* next slot to dequeue             */
    int     queue_tail;         /* next slot to enqueue             */
    int     queue_size;         /* current item count               */

    /* Latch owned by the preload worker process. */
    Latch  *worker_latch;

    /*
     * Per-OID state tracking, indexed by (OID % 64).
     *
     * slot_state[] is keyed by OID modulo 64 — NOT by ring-buffer position.
     * This means the state remains readable even after the worker dequeues
     * the OID (advancing queue_head past it).  biscuit_preload_state() reads
     * slot_state[indexoid % 64] directly, with no ring-buffer scan.
     *
     * Collision risk: two distinct OIDs that share the same slot (OID % 64).
     * We track the OID stored in each slot so callers can detect this and
     * fall back to BISCUIT_PRELOAD_NONE safely.
     */
    pg_atomic_uint32 slot_state[64];  /* BISCUIT_PRELOAD_* for OID % 64  */
    pg_atomic_uint32 slot_oid[64];    /* OID currently owning the slot    */
} BiscuitPreloadShmem;

/* ================================================================
 * API
 * ================================================================ */

/*
 * Size of the shared-memory segment used by the preloader.
 * Must be called from the shmem_request_hook.
 */
extern Size biscuit_preload_shmem_size(void);

/*
 * Called from _PG_init to register the background worker and allocate
 * shared memory.  Safe to call multiple times (idempotent).
 */
extern void biscuit_preload_init(void);

/*
 * Request background preloading for the given index OID.
 * Returns immediately; the worker does the heavy lifting.
 * If the worker is not available (e.g. max_worker_processes reached),
 * this is a no-op — the skeleton will remain and the fallback seq-scan
 * path handles queries until a full load happens lazily.
 */
extern void biscuit_preload_request(Oid indexoid);

/*
 * Return the current BISCUIT_PRELOAD_* state for the given index OID
 * as recorded in the shared-memory ring buffer.
 * Returns BISCUIT_PRELOAD_NONE when the OID is not in the queue.
 * Cheap: reads an atomic uint32, no lock.
 */
extern uint32 biscuit_preload_state(Oid indexoid);

/*
 * Build just the skeleton: TIDs and data_cache populated, all bitmap
 * fields zeroed/NULL.  preload_state is set to BISCUIT_PRELOAD_SKELETON.
 * Returns the new BiscuitIndex; caller must cache it.
 */
extern BiscuitIndex *biscuit_load_skeleton(Relation index);

/*
 * Complete the bitmap population for an already-skeleton index.
 * Called from the background worker.  When done, sets preload_state to
 * BISCUIT_PRELOAD_DONE.
 */
extern void biscuit_complete_preload(Oid indexoid);

/*
 * Complete the bitmap population using a skeleton BiscuitIndex that the
 * calling backend has already built (data_cache / column_data_cache
 * populated, all bitmap fields NULL).  Does NOT open the relation or do
 * a heap scan — it indexes only from the in-memory string cache.
 *
 * On success sets idx->preload_state = BISCUIT_PRELOAD_DONE and writes
 * DONE into the shared-memory slot so other backends see it.
 *
 * Used by the foreground rescan path when it detects that the worker has
 * finished: instead of calling biscuit_load_index() (full heap scan +
 * bitmap build), it calls this on the skeleton it already has in memory,
 * building bitmaps in O(total-string-bytes) time with no extra I/O.
 */
extern void biscuit_complete_preload_local(BiscuitIndex *idx, Oid indexoid);

/*
 * Fallback scan used while preload_state < BISCUIT_PRELOAD_DONE.
 * Walks data_cache / column_data_cache and matches pattern using
 * pg_pattern_fixed_prefix / plain strstr — no bitmaps.
 * Writes matching TIDs into *out_tids / *out_count (palloc'd).
 */
extern void biscuit_fallback_scan(BiscuitIndex *idx,
                                  const char   *pattern,
                                  bool          ilike,
                                  int           col_idx,
                                  ItemPointerData **out_tids,
                                  int          *out_count);

/*
 * Background worker main entry point (registered with
 * RegisterBackgroundWorker).
 */
extern void biscuit_preload_worker_main(Datum main_arg);

#endif /* BISCUIT_PRELOAD_H */
