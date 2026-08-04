/*
 * biscuit_persist.h
 * Disk snapshot persistence for BiscuitIndex.
 *
 * This is deliberately NOT crash-safe / WAL-integrated: it is a
 * best-effort snapshot intended for read-mostly workloads where the
 * cost we're trying to eliminate is "rebuild every bitmap from a full
 * heap scan on the first query after a cold backend/cache miss".
 *
 * If the snapshot is missing, unreadable, version-mismatched, or
 * stale (its recorded generation doesn't match the index's current
 * generation in the metapage), the caller must fall back to
 * biscuit_build()/biscuit_load_index()'s normal from-heap rebuild --
 * correctness never depends on the snapshot existing.
 */

#ifndef BISCUIT_PERSIST_H
#define BISCUIT_PERSIST_H

#include "biscuit_common.h"

/*
 * Write a full snapshot of idx to disk for indexoid.
 * Best-effort: logs a WARNING and returns without erroring if the
 * write fails (a missing/corrupt snapshot just means the next cold
 * load falls back to a full rebuild -- it must never take down an
 * INSERT/build).
 *
 * Takes a bare Oid rather than a Relation because the caller that
 * flushes dirty snapshots at backend exit (biscuit_cache.c) only has
 * an Oid on hand -- opening a relation that late in backend shutdown
 * is not safe. The function body never needed anything from Relation
 * beyond its Oid anyway.
 */
extern void biscuit_persist_save(Oid indexoid, BiscuitIndex *idx);

/*
 * Try to load a previously-saved snapshot for this index into a
 * freshly palloc'd BiscuitIndex in CacheMemoryContext.
 *
 * Returns NULL (and leaves nothing behind) if there is no snapshot,
 * it's corrupt, it was built by an incompatible version, or its
 * recorded generation doesn't match the index's current generation
 * in the metapage (i.e. it's stale relative to mutations that landed
 * since it was written) -- callers must treat NULL as "go do a
 * normal biscuit_build() rebuild". On success, idx->gen and
 * idx->gen_at_last_snapshot are both initialized to the snapshot's
 * generation.
 */
extern BiscuitIndex *biscuit_persist_load(Relation index);

/*
 * biscuit_persist_save_row_identity
 *
 * As of the in-place row-identity rewrite (biscuit_rowstore.c) this ONLY
 * re-persists the HEADER blob (num_records, capacity, counters, ...).
 * TIDS and STRCACHE used to be rewritten here too
 * (the whole ItemPointerData[]/string array, every call -- O(num_records)
 * per commit, the root cause fixed by biscuit_rowstore.c: 300 single-row
 * transactions cost 70x more WAL/row than the same 300 rows batched into
 * one transaction). They are now made durable incrementally, in place, at
 * the exact moment each row's slot is written -- see
 * biscuit_persist_row_identity_write_record() below -- so there is nothing
 * left for a once-per-statement batch pass to do for them.
 *
 * HEADER stays on the once-per-statement deferred-flush path (see
 * biscuit_index.c's biscuit_mark_row_identity_dirty() /
 * biscuit_flush_dirty_row_identity()) because it's a single scalar blob --
 * rewriting it once per statement instead of once per row is a trivial,
 * unconditionally safe amortization, not a workaround for anything.
 * Internally it's now also just an in-place single-page overwrite
 * (biscuit_rowstore_header_write()) rather than a chain reallocation, so
 * this call is O(1), not O(num_records), unlike its pre-rewrite self.
 *
 * Callers already hold an open Relation and should call this once per
 * statement (see biscuit_insert()).
 */
extern void biscuit_persist_save_row_identity(Relation index, BiscuitIndex *idx);

/*
 * biscuit_persist_row_identity_write_record
 *
 * Durably write TIDS[slot_idx] and every (column, is_lower) STRCACHE
 * entry for slot_idx, in place, from idx's current in-memory state
 * (idx->tids[slot_idx], idx->data_cache[slot_idx]/idx->column_data_cache[..]
 * [slot_idx]). Call this immediately after the in-memory assignment for
 * every row a steady-state INSERT (fresh slot, freelist-slot reuse, or a
 * slot landing on a not-yet-allocated logical page -- all three cases are
 * handled uniformly by biscuit_rowstore.c) or the build-time load path
 * writes -- there is no separate build-mode/NULL-index special case here
 * (contrast biscuit_index_single_record()/biscuit_index_column_record(),
 * which are shared between build and steady-state insert and so do have
 * one): both paths need this row durable the same way.
 *
 * O(1) per call (a handful of single-page GenericXLog writes), which is
 * the entire point -- see biscuit_persist_save_row_identity()'s comment
 * for what this replaced.
 *
 * `mode` is passed straight through to biscuit_rowstore_tid_write() as
 * the caller's assertion about the slot's expected prior state (see
 * BiscuitSlotWriteMode in biscuit_common.h). Pass
 * BISCUIT_SLOT_WRITE_FRESH only from a call site that just claimed the
 * slot via biscuit_claim_new_slot(); every rewrite of a slot the caller
 * already owns (the UPDATE path, the bulkdelete clear loop, the
 * whole-snapshot bulk save) passes BISCUIT_SLOT_WRITE_INPLACE.
 */
extern void biscuit_persist_row_identity_write_record(Relation index,
                                                        BiscuitIndex *idx,
                                                        uint32 slot_idx,
                                                        BiscuitSlotWriteMode mode);

/*
 * biscuit_rowstore_alloc_lock / biscuit_rowstore_alloc_unlock
 *
 * The heavyweight page lock (LockPage on BISCUIT_ROWSTORE_LOCK_BLKNO)
 * biscuit_persist_row_identity_write_record() holds across its whole
 * BiscuitXlogBatch -- TIDS plus every column's STRCACHE slot for one row.
 * See its definition in biscuit_persist.c for the full scope/ordering
 * contract.
 *
 * Exported (rather than static to biscuit_persist.c) solely so
 * biscuit_pendlog.c's drain can take it too, around the span in which it
 * reads STRCACHE back out via biscuit_delta_expand_slots()
 * (pendlog_drain_internal(), biscuit_pendlog.c). That read is a plain
 * LWLock (LockBuffer SHARE), invisible to the deadlock detector; without
 * this shared lock, a drain in one backend and an open batch in another
 * could each block on an LWLock the other holds, with no heavyweight edge
 * connecting them for the detector to see -- a silent hang instead of a
 * caught-and-aborted deadlock. Do not call this from anywhere else:
 * ordering is total only because the drain is the sole caller that nests
 * it inside BISCUIT_METAPAGE_BLKNO, and an ordinary row-identity write
 * never takes BISCUIT_METAPAGE_BLKNO at all. A third caller with its own
 * nesting order would reopen exactly the hazard this closes.
 */
extern void biscuit_rowstore_alloc_lock(Relation index);
extern void biscuit_rowstore_alloc_unlock(Relation index);

/*
 * Remove the on-disk snapshot for this index (DROP INDEX / REINDEX).
 * Safe to call even if no snapshot exists.
 */
extern void biscuit_persist_drop(Oid indexoid);

#endif /* BISCUIT_PERSIST_H */
