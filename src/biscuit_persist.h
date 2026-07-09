/*
 * biscuit_persist.h
 * Disk snapshot persistence for BiscuitIndex.
 *
 * This is deliberately NOT crash-safe / WAL-integrated: it is a
 * best-effort snapshot intended for read-mostly workloads where the
 * cost we're trying to eliminate is "rebuild every bitmap from a full
 * heap scan on the first query after a cold backend/cache miss".
 *
 * If the snapshot is missing, unreadable, or version-mismatched, the
 * caller must fall back to biscuit_build()/biscuit_load_index()'s
 * normal from-heap rebuild -- correctness never depends on the
 * snapshot existing.
 */

#ifndef BISCUIT_PERSIST_H
#define BISCUIT_PERSIST_H

#include "biscuit_common.h"

/*
 * Write a full snapshot of idx to disk for RelationGetRelid(index).
 * Best-effort: logs a WARNING and returns without erroring if the
 * write fails (a missing/corrupt snapshot just means the next cold
 * load falls back to a full rebuild -- it must never take down an
 * INSERT/build).
 */
extern void biscuit_persist_save(Relation index, BiscuitIndex *idx);

/*
 * Try to load a previously-saved snapshot for this index into a
 * freshly palloc'd BiscuitIndex in CacheMemoryContext.
 *
 * Returns NULL (and leaves nothing behind) if there is no snapshot,
 * it's corrupt, or it was built by an incompatible version -- callers
 * must treat NULL as "go do a normal biscuit_build() rebuild".
 */
extern BiscuitIndex *biscuit_persist_load(Relation index);

/*
 * Remove the on-disk snapshot for this index (DROP INDEX / REINDEX).
 * Safe to call even if no snapshot exists.
 */
extern void biscuit_persist_drop(Oid indexoid);

#endif /* BISCUIT_PERSIST_H */
