/*
 * biscuit_index.h
 * Index build, load, disk serialization, and CRUD helper declarations.
 */

#ifndef BISCUIT_INDEX_H
#define BISCUIT_INDEX_H

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_persist.h"

/* ==================== OPCLASS CASE-MODE GATING ==================== */

/*
 * Determine which structure set(s) column `col` of `index` should be
 * built/maintained with, based on the opfamily its opclass belongs to
 * (biscuit_ops / biscuit_like_ops / biscuit_ilike_ops -- see biscuit.sql).
 * Returns BISCUIT_MODE_LIKE, BISCUIT_MODE_ILIKE, or BISCUIT_MODE_BOTH.
 * Falls back to BISCUIT_MODE_BOTH (the safe default -- build everything)
 * if the opfamily can't be resolved, e.g. for an opfamily this code
 * doesn't recognize.
 */
extern uint8 biscuit_get_column_case_mode(Relation index, int col);

/* ==================== DISK I/O ==================== */

extern void biscuit_write_metadata_to_disk(Relation index, BiscuitIndex *idx);
extern bool biscuit_read_metadata_from_disk(Relation index,
                                            int *num_records,
                                            int *num_columns,
                                            int *max_len,
                                            uint64 *gen);

/*
 * biscuit_read_pending_stats
 * Share-locked read of the metapage's pending-list observability fields:
 * the configured per-structure drain threshold, the last-known total
 * undrained bytes across every structure's pending chain (refreshed once
 * per VACUUM, see BiscuitMetaPageData.total_pending_bytes's field
 * comment), and the lifetime count of drains performed. Returns false
 * (with all three out-params set to safe defaults/zero) if the relation
 * has no usable metapage yet. Used by biscuit_index_stats() and
 * biscuit_pending_list_stats() (biscuit.c) for operational visibility
 * into unmerged write volume.
 */
extern bool biscuit_read_pending_stats(Relation index,
                                       uint32 *pending_list_limit,
                                       uint64 *total_pending_bytes,
                                       uint64 *total_drains);

/* ==================== INDEX BUILD & LOAD ==================== */

extern IndexBuildResult *biscuit_build(Relation heap,
                                       Relation index,
                                       IndexInfo *indexInfo);
extern void              biscuit_buildempty(Relation index);
extern BiscuitIndex     *biscuit_load_index(Relation index);

/*
 * biscuit_get_current_index
 *
 * BLOCKER-1 fix (cross-backend visibility of committed inserts/updates/
 * deletes). Resolves `index` through the per-backend biscuit_cache like
 * biscuit_load_index() does on a miss, but on a cache HIT additionally
 * compares the cached copy's generation against the metapage's
 * authoritative, durably-persisted generation counter (a single
 * share-locked metapage read) and transparently evicts + reloads from
 * disk if another backend has committed a mutation since this backend's
 * copy was built. Every caller that previously did:
 *
 *     idx = biscuit_cache_lookup(RelationGetRelid(index));
 *     if (!idx) idx = biscuit_load_index(index);
 *
 * should use this instead -- both read paths (biscuit_beginscan()/
 * biscuit_rescan() in biscuit_scan.c) and write paths (biscuit_insert()/
 * biscuit_bulkdelete() here) need it: a writer that mutates from a stale
 * base would otherwise bump its own idx->gen from behind, and the Max()
 * carry-forward in biscuit_write_metadata_to_disk() would silently
 * swallow that bump, breaking the counter's monotonicity for every other
 * backend's freshness check. See the function definition in
 * biscuit_index.c for the full root-cause writeup.
 */
extern BiscuitIndex     *biscuit_get_current_index(Relation index);

/*
 * biscuit_resync_gen_after_self_drain
 *
 * Call this immediately after this backend has itself driven a pendlog
 * compaction/drain to completion (biscuit_pendlog_compact() or
 * biscuit_pendlog_drain_all()), whether triggered synchronously from the
 * append path or deferred out of an open batch
 * (biscuit_pendlog_batch_end(), biscuit_pendlog.c). Both of those bump
 * the metapage's generation counter unconditionally, which is correct
 * for every OTHER backend's staleness check but wrong for this one: idx
 * is already the complete, current truth (the write path's in-memory
 * fan-out is unconditional, independent of what gets drained), so
 * without this call this backend's own next biscuit_get_current_index()
 * would see disk_gen > idx->gen and wrongly discard-and-reload from the
 * durable HEADER snapshot, which lags any row written earlier in an
 * in-flight statement. `idx` may be NULL (e.g. looked up via
 * biscuit_cache_lookup() and missing), in which case this is a no-op.
 * See the definition in biscuit_index.c for the full writeup.
 */
extern void               biscuit_resync_gen_after_self_drain(Relation index, BiscuitIndex *idx);

/* ==================== CRUD HELPERS ==================== */

extern void biscuit_init_crud_structures(BiscuitIndex *idx);
extern void biscuit_push_free_slot(BiscuitIndex *idx, uint32_t slot);

/*
 * biscuit_pop_free_slot
 *
 * Currently has NO callers. biscuit_insert() used to reuse tombstoned
 * slots through this, but idx->free_list is process-local (rebuilt from
 * the durable FREELIST blob on load, then mutated only in this backend's
 * copy), so two backends both holding slot 42 on their lists would both
 * pop it for two different rows -- the same silent-overwrite collision as
 * the num_records race that biscuit_claim_new_slot() fixes, just sourced
 * from the other allocator. Taking the metapage lock around the pop does
 * not help while the list being popped from is still private.
 *
 * Retained (rather than deleted) because it is the natural shape of the
 * eventual fix: a metapage-anchored durable freelist claimed under the
 * same lock as biscuit_claim_new_slot(), at which point this function
 * becomes its shared-state counterpart. biscuit_push_free_slot() is still
 * live -- biscuit_bulkdelete() keeps the durable list accurate so that
 * work has the data it needs.
 */
extern bool biscuit_pop_free_slot(BiscuitIndex *idx, uint32_t *slot);

/*
 * Remove a single record from every character/length bitmap in the index.
 * Used by bulkdelete and update (UPDATE-as-delete-then-insert) paths.
 *
 * index / pending_list_limit: per the mutation contract ("Biscuit
 * WAL-Logged Storage: Phase 1 Contract" §1), every steady-state removal
 * durably records itself via a pending-list append against each touched
 * structure's directory entry, in addition to the existing in-memory
 * biscuit_roaring_remove(). Both are always required now -- there is no
 * build-mode/NULL-index case for this function (contrast
 * biscuit_index_single_record()/biscuit_index_column_record(), which are
 * shared between the one-time build path and steady-state insert and so
 * do have one). pending_list_limit is the statement-cached
 * BiscuitMetaPageData.pending_list_limit (biscuit_read_pending_list_limit()),
 * read once per statement by the caller.
 */
extern void biscuit_remove_from_all_indices(Relation index, BiscuitIndex *idx,
                                             uint32_t rec_idx, uint32 pending_list_limit);

/* ==================== AM CALLBACKS ==================== */

extern bool biscuit_insert(Relation index,
                           Datum *values,
                           bool *isnull,
                           ItemPointer ht_ctid,
                           Relation heapRelation,
                           IndexUniqueCheck checkUnique,
                           bool indexUnchanged,
                           IndexInfo *indexInfo);

extern IndexBulkDeleteResult *biscuit_bulkdelete(IndexVacuumInfo *info,
                                                  IndexBulkDeleteResult *stats,
                                                  IndexBulkDeleteCallback callback,
                                                  void *callback_state);

extern IndexBulkDeleteResult *biscuit_vacuumcleanup(IndexVacuumInfo *info,
                                                     IndexBulkDeleteResult *stats);

extern bool biscuit_canreturn(Relation index, int attno);

extern void biscuit_costestimate(PlannerInfo *root,
                                 IndexPath *path,
                                 double loop_count,
                                 Cost *indexStartupCost,
                                 Cost *indexTotalCost,
                                 Selectivity *indexSelectivity,
                                 double *indexCorrelation,
                                 double *indexPages);

extern bytea *biscuit_options(Datum reloptions, bool validate);
extern bool   biscuit_validate(Oid opclassoid);
extern void   biscuit_adjustmembers(Oid opfamilyoid, Oid opclassoid,
                                    List *operators, List *functions);

#endif /* BISCUIT_INDEX_H */
