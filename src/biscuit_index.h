/*
 * biscuit_index.h
 * Index build, load, disk serialization, and CRUD helper declarations.
 */

#ifndef BISCUIT_INDEX_H
#define BISCUIT_INDEX_H

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_persist.h"

/* ==================== DISK I/O ==================== */

extern void biscuit_write_metadata_to_disk(Relation index, BiscuitIndex *idx);
extern bool biscuit_read_metadata_from_disk(Relation index,
                                            int *num_records,
                                            int *num_columns,
                                            int *max_len,
                                            uint64 *gen);

/* ==================== INDEX BUILD & LOAD ==================== */

extern IndexBuildResult *biscuit_build(Relation heap,
                                       Relation index,
                                       IndexInfo *indexInfo);
extern void              biscuit_buildempty(Relation index);
extern BiscuitIndex     *biscuit_load_index(Relation index);

/* ==================== CRUD HELPERS ==================== */

extern void biscuit_init_crud_structures(BiscuitIndex *idx);
extern void biscuit_push_free_slot(BiscuitIndex *idx, uint32_t slot);
extern bool biscuit_pop_free_slot(BiscuitIndex *idx, uint32_t *slot);

/*
 * Remove a single record from every character/length bitmap in the index.
 * Used by bulkdelete and update paths.
 */
extern void biscuit_remove_from_all_indices(BiscuitIndex *idx, uint32_t rec_idx);

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

