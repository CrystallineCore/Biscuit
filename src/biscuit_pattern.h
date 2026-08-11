/*
 * biscuit_pattern.h
 * LIKE / ILIKE pattern parsing, analysis, and bitmap-level matching
 * for single-column and multi-column Biscuit indexes.
 */

#ifndef BISCUIT_PATTERN_H
#define BISCUIT_PATTERN_H

#include "biscuit_common.h"
#include "biscuit_bitmap.h"

/* ==================== CHARINDEX BITMAP ACCESSORS ==================== */
/*
 * These functions are called from two different contexts, with two
 * different Relation-argument contracts (see "Biscuit WAL-Logged
 * Storage: Phase 1 Contract" §3):
 *
 *   - Read path (query evaluation, biscuit_pattern.c's own match_*
 *     recursive_windowed_match*\query_pattern* functions): pass the
 *     scan's real Relation. The returned bitmap is reconciled against
 *     that structure's not-yet-drained pending-list records
 *     (biscuit_reconcile_pending()) before being handed back -- either
 *     the same live, borrowed pointer (no pending records -- the common
 *     case, zero extra cost) or a fresh, context-scoped bitmap the
 *     caller does not need to explicitly free.
 *
 *   - Write path (biscuit_index.c's biscuit_index_single_record()/
 *     biscuit_index_column_record(), which need the actual live pointer
 *     to mutate in place via biscuit_roaring_add()): pass index == NULL.
 *     biscuit_reconcile_pending() treats NULL as "no reconciliation,
 *     return the raw cached pointer unchanged" -- exactly the pre-Phase-1
 *     behavior these two callers still depend on.
 *
 * The biscuit_set_*_bitmap() functions are unaffected -- they are
 * build/insert-time-only (populate the in-memory CharIndex/ColumnIndex
 * structures), never called from the read path, so they take no
 * Relation argument.
 */

extern RoaringBitmap *biscuit_get_pos_bitmap(Relation index, BiscuitIndex *idx, unsigned char ch, int pos);
extern RoaringBitmap *biscuit_get_neg_bitmap(Relation index, BiscuitIndex *idx, unsigned char ch, int neg_offset);
extern void           biscuit_set_pos_bitmap(BiscuitIndex *idx, unsigned char ch, int pos, RoaringBitmap *bm);
extern void           biscuit_set_neg_bitmap(BiscuitIndex *idx, unsigned char ch, int neg_offset, RoaringBitmap *bm);

extern RoaringBitmap *biscuit_get_pos_bitmap_lower(Relation index, BiscuitIndex *idx, unsigned char ch, int pos);
extern RoaringBitmap *biscuit_get_neg_bitmap_lower(Relation index, BiscuitIndex *idx, unsigned char ch, int neg_offset);
extern void           biscuit_set_pos_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int pos, RoaringBitmap *bm);
extern void           biscuit_set_neg_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int neg_offset, RoaringBitmap *bm);

/* ==================== COLUMN-LEVEL CHARINDEX ACCESSORS ==================== */
/*
 * Multi-column equivalents: operate on a ColumnIndex * directly instead of
 * routing through the top-level BiscuitIndex legacy fields. Used by
 * biscuit_index_column_record() during index build/insert and by the
 * multi-column query evaluation path in biscuit_pattern.c.
 *
 * col_idx: ColumnIndex itself carries no back-pointer to its own column
 * number, but that number is exactly what a structure's directory
 * identity (col, is_lower, kind, ch, position) needs -- so callers must
 * pass it alongside col. Same index==NULL write-path / real-Relation
 * read-path split as the single-column accessors above.
 */
extern RoaringBitmap *biscuit_get_col_pos_bitmap(Relation index, ColumnIndex *col, int col_idx,
                                                  unsigned char ch, int pos);
extern RoaringBitmap *biscuit_get_col_neg_bitmap(Relation index, ColumnIndex *col, int col_idx,
                                                  unsigned char ch, int neg_offset);
extern void           biscuit_set_col_pos_bitmap(ColumnIndex *col, unsigned char ch, int pos, RoaringBitmap *bm);
extern void           biscuit_set_col_neg_bitmap(ColumnIndex *col, unsigned char ch, int neg_offset, RoaringBitmap *bm);

extern RoaringBitmap *biscuit_get_col_pos_bitmap_lower(Relation index, ColumnIndex *col, int col_idx,
                                                        unsigned char ch, int pos);
extern RoaringBitmap *biscuit_get_col_neg_bitmap_lower(Relation index, ColumnIndex *col, int col_idx,
                                                        unsigned char ch, int neg_offset);
extern void           biscuit_set_col_pos_bitmap_lower(ColumnIndex *col, unsigned char ch, int pos, RoaringBitmap *bm);
extern void           biscuit_set_col_neg_bitmap_lower(ColumnIndex *col, unsigned char ch, int neg_offset, RoaringBitmap *bm);

/*
 * biscuit_get_negation_base_set
 * The reconciled "all non-null indexed rows of this column" set that
 * NOT LIKE / NOT ILIKE inverts against. Caller owns the result.
 *
 * Use this rather than reading col->length_ge_bitmaps[0] directly: the raw
 * array is not reconciled against the shared pending log, and mixing an
 * unreconciled base set with a reconciled subtrahend silently drops every
 * row whose membership is still undrained. See the implementation comment
 * for the freelist-reuse failure this fixed.
 */
extern RoaringBitmap *biscuit_get_negation_base_set(Relation index,
                                                     ColumnIndex *col,
                                                     int col_idx,
                                                     bool is_lower,
                                                     int num_records);

extern RoaringBitmap *biscuit_get_negation_base_set_legacy(Relation index,
                                                            BiscuitIndex *idx,
                                                            bool is_lower);

/* ==================== PATTERN PARSING ==================== */

extern ParsedPattern *biscuit_parse_pattern(const char *pattern);
extern void           biscuit_free_parsed_pattern(ParsedPattern *parsed);

/* ==================== SINGLE-COLUMN QUERY ==================== */

/* Case-sensitive LIKE */
extern RoaringBitmap *biscuit_query_pattern(Relation index, BiscuitIndex *idx, const char *pattern);

/* Case-insensitive ILIKE */
extern RoaringBitmap *biscuit_query_pattern_ilike(Relation index, BiscuitIndex *idx, const char *pattern);

/*
 * Mask-aware variants of the two functions above.
 *
 * mask, when non-NULL, is the running candidate set from any cheaper
 * scan keys already evaluated for this AND-conjunction (see
 * biscuit_build_query_plan() below and biscuit_rescan() in
 * biscuit_scan.c, which threads the shrinking result of each predicate
 * into the next one). An empty mask short-circuits immediately. Where
 * it's cheap to do so (the char_cache+scalar-verify branch for a pure
 * single-segment infix pattern, and the windowed-match sweep's initial
 * candidate set) the mask is intersected in *before* the expensive work
 * runs, not after -- that's the whole point: a 26-row mask means the
 * scalar verify touches 26 strings and the windowed sweep's per-position
 * bitmap ANDs touch a 26-row container, not the full row set.
 *
 * This is NOT a guarantee that the returned bitmap is fully ANDed with
 * mask on every code path -- masking above is a performance shortcut
 * applied only in the two expensive branches described above. Callers
 * must still AND the result with their own mask afterward for
 * correctness, exactly as they would with the unmasked functions'
 * results; biscuit_query_pattern()/biscuit_query_pattern_ilike() are
 * thin mask=NULL wrappers around these for every existing caller.
 */
extern RoaringBitmap *biscuit_query_pattern_masked(Relation index, BiscuitIndex *idx,
                                                    const char *pattern, const RoaringBitmap *mask);
extern RoaringBitmap *biscuit_query_pattern_ilike_masked(Relation index, BiscuitIndex *idx,
                                                          const char *pattern, const RoaringBitmap *mask);

/* ==================== MULTI-COLUMN QUERY ==================== */

/* Per-column case-sensitive LIKE */
extern RoaringBitmap *biscuit_query_column_pattern(Relation index,
                                                   BiscuitIndex *idx,
                                                   int col_idx,
                                                   const char *pattern);

/* Per-column case-insensitive ILIKE */
extern RoaringBitmap *biscuit_query_column_pattern_ilike(Relation index,
                                                         BiscuitIndex *idx,
                                                         int col_idx,
                                                         const char *pattern);

/* Mask-aware variants -- see biscuit_query_pattern_masked()'s comment
 * above for the mask contract, which applies identically here. */
extern RoaringBitmap *biscuit_query_column_pattern_masked(Relation index, BiscuitIndex *idx, int col_idx,
                                                           const char *pattern, const RoaringBitmap *mask);
extern RoaringBitmap *biscuit_query_column_pattern_ilike_masked(Relation index, BiscuitIndex *idx, int col_idx,
                                                                 const char *pattern, const RoaringBitmap *mask);

/* ==================== QUERY PLAN / OPTIMIZER ==================== */

extern QueryPlan *biscuit_build_query_plan(BiscuitIndex *idx,
                                           ScanKey keys, int nkeys);
extern void       biscuit_free_query_plan(QueryPlan *plan);

/*
 * biscuit_reconcile_scratch_cxt
 *
 * The current scan's scratch MemoryContext (BiscuitScanOpaque.scratch_cxt,
 * biscuit_common.h), for biscuit_reconcile_pending() (biscuit_pattern.c)
 * to anchor a freshly-copied reconciled bitmap's cleanup to, instead of
 * an assumption about how long the ambient CurrentMemoryContext happens
 * to live.
 *
 * Set near the top of every biscuit_rescan() call (biscuit_scan.c) to
 * that scan's own so->scratch_cxt before either candidate-building
 * helper runs -- which is the only place any call chain reaching
 * biscuit_reconcile_pending() originates from (the write path never
 * triggers reconciliation at all; see biscuit_reconcile_pending()'s own
 * comment). Never NULLed back out afterward: nothing ever reads this
 * outside a biscuit_rescan() call's own dynamic extent, and the next
 * biscuit_rescan() call (for this scan or another) overwrites it before
 * any reconciliation happens under the new value, so there is no stale-
 * read window to guard against, and no PG_TRY/PG_FINALLY save/restore
 * dance is needed for what is, in practice, never-reentrant use.
 *
 * NULL is a valid value (no scan currently reconciling, or an
 * unexpected call path) -- biscuit_reconcile_register_cleanup() falls
 * back to CurrentMemoryContext in that case.
 */
extern MemoryContext biscuit_reconcile_scratch_cxt;

#endif /* BISCUIT_PATTERN_H */
