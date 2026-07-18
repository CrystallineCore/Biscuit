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

/* ==================== PATTERN PARSING ==================== */

extern ParsedPattern *biscuit_parse_pattern(const char *pattern);
extern void           biscuit_free_parsed_pattern(ParsedPattern *parsed);

/* ==================== SINGLE-COLUMN QUERY ==================== */

/* Case-sensitive LIKE */
extern RoaringBitmap *biscuit_query_pattern(Relation index, BiscuitIndex *idx, const char *pattern);

/* Case-insensitive ILIKE */
extern RoaringBitmap *biscuit_query_pattern_ilike(Relation index, BiscuitIndex *idx, const char *pattern);

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

/* ==================== QUERY PLAN / OPTIMIZER ==================== */

extern QueryPlan *biscuit_build_query_plan(BiscuitIndex *idx,
                                           ScanKey keys, int nkeys);
extern void       biscuit_free_query_plan(QueryPlan *plan);

#endif /* BISCUIT_PATTERN_H */
