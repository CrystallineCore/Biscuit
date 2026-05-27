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
 * These functions are called from biscuit_index.c to build the index.
 * They were previously static in biscuit_pattern.c; they are now extern
 * so both translation units can share them without a separate header.
 */

extern RoaringBitmap *biscuit_get_pos_bitmap(BiscuitIndex *idx, unsigned char ch, int pos);
extern RoaringBitmap *biscuit_get_neg_bitmap(BiscuitIndex *idx, unsigned char ch, int neg_offset);
extern void           biscuit_set_pos_bitmap(BiscuitIndex *idx, unsigned char ch, int pos, RoaringBitmap *bm);
extern void           biscuit_set_neg_bitmap(BiscuitIndex *idx, unsigned char ch, int neg_offset, RoaringBitmap *bm);

extern RoaringBitmap *biscuit_get_pos_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int pos);
extern RoaringBitmap *biscuit_get_neg_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int neg_offset);
extern void           biscuit_set_pos_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int pos, RoaringBitmap *bm);
extern void           biscuit_set_neg_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int neg_offset, RoaringBitmap *bm);

/* ==================== COLUMN-LEVEL CHARINDEX ACCESSORS ==================== */
/*
 * Multi-column equivalents: operate on a ColumnIndex * directly instead of
 * routing through the top-level BiscuitIndex legacy fields.
 * Used by biscuit_index_column_record() during index build and by the
 * biscuit_complete_preload_local() warm-up path.
 */
extern RoaringBitmap *biscuit_get_col_pos_bitmap(ColumnIndex *col, unsigned char ch, int pos);
extern RoaringBitmap *biscuit_get_col_neg_bitmap(ColumnIndex *col, unsigned char ch, int neg_offset);
extern void           biscuit_set_col_pos_bitmap(ColumnIndex *col, unsigned char ch, int pos, RoaringBitmap *bm);
extern void           biscuit_set_col_neg_bitmap(ColumnIndex *col, unsigned char ch, int neg_offset, RoaringBitmap *bm);

extern RoaringBitmap *biscuit_get_col_pos_bitmap_lower(ColumnIndex *col, unsigned char ch, int pos);
extern RoaringBitmap *biscuit_get_col_neg_bitmap_lower(ColumnIndex *col, unsigned char ch, int neg_offset);
extern void           biscuit_set_col_pos_bitmap_lower(ColumnIndex *col, unsigned char ch, int pos, RoaringBitmap *bm);
extern void           biscuit_set_col_neg_bitmap_lower(ColumnIndex *col, unsigned char ch, int neg_offset, RoaringBitmap *bm);

/* ==================== PATTERN PARSING ==================== */

extern ParsedPattern *biscuit_parse_pattern(const char *pattern);
extern void           biscuit_free_parsed_pattern(ParsedPattern *parsed);

/* ==================== SINGLE-COLUMN QUERY ==================== */

/* Case-sensitive LIKE */
extern RoaringBitmap *biscuit_query_pattern(BiscuitIndex *idx, const char *pattern);

/* Case-insensitive ILIKE */
extern RoaringBitmap *biscuit_query_pattern_ilike(BiscuitIndex *idx, const char *pattern);

/* ==================== MULTI-COLUMN QUERY ==================== */

/* Per-column case-sensitive LIKE */
extern RoaringBitmap *biscuit_query_column_pattern(BiscuitIndex *idx,
                                                   int col_idx,
                                                   const char *pattern);

/* Per-column case-insensitive ILIKE */
extern RoaringBitmap *biscuit_query_column_pattern_ilike(BiscuitIndex *idx,
                                                         int col_idx,
                                                         const char *pattern);

/* ==================== QUERY PLAN / OPTIMIZER ==================== */

extern QueryPlan *biscuit_build_query_plan(BiscuitIndex *idx,
                                           ScanKey keys, int nkeys);
extern void       biscuit_free_query_plan(QueryPlan *plan);

#endif /* BISCUIT_PATTERN_H */