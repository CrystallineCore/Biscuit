/*
 * biscuit_bitmap.h
 * Roaring bitmap abstraction layer declarations.
 * Wraps CRoaring when available, falls back to a simple bitset.
 */

#ifndef BISCUIT_BITMAP_H
#define BISCUIT_BITMAP_H

#include "biscuit_common.h"

/* ==================== BITMAP WRAPPERS ==================== */

extern RoaringBitmap *biscuit_roaring_create(void);
extern void           biscuit_roaring_add(RoaringBitmap *rb, uint32_t value);
extern void           biscuit_roaring_remove(RoaringBitmap *rb, uint32_t value);
extern uint64_t       biscuit_roaring_count(const RoaringBitmap *rb);
extern bool           biscuit_roaring_is_empty(const RoaringBitmap *rb);
extern void           biscuit_roaring_free(RoaringBitmap *rb);
extern RoaringBitmap *biscuit_roaring_copy(const RoaringBitmap *rb);
extern void           biscuit_roaring_and_inplace(RoaringBitmap *a, const RoaringBitmap *b);
extern void           biscuit_roaring_or_inplace(RoaringBitmap *a, const RoaringBitmap *b);
extern void           biscuit_roaring_andnot_inplace(RoaringBitmap *a, const RoaringBitmap *b);
extern uint32_t      *biscuit_roaring_to_array(const RoaringBitmap *rb, uint64_t *count);

/* ==================== MEMORY USAGE HELPERS ==================== */

extern size_t biscuit_roaring_memory_usage(const RoaringBitmap *rb);
extern size_t biscuit_charindex_memory_usage(const CharIndex *cidx);
extern size_t biscuit_columnindex_memory_usage(const ColumnIndex *col_idx);

#endif /* BISCUIT_BITMAP_H */