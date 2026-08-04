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

/*
 * biscuit_roaring_create_sized
 *
 * Create a bitmap that will hold values up to max_value.
 *
 * On the CRoaring build this is a no-op shim: containers are allocated per
 * 2^16 block on demand and there is no capacity to reserve. On the fallback
 * bitset the presize is real and it is not merely an optimisation --
 * biscuit_roaring_add() grows to (block + 1) * 2 with palloc0 + memcpy +
 * pfree, so a delta built in ascending slot order otherwise reallocates all
 * the way up.
 *
 * SIZE FROM THE MAXIMUM VALUE, NEVER FROM THE ELEMENT COUNT. Slots are
 * claimed monotonically and recycled, so a 500-element delta can contain
 * slot 900,000; a capacity derived from a count is large enough right up
 * until it is not, and on the fallback path that is a buffer overrun rather
 * than a mis-size.
 */
extern RoaringBitmap *biscuit_roaring_create_sized(uint32_t max_value);

extern void           biscuit_roaring_add(RoaringBitmap *rb, uint32_t value);

/* Membership test. NULL is treated as empty. */
extern bool           biscuit_roaring_contains(const RoaringBitmap *rb, uint32_t value);
extern void           biscuit_roaring_remove(RoaringBitmap *rb, uint32_t value);
extern uint64_t       biscuit_roaring_count(const RoaringBitmap *rb);
extern bool           biscuit_roaring_is_empty(const RoaringBitmap *rb);
extern void           biscuit_roaring_free(RoaringBitmap *rb);
extern RoaringBitmap *biscuit_roaring_copy(const RoaringBitmap *rb);
extern void           biscuit_roaring_and_inplace(RoaringBitmap *a, const RoaringBitmap *b);
extern void           biscuit_roaring_or_inplace(RoaringBitmap *a, const RoaringBitmap *b);
extern void           biscuit_roaring_andnot_inplace(RoaringBitmap *a, const RoaringBitmap *b);
extern uint32_t      *biscuit_roaring_to_array(const RoaringBitmap *rb, uint64_t *count);

/* ==================== SERIALIZATION ====================
 *
 * Needed by biscuit_blob.c's compacted-blob chunk chain: the chain itself
 * is deliberately bitmap-agnostic ("just bytes in, bytes out" -- see the
 * design doc §1), so the RoaringBitmap<->bytes conversion lives here
 * alongside the rest of the bitmap wrapper layer, not in biscuit_blob.c.
 */

/* Bytes biscuit_roaring_serialize() would produce for rb; for pre-sizing. */
extern uint32_t biscuit_roaring_serialized_size(const RoaringBitmap *rb);

/* Serialize rb into a palloc'd buffer in the current memory context. */
extern char    *biscuit_roaring_serialize(const RoaringBitmap *rb, uint32_t *out_len);

/* Deserialize len bytes at buf into a freshly palloc'd RoaringBitmap. */
extern RoaringBitmap *biscuit_roaring_deserialize(const char *buf, uint32_t len);

/* ==================== MEMORY USAGE HELPERS ==================== */

extern size_t biscuit_roaring_memory_usage(const RoaringBitmap *rb);
extern size_t biscuit_charindex_memory_usage(const CharIndex *cidx);
extern size_t biscuit_columnindex_memory_usage(const ColumnIndex *col_idx);

#endif /* BISCUIT_BITMAP_H */
