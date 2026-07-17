/*
 * biscuit_bitmap.c
 * Roaring bitmap abstraction layer.
 *
 * When compiled with HAVE_ROARING, every operation delegates to the
 * CRoaring library.  Otherwise a simple uint64_t block-array fallback
 * is used so the extension can be built without the external library.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"

/* ==================== ROARING WRAPPERS ==================== */

#ifdef HAVE_ROARING

RoaringBitmap *
biscuit_roaring_create(void)
{
    return roaring_bitmap_create();
}

void
biscuit_roaring_add(RoaringBitmap *rb, uint32_t value)
{
    roaring_bitmap_add(rb, value);
}

void
biscuit_roaring_remove(RoaringBitmap *rb, uint32_t value)
{
    roaring_bitmap_remove(rb, value);
}

uint64_t
biscuit_roaring_count(const RoaringBitmap *rb)
{
    return roaring_bitmap_get_cardinality(rb);
}

bool
biscuit_roaring_is_empty(const RoaringBitmap *rb)
{
    return roaring_bitmap_get_cardinality(rb) == 0;
}

void
biscuit_roaring_free(RoaringBitmap *rb)
{
    if (rb)
        roaring_bitmap_free(rb);
}

RoaringBitmap *
biscuit_roaring_copy(const RoaringBitmap *rb)
{
    return roaring_bitmap_copy(rb);
}

void
biscuit_roaring_and_inplace(RoaringBitmap *a, const RoaringBitmap *b)
{
    roaring_bitmap_and_inplace(a, b);
}

void
biscuit_roaring_or_inplace(RoaringBitmap *a, const RoaringBitmap *b)
{
    roaring_bitmap_or_inplace(a, b);
}

void
biscuit_roaring_andnot_inplace(RoaringBitmap *a, const RoaringBitmap *b)
{
    roaring_bitmap_andnot_inplace(a, b);
}

uint32_t *
biscuit_roaring_to_array(const RoaringBitmap *rb, uint64_t *count)
{
    uint32_t *array;
    *count = roaring_bitmap_get_cardinality(rb);
    if (*count == 0)
        return NULL;
    array = (uint32_t *) palloc(*count * sizeof(uint32_t));
    roaring_bitmap_to_uint32_array(rb, array);
    return array;
}

#else  /* !HAVE_ROARING – fallback bitset */

RoaringBitmap *
biscuit_roaring_create(void)
{
    RoaringBitmap *rb = (RoaringBitmap *) palloc0(sizeof(RoaringBitmap));
    rb->capacity = 16;
    rb->blocks   = (uint64_t *) palloc0(rb->capacity * sizeof(uint64_t));
    return rb;
}

void
biscuit_roaring_add(RoaringBitmap *rb, uint32_t value)
{
    int block = value >> 6;
    int bit   = value & 63;

    if (block >= rb->capacity)
    {
        int      new_cap    = (block + 1) * 2;
        uint64_t *new_blocks = (uint64_t *) palloc0(new_cap * sizeof(uint64_t));
        if (rb->num_blocks > 0)
            memcpy(new_blocks, rb->blocks, rb->num_blocks * sizeof(uint64_t));
        pfree(rb->blocks);
        rb->blocks   = new_blocks;
        rb->capacity = new_cap;
    }
    if (block >= rb->num_blocks)
        rb->num_blocks = block + 1;
    rb->blocks[block] |= (1ULL << bit);
}

void
biscuit_roaring_remove(RoaringBitmap *rb, uint32_t value)
{
    int block = value >> 6;
    int bit   = value & 63;
    if (block < rb->num_blocks)
        rb->blocks[block] &= ~(1ULL << bit);
}

uint64_t
biscuit_roaring_count(const RoaringBitmap *rb)
{
    uint64_t count = 0;
    int i;
    for (i = 0; i < rb->num_blocks; i++)
        count += __builtin_popcountll(rb->blocks[i]);
    return count;
}

bool
biscuit_roaring_is_empty(const RoaringBitmap *rb)
{
    int i;
    for (i = 0; i < rb->num_blocks; i++)
        if (rb->blocks[i])
            return false;
    return true;
}

void
biscuit_roaring_free(RoaringBitmap *rb)
{
    if (!rb)
        return;
    if (rb->blocks)
        pfree(rb->blocks);
    pfree(rb);
}

RoaringBitmap *
biscuit_roaring_copy(const RoaringBitmap *rb)
{
    RoaringBitmap *copy = (RoaringBitmap *) palloc(sizeof(RoaringBitmap));
    copy->num_blocks = rb->num_blocks;
    copy->capacity   = rb->capacity;
    copy->blocks     = (uint64_t *) palloc(rb->capacity * sizeof(uint64_t));
    memcpy(copy->blocks, rb->blocks, rb->capacity * sizeof(uint64_t));
    return copy;
}

void
biscuit_roaring_and_inplace(RoaringBitmap *a, const RoaringBitmap *b)
{
    int i;
    int min_blocks = (a->num_blocks < b->num_blocks) ? a->num_blocks : b->num_blocks;
    for (i = 0; i < min_blocks; i++)
        a->blocks[i] &= b->blocks[i];
    for (i = min_blocks; i < a->num_blocks; i++)
        a->blocks[i] = 0;
}

void
biscuit_roaring_or_inplace(RoaringBitmap *a, const RoaringBitmap *b)
{
    int i;
    if (b->num_blocks > a->capacity)
    {
        int      new_cap     = b->num_blocks * 2;
        uint64_t *new_blocks = (uint64_t *) palloc0(new_cap * sizeof(uint64_t));
        if (a->num_blocks > 0)
            memcpy(new_blocks, a->blocks, a->num_blocks * sizeof(uint64_t));
        pfree(a->blocks);
        a->blocks   = new_blocks;
        a->capacity = new_cap;
    }
    if (b->num_blocks > a->num_blocks)
        a->num_blocks = b->num_blocks;
    for (i = 0; i < b->num_blocks; i++)
        a->blocks[i] |= b->blocks[i];
}

void
biscuit_roaring_andnot_inplace(RoaringBitmap *a, const RoaringBitmap *b)
{
    int i;
    int min_blocks = (a->num_blocks < b->num_blocks) ? a->num_blocks : b->num_blocks;
    for (i = 0; i < min_blocks; i++)
        a->blocks[i] &= ~b->blocks[i];
}

uint32_t *
biscuit_roaring_to_array(const RoaringBitmap *rb, uint64_t *count)
{
    uint64_t  total = biscuit_roaring_count(rb);
    uint32_t *array;
    uint64_t  idx   = 0;
    int       i;

    *count = total;
    if (total == 0)
        return NULL;

    array = (uint32_t *) palloc(total * sizeof(uint32_t));
    for (i = 0; i < rb->num_blocks; i++)
    {
        uint64_t word = rb->blocks[i];
        while (word)
        {
            int bit     = __builtin_ctzll(word);
            array[idx++] = (uint32_t)(i * 64 + bit);
            word        &= word - 1;
        }
    }
    return array;
}

#endif  /* HAVE_ROARING */

/* ==================== SERIALIZATION ==================== */

#ifdef HAVE_ROARING

uint32_t
biscuit_roaring_serialized_size(const RoaringBitmap *rb)
{
    if (!rb)
        return 0;
    return (uint32_t) roaring_bitmap_portable_size_in_bytes(rb);
}

char *
biscuit_roaring_serialize(const RoaringBitmap *rb, uint32_t *out_len)
{
    size_t  sz;
    char   *buf;

    if (!rb)
    {
        *out_len = 0;
        return NULL;
    }

    sz  = roaring_bitmap_portable_size_in_bytes(rb);
    buf = (char *) palloc(sz);
    roaring_bitmap_portable_serialize(rb, buf);
    *out_len = (uint32_t) sz;
    return buf;
}

RoaringBitmap *
biscuit_roaring_deserialize(const char *buf, uint32_t len)
{
    RoaringBitmap *rb;

    if (!buf || len == 0)
        return biscuit_roaring_create();

    rb = roaring_bitmap_portable_deserialize_safe(buf, len);
    if (!rb)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("biscuit: corrupt roaring bitmap payload (%u bytes)", len)));
    return rb;
}

#else  /* !HAVE_ROARING – fallback bitset */

/*
 * Fallback wire format (used only when the extension is built without
 * CRoaring): a little struct-free byte stream, portable across the
 * fallback bitset's own in-memory layout --
 *
 *   uint32  num_blocks
 *   num_blocks * uint64  blocks[]
 *
 * capacity is NOT part of the format -- it's a purely in-memory
 * over-allocation hint and is reconstructed as exactly num_blocks on
 * deserialize (biscuit_roaring_add() will grow it again as needed).
 */

uint32_t
biscuit_roaring_serialized_size(const RoaringBitmap *rb)
{
    if (!rb)
        return 0;
    return (uint32_t) (sizeof(uint32_t) + (size_t) rb->num_blocks * sizeof(uint64_t));
}

char *
biscuit_roaring_serialize(const RoaringBitmap *rb, uint32_t *out_len)
{
    uint32_t  len;
    char     *buf;
    uint32_t  nblocks;

    if (!rb)
    {
        *out_len = 0;
        return NULL;
    }

    len     = biscuit_roaring_serialized_size(rb);
    buf     = (char *) palloc(len);
    nblocks = (uint32_t) rb->num_blocks;

    memcpy(buf, &nblocks, sizeof(uint32_t));
    if (rb->num_blocks > 0)
        memcpy(buf + sizeof(uint32_t), rb->blocks, (size_t) rb->num_blocks * sizeof(uint64_t));

    *out_len = len;
    return buf;
}

RoaringBitmap *
biscuit_roaring_deserialize(const char *buf, uint32_t len)
{
    RoaringBitmap *rb;
    uint32_t        nblocks;

    if (!buf || len == 0)
        return biscuit_roaring_create();

    if (len < sizeof(uint32_t))
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("biscuit: truncated bitmap payload (%u bytes)", len)));

    memcpy(&nblocks, buf, sizeof(uint32_t));

    if ((uint64_t) len != (uint64_t) sizeof(uint32_t) + (uint64_t) nblocks * sizeof(uint64_t))
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("biscuit: bitmap payload length mismatch (declared %u blocks, got %u bytes)",
                        nblocks, len)));

    rb = (RoaringBitmap *) palloc0(sizeof(RoaringBitmap));
    rb->num_blocks = (int) nblocks;
    rb->capacity   = (int) nblocks;
    if (nblocks > 0)
    {
        rb->blocks = (uint64_t *) palloc(nblocks * sizeof(uint64_t));
        memcpy(rb->blocks, buf + sizeof(uint32_t), (size_t) nblocks * sizeof(uint64_t));
    }
    else
    {
        /* Keep a non-NULL 0-capacity-safe buffer so later appends behave
         * the same as a freshly-created bitmap (biscuit_roaring_create()
         * always starts with a real allocation). */
        rb->capacity = 16;
        rb->blocks   = (uint64_t *) palloc0(rb->capacity * sizeof(uint64_t));
    }
    return rb;
}

#endif /* HAVE_ROARING */

/* ==================== MEMORY USAGE HELPERS ==================== */

size_t
biscuit_roaring_memory_usage(const RoaringBitmap *rb)
{
    if (!rb)
        return 0;
#ifdef HAVE_ROARING
    return roaring_bitmap_size_in_bytes(rb);
#else
    return sizeof(RoaringBitmap) + (rb->capacity * sizeof(uint64_t));
#endif
}

size_t
biscuit_charindex_memory_usage(const CharIndex *cidx)
{
    size_t total = 0;
    int    i;

    if (!cidx)
        return 0;

    total += cidx->capacity * sizeof(PosEntry);
    for (i = 0; i < cidx->count; i++)
        total += biscuit_roaring_memory_usage(cidx->entries[i].bitmap);

    return total;
}

size_t
biscuit_columnindex_memory_usage(const ColumnIndex *col_idx)
{
    size_t total = 0;
    int    ch, i;

    if (!col_idx)
        return 0;

    if (col_idx->max_length < 0)
    {
        elog(WARNING, "Invalid max_length in ColumnIndex: %d", col_idx->max_length);
        return 0;
    }

    for (ch = 0; ch < CHAR_RANGE; ch++)
    {
        total += biscuit_charindex_memory_usage(&col_idx->pos_idx[ch]);
        total += biscuit_charindex_memory_usage(&col_idx->neg_idx[ch]);
        total += biscuit_roaring_memory_usage(col_idx->char_cache[ch]);

        total += biscuit_charindex_memory_usage(&col_idx->pos_idx_lower[ch]);
        total += biscuit_charindex_memory_usage(&col_idx->neg_idx_lower[ch]);
        total += biscuit_roaring_memory_usage(col_idx->char_cache_lower[ch]);
    }

    /*
     * NOTE: max_length / max_length_lower are the *allocated array sizes*
     * for length_bitmaps[_lower] and length_ge_bitmaps[_lower] (see
     * biscuit_index.c, e.g. "cidx->length_bitmaps = palloc0(cidx->max_length
     * * sizeof(RoaringBitmap *))"), so valid indices are 0 .. max_length-1.
     * These loops previously used "<=", reading one pointer past the end of
     * the palloc'd array and passing whatever garbage bytes were found there
     * to biscuit_roaring_memory_usage() as a RoaringBitmap*, which could
     * crash with a #GP(0) if those bytes formed a non-canonical address.
     */
    if (col_idx->length_bitmaps)
    {
        for (i = 0; i < col_idx->max_length; i++)
            total += biscuit_roaring_memory_usage(col_idx->length_bitmaps[i]);
        total += col_idx->max_length * sizeof(RoaringBitmap *);
    }

    if (col_idx->length_ge_bitmaps)
    {
        for (i = 0; i < col_idx->max_length; i++)
            total += biscuit_roaring_memory_usage(col_idx->length_ge_bitmaps[i]);
        total += col_idx->max_length * sizeof(RoaringBitmap *);
    }

    if (col_idx->length_bitmaps_lower && col_idx->max_length_lower > 0)
    {
        for (i = 0; i < col_idx->max_length_lower; i++)
            total += biscuit_roaring_memory_usage(col_idx->length_bitmaps_lower[i]);
        total += col_idx->max_length_lower * sizeof(RoaringBitmap *);
    }

    if (col_idx->length_ge_bitmaps_lower && col_idx->max_length_lower > 0)
    {
        for (i = 0; i < col_idx->max_length_lower; i++)
            total += biscuit_roaring_memory_usage(col_idx->length_ge_bitmaps_lower[i]);
        total += col_idx->max_length_lower * sizeof(RoaringBitmap *);
    }

    return total;
}
