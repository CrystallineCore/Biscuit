/*
 * biscuit_pattern.c
 * LIKE / ILIKE pattern parsing, bitmap-level position matching,
 * recursive windowed matching, and the multi-column query optimizer.
 *
 * The public interface is declared in biscuit_pattern.h.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_utf8.h"
#include "biscuit_pattern.h"
#include "biscuit_blob.h"   /* biscuit_pending_drain -- read-time reconciliation,
                              * see "Biscuit WAL-Logged Storage: Phase 1 Contract" §3 */
#include "biscuit_dir.h"    /* biscuit_dir_find, BiscuitDirEntry */

/* ================================================================
 * SECTION 0 – Read-time pending-list reconciliation
 * ================================================================
 *
 * Chosen strategy (Phase 1 Contract §3): in-memory merge at scan time,
 * not a forced pre-scan drain. Every bitmap-fetch helper below
 * (biscuit_get_pos_bitmap et al.) routes its result through
 * biscuit_reconcile_pending() before returning, so callers throughout
 * this file (biscuit_match_part_at_pos, biscuit_recursive_windowed_match,
 * their multi-column/ILIKE counterparts, and biscuit_query_pattern*
 * biscuit_query_column_pattern*) see an already-reconciled bitmap and
 * need no reconciliation logic of their own -- they only had to start
 * passing the scan's Relation down to the fetch helpers.
 *
 * Ownership contract callers can rely on: the returned pointer is either
 * (a) the same live, borrowed pointer the caller would have gotten
 * before this phase (structure has no undrained pending records --
 * cheap common case, one directory lookup, zero copies), or (b) a fresh,
 * context-scoped RoaringBitmap the caller does not need to explicitly
 * free (matches this file's existing convention for biscuit_get_length_ge()
 * et al., which have always returned a fresh copy reclaimed by ordinary
 * memory-context cleanup rather than an explicit pfree at every call
 * site). Either way, callers never need to know which case occurred.
 */
static RoaringBitmap *
biscuit_reconcile_pending(Relation index, RoaringBitmap *cached,
                           int32 col, bool is_lower, uint8 kind,
                           int32 ch, int32 position)
{
    BiscuitDirEntry     entry;
    BiscuitDirEntryRef  ref;
    RoaringBitmap      *merged;

    if (index == NULL)
        return cached;   /* no backing Relation available -- tolerate
                           * defensively rather than crash; every real
                           * query-path caller has one (biscuit_scan.c
                           * always has scan->indexRelation) */

    if (!biscuit_dir_find(index, col, is_lower, kind, ch, position, &entry, &ref))
        return cached;   /* nothing durable beyond what's already cached */

    if (entry.pending_count == 0)
        return cached;   /* already reconciled -- the common case */

    merged = cached ? biscuit_roaring_copy(cached) : biscuit_roaring_create();

    /*
     * do_blob_rewrite = false: apply pending records to `merged` only.
     * Leaves both the pending chain and the compacted blob untouched --
     * this is a read, not a drain (see biscuit_blob.c's biscuit_pending_drain()
     * header comment and Phase 1 Contract §3 for why the non-destructive
     * behavior is required here specifically).
     */
    biscuit_pending_drain(index, entry.pending_head,
                           &entry.pending_head, &entry.pending_tail,
                           &entry.blob_head, /* do_blob_rewrite = */ false,
                           merged, /* stats = */ NULL);

    return merged;
}

/* ================================================================
 * SECTION 1 – CharIndex bitmap accessor helpers
 * ================================================================
 *
 * Sorted PosEntry arrays use binary search for O(log n) lookup and
 * insertion-sort to keep entries ordered.
 */

/* ---------- single-column (legacy) case-sensitive ---------- */

RoaringBitmap *
biscuit_get_pos_bitmap(Relation index, BiscuitIndex *idx, unsigned char ch, int pos)
{
    CharIndex     *cidx   = &idx->pos_idx_legacy[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < pos) left  = mid + 1;
        else                                    right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, -1, false,
                                      BISCUIT_DIR_KIND_POS, ch, pos);
}

RoaringBitmap *
biscuit_get_neg_bitmap(Relation index, BiscuitIndex *idx, unsigned char ch, int neg_offset)
{
    CharIndex     *cidx   = &idx->neg_idx_legacy[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < neg_offset) left  = mid + 1;
        else                                           right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, -1, false,
                                      BISCUIT_DIR_KIND_NEG, ch, neg_offset);
}

void
biscuit_set_pos_bitmap(BiscuitIndex *idx, unsigned char ch, int pos, RoaringBitmap *bm)
{
    CharIndex *cidx       = &idx->pos_idx_legacy[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < pos) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_neg_bitmap(BiscuitIndex *idx, unsigned char ch, int neg_offset, RoaringBitmap *bm)
{
    CharIndex *cidx       = &idx->neg_idx_legacy[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < neg_offset) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

/* ---------- single-column case-insensitive (lower) ---------- */

RoaringBitmap *
biscuit_get_pos_bitmap_lower(Relation index, BiscuitIndex *idx, unsigned char ch, int pos)
{
    CharIndex     *cidx   = &idx->pos_idx_lower[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < pos) left  = mid + 1;
        else                                    right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, -1, true,
                                      BISCUIT_DIR_KIND_POS, ch, pos);
}

RoaringBitmap *
biscuit_get_neg_bitmap_lower(Relation index, BiscuitIndex *idx, unsigned char ch, int neg_offset)
{
    CharIndex     *cidx   = &idx->neg_idx_lower[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < neg_offset) left  = mid + 1;
        else                                           right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, -1, true,
                                      BISCUIT_DIR_KIND_NEG, ch, neg_offset);
}

void
biscuit_set_pos_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int pos, RoaringBitmap *bm)
{
    CharIndex *cidx       = &idx->pos_idx_lower[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < pos) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_neg_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int neg_offset, RoaringBitmap *bm)
{
    CharIndex *cidx       = &idx->neg_idx_lower[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < neg_offset) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

/* ---------- multi-column accessors ---------- */

RoaringBitmap *
biscuit_get_col_pos_bitmap(Relation index, ColumnIndex *col, int col_idx,
                            unsigned char ch, int pos)
{
    CharIndex     *cidx   = &col->pos_idx[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < pos) left  = mid + 1;
        else                                    right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, col_idx, false,
                                      BISCUIT_DIR_KIND_POS, ch, pos);
}

RoaringBitmap *
biscuit_get_col_neg_bitmap(Relation index, ColumnIndex *col, int col_idx,
                            unsigned char ch, int neg_offset)
{
    CharIndex     *cidx   = &col->neg_idx[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < neg_offset) left  = mid + 1;
        else                                           right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, col_idx, false,
                                      BISCUIT_DIR_KIND_NEG, ch, neg_offset);
}

RoaringBitmap *
biscuit_get_col_pos_bitmap_lower(Relation index, ColumnIndex *col, int col_idx,
                                  unsigned char ch, int pos)
{
    CharIndex     *cidx   = &col->pos_idx_lower[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < pos) left  = mid + 1;
        else                                    right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, col_idx, true,
                                      BISCUIT_DIR_KIND_POS, ch, pos);
}

RoaringBitmap *
biscuit_get_col_neg_bitmap_lower(Relation index, ColumnIndex *col, int col_idx,
                                  unsigned char ch, int neg_offset)
{
    CharIndex     *cidx   = &col->neg_idx_lower[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < neg_offset) left  = mid + 1;
        else                                           right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, col_idx, true,
                                      BISCUIT_DIR_KIND_NEG, ch, neg_offset);
}

void
biscuit_set_col_pos_bitmap(ColumnIndex *col, unsigned char ch, int pos, RoaringBitmap *bm)
{
    CharIndex *cidx       = &col->pos_idx[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < pos) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_col_neg_bitmap(ColumnIndex *col, unsigned char ch, int neg_offset, RoaringBitmap *bm)
{
    CharIndex *cidx       = &col->neg_idx[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < neg_offset) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_col_pos_bitmap_lower(ColumnIndex *col, unsigned char ch, int pos, RoaringBitmap *bm)
{
    CharIndex *cidx       = &col->pos_idx_lower[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < pos) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_col_neg_bitmap_lower(ColumnIndex *col, unsigned char ch, int neg_offset, RoaringBitmap *bm)
{
    CharIndex *cidx       = &col->neg_idx_lower[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < neg_offset) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

/* ================================================================
 * SECTION 2 – Length bitmap helpers
 * ================================================================ */

static RoaringBitmap *
biscuit_get_length_ge(Relation index, BiscuitIndex *idx, int min_len)
{
    RoaringBitmap *cached = NULL;
    int            position = (min_len <= 0) ? 0 : min_len;

    if (min_len <= 0 && idx->length_ge_bitmaps_legacy && idx->length_ge_bitmaps_legacy[0])
        cached = idx->length_ge_bitmaps_legacy[0];
    else if (min_len < idx->max_length_legacy && idx->length_ge_bitmaps_legacy &&
             idx->length_ge_bitmaps_legacy[min_len])
        cached = idx->length_ge_bitmaps_legacy[min_len];

    /*
     * biscuit_reconcile_pending() returns either `cached` unchanged (a
     * live, borrowed pointer -- must NOT be freed here) or a fresh,
     * context-scoped bitmap. The pre-existing contract for this function
     * is "always hand the caller an owned copy it may mutate/free" --
     * preserve that by copying only in the unchanged-cached case; the
     * pending-merge case already returned a fresh object that's safe to
     * hand back directly.
     */
    {
        RoaringBitmap *reconciled = biscuit_reconcile_pending(index, cached, -1, false,
                                                                BISCUIT_DIR_KIND_LEN_GE,
                                                                -1, position);
        if (reconciled == cached)
            return cached ? biscuit_roaring_copy(cached) : biscuit_roaring_create();
        return reconciled;
    }
}

static RoaringBitmap *
biscuit_get_length_ge_lower(Relation index, BiscuitIndex *idx, int min_len)
{
    RoaringBitmap *cached = NULL;
    int            position = (min_len <= 0) ? 0 : min_len;

    if (min_len <= 0 && idx->length_ge_bitmaps_lower && idx->length_ge_bitmaps_lower[0])
        cached = idx->length_ge_bitmaps_lower[0];
    else if (min_len < idx->max_length_lower && idx->length_ge_bitmaps_lower &&
             idx->length_ge_bitmaps_lower[min_len])
        cached = idx->length_ge_bitmaps_lower[min_len];

    {
        RoaringBitmap *reconciled = biscuit_reconcile_pending(index, cached, -1, true,
                                                                BISCUIT_DIR_KIND_LEN_GE,
                                                                -1, position);
        if (reconciled == cached)
            return cached ? biscuit_roaring_copy(cached) : biscuit_roaring_create();
        return reconciled;
    }
}

static RoaringBitmap *
biscuit_get_col_length_ge(Relation index, ColumnIndex *col, int col_idx, int min_len)
{
    RoaringBitmap *cached = NULL;
    int            position = (min_len <= 0) ? 0 : min_len;

    if (min_len <= 0 && col->length_ge_bitmaps && col->length_ge_bitmaps[0])
        cached = col->length_ge_bitmaps[0];
    else if (min_len < col->max_length && col->length_ge_bitmaps &&
             col->length_ge_bitmaps[min_len])
        cached = col->length_ge_bitmaps[min_len];

    {
        RoaringBitmap *reconciled = biscuit_reconcile_pending(index, cached, col_idx, false,
                                                                BISCUIT_DIR_KIND_LEN_GE,
                                                                -1, position);
        if (reconciled == cached)
            return cached ? biscuit_roaring_copy(cached) : biscuit_roaring_create();
        return reconciled;
    }
}

static RoaringBitmap *
biscuit_get_col_length_ge_lower(Relation index, ColumnIndex *col, int col_idx, int min_len)
{
    RoaringBitmap *cached = NULL;
    int            position = (min_len <= 0) ? 0 : min_len;

    if (min_len <= 0 && col->length_ge_bitmaps_lower && col->length_ge_bitmaps_lower[0])
        cached = col->length_ge_bitmaps_lower[0];
    else if (min_len < col->max_length_lower && col->length_ge_bitmaps_lower &&
             col->length_ge_bitmaps_lower[min_len])
        cached = col->length_ge_bitmaps_lower[min_len];

    {
        RoaringBitmap *reconciled = biscuit_reconcile_pending(index, cached, col_idx, true,
                                                                BISCUIT_DIR_KIND_LEN_GE,
                                                                -1, position);
        if (reconciled == cached)
            return cached ? biscuit_roaring_copy(cached) : biscuit_roaring_create();
        return reconciled;
    }
}

/* ================================================================
 * SECTION 3 – Pattern parsing
 * ================================================================
 *
 * BISCUIT_LITERAL_ESC is a sentinel byte (SOH, 0x01) stored in
 * ParsedPattern part strings to indicate that the following byte is
 * a literal character rather than a LIKE wildcard.  It is used to
 * distinguish a literal '_' (escaped as '\\_' in SQL) from the
 * single-character wildcard '_'.
 *
 * 0x01 (SOH) is chosen because:
 *  - It cannot appear in valid UTF-8 text stored in PostgreSQL.
 *  - It cannot be a LIKE wildcard ('%' or '_').
 *  - It is distinct from any real data byte the matchers would see.
 */
#define BISCUIT_LITERAL_ESC  '\x01'

/*
 * biscuit_part_char_count
 * Count the number of LIKE pattern characters represented by a part
 * string that may contain BISCUIT_LITERAL_ESC + byte sentinel pairs.
 * Each sentinel pair counts as one character.  Regular UTF-8 sequences
 * are counted normally.
 */
static int
biscuit_part_char_count(const char *part, int byte_len)
{
    int count = 0;
    int i     = 0;
    while (i < byte_len)
    {
        unsigned char c = (unsigned char) part[i];
        if (c == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            i += 2; /* sentinel + literal byte = 1 char */
        }
        else
        {
            i += biscuit_utf8_char_length(c);
        }
        count++;
    }
    return count;
}

/*
 * biscuit_part_seed_byte
 * ----------------------
 * Return the first concrete (non-wildcard) byte from a part string for
 * use as a char-cache lookup seed.  Skips:
 *   - bare '_' bytes (single-char wildcard)
 *   - BISCUIT_LITERAL_ESC prefixes, but returns the byte AFTER the prefix
 *     (that byte is the literal character to seed from)
 * Returns 0 (NUL) if the part is all wildcards.
 */
static unsigned char
biscuit_part_seed_byte(const char *part, int byte_len)
{
    int i = 0;
    while (i < byte_len)
    {
        unsigned char c = (unsigned char) part[i];
        if (c == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            if (i + 1 < byte_len)
                return (unsigned char) part[i + 1]; /* literal char */
            i += 2;
        }
        else if (c == '_')
        {
            i++;  /* skip single-char wildcard */
        }
        else
        {
            return c;  /* first concrete character */
        }
    }
    return 0;  /* all wildcards */
}

/*
 * biscuit_part_match_substr
 * -------------------------
 * Check whether the sentinel-encoded part string matches a substring of
 * haystack starting at byte offset hay_off.
 *
 * Rules:
 *   - A bare '_' in the part matches exactly one UTF-8 character in hay.
 *   - A BISCUIT_LITERAL_ESC + byte pair matches that exact literal byte.
 *   - All other bytes match themselves exactly.
 *
 * Returns true on a full match of the part, false otherwise.
 * hay/hay_byte_len: the full string and its byte length.
 * hay_off: byte offset within hay where matching starts.
 */
static bool
biscuit_part_match_substr(const char *hay, int hay_byte_len, int hay_off,
                           const char *part, int part_byte_len)
{
    int pi = 0;  /* part byte position */
    int hi = hay_off;  /* haystack byte position */

    while (pi < part_byte_len)
    {
        unsigned char pc;
        if (hi >= hay_byte_len)
            return false;

        pc = (unsigned char) part[pi];

        if (pc == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal;
            /* Literal match: part[pi+1] must equal hay[hi] exactly */
            if (pi + 1 >= part_byte_len)
                return false;
            literal = (unsigned char) part[pi + 1];
            if ((unsigned char) hay[hi] != literal)
                return false;
            pi += 2;
            hi += 1;
        }
        else if (pc == '_')
        {
            /* Single-char wildcard: consume one UTF-8 char from haystack */
            int cl = biscuit_utf8_char_length((unsigned char) hay[hi]);
            if (hi + cl > hay_byte_len)
                return false;
            pi += 1;
            hi += cl;
        }
        else
        {
            /* Literal byte from pattern: must match exactly */
            if ((unsigned char) hay[hi] != pc)
                return false;
            pi += 1;
            hi += 1;
        }
    }
    return true;  /* all part bytes consumed */
}

/*
 * biscuit_wildcard_contains
 * --------------------------
 * Wildcard-aware replacement for strstr(): returns true if the
 * sentinel-encoded `part` (which may contain bare '_' single-char
 * wildcards and/or BISCUIT_LITERAL_ESC-escaped literal underscores)
 * matches a substring of `hay` starting at some UTF-8 character
 * boundary.
 *
 * A plain strstr() treats '_' as a literal byte, which is wrong for
 * SQL LIKE/ILIKE semantics ('_' must match any single character).
 * This function instead tries biscuit_part_match_substr() — which
 * already implements correct '_'/escape handling — at every valid
 * starting offset, exactly like strstr() would scan every starting
 * offset for a literal needle.
 */
static bool
biscuit_wildcard_contains(const char *hay, int hay_byte_len,
                           const char *part, int part_byte_len)
{
    int hay_off = 0;

    if (part_byte_len == 0)
        return true;

    while (hay_off <= hay_byte_len)
    {
        if (biscuit_part_match_substr(hay, hay_byte_len, hay_off,
                                       part, part_byte_len))
            return true;
        if (hay_off >= hay_byte_len)
            break;
        /* Advance by one whole UTF-8 character, not one raw byte, so we
         * never attempt a match starting mid-way through a multi-byte
         * sequence. */
        hay_off += biscuit_utf8_char_length((unsigned char) hay[hay_off]);
    }
    return false;
}


/* Splits a LIKE/ILIKE pattern on unescaped '%' wildcards.
 *
 * SQL escape convention (backslash, matching what PostgreSQL passes to
 * index AMs after processing the ESCAPE clause):
 *   '\%'  -> literal '%'  (not a wildcard separator)
 *   '\_'  -> literal '_'  (not a single-char wildcard)
 *   '\\'  -> literal '\'
 *
 * Each resulting part string has escape sequences collapsed to the
 * actual character they represent, so the downstream bitmap matchers
 * see real bytes and never mis-treat them as wildcards.
 *
 * starts_percent / ends_percent reflect whether the *unescaped* pattern
 * starts/ends with a wildcard '%'.
 */
ParsedPattern *
biscuit_parse_pattern(const char *pattern)
{
    ParsedPattern *parsed;
    int            plen      = strlen(pattern);
    int            max_parts = plen + 1;
    int            part_count;
    char          *buf;      /* unescaped part accumulator */
    int            buf_len;
    bool           in_part;
    int            i;

    buf      = (char *) palloc(2 * plen + 1); /* 2× for literal-_ sentinel pairs */
    buf_len  = 0;
    in_part  = false;

    parsed                 = (ParsedPattern *) palloc(sizeof(ParsedPattern));
    parsed->parts          = (char **) palloc(max_parts * sizeof(char *));
    parsed->part_lens      = (int *)   palloc(max_parts * sizeof(int));
    parsed->part_byte_lens = (int *)   palloc(max_parts * sizeof(int));
    parsed->part_count     = 0;
    parsed->starts_percent = false;
    parsed->ends_percent   = false;
    part_count             = 0;

    /*
     * Determine starts_percent: true only when the very first character
     * is an unescaped '%'.
     */
    if (plen > 0)
    {
        if (pattern[0] == '\\' && plen > 1)
            parsed->starts_percent = false;  /* escaped char — not a wildcard */
        else
            parsed->starts_percent = (pattern[0] == '%');
    }

    /*
     * Determine ends_percent: true only when the last character is an
     * unescaped '%'.  Count consecutive backslashes immediately before
     * it; an odd count means the '%' is escaped.
     */
    if (plen > 0 && pattern[plen - 1] == '%')
    {
        int bs = 0, j = plen - 2;
        while (j >= 0 && pattern[j] == '\\') { bs++; j--; }
        parsed->ends_percent = ((bs % 2) == 0);
    }

    /* Main scan: walk pattern, unescape on the fly, split on bare '%'. */
    for (i = 0; i <= plen; )
    {
        if (i == plen)
        {
            /* End of pattern: flush current part if any. */
            if (in_part)
            {
                char *part_str             = pnstrdup(buf, buf_len);
                parsed->parts[part_count]          = part_str;
                parsed->part_byte_lens[part_count]  = buf_len;
                parsed->part_lens[part_count]       =
                    biscuit_part_char_count(part_str, buf_len);
                part_count++;
            }
            break;
        }

        if (pattern[i] == '\\' && i + 1 < plen)
        {
            char escaped = pattern[i + 1];
            /*
             * Backslash escapes any following character:
             *   '\%'  -> literal '%'  (not a wildcard separator)
             *   '\_'  -> literal '_'  (emit sentinel so matchers don't
             *                          treat it as a single-char wildcard)
             *   '\\'  -> literal '\'
             *   '\X'  -> literal 'X'  (for any other X, e.g. '\.')
             *
             * For '_' we store BISCUIT_LITERAL_ESC + '_' in the part
             * buffer so downstream matchers can distinguish literal '_'
             * from wildcard '_'.  All other escaped chars are stored as
             * their literal byte (they have no wildcard meaning anyway).
             */
            if (!in_part)
                in_part = true;
            if (escaped == '_')
            {
                buf[buf_len++] = BISCUIT_LITERAL_ESC;
                buf[buf_len++] = '_';
            }
            else
            {
                /* '%', '\\', '.', '-', etc. — just the literal byte */
                buf[buf_len++] = escaped;
            }
            i += 2;
        }
        else if (pattern[i] == '%')
        {
            /* Unescaped '%': wildcard separator — flush current part. */
            if (in_part)
            {
                char *part_str             = pnstrdup(buf, buf_len);
                parsed->parts[part_count]          = part_str;
                parsed->part_byte_lens[part_count]  = buf_len;
                parsed->part_lens[part_count]       =
                    biscuit_part_char_count(part_str, buf_len);
                part_count++;
                in_part = false;
                buf_len = 0;
            }
            i++;
        }
        else
        {
            /* Regular character (including unescaped '_'): copy verbatim.
             * Multi-byte UTF-8 sequences are copied whole. */
            int char_len = biscuit_utf8_char_length((unsigned char) pattern[i]);
            if (i + char_len > plen)
                char_len = plen - i;
            if (!in_part)
                in_part = true;
            memcpy(buf + buf_len, pattern + i, char_len);
            buf_len += char_len;
            i       += char_len;
        }
    }

    pfree(buf);
    parsed->part_count = part_count;
    return parsed;
}

void
biscuit_free_parsed_pattern(ParsedPattern *parsed)
{
    int i;

    if (!parsed)
        return;

    if (parsed->parts)
    {
        for (i = 0; i < parsed->part_count; i++)
        {
            if (parsed->parts[i])
            {
                pfree(parsed->parts[i]);
                parsed->parts[i] = NULL;
            }
        }
        pfree(parsed->parts);
    }
    if (parsed->part_lens)     pfree(parsed->part_lens);
    if (parsed->part_byte_lens) pfree(parsed->part_byte_lens);
    pfree(parsed);
}

/* ================================================================
 * SECTION 4 – Low-level part matching (single-column)
 * ================================================================ */

/*
 * Match part at a specific character position (from start) – case-sensitive.
 * Handles underscore (_) wildcards and multi-byte UTF-8 sequences.
 */
static RoaringBitmap *
biscuit_match_part_at_pos(Relation index, BiscuitIndex *idx, const char *part,
                          int part_byte_len, int start_pos)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int            part_byte_pos  = 0;
    int            char_pos       = start_pos;
    int            concrete_chars = 0;
    bool           first_char     = true;

    while (part_byte_pos < part_byte_len)
    {
        unsigned char first_byte = (unsigned char) part[part_byte_pos];

        /* BISCUIT_LITERAL_ESC prefix: next byte is a literal character,
         * not a wildcard.  Consume both bytes and treat the second as a
         * concrete 1-byte character for bitmap lookup. */
        if (first_byte == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal_byte;
            RoaringBitmap *char_bm;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            literal_byte = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            char_bm = biscuit_get_pos_bitmap(index, idx, literal_byte, char_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2;
            char_pos++;
            continue;
        }

        if (first_byte == '_') { part_byte_pos++; char_pos++; continue; }

        {
        int char_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_len > part_byte_len)
            char_len = part_byte_len - part_byte_pos;

        concrete_chars++;

        if (char_len == 1)
        {
            RoaringBitmap *char_bm = biscuit_get_pos_bitmap(index, idx, first_byte, char_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
        }
        else
        {
            RoaringBitmap *multibyte = NULL;
            int b;
            for (b = 0; b < char_len; b++)
            {
                unsigned char bv = (unsigned char) part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_pos_bitmap(index, idx, bv, char_pos);
                if (!byte_bm) {
                    if (multibyte) biscuit_roaring_free(multibyte);
                    if (result)   biscuit_roaring_free(result);
                    return biscuit_roaring_create();
                }
                if (b == 0) multibyte = biscuit_roaring_copy(byte_bm);
                else { biscuit_roaring_and_inplace(multibyte, byte_bm); if (biscuit_roaring_is_empty(multibyte)) { biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } }
            }
            if (first_char) { result = multibyte; first_char = false; }
            else { biscuit_roaring_and_inplace(result, multibyte); biscuit_roaring_free(multibyte); if (biscuit_roaring_is_empty(result)) return result; }
        }

        part_byte_pos += char_len;
        char_pos++;
        } /* end char_len block */
    }

    {
        int pattern_char_count = biscuit_part_char_count(part, part_byte_len);
        if (concrete_chars == 0) {
            if (result) biscuit_roaring_free(result);
            result = biscuit_get_length_ge(index, idx, start_pos + pattern_char_count);
        } else {
            len_filter = biscuit_get_length_ge(index, idx, start_pos + pattern_char_count);
            if (len_filter) {
                if (result) biscuit_roaring_and_inplace(result, len_filter);
                else        { result = len_filter; len_filter = NULL; }
                if (len_filter) biscuit_roaring_free(len_filter);
            }
        }
    }

    return result ? result : biscuit_roaring_create();
}

/* Match part anchored at the end of string – case-sensitive. */
static RoaringBitmap *
biscuit_match_part_at_end(Relation index, BiscuitIndex *idx, const char *part, int part_byte_len)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int            concrete_chars        = 0;
    bool           first_char            = true;
    int            pattern_char_count    = biscuit_part_char_count(part, part_byte_len);
    int            part_byte_pos         = 0;
    int            char_offset_from_end  = 0;

    while (part_byte_pos < part_byte_len)
    {
        unsigned char first_byte = (unsigned char) part[part_byte_pos];

        if (first_byte == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal_byte;
            RoaringBitmap *char_bm;
            int neg_pos;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            literal_byte = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            neg_pos = -(pattern_char_count - char_offset_from_end);
            char_bm = biscuit_get_neg_bitmap(index, idx, literal_byte, neg_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2;
            char_offset_from_end++;
            continue;
        }

        if (first_byte == '_') { part_byte_pos++; char_offset_from_end++; continue; }

        {
        int char_len = biscuit_utf8_char_length(first_byte);
        int neg_pos;
        if (part_byte_pos + char_len > part_byte_len)
            char_len = part_byte_len - part_byte_pos;

        concrete_chars++;
        neg_pos = -(pattern_char_count - char_offset_from_end);

        if (char_len == 1)
        {
            RoaringBitmap *char_bm = biscuit_get_neg_bitmap(index, idx, first_byte, neg_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
        }
        else
        {
            RoaringBitmap *multibyte = NULL;
            int b;
            for (b = 0; b < char_len; b++)
            {
                unsigned char bv = (unsigned char) part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_neg_bitmap(index, idx, bv, neg_pos);
                if (!byte_bm) { if (multibyte) biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
                if (b == 0) multibyte = biscuit_roaring_copy(byte_bm);
                else { biscuit_roaring_and_inplace(multibyte, byte_bm); if (biscuit_roaring_is_empty(multibyte)) { biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } }
            }
            if (first_char) { result = multibyte; first_char = false; }
            else { biscuit_roaring_and_inplace(result, multibyte); biscuit_roaring_free(multibyte); if (biscuit_roaring_is_empty(result)) return result; }
        }

        part_byte_pos += char_len;
        char_offset_from_end++;
        } /* end char_len block */
    }

    if (concrete_chars == 0) {
        if (result) biscuit_roaring_free(result);
        result = biscuit_get_length_ge(index, idx, pattern_char_count);
    } else {
        len_filter = biscuit_get_length_ge(index, idx, pattern_char_count);
        if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); }
    }

    return result ? result : biscuit_roaring_create();
}

/* Case-insensitive variants reuse the same logic via _lower accessors */
static RoaringBitmap *
biscuit_match_part_at_pos_ilike(Relation index, BiscuitIndex *idx, const char *part,
                                int part_byte_len, int start_pos)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int            part_byte_pos  = 0;
    int            char_pos       = start_pos;
    int            concrete_chars = 0;
    bool           first_char     = true;

    while (part_byte_pos < part_byte_len)
    {
        unsigned char first_byte = (unsigned char) part[part_byte_pos];
        if (first_byte == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal_byte;
            RoaringBitmap *char_bm;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            literal_byte = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            char_bm = biscuit_get_pos_bitmap_lower(index, idx, literal_byte, char_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2;
            char_pos++;
            continue;
        }
        if (first_byte == '_') { part_byte_pos++; char_pos++; continue; }

        {
        int char_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_len > part_byte_len)
            char_len = part_byte_len - part_byte_pos;
        concrete_chars++;

        if (char_len == 1)
        {
            RoaringBitmap *char_bm = biscuit_get_pos_bitmap_lower(index, idx, first_byte, char_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
        }
        else
        {
            RoaringBitmap *multibyte = NULL; int b;
            for (b = 0; b < char_len; b++) {
                unsigned char bv = (unsigned char) part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_pos_bitmap_lower(index, idx, bv, char_pos);
                if (!byte_bm) { if (multibyte) biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
                if (b == 0) multibyte = biscuit_roaring_copy(byte_bm);
                else { biscuit_roaring_and_inplace(multibyte, byte_bm); if (biscuit_roaring_is_empty(multibyte)) { biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } }
            }
            if (first_char) { result = multibyte; first_char = false; }
            else { biscuit_roaring_and_inplace(result, multibyte); biscuit_roaring_free(multibyte); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += char_len;
        char_pos++;
        } /* end char_len block */
    }

    {
        int pattern_char_count = biscuit_part_char_count(part, part_byte_len);
        if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_length_ge_lower(index, idx, start_pos + pattern_char_count); }
        else { len_filter = biscuit_get_length_ge_lower(index, idx, start_pos + pattern_char_count); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } }
    }

    return result ? result : biscuit_roaring_create();
}

static RoaringBitmap *
biscuit_match_part_at_end_ilike(Relation index, BiscuitIndex *idx, const char *part, int part_byte_len)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int            concrete_chars       = 0;
    bool           first_char           = true;
    int            pattern_char_count   = biscuit_part_char_count(part, part_byte_len);
    int            part_byte_pos        = 0;
    int            char_offset_from_end = 0;

    while (part_byte_pos < part_byte_len)
    {
        unsigned char first_byte = (unsigned char) part[part_byte_pos];
        if (first_byte == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal_byte;
            RoaringBitmap *char_bm;
            int neg_pos;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            literal_byte = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            neg_pos = -(pattern_char_count - char_offset_from_end);
            char_bm = biscuit_get_neg_bitmap_lower(index, idx, literal_byte, neg_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2;
            char_offset_from_end++;
            continue;
        }
        if (first_byte == '_') { part_byte_pos++; char_offset_from_end++; continue; }
        {
        int char_len = biscuit_utf8_char_length(first_byte);
        int neg_pos;
        if (part_byte_pos + char_len > part_byte_len) char_len = part_byte_len - part_byte_pos;
        concrete_chars++;
        neg_pos = -(pattern_char_count - char_offset_from_end);
        if (char_len == 1) {
            RoaringBitmap *char_bm = biscuit_get_neg_bitmap_lower(index, idx, first_byte, neg_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *multibyte = NULL; int b;
            for (b = 0; b < char_len; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *byte_bm = biscuit_get_neg_bitmap_lower(index, idx, bv, neg_pos); if (!byte_bm) { if (multibyte) biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) multibyte = biscuit_roaring_copy(byte_bm); else { biscuit_roaring_and_inplace(multibyte, byte_bm); if (biscuit_roaring_is_empty(multibyte)) { biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = multibyte; first_char = false; }
            else { biscuit_roaring_and_inplace(result, multibyte); biscuit_roaring_free(multibyte); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += char_len;
        char_offset_from_end++;
        } /* end char_len block */
    }

    if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_length_ge_lower(index, idx, pattern_char_count); }
    else { len_filter = biscuit_get_length_ge_lower(index, idx, pattern_char_count); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } }

    return result ? result : biscuit_roaring_create();
}


/* ================================================================
 * SECTION 5 – Iterative windowed matching (single-column)
 *
 * Replaces the former recursive implementations to eliminate both
 * stack-overflow risk and the O(positions^parts) allocation pattern
 * that caused OOM crashes on long-text columns.
 *
 * Algorithm: explicit work-stack of (part_idx, min_pos, candidates)
 * frames.  Children are pushed instead of recursed into.  Each frame
 * owns its candidates bitmap and frees it when the frame is done.
 * This bounds live bitmap count to O(part_count * max_len) instead of
 * the former exponential.
 * ================================================================ */

typedef struct WMFrame {
    int            part_idx;
    int            min_pos;
    RoaringBitmap *candidates;
} WMFrame;

#define WM_MAX_STACK 512

static void
biscuit_recursive_windowed_match(
    Relation index, RoaringBitmap *result, BiscuitIndex *idx,
    const char **parts, int *part_lens, int part_count,
    bool ends_percent, int part_idx, int min_pos,
    RoaringBitmap *current_candidates, int max_len)
{
    int i;
    WMFrame *stack     = (WMFrame *) palloc(WM_MAX_STACK * sizeof(WMFrame));
    int      stack_top = 0;
    int *pcl = (int *) palloc(part_count * sizeof(int));
    int *suf = (int *) palloc(part_count * sizeof(int));
    for (i = 0; i < part_count; i++)
        pcl[i] = biscuit_part_char_count(parts[i], part_lens[i]);
    suf[part_count - 1] = 0;
    for (i = part_count - 2; i >= 0; i--)
        suf[i] = suf[i + 1] + pcl[i + 1];

    stack[stack_top].part_idx   = part_idx;
    stack[stack_top].min_pos    = min_pos;
    stack[stack_top].candidates = biscuit_roaring_copy(current_candidates);
    stack_top++;

    while (stack_top > 0)
    {
        int            pidx;
        int            mpos;
        RoaringBitmap *cands;
        int            part_cl;
        int            suf_len;
        int            max_pos;
        stack_top--;
        pidx  = stack[stack_top].part_idx;
        mpos  = stack[stack_top].min_pos;
        cands = stack[stack_top].candidates;

        if (pidx >= part_count)
        {
            biscuit_roaring_or_inplace(result, cands);
            biscuit_roaring_free(cands);
            continue;
        }

        part_cl = pcl[pidx];
        suf_len = suf[pidx];
        max_pos = max_len - part_cl - suf_len;

        if (pidx == part_count - 1 && !ends_percent)
        {
            RoaringBitmap *em = biscuit_match_part_at_end(index, idx, parts[pidx], part_lens[pidx]);
            if (em)
            {
                int mrl;
                RoaringBitmap *lc;
                biscuit_roaring_and_inplace(em, cands);
                mrl = mpos + part_cl;
                lc = biscuit_get_length_ge(index, idx, mrl);
                if (lc) { biscuit_roaring_and_inplace(em, lc); biscuit_roaring_free(lc); }
                if (!biscuit_roaring_is_empty(em))
                    biscuit_roaring_or_inplace(result, em);
                biscuit_roaring_free(em);
            }
            biscuit_roaring_free(cands);
            continue;
        }

        if (mpos > max_pos) { biscuit_roaring_free(cands); continue; }

        for (i = mpos; i <= max_pos; i++)
        {
            int pos;
            RoaringBitmap *pm;
            RoaringBitmap *nc;
            pos = i;
            pm = biscuit_match_part_at_pos(index, idx, parts[pidx],
                                           part_lens[pidx], pos);
            if (!pm)
                continue;
            nc = biscuit_roaring_copy(cands);
            biscuit_roaring_and_inplace(nc, pm);
            biscuit_roaring_free(pm);
            if (biscuit_roaring_is_empty(nc))
            {
                biscuit_roaring_free(nc);
                continue;
            }
            if (stack_top >= WM_MAX_STACK)
            {
                biscuit_roaring_or_inplace(result, nc);
                biscuit_roaring_free(nc);
                continue;
            }
            stack[stack_top].part_idx   = pidx + 1;
            stack[stack_top].min_pos    = pos + part_cl;
            stack[stack_top].candidates = nc;
            stack_top++;
        }
        biscuit_roaring_free(cands);
    }

    pfree(stack);
    pfree(pcl);
    pfree(suf);
}

static void
biscuit_recursive_windowed_match_ilike(
    Relation index, RoaringBitmap *result, BiscuitIndex *idx,
    const char **parts, int *part_lens, int part_count,
    bool ends_percent, int part_idx, int min_pos,
    RoaringBitmap *current_candidates, int max_len)
{
    int i;
    WMFrame *stack     = (WMFrame *) palloc(WM_MAX_STACK * sizeof(WMFrame));
    int      stack_top = 0;
    int *pcl = (int *) palloc(part_count * sizeof(int));
    int *suf = (int *) palloc(part_count * sizeof(int));
    for (i = 0; i < part_count; i++)
        pcl[i] = biscuit_part_char_count(parts[i], part_lens[i]);
    suf[part_count - 1] = 0;
    for (i = part_count - 2; i >= 0; i--)
        suf[i] = suf[i + 1] + pcl[i + 1];

    stack[stack_top].part_idx   = part_idx;
    stack[stack_top].min_pos    = min_pos;
    stack[stack_top].candidates = biscuit_roaring_copy(current_candidates);
    stack_top++;

    while (stack_top > 0)
    {
        int            pidx;
        int            mpos;
        RoaringBitmap *cands;
        int            part_cl;
        int            suf_len;
        int            max_pos;
        stack_top--;
        pidx  = stack[stack_top].part_idx;
        mpos  = stack[stack_top].min_pos;
        cands = stack[stack_top].candidates;

        if (pidx >= part_count)
        {
            biscuit_roaring_or_inplace(result, cands);
            biscuit_roaring_free(cands);
            continue;
        }

        part_cl = pcl[pidx];
        suf_len = suf[pidx];
        max_pos = max_len - part_cl - suf_len;

        if (pidx == part_count - 1 && !ends_percent)
        {
            RoaringBitmap *em = biscuit_match_part_at_end_ilike(index, idx, parts[pidx], part_lens[pidx]);
            if (em)
            {
                int mrl;
                RoaringBitmap *lc;
                biscuit_roaring_and_inplace(em, cands);
                mrl = mpos + part_cl;
                lc = biscuit_get_length_ge_lower(index, idx, mrl);
                if (lc) { biscuit_roaring_and_inplace(em, lc); biscuit_roaring_free(lc); }
                if (!biscuit_roaring_is_empty(em))
                    biscuit_roaring_or_inplace(result, em);
                biscuit_roaring_free(em);
            }
            biscuit_roaring_free(cands);
            continue;
        }

        if (mpos > max_pos) { biscuit_roaring_free(cands); continue; }

        for (i = mpos; i <= max_pos; i++)
        {
            int pos;
            RoaringBitmap *pm;
            RoaringBitmap *nc;
            pos = i;
            pm = biscuit_match_part_at_pos_ilike(index, idx, parts[pidx], part_lens[pidx], pos);
            if (!pm) continue;
            nc = biscuit_roaring_copy(cands);
            biscuit_roaring_and_inplace(nc, pm);
            biscuit_roaring_free(pm);
            if (biscuit_roaring_is_empty(nc)) { biscuit_roaring_free(nc); continue; }
            if (stack_top >= WM_MAX_STACK)
            {
                biscuit_roaring_or_inplace(result, nc);
                biscuit_roaring_free(nc);
                continue;
            }
            stack[stack_top].part_idx   = pidx + 1;
            stack[stack_top].min_pos    = pos + part_cl;
            stack[stack_top].candidates = nc;
            stack_top++;
        }
        biscuit_roaring_free(cands);
    }

    pfree(stack);
    pfree(pcl);
    pfree(suf);
}

/* ================================================================
 * SECTION 6 – Multi-column part matching helpers
 * ================================================================ */

static RoaringBitmap *
biscuit_match_col_part_at_pos(Relation index, ColumnIndex *col, int col_idx,
                              const char *part,
                              int part_byte_len, int start_pos)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int part_byte_pos = 0, char_pos = start_pos, concrete_chars = 0;
    bool first_char = true;

    while (part_byte_pos < part_byte_len) {
        unsigned char fb = (unsigned char) part[part_byte_pos];
        if (fb == (unsigned char) BISCUIT_LITERAL_ESC) {
            unsigned char lb; RoaringBitmap *bm;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            lb = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            bm = biscuit_get_col_pos_bitmap(index, col, col_idx, lb, char_pos);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2; char_pos++; continue;
        }
        if (fb == '_') { part_byte_pos++; char_pos++; continue; }
        {
        int cl = biscuit_utf8_char_length(fb);
        if (part_byte_pos + cl > part_byte_len) cl = part_byte_len - part_byte_pos;
        concrete_chars++;
        if (cl == 1) {
            RoaringBitmap *bm = biscuit_get_col_pos_bitmap(index, col, col_idx, fb, char_pos);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *mb = NULL; int b;
            for (b = 0; b < cl; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *bm = biscuit_get_col_pos_bitmap(index, col, col_idx, bv, char_pos); if (!bm) { if (mb) biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) mb = biscuit_roaring_copy(bm); else { biscuit_roaring_and_inplace(mb, bm); if (biscuit_roaring_is_empty(mb)) { biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = mb; first_char = false; } else { biscuit_roaring_and_inplace(result, mb); biscuit_roaring_free(mb); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += cl; char_pos++;
        } /* end cl block */
    }

    { int pcc = biscuit_part_char_count(part, part_byte_len);
      if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_col_length_ge(index, col, col_idx, start_pos + pcc); }
      else { len_filter = biscuit_get_col_length_ge(index, col, col_idx, start_pos + pcc); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } } }
    return result ? result : biscuit_roaring_create();
}

static RoaringBitmap *
biscuit_match_col_part_at_end(Relation index, ColumnIndex *col, int col_idx,
                              const char *part, int part_byte_len)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int concrete_chars = 0, pcc = biscuit_part_char_count(part, part_byte_len);
    int part_byte_pos = 0, cfe = 0;
    bool first_char = true;

    while (part_byte_pos < part_byte_len) {
        unsigned char fb = (unsigned char) part[part_byte_pos];
        if (fb == (unsigned char) BISCUIT_LITERAL_ESC) {
            unsigned char lb; RoaringBitmap *bm; int neg;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            lb = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            neg = -(pcc - cfe);
            bm = biscuit_get_col_neg_bitmap(index, col, col_idx, lb, neg);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2; cfe++; continue;
        }
        if (fb == '_') { part_byte_pos++; cfe++; continue; }
        {
        int cl = biscuit_utf8_char_length(fb);
        int neg;
        if (part_byte_pos + cl > part_byte_len) cl = part_byte_len - part_byte_pos;
        concrete_chars++;
        neg = -(pcc - cfe);
        if (cl == 1) {
            RoaringBitmap *bm = biscuit_get_col_neg_bitmap(index, col, col_idx, fb, neg);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *mb = NULL; int b;
            for (b = 0; b < cl; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *bm = biscuit_get_col_neg_bitmap(index, col, col_idx, bv, neg); if (!bm) { if (mb) biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) mb = biscuit_roaring_copy(bm); else { biscuit_roaring_and_inplace(mb, bm); if (biscuit_roaring_is_empty(mb)) { biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = mb; first_char = false; } else { biscuit_roaring_and_inplace(result, mb); biscuit_roaring_free(mb); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += cl; cfe++;
        } /* end cl block */
    }

    if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_col_length_ge(index, col, col_idx, pcc); }
    else { len_filter = biscuit_get_col_length_ge(index, col, col_idx, pcc); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } }
    return result ? result : biscuit_roaring_create();
}

/* Case-insensitive multi-column variants */
static RoaringBitmap *
biscuit_match_col_part_at_pos_ilike(Relation index, ColumnIndex *col, int col_idx,
                                    const char *part,
                                    int part_byte_len, int start_pos)
{
    /* Identical to biscuit_match_col_part_at_pos but uses _lower accessors */
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int part_byte_pos = 0, char_pos = start_pos, concrete_chars = 0;
    bool first_char = true;

    while (part_byte_pos < part_byte_len) {
        unsigned char fb = (unsigned char) part[part_byte_pos];
        if (fb == (unsigned char) BISCUIT_LITERAL_ESC) {
            unsigned char lb; RoaringBitmap *bm;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            lb = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            bm = biscuit_get_col_pos_bitmap_lower(index, col, col_idx, lb, char_pos);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2; char_pos++; continue;
        }
        if (fb == '_') { part_byte_pos++; char_pos++; continue; }
        {
        int cl = biscuit_utf8_char_length(fb);
        if (part_byte_pos + cl > part_byte_len) cl = part_byte_len - part_byte_pos;
        concrete_chars++;
        if (cl == 1) {
            RoaringBitmap *bm = biscuit_get_col_pos_bitmap_lower(index, col, col_idx, fb, char_pos);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *mb = NULL; int b;
            for (b = 0; b < cl; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *bm = biscuit_get_col_pos_bitmap_lower(index, col, col_idx, bv, char_pos); if (!bm) { if (mb) biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) mb = biscuit_roaring_copy(bm); else { biscuit_roaring_and_inplace(mb, bm); if (biscuit_roaring_is_empty(mb)) { biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = mb; first_char = false; } else { biscuit_roaring_and_inplace(result, mb); biscuit_roaring_free(mb); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += cl; char_pos++;
        } /* end cl block */
    }

    { int pcc = biscuit_part_char_count(part, part_byte_len);
      if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_col_length_ge_lower(index, col, col_idx, start_pos + pcc); }
      else { len_filter = biscuit_get_col_length_ge_lower(index, col, col_idx, start_pos + pcc); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } } }
    return result ? result : biscuit_roaring_create();
}

static RoaringBitmap *
biscuit_match_col_part_at_end_ilike(Relation index, ColumnIndex *col, int col_idx,
                                    const char *part, int part_byte_len)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int concrete_chars = 0, pcc = biscuit_part_char_count(part, part_byte_len);
    int part_byte_pos = 0, cfe = 0;
    bool first_char = true;

    while (part_byte_pos < part_byte_len) {
        unsigned char fb = (unsigned char) part[part_byte_pos];
        if (fb == (unsigned char) BISCUIT_LITERAL_ESC) {
            unsigned char lb; RoaringBitmap *bm; int neg;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            lb = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            neg = -(pcc - cfe);
            bm = biscuit_get_col_neg_bitmap_lower(index, col, col_idx, lb, neg);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2; cfe++; continue;
        }
        if (fb == '_') { part_byte_pos++; cfe++; continue; }
        {
        int cl = biscuit_utf8_char_length(fb);
        int neg;
        if (part_byte_pos + cl > part_byte_len) cl = part_byte_len - part_byte_pos;
        concrete_chars++;
        neg = -(pcc - cfe);
        if (cl == 1) {
            RoaringBitmap *bm = biscuit_get_col_neg_bitmap_lower(index, col, col_idx, fb, neg);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *mb = NULL; int b;
            for (b = 0; b < cl; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *bm = biscuit_get_col_neg_bitmap_lower(index, col, col_idx, bv, neg); if (!bm) { if (mb) biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) mb = biscuit_roaring_copy(bm); else { biscuit_roaring_and_inplace(mb, bm); if (biscuit_roaring_is_empty(mb)) { biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = mb; first_char = false; } else { biscuit_roaring_and_inplace(result, mb); biscuit_roaring_free(mb); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += cl; cfe++;
        } /* end cl block */
    }

    if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_col_length_ge_lower(index, col, col_idx, pcc); }
    else { len_filter = biscuit_get_col_length_ge_lower(index, col, col_idx, pcc); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } }
    return result ? result : biscuit_roaring_create();
}


/* ================================================================
 * SECTION 7 – Iterative windowed matching (multi-column)
 *
 * Same stack-based approach as Section 5 but operating on a
 * ColumnIndex pointer and using the col-specific bitmap accessors.
 * ================================================================ */

static void
biscuit_recursive_windowed_match_col(
    Relation index, RoaringBitmap *result, ColumnIndex *col, int col_idx,
    const char **parts, int *part_byte_lens, int part_count,
    bool ends_percent, int part_idx, int min_char_pos,
    RoaringBitmap *current_candidates, int max_char_len)
{
    int i;
    WMFrame *stack     = (WMFrame *) palloc(WM_MAX_STACK * sizeof(WMFrame));
    int      stack_top = 0;
    int *pcl = (int *) palloc(part_count * sizeof(int));
    int *suf = (int *) palloc(part_count * sizeof(int));
    for (i = 0; i < part_count; i++)
        pcl[i] = biscuit_part_char_count(parts[i], part_byte_lens[i]);
    suf[part_count - 1] = 0;
    for (i = part_count - 2; i >= 0; i--)
        suf[i] = suf[i + 1] + pcl[i + 1];

    stack[stack_top].part_idx   = part_idx;
    stack[stack_top].min_pos    = min_char_pos;
    stack[stack_top].candidates = biscuit_roaring_copy(current_candidates);
    stack_top++;

    while (stack_top > 0)
    {
        int            pidx;
        int            mpos;
        RoaringBitmap *cands;
        int            part_cl;
        int            suf_len;
        int            max_pos;
        stack_top--;
        pidx  = stack[stack_top].part_idx;
        mpos  = stack[stack_top].min_pos;
        cands = stack[stack_top].candidates;

        if (pidx >= part_count)
        {
            biscuit_roaring_or_inplace(result, cands);
            biscuit_roaring_free(cands);
            continue;
        }

        part_cl = pcl[pidx];
        suf_len = suf[pidx];
        max_pos = max_char_len - part_cl - suf_len;

        if (pidx == part_count - 1 && !ends_percent)
        {
            RoaringBitmap *em = biscuit_match_col_part_at_end(index, col, col_idx, parts[pidx], part_byte_lens[pidx]);
            if (em)
            {
                int mrl;
                RoaringBitmap *lc;
                biscuit_roaring_and_inplace(em, cands);
                mrl = mpos + part_cl;
                lc = biscuit_get_col_length_ge(index, col, col_idx, mrl);
                if (lc) { biscuit_roaring_and_inplace(em, lc); biscuit_roaring_free(lc); }
                if (!biscuit_roaring_is_empty(em))
                    biscuit_roaring_or_inplace(result, em);
                biscuit_roaring_free(em);
            }
            biscuit_roaring_free(cands);
            continue;
        }

        if (mpos > max_pos) { biscuit_roaring_free(cands); continue; }

        for (i = mpos; i <= max_pos; i++)
        {
            int pos;
            RoaringBitmap *pm;
            RoaringBitmap *nc;
            pos = i;
            pm = biscuit_match_col_part_at_pos(index, col, col_idx, parts[pidx], part_byte_lens[pidx], pos);
            if (!pm) continue;
            nc = biscuit_roaring_copy(cands);
            biscuit_roaring_and_inplace(nc, pm);
            biscuit_roaring_free(pm);
            if (biscuit_roaring_is_empty(nc)) { biscuit_roaring_free(nc); continue; }
            if (stack_top >= WM_MAX_STACK)
            {
                biscuit_roaring_or_inplace(result, nc);
                biscuit_roaring_free(nc);
                continue;
            }
            stack[stack_top].part_idx   = pidx + 1;
            stack[stack_top].min_pos    = pos + part_cl;
            stack[stack_top].candidates = nc;
            stack_top++;
        }
        biscuit_roaring_free(cands);
    }

    pfree(stack);
    pfree(pcl);
    pfree(suf);
}

static void
biscuit_recursive_windowed_match_col_ilike(
    Relation index, RoaringBitmap *result, ColumnIndex *col, int col_idx,
    const char **parts, int *part_byte_lens, int part_count,
    bool ends_percent, int part_idx, int min_char_pos,
    RoaringBitmap *current_candidates, int max_char_len)
{
    int i;
    WMFrame *stack     = (WMFrame *) palloc(WM_MAX_STACK * sizeof(WMFrame));
    int      stack_top = 0;
    int *pcl = (int *) palloc(part_count * sizeof(int));
    int *suf = (int *) palloc(part_count * sizeof(int));
    for (i = 0; i < part_count; i++)
        pcl[i] = biscuit_part_char_count(parts[i], part_byte_lens[i]);
    suf[part_count - 1] = 0;
    for (i = part_count - 2; i >= 0; i--)
        suf[i] = suf[i + 1] + pcl[i + 1];

    stack[stack_top].part_idx   = part_idx;
    stack[stack_top].min_pos    = min_char_pos;
    stack[stack_top].candidates = biscuit_roaring_copy(current_candidates);
    stack_top++;

    while (stack_top > 0)
    {
        int            pidx;
        int            mpos;
        RoaringBitmap *cands;
        int            part_cl;
        int            suf_len;
        int            max_pos;
        stack_top--;
        pidx  = stack[stack_top].part_idx;
        mpos  = stack[stack_top].min_pos;
        cands = stack[stack_top].candidates;

        if (pidx >= part_count)
        {
            biscuit_roaring_or_inplace(result, cands);
            biscuit_roaring_free(cands);
            continue;
        }

        part_cl = pcl[pidx];
        suf_len = suf[pidx];
        max_pos = max_char_len - part_cl - suf_len;

        if (pidx == part_count - 1 && !ends_percent)
        {
            RoaringBitmap *em = biscuit_match_col_part_at_end_ilike(index, col, col_idx, parts[pidx], part_byte_lens[pidx]);
            if (em)
            {
                int mrl;
                RoaringBitmap *lc;
                biscuit_roaring_and_inplace(em, cands);
                mrl = mpos + part_cl;
                lc = biscuit_get_col_length_ge_lower(index, col, col_idx, mrl);
                if (lc) { biscuit_roaring_and_inplace(em, lc); biscuit_roaring_free(lc); }
                if (!biscuit_roaring_is_empty(em))
                    biscuit_roaring_or_inplace(result, em);
                biscuit_roaring_free(em);
            }
            biscuit_roaring_free(cands);
            continue;
        }

        if (mpos > max_pos) { biscuit_roaring_free(cands); continue; }

        for (i = mpos; i <= max_pos; i++)
        {
            int pos;
            RoaringBitmap *pm;
            RoaringBitmap *nc;
            pos = i;
            pm = biscuit_match_col_part_at_pos_ilike(index, col, col_idx, parts[pidx], part_byte_lens[pidx], pos);
            if (!pm) continue;
            nc = biscuit_roaring_copy(cands);
            biscuit_roaring_and_inplace(nc, pm);
            biscuit_roaring_free(pm);
            if (biscuit_roaring_is_empty(nc)) { biscuit_roaring_free(nc); continue; }
            if (stack_top >= WM_MAX_STACK)
            {
                biscuit_roaring_or_inplace(result, nc);
                biscuit_roaring_free(nc);
                continue;
            }
            stack[stack_top].part_idx   = pidx + 1;
            stack[stack_top].min_pos    = pos + part_cl;
            stack[stack_top].candidates = nc;
            stack_top++;
        }
        biscuit_roaring_free(cands);
    }

    pfree(stack);
    pfree(pcl);
    pfree(suf);
}

/* ================================================================
 * SECTION 8 – Public query entry points (single-column)
 * ================================================================ */

RoaringBitmap *
biscuit_query_pattern(Relation index, BiscuitIndex *idx, const char *pattern)
{
    int            plen = strlen(pattern);
    ParsedPattern *parsed = NULL;
    int            min_len, i;
    RoaringBitmap *result = NULL;
    int            wildcard_count = 0, percent_count = 0;
    bool           only_wildcards = true;

    /*
     * Defensive gate: an index built with biscuit_ilike_ops never
     * populates the case-sensitive structures this function depends on.
     * The planner should never route a LIKE/NOT LIKE qual to such an
     * index (biscuit_ilike_ops's opfamily doesn't register those
     * operators), so reaching this with legacy_case_mode missing
     * BISCUIT_MODE_LIKE indicates a programming error, not a normal
     * query.
     */
    if (!(idx->legacy_case_mode & BISCUIT_MODE_LIKE))
        ereport(ERROR,
                (errmsg("biscuit: this index was not built with LIKE support"),
                 errhint("The index's opclass (biscuit_ilike_ops) only supports ILIKE/NOT ILIKE.")));

    if (plen == 0) {
        /* Empty pattern '' matches only records with an empty string value */
        RoaringBitmap *lb = (idx->length_bitmaps_legacy)
            ? biscuit_reconcile_pending(index, idx->length_bitmaps_legacy[0], -1, false,
                                          BISCUIT_DIR_KIND_LEN, -1, 0)
            : NULL;
        return lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
    }

    if (plen == 1 && pattern[0] == '%') {
        /* '%' matches every non-tombstoned, non-NULL record */
        RoaringBitmap *lgb = (idx->length_ge_bitmaps_legacy)
            ? biscuit_reconcile_pending(index, idx->length_ge_bitmaps_legacy[0], -1, false,
                                          BISCUIT_DIR_KIND_LEN_GE, -1, 0)
            : NULL;
        if (lgb)
            result = biscuit_roaring_copy(lgb);
        else {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                bool ts = false;
                #ifdef HAVE_ROARING
                ts = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
                #else
                { uint32_t bl = i >> 6, bt = i & 63; ts = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
                #endif
                if (!ts && idx->data_cache[i]) biscuit_roaring_add(result, i);
            }
        }
        return result;
    }

    for (i = 0; i < plen; i++) {
        if (pattern[i] == '%') percent_count++;
        else if (pattern[i] == '_') wildcard_count++;
        else { only_wildcards = false; break; }
    }

    if (only_wildcards) {
        if (percent_count > 0) return biscuit_get_length_ge(index, idx, wildcard_count);
        if (wildcard_count < idx->max_length_legacy && idx->length_bitmaps_legacy[wildcard_count])
        {
            RoaringBitmap *lb = biscuit_reconcile_pending(index, idx->length_bitmaps_legacy[wildcard_count],
                                                            -1, false, BISCUIT_DIR_KIND_LEN, -1, wildcard_count);
            return lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
        }
        return biscuit_roaring_create();
    }

    PG_TRY();
    {
        parsed = biscuit_parse_pattern(pattern);

        if (parsed->part_count == 0) {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                bool ts = false;
                #ifdef HAVE_ROARING
                ts = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
                #else
                { uint32_t bl = i >> 6, bt = i & 63; ts = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
                #endif
                if (!ts) biscuit_roaring_add(result, i);
            }
            biscuit_free_parsed_pattern(parsed);
            parsed = NULL; /* FIX 5: prevent double-free in PG_CATCH */
            return result;
        }

        min_len = 0;
        for (i = 0; i < parsed->part_count; i++)
            min_len += parsed->part_lens[i];

        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                result = biscuit_match_part_at_pos(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (result && min_len < idx->max_length_legacy && idx->length_bitmaps_legacy[min_len])
                {
                    RoaringBitmap *lb = biscuit_reconcile_pending(index, idx->length_bitmaps_legacy[min_len],
                                                                    -1, false, BISCUIT_DIR_KIND_LEN, -1, min_len);
                    biscuit_roaring_and_inplace(result, lb);
                }
                else if (!result || min_len >= idx->max_length_legacy) { if (result) biscuit_roaring_free(result); result = biscuit_roaring_create(); }
            } else if (!parsed->starts_percent) {
                result = biscuit_match_part_at_pos(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                result = biscuit_match_part_at_end(index, idx, parsed->parts[0], parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                /* '%abc%' – substring via char-cache and brute verification */
                result = biscuit_roaring_create();
                if (parsed->part_byte_lens[0] > 0) {
                    unsigned char first_byte = biscuit_part_seed_byte(
                        parsed->parts[0], parsed->part_byte_lens[0]);
                    if (first_byte != 0) {
                        RoaringBitmap *cc = biscuit_reconcile_pending(index, idx->char_cache_legacy[first_byte],
                                                                        -1, false, BISCUIT_DIR_KIND_CACHE,
                                                                        first_byte, -1);
                        RoaringBitmap *candidates = cc
                            ? biscuit_roaring_copy(cc)
                            : biscuit_roaring_create();
                        int part_char_len = parsed->part_lens[0];
                        RoaringBitmap *lf = biscuit_get_length_ge(index, idx, part_char_len);
                        if (lf) { biscuit_roaring_and_inplace(candidates, lf); biscuit_roaring_free(lf); }

                        #ifdef HAVE_ROARING
                        {
                            roaring_uint32_iterator_t *iter = roaring_iterator_create(candidates);
                            while (iter->has_value) {
                                uint32_t rec = iter->current_value;
                                if (rec < (uint32_t) idx->num_records && idx->data_cache[rec]) {
                                    const char *hay = idx->data_cache[rec];
                                    int hbl = strlen(hay), hcl = biscuit_utf8_char_count(hay, hbl);
                                    bool found = false;
                                    for (int cp = 0; cp <= hcl - part_char_len && !found; cp++) {
                                        int bo = biscuit_utf8_char_to_byte_offset(hay, hbl, cp);
                                        if (bo < 0) continue;
                                        if (biscuit_part_match_substr(hay, hbl, bo,
                                                parsed->parts[0], parsed->part_byte_lens[0]))
                                            found = true;
                                    }
                                    if (found) biscuit_roaring_add(result, rec);
                                }
                                roaring_uint32_iterator_advance(iter);
                            }
                            roaring_uint32_iterator_free(iter);
                        }
                        #else
                        {
                            uint64_t cnt;
                            uint32_t *indices = biscuit_roaring_to_array(candidates, &cnt);
                            if (indices) {
                                for (int j = 0; j < (int) cnt; j++) {
                                    uint32_t rec = indices[j];
                                    if (rec < (uint32_t) idx->num_records && idx->data_cache[rec]) {
                                        const char *hay = idx->data_cache[rec];
                                        int hbl = strlen(hay), hcl = biscuit_utf8_char_count(hay, hbl);
                                        bool found = false;
                                        for (int cp = 0; cp <= hcl - part_char_len && !found; cp++) {
                                            int bo = biscuit_utf8_char_to_byte_offset(hay, hbl, cp);
                                            if (bo < 0) continue;
                                            if (biscuit_part_match_substr(hay, hbl, bo,
                                                    parsed->parts[0], parsed->part_byte_lens[0]))
                                                found = true;
                                        }
                                        if (found) biscuit_roaring_add(result, rec);
                                    }
                                }
                                pfree(indices);
                            }
                        }
                        #endif
                        biscuit_roaring_free(candidates);
                    }
                }
            }
        } else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            RoaringBitmap *prefix = biscuit_match_part_at_pos(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
            RoaringBitmap *suffix = biscuit_match_part_at_end(index, idx, parsed->parts[1], parsed->part_byte_lens[1]);
            if (!prefix || !suffix) { if (prefix) biscuit_roaring_free(prefix); if (suffix) biscuit_roaring_free(suffix); result = biscuit_roaring_create(); }
            else {
                RoaringBitmap *lf;
                biscuit_roaring_and_inplace(prefix, suffix); biscuit_roaring_free(suffix);
                lf = biscuit_get_length_ge(index, idx, min_len);
                if (lf) { biscuit_roaring_and_inplace(prefix, lf); biscuit_roaring_free(lf); }
                result = prefix;
            }
        } else {
            RoaringBitmap *candidates;
            result = biscuit_roaring_create();
            candidates = biscuit_get_length_ge(index, idx, min_len);
            if (candidates && !biscuit_roaring_is_empty(candidates)) {
                if (!parsed->starts_percent) {
                    RoaringBitmap *first = biscuit_match_part_at_pos(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                    if (first) { biscuit_roaring_and_inplace(first, candidates); biscuit_roaring_free(candidates); candidates = first; }
                }
                if (!biscuit_roaring_is_empty(candidates)) {
                    biscuit_recursive_windowed_match(index, result, idx,
                        (const char **) parsed->parts, parsed->part_byte_lens, parsed->part_count,
                        parsed->ends_percent, 0, 0, candidates, idx->max_len);
                }
                biscuit_roaring_free(candidates);
            } else if (candidates) biscuit_roaring_free(candidates);
        }

        biscuit_free_parsed_pattern(parsed);
        parsed = NULL; /* FIX 5: prevent double-free in PG_CATCH */
    }
    PG_CATCH();
    {
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return result ? result : biscuit_roaring_create();
}

/* ILIKE single-column: lower-case the pattern first, then reuse logic */

RoaringBitmap *
biscuit_query_pattern_ilike(Relation index, BiscuitIndex *idx, const char *pattern)
{
    int            plen = strlen(pattern);
    char          *pl;
    RoaringBitmap *result;

    /*
     * Defensive gate: an index built with biscuit_like_ops never
     * populates the "_lower" structures this function depends on. The
     * planner should never route an ILIKE/NOT ILIKE qual to such an
     * index in the first place (biscuit_like_ops's opfamily doesn't
     * register those operators -- see biscuit.sql), so reaching this
     * with legacy_case_mode missing BISCUIT_MODE_ILIKE indicates a
     * programming error rather than a normal query, and we fail loudly
     * instead of dereferencing NULL/empty bitmap arrays.
     */
    if (!(idx->legacy_case_mode & BISCUIT_MODE_ILIKE))
        ereport(ERROR,
                (errmsg("biscuit: this index was not built with ILIKE support"),
                 errhint("The index's opclass (biscuit_like_ops) only supports LIKE/NOT LIKE.")));

    pl = biscuit_str_tolower(pattern, plen);

    /* delegate with lowercased pattern, using _lower accessors implicitly
       via biscuit_match_part_at_pos_ilike / _end_ilike / get_length_ge_lower */

    ParsedPattern *parsed = NULL;
    int            min_len, i;
    int            wildcard_count = 0, percent_count = 0;
    bool           only_wildcards = true;

    plen = strlen(pl);

    if (plen == 0) {
        /*
         * Empty pattern '' matches only records with an empty string
         * value. Use the case-insensitive length bitmap here, not the
         * case-sensitive legacy one -- a column built with
         * biscuit_ilike_ops only (no LIKE support) never populates
         * length_bitmaps_legacy, and even when both are built, going
         * through the _lower structures is the correct, mode-consistent
         * choice for an ILIKE entry point.
         */
        RoaringBitmap *lb = (idx->length_bitmaps_lower)
            ? biscuit_reconcile_pending(index, idx->length_bitmaps_lower[0], -1, true,
                                          BISCUIT_DIR_KIND_LEN, -1, 0)
            : NULL;
        result = lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
        pfree(pl); return result;
    }

    if (plen == 1 && pl[0] == '%') {
        /* '%' matches every non-tombstoned, non-NULL record */
        RoaringBitmap *lgb = (idx->length_ge_bitmaps_lower)
            ? biscuit_reconcile_pending(index, idx->length_ge_bitmaps_lower[0], -1, true,
                                          BISCUIT_DIR_KIND_LEN_GE, -1, 0)
            : NULL;
        if (lgb)
            result = biscuit_roaring_copy(lgb);
        else {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                bool ts = false;
#ifdef HAVE_ROARING
                ts = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
#else
                { uint32_t bl = i >> 6, bt = i & 63; ts = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
#endif
                if (!ts && idx->data_cache_lower && idx->data_cache_lower[i]) biscuit_roaring_add(result, i);
            }
        }
        pfree(pl); return result;
    }

    for (i = 0; i < plen; i++) {
        if (pl[i] == '%') percent_count++;
        else if (pl[i] == '_') wildcard_count++;
        else { only_wildcards = false; break; }
    }

    if (only_wildcards) {
        if (percent_count > 0) result = biscuit_get_length_ge_lower(index, idx, wildcard_count);
        else if (wildcard_count < idx->max_length_lower && idx->length_bitmaps_lower && idx->length_bitmaps_lower[wildcard_count])
        {
            RoaringBitmap *lb = biscuit_reconcile_pending(index, idx->length_bitmaps_lower[wildcard_count],
                                                            -1, true, BISCUIT_DIR_KIND_LEN, -1, wildcard_count);
            result = lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
        }
        else result = biscuit_roaring_create();
        pfree(pl); return result;
    }

    PG_TRY();
    {
        parsed = biscuit_parse_pattern(pl);
        if (parsed->part_count == 0) {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                bool ts = false;
#ifdef HAVE_ROARING
                ts = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
#else
                { uint32_t bl = i >> 6, bt = i & 63; ts = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
#endif
                if (!ts) biscuit_roaring_add(result, i);
            }
            biscuit_free_parsed_pattern(parsed); parsed = NULL; /* FIX 5 */ pfree(pl); return result;
        }
        min_len = 0;
        for (i = 0; i < parsed->part_count; i++) min_len += parsed->part_lens[i];

        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                result = biscuit_match_part_at_pos_ilike(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (result && min_len < idx->max_length_lower && idx->length_bitmaps_lower && idx->length_bitmaps_lower[min_len])
                {
                    RoaringBitmap *lb = biscuit_reconcile_pending(index, idx->length_bitmaps_lower[min_len],
                                                                    -1, true, BISCUIT_DIR_KIND_LEN, -1, min_len);
                    biscuit_roaring_and_inplace(result, lb);
                }
                else if (!result || min_len >= idx->max_length_lower) { if (result) biscuit_roaring_free(result); result = biscuit_roaring_create(); }
            } else if (!parsed->starts_percent) {
                result = biscuit_match_part_at_pos_ilike(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                result = biscuit_match_part_at_end_ilike(index, idx, parsed->parts[0], parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                result = biscuit_roaring_create();
                /* substring ILIKE – similar brute verification via lowercase cache */
                if (parsed->part_byte_lens[0] > 0) {
                    unsigned char first_byte = biscuit_part_seed_byte(
                        parsed->parts[0], parsed->part_byte_lens[0]);
                    RoaringBitmap *cc = (first_byte != 0)
                        ? biscuit_reconcile_pending(index, idx->char_cache_lower[first_byte], -1, true,
                                                      BISCUIT_DIR_KIND_CACHE, first_byte, -1)
                        : NULL;
                    RoaringBitmap *candidates = cc
                        ? biscuit_roaring_copy(cc)
                        : biscuit_roaring_create();
                    int pcl = parsed->part_lens[0];
                    RoaringBitmap *lf = biscuit_get_length_ge_lower(index, idx, pcl);
                    if (lf) { biscuit_roaring_and_inplace(candidates, lf); biscuit_roaring_free(lf); }
                    #ifdef HAVE_ROARING
                    { roaring_uint32_iterator_t *iter = roaring_iterator_create(candidates);
                      while (iter->has_value) { uint32_t rec = iter->current_value;
                        if (rec < (uint32_t) idx->num_records && idx->data_cache_lower && idx->data_cache_lower[rec]) {
                            const char *hay = idx->data_cache_lower[rec];
                            int hbl = strlen(hay), hcl = biscuit_utf8_char_count(hay, hbl);
                            bool found = false;
                            for (int cp = 0; cp <= hcl - pcl && !found; cp++) {
                                int bo = biscuit_utf8_char_to_byte_offset(hay, hbl, cp);
                                if (bo >= 0 && biscuit_part_match_substr(hay, hbl, bo,
                                        parsed->parts[0], parsed->part_byte_lens[0]))
                                    found = true;
                            }
                            if (found) biscuit_roaring_add(result, rec);
                        }
                        roaring_uint32_iterator_advance(iter); }
                      roaring_uint32_iterator_free(iter); }
                    #else
                    { uint64_t cnt; uint32_t *indices = biscuit_roaring_to_array(candidates, &cnt);
                      if (indices) { for (int j = 0; j < (int) cnt; j++) { uint32_t rec = indices[j];
                            if (rec < (uint32_t) idx->num_records && idx->data_cache_lower && idx->data_cache_lower[rec]) {
                                const char *hay = idx->data_cache_lower[rec];
                                int hbl = strlen(hay), hcl = biscuit_utf8_char_count(hay, hbl);
                                bool found = false;
                                for (int cp = 0; cp <= hcl - pcl && !found; cp++) {
                                    int bo = biscuit_utf8_char_to_byte_offset(hay, hbl, cp);
                                    if (bo >= 0 && biscuit_part_match_substr(hay, hbl, bo,
                                            parsed->parts[0], parsed->part_byte_lens[0]))
                                        found = true;
                                }
                                if (found) biscuit_roaring_add(result, rec); } }
                          pfree(indices); } }
                    #endif
                    biscuit_roaring_free(candidates);
                }
            }
        } else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            RoaringBitmap *prefix = biscuit_match_part_at_pos_ilike(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
            RoaringBitmap *suffix = biscuit_match_part_at_end_ilike(index, idx, parsed->parts[1], parsed->part_byte_lens[1]);
            if (!prefix || !suffix) { if (prefix) biscuit_roaring_free(prefix); if (suffix) biscuit_roaring_free(suffix); result = biscuit_roaring_create(); }
            else { RoaringBitmap *lf;
                   biscuit_roaring_and_inplace(prefix, suffix); biscuit_roaring_free(suffix);
                   lf = biscuit_get_length_ge_lower(index, idx, min_len);
                   if (lf) { biscuit_roaring_and_inplace(prefix, lf); biscuit_roaring_free(lf); }
                   result = prefix; }
        } else {
            RoaringBitmap *candidates;
            result = biscuit_roaring_create();
            candidates = biscuit_get_length_ge_lower(index, idx, min_len);
            if (candidates && !biscuit_roaring_is_empty(candidates)) {
                if (!parsed->starts_percent) { RoaringBitmap *first = biscuit_match_part_at_pos_ilike(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0); if (first) { biscuit_roaring_and_inplace(first, candidates); biscuit_roaring_free(candidates); candidates = first; } }
                if (!biscuit_roaring_is_empty(candidates))
                    biscuit_recursive_windowed_match_ilike(index, result, idx, (const char **) parsed->parts, parsed->part_byte_lens, parsed->part_count, parsed->ends_percent, 0, 0, candidates, idx->max_length_lower);
                biscuit_roaring_free(candidates);
            } else if (candidates) biscuit_roaring_free(candidates);
        }

        biscuit_free_parsed_pattern(parsed);
        parsed = NULL; /* FIX 5 */
    }
    PG_CATCH();
    {
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        pfree(pl);
        PG_RE_THROW();
    }
    PG_END_TRY();

    pfree(pl);
    return result ? result : biscuit_roaring_create();
}

/* ================================================================
 * SECTION 9 – Public multi-column query entry points
 * ================================================================ */

RoaringBitmap *
biscuit_query_column_pattern(Relation index, BiscuitIndex *idx, int col_idx, const char *pattern)
{
    ColumnIndex   *col;
    int            plen = strlen(pattern);
    ParsedPattern *parsed = NULL;
    int            min_len, i;
    RoaringBitmap *result = NULL;
    int            wildcard_count = 0, percent_count = 0;
    bool           only_wildcards = true;

    if (!idx || col_idx < 0 || col_idx >= idx->num_columns || !idx->column_indices)
        return biscuit_roaring_create();

    /*
     * Defensive gate: a column built with biscuit_ilike_ops never
     * populates the case-sensitive structures this function needs. The
     * planner should never route a LIKE/NOT LIKE qual to such a column
     * (biscuit_ilike_ops's opfamily doesn't register those operators),
     * so reaching this indicates a programming error rather than a
     * normal query.
     */
    if (idx->column_case_mode && !(idx->column_case_mode[col_idx] & BISCUIT_MODE_LIKE))
        ereport(ERROR,
                (errmsg("biscuit: column %d of this index was not built with LIKE support",
                        col_idx),
                 errhint("This column's opclass (biscuit_ilike_ops) only supports ILIKE/NOT ILIKE.")));

    col = &idx->column_indices[col_idx];

    if (!col->length_bitmaps || !col->length_ge_bitmaps || col->max_length <= 0)
        return biscuit_roaring_create();

    if (plen == 0) {
        RoaringBitmap *lb = biscuit_reconcile_pending(index, col->length_bitmaps[0],
                                                        col_idx, false, BISCUIT_DIR_KIND_LEN, -1, 0);
        return lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
    }
    if (plen == 1 && pattern[0] == '%') {
        RoaringBitmap *lgb = biscuit_reconcile_pending(index, col->length_ge_bitmaps[0],
                                                         col_idx, false, BISCUIT_DIR_KIND_LEN_GE, -1, 0);
        return lgb ? biscuit_roaring_copy(lgb) : biscuit_roaring_create();
    }

    for (i = 0; i < plen; i++) {
        if (pattern[i] == '%') percent_count++;
        else if (pattern[i] == '_') wildcard_count++;
        else { only_wildcards = false; break; }
    }

    if (only_wildcards) {
        /*
         * col->max_length is the allocated size of length_bitmaps /
         * length_ge_bitmaps (valid indices 0..max_length-1), so this must
         * be a strict "<". "<=" let wildcard_count == max_length slip
         * through, reading one RoaringBitmap* past the end of the
         * palloc'd array on an ordinary LIKE/ILIKE query whose all-wildcard
         * pattern length equals the column's max indexed length.
         */
        if (percent_count > 0 && wildcard_count < col->max_length && col->length_ge_bitmaps[wildcard_count])
        {
            RoaringBitmap *lgb = biscuit_reconcile_pending(index, col->length_ge_bitmaps[wildcard_count],
                                                             col_idx, false, BISCUIT_DIR_KIND_LEN_GE,
                                                             -1, wildcard_count);
            return lgb ? biscuit_roaring_copy(lgb) : biscuit_roaring_create();
        }
        if (!percent_count && wildcard_count < col->max_length && col->length_bitmaps[wildcard_count])
        {
            RoaringBitmap *lb = biscuit_reconcile_pending(index, col->length_bitmaps[wildcard_count],
                                                            col_idx, false, BISCUIT_DIR_KIND_LEN,
                                                            -1, wildcard_count);
            return lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
        }
        return biscuit_roaring_create();
    }

    PG_TRY();
    {
        parsed = biscuit_parse_pattern(pattern);
        if (parsed->part_count == 0) {
            RoaringBitmap *lgb = biscuit_reconcile_pending(index, col->length_ge_bitmaps[0],
                                                             col_idx, false, BISCUIT_DIR_KIND_LEN_GE, -1, 0);
            result = lgb ? biscuit_roaring_copy(lgb) : biscuit_roaring_create();
            biscuit_free_parsed_pattern(parsed); parsed = NULL; /* FIX 5 */ return result;
        }
        min_len = 0;
        for (i = 0; i < parsed->part_count; i++) min_len += parsed->part_lens[i];

        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                result = biscuit_match_col_part_at_pos(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                /* col->max_length is the array size (valid indices 0..max_length-1); "<=" read one past the end */
                if (result && min_len < col->max_length && col->length_bitmaps[min_len])
                {
                    RoaringBitmap *lb = biscuit_reconcile_pending(index, col->length_bitmaps[min_len],
                                                                    col_idx, false, BISCUIT_DIR_KIND_LEN,
                                                                    -1, min_len);
                    biscuit_roaring_and_inplace(result, lb);
                }
                else if (result) { biscuit_roaring_free(result); result = biscuit_roaring_create(); }
                else result = biscuit_roaring_create();
            } else if (!parsed->starts_percent) {
                result = biscuit_match_col_part_at_pos(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                result = biscuit_match_col_part_at_end(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                /*
                 * Substring LIKE: %needle%
                 * Seed candidates from char_cache[first_concrete_byte]
                 * (case-sensitive) — biscuit_part_seed_byte() skips any
                 * leading '_' wildcards / BISCUIT_LITERAL_ESC prefixes so
                 * a pattern like '%_lex%' seeds from 'l', not '_'.
                 * Filter by minimum length, then verify with a
                 * wildcard-aware match.
                 */
                result = biscuit_roaring_create();
                if (parsed->part_byte_lens[0] > 0)
                {
                    unsigned char  fb    = biscuit_part_seed_byte(parsed->parts[0], parsed->part_byte_lens[0]);
                    RoaringBitmap *cc    = fb != 0
                                          ? biscuit_reconcile_pending(index, col->char_cache[fb], col_idx,
                                                                       false, BISCUIT_DIR_KIND_CACHE, fb, -1)
                                          : NULL;
                    RoaringBitmap *lgb0  = biscuit_reconcile_pending(index, col->length_ge_bitmaps[0], col_idx,
                                                                       false, BISCUIT_DIR_KIND_LEN_GE, -1, 0);
                    RoaringBitmap *cands = (fb != 0 && cc)
                                          ? biscuit_roaring_copy(cc)
                                          : (fb != 0
                                             ? biscuit_roaring_create()
                                             : (lgb0
                                                ? biscuit_roaring_copy(lgb0)
                                                : biscuit_roaring_create()));
                    int            pcl   = parsed->part_lens[0];
                    RoaringBitmap *lf    = biscuit_get_col_length_ge(index, col, col_idx, pcl);

                    if (lf) { biscuit_roaring_and_inplace(cands, lf); biscuit_roaring_free(lf); }

#ifdef HAVE_ROARING
                    {
                        roaring_uint32_iterator_t *iter = roaring_iterator_create(cands);
                        while (iter->has_value)
                        {
                            uint32_t    rec = iter->current_value;
                            const char *hay;
                            if (rec < (uint32_t) idx->num_records &&
                                idx->column_data_cache &&
                                idx->column_data_cache[col_idx] &&
                                (hay = idx->column_data_cache[col_idx][rec]) != NULL)
                            {
                                if (biscuit_wildcard_contains(hay, strlen(hay),
                                                               parsed->parts[0],
                                                               parsed->part_byte_lens[0]))
                                    biscuit_roaring_add(result, rec);
                            }
                            roaring_uint32_iterator_advance(iter);
                        }
                        roaring_uint32_iterator_free(iter);
                    }
#else
                    {
                        uint64_t  cnt;
                        uint32_t *indices = biscuit_roaring_to_array(cands, &cnt);
                        if (indices)
                        {
                            int j;
                            for (j = 0; j < (int) cnt; j++)
                            {
                                uint32_t    rec = indices[j];
                                const char *hay;
                                if (rec < (uint32_t) idx->num_records &&
                                    idx->column_data_cache &&
                                    idx->column_data_cache[col_idx] &&
                                    (hay = idx->column_data_cache[col_idx][rec]) != NULL)
                                {
                                    if (biscuit_wildcard_contains(hay, strlen(hay),
                                                                   parsed->parts[0],
                                                                   parsed->part_byte_lens[0]))
                                        biscuit_roaring_add(result, rec);
                                }
                            }
                            pfree(indices);
                        }
                    }
#endif
                    biscuit_roaring_free(cands);
                }
            }
        } else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            RoaringBitmap *prefix = biscuit_match_col_part_at_pos(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
            RoaringBitmap *suffix = biscuit_match_col_part_at_end(index, col, col_idx, parsed->parts[1], parsed->part_byte_lens[1]);
            if (!prefix || !suffix) { if (prefix) biscuit_roaring_free(prefix); if (suffix) biscuit_roaring_free(suffix); result = biscuit_roaring_create(); }
            else { RoaringBitmap *lf;
                   biscuit_roaring_and_inplace(prefix, suffix); biscuit_roaring_free(suffix);
                   lf = biscuit_get_col_length_ge(index, col, col_idx, min_len);
                   if (lf) { biscuit_roaring_and_inplace(prefix, lf); biscuit_roaring_free(lf); }
                   result = prefix; }
        } else {
            RoaringBitmap *cands;
            result = biscuit_roaring_create();
            cands = biscuit_get_col_length_ge(index, col, col_idx, min_len);
            if (cands && !biscuit_roaring_is_empty(cands)) {
                if (!parsed->starts_percent) { RoaringBitmap *first = biscuit_match_col_part_at_pos(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0); if (first) { biscuit_roaring_and_inplace(first, cands); biscuit_roaring_free(cands); cands = first; } }
                if (!biscuit_roaring_is_empty(cands))
                    biscuit_recursive_windowed_match_col(index, result, col, col_idx, (const char **) parsed->parts, parsed->part_byte_lens, parsed->part_count, parsed->ends_percent, 0, 0, cands, col->max_length);
                biscuit_roaring_free(cands);
            } else if (cands) biscuit_roaring_free(cands);
        }
        biscuit_free_parsed_pattern(parsed);
        parsed = NULL; /* FIX 5 */
    }
    PG_CATCH();
    {
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return result ? result : biscuit_roaring_create();
}

/* ILIKE variant for multi-column: lowercase pattern first */
RoaringBitmap *
biscuit_query_column_pattern_ilike(Relation index, BiscuitIndex *idx, int col_idx, const char *pattern)
{
    char          *pl;
    RoaringBitmap *result;
    ColumnIndex   *col;
    int            plen = strlen(pattern);

    if (!idx || col_idx < 0 || col_idx >= idx->num_columns || !idx->column_indices)
        return biscuit_roaring_create();

    /*
     * Defensive gate: a column built with biscuit_like_ops never
     * populates the case-insensitive "_lower" structures this function
     * needs. The planner should never route an ILIKE/NOT ILIKE qual to
     * such a column (biscuit_like_ops's opfamily doesn't register those
     * operators), so reaching this indicates a programming error rather
     * than a normal query.
     */
    if (idx->column_case_mode && !(idx->column_case_mode[col_idx] & BISCUIT_MODE_ILIKE))
        ereport(ERROR,
                (errmsg("biscuit: column %d of this index was not built with ILIKE support",
                        col_idx),
                 errhint("This column's opclass (biscuit_like_ops) only supports LIKE/NOT LIKE.")));

    col = &idx->column_indices[col_idx];
    pl  = biscuit_str_tolower(pattern, plen);

    /* delegate via case-insensitive (lower) accessors in a similar flow */
    {
    ParsedPattern *parsed = NULL;
    int            min_len, i;
    int            wc = 0, pc = 0;
    bool           ow = true;

    plen = strlen(pl);
    result = NULL;

    if (plen == 0) {
        RoaringBitmap *lb = (col->length_bitmaps_lower)
            ? biscuit_reconcile_pending(index, col->length_bitmaps_lower[0], col_idx, true,
                                          BISCUIT_DIR_KIND_LEN, -1, 0)
            : NULL;
        result = lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
        pfree(pl); return result;
    }
    if (plen == 1 && pl[0] == '%') {
        RoaringBitmap *lgb = (col->length_ge_bitmaps_lower)
            ? biscuit_reconcile_pending(index, col->length_ge_bitmaps_lower[0], col_idx, true,
                                          BISCUIT_DIR_KIND_LEN_GE, -1, 0)
            : NULL;
        result = lgb ? biscuit_roaring_copy(lgb) : biscuit_roaring_create();
        pfree(pl); return result;
    }

    for (i = 0; i < plen; i++) { if (pl[i] == '%') pc++; else if (pl[i] == '_') wc++; else { ow = false; break; } }
    if (ow) {
        if (pc > 0) result = biscuit_get_col_length_ge_lower(index, col, col_idx, wc);
        /* col->max_length_lower is the array size (valid indices 0..max_length_lower-1); "<=" read one past the end */
        else if (wc < col->max_length_lower && col->length_bitmaps_lower && col->length_bitmaps_lower[wc])
        {
            RoaringBitmap *lb = biscuit_reconcile_pending(index, col->length_bitmaps_lower[wc], col_idx, true,
                                                            BISCUIT_DIR_KIND_LEN, -1, wc);
            result = lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
        }
        else result = biscuit_roaring_create();
        pfree(pl); return result;
    }

    PG_TRY();
    {
        parsed = biscuit_parse_pattern(pl);
        if (parsed->part_count == 0) {
            RoaringBitmap *lgb = (col->length_ge_bitmaps_lower)
                ? biscuit_reconcile_pending(index, col->length_ge_bitmaps_lower[0], col_idx, true,
                                              BISCUIT_DIR_KIND_LEN_GE, -1, 0)
                : NULL;
            result = lgb ? biscuit_roaring_copy(lgb) : biscuit_roaring_create();
            biscuit_free_parsed_pattern(parsed); parsed = NULL; /* FIX 5 */ pfree(pl); return result;
        }
        min_len = 0;
        for (i = 0; i < parsed->part_count; i++) min_len += parsed->part_lens[i];

        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                result = biscuit_match_col_part_at_pos_ilike(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                /* col->max_length_lower is the array size (valid indices 0..max_length_lower-1); "<=" read one past the end */
                if (result && min_len < col->max_length_lower && col->length_bitmaps_lower && col->length_bitmaps_lower[min_len])
                {
                    RoaringBitmap *lb = biscuit_reconcile_pending(index, col->length_bitmaps_lower[min_len], col_idx,
                                                                    true, BISCUIT_DIR_KIND_LEN, -1, min_len);
                    biscuit_roaring_and_inplace(result, lb);
                }
                else if (result) { biscuit_roaring_free(result); result = biscuit_roaring_create(); }
                else result = biscuit_roaring_create();
            } else if (!parsed->starts_percent) {
                result = biscuit_match_col_part_at_pos_ilike(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                result = biscuit_match_col_part_at_end_ilike(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                /*
                 * Substring ILIKE: %needle%
                 * pl (the pattern) has already been fully lowercased above.
                 * parsed->parts[0] is therefore also lowercase.
                 * Seed candidates from char_cache_lower[first_concrete_byte]
                 * — biscuit_part_seed_byte() skips leading '_' wildcards /
                 * BISCUIT_LITERAL_ESC prefixes so a pattern like '%_lex%'
                 * seeds from 'l', not '_'. Filter by minimum length, then
                 * verify with a wildcard-aware match against the
                 * lowercased data cache.
                 */
                result = biscuit_roaring_create();
                if (parsed->part_byte_lens[0] > 0)
                {
                    unsigned char  fb   = biscuit_part_seed_byte(parsed->parts[0], parsed->part_byte_lens[0]); /* lowercase first concrete byte */
                    RoaringBitmap *cc   = fb != 0
                                        ? biscuit_reconcile_pending(index, col->char_cache_lower[fb], col_idx,
                                                                     true, BISCUIT_DIR_KIND_CACHE, fb, -1)
                                        : NULL;
                    RoaringBitmap *lgb0 = col->length_ge_bitmaps_lower
                                        ? biscuit_reconcile_pending(index, col->length_ge_bitmaps_lower[0], col_idx,
                                                                     true, BISCUIT_DIR_KIND_LEN_GE, -1, 0)
                                        : NULL;
                    RoaringBitmap *cands = (fb != 0 && cc)
                                          ? biscuit_roaring_copy(cc)
                                          : (fb != 0
                                             ? biscuit_roaring_create()
                                             : (lgb0
                                                ? biscuit_roaring_copy(lgb0)
                                                : biscuit_roaring_create()));
                    int            pcl   = parsed->part_lens[0];
                    RoaringBitmap *lf    = biscuit_get_col_length_ge_lower(index, col, col_idx, pcl);

                    if (lf) { biscuit_roaring_and_inplace(cands, lf); biscuit_roaring_free(lf); }

#ifdef HAVE_ROARING
                    {
                        roaring_uint32_iterator_t *iter = roaring_iterator_create(cands);
                        while (iter->has_value)
                        {
                            uint32_t    rec = iter->current_value;
                            const char *hay;
                            /*
                             * Use the pre-lowercased cache populated at build
                             * / insert time.  This avoids a
                             * palloc + tolower + pfree per candidate — the hot
                             * path inside an already bitmap-pruned set.
                             * column_data_cache_lower mirrors column_data_cache
                             * slot-for-slot; a NULL entry means the source value
                             * was NULL, so the same guard applies.
                             */
                            if (rec < (uint32_t) idx->num_records &&
                                idx->column_data_cache_lower &&
                                idx->column_data_cache_lower[col_idx] &&
                                (hay = idx->column_data_cache_lower[col_idx][rec]) != NULL)
                            {
                                if (biscuit_wildcard_contains(hay, strlen(hay),
                                                               parsed->parts[0],
                                                               parsed->part_byte_lens[0]))
                                    biscuit_roaring_add(result, rec);
                            }
                            roaring_uint32_iterator_advance(iter);
                        }
                        roaring_uint32_iterator_free(iter);
                    }
#else
                    {
                        uint64_t  cnt;
                        uint32_t *indices = biscuit_roaring_to_array(cands, &cnt);
                        if (indices)
                        {
                            int j;
                            for (j = 0; j < (int) cnt; j++)
                            {
                                uint32_t    rec = indices[j];
                                const char *hay;
                                if (rec < (uint32_t) idx->num_records &&
                                    idx->column_data_cache_lower &&
                                    idx->column_data_cache_lower[col_idx] &&
                                    (hay = idx->column_data_cache_lower[col_idx][rec]) != NULL)
                                {
                                    if (biscuit_wildcard_contains(hay, strlen(hay),
                                                                   parsed->parts[0],
                                                                   parsed->part_byte_lens[0]))
                                        biscuit_roaring_add(result, rec);
                                }
                            }
                            pfree(indices);
                        }
                    }
#endif
                    biscuit_roaring_free(cands);
                }
            }
        } else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            RoaringBitmap *prefix = biscuit_match_col_part_at_pos_ilike(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
            RoaringBitmap *suffix = biscuit_match_col_part_at_end_ilike(index, col, col_idx, parsed->parts[1], parsed->part_byte_lens[1]);
            if (!prefix || !suffix) { if (prefix) biscuit_roaring_free(prefix); if (suffix) biscuit_roaring_free(suffix); result = biscuit_roaring_create(); }
            else { RoaringBitmap *lf; biscuit_roaring_and_inplace(prefix, suffix); biscuit_roaring_free(suffix); lf = biscuit_get_col_length_ge_lower(index, col, col_idx, min_len); if (lf) { biscuit_roaring_and_inplace(prefix, lf); biscuit_roaring_free(lf); } result = prefix; }
        } else {
            RoaringBitmap *cands;
            result = biscuit_roaring_create();
            cands = biscuit_get_col_length_ge_lower(index, col, col_idx, min_len);
            if (cands && !biscuit_roaring_is_empty(cands)) {
                if (!parsed->starts_percent) { RoaringBitmap *first = biscuit_match_col_part_at_pos_ilike(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0); if (first) { biscuit_roaring_and_inplace(first, cands); biscuit_roaring_free(cands); cands = first; } }
                if (!biscuit_roaring_is_empty(cands))
                    biscuit_recursive_windowed_match_col_ilike(index, result, col, col_idx, (const char **) parsed->parts, parsed->part_byte_lens, parsed->part_count, parsed->ends_percent, 0, 0, cands, col->max_length_lower);
                biscuit_roaring_free(cands);
            } else if (cands) biscuit_roaring_free(cands);
        }
        biscuit_free_parsed_pattern(parsed);
        parsed = NULL; /* FIX 5 */
    }
    PG_CATCH();
    {
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        pfree(pl);
        PG_RE_THROW();
    }
    PG_END_TRY();

    pfree(pl);
    return result ? result : biscuit_roaring_create();
    } /* end parsed block */
}

/* ================================================================
 * SECTION 10 – Query plan / optimizer
 * ================================================================ */

static int
calculate_anchor_strength(const char *pattern, bool is_prefix, bool is_suffix)
{
    int strength = 0, i, len = strlen(pattern);
    if (!is_prefix && !is_suffix) return 0;
    if (is_prefix) { for (i = 0; i < len && pattern[i] != '%'; i++) strength += (pattern[i] != '_') ? 10 : 3; }
    if (is_suffix) { int ss = len; for (i = len - 1; i >= 0 && pattern[i] != '%'; i--) ss = i; for (i = ss; i < len; i++) strength += (pattern[i] != '_') ? 10 : 3; }
    return Min(strength, 100);
}

static void
analyze_pattern(QueryPredicate *pred)
{
    const char *p = pred->pattern;
    int len = strlen(p), i;
    bool in_pct = false;

    pred->concrete_chars = pred->underscore_count = pred->percent_count = 0;
    pred->partition_count = 0; pred->has_percent = false;

    for (i = 0; i < len; i++) {
        if (p[i] == '%') { pred->has_percent = true; if (!in_pct) { pred->percent_count++; in_pct = true; } }
        else { if (in_pct) pred->partition_count++; in_pct = false; if (p[i] == '_') pred->underscore_count++; else pred->concrete_chars++; }
    }
    if (!in_pct && len > 0) pred->partition_count++;

    pred->is_exact  = !pred->has_percent && pred->underscore_count == 0;
    pred->is_prefix = (len > 0 && p[0] != '%') && pred->has_percent;
    pred->is_suffix = (len > 0 && p[len - 1] != '%') && pred->has_percent;
    pred->is_substring = pred->starts_percent = pred->ends_percent = false;
    if (pred->has_percent) {
        pred->starts_percent  = (p[0] == '%');
        pred->ends_percent    = (p[len - 1] == '%');
        pred->is_substring    = pred->starts_percent && pred->ends_percent && pred->percent_count >= 2;
    }
    pred->anchor_strength = calculate_anchor_strength(p, pred->is_prefix, pred->is_suffix);
    pred->selectivity_score = pred->is_exact ? 0.0
        : pred->is_prefix || pred->is_suffix ? 0.1 + (pred->percent_count * 0.1)
        : pred->is_substring ? 0.5 : 0.8;
    pred->selectivity_score -= (pred->concrete_chars * 0.05);
    if (pred->selectivity_score < 0.0) pred->selectivity_score = 0.01;
    if (pred->selectivity_score > 1.0) pred->selectivity_score = 1.0;
}

static int
compare_predicates(const void *a, const void *b)
{
    const QueryPredicate *pa = (const QueryPredicate *) a;
    const QueryPredicate *pb = (const QueryPredicate *) b;
    if (pa->selectivity_score < pb->selectivity_score) return -1;
    if (pa->selectivity_score > pb->selectivity_score) return  1;
    return 0;
}

QueryPlan *
biscuit_build_query_plan(BiscuitIndex *idx, ScanKey keys, int nkeys)
{
    QueryPlan *plan;
    int        i;

    (void) idx;  /* reserved for future cardinality-aware planning */

    plan            = (QueryPlan *) palloc(sizeof(QueryPlan));
    plan->predicates = (QueryPredicate *) palloc(nkeys * sizeof(QueryPredicate));
    plan->count     = 0;
    plan->capacity  = nkeys;

    for (i = 0; i < nkeys; i++)
    {
        ScanKey        key  = &keys[i];
        QueryPredicate *pred = &plan->predicates[plan->count];

        if (key->sk_flags & SK_ISNULL)
            continue;

        pred->column_index = key->sk_attno - 1;
        pred->scan_key     = key;

        {
            text *pt = DatumGetTextPP(key->sk_argument);
            pred->pattern = pstrdup(text_to_cstring(pt));
        }

        analyze_pattern(pred);
        plan->count++;
    }

    /* Sort by selectivity: most selective first */
    if (plan->count > 1)
        qsort(plan->predicates, plan->count, sizeof(QueryPredicate), compare_predicates);

    return plan;
}

void
biscuit_free_query_plan(QueryPlan *plan)
{
    int i;

    if (!plan)
        return;

    PG_TRY();
    {
        if (plan->predicates) {
            for (i = 0; i < plan->count; i++) {
                if (plan->predicates[i].pattern) {
                    pfree(plan->predicates[i].pattern);
                    plan->predicates[i].pattern = NULL;
                }
            }
            pfree(plan->predicates);
            plan->predicates = NULL;
        }
        pfree(plan);
    }
    PG_CATCH();
    {
        FlushErrorState();
    }
    PG_END_TRY();
}
