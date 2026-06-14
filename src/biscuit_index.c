/*
 * biscuit_index.c
 * Index construction (build / load), disk metadata I/O, CRUD record
 * management, and AM maintenance callbacks (insert, bulkdelete,
 * vacuumcleanup, costestimate, options, validate, adjustmembers).
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_utf8.h"
#include "biscuit_cache.h"
#include "biscuit_index.h"

/* ================================================================
 * SECTION 1 – Disk metadata I/O
 * ================================================================ */

void
biscuit_write_metadata_to_disk(Relation index, BiscuitIndex *idx)
{
    Buffer             buf;
    Page               page;
    GenericXLogState  *state;
    BiscuitMetaPageData *meta;

    buf   = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    state = GenericXLogStart(index);
    page  = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

    PageInit(page, BufferGetPageSize(buf), sizeof(BiscuitMetaPageData));

    meta          = (BiscuitMetaPageData *) PageGetSpecialPointer(page);
    meta->magic   = BISCUIT_MAGIC;
    meta->version = BISCUIT_VERSION;
    meta->num_records = idx->num_records;
    meta->root    = 0;

    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

bool
biscuit_read_metadata_from_disk(Relation index,
                                int *num_records,
                                int *num_columns,
                                int *max_len)
{
    Buffer             buf;
    Page               page;
    BiscuitMetaPageData *meta;
    BlockNumber        nblocks = RelationGetNumberOfBlocks(index);

    if (nblocks == 0)
    {
        *num_records = *num_columns = *max_len = 0;
        return false;
    }

    buf  = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);

    if (PageIsNew(page) || PageIsEmpty(page))
    {
        UnlockReleaseBuffer(buf);
        *num_records = *num_columns = *max_len = 0;
        return false;
    }

    meta = (BiscuitMetaPageData *) PageGetSpecialPointer(page);

    if (meta->magic != BISCUIT_MAGIC)
    {
        UnlockReleaseBuffer(buf);
        *num_records = *num_columns = *max_len = 0;
        return false;
    }

    *num_records = meta->num_records;
    *num_columns = 0;
    *max_len     = 0;

    UnlockReleaseBuffer(buf);
    return true;
}

/* ================================================================
 * SECTION 2 – CRUD helpers
 * ================================================================ */

void
biscuit_init_crud_structures(BiscuitIndex *idx)
{
    idx->tombstones     = biscuit_roaring_create();
    idx->free_capacity  = 64;
    idx->free_count     = 0;
    idx->free_list      = (uint32_t *) palloc(idx->free_capacity * sizeof(uint32_t));
    idx->tombstone_count = 0;
    idx->insert_count   = 0;
    idx->update_count   = 0;
    idx->delete_count   = 0;
}

void
biscuit_push_free_slot(BiscuitIndex *idx, uint32_t slot)
{
    if (idx->free_count >= idx->free_capacity)
    {
        int       new_cap  = idx->free_capacity * 2;
        uint32_t *new_list = (uint32_t *) palloc(new_cap * sizeof(uint32_t));
        memcpy(new_list, idx->free_list, idx->free_count * sizeof(uint32_t));
        pfree(idx->free_list);
        idx->free_list     = new_list;
        idx->free_capacity = new_cap;
    }
    idx->free_list[idx->free_count++] = slot;
}

bool
biscuit_pop_free_slot(BiscuitIndex *idx, uint32_t *slot)
{
    if (idx->free_count == 0)
        return false;
    *slot = idx->free_list[--idx->free_count];
    return true;
}

/*
 * Remove a record from every character and length bitmap.
 * Handles both single-column (legacy) and multi-column layouts.
 */
void
biscuit_remove_from_all_indices(BiscuitIndex *idx, uint32_t rec_idx)
{
    int ch, j, col;

    if (!idx)
        return;

    /* -------- Multi-column -------- */
    if (idx->num_columns > 1 && idx->column_indices)
    {
        for (col = 0; col < idx->num_columns; col++)
        {
            ColumnIndex *cidx = &idx->column_indices[col];

            for (ch = 0; ch < CHAR_RANGE; ch++)
            {
                /* case-sensitive */
                for (j = 0; j < cidx->pos_idx[ch].count; j++)
                    biscuit_roaring_remove(cidx->pos_idx[ch].entries[j].bitmap, rec_idx);
                for (j = 0; j < cidx->neg_idx[ch].count; j++)
                    biscuit_roaring_remove(cidx->neg_idx[ch].entries[j].bitmap, rec_idx);
                if (cidx->char_cache[ch])
                    biscuit_roaring_remove(cidx->char_cache[ch], rec_idx);

                /* case-insensitive */
                for (j = 0; j < cidx->pos_idx_lower[ch].count; j++)
                    biscuit_roaring_remove(cidx->pos_idx_lower[ch].entries[j].bitmap, rec_idx);
                for (j = 0; j < cidx->neg_idx_lower[ch].count; j++)
                    biscuit_roaring_remove(cidx->neg_idx_lower[ch].entries[j].bitmap, rec_idx);
                if (cidx->char_cache_lower[ch])
                    biscuit_roaring_remove(cidx->char_cache_lower[ch], rec_idx);
            }

            if (cidx->length_bitmaps)
                for (j = 0; j < cidx->max_length; j++)
                    if (cidx->length_bitmaps[j])
                        biscuit_roaring_remove(cidx->length_bitmaps[j], rec_idx);

            if (cidx->length_ge_bitmaps)
                for (j = 0; j < cidx->max_length; j++)
                    if (cidx->length_ge_bitmaps[j])
                        biscuit_roaring_remove(cidx->length_ge_bitmaps[j], rec_idx);

            if (cidx->length_bitmaps_lower)
                for (j = 0; j < cidx->max_length_lower; j++)
                    if (cidx->length_bitmaps_lower[j])
                        biscuit_roaring_remove(cidx->length_bitmaps_lower[j], rec_idx);

            if (cidx->length_ge_bitmaps_lower)
                for (j = 0; j < cidx->max_length_lower; j++)
                    if (cidx->length_ge_bitmaps_lower[j])
                        biscuit_roaring_remove(cidx->length_ge_bitmaps_lower[j], rec_idx);
        }
        return;
    }

    /* -------- Single-column (legacy) -------- */
    for (ch = 0; ch < CHAR_RANGE; ch++)
    {
        /* case-sensitive */
        for (j = 0; j < idx->pos_idx_legacy[ch].count; j++)
            biscuit_roaring_remove(idx->pos_idx_legacy[ch].entries[j].bitmap, rec_idx);
        for (j = 0; j < idx->neg_idx_legacy[ch].count; j++)
            biscuit_roaring_remove(idx->neg_idx_legacy[ch].entries[j].bitmap, rec_idx);
        if (idx->char_cache_legacy[ch])
            biscuit_roaring_remove(idx->char_cache_legacy[ch], rec_idx);

        /* case-insensitive */
        for (j = 0; j < idx->pos_idx_lower[ch].count; j++)
            biscuit_roaring_remove(idx->pos_idx_lower[ch].entries[j].bitmap, rec_idx);
        for (j = 0; j < idx->neg_idx_lower[ch].count; j++)
            biscuit_roaring_remove(idx->neg_idx_lower[ch].entries[j].bitmap, rec_idx);
        if (idx->char_cache_lower[ch])
            biscuit_roaring_remove(idx->char_cache_lower[ch], rec_idx);
    }

    if (idx->max_length_legacy > 0)
    {
        for (j = 0; j < idx->max_length_legacy; j++)
        {
            if (idx->length_bitmaps_legacy && idx->length_bitmaps_legacy[j])
                biscuit_roaring_remove(idx->length_bitmaps_legacy[j], rec_idx);
            if (idx->length_ge_bitmaps_legacy && idx->length_ge_bitmaps_legacy[j])
                biscuit_roaring_remove(idx->length_ge_bitmaps_legacy[j], rec_idx);
        }
    }

    if (idx->max_length_lower > 0)
    {
        for (j = 0; j < idx->max_length_lower; j++)
        {
            if (idx->length_bitmaps_lower && idx->length_bitmaps_lower[j])
                biscuit_roaring_remove(idx->length_bitmaps_lower[j], rec_idx);
            if (idx->length_ge_bitmaps_lower && idx->length_ge_bitmaps_lower[j])
                biscuit_roaring_remove(idx->length_ge_bitmaps_lower[j], rec_idx);
        }
    }
}

/* ================================================================
 * SECTION 3 – Index build
 * ================================================================
 *
 * The build functions scan the heap and populate all in-memory
 * structures.  Multi-column logic mirrors single-column but fans out
 * across ColumnIndex instances.  For brevity the CharIndex insertion
 * helpers are imported from biscuit_pattern.c via the static linkage
 * within the same translation unit; they are re-declared here as
 * forward references through biscuit_pattern.h.
 */

#include "biscuit_pattern.h"   /* for set_pos/neg_bitmap helpers etc */
#include "biscuit_preload.h"   /* for BISCUIT_PRELOAD_DONE = 3 */

/*
 * Helper: add a single text record to the single-column (legacy) index.
 * Called once per non-NULL heap tuple during build and load.
 *
 * str / byte_len  : original (UTF-8) string
 * rec_idx         : slot in the index arrays to write into
 */
static void
biscuit_index_single_record(BiscuitIndex *idx,
                             const char   *str,
                             int           byte_len,
                             int           rec_idx)
{
    int byte_pos  = 0;
    int char_pos  = 0;
    int char_count = biscuit_utf8_char_count(str, byte_len);

    /* ---- Case-sensitive character indexing ---- */
    byte_pos = char_pos = 0;
    while (byte_pos < byte_len)
    {
        unsigned char first_byte = (unsigned char) str[byte_pos];
        int           char_len   = biscuit_utf8_char_length(first_byte);
        int           b;

        if (byte_pos + char_len > byte_len)
            char_len = byte_len - byte_pos;

        for (b = 0; b < char_len; b++)
        {
            unsigned char uch = (unsigned char) str[byte_pos + b];
            RoaringBitmap *bm;
            int remaining_chars;
            int neg_offset;

            /* positive position */
            bm = biscuit_get_pos_bitmap(idx, uch, char_pos);
            if (!bm) {
                bm = biscuit_roaring_create();
                biscuit_set_pos_bitmap(idx, uch, char_pos, bm);
            }
            biscuit_roaring_add(bm, rec_idx);

            /* negative position */
            remaining_chars = biscuit_utf8_char_count(str + byte_pos, byte_len - byte_pos);
            neg_offset = -remaining_chars;
            bm = biscuit_get_neg_bitmap(idx, uch, neg_offset);
            if (!bm) {
                bm = biscuit_roaring_create();
                biscuit_set_neg_bitmap(idx, uch, neg_offset, bm);
            }
            biscuit_roaring_add(bm, rec_idx);

            /* character cache */
            if (!idx->char_cache_legacy[uch])
                idx->char_cache_legacy[uch] = biscuit_roaring_create();
            biscuit_roaring_add(idx->char_cache_legacy[uch], rec_idx);
        }

        byte_pos += char_len;
        char_pos++;
    }

    /* ---- Case-insensitive character indexing ---- */
    {
        char *str_lower      = biscuit_str_tolower(str, byte_len);
        int   lower_byte_len = strlen(str_lower);
        int   lower_char_count = biscuit_utf8_char_count(str_lower, lower_byte_len);

        idx->data_cache_lower[rec_idx] = str_lower;

        if (lower_char_count > idx->max_length_lower)
            idx->max_length_lower = lower_char_count;

        byte_pos = char_pos = 0;
        while (byte_pos < lower_byte_len)
        {
            unsigned char first_byte = (unsigned char) str_lower[byte_pos];
            int           char_len   = biscuit_utf8_char_length(first_byte);
            int           b;

            if (byte_pos + char_len > lower_byte_len)
                char_len = lower_byte_len - byte_pos;

            for (b = 0; b < char_len; b++)
            {
                unsigned char uch = (unsigned char) str_lower[byte_pos + b];
                RoaringBitmap *bm;
                int remaining_chars;
                int neg_offset;

                bm = biscuit_get_pos_bitmap_lower(idx, uch, char_pos);
                if (!bm) {
                    bm = biscuit_roaring_create();
                    biscuit_set_pos_bitmap_lower(idx, uch, char_pos, bm);
                }
                biscuit_roaring_add(bm, rec_idx);

                remaining_chars = biscuit_utf8_char_count(str_lower + byte_pos, lower_byte_len - byte_pos);
                neg_offset = -remaining_chars;
                bm = biscuit_get_neg_bitmap_lower(idx, uch, neg_offset);
                if (!bm) {
                    bm = biscuit_roaring_create();
                    biscuit_set_neg_bitmap_lower(idx, uch, neg_offset, bm);
                }
                biscuit_roaring_add(bm, rec_idx);

                if (!idx->char_cache_lower[uch])
                    idx->char_cache_lower[uch] = biscuit_roaring_create();
                biscuit_roaring_add(idx->char_cache_lower[uch], rec_idx);
            }

            byte_pos += char_len;
            char_pos++;
        }
    }

    /* Track max case-sensitive character length */
    if (char_count > idx->max_len)
        idx->max_len = char_count;
}

/*
 * biscuit_index_column_record
 * ----------------------------
 * Populate all character-level and case-insensitive bitmaps for a single
 * string value into the ColumnIndex for column `col`.
 *
 * This is the multi-column analogue of biscuit_index_single_record().
 * It uses the biscuit_set_col_*_bitmap helpers (static in biscuit_pattern.c
 * but inlined into this TU via the included header) which operate directly
 * on a ColumnIndex pointer rather than routing through the top-level
 * BiscuitIndex legacy fields.
 *
 * Parameters
 *   idx      – the owning BiscuitIndex (needed for tolower utility)
 *   col      – column number (0-based) selecting column_indices[col]
 *   str      – original UTF-8 string (NOT NUL-terminated beyond byte_len)
 *   byte_len – byte length of str
 *   rec_idx  – record slot being indexed
 */
static void
biscuit_index_column_record(BiscuitIndex *idx,
                             int           col,
                             const char   *str,
                             int           byte_len,
                             int           rec_idx)
{
    ColumnIndex   *cidx       = &idx->column_indices[col];
    int            byte_pos   = 0;
    int            char_pos   = 0;
    int            char_count = biscuit_utf8_char_count(str, byte_len);

    /* ----------------------------------------------------------------
     * Case-sensitive pass
     * ---------------------------------------------------------------- */
    byte_pos = char_pos = 0;
    while (byte_pos < byte_len)
    {
        unsigned char first_byte = (unsigned char) str[byte_pos];
        int           char_len   = biscuit_utf8_char_length(first_byte);
        int           b;

        if (byte_pos + char_len > byte_len)
            char_len = byte_len - byte_pos;

        for (b = 0; b < char_len; b++)
        {
            unsigned char  uch = (unsigned char) str[byte_pos + b];
            RoaringBitmap *bm;
            int            remaining_chars;
            int            neg_offset;

            /* positive-position bitmap */
            bm = biscuit_get_col_pos_bitmap(cidx, uch, char_pos);
            if (!bm)
            {
                bm = biscuit_roaring_create();
                biscuit_set_col_pos_bitmap(cidx, uch, char_pos, bm);
            }
            biscuit_roaring_add(bm, rec_idx);

            /* negative-position bitmap */
            remaining_chars = biscuit_utf8_char_count(str + byte_pos, byte_len - byte_pos);
            neg_offset      = -remaining_chars;
            bm = biscuit_get_col_neg_bitmap(cidx, uch, neg_offset);
            if (!bm)
            {
                bm = biscuit_roaring_create();
                biscuit_set_col_neg_bitmap(cidx, uch, neg_offset, bm);
            }
            biscuit_roaring_add(bm, rec_idx);

            /* character-presence cache */
            if (!cidx->char_cache[uch])
                cidx->char_cache[uch] = biscuit_roaring_create();
            biscuit_roaring_add(cidx->char_cache[uch], rec_idx);
        }

        byte_pos += char_len;
        char_pos++;
    }

    /* Track max character length for this column */
    if (char_count > cidx->max_length)
        cidx->max_length = char_count;

    /* ----------------------------------------------------------------
     * Case-insensitive pass
     * ---------------------------------------------------------------- */
    {
        char *str_lower      = biscuit_str_tolower(str, byte_len);
        int   lower_byte_len = (int) strlen(str_lower);
        int   lower_char_count;

        lower_char_count = biscuit_utf8_char_count(str_lower, lower_byte_len);

        if (lower_char_count > cidx->max_length_lower)
            cidx->max_length_lower = lower_char_count;

        byte_pos = char_pos = 0;
        while (byte_pos < lower_byte_len)
        {
            unsigned char first_byte = (unsigned char) str_lower[byte_pos];
            int           char_len   = biscuit_utf8_char_length(first_byte);
            int           b;

            if (byte_pos + char_len > lower_byte_len)
                char_len = lower_byte_len - byte_pos;

            for (b = 0; b < char_len; b++)
            {
                unsigned char  uch = (unsigned char) str_lower[byte_pos + b];
                RoaringBitmap *bm;
                int            remaining_chars;
                int            neg_offset;

                /* positive-position (lower) */
                bm = biscuit_get_col_pos_bitmap_lower(cidx, uch, char_pos);
                if (!bm)
                {
                    bm = biscuit_roaring_create();
                    biscuit_set_col_pos_bitmap_lower(cidx, uch, char_pos, bm);
                }
                biscuit_roaring_add(bm, rec_idx);

                /* negative-position (lower) */
                remaining_chars = biscuit_utf8_char_count(str_lower + byte_pos,
                                                           lower_byte_len - byte_pos);
                neg_offset      = -remaining_chars;
                bm = biscuit_get_col_neg_bitmap_lower(cidx, uch, neg_offset);
                if (!bm)
                {
                    bm = biscuit_roaring_create();
                    biscuit_set_col_neg_bitmap_lower(cidx, uch, neg_offset, bm);
                }
                biscuit_roaring_add(bm, rec_idx);

                /* character-presence cache (lower) */
                if (!cidx->char_cache_lower[uch])
                    cidx->char_cache_lower[uch] = biscuit_roaring_create();
                biscuit_roaring_add(cidx->char_cache_lower[uch], rec_idx);
            }

            byte_pos += char_len;
            char_pos++;
        }

        pfree(str_lower);
    }
}

/*
 * Build a brand-new index from the heap.  Returns an IndexBuildResult.
 * Handles both single-column and multi-column cases.
 */
IndexBuildResult *
biscuit_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    BiscuitIndex     *idx;
    TupleTableSlot   *slot;
    TableScanDesc     scan;
    MemoryContext     oldcontext;
    int               ch, natts, col, rec_idx;

    /*
     * All BiscuitIndex data must live in CacheMemoryContext, not in
     * rd_indexcxt.  PostgreSQL calls MemoryContextDelete(rd_indexcxt) inside
     * RelationClearRelation on any relcache invalidation (ANALYZE, DDL, cache
     * sweeps), which would free all our data while the cache entry still holds
     * the pointer.  CacheMemoryContext is never reset by PostgreSQL and is the
     * correct long-lived home for session-scoped index structures.
     */
    oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

    PG_TRY();
    {
        natts = index->rd_index->indnatts;

        idx               = (BiscuitIndex *) palloc0(sizeof(BiscuitIndex));
        idx->capacity     = 1024;
        idx->num_records  = 0;
        idx->num_columns  = natts;
        idx->max_len      = 0;
        idx->tids         = (ItemPointerData *) palloc(idx->capacity * sizeof(ItemPointerData));

        if (natts == 1)
        {
            /* ---- Single-column initialisation ---- */
            Oid      typoutput;
            bool     typIsVarlena;
            AttrNumber attnum = index->rd_index->indkey.values[0];
            Form_pg_attribute attr = TupleDescAttr(heap->rd_att, attnum - 1);

            getTypeOutputInfo(attr->atttypid, &typoutput, &typIsVarlena);

            idx->data_cache = (char **) palloc0(idx->capacity * sizeof(char *));
            idx->data_cache_lower = (char **) palloc0(idx->capacity * sizeof(char *));

            for (ch = 0; ch < CHAR_RANGE; ch++)
            {
                idx->pos_idx_legacy[ch].entries  = (PosEntry *) palloc(64 * sizeof(PosEntry));
                idx->pos_idx_legacy[ch].count    = 0;
                idx->pos_idx_legacy[ch].capacity = 64;
                idx->neg_idx_legacy[ch].entries  = (PosEntry *) palloc(64 * sizeof(PosEntry));
                idx->neg_idx_legacy[ch].count    = 0;
                idx->neg_idx_legacy[ch].capacity = 64;
                idx->char_cache_legacy[ch]       = NULL;

                idx->pos_idx_lower[ch].entries   = (PosEntry *) palloc(64 * sizeof(PosEntry));
                idx->pos_idx_lower[ch].count     = 0;
                idx->pos_idx_lower[ch].capacity  = 64;
                idx->neg_idx_lower[ch].entries   = (PosEntry *) palloc(64 * sizeof(PosEntry));
                idx->neg_idx_lower[ch].count     = 0;
                idx->neg_idx_lower[ch].capacity  = 64;
                idx->char_cache_lower[ch]        = NULL;
            }

            biscuit_init_crud_structures(idx);

            slot = table_slot_create(heap, NULL);
            #if PG_VERSION_NUM >= 190000
                scan = table_beginscan(heap, SnapshotAny, 0, NULL, 0);
            #else
                scan = table_beginscan(heap, SnapshotAny, 0, NULL);
            #endif
            while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
            {
                Datum  val;
                bool   isnull;
                text  *txt;
                char  *str;
                int    byte_len;

                slot_getallattrs(slot);
                val = slot_getattr(slot, indexInfo->ii_IndexAttrNumbers[0], &isnull);

                if (!isnull)
                {
                    txt      = DatumGetTextPP(val);
                    str      = VARDATA_ANY(txt);
                    byte_len = VARSIZE_ANY_EXHDR(txt);

                    if (idx->num_records >= idx->capacity)
                    {
                        idx->capacity *= 2;
                        idx->tids = (ItemPointerData *) repalloc(
                            idx->tids, idx->capacity * sizeof(ItemPointerData));
                        idx->data_cache = (char **) repalloc(
                            idx->data_cache, idx->capacity * sizeof(char *));
                        idx->data_cache_lower = (char **) repalloc(
                            idx->data_cache_lower, idx->capacity * sizeof(char *));
                    }

                    ItemPointerCopy(&slot->tts_tid, &idx->tids[idx->num_records]);
                    idx->data_cache[idx->num_records] = pnstrdup(str, byte_len);

                    biscuit_index_single_record(idx, str, byte_len, idx->num_records);

                    idx->num_records++;
                }
            }

            table_endscan(scan);
            ExecDropSingleTupleTableSlot(slot);

            /* Build length bitmaps */
            idx->max_length_legacy = idx->max_len + 1;
            idx->max_length_lower += 1;

            idx->length_bitmaps_legacy    = (RoaringBitmap **) palloc0(idx->max_length_legacy * sizeof(RoaringBitmap *));
            idx->length_ge_bitmaps_legacy = (RoaringBitmap **) palloc0(idx->max_length_legacy * sizeof(RoaringBitmap *));
            idx->length_bitmaps_lower     = (RoaringBitmap **) palloc0(idx->max_length_lower   * sizeof(RoaringBitmap *));
            idx->length_ge_bitmaps_lower  = (RoaringBitmap **) palloc0(idx->max_length_lower   * sizeof(RoaringBitmap *));

            for (ch = 0; ch < idx->max_length_legacy; ch++)
                idx->length_ge_bitmaps_legacy[ch] = biscuit_roaring_create();
            for (ch = 0; ch < idx->max_length_lower; ch++)
                idx->length_ge_bitmaps_lower[ch] = biscuit_roaring_create();

            for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
            {
                int bl;
                int cl;
                if (!idx->data_cache[rec_idx]) continue;

                bl = strlen(idx->data_cache[rec_idx]);
                cl = biscuit_utf8_char_count(idx->data_cache[rec_idx], bl);

                if (cl < idx->max_length_legacy)
                {
                    if (!idx->length_bitmaps_legacy[cl])
                        idx->length_bitmaps_legacy[cl] = biscuit_roaring_create();
                    biscuit_roaring_add(idx->length_bitmaps_legacy[cl], rec_idx);
                }
                for (int i = 0; i <= cl && i < idx->max_length_legacy; i++)
                    biscuit_roaring_add(idx->length_ge_bitmaps_legacy[i], rec_idx);

                if (idx->data_cache_lower[rec_idx])
                {
                    int lbl = strlen(idx->data_cache_lower[rec_idx]);
                    int lcl = biscuit_utf8_char_count(idx->data_cache_lower[rec_idx], lbl);

                    if (lcl < idx->max_length_lower)
                    {
                        if (!idx->length_bitmaps_lower[lcl])
                            idx->length_bitmaps_lower[lcl] = biscuit_roaring_create();
                        biscuit_roaring_add(idx->length_bitmaps_lower[lcl], rec_idx);
                    }
                    for (int i = 0; i <= lcl && i < idx->max_length_lower; i++)
                        biscuit_roaring_add(idx->length_ge_bitmaps_lower[i], rec_idx);
                }
            }
        }
        else
        {
            /*
             * Multi-column: allocate a ColumnIndex per column and populate
             * each with the same character-level logic.
             * (Abbreviated here — mirrors single-column per column.)
             */
            idx->column_types      = (Oid *)        palloc(natts * sizeof(Oid));
            idx->output_funcs      = (FmgrInfo *)   palloc(natts * sizeof(FmgrInfo));
            idx->column_data_cache = (char ***)     palloc(natts * sizeof(char **));
            idx->column_indices    = (ColumnIndex *) palloc0(natts * sizeof(ColumnIndex));

            for (col = 0; col < natts; col++)
            {
                AttrNumber        col_attnum = index->rd_index->indkey.values[col];
                Form_pg_attribute col_attr   = TupleDescAttr(heap->rd_att, col_attnum - 1);
                Oid               typoutput;
                bool              typIsVarlena;
                ColumnIndex       *cidx = &idx->column_indices[col];

                idx->column_types[col] = col_attr->atttypid;
                getTypeOutputInfo(col_attr->atttypid, &typoutput, &typIsVarlena);
                fmgr_info(typoutput, &idx->output_funcs[col]);
                idx->column_data_cache[col] = (char **) palloc0(idx->capacity * sizeof(char *));

                for (ch = 0; ch < CHAR_RANGE; ch++)
                {
                    cidx->pos_idx[ch].entries  = (PosEntry *) palloc(64 * sizeof(PosEntry));
                    cidx->pos_idx[ch].count    = 0; cidx->pos_idx[ch].capacity = 64;
                    cidx->neg_idx[ch].entries  = (PosEntry *) palloc(64 * sizeof(PosEntry));
                    cidx->neg_idx[ch].count    = 0; cidx->neg_idx[ch].capacity = 64;
                    cidx->char_cache[ch]       = NULL;

                    cidx->pos_idx_lower[ch].entries  = (PosEntry *) palloc(64 * sizeof(PosEntry));
                    cidx->pos_idx_lower[ch].count    = 0; cidx->pos_idx_lower[ch].capacity = 64;
                    cidx->neg_idx_lower[ch].entries  = (PosEntry *) palloc(64 * sizeof(PosEntry));
                    cidx->neg_idx_lower[ch].count    = 0; cidx->neg_idx_lower[ch].capacity = 64;
                    cidx->char_cache_lower[ch]       = NULL;
                }
            }

            biscuit_init_crud_structures(idx);

            slot = table_slot_create(heap, NULL);
            #if PG_VERSION_NUM >= 190000
                scan = table_beginscan(heap, SnapshotAny, 0, NULL, 0);
            #else
                scan = table_beginscan(heap, SnapshotAny, 0, NULL);
            #endif
            while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
            {
                
                bool all_non_null = true;
                slot_getallattrs(slot);
                for (col = 0; col < natts; col++) {
                    bool isnull;
                    slot_getattr(slot, index->rd_index->indkey.values[col], &isnull);
                    if (isnull) { all_non_null = false; break; }
                }
                if (!all_non_null) continue;

                if (idx->num_records >= idx->capacity)
                {
                    idx->capacity *= 2;
                    idx->tids = (ItemPointerData *) repalloc(idx->tids, idx->capacity * sizeof(ItemPointerData));
                    for (col = 0; col < natts; col++)
                        idx->column_data_cache[col] = (char **) repalloc(idx->column_data_cache[col], idx->capacity * sizeof(char *));
                }

                ItemPointerCopy(&slot->tts_tid, &idx->tids[idx->num_records]);

                for (col = 0; col < natts; col++)
                {
                    Datum      val;
                    bool       isnull;
                    int        out_len;
                    char      *str;

                    val = slot_getattr(slot, index->rd_index->indkey.values[col], &isnull);
                    if (isnull) { idx->column_data_cache[col][idx->num_records] = NULL; continue; }

                    str = biscuit_datum_to_text(val, idx->column_types[col], &idx->output_funcs[col], &out_len);
                    idx->column_data_cache[col][idx->num_records] = str;

                    /*
                     * Populate all character-level and case-insensitive bitmaps
                     * for this column.  Previously this was a stub comment; the
                     * missing call was the root cause of multi-column indexes
                     * returning 0 rows for every query.
                     */
                    biscuit_index_column_record(idx, col, str, out_len, idx->num_records);
                }

                idx->num_records++;
            }

            table_endscan(scan);
            ExecDropSingleTupleTableSlot(slot);

            /* Build per-column length bitmaps */
            for (col = 0; col < natts; col++)
            {
                ColumnIndex *cidx = &idx->column_indices[col];
                int max_cl = 0;

                for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
                {
                    int bl;
                    int cl;
                    if (!idx->column_data_cache[col][rec_idx]) continue;
                    bl = strlen(idx->column_data_cache[col][rec_idx]);
                    cl = biscuit_utf8_char_count(idx->column_data_cache[col][rec_idx], bl);
                    if (cl > max_cl) max_cl = cl;
                }

                cidx->max_length = max_cl + 1;
                cidx->length_bitmaps    = (RoaringBitmap **) palloc0(cidx->max_length * sizeof(RoaringBitmap *));
                cidx->length_ge_bitmaps = (RoaringBitmap **) palloc0(cidx->max_length * sizeof(RoaringBitmap *));
                for (int i = 0; i < cidx->max_length; i++)
                    cidx->length_ge_bitmaps[i] = biscuit_roaring_create();

                for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
                {
                    int bl;
                    int cl;
                    if (!idx->column_data_cache[col][rec_idx]) continue;
                    bl = strlen(idx->column_data_cache[col][rec_idx]);
                    cl = biscuit_utf8_char_count(idx->column_data_cache[col][rec_idx], bl);
                    if (cl < cidx->max_length)
                    {
                        if (!cidx->length_bitmaps[cl]) cidx->length_bitmaps[cl] = biscuit_roaring_create();
                        biscuit_roaring_add(cidx->length_bitmaps[cl], rec_idx);
                    }
                    for (int i = 0; i <= cl && i < cidx->max_length; i++)
                        biscuit_roaring_add(cidx->length_ge_bitmaps[i], rec_idx);
                }

                /* Case-insensitive length bitmaps — compute max_length_lower independently.
                 * Cannot simply copy max_length: lowercasing can change character count
                 * (e.g. German ß → ss doubles that character). */
                {
                    int max_cl_lower = 0;

                    for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
                    {
                        int bl, cl;
                        const char *s = idx->column_data_cache[col][rec_idx];
                        char       *sl;
                        if (!s) continue;
                        bl = strlen(s);
                        sl = biscuit_str_tolower(s, bl);
                        cl = biscuit_utf8_char_count(sl, strlen(sl));
                        pfree(sl);
                        if (cl > max_cl_lower) max_cl_lower = cl;
                    }

                    cidx->max_length_lower = max_cl_lower + 1;
                    cidx->length_bitmaps_lower    = (RoaringBitmap **) palloc0(cidx->max_length_lower * sizeof(RoaringBitmap *));
                    cidx->length_ge_bitmaps_lower = (RoaringBitmap **) palloc0(cidx->max_length_lower * sizeof(RoaringBitmap *));
                    for (int i = 0; i < cidx->max_length_lower; i++)
                        cidx->length_ge_bitmaps_lower[i] = biscuit_roaring_create();

                    for (rec_idx = 0; rec_idx < idx->num_records; rec_idx++)
                    {
                        int         bl, cl;
                        const char *s = idx->column_data_cache[col][rec_idx];
                        char       *sl;
                        if (!s) continue;
                        bl = strlen(s);
                        sl = biscuit_str_tolower(s, bl);
                        cl = biscuit_utf8_char_count(sl, strlen(sl));
                        pfree(sl);
                        if (cl < cidx->max_length_lower)
                        {
                            if (!cidx->length_bitmaps_lower[cl])
                                cidx->length_bitmaps_lower[cl] = biscuit_roaring_create();
                            biscuit_roaring_add(cidx->length_bitmaps_lower[cl], rec_idx);
                        }
                        for (int i = 0; i <= cl && i < cidx->max_length_lower; i++)
                            biscuit_roaring_add(cidx->length_ge_bitmaps_lower[i], rec_idx);
                    }
                }
            }
        }

        /*
         * Mark the index fully warm.  biscuit_build() constructs all
         * bitmaps synchronously, so by the time we reach here the index
         * is identical to what biscuit_complete_preload() would produce.
         * Without this, preload_state stays 0 (BISCUIT_PRELOAD_NONE)
         * because palloc0 zero-fills the struct, and rescan's check
         *
         *   bitmaps_ready = (so->index->preload_state >= BISCUIT_PRELOAD_DONE)
         *
         * is always false — every query falls through to the O(n) fallback
         * strstr scan even though the bitmaps are fully populated.
         */
        idx->preload_state = BISCUIT_PRELOAD_DONE;

        biscuit_write_metadata_to_disk(index, idx);
        biscuit_register_callback();
        biscuit_cache_insert(RelationGetRelid(index), idx);
        index->rd_amcache = idx;

        MemoryContextSwitchTo(oldcontext);

        result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
        result->heap_tuples  = idx->num_records;
        result->index_tuples = idx->num_records;

        return result;
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcontext);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void
biscuit_buildempty(Relation index)
{
    /* Nothing to write for an empty index */
    (void) index;
}

/*
 * Load (rebuild) the index from the heap on a cache miss.
 * Delegates to biscuit_build logic by re-scanning the heap.
 */
BiscuitIndex *
biscuit_load_index(Relation index)
{
    IndexInfo        *indexInfo;
    IndexBuildResult *dummy;
    Relation          heap;

    heap = table_open(index->rd_index->indrelid, AccessShareLock);

    indexInfo = BuildIndexInfo(index);

    /* Re-use build path */
    dummy = biscuit_build(heap, index, indexInfo);
    pfree(dummy);

    table_close(heap, AccessShareLock);

    return (BiscuitIndex *) index->rd_amcache;
}

bool
biscuit_insert(Relation index,
               Datum *values,
               bool *isnull,
               ItemPointer ht_ctid,
               Relation heapRelation,
               IndexUniqueCheck checkUnique,
               bool indexUnchanged,
               IndexInfo *indexInfo)
{
    BiscuitIndex  *idx;
    MemoryContext  oldcontext;
    uint32_t       slot;
    bool           found_existing  = false;
    bool           is_reusing_slot = false;
    int            col;

    (void) heapRelation;
    (void) checkUnique;
    (void) indexUnchanged;
    (void) indexInfo;

    idx = (BiscuitIndex *) index->rd_amcache;
    if (!idx)
    {
        idx = biscuit_load_index(index);
        index->rd_amcache = idx;
    }

    oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

    /* Check for duplicate TID (UPDATE path) */
    for (int i = 0; i < idx->num_records; i++)
    {
        if (ItemPointerEquals(&idx->tids[i], ht_ctid))
        {
            found_existing = true;
            slot           = i;

            /* Remove old data from all bitmaps */
            biscuit_remove_from_all_indices(idx, slot);

            if (idx->num_columns == 1)
            {
                if (idx->data_cache[slot])      { pfree(idx->data_cache[slot]);      idx->data_cache[slot]      = NULL; }
                if (idx->data_cache_lower[slot]) { pfree(idx->data_cache_lower[slot]); idx->data_cache_lower[slot] = NULL; }
            }
            else
            {
                for (col = 0; col < idx->num_columns; col++)
                    if (idx->column_data_cache[col][slot]) { pfree(idx->column_data_cache[col][slot]); idx->column_data_cache[col][slot] = NULL; }
            }

            /* Un-tombstone if needed */
            biscuit_roaring_remove(idx->tombstones, slot);
            idx->update_count++;
            break;
        }
    }

    /* Try to reuse a free slot */
    if (!found_existing && biscuit_pop_free_slot(idx, &slot))
    {
        is_reusing_slot = true;
        /* Un-tombstone the recycled slot so NOT LIKE inversion
         * doesn't exclude a live record. */
        biscuit_roaring_remove(idx->tombstones, slot);
        if (idx->tombstone_count > 0)
            idx->tombstone_count--;
    }

    if (!found_existing && !is_reusing_slot)
    {
        /* Append new slot */
        if (idx->num_records >= idx->capacity)
        {
            int old_capacity = idx->capacity;
            idx->capacity *= 2;
            idx->tids = (ItemPointerData *) repalloc(idx->tids, idx->capacity * sizeof(ItemPointerData));
            if (idx->num_columns == 1)
            {
                idx->data_cache       = (char **) repalloc(idx->data_cache,       idx->capacity * sizeof(char *));
                idx->data_cache_lower = (char **) repalloc(idx->data_cache_lower, idx->capacity * sizeof(char *));
                /*
                 * FIX A: Zero-initialise the newly allocated tail so that
                 * data_cache[slot] and data_cache_lower[slot] are reliably
                 * NULL for fresh slots.  Without this, repalloc leaves the
                 * memory uninitialised; the guard at "if (idx->data_cache_lower[slot])"
                 * below may pass on garbage and strlen/utf8_char_count then
                 * dereferences a wild pointer, producing the segfault at
                 * address 0xfffffffffffffff8 (-8).
                 */
                memset(idx->data_cache       + old_capacity, 0,
                       (idx->capacity - old_capacity) * sizeof(char *));
                memset(idx->data_cache_lower  + old_capacity, 0,
                       (idx->capacity - old_capacity) * sizeof(char *));
            }
            else
            {
                for (col = 0; col < idx->num_columns; col++)
                {
                    int old_cap = old_capacity; /* captured before the loop */
                    idx->column_data_cache[col] = (char **) repalloc(
                        idx->column_data_cache[col], idx->capacity * sizeof(char *));
                    /*
                     * FIX B: Same zero-init requirement for the multi-column
                     * cache arrays.  biscuit_bulkdelete reads column_data_cache[0][i]
                     * to detect live records; uninitialised bytes here cause it to
                     * treat garbage as a valid pointer and skip or misclassify rows,
                     * and the subsequent andnot/remove passes then touch freed memory.
                     */
                    memset(idx->column_data_cache[col] + old_cap, 0,
                           (idx->capacity - old_cap) * sizeof(char *));
                }
            }
        }
        slot = idx->num_records++;
    }

    ItemPointerCopy(ht_ctid, &idx->tids[slot]);

    /* Insert record data */
    if (idx->num_columns == 1)
    {
        if (!isnull[0])
        {
            text *txt     = DatumGetTextPP(values[0]);
            char *str     = VARDATA_ANY(txt);
            int   byte_len = VARSIZE_ANY_EXHDR(txt);

            idx->data_cache[slot] = pnstrdup(str, byte_len);

            /*
             * biscuit_index_single_record writes idx->data_cache_lower[slot]
             * as a side-effect (line ~318).  It must run BEFORE the length-
             * bitmap block below reads data_cache_lower[slot].
             */
            biscuit_index_single_record(idx, str, byte_len, slot);

            /* Grow length bitmaps if needed */
            {
            int cl = biscuit_utf8_char_count(str, byte_len);
            if (cl >= idx->max_length_legacy)
            {
                int old_ml = idx->max_length_legacy;
                int new_ml = (cl + 1) * 2;
                idx->length_bitmaps_legacy    = (RoaringBitmap **) repalloc(idx->length_bitmaps_legacy,    new_ml * sizeof(RoaringBitmap *));
                idx->length_ge_bitmaps_legacy = (RoaringBitmap **) repalloc(idx->length_ge_bitmaps_legacy, new_ml * sizeof(RoaringBitmap *));
                for (int i = old_ml; i < new_ml; i++) { idx->length_bitmaps_legacy[i] = NULL; idx->length_ge_bitmaps_legacy[i] = biscuit_roaring_create(); }
                idx->max_length_legacy = new_ml;
            }
            if (!idx->length_bitmaps_legacy[cl]) idx->length_bitmaps_legacy[cl] = biscuit_roaring_create();
            biscuit_roaring_add(idx->length_bitmaps_legacy[cl], slot);
            for (int i = 0; i <= cl && i < idx->max_length_legacy; i++)
                biscuit_roaring_add(idx->length_ge_bitmaps_legacy[i], slot);

            /*
             * Lowercase length bitmaps.
             * data_cache_lower[slot] was populated by biscuit_index_single_record
             * above; for a NULL-valued column this slot was zeroed by FIX A so
             * the guard below is now always reliable.
             */
            if (idx->data_cache_lower[slot])
            {
                int lbl = strlen(idx->data_cache_lower[slot]);
                int lcl = biscuit_utf8_char_count(idx->data_cache_lower[slot], lbl);
                if (lcl >= idx->max_length_lower)
                {
                    int old_ml = idx->max_length_lower;
                    int new_ml = (lcl + 1) * 2;
                    idx->length_bitmaps_lower    = (RoaringBitmap **) repalloc(idx->length_bitmaps_lower,    new_ml * sizeof(RoaringBitmap *));
                    idx->length_ge_bitmaps_lower = (RoaringBitmap **) repalloc(idx->length_ge_bitmaps_lower, new_ml * sizeof(RoaringBitmap *));
                    for (int i = old_ml; i < new_ml; i++) { idx->length_bitmaps_lower[i] = NULL; idx->length_ge_bitmaps_lower[i] = biscuit_roaring_create(); }
                    idx->max_length_lower = new_ml;
                }
                if (!idx->length_bitmaps_lower[lcl]) idx->length_bitmaps_lower[lcl] = biscuit_roaring_create();
                biscuit_roaring_add(idx->length_bitmaps_lower[lcl], slot);
                for (int i = 0; i <= lcl && i < idx->max_length_lower; i++)
                    biscuit_roaring_add(idx->length_ge_bitmaps_lower[i], slot);
            }
            } /* end cl block */
        }
        else
        {
            idx->data_cache[slot]       = NULL;
            idx->data_cache_lower[slot] = NULL;
        }
    }
    else
    {
        for (col = 0; col < idx->num_columns; col++)
        {
            if (!isnull[col])
            {
                int   out_len;
                char *str = biscuit_datum_to_text(values[col], idx->column_types[col], &idx->output_funcs[col], &out_len);
                idx->column_data_cache[col][slot] = str;

                /*
                 * FIX 3 (pre-existing): call was previously missing, causing
                 * every post-build insert to be absent from all bitmaps.
                 */
                biscuit_index_column_record(idx, col, str, out_len, slot);
            }
            else
                idx->column_data_cache[col][slot] = NULL;
        }
    }

    if (!found_existing && !is_reusing_slot)
        idx->insert_count++;

    MemoryContextSwitchTo(oldcontext);

    return true;
}

/* ================================================================
 * SECTION 5 – BULKDELETE
 * ================================================================ */

IndexBulkDeleteResult *
biscuit_bulkdelete(IndexVacuumInfo *info,
                   IndexBulkDeleteResult *stats,
                   IndexBulkDeleteCallback callback,
                   void *callback_state)
{
    Relation       index = info->index;
    BiscuitIndex  *idx;
    int            i, j, ch, col;
    MemoryContext  oldcontext;
    RoaringBitmap *records_to_delete;
    uint64_t       delete_count;
    uint32_t      *delete_indices;

    idx = (BiscuitIndex *) index->rd_amcache;
    if (!idx) { idx = biscuit_load_index(index); index->rd_amcache = idx; }

    if (!stats)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

    oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

    records_to_delete = biscuit_roaring_create();

    for (i = 0; i < idx->num_records; i++)
    {
        bool has_data;
        bool already_tombstoned;

        if (idx->num_columns == 1)
            has_data = (idx->data_cache[i] != NULL);
        else
            has_data = (idx->column_data_cache && idx->column_data_cache[0] && idx->column_data_cache[0][i] != NULL);

        if (!has_data) continue;

#ifdef HAVE_ROARING
        already_tombstoned = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
#else
        { uint32_t bl = i >> 6, bt = i & 63; already_tombstoned = (bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
#endif
        if (already_tombstoned) continue;

        if (callback(&idx->tids[i], callback_state))
        {
            biscuit_roaring_add(idx->tombstones, (uint32_t) i);
            biscuit_roaring_add(records_to_delete, (uint32_t) i);
            idx->tombstone_count++;
            biscuit_push_free_slot(idx, (uint32_t) i);
            stats->tuples_removed++;
            idx->delete_count++;
        }
    }

    delete_count = biscuit_roaring_count(records_to_delete);

    if (delete_count > 0)
    {
        delete_indices = biscuit_roaring_to_array(records_to_delete, &delete_count);

        if (delete_indices)
        {
            if (idx->num_columns == 1)
            {
                for (ch = 0; ch < CHAR_RANGE; ch++)
                {
                    for (j = 0; j < idx->pos_idx_legacy[ch].count; j++)
                        biscuit_roaring_andnot_inplace(idx->pos_idx_legacy[ch].entries[j].bitmap, records_to_delete);
                    for (j = 0; j < idx->neg_idx_legacy[ch].count; j++)
                        biscuit_roaring_andnot_inplace(idx->neg_idx_legacy[ch].entries[j].bitmap, records_to_delete);
                    if (idx->char_cache_legacy[ch])
                        biscuit_roaring_andnot_inplace(idx->char_cache_legacy[ch], records_to_delete);

                    for (j = 0; j < idx->pos_idx_lower[ch].count; j++)
                        biscuit_roaring_andnot_inplace(idx->pos_idx_lower[ch].entries[j].bitmap, records_to_delete);
                    for (j = 0; j < idx->neg_idx_lower[ch].count; j++)
                        biscuit_roaring_andnot_inplace(idx->neg_idx_lower[ch].entries[j].bitmap, records_to_delete);
                    if (idx->char_cache_lower[ch])
                        biscuit_roaring_andnot_inplace(idx->char_cache_lower[ch], records_to_delete);
                }

                for (j = 0; j < idx->max_length_legacy; j++)
                {
                    if (idx->length_bitmaps_legacy && idx->length_bitmaps_legacy[j])
                        biscuit_roaring_andnot_inplace(idx->length_bitmaps_legacy[j], records_to_delete);
                    if (idx->length_ge_bitmaps_legacy && idx->length_ge_bitmaps_legacy[j])
                        biscuit_roaring_andnot_inplace(idx->length_ge_bitmaps_legacy[j], records_to_delete);
                }
                for (j = 0; j < idx->max_length_lower; j++)
                {
                    if (idx->length_bitmaps_lower && idx->length_bitmaps_lower[j])
                        biscuit_roaring_andnot_inplace(idx->length_bitmaps_lower[j], records_to_delete);
                    if (idx->length_ge_bitmaps_lower && idx->length_ge_bitmaps_lower[j])
                        biscuit_roaring_andnot_inplace(idx->length_ge_bitmaps_lower[j], records_to_delete);
                }

                for (j = 0; j < (int) delete_count; j++)
                {
                    if (idx->data_cache[delete_indices[j]])
                        { pfree(idx->data_cache[delete_indices[j]]); idx->data_cache[delete_indices[j]] = NULL; }
                    if (idx->data_cache_lower && idx->data_cache_lower[delete_indices[j]])
                        { pfree(idx->data_cache_lower[delete_indices[j]]); idx->data_cache_lower[delete_indices[j]] = NULL; }
                }
            }
            else
            {
                for (col = 0; col < idx->num_columns; col++)
                {
                    ColumnIndex *cidx = &idx->column_indices[col];

                    for (ch = 0; ch < CHAR_RANGE; ch++)
                    {
                        for (j = 0; j < cidx->pos_idx[ch].count; j++)
                            biscuit_roaring_andnot_inplace(cidx->pos_idx[ch].entries[j].bitmap, records_to_delete);
                        for (j = 0; j < cidx->neg_idx[ch].count; j++)
                            biscuit_roaring_andnot_inplace(cidx->neg_idx[ch].entries[j].bitmap, records_to_delete);
                        if (cidx->char_cache[ch])
                            biscuit_roaring_andnot_inplace(cidx->char_cache[ch], records_to_delete);

                        for (j = 0; j < cidx->pos_idx_lower[ch].count; j++)
                            biscuit_roaring_andnot_inplace(cidx->pos_idx_lower[ch].entries[j].bitmap, records_to_delete);
                        for (j = 0; j < cidx->neg_idx_lower[ch].count; j++)
                            biscuit_roaring_andnot_inplace(cidx->neg_idx_lower[ch].entries[j].bitmap, records_to_delete);
                        if (cidx->char_cache_lower[ch])
                            biscuit_roaring_andnot_inplace(cidx->char_cache_lower[ch], records_to_delete);
                    }

                    /*
                     * FIX 4: Was j <= cidx->max_length / j <= cidx->max_length_lower,
                     * reading one slot past the end of the palloc'd arrays (valid
                     * indices 0..max_length-1).  This caused the GPF in biscuit.so
                     * at 15:05 on the first VACUUM after the earlier fixes, because
                     * autovacuum hit this path before the extension was reloaded.
                     */
                    for (j = 0; j < cidx->max_length; j++)
                    {
                        if (cidx->length_bitmaps && cidx->length_bitmaps[j])
                            biscuit_roaring_andnot_inplace(cidx->length_bitmaps[j], records_to_delete);
                        if (cidx->length_ge_bitmaps && cidx->length_ge_bitmaps[j])
                            biscuit_roaring_andnot_inplace(cidx->length_ge_bitmaps[j], records_to_delete);
                    }
                    for (j = 0; j < cidx->max_length_lower; j++)
                    {
                        if (cidx->length_bitmaps_lower && cidx->length_bitmaps_lower[j])
                            biscuit_roaring_andnot_inplace(cidx->length_bitmaps_lower[j], records_to_delete);
                        if (cidx->length_ge_bitmaps_lower && cidx->length_ge_bitmaps_lower[j])
                            biscuit_roaring_andnot_inplace(cidx->length_ge_bitmaps_lower[j], records_to_delete);
                    }
                }

                for (j = 0; j < (int) delete_count; j++)
                    for (col = 0; col < idx->num_columns; col++)
                        if (idx->column_data_cache[col][delete_indices[j]])
                            { pfree(idx->column_data_cache[col][delete_indices[j]]); idx->column_data_cache[col][delete_indices[j]] = NULL; }
            }

            pfree(delete_indices);
        }
    }

    biscuit_roaring_free(records_to_delete);

    /* BEFORE clearing tombstones, purge stale bitmap data for
    * tombstoned slots that were never reused (still on free_list). */
    if (idx->tombstone_count >= TOMBSTONE_CLEANUP_THRESHOLD)
    {
        uint64_t  ts_count;
        uint32_t *ts_slots = biscuit_roaring_to_array(idx->tombstones, &ts_count);
        if (ts_slots)
        {
            uint64_t k;
            for (k = 0; k < ts_count; k++)
                biscuit_remove_from_all_indices(idx, ts_slots[k]);
            pfree(ts_slots);
        }
        biscuit_roaring_free(idx->tombstones);
        idx->tombstones      = biscuit_roaring_create();
        idx->tombstone_count = 0;
    }

    MemoryContextSwitchTo(oldcontext);

    stats->num_pages   = 1;
    stats->pages_deleted = 0;
    stats->pages_free  = 0;

    return stats;
}

/* ================================================================
 * SECTION 6 – Remaining AM callbacks
 * ================================================================ */

IndexBulkDeleteResult *
biscuit_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    (void) info;
    return stats;
}

bool
biscuit_canreturn(Relation index, int attno)
{
    (void) index;
    (void) attno;
    return false;
}

void
biscuit_costestimate(PlannerInfo *root, IndexPath *path,
                    double loop_count,
                    Cost *indexStartupCost, Cost *indexTotalCost,
                    Selectivity *indexSelectivity,
                    double *indexCorrelation, double *indexPages)
{
    Relation    index    = (path->indexinfo->indexoid != InvalidOid)
                           ? index_open(path->indexinfo->indexoid, AccessShareLock)
                           : NULL;
    BlockNumber numPages = 1;

    (void) root;
    (void) loop_count;

    if (index)
    {
        numPages = RelationGetNumberOfBlocks(index);
        if (numPages == 0) numPages = 1;
        index_close(index, AccessShareLock);
    }

    if (path->indexclauses == NIL)
    {
        /*
         * No usable quals (e.g. plain SELECT * with no WHERE). Make this
         * path unattractive so the planner falls back to a seqscan instead
         * of using Biscuit with zero scan keys, which would return 0 rows.
         */
        *indexStartupCost = 1.0e10;
        *indexTotalCost   = 1.0e10;
        *indexSelectivity = 1.0;
        *indexCorrelation = 0.0;
        if (indexPages) *indexPages = numPages;
        return;
    }

    *indexStartupCost  = 0.0;
    *indexTotalCost    = 0.01 + (numPages * random_page_cost);
    *indexSelectivity  = 0.01;
    *indexCorrelation  = 1.0;
    if (indexPages) *indexPages = numPages;
}
bytea *
biscuit_options(Datum reloptions, bool validate)
{
    (void) reloptions;
    (void) validate;
    return NULL;
}

bool
biscuit_validate(Oid opclassoid)
{
    (void) opclassoid;
    return true;
}

void
biscuit_adjustmembers(Oid opfamilyoid, Oid opclassoid,
                      List *operators, List *functions)
{
    /* Nothing to adjust */
    (void) opfamilyoid;
    (void) opclassoid;
    (void) operators;
    (void) functions;
}
