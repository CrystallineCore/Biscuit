/*
 * biscuit_persist.c
 * BiscuitIndex save/load/drop against the WAL-logged directory +
 * compacted-blob + pending-list page format (see the design doc,
 * "Biscuit WAL-Logged Storage: Pending-List Design", and biscuit_dir.c /
 * biscuit_blob.c for the primitives this file is built on).
 *
 * This is a full replacement of the old external-file snapshot mechanism
 * (a flat file per index under $PGDATA/pg_biscuit/, written with a
 * temp-file-then-rename swap and a trailing CRC32C checksum) -- that
 * mechanism, and every helper specific to it (biscuit_persist_path/
 * _tmp_path/_ensure_dir, the FILE*-based WStream/RStream read/write
 * helpers), is deleted outright, not wrapped or version-gated. There is
 * no dual-format reader: BISCUIT_VERSION was already bumped for the
 * pending-list cutover (see biscuit_common.h), so any pre-existing index
 * must be REINDEXed under this extension version -- that is an accepted,
 * expected requirement, not something this file tries to work around.
 *
 * What "save"/"load"/"drop" mean now
 * -----------------------------------
 * Every bitmap-shaped structure this file used to dump into one flat file
 * (a `pos_idx[ch]` entry's bitmap, a `char_cache[ch]`, a
 * `length_bitmaps[i]`, etc.) now gets its own BiscuitDirEntry, at exactly
 * the granularity biscuit_common.h's design already addresses at:
 * `(col, is_lower, kind, ch, position)`. biscuit_persist_save() walks
 * every one of those structures and biscuit_dir_upsert()s its current
 * in-memory content as a compacted blob (biscuit_page_write_blob()),
 * freeing whatever blob chain previously occupied that directory entry.
 * biscuit_persist_load() does the reverse: one pass over every directory
 * entry a column's chain holds, decoding each one's blob (and merging in
 * any pending deltas -- see biscuit_persist_merge_pending() below; dead
 * code for now since nothing appends to a pending chain yet, but correct
 * once CRUD wiring lands) back into the right BiscuitIndex field.
 *
 * The rest of BiscuitIndex's persistent state that isn't a
 * RoaringBitmap-per-(ch,position) structure at all -- the tid array, the
 * tombstone bitmap, the free-slot list, the per-record string caches, and
 * assorted scalar bookkeeping (capacity, max_len, insert/update/delete
 * counts, ...) -- reuses the exact same directory+blob machinery under a
 * few new BISCUIT_DIR_KIND_* values added for this file specifically
 * (TIDS/TOMBSTONES/FREELIST/STRCACHE/HEADER -- see biscuit_common.h).
 * biscuit_blob.c's chunk chain has no bitmap-specific logic at all ("just
 * bytes in, bytes out", design doc §1), so it works identically for a
 * flat ItemPointerData[] dump or a length-prefixed string array as it
 * does for a serialized RoaringBitmap.
 *
 * There is no more "is the snapshot stale relative to the metapage's
 * generation" check, and no separate concept of "snapshot" vs "live
 * state" at all: the directory + blob pages this file reads and writes
 * *are* the durable state, always current as of whichever
 * biscuit_persist_save() call last touched them, not a point-in-time
 * dump that can drift out of sync with something else. This is exactly
 * the shift the design doc's Round 5/6 discussion describes (deleting
 * the old cache/snapshot-staleness machinery because there is no
 * "expensive reconstruction to amortize" left once the durable pages
 * themselves are what's being read).
 *
 * A behavioral note worth flagging explicitly: the old file-based
 * biscuit_persist_save() deliberately took a bare Oid (not a Relation)
 * specifically so biscuit_cache.c's proc-exit callback could call it very
 * late in backend shutdown, since building a file path needs no catalog
 * access. That assumption no longer holds -- this version needs a real
 * Relation (relation_open()) to reach the buffer manager, and
 * relation_open() is not safe to call from a true proc-exit context. This
 * is not fixed in this file: biscuit_cache.c's whole proc-exit-flush
 * design is already slated for deletion (not adaptation) in a later
 * phase per the design doc's Round 5 finding, so patching this file to
 * accommodate a caller that's about to be deleted would be wasted work.
 * Flagging it here so whoever does that deletion isn't surprised by why
 * the old proc-exit flush call site can no longer work.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_blob.h"
#include "biscuit_dir.h"
#include "biscuit_persist.h"
#include "biscuit_index.h"   /* for biscuit_get_column_case_mode() /
                               * biscuit_read_metadata_from_disk() */
#include "access/relation.h"
#include "portability/instr_time.h"   /* DIAGNOSTIC ONLY -- instr_time */

/* ================================================================
 * Small in-memory growable-buffer (write) / cursor (read) helpers.
 *
 * These are the direct replacement for the old WStream/RStream: same
 * length-prefixed-field shape, but built against a palloc'd in-memory
 * buffer instead of a FILE* -- the buffer is what gets handed to
 * biscuit_page_write_blob()/comes back from biscuit_page_read_blob(),
 * which do the actual paging/WAL-logging. No checksum here: that was
 * specifically for detecting a torn/corrupt *file write*, a failure mode
 * that doesn't exist for these blobs (they're GenericXLog-protected pages
 * -- biscuit_page_read_blob() already cross-checks total_len/total_chunks/
 * chunk_seq on every chunk and ERRORs on a structural mismatch).
 * ================================================================ */

typedef struct
{
    char   *data;
    Size    len;
    Size    cap;
} PBuf;

static void
pbuf_init(PBuf *b)
{
    b->cap  = 256;
    b->len  = 0;
    b->data = (char *) palloc(b->cap);
}

static void
pbuf_put(PBuf *b, const void *p, Size n)
{
    if (n == 0)
        return;
    if (b->len + n > b->cap)
    {
        while (b->cap < b->len + n)
            b->cap *= 2;
        b->data = (char *) repalloc(b->data, b->cap);
    }
    memcpy(b->data + b->len, p, n);
    b->len += n;
}

static void pbuf_put_i32(PBuf *b, int32 v)  { pbuf_put(b, &v, sizeof(v)); }
static void pbuf_put_i64(PBuf *b, int64 v)  { pbuf_put(b, &v, sizeof(v)); }
static void pbuf_put_u32(PBuf *b, uint32 v) { pbuf_put(b, &v, sizeof(v)); }

static void
pbuf_put_str(PBuf *b, const char *s)
{
    if (!s)
    {
        pbuf_put_i32(b, -1);
        return;
    }
    {
        int32 len = (int32) strlen(s);
        pbuf_put_i32(b, len);
        pbuf_put(b, s, len);
    }
}

typedef struct
{
    const char *data;
    Size        len;
    Size        pos;
    bool        error;
} PCur;

static void
pcur_init(PCur *c, const char *data, Size len)
{
    c->data  = data;
    c->len   = len;
    c->pos   = 0;
    c->error = false;
}

static bool
pcur_get(PCur *c, void *out, Size n)
{
    if (c->error || c->pos + n > c->len)
    {
        c->error = true;
        return false;
    }
    memcpy(out, c->data + c->pos, n);
    c->pos += n;
    return true;
}

static int32
pcur_get_i32(PCur *c) { int32 v = 0; pcur_get(c, &v, sizeof(v)); return v; }
static int64
pcur_get_i64(PCur *c) { int64 v = 0; pcur_get(c, &v, sizeof(v)); return v; }
static uint32
pcur_get_u32(PCur *c) { uint32 v = 0; pcur_get(c, &v, sizeof(v)); return v; }

static char *
pcur_get_str(PCur *c, MemoryContext cxt)
{
    int32         len = pcur_get_i32(c);
    char         *s;
    MemoryContext old;

    if (c->error || len < 0)
        return NULL;

    old = MemoryContextSwitchTo(cxt);
    s   = (char *) palloc(len + 1);
    MemoryContextSwitchTo(old);

    if (!pcur_get(c, s, len))
    {
        pfree(s);
        return NULL;
    }
    s[len] = '\0';
    return s;
}

/* ================================================================
 * Generic per-structure directory+blob write/read.
 *
 * These are the only two functions that actually call biscuit_dir_*()/
 * biscuit_page_*_blob() -- everything else in this file (the CharIndex/
 * length-array/string-cache walkers below) is just enumerating which
 * (col, is_lower, kind, ch, position) identities exist and calling
 * through these two.
 * ================================================================ */

/*
 * Write (or overwrite) one structure's raw bytes. data == NULL / len == 0
 * means "this structure is absent" -- mirrors biscuit_page_write_blob()'s
 * own len==0 contract, and an absent structure with a pre-existing
 * directory entry has that entry's blob_head reset to InvalidBlockNumber
 * (not deleted -- directory entries are never removed, per §5) so a
 * later re-population finds the same entry via biscuit_dir_find().
 *
 * When neither an existing entry nor new data exist, no directory entry
 * is created at all -- an absent structure that was never written stays
 * simply unreferenced, exactly like today's in-memory NULL RoaringBitmap*
 * fields.
 */
static void
biscuit_persist_write_raw(Relation index,
                           int32 col, bool is_lower, uint8 kind,
                           int32 ch, int32 position,
                           const char *data, uint32 len)
{
    BiscuitDirEntry    existing;
    BiscuitDirEntryRef ref;
    BiscuitDirEntry    newentry;
    BlockNumber        new_head = InvalidBlockNumber;

    if (data && len > 0)
        biscuit_page_write_blob(index, data, len, &new_head, NULL);

    if (biscuit_dir_find(index, col, is_lower, kind, ch, position, &existing, &ref))
    {
        BlockNumber old_head = existing.blob_head;

        newentry = existing;
        newentry.blob_head = new_head;
        biscuit_dir_update(index, &ref, &newentry);

        if (old_head != InvalidBlockNumber && old_head != new_head)
            biscuit_page_free_blob(index, old_head);
        return;
    }

    if (new_head == InvalidBlockNumber)
        return;   /* nothing to persist, nothing existed before */

    memset(&newentry, 0, sizeof(newentry));
    newentry.col           = (int16) col;
    newentry.is_lower      = is_lower;
    newentry.kind          = kind;
    newentry.ch            = ch;
    newentry.position       = position;
    newentry.blob_head      = new_head;
    newentry.pending_head   = InvalidBlockNumber;
    newentry.pending_tail   = InvalidBlockNumber;
    newentry.pending_count  = 0;
    newentry.pending_bytes  = 0;

    biscuit_dir_insert(index, &newentry, NULL);
}

static void
biscuit_persist_write_bitmap(Relation index,
                              int32 col, bool is_lower, uint8 kind,
                              int32 ch, int32 position,
                              const RoaringBitmap *bm)
{
    char   *buf = NULL;
    uint32  len = 0;

    if (bm)
        buf = biscuit_roaring_serialize(bm, &len);

    biscuit_persist_write_raw(index, col, is_lower, kind, ch, position, buf, len);

    if (buf)
        pfree(buf);
}

/*
 * biscuit_persist_merge_pending
 *
 * Read-only walk of a pending chain, applying every record to `target`
 * via the existing biscuit_roaring_add()/_remove() -- the design doc §4
 * merge-at-scan strategy, applied here to give biscuit_persist_load() the
 * same correctness guarantee a live scan would have. Deliberately NOT
 * biscuit_pending_drain(): that function is destructive (frees the
 * pending chain and optionally rewrites the blob), which is exactly wrong
 * for a read -- per §4, "a scan never triggers a drain". This duplicates
 * the small read-loop portion of biscuit_pending_drain()'s walk rather
 * than exposing a new non-destructive primitive from biscuit_blob.h,
 * since that file's exported contract wasn't part of this phase's scope.
 *
 * Dead code in practice as of this phase: nothing yet appends to a
 * pending chain (that's future CRUD call-site wiring), so every
 * directory entry's pending_head is InvalidBlockNumber and this is never
 * actually invoked with real records. Implemented now anyway so
 * biscuit_persist_load() is correct the moment that wiring lands, rather
 * than silently ignoring pending deltas until someone remembers to add
 * this.
 */
static void
biscuit_persist_merge_pending(Relation index, BlockNumber pending_head, RoaringBitmap *target)
{
    BlockNumber cur = pending_head;

    while (cur != InvalidBlockNumber)
    {
        Buffer                     buf = ReadBuffer(index, cur);
        Page                       page;
        BiscuitPendingPageHeader  *hdr;
        BiscuitPendingRecord      *rec;
        BiscuitPageOpaque          opaque;
        BlockNumber                next;
        uint32                     i;

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        hdr  = (BiscuitPendingPageHeader *) ((char *) page + SizeOfPageHeaderData);
        rec  = (BiscuitPendingRecord *) ((char *) hdr + MAXALIGN(sizeof(BiscuitPendingPageHeader)));

        for (i = 0; i < hdr->num_records; i++)
        {
            if (rec[i].op == BISCUIT_PENDING_OP_ADD)
                biscuit_roaring_add(target, rec[i].value);
            else if (rec[i].op == BISCUIT_PENDING_OP_REMOVE)
                biscuit_roaring_remove(target, rec[i].value);
        }

        opaque = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        next   = opaque->next;
        UnlockReleaseBuffer(buf);
        cur = next;
    }
}

/*
 * DIAGNOSTIC ONLY -- not part of the fix, just instrumentation to confirm
 * where biscuit_persist_load()'s cold-load time actually goes. Safe to
 * remove once confirmed; DEBUG1-gated so it's silent by default.
 */
static long biscuit_diag_decode_count = 0;

/*
 * Decode one directory entry's compacted blob, merging in its pending
 * chain if it has one. Returns NULL for a genuinely absent structure
 * (blob_head == InvalidBlockNumber and no pending records either) --
 * callers store that straight into a RoaringBitmap* field, matching
 * today's "NULL means absent" convention throughout biscuit_bitmap.c/
 * biscuit_pattern.c.
 */
static RoaringBitmap *
biscuit_persist_decode_entry(Relation index, const BiscuitDirEntry *entry)
{
    RoaringBitmap *bm = NULL;

    biscuit_diag_decode_count++;   /* DIAGNOSTIC ONLY */

    if (entry->blob_head != InvalidBlockNumber)
    {
        char   *data;
        uint32  len;

        biscuit_page_read_blob(index, entry->blob_head, &data, &len);
        bm = biscuit_roaring_deserialize(data, len);
        if (data)
            pfree(data);
    }

    if (entry->pending_head != InvalidBlockNumber)
    {
        if (!bm)
            bm = biscuit_roaring_create();
        biscuit_persist_merge_pending(index, entry->pending_head, bm);
    }

    return bm;
}

/* ================================================================
 * CharIndex (pos_idx[ch] / neg_idx[ch]) save/load
 * ================================================================ */

static void
biscuit_persist_save_charindex(Relation index, int32 col, bool is_lower,
                                uint8 kind, int32 ch, const CharIndex *ci)
{
    int i;

    for (i = 0; i < ci->count; i++)
        biscuit_persist_write_bitmap(index, col, is_lower, kind, ch,
                                      ci->entries[i].pos, ci->entries[i].bitmap);
}

static void
biscuit_persist_save_length_arrays(Relation index, int32 col, bool is_lower,
                                    RoaringBitmap **len_arr, RoaringBitmap **len_ge_arr,
                                    int max_length)
{
    int i;

    for (i = 0; i < max_length; i++)
    {
        if (len_arr)
            biscuit_persist_write_bitmap(index, col, is_lower, BISCUIT_DIR_KIND_LEN,
                                          -1, i, len_arr[i]);
        if (len_ge_arr)
            biscuit_persist_write_bitmap(index, col, is_lower, BISCUIT_DIR_KIND_LEN_GE,
                                          -1, i, len_ge_arr[i]);
    }
}

static void
biscuit_persist_save_strcache(Relation index, int32 col,
                               char **cache, char **cache_lower, int num_records)
{
    PBuf b1, b2;
    int  i;

    pbuf_init(&b1);
    pbuf_init(&b2);

    for (i = 0; i < num_records; i++)
    {
        pbuf_put_str(&b1, cache ? cache[i] : NULL);
        pbuf_put_str(&b2, cache_lower ? cache_lower[i] : NULL);
    }

    biscuit_persist_write_raw(index, col, false, BISCUIT_DIR_KIND_STRCACHE, -1, -1,
                               b1.len ? b1.data : NULL, (uint32) b1.len);
    biscuit_persist_write_raw(index, col, true, BISCUIT_DIR_KIND_STRCACHE, -1, -1,
                               b2.len ? b2.data : NULL, (uint32) b2.len);

    pfree(b1.data);
    pfree(b2.data);
}

/* ================================================================
 * SAVE
 * ================================================================ */

void
biscuit_persist_save(Oid indexoid, BiscuitIndex *idx)
{
    Relation index;

    index = relation_open(indexoid, RowExclusiveLock);

    PG_TRY();
    {
        PBuf header;
        int  ch, i;

        /* ---- scalar bookkeeping (see BISCUIT_DIR_KIND_HEADER comment) ---- */
        pbuf_init(&header);
        pbuf_put_i32(&header, idx->num_records);
        pbuf_put_i32(&header, idx->capacity);
        pbuf_put_i32(&header, idx->max_len);
        pbuf_put_i32(&header, idx->max_length_legacy);
        pbuf_put_i32(&header, idx->max_length_lower);
        pbuf_put_i64(&header, idx->insert_count);
        pbuf_put_i64(&header, idx->update_count);
        pbuf_put_i64(&header, idx->delete_count);
        pbuf_put_i32(&header, idx->tombstone_count);
        pbuf_put_i32(&header, idx->num_columns);

        if (idx->num_columns > 1)
        {
            for (i = 0; i < idx->num_columns; i++)
            {
                pbuf_put_u32(&header, idx->column_types[i]);
                pbuf_put_i32(&header, idx->column_indices[i].max_length);
                pbuf_put_i32(&header, idx->column_indices[i].max_length_lower);
            }
        }

        biscuit_persist_write_raw(index, BISCUIT_DIR_COL_SINGLETON, false,
                                   BISCUIT_DIR_KIND_HEADER, -1, -1,
                                   header.data, (uint32) header.len);
        pfree(header.data);

        /* ---- tids ---- */
        biscuit_persist_write_raw(index, BISCUIT_DIR_COL_SINGLETON, false,
                                   BISCUIT_DIR_KIND_TIDS, -1, -1,
                                   idx->num_records > 0 ? (const char *) idx->tids : NULL,
                                   (uint32) (idx->num_records * sizeof(ItemPointerData)));

        /* ---- tombstones ---- */
        biscuit_persist_write_bitmap(index, BISCUIT_DIR_COL_SINGLETON, false,
                                      BISCUIT_DIR_KIND_TOMBSTONES, -1, -1,
                                      idx->tombstones);

        /* ---- free list ---- */
        biscuit_persist_write_raw(index, BISCUIT_DIR_COL_SINGLETON, false,
                                   BISCUIT_DIR_KIND_FREELIST, -1, -1,
                                   idx->free_count > 0 ? (const char *) idx->free_list : NULL,
                                   (uint32) (idx->free_count * sizeof(uint32_t)));

        if (idx->num_columns == 1)
        {
            biscuit_persist_save_strcache(index, BISCUIT_DIR_COL_LEGACY,
                                           idx->data_cache, idx->data_cache_lower,
                                           idx->num_records);

            for (ch = 0; ch < CHAR_RANGE; ch++)
            {
                biscuit_persist_save_charindex(index, BISCUIT_DIR_COL_LEGACY, false,
                                                BISCUIT_DIR_KIND_POS, ch, &idx->pos_idx_legacy[ch]);
                biscuit_persist_save_charindex(index, BISCUIT_DIR_COL_LEGACY, false,
                                                BISCUIT_DIR_KIND_NEG, ch, &idx->neg_idx_legacy[ch]);
                biscuit_persist_write_bitmap(index, BISCUIT_DIR_COL_LEGACY, false,
                                              BISCUIT_DIR_KIND_CACHE, ch, -1,
                                              idx->char_cache_legacy[ch]);

                biscuit_persist_save_charindex(index, BISCUIT_DIR_COL_LEGACY, true,
                                                BISCUIT_DIR_KIND_POS, ch, &idx->pos_idx_lower[ch]);
                biscuit_persist_save_charindex(index, BISCUIT_DIR_COL_LEGACY, true,
                                                BISCUIT_DIR_KIND_NEG, ch, &idx->neg_idx_lower[ch]);
                biscuit_persist_write_bitmap(index, BISCUIT_DIR_COL_LEGACY, true,
                                              BISCUIT_DIR_KIND_CACHE, ch, -1,
                                              idx->char_cache_lower[ch]);
            }

            biscuit_persist_save_length_arrays(index, BISCUIT_DIR_COL_LEGACY, false,
                                                idx->length_bitmaps_legacy,
                                                idx->length_ge_bitmaps_legacy,
                                                idx->max_length_legacy);
            biscuit_persist_save_length_arrays(index, BISCUIT_DIR_COL_LEGACY, true,
                                                idx->length_bitmaps_lower,
                                                idx->length_ge_bitmaps_lower,
                                                idx->max_length_lower);
        }
        else
        {
            int col;

            for (col = 0; col < idx->num_columns; col++)
                biscuit_persist_save_strcache(index, col,
                                               idx->column_data_cache[col],
                                               idx->column_data_cache_lower[col],
                                               idx->num_records);

            for (col = 0; col < idx->num_columns; col++)
            {
                ColumnIndex *cidx = &idx->column_indices[col];

                for (ch = 0; ch < CHAR_RANGE; ch++)
                {
                    biscuit_persist_save_charindex(index, col, false,
                                                    BISCUIT_DIR_KIND_POS, ch, &cidx->pos_idx[ch]);
                    biscuit_persist_save_charindex(index, col, false,
                                                    BISCUIT_DIR_KIND_NEG, ch, &cidx->neg_idx[ch]);
                    biscuit_persist_write_bitmap(index, col, false,
                                                  BISCUIT_DIR_KIND_CACHE, ch, -1,
                                                  cidx->char_cache[ch]);

                    biscuit_persist_save_charindex(index, col, true,
                                                    BISCUIT_DIR_KIND_POS, ch, &cidx->pos_idx_lower[ch]);
                    biscuit_persist_save_charindex(index, col, true,
                                                    BISCUIT_DIR_KIND_NEG, ch, &cidx->neg_idx_lower[ch]);
                    biscuit_persist_write_bitmap(index, col, true,
                                                  BISCUIT_DIR_KIND_CACHE, ch, -1,
                                                  cidx->char_cache_lower[ch]);
                }

                biscuit_persist_save_length_arrays(index, col, false,
                                                    cidx->length_bitmaps, cidx->length_ge_bitmaps,
                                                    cidx->max_length);
                biscuit_persist_save_length_arrays(index, col, true,
                                                    cidx->length_bitmaps_lower, cidx->length_ge_bitmaps_lower,
                                                    cidx->max_length_lower);
            }
        }
    }
    PG_CATCH();
    {
        /*
         * No longer best-effort: biscuit_load_index() has no from-heap
         * rebuild fallback (see its comment in biscuit_index.c), so a
         * silently-swallowed save failure here would leave a cold load
         * of this index with nothing readable. Close the relation to
         * avoid leaking the lock, then let the error propagate -- for
         * the build-time call site (biscuit_build()) that means the
         * CREATE INDEX itself fails, which is the correct outcome.
         */
        relation_close(index, RowExclusiveLock);
        PG_RE_THROW();
    }
    PG_END_TRY();

    relation_close(index, RowExclusiveLock);

    idx->gen_at_last_snapshot = idx->gen;

    elog(DEBUG1, "biscuit: saved directory-backed structures for index %u (%d records, gen " UINT64_FORMAT ")",
         indexoid, idx->num_records, idx->gen);
}

/* ================================================================
 * LOAD
 * ================================================================ */

/* Growable (pos, bitmap) list -- mirrors the old on-disk CharIndex shape,
 * built up during the directory walk below instead of read as one
 * contiguous count-prefixed run (the directory doesn't store an explicit
 * per-character count -- see file header). */
typedef struct
{
    PosEntry *entries;
    int       count;
    int       capacity;
} LoadBucket;

static void
load_bucket_add(LoadBucket *b, int32 pos, RoaringBitmap *bm)
{
    if (b->count >= b->capacity)
    {
        int newcap = b->capacity ? b->capacity * 2 : 8;

        b->entries  = b->entries
            ? (PosEntry *) repalloc(b->entries, newcap * sizeof(PosEntry))
            : (PosEntry *) palloc(newcap * sizeof(PosEntry));
        b->capacity = newcap;
    }
    b->entries[b->count].pos    = pos;
    b->entries[b->count].bitmap = bm;
    b->count++;
}

/* Per-column walk state shared by biscuit_persist_load_column_walk_cb(). */
typedef struct
{
    Relation        index;
    LoadBucket      pos[2][CHAR_RANGE];
    LoadBucket      neg[2][CHAR_RANGE];
    RoaringBitmap  *cache[2][CHAR_RANGE];
    RoaringBitmap **len_arr[2];       /* pre-sized to max_length[lower] by caller */
    RoaringBitmap **len_ge_arr[2];
    int             max_length[2];
} LoadColumnState;

static void
biscuit_persist_load_column_walk_cb(const BiscuitDirEntry *entry, void *vstate)
{
    LoadColumnState *st    = (LoadColumnState *) vstate;
    int              lower = entry->is_lower ? 1 : 0;

    switch (entry->kind)
    {
        case BISCUIT_DIR_KIND_POS:
            load_bucket_add(&st->pos[lower][(unsigned char) entry->ch], entry->position,
                             biscuit_persist_decode_entry(st->index, entry));
            break;
        case BISCUIT_DIR_KIND_NEG:
            load_bucket_add(&st->neg[lower][(unsigned char) entry->ch], entry->position,
                             biscuit_persist_decode_entry(st->index, entry));
            break;
        case BISCUIT_DIR_KIND_CACHE:
            st->cache[lower][(unsigned char) entry->ch] = biscuit_persist_decode_entry(st->index, entry);
            break;
        case BISCUIT_DIR_KIND_LEN:
            if (entry->position >= 0 && entry->position < st->max_length[lower])
                st->len_arr[lower][entry->position] = biscuit_persist_decode_entry(st->index, entry);
            break;
        case BISCUIT_DIR_KIND_LEN_GE:
            if (entry->position >= 0 && entry->position < st->max_length[lower])
                st->len_ge_arr[lower][entry->position] = biscuit_persist_decode_entry(st->index, entry);
            break;
        default:
            /* HEADER/TIDS/TOMBSTONES/FREELIST/STRCACHE: read separately
             * via direct biscuit_dir_find() calls, not via this walk. */
            break;
    }
}

/* Copy a LoadBucket's accumulated (pos,bitmap) pairs into a freshly
 * palloc'd CharIndex, in CacheMemoryContext (matching the old code's
 * allocation context for every in-memory structure it built). */
static void
load_bucket_into_charindex(CharIndex *ci, const LoadBucket *b)
{
    ci->count    = b->count;
    ci->capacity = Max(b->count, 8);
    ci->entries  = (PosEntry *) palloc(ci->capacity * sizeof(PosEntry));
    if (b->count > 0)
        memcpy(ci->entries, b->entries, b->count * sizeof(PosEntry));
}

/* Read one (col,is_lower) pair's whole string-cache blob back into a
 * palloc'd char*[num_records] array (NULL entries preserved). */
static char **
biscuit_persist_load_strcache(Relation index, int32 col, bool is_lower, int num_records)
{
    BiscuitDirEntry entry;
    char          **arr;
    char           *data;
    uint32          len;
    PCur            cur;
    int             i;

    arr = (char **) palloc0(Max(num_records, 1) * sizeof(char *));

    if (!biscuit_dir_find(index, col, is_lower, BISCUIT_DIR_KIND_STRCACHE, -1, -1, &entry, NULL))
        return arr;   /* nothing saved yet -- all-NULL array, matches absent cache */

    if (entry.blob_head == InvalidBlockNumber)
        return arr;

    biscuit_page_read_blob(index, entry.blob_head, &data, &len);
    pcur_init(&cur, data, len);

    for (i = 0; i < num_records; i++)
        arr[i] = pcur_get_str(&cur, CacheMemoryContext);

    if (data)
        pfree(data);

    if (cur.error)
        ereport(ERROR,
                (errmsg("biscuit: truncated string-cache blob for index %u (col=%d is_lower=%d)",
                        RelationGetRelid(index), col, (int) is_lower)));

    return arr;
}

static void
biscuit_persist_load_column(Relation index, int32 col,
                             int max_length, int max_length_lower,
                             /* out params, all filled in CacheMemoryContext */
                             CharIndex *pos_idx, CharIndex *neg_idx, RoaringBitmap **char_cache,
                             CharIndex *pos_idx_lower, CharIndex *neg_idx_lower, RoaringBitmap **char_cache_lower,
                             RoaringBitmap ***length_bitmaps, RoaringBitmap ***length_ge_bitmaps,
                             RoaringBitmap ***length_bitmaps_lower, RoaringBitmap ***length_ge_bitmaps_lower)
{
    LoadColumnState st;
    int             ch;

    memset(&st, 0, sizeof(st));
    st.index         = index;
    st.max_length[0] = max_length;
    st.max_length[1] = max_length_lower;

    *length_bitmaps          = (RoaringBitmap **) palloc0(Max(max_length, 1) * sizeof(RoaringBitmap *));
    *length_ge_bitmaps       = (RoaringBitmap **) palloc0(Max(max_length, 1) * sizeof(RoaringBitmap *));
    *length_bitmaps_lower    = (RoaringBitmap **) palloc0(Max(max_length_lower, 1) * sizeof(RoaringBitmap *));
    *length_ge_bitmaps_lower = (RoaringBitmap **) palloc0(Max(max_length_lower, 1) * sizeof(RoaringBitmap *));
    st.len_arr[0]       = *length_bitmaps;
    st.len_ge_arr[0]    = *length_ge_bitmaps;
    st.len_arr[1]       = *length_bitmaps_lower;
    st.len_ge_arr[1]    = *length_ge_bitmaps_lower;

    biscuit_dir_foreach_column(index, biscuit_dir_slot_for_col(col),
                                biscuit_persist_load_column_walk_cb, &st);

    for (ch = 0; ch < CHAR_RANGE; ch++)
    {
        load_bucket_into_charindex(&pos_idx[ch], &st.pos[0][ch]);
        load_bucket_into_charindex(&neg_idx[ch], &st.neg[0][ch]);
        char_cache[ch] = st.cache[0][ch];

        load_bucket_into_charindex(&pos_idx_lower[ch], &st.pos[1][ch]);
        load_bucket_into_charindex(&neg_idx_lower[ch], &st.neg[1][ch]);
        char_cache_lower[ch] = st.cache[1][ch];

        if (st.pos[0][ch].entries) pfree(st.pos[0][ch].entries);
        if (st.neg[0][ch].entries) pfree(st.neg[0][ch].entries);
        if (st.pos[1][ch].entries) pfree(st.pos[1][ch].entries);
        if (st.neg[1][ch].entries) pfree(st.neg[1][ch].entries);
    }
}

BiscuitIndex *
biscuit_persist_load(Relation index)
{
    Oid            indexoid = RelationGetRelid(index);
    int            natts    = index->rd_index->indnatts;
    BiscuitIndex  *idx      = NULL;
    MemoryContext  oldcontext;
    BiscuitDirEntry header_entry;
    instr_time     diag_start;   /* DIAGNOSTIC ONLY */

    biscuit_diag_decode_count = 0;   /* DIAGNOSTIC ONLY */
    INSTR_TIME_SET_CURRENT(diag_start);

    if (!biscuit_dir_find(index, BISCUIT_DIR_COL_SINGLETON, false, BISCUIT_DIR_KIND_HEADER,
                           -1, -1, &header_entry, NULL))
        return NULL;   /* nothing saved yet -- normal, caller falls back to a rebuild */

    oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

    PG_TRY();
    {
        PCur    hcur;
        char   *hdata;
        uint32  hlen;
        int32   num_columns;
        int32   raw_capacity;
        uint64  live_gen;
        int     unused_records, unused_columns, unused_max_len;

        biscuit_page_read_blob(index, header_entry.blob_head, &hdata, &hlen);
        pcur_init(&hcur, hdata, hlen);

        idx = (BiscuitIndex *) palloc0(sizeof(BiscuitIndex));

        idx->num_records = pcur_get_i32(&hcur);
        raw_capacity      = pcur_get_i32(&hcur);
        idx->capacity     = Max(raw_capacity, 1);
        idx->max_len              = pcur_get_i32(&hcur);
        idx->max_length_legacy    = pcur_get_i32(&hcur);
        idx->max_length_lower     = pcur_get_i32(&hcur);
        idx->insert_count         = pcur_get_i64(&hcur);
        idx->update_count         = pcur_get_i64(&hcur);
        idx->delete_count         = pcur_get_i64(&hcur);
        idx->tombstone_count      = pcur_get_i32(&hcur);
        num_columns                = pcur_get_i32(&hcur);
        idx->num_columns           = num_columns;

        if (hcur.error || num_columns != natts || idx->num_records < 0 ||
            idx->capacity < idx->num_records)
            ereport(ERROR,
                    (errmsg("biscuit: directory header mismatch for index %u", indexoid)));

        /*
         * Case-mode gating is deliberately NOT part of the persisted
         * state (design doc) -- always recomputed fresh from the live
         * Relation's opclass, so a REINDEX under a different opclass can
         * never serve the wrong structure set from stale data.
         */
        if (num_columns == 1)
        {
            idx->legacy_case_mode = biscuit_get_column_case_mode(index, 0);
        }
        else
        {
            int i;

            idx->column_case_mode = (uint8 *) palloc(num_columns * sizeof(uint8));
            for (i = 0; i < num_columns; i++)
                idx->column_case_mode[i] = biscuit_get_column_case_mode(index, i);
        }

        if (num_columns > 1)
        {
            int i;

            idx->column_types   = (Oid *) palloc(natts * sizeof(Oid));
            idx->output_funcs   = (FmgrInfo *) palloc(natts * sizeof(FmgrInfo));
            idx->column_indices = (ColumnIndex *) palloc0(natts * sizeof(ColumnIndex));

            for (i = 0; i < num_columns; i++)
            {
                Oid  typoutput;
                bool typIsVarlena;

                idx->column_types[i] = pcur_get_u32(&hcur);
                idx->column_indices[i].max_length       = pcur_get_i32(&hcur);
                idx->column_indices[i].max_length_lower = pcur_get_i32(&hcur);

                getTypeOutputInfo(idx->column_types[i], &typoutput, &typIsVarlena);
                fmgr_info(typoutput, &idx->output_funcs[i]);
            }
        }

        if (hcur.error)
            ereport(ERROR, (errmsg("biscuit: truncated directory header for index %u", indexoid)));
        if (hdata)
            pfree(hdata);

        /* ---- tids ---- */
        idx->tids = (ItemPointerData *) palloc(idx->capacity * sizeof(ItemPointerData));
        if (idx->num_records > 0)
        {
            BiscuitDirEntry tids_entry;
            char           *tdata;
            uint32          tlen;

            if (!biscuit_dir_find(index, BISCUIT_DIR_COL_SINGLETON, false, BISCUIT_DIR_KIND_TIDS,
                                   -1, -1, &tids_entry, NULL) ||
                tids_entry.blob_head == InvalidBlockNumber)
                ereport(ERROR, (errmsg("biscuit: missing tid array for index %u", indexoid)));

            biscuit_page_read_blob(index, tids_entry.blob_head, &tdata, &tlen);
            if (tlen != (uint32) (idx->num_records * sizeof(ItemPointerData)))
                ereport(ERROR, (errmsg("biscuit: tid array size mismatch for index %u", indexoid)));
            memcpy(idx->tids, tdata, tlen);
            if (tdata)
                pfree(tdata);
        }

        /* ---- tombstones ---- */
        {
            BiscuitDirEntry tomb_entry;

            if (biscuit_dir_find(index, BISCUIT_DIR_COL_SINGLETON, false, BISCUIT_DIR_KIND_TOMBSTONES,
                                  -1, -1, &tomb_entry, NULL))
                idx->tombstones = biscuit_persist_decode_entry(index, &tomb_entry);
            if (!idx->tombstones)
                idx->tombstones = biscuit_roaring_create();
        }

        /* ---- free list ---- */
        {
            BiscuitDirEntry fl_entry;

            idx->free_count = 0;
            if (biscuit_dir_find(index, BISCUIT_DIR_COL_SINGLETON, false, BISCUIT_DIR_KIND_FREELIST,
                                  -1, -1, &fl_entry, NULL) &&
                fl_entry.blob_head != InvalidBlockNumber)
            {
                char   *fdata;
                uint32  flen;

                biscuit_page_read_blob(index, fl_entry.blob_head, &fdata, &flen);
                idx->free_count    = (int) (flen / sizeof(uint32_t));
                idx->free_capacity = Max(idx->free_count, 64);
                idx->free_list     = (uint32_t *) palloc(idx->free_capacity * sizeof(uint32_t));
                if (idx->free_count > 0)
                    memcpy(idx->free_list, fdata, idx->free_count * sizeof(uint32_t));
                if (fdata)
                    pfree(fdata);
            }
            else
            {
                idx->free_capacity = 64;
                idx->free_list     = (uint32_t *) palloc(idx->free_capacity * sizeof(uint32_t));
            }
        }

        if (num_columns == 1)
        {
            idx->data_cache       = biscuit_persist_load_strcache(index, BISCUIT_DIR_COL_LEGACY, false, idx->num_records);
            idx->data_cache_lower = biscuit_persist_load_strcache(index, BISCUIT_DIR_COL_LEGACY, true, idx->num_records);

            biscuit_persist_load_column(index, BISCUIT_DIR_COL_LEGACY,
                                         idx->max_length_legacy, idx->max_length_lower,
                                         idx->pos_idx_legacy, idx->neg_idx_legacy, idx->char_cache_legacy,
                                         idx->pos_idx_lower, idx->neg_idx_lower, idx->char_cache_lower,
                                         &idx->length_bitmaps_legacy, &idx->length_ge_bitmaps_legacy,
                                         &idx->length_bitmaps_lower, &idx->length_ge_bitmaps_lower);
        }
        else
        {
            int col;

            idx->column_data_cache       = (char ***) palloc(natts * sizeof(char **));
            idx->column_data_cache_lower = (char ***) palloc(natts * sizeof(char **));

            for (col = 0; col < num_columns; col++)
            {
                idx->column_data_cache[col] =
                    biscuit_persist_load_strcache(index, col, false, idx->num_records);
                idx->column_data_cache_lower[col] =
                    biscuit_persist_load_strcache(index, col, true, idx->num_records);
            }

            for (col = 0; col < num_columns; col++)
            {
                ColumnIndex *cidx = &idx->column_indices[col];

                biscuit_persist_load_column(index, col,
                                             cidx->max_length, cidx->max_length_lower,
                                             cidx->pos_idx, cidx->neg_idx, cidx->char_cache,
                                             cidx->pos_idx_lower, cidx->neg_idx_lower, cidx->char_cache_lower,
                                             &cidx->length_bitmaps, &cidx->length_ge_bitmaps,
                                             &cidx->length_bitmaps_lower, &cidx->length_ge_bitmaps_lower);
            }
        }

        /* DIAGNOSTIC ONLY */
        {
            instr_time diag_now;

            INSTR_TIME_SET_CURRENT(diag_now);
            INSTR_TIME_SUBTRACT(diag_now, diag_start);
            elog(DEBUG1,
                 "biscuit: cold load decoded %ld structure(s) for index %u in %.3f ms",
                 biscuit_diag_decode_count, indexoid,
                 INSTR_TIME_GET_MILLISEC(diag_now));
        }

        /*
         * gen/gen_at_last_snapshot: no more "is this stale relative to
         * the metapage" check (see file header) -- what we just read *is*
         * the live durable state by construction. Still populated from
         * the metapage's current gen since idx->gen is the in-memory
         * generation counter consulted elsewhere (e.g. cache
         * invalidation bookkeeping); gen_at_last_snapshot is kept in
         * lockstep with it here purely for field-consistency, not
         * because anything still compares the two to decide whether to
         * re-save.
         */
        if (biscuit_read_metadata_from_disk(index, &unused_records, &unused_columns,
                                             &unused_max_len, &live_gen))
        {
            idx->gen                  = live_gen;
            idx->gen_at_last_snapshot = live_gen;
        }
    }
    PG_CATCH();
    {
        /*
         * Corrupt/inconsistent directory state -- discard everything
         * (it's all in CacheMemoryContext, which we don't reset, so just
         * drop the pointer) and return NULL. biscuit_load_index() treats
         * a NULL return as "nothing readable" and raises an ERROR with a
         * REINDEX hint -- there is no from-heap rebuild fallback to defer
         * to anymore.
         */
        ErrorData *edata = CopyErrorData();

        FlushErrorState();
        MemoryContextSwitchTo(oldcontext);
        elog(WARNING, "biscuit: discarding unreadable directory-backed state for index %u (%s)",
             indexoid, edata->message);
        FreeErrorData(edata);
        return NULL;
    }
    PG_END_TRY();

    MemoryContextSwitchTo(oldcontext);

    elog(DEBUG1, "biscuit: loaded directory-backed state for index %u (%d records, gen " UINT64_FORMAT ")",
         indexoid, idx->num_records, idx->gen);

    return idx;
}

/* ================================================================
 * DROP
 * ================================================================ */

typedef struct
{
    Relation index;
} DropWalkState;

static void
biscuit_persist_drop_walk_cb(const BiscuitDirEntry *entry, void *vstate)
{
    DropWalkState *st = (DropWalkState *) vstate;

    if (entry->blob_head != InvalidBlockNumber)
        biscuit_page_free_blob(st->index, entry->blob_head);
    if (entry->pending_head != InvalidBlockNumber)
        biscuit_page_free_chain(st->index, entry->pending_head);
}

void
biscuit_persist_drop(Oid indexoid)
{
    Relation index = try_relation_open(indexoid, RowExclusiveLock);

    if (!index)
        return;   /* relation already gone -- nothing to drop, matches
                    * "safe to call even if no snapshot exists" */

    PG_TRY();
    {
        int num_slots = biscuit_dir_num_slots(index);
        int slot;

        for (slot = 0; slot < num_slots; slot++)
        {
            DropWalkState st;

            st.index = index;
            biscuit_dir_foreach_column(index, slot, biscuit_persist_drop_walk_cb, &st);
        }

        biscuit_dir_drop_all(index);
    }
    PG_CATCH();
    {
        ErrorData *edata = CopyErrorData();

        FlushErrorState();
        relation_close(index, RowExclusiveLock);
        elog(WARNING, "biscuit: error dropping directory-backed state for index %u: %s",
             indexoid, edata->message);
        FreeErrorData(edata);
        return;
    }
    PG_END_TRY();

    relation_close(index, RowExclusiveLock);
}
