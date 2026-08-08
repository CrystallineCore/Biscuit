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
 * entry a column's chain holds, decoding each one's blob back into the
 * right BiscuitIndex field. Note it decodes the *compacted* blob only:
 * undrained deltas live in the index-wide shared log (biscuit_pendlog.c)
 * and are applied per-statement by the read path, not folded in here --
 * the log keeps growing after a load completes, so a load-time merge
 * would be both wrong and pointless.
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
#include "biscuit_pendlog.h"
#include "biscuit_rowstore.h"   /* in-place TIDS/STRCACHE/HEADER I/O --
                                  * see biscuit_rowstore.h's file header for
                                  * why HEADER/TIDS/STRCACHE moved off the
                                  * biscuit_page_write_blob()/_read_blob()
                                  * whole-rewrite path used below for
                                  * everything else (POS/NEG/CACHE/LEN/
                                  * LEN_GE/TOMBSTONES/FREELIST) */
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

/*
 * The length-prefixed string put/get pair that used to live here
 * (pbuf_put_str/pcur_get_str) is gone: its only callers were the STRCACHE
 * whole-array save/load, which concatenated every record's string into one
 * blob. STRCACHE is now stored per-slot via biscuit_rowstore.c (pointer
 * array + value heap), so nothing length-prefixes strings into a PBuf
 * anymore. The remaining PBuf/PCur helpers still serve the HEADER blob.
 */

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

    BiscuitDirEntryInit(&newentry, col, is_lower, kind, ch, position);
    newentry.blob_head = new_head;

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

/* ================================================================
 * Row-identity allocation lock
 * ================================================================
 *
 * One heavyweight ExclusiveLock per index, held across the whole
 * row-identity durable write. See BISCUIT_ROWSTORE_LOCK_BLKNO in
 * biscuit_common.h for what it protects and why.
 *
 * Scope and ordering:
 *   - Taken before any buffer lock in this path and released after all of
 *     them are dropped, so it never participates in a buffer-lock cycle
 *     *within this backend*.
 *   - biscuit_page_alloc() takes the metapage buffer lock and
 *     LockRelationForExtension() beneath us; nothing anywhere takes those
 *     and then reaches for this lock, so the order is total.
 *   - biscuit_pendlog_drain_all() serializes drainers against each other on
 *     BISCUIT_METAPAGE_BLKNO, a different tag. A single backend never
 *     holds both at once -- biscuit_insert() does all of its pendlog work
 *     (including any opportunistic drain) before it reaches the
 *     row-identity write -- but that says nothing about two *different*
 *     backends, and until the fix below, nothing did: a drain running in
 *     one backend and a row-identity write's BiscuitXlogBatch running in
 *     another could each be waiting on a buffer content LWLock the other
 *     holds (the drain reading STRCACHE via biscuit_delta_expand_slots(),
 *     the batch holding a STRCACHE/TID page open across the row), with
 *     nothing but that LWLock pair connecting them. LWLocks are invisible
 *     to the deadlock detector, so that cycle hangs forever instead of
 *     getting caught and aborted.
 *
 *     The fix: biscuit_pendlog.c's pendlog_drain_internal() now takes this
 *     same lock (see its call site) for the span in which it reads
 *     STRCACHE, so "a batch is open" and "a drain is reading STRCACHE"
 *     become mutually exclusive via one heavyweight primitive instead of
 *     racing through independent LWLocks. Ordering stays total: only the
 *     drain ever nests it inside BISCUIT_METAPAGE_BLKNO; an ordinary
 *     row-identity write never takes BISCUIT_METAPAGE_BLKNO at all, so
 *     there is no path back the other way for a cycle to close.
 *
 * No PG_TRY is needed to release on error: heavyweight locks are dropped
 * by the lock manager at transaction end, including abort. The explicit
 * unlock is just early release so a long transaction does not hold the
 * index's allocation lock across statements.
 */
void
biscuit_rowstore_alloc_lock(Relation index)
{
    LockPage(index, BISCUIT_ROWSTORE_LOCK_BLKNO, ExclusiveLock);
}

void
biscuit_rowstore_alloc_unlock(Relation index)
{
    UnlockPage(index, BISCUIT_ROWSTORE_LOCK_BLKNO, ExclusiveLock);
}

/* Forward declaration: defined further down alongside biscuit_persist_save_strcache()
 * (its build-time bulk-path sibling), but biscuit_persist_row_identity_write_record()
 * below needs it too and is declared first to sit next to biscuit_persist_write_tid(). */
static void biscuit_persist_row_identity_write_str(Relation index, int32 col, bool is_lower,
                                                     uint32 slot_idx, const char *str,
                                                     BiscuitXlogBatch *batch);

/* ================================================================
 * HEADER / TIDS -- directory-aware wrappers around biscuit_rowstore.c's
 * in-place primitives. Both kinds have exactly one directory entry each
 * (BISCUIT_DIR_COL_SINGLETON), so "find-or-create the entry, write
 * through the rowstore, persist blob_head if it moved" is the whole
 * pattern -- mirrors biscuit_persist_row_identity_write_str() above for
 * STRCACHE's (col, is_lower)-keyed entries.
 * ================================================================ */

static void
biscuit_persist_write_header_blob(Relation index, const char *data, uint32 len)
{
    BiscuitDirEntry    entry;
    BiscuitDirEntryRef ref;
    BlockNumber        head;

    if (!biscuit_dir_find(index, BISCUIT_DIR_COL_SINGLETON, false, BISCUIT_DIR_KIND_HEADER,
                           -1, -1, &entry, &ref))
    {
        BiscuitDirEntryInit(&entry, BISCUIT_DIR_COL_SINGLETON, false,
                             BISCUIT_DIR_KIND_HEADER, -1, -1);
        biscuit_dir_insert(index, &entry, &ref);
    }

    head = entry.blob_head;
    biscuit_rowstore_header_write(index, &head, data, len);

    if (head != entry.blob_head)
    {
        entry.blob_head = head;
        biscuit_dir_update(index, &ref, &entry);
    }
}

/*
 * biscuit_persist_write_tid
 * Find-or-create the singleton TIDS directory entry, then durably write
 * one slot. Shared by the build-time bulk path (biscuit_persist_save(),
 * looping once per record) and the steady-state per-row entry point
 * (biscuit_persist_row_identity_write_record()).
 */
static void
biscuit_persist_write_tid(Relation index, uint32 slot_idx, const ItemPointerData *tid,
                           BiscuitSlotWriteMode mode, BiscuitXlogBatch *batch)
{
    BiscuitDirEntry    entry;
    BiscuitDirEntryRef ref;
    BlockNumber        pagedir_root;

    if (!biscuit_dir_find(index, BISCUIT_DIR_COL_SINGLETON, false, BISCUIT_DIR_KIND_TIDS,
                           -1, -1, &entry, &ref))
    {
        /* blob_head is the pagedir root for this kind; no value heap. */
        BiscuitDirEntryInit(&entry, BISCUIT_DIR_COL_SINGLETON, false,
                             BISCUIT_DIR_KIND_TIDS, -1, -1);
        biscuit_dir_insert(index, &entry, &ref);
    }

    pagedir_root = entry.blob_head;
    biscuit_rowstore_tid_write(index, &pagedir_root, slot_idx, tid, mode, batch);

    if (pagedir_root != entry.blob_head)
    {
        entry.blob_head = pagedir_root;
        biscuit_dir_update(index, &ref, &entry);
    }
}

/*
 * biscuit_persist_row_identity_write_record
 * Public entry point (biscuit_persist.h) for the steady-state per-row
 * durability write: one TIDS slot plus every (column, is_lower) STRCACHE
 * slot for record slot_idx, all from idx's current in-memory state.
 */
void
biscuit_persist_row_identity_write_record(Relation index, BiscuitIndex *idx, uint32 slot_idx,
                                           BiscuitSlotWriteMode mode)
{
    BiscuitXlogBatch batch;

    /*
     * Serialize the whole sequence, not just the page allocations inside
     * it. Every one of the calls below is a find-or-create against shared
     * directory-entry state, and the TIDS write and the STRCACHE writes
     * must agree about that state -- locking each one individually would
     * leave the gaps between them open.
     */
    biscuit_rowstore_alloc_lock(index);

    /*
     * One shared GenericXLogState for this row's steady-state writes
     * (TID slot, each column's raw/lower STRCACHE slot), instead of one
     * GenericXLogStart/Finish pair per page. See BiscuitXlogBatch's
     * comment in biscuit_common.h for the mechanics and the shape of
     * the WAL-record-count reduction this buys. A rollover inside any
     * one of the calls below (a fresh TIDSLOT/STRPTR logical page, a
     * fresh STRHEAP tail) transparently flushes this batch first and
     * runs as its own isolated transaction, same as before batching
     * existed -- the caller here doesn't need to know which happened.
     */
    biscuit_xlog_batch_init(&batch, index);

    biscuit_persist_write_tid(index, slot_idx, &idx->tids[slot_idx], mode, &batch);

    if (idx->num_columns == 1)
    {
        biscuit_persist_row_identity_write_str(index, BISCUIT_DIR_COL_LEGACY, false,
                                                slot_idx,
                                                idx->data_cache ? idx->data_cache[slot_idx] : NULL,
                                                &batch);
        biscuit_persist_row_identity_write_str(index, BISCUIT_DIR_COL_LEGACY, true,
                                                slot_idx,
                                                idx->data_cache_lower ? idx->data_cache_lower[slot_idx] : NULL,
                                                &batch);
    }
    else
    {
        int col;

        for (col = 0; col < idx->num_columns; col++)
        {
            biscuit_persist_row_identity_write_str(index, col, false, slot_idx,
                                                    idx->column_data_cache[col] ?
                                                        idx->column_data_cache[col][slot_idx] : NULL,
                                                    &batch);
            biscuit_persist_row_identity_write_str(index, col, true, slot_idx,
                                                    idx->column_data_cache_lower[col] ?
                                                        idx->column_data_cache_lower[col][slot_idx] : NULL,
                                                    &batch);
        }
    }

    /* Flush whatever the batch is still holding -- there is always at
     * least the TID write left open here in the (common) all-in-place,
     * no-rollover case. */
    biscuit_xlog_batch_flush(&batch);

    biscuit_rowstore_alloc_unlock(index);
}

/*
 * DIAGNOSTIC ONLY -- not part of the fix, just instrumentation to confirm
 * where biscuit_persist_load()'s cold-load time actually goes. Safe to
 * remove once confirmed; DEBUG1-gated so it's silent by default.
 */
static long biscuit_diag_decode_count = 0;

/*
 * biscuit_persist_read_blob_retry
 *
 * biscuit_page_read_blob() now raises ERRCODE_T_R_SERIALIZATION_FAILURE
 * (see biscuit_blob.c) when the chain it was asked to read was
 * concurrently retired by a compaction drain between the caller's
 * directory lookup and biscuit_page_read_blob()'s first lock acquisition.
 * That is not corruption -- the directory entry has simply moved on to a
 * new compacted chain in the meantime -- so the correct response is to
 * re-resolve the entry by its (col, is_lower, kind, ch, position) identity
 * and read again, not to abort the statement.
 *
 * Every caller in this file that pairs a biscuit_dir_find() with a
 * biscuit_page_read_blob() has exactly the identity it needs to retry
 * this way, since biscuit_dir_find() always re-reads the directory fresh
 * from disk (biscuit_dir.c takes no lock across calls, so this costs
 * nothing when there's no race to retry).
 *
 * Bounded at a handful of attempts: a single drain retires a given
 * structure's old chain at most once, so genuinely hitting this more than
 * once or twice in a row would mean something else is wrong, and this
 * should not become an unbounded spin.
 */
static void
biscuit_persist_read_blob_retry(Relation index,
                                 int32 col, bool is_lower, uint8 kind,
                                 int32 ch, int32 position,
                                 BlockNumber initial_head,
                                 char **out_data, uint32 *out_len)
{
    volatile BlockNumber head = initial_head;
    volatile int attempt;

    for (attempt = 0; ; attempt++)
    {
        MemoryContext oldcxt = CurrentMemoryContext;

        PG_TRY();
        {
            biscuit_page_read_blob(index, head, out_data, out_len);
            return;
        }
        PG_CATCH();
        {
            ErrorData     *edata;
            BiscuitDirEntry fresh;

            MemoryContextSwitchTo(oldcxt);
            edata = CopyErrorData();

            if (edata->sqlerrcode != ERRCODE_T_R_SERIALIZATION_FAILURE ||
                attempt >= 4)
            {
                /* Not our race, or retries exhausted: propagate as-is. */
                ReThrowError(edata);
            }
            FlushErrorState();

            if (!biscuit_dir_find(index, col, is_lower, kind, ch, position,
                                   &fresh, NULL) ||
                fresh.blob_head == InvalidBlockNumber)
            {
                /*
                 * The structure is gone entirely now (e.g. genuinely
                 * emptied out from under us) -- nothing left to read.
                 */
                *out_data = NULL;
                *out_len  = 0;
                return;
            }

            head = fresh.blob_head;
            /* loop and retry with the freshly-resolved head */
        }
        PG_END_TRY();
    }
}

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

        biscuit_persist_read_blob_retry(index,
                                         entry->col, entry->is_lower, entry->kind,
                                         entry->ch, entry->position,
                                         entry->blob_head, &data, &len);
        if (data)
        {
            bm = biscuit_roaring_deserialize(data, len);
            pfree(data);
        }
    }

    /*
     * No pending-chain merge here any more. Bitmap kinds no longer own a
     * per-structure pending chain at all -- undrained deltas live in the
     * index-wide shared log (biscuit_pendlog.c) and are applied by the
     * read path via biscuit_pendlog_snapshot()/_apply(), not at load time.
     * This function therefore returns the *compacted* bitmap only, which
     * is exactly what the caller wants to cache: reconciliation against
     * the log happens per-statement on read, because the log keeps growing
     * after this load completes.
     */
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

/*
 * biscuit_persist_row_identity_write_str
 *
 * Find-or-create the (col, is_lower) STRCACHE directory entry, then write
 * slot_idx's string into it via biscuit_rowstore_str_write(). Shared by
 * both the build-time bulk path (biscuit_persist_save_strcache() below,
 * called once per record) and the steady-state per-row entry point
 * (biscuit_persist_row_identity_write_record()) -- there is exactly one
 * way to durably write a STRCACHE slot, regardless of which caller needed
 * it written.
 */
static void
biscuit_persist_row_identity_write_str(Relation index, int32 col, bool is_lower,
                                        uint32 slot_idx, const char *str,
                                        BiscuitXlogBatch *batch)
{
    BiscuitDirEntry     entry;
    BiscuitDirEntryRef  ref;
    BlockNumber         ptr_root, heap_head, heap_tail;
    bool                changed;

    if (!biscuit_dir_find(index, col, is_lower, BISCUIT_DIR_KIND_STRCACHE, -1, -1, &entry, &ref))
    {
        /* blob_head = ptr-array pagedir root; strheap_* = value heap. */
        BiscuitDirEntryInit(&entry, col, is_lower,
                             BISCUIT_DIR_KIND_STRCACHE, -1, -1);
        biscuit_dir_insert(index, &entry, &ref);
    }

    ptr_root  = entry.blob_head;
    heap_head = entry.strheap_head;
    heap_tail = entry.strheap_tail;

    biscuit_rowstore_str_write(index, &ptr_root, &heap_head, &heap_tail,
                                slot_idx, str, str ? (int32) strlen(str) : -1,
                                batch);

    changed = (ptr_root != entry.blob_head ||
               heap_head != entry.strheap_head ||
               heap_tail != entry.strheap_tail);

    if (changed)
    {
        entry.blob_head    = ptr_root;
        entry.strheap_head = heap_head;
        entry.strheap_tail = heap_tail;
        biscuit_dir_update(index, &ref, &entry);
    }
}

static void
biscuit_persist_save_strcache(Relation index, int32 col,
                               char **cache, char **cache_lower, int num_records)
{
    int i;

    for (i = 0; i < num_records; i++)
    {
        /*
         * NULL batch: this is the build-time bulk path (index build,
         * REINDEX), which already amortizes its cost across the whole
         * relation scan rather than per-row, and isn't the steady-state
         * insert path the batching in biscuit_persist_row_identity_
         * write_record() targets. Left as one transaction per page, same
         * as before.
         */
        biscuit_persist_row_identity_write_str(index, col, false, (uint32) i,
                                                cache ? cache[i] : NULL, NULL);
        biscuit_persist_row_identity_write_str(index, col, true, (uint32) i,
                                                cache_lower ? cache_lower[i] : NULL, NULL);
    }
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

        biscuit_persist_write_header_blob(index, header.data, (uint32) header.len);
        pfree(header.data);

        /* ---- tids ---- */
        for (i = 0; i < idx->num_records; i++)
            /*
             * Whole-snapshot rewrite: every slot here is one this call
             * already owns by definition, so no fresh-claim occupancy
             * check applies.
             */
            biscuit_persist_write_tid(index, (uint32) i, &idx->tids[i],
                                       BISCUIT_SLOT_WRITE_INPLACE, NULL);

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

/*
 * biscuit_persist_save_row_identity
 *
 * As of the in-place row-identity rewrite (biscuit_rowstore.c): re-persists
 * the HEADER (scalar bookkeeping -- num_records, capacity, counters, ...),
 * the TOMBSTONES bitmap, and the
 * FREELIST. TIDS and the per-column STRCACHE are deliberately NOT written
 * here anymore -- see biscuit_persist.h's comment on this function and on
 * biscuit_persist_row_identity_write_record() for why: they are now made
 * durable incrementally, one slot at a time, at the exact row-mutation
 * call sites in biscuit_index.c, instead of being rewritten wholesale
 * here on every commit (the O(num_records)-per-commit root cause the
 * "Biscuit Row-Identity In-Place Storage" implementation summary measured
 * and fixed).
 *
 * HEADER is now a single in-place page write (biscuit_rowstore_header_write())
 * rather than a chain reallocation, so this whole function is O(1) in
 * num_records, not O(num_records) -- it no longer needs (and doesn't get)
 * any special "don't call this per row" caveat; the existing once-per-
 * statement deferred-flush call sites in biscuit_index.c are kept simply
 * because there's no reason to write the header more often than that, not
 * because doing so would be expensive.
 *
 * TOMBSTONES/FREELIST are unchanged from before this phase -- both are
 * already proportional to deleted-row count rather than total table size,
 * so they were never the cost problem this phase targets.
 *
 * Takes a live Relation (already open) rather than an Oid, because every
 * caller (biscuit_insert/biscuit_bulkdelete) already holds one.
 */
void
biscuit_persist_save_row_identity(Relation index, BiscuitIndex *idx)
{
    Oid  indexoid = RelationGetRelid(index);
    PBuf header;
    int  i;
    int  header_records = idx->num_records;

    /*
     * NON-REGRESSING RECORD COUNT.
     *
     * idx->num_records is only pulled forward from the metapage inside
     * biscuit_claim_new_slot(), which runs solely in biscuit_insert()'s
     * !found_existing branch. An UPDATE (found_existing) or a
     * biscuit_bulkdelete() marks the index dirty and reaches this function
     * without ever having consulted the metapage, so a backend whose cached
     * copy predates other backends' claims -- or its own earlier aborted
     * ones -- would serialize a *lower* num_records than the authoritative
     * high-water mark, permanently truncating what the next cold load reads
     * back (see the reconciliation comment in biscuit_persist_load()).
     *
     * Mirror biscuit_write_metadata_to_disk()'s Max(): never publish a
     * count below the metapage's. The metapage read happens before
     * biscuit_rowstore_alloc_lock() below so this adds no nesting to the
     * lock order.
     *
     * THIS VALUE IS A CLAIM HIGH-WATER MARK. IT IS NOT A DURABILITY
     * WATERMARK, AND NOTHING MAY TREAT IT AS ONE.
     *
     * The clamp above is unconditional and runs for ANY dirty-marking
     * transaction -- an UPDATE in place, a bulkdelete -- none of which
     * called biscuit_claim_new_slot() or know anything about the slot that
     * pushed meta->num_records up. So a transaction B committing while
     * writer A is still between its claim and its TID write will bake A's
     * not-yet-durable slot into the durably-saved header_records. There is
     * no cheap way to avoid that: meta->num_records is the only shared
     * counter, and it counts claims by construction
     * (biscuit_claim_new_slot() publishes it before the row write precisely
     * to keep the claim off that critical path).
     *
     * Keeping the clamp is still right -- without it the header regresses
     * and the next cold load truncates the index, which is the defect this
     * block was added for. What was wrong was a reader downstream inferring
     * durability from it: biscuit_persist_load()'s hole-clamp used to treat
     * [0, header_records) as known-populated and scan only above it, so a
     * laundered hole sat permanently below its floor and was never seen.
     * That inference is gone (see that function), and holes are now resolved
     * where they can actually be identified -- per slot, from the
     * unambiguous all-zero TID encoding, at read time in
     * biscuit_collect_sorted_tids_single().
     *
     * If a genuine durability watermark is ever wanted -- a second metapage
     * counter bumped only after biscuit_persist_row_identity_write_record()
     * succeeds -- this is the site that would consume it. It would be an
     * on-disk format change, and nothing currently needs it: every consumer
     * that used to require the distinction now derives it per slot instead.
     *
     * The clamped value is written to the header ONLY -- idx->num_records
     * itself is deliberately left alone. The in-memory arrays (tids,
     * data_cache, data_cache_lower) are exactly idx->capacity long, and
     * every loop over [0, idx->num_records) in biscuit_index.c indexes them
     * directly; raising the counter past capacity here would turn those
     * loops into out-of-bounds reads. Widening the arrays is
     * biscuit_ensure_slot_capacity()'s job and is driven by the claimed slot
     * number, not by this counter. Slots in the gap belong to other
     * backends' rows and stay holes in this backend's copy until it next
     * reloads -- the "cold reader must reload" contract documented on
     * biscuit_claim_new_slot().
     */
    {
        int    meta_records = 0;
        int    meta_columns, meta_max_len;
        uint64 meta_gen = 0;

        if (biscuit_read_metadata_from_disk(index, &meta_records,
                                             &meta_columns, &meta_max_len,
                                             &meta_gen) &&
            meta_records > header_records)
        {
            elog(DEBUG1,
                 "biscuit: index %u raising header records %d -> %d (metapage)",
                 indexoid, header_records, meta_records);
            header_records = meta_records;
        }
    }

    /*
     * Same directory find-or-create race as the per-row path: all three
     * writes below are read-modify-writes of shared directory entries
     * (HEADER, TOMBSTONES, FREELIST), and biscuit_dir_insert() does not
     * detect duplicates. This runs once per statement at pre-commit, so
     * the lock is uncontended in practice -- but "uncontended in practice"
     * was exactly the assumption that produced the original bug.
     */
    biscuit_rowstore_alloc_lock(index);

    PG_TRY();
    {
        /* ---- header (scalar bookkeeping) ---- */
        pbuf_init(&header);
        /* header_records, not idx->num_records -- see the non-regressing
         * record count comment at the top of this function. capacity is
         * widened to match so the "capacity >= num_records" validation in
         * biscuit_persist_load() still holds for the value we just clamped
         * up; the next loader allocates its arrays at that width. */
        pbuf_put_i32(&header, header_records);
        pbuf_put_i32(&header, Max(idx->capacity, header_records));
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

        biscuit_persist_write_header_blob(index, header.data, (uint32) header.len);
        pfree(header.data);

        /* ---- tombstones ---- */
        biscuit_persist_write_bitmap(index, BISCUIT_DIR_COL_SINGLETON, false,
                                      BISCUIT_DIR_KIND_TOMBSTONES, -1, -1,
                                      idx->tombstones);

        /* ---- free list ---- */
        biscuit_persist_write_raw(index, BISCUIT_DIR_COL_SINGLETON, false,
                                   BISCUIT_DIR_KIND_FREELIST, -1, -1,
                                   idx->free_count > 0 ? (const char *) idx->free_list : NULL,
                                   (uint32) (idx->free_count * sizeof(uint32_t)));
    }
    PG_CATCH();
    {
        /* Same rationale as biscuit_persist_save(): a swallowed failure
         * here would leave a cold load unable to see these rows. Let it
         * propagate so the surrounding INSERT fails visibly. The
         * allocation lock needs no explicit release on this path -- abort
         * drops it. */
        PG_RE_THROW();
    }
    PG_END_TRY();

    biscuit_rowstore_alloc_unlock(index);

    elog(DEBUG1, "biscuit: re-saved header/tombstones/freelist for index %u (%d records, gen " UINT64_FORMAT ")",
         indexoid, header_records, idx->gen);
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
static int
biscuit_posentry_cmp(const void *a, const void *b)
{
    int pa = ((const PosEntry *) a)->pos;
    int pb = ((const PosEntry *) b)->pos;
    return (pa > pb) - (pa < pb);
}

static void
load_bucket_into_charindex(CharIndex *ci, const LoadBucket *b)
{
    ci->count    = b->count;
    ci->capacity = Max(b->count, 8);
    ci->entries  = (PosEntry *) palloc(ci->capacity * sizeof(PosEntry));
    if (b->count > 0)
        memcpy(ci->entries, b->entries, b->count * sizeof(PosEntry));

    /*
     * Sort by pos. biscuit_get_pos_bitmap()/biscuit_get_neg_bitmap()
     * binary-search cidx->entries by pos, which requires them ordered.
     * The load bucket is filled in directory-discovery order, which is
     * only coincidentally sorted: when a steady-state INSERT creates the
     * first POS/NEG structure for a new (char,position) AFTER build already
     * created other positions for that same char, the directory chain --
     * and hence this bucket -- holds positions out of order. Without this
     * sort the binary search misses the out-of-order entry and returns
     * NULL, so a prefix/exact query silently drops every row that depends
     * on that position once VACUUM (or an inline drain) has moved the
     * records from the pending list into the compacted blob. (Before the
     * drain the same query worked, because reconciliation merged the
     * pending records regardless of entry order -- which is why this only
     * manifested after a VACUUM.)
     */
    if (ci->count > 1)
        qsort(ci->entries, ci->count, sizeof(PosEntry), biscuit_posentry_cmp);
}

/* Read one (col,is_lower) pair's whole string-cache blob back into a
 * palloc'd char*[capacity] array (NULL entries preserved).
 *
 * The array is allocated to the full `capacity` -- the same width as
 * idx->tids -- even though only the first `num_records` entries are read
 * back from the blob (the rest stay NULL from palloc0). This is load-
 * bearing: the steady-state insert path (biscuit_insert() in
 * biscuit_index.c) grows tids / data_cache / data_cache_lower together and
 * only when `num_records >= capacity`, so it assumes all three arrays are
 * always exactly `capacity` long. Sizing this cache to `num_records`
 * instead (as this function used to) left the gap [num_records, capacity)
 * unallocated: after a cold reload, the very next insert would write
 * data_cache[num_records] past the end of a too-small allocation, smashing
 * an adjacent palloc chunk header and later tripping repalloc()/pfree()
 * with an "invalid pointer" error. */
static char **
biscuit_persist_load_strcache(Relation index, int32 col, bool is_lower,
                              int num_records, int capacity)
{
    BiscuitDirEntry entry;
    char          **arr;

    /* Width follows capacity (>= num_records, enforced by the header
     * validation in biscuit_persist_load); Max(...,1) keeps a 0-capacity
     * edge case from producing a zero-byte allocation. */
    arr = (char **) palloc0(Max(capacity, 1) * sizeof(char *));

    if (!biscuit_dir_find(index, col, is_lower, BISCUIT_DIR_KIND_STRCACHE, -1, -1, &entry, NULL))
        return arr;   /* nothing saved yet -- all-NULL array, matches absent cache */

    if (entry.blob_head == InvalidBlockNumber)
        return arr;   /* pointer-array page directory never allocated */

    /*
     * entry.blob_head is the pointer-array page-directory root (see
     * biscuit_common.h's "Field repurposing" comment), not a concatenated
     * length-prefixed blob as it was before the in-place rewrite.
     *
     * Read via the bulk path rather than a per-slot loop: reading slots
     * individually re-walks the page directory and re-reads the STRPTR
     * page for every record, which measured ~1.8x slower on cold load than
     * the single-blob read this replaced.
     * biscuit_rowstore_str_read_all() walks the directory once and reuses
     * the value-heap buffer across consecutive slots (the heap is
     * bump-allocated in slot order, so runs of slots share a page).
     *
     * A slot that comes back NULL is either an explicitly-NULL entry or
     * one whose logical page was never allocated. Those are
     * indistinguishable here and identical in effect -- both leave the
     * palloc0'd NULL in place, exactly as the old length-prefixed
     * NULL encoding did -- so, unlike the old single-blob read, there is
     * no truncation condition to detect and no error to raise.
     */
    biscuit_rowstore_str_read_all(index, entry.blob_head, (uint32) num_records,
                                   CacheMemoryContext, arr);

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

/*
 * biscuit_persist_load() makes dozens of independent (biscuit_dir_find ->
 * biscuit_page_read_blob) round trips, one per structure, with nothing
 * tying them together -- so a concurrent drain can repoint a directory
 * entry and retire its old chain (biscuit_retire_page_locked() repurposes
 * a retired page's opaque->next into a freelist link immediately and
 * unconditionally, with no horizon check -- the recycle_xid horizon in
 * biscuit_page_alloc() only gates *reuse* of a retired page's content, a
 * separate later step) while this function is still mid-walk against the
 * *old* chain it read earlier in the same call. That is the "blob chunk
 * chain inconsistency (expected seq N)" corruption reported against v14.
 *
 * The fix is NOT a lock. An earlier attempt serialized the whole load
 * against the whole drain with a heavyweight LockPage() on
 * BISCUIT_METAPAGE_BLKNO, matching pendlog_drain_internal()'s
 * ExclusiveLock on the same tag -- correct in isolation, but it let this
 * function hold that heavyweight lock across a long walk that also
 * acquires and lock-couples ordinary buffer-content LWLocks throughout.
 * That combination reintroduced the v12 hang as a genuine, undetectable
 * deadlock: a cycle that passes through an LWLock is invisible to
 * PostgreSQL's (heavyweight-lock-only) deadlock detector, so two backends
 * each holding one class of lock while waiting on the other's simply wait
 * forever with nothing logged.
 *
 * Retrying is the right tool here, not locking, and the metapage already
 * carries exactly the counter needed to detect a torn read:
 * total_drains (see BiscuitMetaPageData / biscuit_read_pending_stats()).
 * A drain always bumps it, under its own lock, before this function could
 * possibly observe the directory entries it touched. So: snapshot
 * total_drains before the walk and again after; if it moved, some drain
 * ran (at least partly) concurrently with this attempt and the read must
 * be treated as unreliable regardless of whether it happened to also
 * throw -- retry rather than trusting a read that got lucky, and rather
 * than discarding one that got unlucky. Only a load that sees a *stable*
 * total_drains across the whole attempt and still fails is genuine
 * corruption worth the WARNING + REINDEX hint.
 *
 * biscuit_read_pending_stats() only takes the metapage's own
 * buffer-content lock, momentarily, exactly like every other reader here
 * -- it adds no new lock class and therefore no new deadlock surface.
 */
#define BISCUIT_LOAD_MAX_ATTEMPTS 12

/*
 * Sleep between load attempts that lost to a drain.
 *
 * Retrying immediately is close to useless against the failure this budget
 * exists to survive. A merge is not an instant: it rewrites every structure
 * the log touched and restructures directory entries, and on a large index
 * that is milliseconds, not microseconds. A tight loop of 8 immediate
 * retries can comfortably fit inside one merge and exhaust the whole budget
 * without ever having sampled a quiet moment -- which then reports as
 * "every attempt raced a drain" and looks like a much rarer event than it
 * is.
 *
 * Doubling from 1 ms and capped at 64 ms: roughly 400 ms of total patience
 * across the budget, which comfortably outlasts a compaction of the bounded
 * prefix size the append path triggers, while still failing fast enough that
 * a genuinely stuck index does not hang a session. CHECK_FOR_INTERRUPTS so
 * the wait stays cancellable.
 */
static void
persist_load_backoff(int attempt)
{
    long usec = 1000L << Min(attempt - 1, 6);   /* 1ms .. 64ms */

    CHECK_FOR_INTERRUPTS();
    pg_usleep(usec);
    CHECK_FOR_INTERRUPTS();
}

BiscuitIndex *
biscuit_persist_load(Relation index)
{
    Oid            indexoid = RelationGetRelid(index);
    int            natts    = index->rd_index->indnatts;
    MemoryContext  oldcontext = CurrentMemoryContext;
    instr_time     diag_start;   /* DIAGNOSTIC ONLY */
    volatile int   attempt;

    biscuit_diag_decode_count = 0;   /* DIAGNOSTIC ONLY */
    INSTR_TIME_SET_CURRENT(diag_start);

    for (attempt = 1; attempt <= BISCUIT_LOAD_MAX_ATTEMPTS; attempt++)
    {
        BiscuitIndex   * volatile idx = NULL;
        BiscuitDirEntry header_entry;
        uint64          drains_before, drains_after;
        BlockNumber     draining_before, draining_after;
        volatile bool   caught_error = false;
        ErrorData      * volatile edata = NULL;

        biscuit_pendlog_drain_state(index, &drains_before, &draining_before);

        if (!biscuit_dir_find(index, BISCUIT_DIR_COL_SINGLETON, false, BISCUIT_DIR_KIND_HEADER,
                               -1, -1, &header_entry, NULL))
        {
            /*
             * CONCURRENCY FIX, ROUND 2 -- "HEADER not found" is not
             * definitive, and total_drains alone cannot tell you whether it
             * is.
             *
             * A drain (pendlog_drain_internal(), biscuit_pendlog.c)
             * restructures directory entries under its own serializing lock,
             * and biscuit_dir_find() walking that directory mid-restructure
             * can transiently miss an entry that has been there since CREATE
             * INDEX. biscuit_load_index() turns the resulting NULL straight
             * into the client-visible "no on-disk snapshot found for index"
             * ERROR -- which is how a perfectly healthy index fails an
             * ordinary INSERT under concurrent load.
             *
             * Round 1 added a total_drains before/after comparison here.
             * That closes the window at the START of a drain and nothing
             * else, because pendlog_detach() bumps total_drains ONCE, at the
             * start, and pendlog_clear_draining() does not bump it again at
             * the end. The counter is therefore CONSTANT for the whole
             * duration of the merge -- which is the long part, the part that
             * actually rewrites blobs and restructures entries. Any lookup
             * that both begins and ends inside one merge sees
             * drains_before == drains_after, concludes "steady, therefore
             * conclusive", and returns NULL. That is the remaining defect,
             * and it is the same blind spot PendLogSnapshotSlot's
             * built_at_draining field exists to document on the read side.
             *
             * pendlog_draining closes it: it is InvalidBlockNumber exactly
             * when no merge is in flight. So a miss is trusted only when the
             * counter held steady AND no merge was running at either end of
             * the lookup. Anything else is retried.
             *
             * Erring toward retry is the right asymmetry. A spurious retry
             * costs one directory walk on an index that is about to be
             * loaded anyway; a spurious NULL fails a user's write with a
             * message telling them to REINDEX a healthy index.
             */
            uint64      drains_after_hdr;
            BlockNumber draining_after_hdr;

            biscuit_pendlog_drain_state(index, &drains_after_hdr,
                                         &draining_after_hdr);

            if (drains_after_hdr != drains_before ||
                draining_after_hdr != draining_before ||
                draining_before != InvalidBlockNumber ||
                draining_after_hdr != InvalidBlockNumber)
            {
                elog(DEBUG1,
                     "biscuit: HEADER lookup for index %u overlapped a concurrent drain "
                     "(total_drains " UINT64_FORMAT " -> " UINT64_FORMAT ", draining %u -> %u), "
                     "retrying (attempt %d/%d)",
                     indexoid, drains_before, drains_after_hdr,
                     draining_before, draining_after_hdr,
                     attempt, BISCUIT_LOAD_MAX_ATTEMPTS);
                persist_load_backoff(attempt);
                continue;
            }

            /*
             * DIAGNOSTIC -- always log this exact NULL return.
             *
             * This is now the "genuinely nothing saved yet" case and nothing
             * else: the counter held steady and no merge was in flight at
             * either end. It should only be reachable before CREATE INDEX's
             * build has completed. If it fires against an index whose build
             * finished, that is a real anomaly and this line is the
             * breadcrumb -- including the attempt number, which distinguishes
             * "always missing" from "went missing".
             */
            elog(LOG,
                 "biscuit: HEADER not found for index %u on attempt %d/%d "
                 "(total_drains steady at " UINT64_FORMAT ", no drain in flight); "
                 "returning NULL (nothing saved yet if pre-build, otherwise investigate)",
                 indexoid, attempt, BISCUIT_LOAD_MAX_ATTEMPTS, drains_before);

            return NULL;   /* nothing saved yet -- genuinely absent */
        }

        MemoryContextSwitchTo(CacheMemoryContext);

        PG_TRY();
        {
            PCur    hcur;
            char   *hdata;
            uint32  hlen;
            int32   num_columns;
            int32   raw_capacity;
            uint64  live_gen = 0;
            bool    have_live_gen = false;
            int     header_records;

            /* HEADER lives on a single in-place page now, not a blob chain --
             * see biscuit_common.h's "Field repurposing" comment. */
            biscuit_rowstore_header_read(index, header_entry.blob_head, &hdata, &hlen);
            pcur_init(&hcur, hdata, hlen);

            idx = (BiscuitIndex *) palloc0(sizeof(BiscuitIndex));

            idx->num_records = pcur_get_i32(&hcur);
            /*
             * The pre-reconciliation header value. Kept only for the
             * diagnostic below; it is deliberately NOT used to bound
             * anything. It is a claim high-water mark that an unrelated
             * committer may have raised past what is durably written -- see
             * biscuit_persist_save_row_identity()'s non-regressing clamp, and
             * the hole accounting after the tids read for what treating it as
             * a durability floor cost.
             */
            header_records    = idx->num_records;
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
             * SLOT-COUNT RECONCILIATION -- do not remove.
             *
             * There are two durable record counts, and the HEADER's is not
             * the authoritative one:
             *
             *   - meta->num_records is advanced under the metapage's
             *     exclusive buffer lock by biscuit_claim_new_slot()
             *     (biscuit_index.c), WAL-logged at the moment of the claim,
             *     and deliberately non-transactional. It is a monotonically
             *     non-decreasing high-water mark of every slot ever handed
             *     out by any backend.
             *
             *   - The HEADER's num_records is this backend's in-memory
             *     idx->num_records, serialized once per transaction at
             *     pre-commit by biscuit_persist_save_row_identity(). It can
             *     legitimately lag the metapage, and it can *regress*: the
             *     UPDATE path (found_existing) and biscuit_bulkdelete()
             *     both mark the index dirty without ever calling
             *     biscuit_claim_new_slot(), so a backend holding a stale
             *     idx->num_records rewrites the header with its own lower
             *     value. An aborted insert leaks its claimed slot for the
             *     same reason, and biscuit_flush_dirty_row_identity() skips
             *     the flush entirely when the cache entry was evicted
             *     mid-transaction.
             *
             * Loading num_records from the header alone therefore truncates
             * the index at whatever value the header happens to hold --
             * typically the CREATE INDEX row count. Every slot above that
             * point stayed at its palloc0 zero in idx->tids below (block 0,
             * offset 0 -- note this is NOT ItemPointerSetInvalid's
             * 0xFFFFFFFF, which is how the two are told apart in a dump),
             * even though biscuit_persist_row_identity_write_record() had
             * durably written the real TID for it at insert time. Reads then
             * silently lost every post-build row, because every read path
             * builds its universe as the range [0, idx->num_records).
             *
             * biscuit_write_metadata_to_disk() already takes
             * Max(meta->num_records, idx->num_records) for exactly this
             * reason; the header write and this load path did not. Take the
             * metapage's value whenever it is higher, and widen capacity to
             * match so tids / data_cache / data_cache_lower stay exactly
             * `capacity` long -- the invariant
             * biscuit_ensure_slot_capacity() and
             * biscuit_persist_load_strcache() both depend on.
             *
             * If the TID page directory really does not cover the reconciled
             * count, biscuit_rowstore_tid_read_all() raises "tid
             * page-directory has fewer slots than num_records" below. A loud
             * error there is the correct outcome and is strictly better than
             * the zero-filled tail this reconciliation replaces.
             *
             * GEN CONSISTENCY -- this same read is also where idx->gen gets
             * its value (see the bottom of this function). num_records
             * here is fixed as of *this* metapage read: nothing below this
             * point re-reads the metapage to widen num_records further, no
             * matter how long the rest of the load (every bitmap, every
             * STRCACHE column) takes to decode. If idx->gen were instead
             * taken from a *second*, later metapage read after that slow
             * decode, it would carry every gen bump any backend committed
             * during the decode -- including bumps from rows this
             * num_records reconciliation never saw -- while idx->num_records
             * stayed pinned to this earlier, smaller snapshot. That
             * combination is self-defeating: biscuit_get_current_index()'s
             * staleness check is disk_gen <= idx->gen, so an idx->gen that
             * has already raced ahead of what idx->num_records actually
             * covers makes every later commit's gen bump look like it
             * happened before this load, permanently suppressing the reload
             * that would pick those rows up. That is exactly how a cold
             * load can freeze num_records at (or below) the build-time
             * count while gen keeps climbing underneath it -- see
             * biscuit_get_current_index()'s comment for the read-side half
             * of this contract. Capturing live_gen here, in the same call
             * that fixes num_records, keeps the two in lockstep: any commit
             * this load did not incorporate -- including one that resolves
             * a trimmed hole below -- necessarily bumped meta->gen after
             * this read, so disk_gen > idx->gen will correctly hold on the
             * next access and trigger a fresh reload.
             */
            {
                int    meta_records = 0;
                int    meta_columns, meta_max_len;

                have_live_gen = biscuit_read_metadata_from_disk(index, &meta_records,
                                                                 &meta_columns, &meta_max_len,
                                                                 &live_gen);

                if (have_live_gen && meta_records > idx->num_records)
                {
                    elog(DEBUG1,
                         "biscuit: index %u header records %d behind metapage %d; "
                         "reconciling to metapage",
                         indexoid, idx->num_records, meta_records);

                    idx->num_records = meta_records;

                    while (idx->capacity < idx->num_records)
                        idx->capacity *= 2;
                }
            }

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
            idx->tids = (ItemPointerData *) palloc0(idx->capacity * sizeof(ItemPointerData));
            if (idx->num_records > 0)
            {
                BiscuitDirEntry tids_entry;

                if (!biscuit_dir_find(index, BISCUIT_DIR_COL_SINGLETON, false, BISCUIT_DIR_KIND_TIDS,
                                       -1, -1, &tids_entry, NULL) ||
                    tids_entry.blob_head == InvalidBlockNumber)
                    ereport(ERROR, (errmsg("biscuit: missing tid array for index %u", indexoid)));

                /*
                 * tids_entry.blob_head is the TIDS page-directory root now, not
                 * a blob chain -- see biscuit_common.h's "Field repurposing"
                 * comment. The old explicit "tlen != num_records * sizeof(...)"
                 * check is subsumed by biscuit_rowstore_tid_read_all(), which
                 * WARNs and leaves the tail zeroed if the directory doesn't
                 * cover num_records slots (it used to ERROR; see the comment
                 * there for why a shortfall became expected once num_records
                 * started being reconciled against the metapage's
                 * non-transactional slot high-water mark).
                 *
                 * Note idx->tids is palloc0'd (was plain palloc): the read fills
                 * exactly [0, num_records), and the tail [num_records, capacity)
                 * must start zeroed the same way a freshly-built index's does,
                 * since the steady-state insert path writes into that tail
                 * without initializing it first.
                 */
                biscuit_rowstore_tid_read_all(index, tids_entry.blob_head,
                                               (uint32) idx->num_records, idx->tids);

                /*
                 * CLAIMED-BUT-NOT-DURABLE SLOTS ARE COUNTED AND REPORTED,
                 * NOT TRIMMED AWAY.
                 *
                 * The state itself is real and expected.
                 * biscuit_claim_new_slot() (biscuit_index.c) publishes
                 * meta->num_records the instant a slot is claimed, under the
                 * metapage lock, and releases that lock before the claiming
                 * backend writes the slot's TID
                 * (biscuit_persist_row_identity_write_record(), later in the
                 * same biscuit_insert()). A load landing in that window
                 * reconciles idx->num_records up to include the slot and
                 * reads it back as the palloc0 zero-fill. Aborted inserters
                 * leak slots in exactly the same shape, permanently and by
                 * documented design.
                 *
                 * The zero-fill is unambiguous: ItemPointerSetInvalid()
                 * writes block 0xFFFFFFFF, never block 0, so (block 0,
                 * offset 0) can only be a slot that was never written.
                 *
                 * WHAT WAS WRONG WITH TRIMMING. This block used to scan
                 * forward from header_records and, on the first such hole,
                 * set idx->num_records = i -- discarding every slot above it.
                 * Two independent defects:
                 *
                 *   1. Holes are INTERIOR, not just trailing. An aborted
                 *      insert leaks its slot forever, so a hole at slot
                 *      41000 of a 58000-row index truncated this backend to
                 *      41000 rows -- silently, permanently, and with exactly
                 *      the "collapses to a smaller count" signature this
                 *      family of bugs keeps producing. Trimming at the first
                 *      hole is only sound if holes can only ever be at the
                 *      tail, and the design guarantees the opposite.
                 *
                 *   2. It could not see the holes that mattered anyway. The
                 *      scan started at header_records on the theory that
                 *      [0, header_records) was written by a committed
                 *      transaction and therefore known-populated. That is
                 *      false: biscuit_persist_save_row_identity() raises
                 *      header_records to meta->num_records unconditionally,
                 *      for ANY dirty-marking transaction -- an UPDATE in
                 *      place, a bulkdelete -- none of which claimed a slot or
                 *      know anything about whether the slot that pushed the
                 *      metapage up is durable. So an unrelated committer
                 *      bakes another backend's in-flight claim into
                 *      header_records, and the hole is below the scan floor
                 *      from then on, for every future loader.
                 *
                 * THE HOLE DOES NOT NEED TRIMMING. Nothing downstream is
                 * entitled to assume every slot in [0, num_records) has a
                 * TID -- num_records is a claim high-water mark, which is
                 * what biscuit_claim_new_slot() says it is. The one consumer
                 * that treated a hole as corruption,
                 * biscuit_collect_sorted_tids_single() (biscuit_tid.c), now
                 * recognises the zero-fill encoding and skips those slots,
                 * which is the CORRECT answer rather than a tolerated one: a
                 * slot whose TID is not durable belongs either to a
                 * transaction that has not committed (invisible to this
                 * snapshot regardless) or to one that aborted (invisible
                 * forever). Skipping it is not an undercount.
                 *
                 * So: count them, report them, change nothing. The count is
                 * worth having because a large or growing number is a real
                 * signal -- claim/write windows are per-insert and transient,
                 * leaked slots accumulate only as fast as inserts abort --
                 * whereas the trim it replaces destroyed the evidence along
                 * with the rows.
                 */
                {
                    int holes       = 0;
                    int first_hole  = -1;
                    int i;

                    for (i = 0; i < idx->num_records; i++)
                    {
                        if (ItemPointerGetBlockNumberNoCheck(&idx->tids[i]) == 0 &&
                            ItemPointerGetOffsetNumberNoCheck(&idx->tids[i]) == 0)
                        {
                            if (first_hole < 0)
                                first_hole = i;
                            holes++;
                        }
                    }

                    if (holes > 0)
                        elog(DEBUG1,
                             "biscuit: index %u loaded with %d slot(s) claimed but not "
                             "durably written (first %d, of %d; header claimed %d); these "
                             "are skipped at scan time and resolve on this backend's next "
                             "reload",
                             indexoid, holes, first_hole, idx->num_records,
                             header_records);
                }
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

                    biscuit_persist_read_blob_retry(index,
                                                     BISCUIT_DIR_COL_SINGLETON, false,
                                                     BISCUIT_DIR_KIND_FREELIST, -1, -1,
                                                     fl_entry.blob_head, &fdata, &flen);
                    idx->free_count    = (int) (flen / sizeof(uint32_t));
                    idx->free_capacity = Max(idx->free_count, 64);
                    idx->free_list     = (uint32_t *) palloc(idx->free_capacity * sizeof(uint32_t));
                    if (idx->free_count > 0 && fdata != NULL)
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
                idx->data_cache       = biscuit_persist_load_strcache(index, BISCUIT_DIR_COL_LEGACY, false, idx->num_records, idx->capacity);
                idx->data_cache_lower = biscuit_persist_load_strcache(index, BISCUIT_DIR_COL_LEGACY, true, idx->num_records, idx->capacity);

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
                        biscuit_persist_load_strcache(index, col, false, idx->num_records, idx->capacity);
                    idx->column_data_cache_lower[col] =
                        biscuit_persist_load_strcache(index, col, true, idx->num_records, idx->capacity);
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
             * the metapage's gen since idx->gen is the in-memory generation
             * counter consulted elsewhere (e.g. cache invalidation
             * bookkeeping); gen_at_last_snapshot is kept in lockstep with it
             * here purely for field-consistency, not because anything still
             * compares the two to decide whether to re-save.
             *
             * Deliberately NOT a fresh biscuit_read_metadata_from_disk() call
             * here. live_gen is the value captured by the SLOT-COUNT
             * RECONCILIATION read above, at the same point idx->num_records
             * was fixed -- see the GEN CONSISTENCY comment there for why a
             * second, later read (which is what used to happen here) breaks
             * the invariant biscuit_get_current_index() depends on to detect
             * staleness at all.
             */
            if (have_live_gen)
            {
                idx->gen                  = live_gen;
                idx->gen_at_last_snapshot = live_gen;
            }
        }
        PG_CATCH();
        {
            /*
             * Order matters here: switch back to the caller's (long-lived)
             * context FIRST, then CopyErrorData() so the copy is allocated in
             * a context that survives FlushErrorState(), then flush. Copying
             * while still notionally "inside" the failed load and freeing the
             * copy only after tearing down the error state was producing a
             * wild-pointer free that segfaulted in FreeErrorData().
             *
             * Do NOT decide here whether this is real corruption -- that
             * depends on whether total_drains moved during this attempt,
             * checked below once we're back at a safe point. idx itself
             * (in CacheMemoryContext, which we don't reset) is simply
             * abandoned; nothing outside this function has seen the
             * pointer yet.
             */
            MemoryContextSwitchTo(oldcontext);
            edata = CopyErrorData();
            FlushErrorState();
            caught_error = true;
        }
        PG_END_TRY();

        if (!caught_error)
            MemoryContextSwitchTo(oldcontext);

        biscuit_pendlog_drain_state(index, &drains_after, &draining_after);

        if (drains_before != drains_after ||
            draining_before != draining_after ||
            draining_before != InvalidBlockNumber ||
            draining_after != InvalidBlockNumber)
        {
            /*
             * A drain was, or became, in flight while this attempt was
             * running. Whether or not this attempt happened to throw, its
             * view of the directory cannot be trusted -- retry from scratch
             * rather than either believing a read that got lucky or
             * discarding one that got unlucky.
             *
             * The draining checks matter as much as the counter here, for
             * the reason spelled out at the HEADER-miss guard above:
             * total_drains does not move during the merge, so a counter-only
             * comparison declares an attempt that ran entirely inside one
             * merge "clean" and then treats whatever it threw as genuine
             * corruption. That is the path that produced "no on-disk snapshot
             * found" on a healthy index.
             */
            if (caught_error)
                FreeErrorData(edata);
            elog(DEBUG1,
                 "biscuit: cold load for index %u overlapped a concurrent drain (total_drains "
                 UINT64_FORMAT " -> " UINT64_FORMAT ", draining %u -> %u), retrying (attempt %d/%d)",
                 indexoid, drains_before, drains_after,
                 draining_before, draining_after,
                 attempt, BISCUIT_LOAD_MAX_ATTEMPTS);
            persist_load_backoff(attempt);
            continue;
        }

        if (caught_error)
        {
            /*
             * No drain touched this attempt at either end and it still
             * failed: this is genuine corrupt/inconsistent directory state,
             * not a torn read. biscuit_load_index() treats a NULL return as
             * "nothing readable" and raises an ERROR with a REINDEX hint --
             * there is no from-heap rebuild fallback to defer to anymore.
             */
            elog(WARNING, "biscuit: discarding unreadable directory-backed state for index %u (%s)",
                 indexoid, edata->message);
            FreeErrorData(edata);
            return NULL;
        }

        elog(DEBUG1, "biscuit: loaded directory-backed state for index %u (%d records, gen " UINT64_FORMAT ")",
             indexoid, idx->num_records, idx->gen);

        return idx;
    }

    /*
     * Every attempt raced a drain.
     *
     * This used to warn and return NULL, which biscuit_load_index() turns
     * into ERRCODE_DATA_CORRUPTED with a REINDEX hint. That verdict is
     * exactly backwards for this path: the index is fine, this backend just
     * never got an uncontended look at it. Telling an operator to REINDEX a
     * healthy index because their write load is busy is worse than telling
     * them nothing.
     *
     * Raise the retryable error here instead, where the cause is known,
     * rather than letting a NULL return be reinterpreted as corruption three
     * frames up. ERRCODE_T_R_SERIALIZATION_FAILURE is what a client's retry
     * logic already understands, and it is what this genuinely is.
     */
    ereport(ERROR,
            (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
             errmsg("biscuit: could not obtain a stable read of index \"%s\"",
                    RelationGetRelationName(index)),
             errdetail("All %d load attempts overlapped a concurrent pending-log drain.",
                       BISCUIT_LOAD_MAX_ATTEMPTS),
             errhint("Retry the statement. This indicates drain contention, not "
                     "corruption -- the index does not need REINDEX. Persistent "
                     "failures suggest biscuit.delta_compaction_slots is too low "
                     "for this write rate.")));

    return NULL;   /* unreachable; keeps the compiler quiet */
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

    /*
     * TIDS/STRCACHE/HEADER don't own a plain compacted-blob chain -- their
     * blob_head/strheap_* fields address the in-place row-identity
     * structures instead (see BiscuitDirEntry's per-kind field table in
     * biscuit_common.h), so freeing them as if they were blob chains would
     * retire only the *directory* pages and orphan every
     * TIDSLOT/STRPTR/STRHEAP page hanging off them. Route each kind to the
     * teardown helper that actually knows its page graph.
     */
    switch (entry->kind)
    {
        case BISCUIT_DIR_KIND_TIDS:
            biscuit_rowstore_free_tid_chain(st->index, entry->blob_head);
            return;

        case BISCUIT_DIR_KIND_STRCACHE:
            biscuit_rowstore_free_str_chains(st->index, entry->blob_head,
                                              entry->strheap_head);
            return;

        case BISCUIT_DIR_KIND_HEADER:
            biscuit_rowstore_free_header(st->index, entry->blob_head);
            return;

        default:
            break;
    }

    /*
     * Everything else (the bitmap kinds and FREELIST) owns exactly one
     * compacted-blob chain and no value heap. There is no pending chain to
     * free: undrained deltas for these kinds live in the shared log, which
     * biscuit_pendlog_free_chain() retires separately as a whole.
     */
    if (entry->blob_head != InvalidBlockNumber)
        biscuit_page_free_blob(st->index, entry->blob_head);
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

        /* Retire the shared pending log too -- biscuit_dir_drop_all() only
     * knows about BISCUIT_PAGE_DIR pages, and the log is rooted in the
     * metapage, not in any directory entry. */
    biscuit_pendlog_free_chain(index);

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
