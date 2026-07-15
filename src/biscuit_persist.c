/*
 * biscuit_persist.c
 * Best-effort disk snapshot persistence for BiscuitIndex.
 *
 * Design
 * ------
 * One flat file per index at:
 *     $PGDATA/pg_biscuit/<dboid>_<indexoid>.bsc
 *
 * We deliberately do NOT use index relation pages / GenericXLog for
 * this -- these snapshots can be tens to hundreds of MB (every roaring
 * bitmap + every string cache) and are pure derived data, not data
 * that needs to participate in WAL replay or crash recovery. A plain
 * OS file, written with a temp-file-then-rename swap, gives us:
 *
 *   - no interaction with the buffer manager / WAL for bulk bitmap data
 *   - atomic replacement (a crash mid-write never leaves a half-written
 *     file where the real name is)
 *   - trivial invalidation: just unlink it, biscuit_load_index() falls
 *     back to a normal from-heap rebuild if the file is absent/corrupt
 *
 * Bitmap serialization is format-per-build: on HAVE_ROARING builds we
 * use CRoaring's own native "portable" format (a bulk memcpy-shaped
 * dump of its compressed containers -- no per-value reconstruction
 * loop on load). On fallback-bitset builds we dump the block array
 * directly, same bulk-copy shape, just uncompressed. Either way,
 * loading a bitmap is a fread() into a buffer plus one bulk parse
 * call, not millions of individual add() calls, which is what keeps
 * snapshot load fast even for large indexes.
 *
 * Integrity: every snapshot ends with a trailing CRC32C checksum
 * covering all preceding bytes (header + body). The checksum is
 * accumulated incrementally inside wput()/rget() -- the same single
 * pass that already writes/reads every field -- so verifying a
 * multi-hundred-MB snapshot costs no extra I/O and no separate
 * whole-file hashing pass; it just falls out of the write/read we were
 * doing anyway. CRC32C (not a cryptographic hash) is used deliberately:
 * it is SIMD-accelerated by Postgres's own port/pg_crc32c.h (the same
 * routine WAL and pg_control use) and is more than sufficient to catch
 * the failure modes we actually care about here -- a crash mid-write
 * or on-disk bit rot -- as opposed to a malicious adversary. On load,
 * a checksum mismatch is treated exactly like a truncated/corrupt
 * snapshot: discard and fall back to a full from-heap rebuild.
 * BISCUIT_PERSIST_VERSION should be bumped whenever the on-disk format
 * changes, so an incompatible snapshot is cleanly rejected (and
 * rebuilt from the heap) rather than misread.
 *
 * Consistency model (per user requirements: read-mostly, best-effort):
 *   - Snapshot is written at the end of biscuit_build() (CREATE INDEX /
 *     first cold load) and should also be re-written after mutating
 *     paths (biscuit_insert / biscuit_bulkdelete) if write volume ever
 *     becomes non-trivial -- see biscuit_index.c call sites.
 *   - On load, we sanity-check the header (magic/version), that
 *     num_records is non-negative and columns match the index's
 *     current attribute count, that the trailing CRC32C checksum
 *     matches the bytes actually read, AND that the generation
 *     recorded in the snapshot header matches the live generation in
 *     the index's metapage (BiscuitMetaPageData.gen). A mismatch on
 *     any of these means the snapshot is stale or unusable, so it is
 *     discarded (WARNING) and the caller falls back to a normal
 *     rebuild -- we never load data known to be stale or corrupt.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_cache.h"
#include "biscuit_persist.h"
#include "biscuit_index.h"   /* for biscuit_read_metadata_from_disk() */

#include "port/pg_crc32c.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>

#define BISCUIT_PERSIST_MAGIC    (BISCUIT_MAGIC ^ 0x01)
#define BISCUIT_PERSIST_VERSION  1  /* bump if the on-disk format changes */
#define BISCUIT_PERSIST_DIR      "pg_biscuit"
#define BISCUIT_PERSIST_HEADER_RESERVED  4  /* uint32 slots reserved for future metadata */

/* ================================================================
 * Path helpers
 * ================================================================ */

static void
biscuit_persist_path(Oid indexoid, char *buf, size_t bufsz)
{
    snprintf(buf, bufsz, "%s/%s/%u_%u.bsc",
             DataDir, BISCUIT_PERSIST_DIR,
             MyDatabaseId, indexoid);
}

static void
biscuit_persist_tmp_path(Oid indexoid, char *buf, size_t bufsz)
{
    snprintf(buf, bufsz, "%s/%s/%u_%u.bsc.tmp%d",
             DataDir, BISCUIT_PERSIST_DIR,
             MyDatabaseId, indexoid, MyProcPid);
}

static bool
biscuit_persist_ensure_dir(void)
{
    char dirpath[MAXPGPATH];

    snprintf(dirpath, sizeof(dirpath), "%s/%s", DataDir, BISCUIT_PERSIST_DIR);

    if (mkdir(dirpath, S_IRWXU) != 0 && errno != EEXIST)
    {
        elog(WARNING, "biscuit: could not create snapshot directory \"%s\": %m",
             dirpath);
        return false;
    }
    return true;
}

/* ================================================================
 * Low-level buffered write/read helpers.
 * All writes go through these so a short write/read is always caught
 * rather than silently producing a truncated/garbage snapshot.
 *
 * Both streams also carry a running CRC32C (crc) that is updated on
 * every successful write/read inside wput()/rget(). This is the
 * mechanism that makes the trailing checksum "free": since every
 * field write/read already funnels through these two functions, the
 * checksum is accumulated as a side effect of I/O we are doing
 * anyway, rather than requiring a dedicated full-file hashing pass
 * before write or after load. Callers that need the final value call
 * FIN_CRC32C() on a copy of the accumulator (see biscuit_persist_save
 * / biscuit_persist_load) -- the trailer write/read itself must not
 * be included, so it happens via the same wput_u32()/rget_u32() calls
 * but its contribution to the accumulator is simply never inspected.
 * ================================================================ */

typedef struct
{
    FILE      *fp;
    bool       error;
    pg_crc32c  crc;
} WStream;

typedef struct
{
    FILE      *fp;
    bool       error;
    pg_crc32c  crc;
} RStream;

static void
wput(WStream *w, const void *data, size_t len)
{
    if (w->error || len == 0)
        return;
    if (fwrite(data, 1, len, w->fp) != len)
    {
        w->error = true;
        return;
    }
    COMP_CRC32C(w->crc, data, len);
}

static void
wput_i32(WStream *w, int32 v)  { wput(w, &v, sizeof(v)); }
static void
wput_i64(WStream *w, int64 v)  { wput(w, &v, sizeof(v)); }
static void
wput_u32(WStream *w, uint32 v) { wput(w, &v, sizeof(v)); }

/* length-prefixed string; len = -1 encodes NULL */
static void
wput_str(WStream *w, const char *s)
{
    if (!s)
    {
        wput_i32(w, -1);
        return;
    }
    {
        int32 len = (int32) strlen(s);
        wput_i32(w, len);
        wput(w, s, len);
    }
}

static bool
rget(RStream *r, void *data, size_t len)
{
    if (r->error || len == 0)
        return len == 0;
    if (fread(data, 1, len, r->fp) != len)
    {
        r->error = true;
        return false;
    }
    COMP_CRC32C(r->crc, data, len);
    return true;
}

static int32
rget_i32(RStream *r) { int32 v = 0; rget(r, &v, sizeof(v)); return v; }
static int64
rget_i64(RStream *r) { int64 v = 0; rget(r, &v, sizeof(v)); return v; }
static uint32
rget_u32(RStream *r) { uint32 v = 0; rget(r, &v, sizeof(v)); return v; }

/* returns palloc'd string, or NULL; *out_null set accordingly */
static char *
rget_str(RStream *r, MemoryContext cxt)
{
    int32 len = rget_i32(r);
    char *s;
    MemoryContext old;

    if (r->error || len < 0)
        return NULL;

    old = MemoryContextSwitchTo(cxt);
    s = (char *) palloc(len + 1);
    MemoryContextSwitchTo(old);

    if (!rget(r, s, len))
    {
        pfree(s);
        return NULL;
    }
    s[len] = '\0';
    return s;
}

/* ================================================================
 * Bitmap (de)serialization -- engine-agnostic: count + uint32 values
 * ================================================================ */

#ifdef HAVE_ROARING

/*
 * Native CRoaring "portable" format: a direct bulk memcpy-shaped dump of
 * the bitmap's own compressed containers.  This is what actually makes
 * deserialize fast -- there is no per-value reconstruction loop at all,
 * unlike the count+values format this replaces (which forced a
 * biscuit_roaring_add() call, and full container-selection logic, for
 * every single set bit -- the source of the 8s load time).
 */
static void
wput_bitmap(WStream *w, const RoaringBitmap *rb)
{
    if (!rb)
    {
        wput_i64(w, -1);   /* NULL marker */
        return;
    }
    {
        size_t  sz  = roaring_bitmap_portable_size_in_bytes(rb);
        char   *buf = (char *) palloc(sz);

        roaring_bitmap_portable_serialize(rb, buf);
        wput_i64(w, (int64) sz);
        wput(w, buf, sz);
        pfree(buf);
    }
}

static RoaringBitmap *
rget_bitmap(RStream *r)
{
    int64 sz = rget_i64(r);
    char *buf;
    RoaringBitmap *rb;

    if (r->error || sz < 0)
        return NULL;

    buf = (char *) palloc(sz);
    if (!rget(r, buf, sz))
    {
        pfree(buf);
        return NULL;
    }

    rb = roaring_bitmap_portable_deserialize_safe(buf, sz);
    pfree(buf);
    return rb;   /* NULL on corrupt input -- caller treats as load failure */
}

#else  /* !HAVE_ROARING -- fallback bitset */

/*
 * The fallback bitmap is already just a flat uint64_t[] -- serialize it
 * as a raw block dump rather than expanding to individual set bits and
 * calling add() in a loop.  Same bulk-memcpy shape as the roaring path
 * above, just without compression.
 */
static void
wput_bitmap(WStream *w, const RoaringBitmap *rb)
{
    if (!rb)
    {
        wput_i32(w, -1);   /* NULL marker */
        return;
    }
    wput_i32(w, rb->num_blocks);
    if (rb->num_blocks > 0)
        wput(w, rb->blocks, rb->num_blocks * sizeof(uint64_t));
}

static RoaringBitmap *
rget_bitmap(RStream *r)
{
    int32 num_blocks = rget_i32(r);
    RoaringBitmap *rb;

    if (r->error || num_blocks < 0)
        return NULL;

    rb = (RoaringBitmap *) palloc0(sizeof(RoaringBitmap));
    rb->num_blocks = num_blocks;
    rb->capacity   = num_blocks;
    rb->blocks     = (uint64_t *) palloc0(Max(num_blocks, 1) * sizeof(uint64_t));

    if (num_blocks > 0 && !rget(r, rb->blocks, num_blocks * sizeof(uint64_t)))
    {
        pfree(rb->blocks);
        pfree(rb);
        return NULL;
    }
    return rb;
}

#endif /* HAVE_ROARING */

/* ================================================================
 * CharIndex (de)serialization
 * ================================================================ */

static void
wput_charindex(WStream *w, const CharIndex *ci)
{
    int i;
    wput_i32(w, ci->count);
    for (i = 0; i < ci->count; i++)
    {
        wput_i32(w, ci->entries[i].pos);
        wput_bitmap(w, ci->entries[i].bitmap);
    }
}

/* ci must already have entries/capacity allocated (mirrors the
 * skeleton-allocation pattern used elsewhere in this codebase) */
static bool
rget_charindex(RStream *r, CharIndex *ci)
{
    int count = rget_i32(r);
    int i;

    if (r->error || count < 0)
        return false;

    if (count > ci->capacity)
    {
        ci->entries  = (PosEntry *) repalloc(ci->entries, count * sizeof(PosEntry));
        ci->capacity = count;
    }
    ci->count = count;

    for (i = 0; i < count; i++)
    {
        ci->entries[i].pos    = rget_i32(r);
        ci->entries[i].bitmap = rget_bitmap(r);
        if (r->error)
            return false;
    }
    return true;
}

/* ================================================================
 * length_bitmaps[] / length_ge_bitmaps[] array (de)serialization
 * ================================================================ */

static void
wput_bitmap_array(WStream *w, RoaringBitmap **arr, int n)
{
    int i;
    wput_i32(w, arr ? n : -1);
    if (!arr)
        return;
    for (i = 0; i < n; i++)
        wput_bitmap(w, arr[i]);
}

static RoaringBitmap **
rget_bitmap_array(RStream *r, int *out_n, MemoryContext cxt)
{
    int n = rget_i32(r);
    RoaringBitmap **arr;
    int i;
    MemoryContext old;

    if (r->error || n < 0)
    {
        *out_n = 0;
        return NULL;
    }

    old = MemoryContextSwitchTo(cxt);
    arr = (RoaringBitmap **) palloc0(n * sizeof(RoaringBitmap *));
    MemoryContextSwitchTo(old);

    for (i = 0; i < n; i++)
    {
        arr[i] = rget_bitmap(r);
        if (r->error)
        {
            *out_n = 0;
            return NULL;
        }
    }
    *out_n = n;
    return arr;
}

/* ================================================================
 * SAVE
 * ================================================================ */

void
biscuit_persist_save(Oid indexoid, BiscuitIndex *idx)
{
    char     finalpath[MAXPGPATH];
    char     tmppath[MAXPGPATH];
    WStream  w;
    int      ch, i;

    if (!biscuit_persist_ensure_dir())
        return;

    biscuit_persist_path(indexoid, finalpath, sizeof(finalpath));
    biscuit_persist_tmp_path(indexoid, tmppath, sizeof(tmppath));

    w.fp    = fopen(tmppath, "wb");
    w.error = false;
    INIT_CRC32C(w.crc);
    if (!w.fp)
    {
        elog(WARNING, "biscuit: could not create snapshot file \"%s\": %m", tmppath);
        return;
    }

    /* ---- header ---- */
    wput_u32(&w, BISCUIT_PERSIST_MAGIC);
    wput_u32(&w, BISCUIT_PERSIST_VERSION);
    /*
     * Generation this snapshot represents.  Written exactly once, here,
     * as part of the shared header path -- both the single-column and
     * multi-column serialization bodies below are pure continuations of
     * this same write sequence and have no early-return that could skip
     * past it.
     */
    wput_i64(&w, (int64) idx->gen);
    /* Reserved for future header metadata; zero-filled so old readers
     * (there are none yet, but this keeps the pattern consistent with
     * the metapage's own reserved slots) ignore it safely. */
    {
        int resv;
        for (resv = 0; resv < BISCUIT_PERSIST_HEADER_RESERVED; resv++)
            wput_u32(&w, 0);
    }
    wput_i32(&w, idx->num_columns);
    wput_i32(&w, idx->num_records);
    wput_i32(&w, idx->capacity);
    wput_i32(&w, idx->max_len);
    wput_i32(&w, idx->max_length_legacy);
    wput_i32(&w, idx->max_length_lower);
    wput_i64(&w, idx->insert_count);
    wput_i64(&w, idx->update_count);
    wput_i64(&w, idx->delete_count);
    wput_i32(&w, idx->tombstone_count);

    /* ---- tids ---- */
    wput(&w, idx->tids, idx->num_records * sizeof(ItemPointerData));

    /* ---- tombstones ---- */
    wput_bitmap(&w, idx->tombstones);

    /* ---- free list ---- */
    wput_i32(&w, idx->free_count);
    if (idx->free_count > 0)
        wput(&w, idx->free_list, idx->free_count * sizeof(uint32_t));

    if (idx->num_columns == 1)
    {
        /* single-column (legacy) layout */
        for (i = 0; i < idx->num_records; i++)
        {
            wput_str(&w, idx->data_cache[i]);
            wput_str(&w, idx->data_cache_lower[i]);
        }

        for (ch = 0; ch < CHAR_RANGE; ch++)
        {
            wput_charindex(&w, &idx->pos_idx_legacy[ch]);
            wput_charindex(&w, &idx->neg_idx_legacy[ch]);
            wput_bitmap(&w, idx->char_cache_legacy[ch]);

            wput_charindex(&w, &idx->pos_idx_lower[ch]);
            wput_charindex(&w, &idx->neg_idx_lower[ch]);
            wput_bitmap(&w, idx->char_cache_lower[ch]);
        }

        wput_bitmap_array(&w, idx->length_bitmaps_legacy, idx->max_length_legacy);
        wput_bitmap_array(&w, idx->length_ge_bitmaps_legacy, idx->max_length_legacy);
        wput_bitmap_array(&w, idx->length_bitmaps_lower, idx->max_length_lower);
        wput_bitmap_array(&w, idx->length_ge_bitmaps_lower, idx->max_length_lower);
    }
    else
    {
        /* multi-column layout */
        int col;

        for (col = 0; col < idx->num_columns; col++)
        {
            wput_u32(&w, idx->column_types[col]);
            for (i = 0; i < idx->num_records; i++)
            {
                wput_str(&w, idx->column_data_cache[col][i]);
                wput_str(&w, idx->column_data_cache_lower[col][i]);
            }
        }

        for (col = 0; col < idx->num_columns; col++)
        {
            ColumnIndex *cidx = &idx->column_indices[col];

            wput_i32(&w, cidx->max_length);
            wput_i32(&w, cidx->max_length_lower);

            for (ch = 0; ch < CHAR_RANGE; ch++)
            {
                wput_charindex(&w, &cidx->pos_idx[ch]);
                wput_charindex(&w, &cidx->neg_idx[ch]);
                wput_bitmap(&w, cidx->char_cache[ch]);

                wput_charindex(&w, &cidx->pos_idx_lower[ch]);
                wput_charindex(&w, &cidx->neg_idx_lower[ch]);
                wput_bitmap(&w, cidx->char_cache_lower[ch]);
            }

            wput_bitmap_array(&w, cidx->length_bitmaps, cidx->max_length);
            wput_bitmap_array(&w, cidx->length_ge_bitmaps, cidx->max_length);
            wput_bitmap_array(&w, cidx->length_bitmaps_lower, cidx->max_length_lower);
            wput_bitmap_array(&w, cidx->length_ge_bitmaps_lower, cidx->max_length_lower);
        }
    }

    /*
     * ---- trailing checksum ----
     * Finalize the CRC accumulated over every byte written so far (the
     * full header + body) and append it as the last 4 bytes of the
     * file. Finalizing into a local copy rather than w.crc itself means
     * the wput_u32() call below -- which, like every other write, feeds
     * back into w.crc -- can't corrupt the value we're about to persist;
     * we simply never look at w.crc again after this point.
     */
    {
        pg_crc32c final_crc = w.crc;

        FIN_CRC32C(final_crc);
        wput_u32(&w, (uint32) final_crc);
    }

    if (w.error || fflush(w.fp) != 0)
    {
        elog(WARNING, "biscuit: error writing snapshot \"%s\", discarding", tmppath);
        fclose(w.fp);
        unlink(tmppath);
        return;
    }

    /* fsync the temp file's contents before the atomic rename */
    if (fsync(fileno(w.fp)) != 0)
        elog(WARNING, "biscuit: fsync of snapshot \"%s\" failed: %m", tmppath);

    fclose(w.fp);

    if (rename(tmppath, finalpath) != 0)
    {
        elog(WARNING, "biscuit: could not install snapshot \"%s\": %m", finalpath);
        unlink(tmppath);
        return;
    }

    /*
     * Only record the snapshot as "up to date" once it has actually
     * landed on disk under its real name -- any earlier return above
     * (write error, fsync-visible failure via rename, etc.) leaves
     * gen_at_last_snapshot untouched so the next save attempt is not
     * skipped.
     */
    idx->gen_at_last_snapshot = idx->gen;

    elog(DEBUG1, "biscuit: wrote disk snapshot for index %u (%d records, gen " UINT64_FORMAT ")",
         indexoid, idx->num_records, idx->gen);
}

/* ================================================================
 * LOAD
 * ================================================================ */

BiscuitIndex *
biscuit_persist_load(Relation index)
{
    Oid            indexoid = RelationGetRelid(index);
    char           path[MAXPGPATH];
    RStream        r;
    BiscuitIndex  *idx = NULL;
    MemoryContext  oldcontext;
    uint32         magic, version;
    uint64         snapshot_gen;
    uint64         live_gen;
    bool           have_live_gen;
    int            reserved_slot;
    int            natts = index->rd_index->indnatts;
    int            ch, i;

    biscuit_persist_path(indexoid, path, sizeof(path));

    r.fp = fopen(path, "rb");
    r.error = false;
    INIT_CRC32C(r.crc);
    if (!r.fp)
        return NULL;   /* no snapshot -- normal, caller falls back */

    magic   = rget_u32(&r);
    version = rget_u32(&r);

    if (r.error || magic != BISCUIT_PERSIST_MAGIC || version != BISCUIT_PERSIST_VERSION)
    {
        elog(DEBUG1, "biscuit: snapshot \"%s\" missing/incompatible, ignoring", path);
        fclose(r.fp);
        return NULL;
    }

    snapshot_gen = (uint64) rget_i64(&r);
    for (reserved_slot = 0; reserved_slot < BISCUIT_PERSIST_HEADER_RESERVED; reserved_slot++)
        (void) rget_u32(&r);

    if (r.error)
    {
        elog(DEBUG1, "biscuit: snapshot \"%s\" truncated header, ignoring", path);
        fclose(r.fp);
        return NULL;
    }

    /*
     * The metapage's gen is the authoritative "current generation" --
     * if it differs from the generation this snapshot was taken at,
     * some insert/vacuum has landed since the snapshot was written and
     * it must not be loaded, even though its magic/version are fine.
     *
     * biscuit_read_metadata_from_disk() unconditionally dereferences its
     * num_records/num_columns/max_len out-params (only "gen" tolerates
     * NULL), so we must pass real storage for them even though we only
     * care about gen here.
     */
    {
        int unused_records, unused_columns, unused_max_len;

        have_live_gen = biscuit_read_metadata_from_disk(index,
                                                         &unused_records,
                                                         &unused_columns,
                                                         &unused_max_len,
                                                         &live_gen);
    }

    if (have_live_gen && live_gen != snapshot_gen)
    {
        elog(WARNING, "biscuit: snapshot \"%s\" is stale (snapshot gen "
                       UINT64_FORMAT ", current gen " UINT64_FORMAT "), "
                       "discarding and falling back to rebuild",
             path, snapshot_gen, live_gen);
        fclose(r.fp);
        return NULL;
    }

    oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

    PG_TRY();
    {
        int32 num_columns   = rget_i32(&r);
        int32 num_records   = rget_i32(&r);
        int32 capacity      = rget_i32(&r);

        if (r.error || num_columns != natts || num_records < 0 || capacity < num_records)
            ereport(ERROR,
                    (errmsg("biscuit: snapshot header mismatch for index %u, ignoring",
                            indexoid)));

        idx = (BiscuitIndex *) palloc0(sizeof(BiscuitIndex));
        idx->num_columns = num_columns;
        idx->num_records = num_records;
        idx->capacity    = Max(capacity, 1);

        /*
         * Case-mode gating (BISCUIT_MODE_LIKE / BISCUIT_MODE_ILIKE) is
         * deliberately NOT part of the on-disk snapshot format -- it is
         * always recomputed fresh from the live index Relation's opclass
         * (index->rd_opfamily), so a snapshot taken under one opclass and
         * then loaded after a REINDEX to a different opclass can never
         * serve the wrong structure set. See biscuit_get_column_case_mode()
         * in biscuit_index.c.
         */
        if (num_columns == 1)
        {
            idx->legacy_case_mode = biscuit_get_column_case_mode(index, 0);
        }
        else
        {
            idx->column_case_mode = (uint8 *) palloc(num_columns * sizeof(uint8));
            for (i = 0; i < num_columns; i++)
                idx->column_case_mode[i] = biscuit_get_column_case_mode(index, i);
        }

        idx->max_len            = rget_i32(&r);
        idx->max_length_legacy  = rget_i32(&r);
        idx->max_length_lower   = rget_i32(&r);
        idx->insert_count       = rget_i64(&r);
        idx->update_count       = rget_i64(&r);
        idx->delete_count       = rget_i64(&r);
        idx->tombstone_count    = rget_i32(&r);

        /* ---- tids ---- */
        idx->tids = (ItemPointerData *) palloc(idx->capacity * sizeof(ItemPointerData));
        if (idx->num_records > 0)
            rget(&r, idx->tids, idx->num_records * sizeof(ItemPointerData));

        /* ---- tombstones ---- */
        idx->tombstones = rget_bitmap(&r);
        if (!idx->tombstones)
            idx->tombstones = biscuit_roaring_create();

        /* ---- free list ---- */
        idx->free_count = rget_i32(&r);
        idx->free_capacity = Max(idx->free_count, 64);
        idx->free_list = (uint32_t *) palloc(idx->free_capacity * sizeof(uint32_t));
        if (idx->free_count > 0)
            rget(&r, idx->free_list, idx->free_count * sizeof(uint32_t));

        if (r.error)
            ereport(ERROR, (errmsg("biscuit: truncated snapshot for index %u", indexoid)));

        if (idx->num_columns == 1)
        {
            idx->data_cache       = (char **) palloc0(idx->capacity * sizeof(char *));
            idx->data_cache_lower = (char **) palloc0(idx->capacity * sizeof(char *));

            for (i = 0; i < idx->num_records; i++)
            {
                idx->data_cache[i]       = rget_str(&r, CacheMemoryContext);
                idx->data_cache_lower[i] = rget_str(&r, CacheMemoryContext);
            }

            for (ch = 0; ch < CHAR_RANGE; ch++)
            {
                idx->pos_idx_legacy[ch].entries  = (PosEntry *) palloc(8 * sizeof(PosEntry));
                idx->pos_idx_legacy[ch].capacity = 8;
                idx->neg_idx_legacy[ch].entries  = (PosEntry *) palloc(8 * sizeof(PosEntry));
                idx->neg_idx_legacy[ch].capacity = 8;
                idx->pos_idx_lower[ch].entries   = (PosEntry *) palloc(8 * sizeof(PosEntry));
                idx->pos_idx_lower[ch].capacity  = 8;
                idx->neg_idx_lower[ch].entries   = (PosEntry *) palloc(8 * sizeof(PosEntry));
                idx->neg_idx_lower[ch].capacity  = 8;

                rget_charindex(&r, &idx->pos_idx_legacy[ch]);
                rget_charindex(&r, &idx->neg_idx_legacy[ch]);
                idx->char_cache_legacy[ch] = rget_bitmap(&r);

                rget_charindex(&r, &idx->pos_idx_lower[ch]);
                rget_charindex(&r, &idx->neg_idx_lower[ch]);
                idx->char_cache_lower[ch] = rget_bitmap(&r);
            }

            {
                int n;
                idx->length_bitmaps_legacy    = rget_bitmap_array(&r, &n, CacheMemoryContext);
                idx->length_ge_bitmaps_legacy = rget_bitmap_array(&r, &n, CacheMemoryContext);
                idx->length_bitmaps_lower     = rget_bitmap_array(&r, &n, CacheMemoryContext);
                idx->length_ge_bitmaps_lower  = rget_bitmap_array(&r, &n, CacheMemoryContext);
            }
        }
        else
        {
            int col;

            idx->column_types            = (Oid *) palloc(natts * sizeof(Oid));
            idx->output_funcs            = (FmgrInfo *) palloc(natts * sizeof(FmgrInfo));
            idx->column_data_cache       = (char ***) palloc(natts * sizeof(char **));
            idx->column_data_cache_lower = (char ***) palloc(natts * sizeof(char **));
            idx->column_indices          = (ColumnIndex *) palloc0(natts * sizeof(ColumnIndex));

            for (col = 0; col < natts; col++)
            {
                Oid  typoutput;
                bool typIsVarlena;

                idx->column_types[col] = rget_u32(&r);
                getTypeOutputInfo(idx->column_types[col], &typoutput, &typIsVarlena);
                fmgr_info(typoutput, &idx->output_funcs[col]);

                idx->column_data_cache[col]       = (char **) palloc0(idx->capacity * sizeof(char *));
                idx->column_data_cache_lower[col] = (char **) palloc0(idx->capacity * sizeof(char *));

                for (i = 0; i < idx->num_records; i++)
                {
                    idx->column_data_cache[col][i]       = rget_str(&r, CacheMemoryContext);
                    idx->column_data_cache_lower[col][i] = rget_str(&r, CacheMemoryContext);
                }
            }

            for (col = 0; col < natts; col++)
            {
                ColumnIndex *cidx = &idx->column_indices[col];
                int n;

                cidx->max_length       = rget_i32(&r);
                cidx->max_length_lower = rget_i32(&r);

                for (ch = 0; ch < CHAR_RANGE; ch++)
                {
                    cidx->pos_idx[ch].entries  = (PosEntry *) palloc(8 * sizeof(PosEntry));
                    cidx->pos_idx[ch].capacity = 8;
                    cidx->neg_idx[ch].entries  = (PosEntry *) palloc(8 * sizeof(PosEntry));
                    cidx->neg_idx[ch].capacity = 8;
                    cidx->pos_idx_lower[ch].entries  = (PosEntry *) palloc(8 * sizeof(PosEntry));
                    cidx->pos_idx_lower[ch].capacity = 8;
                    cidx->neg_idx_lower[ch].entries  = (PosEntry *) palloc(8 * sizeof(PosEntry));
                    cidx->neg_idx_lower[ch].capacity = 8;

                    rget_charindex(&r, &cidx->pos_idx[ch]);
                    rget_charindex(&r, &cidx->neg_idx[ch]);
                    cidx->char_cache[ch] = rget_bitmap(&r);

                    rget_charindex(&r, &cidx->pos_idx_lower[ch]);
                    rget_charindex(&r, &cidx->neg_idx_lower[ch]);
                    cidx->char_cache_lower[ch] = rget_bitmap(&r);
                }

                cidx->length_bitmaps          = rget_bitmap_array(&r, &n, CacheMemoryContext);
                cidx->length_ge_bitmaps       = rget_bitmap_array(&r, &n, CacheMemoryContext);
                cidx->length_bitmaps_lower    = rget_bitmap_array(&r, &n, CacheMemoryContext);
                cidx->length_ge_bitmaps_lower = rget_bitmap_array(&r, &n, CacheMemoryContext);
            }
        }

        if (r.error)
            ereport(ERROR, (errmsg("biscuit: truncated snapshot for index %u", indexoid)));

        /*
         * ---- trailing checksum ----
         * Snapshot everything the running CRC has accumulated so far
         * (the full header + body we just streamed through) *before*
         * reading the 4 trailer bytes themselves -- the trailer is not
         * part of its own coverage. This is the only "verification"
         * step: it falls directly out of the single read pass above,
         * so there is no separate whole-file hashing pass here, no
         * matter how large the snapshot is.
         *
         * A mismatch means the file was truncated/corrupted/tampered
         * with since it was written; treat it exactly like any other
         * unreadable snapshot -- discard and let the caller fall back
         * to a full from-heap rebuild rather than trust the bytes.
         */
        {
            pg_crc32c computed_crc = r.crc;
            uint32    stored_crc;

            FIN_CRC32C(computed_crc);
            stored_crc = (uint32) rget_u32(&r);

            if (r.error)
                ereport(ERROR, (errmsg("biscuit: snapshot for index %u missing checksum trailer",
                                        indexoid)));

            if (stored_crc != (uint32) computed_crc)
                ereport(ERROR, (errmsg("biscuit: checksum mismatch in snapshot for index %u, "
                                        "file may be corrupt or truncated", indexoid)));
        }

        /*
         * Initialize the in-memory generation counters from the
         * snapshot we just loaded.  gen mirrors what was live when the
         * snapshot was taken (already verified above to match the
         * metapage), and gen_at_last_snapshot is set to match it since
         * this loaded copy *is* the last snapshot -- it must not be
         * treated as needing an immediate re-save.
         */
        idx->gen                 = snapshot_gen;
        idx->gen_at_last_snapshot = snapshot_gen;
    }
    PG_CATCH();
    {
        /*
         * Any failure here means a corrupt/truncated/incompatible
         * snapshot -- discard everything we allocated (it's all in
         * CacheMemoryContext, which we don't reset, so explicitly
         * bail out to NULL and let the caller do a normal rebuild)
         * rather than propagating the error to the query.
         */
        FlushErrorState();
        MemoryContextSwitchTo(oldcontext);
        fclose(r.fp);
        elog(WARNING, "biscuit: discarding unreadable snapshot for index %u, "
                       "falling back to full rebuild", indexoid);
        return NULL;
    }
    PG_END_TRY();

    MemoryContextSwitchTo(oldcontext);
    fclose(r.fp);

    elog(DEBUG1, "biscuit: loaded disk snapshot for index %u (%d records, gen " UINT64_FORMAT ")",
         indexoid, idx->num_records, idx->gen);

    return idx;
}

/* ================================================================
 * DROP
 * ================================================================ */

void
biscuit_persist_drop(Oid indexoid)
{
    char path[MAXPGPATH];

    biscuit_persist_path(indexoid, path, sizeof(path));
    if (unlink(path) != 0 && errno != ENOENT)
        elog(WARNING, "biscuit: could not remove snapshot \"%s\": %m", path);
}
