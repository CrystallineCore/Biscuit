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
 * call, not millions of individual add() calls -- that per-value
 * loop was the actual cause of the ~8s snapshot-load time this
 * revision fixes. BISCUIT_PERSIST_VERSION was bumped so any
 * old-format snapshot on disk is ignored (safe rebuild-once fallback)
 * rather than misparsed.
 *
 * Consistency model (per user requirements: read-mostly, best-effort):
 *   - Snapshot is written at the end of biscuit_build() (CREATE INDEX /
 *     first cold load) and should also be re-written after mutating
 *     paths (biscuit_insert / biscuit_bulkdelete) if write volume ever
 *     becomes non-trivial -- see biscuit_index.c call sites.
 *   - On load, we only sanity-check the header (magic/version) and
 *     that num_records is non-negative and columns match the index's
 *     current attribute count. We do NOT verify the snapshot reflects
 *     the current heap contents (e.g. rows added by a different
 *     session since the last save) -- callers accept this staleness
 *     window in exchange for skipping the rebuild.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_cache.h"
#include "biscuit_persist.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>

#define BISCUIT_PERSIST_MAGIC    0x42534E50  /* "BSNP" */
#define BISCUIT_PERSIST_VERSION  2  /* v2: native roaring portable (de)serialize */
#define BISCUIT_PERSIST_DIR      "pg_biscuit"

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
 * ================================================================ */

typedef struct
{
    FILE *fp;
    bool  error;
} WStream;

typedef struct
{
    FILE *fp;
    bool  error;
} RStream;

static void
wput(WStream *w, const void *data, size_t len)
{
    if (w->error || len == 0)
        return;
    if (fwrite(data, 1, len, w->fp) != len)
        w->error = true;
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
biscuit_persist_save(Relation index, BiscuitIndex *idx)
{
    Oid      indexoid = RelationGetRelid(index);
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
    if (!w.fp)
    {
        elog(WARNING, "biscuit: could not create snapshot file \"%s\": %m", tmppath);
        return;
    }

    /* ---- header ---- */
    wput_u32(&w, BISCUIT_PERSIST_MAGIC);
    wput_u32(&w, BISCUIT_PERSIST_VERSION);
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

    elog(DEBUG1, "biscuit: wrote disk snapshot for index %u (%d records)",
         indexoid, idx->num_records);
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
    int            natts = index->rd_index->indnatts;
    int            ch, i;

    biscuit_persist_path(indexoid, path, sizeof(path));

    r.fp = fopen(path, "rb");
    r.error = false;
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

        idx->preload_state = 0;
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

    elog(DEBUG1, "biscuit: loaded disk snapshot for index %u (%d records)",
         indexoid, idx->num_records);

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
