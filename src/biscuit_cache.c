/*
 * biscuit_cache.c
 * Session-scoped cache for BiscuitIndex objects.
 *
 * Each index is kept in CacheMemoryContext so it survives across
 * transactions.  A relcache callback evicts entries when relations are
 * dropped; a proc-exit hook just drops the process-local cache on
 * backend shutdown (there is nothing left to flush -- every mutation is
 * already durable the moment it happens, see biscuit_persist.c).
 */

#include "biscuit_common.h"
#include "biscuit_cache.h"
#include "biscuit_bitmap.h"    /* biscuit_roaring_free() -- see
                                 * biscuit_index_free_bitmaps() below for why
                                 * this file, specifically, needs it */
#include "biscuit_pendlog.h"   /* biscuit_pendlog_invalidate() -- the snapshot
                                 * cache must be dropped alongside the index
                                 * cache on relcache invalidation */

/* ==================== CACHE STATE ==================== */

typedef struct BiscuitIndexCacheEntry {
    Oid                        indexoid;
    BiscuitIndex              *index;
    struct BiscuitIndexCacheEntry *next;
} BiscuitIndexCacheEntry;

static BiscuitIndexCacheEntry *biscuit_cache_head       = NULL;
static bool                    biscuit_callback_registered = false;

/*
 * Forward declaration: defined further down (see its own comment), but
 * biscuit_cache_insert()'s defensive replace-in-place branch below needs
 * it too -- see that branch's comment. Not static -- also called from
 * biscuit_persist.c's discarded-load-attempt paths; see biscuit_cache.h.
 */

/* ==================== LOOKUP ==================== */

BiscuitIndex *
biscuit_cache_lookup(Oid indexoid)
{
    BiscuitIndexCacheEntry *entry;

    for (entry = biscuit_cache_head; entry != NULL; entry = entry->next)
    {
        if (entry->indexoid == indexoid)
            return entry->index;
    }
    return NULL;
}

/* ==================== INSERT ==================== */

/*
 * Insert (or replace) an index in the cache.
 * Always allocates the list node in CacheMemoryContext.
 */
void
biscuit_cache_insert(Oid indexoid, BiscuitIndex *idx)
{
    BiscuitIndexCacheEntry *entry;
    MemoryContext            oldcontext;

    /*
     * Fast path: an entry for this indexoid already exists.  Callers
     * (e.g. biscuit_insert) call this once per tuple to make sure the
     * global cache reflects the latest mutated BiscuitIndex, even though
     * the pointer itself usually hasn't changed within a statement.
     * Updating the existing node in place avoids a palloc + remove/insert
     * cycle (and the associated DEBUG1 log spam) for every single row.
     */
    for (entry = biscuit_cache_head; entry != NULL; entry = entry->next)
    {
        if (entry->indexoid == indexoid)
        {
            /*
             * Defensive: every current call site (biscuit_build(),
             * biscuit_persist_load() via biscuit_load_index(), and
             * biscuit_insert()/biscuit_bulkdelete() re-inserting their own
             * already-cached idx unchanged) either targets an oid with no
             * existing entry, or passes back the SAME idx pointer already
             * stored here. If a future caller ever replaces this entry's
             * idx with a genuinely different object without going through
             * biscuit_cache_remove() first, don't silently orphan the old
             * one's context the way the pre-context-ownership code did.
             */
            if (entry->index && entry->index != idx && entry->index->reserved[0] != 0)
            {
                /*
                 * Same requirement as biscuit_cache_entry_release(): the
                 * superseded index's bitmaps are CRoaring-allocated, not
                 * palloc'd, so MemoryContextDelete() alone would orphan
                 * them. Free them first -- see
                 * biscuit_index_free_bitmaps()'s comment for the full
                 * explanation. This branch is not currently reached by any
                 * call site (see the comment above), but leaving it to
                 * silently leak if it ever is would be the same mistake
                 * this whole fix exists to close.
                 */
                biscuit_index_free_bitmaps(entry->index);
                MemoryContextDelete((MemoryContext) (uintptr_t) entry->index->reserved[0]);
            }
            entry->index = idx;
            return;
        }
    }

    /* No existing entry: allocate a new node in CacheMemoryContext */
    oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

    entry            = (BiscuitIndexCacheEntry *) palloc(sizeof(BiscuitIndexCacheEntry));
    entry->indexoid  = indexoid;
    entry->index     = idx;
    entry->next      = biscuit_cache_head;
    biscuit_cache_head = entry;

    MemoryContextSwitchTo(oldcontext);

    elog(DEBUG1, "Biscuit: Cached index %u", indexoid);
}

/* ==================== REMOVE ==================== */

/*
 * Free every per-position bitmap in one CharIndex[CHAR_RANGE] array (a
 * POS or a NEG side). Shared by the legacy single-column fields and every
 * per-column ColumnIndex, case-sensitive and case-insensitive alike.
 */
static void
biscuit_charindex_array_free_bitmaps(CharIndex *idx_arr)
{
    int ch;

    for (ch = 0; ch < CHAR_RANGE; ch++)
    {
        CharIndex *ci = &idx_arr[ch];
        int        i;

        for (i = 0; i < ci->count; i++)
            SAFE_BITMAP_FREE(ci->entries[i].bitmap);
    }
}

/* Free every bitmap in one char_cache[CHAR_RANGE] array (a CACHE side). */
static void
biscuit_char_cache_free_bitmaps(RoaringBitmap **char_cache)
{
    int ch;

    for (ch = 0; ch < CHAR_RANGE; ch++)
        SAFE_BITMAP_FREE(char_cache[ch]);
}

/* Free a length_bitmaps[]/length_ge_bitmaps[] pair, each sized max_length
 * (or max_length_lower) entries -- see biscuit_persist_load_column()'s
 * palloc0() calls, which is what the load path sizes them to, and
 * biscuit_index.c's build-time growth, which keeps them at the same
 * width. NULL arrays (max_length == 0, or a column that was never fully
 * built) are handled the same way SAFE_BITMAP_FREE handles a NULL bitmap:
 * a no-op loop bound. */
static void
biscuit_length_arrays_free_bitmaps(RoaringBitmap **length_bitmaps,
                                    RoaringBitmap **length_ge_bitmaps,
                                    int max_length)
{
    int i;

    if (length_bitmaps != NULL)
        for (i = 0; i < max_length; i++)
            SAFE_BITMAP_FREE(length_bitmaps[i]);

    if (length_ge_bitmaps != NULL)
        for (i = 0; i < max_length; i++)
            SAFE_BITMAP_FREE(length_ge_bitmaps[i]);
}

/*
 * Free every RoaringBitmap a single ColumnIndex owns (both case variants).
 * Used for every entry of idx->column_indices[] on a multi-column index.
 */
static void
biscuit_column_index_free_bitmaps(ColumnIndex *col)
{
    biscuit_charindex_array_free_bitmaps(col->pos_idx);
    biscuit_charindex_array_free_bitmaps(col->neg_idx);
    biscuit_char_cache_free_bitmaps(col->char_cache);

    biscuit_charindex_array_free_bitmaps(col->pos_idx_lower);
    biscuit_charindex_array_free_bitmaps(col->neg_idx_lower);
    biscuit_char_cache_free_bitmaps(col->char_cache_lower);

    biscuit_length_arrays_free_bitmaps(col->length_bitmaps, col->length_ge_bitmaps,
                                        col->max_length);
    biscuit_length_arrays_free_bitmaps(col->length_bitmaps_lower, col->length_ge_bitmaps_lower,
                                        col->max_length_lower);
}

/*
 * biscuit_index_free_bitmaps
 *
 * Explicitly free every RoaringBitmap a BiscuitIndex owns, walking the
 * same fields biscuit_persist_load()/biscuit_build() populate.
 *
 * WHY THIS HAS TO EXIST SEPARATELY FROM MemoryContextDelete(idx_cxt):
 *
 * Under HAVE_ROARING, biscuit_roaring_create()/_create_sized()/
 * _deserialize() delegate straight to CRoaring (roaring_bitmap_create()
 * etc), which manages its own memory via plain malloc/free -- it has no
 * idea PostgreSQL memory contexts exist. Every RoaringBitmap* stored
 * anywhere in a BiscuitIndex (per-character POS/NEG/CACHE bitmaps, the
 * LEN/LEN_GE ladder, per column, both case variants, plus tombstones) is
 * therefore NOT reclaimed by MemoryContextDelete(): that call frees only
 * the palloc'd scaffolding around them (the CharIndex.entries arrays, the
 * length_bitmaps[] pointer arrays, idx itself) and leaves every bitmap
 * object it pointed at dangling in CRoaring's own heap, unreachable and
 * therefore unfreeable for the rest of the backend's life.
 *
 * biscuit_pendlog.c's pendlog_snapshot_free() already documents and
 * handles this exact hazard for its own RoaringBitmap-holding structure
 * (see its comment: "a RoaringBitmap is allocated by CRoaring's own
 * allocator, not by palloc, so deleting the context reclaims the hash
 * entries while leaking every bitmap they point at"). This function is
 * the equivalent fix for BiscuitIndex, which never got it: every eviction
 * -- and biscuit_get_current_index() evicts and reloads on essentially
 * every statement under concurrent writers -- was leaking the complete
 * bitmap set of the previous copy, permanently, on top of whatever the
 * v58/strcache-ownership fixes already closed. That is the dominant
 * contributor to the unbounded, no-plateau, no-reclaim per-backend RSS
 * growth in BISCUIT-3.0.0-GA-Report.md's §10 series (v58 through v62):
 * bitmaps -- thousands of small per-(character, position) structures per
 * column -- are the bulk of a LIKE/ILIKE index's in-memory footprint, far
 * more of it than the row text or the on-disk size alone would suggest,
 * which is exactly why the leak rate measured so far in excess of the
 * index's on-disk size. SAFE_BITMAP_FREE() (biscuit_common.h) already
 * existed for this purpose and was unused anywhere in the tree before
 * this function.
 *
 * Must be called BEFORE MemoryContextDelete() on idx's own context --
 * once that call runs, every pointer this function would walk is gone.
 * idx may be NULL (mirrors biscuit_cleanup_index()'s contract).
 *
 * Exported (declared in biscuit_cache.h): biscuit_persist.c's
 * biscuit_persist_load() needs this too, for its discarded-load-attempt
 * paths -- a torn/distrusted or genuinely-corrupt attempt can still have
 * fully decoded real bitmaps into load_cxt before being thrown away, and
 * those need the identical explicit-free treatment before load_cxt
 * itself is deleted or reused by the next attempt. See that function's
 * call sites for the details.
 */
void
biscuit_index_free_bitmaps(BiscuitIndex *idx)
{
    int i;

    if (idx == NULL)
        return;

    /* Legacy single-column fields. */
    biscuit_charindex_array_free_bitmaps(idx->pos_idx_legacy);
    biscuit_charindex_array_free_bitmaps(idx->neg_idx_legacy);
    biscuit_char_cache_free_bitmaps(idx->char_cache_legacy);

    biscuit_charindex_array_free_bitmaps(idx->pos_idx_lower);
    biscuit_charindex_array_free_bitmaps(idx->neg_idx_lower);
    biscuit_char_cache_free_bitmaps(idx->char_cache_lower);

    biscuit_length_arrays_free_bitmaps(idx->length_bitmaps_legacy, idx->length_ge_bitmaps_legacy,
                                        idx->max_length_legacy);
    biscuit_length_arrays_free_bitmaps(idx->length_bitmaps_lower, idx->length_ge_bitmaps_lower,
                                        idx->max_length_lower);

    /* Multi-column fields, when this index has more than one column. */
    if (idx->column_indices != NULL)
        for (i = 0; i < idx->num_columns; i++)
            biscuit_column_index_free_bitmaps(&idx->column_indices[i]);

    SAFE_BITMAP_FREE(idx->tombstones);
}

/*
 * Unlink a cache entry AND free the BiscuitIndex it points at.
 *
 * Every BiscuitIndex now lives in its own child context of
 * CacheMemoryContext (idx->reserved[0] holds the MemoryContext handle --
 * see biscuit_persist_load()'s and biscuit_build()'s comments in
 * biscuit_persist.c/biscuit_index.c for why). This used to only unlink the
 * list node and leave the BiscuitIndex itself sitting in
 * CacheMemoryContext with nothing pointing at it -- CacheMemoryContext is
 * never reset by PostgreSQL, so that was a permanent per-eviction leak,
 * and biscuit_get_current_index() evicts+reloads on essentially every
 * statement under concurrent writers (BISCUIT-3.0.0-GA-Report.md §10.4,
 * the 737 MB per-backend growth / cluster OOM finding). Deleting the
 * context here frees the whole BiscuitIndex -- TIDs, every cached string,
 * every bitmap's palloc'd scaffolding -- in one call.
 *
 * That deletion does NOT reach the bitmaps' own CRoaring-allocated
 * memory (see biscuit_index_free_bitmaps()'s comment for why), so this
 * walks and explicitly frees every one of them first. Getting the order
 * backwards -- deleting the context, then trying to walk idx's fields --
 * would be a use-after-free, since idx itself lives inside the context
 * being deleted.
 *
 * The BiscuitIndexCacheEntry list node itself is a separate, tiny
 * allocation directly in CacheMemoryContext (see biscuit_cache_insert()),
 * not inside idx's context, so it's pfree'd here too rather than left
 * behind.
 */
static void
biscuit_cache_entry_release(BiscuitIndexCacheEntry *entry)
{
    if (entry->index && entry->index->reserved[0] != 0)
    {
        MemoryContext idx_cxt = (MemoryContext) (uintptr_t) entry->index->reserved[0];

        biscuit_index_free_bitmaps(entry->index);
        MemoryContextDelete(idx_cxt);
    }
    pfree(entry);
}

void
biscuit_cache_remove(Oid indexoid)
{
    BiscuitIndexCacheEntry **entry_ptr = &biscuit_cache_head;
    BiscuitIndexCacheEntry  *entry;

    /*
     * relid == InvalidOid means "everything" -- PostgreSQL invokes
     * relcache callbacks that way on a sinval queue overflow, when it can
     * no longer say which relations changed. biscuit_pendlog_invalidate()
     * already interprets it that way; this function used to not, so an
     * overflow dropped every cached pending-log snapshot while keeping
     * every cached BiscuitIndex. That combination is worse than either
     * half alone: the in-memory index survives as authoritative while the
     * deltas that were supposed to be reconciled against it are gone,
     * which yields silently stale reads instead of a clean cache miss.
     * Dropping everything just costs a reload from durable state.
     */
    if (!OidIsValid(indexoid))
    {
        while (biscuit_cache_head != NULL)
        {
            entry = biscuit_cache_head;
            biscuit_cache_head = entry->next;
            biscuit_cache_entry_release(entry);
        }
        elog(DEBUG1, "Biscuit: Dropped all cache entries (global invalidation)");
        return;
    }

    while (*entry_ptr != NULL)
    {
        entry = *entry_ptr;
        if (entry->indexoid == indexoid)
        {
            *entry_ptr = entry->next;
            biscuit_cache_entry_release(entry);
            elog(DEBUG1, "Biscuit: Removed cache entry for index %u", indexoid);
            return;
        }
        entry_ptr = &entry->next;
    }
}

/* ==================== CLEANUP ==================== */

/*
 * Mark a BiscuitIndex as invalid.
 * We intentionally do not free its memory: CacheMemoryContext owns it.
 */
void
biscuit_cleanup_index(BiscuitIndex *idx)
{
    if (!idx)
        return;
    /*
     * Null out the pointer chain so callers can detect a cleaned-up
     * index, but leave deallocation to CacheMemoryContext reset.
     */
    (void) idx;
}

/* ==================== CALLBACKS ==================== */

static void
biscuit_relcache_callback(Datum arg, Oid relid)
{
    (void) arg;
    biscuit_cache_remove(relid);

    /*
     * Drop any cached pending-log snapshot for this relation too. These are
     * two independent process-local caches keyed by the same Oid, and
     * leaving the snapshot behind after a DROP/REINDEX would let a later
     * query reconcile against deltas belonging to an index that no longer
     * exists. relid == InvalidOid means "everything", which
     * biscuit_pendlog_invalidate() already interprets the same way.
     */
    biscuit_pendlog_invalidate(relid);
    //elog(DEBUG1, "Biscuit: Invalidated cache for relation %u", relid);
}

static void
biscuit_module_unload_callback(int code, unsigned long datum)
{
    (void) code;
    (void) datum;

    /*
     * Nothing to flush here: every insert/delete already durably appends
     * to its structures' pending lists (biscuit_pending_mutate_structure())
     * the moment it happens, and VACUUM's biscuit_vacuumcleanup() drains
     * those pending lists into compacted blobs directly against the
     * on-disk directory, with no in-memory BiscuitIndex involved. There is
     * no "unsaved snapshot" concept left to reconcile at proc-exit: the
     * old flush that used to live here compared idx->gen against
     * idx->gen_at_last_snapshot and called biscuit_persist_save() to catch
     * up, but biscuit_persist_save() now requires relation_open() (a real
     * Relation, not just an Oid) to reach the buffer manager, and
     * relation_open() is not safe to call this late in backend shutdown --
     * see biscuit_persist.c's file header for the full history. This
     * callback now exists solely to drop the process-local cache.
     */
    /*
     * COMPACTION ON CLEAN SHUTDOWN (design §7.3) IS NOT IMPLEMENTED HERE,
     * AND CANNOT BE.
     *
     * The design proposes compacting at clean shutdown so that a *planned*
     * restart discards nothing, leaving crash restart -- which is already
     * replaying WAL -- as the only case that rebuilds a delta from
     * scratch. The benefit is real: a 100k-row delta costs an estimated
     * ~3 s to rebuild, and throwing that away on every ordinary restart is
     * pure waste. It is also described as "one hook", which is what makes
     * it look cheap.
     *
     * It is not one hook, because this is the wrong place for it, for
     * exactly the reason recorded above and in biscuit_persist.c's file
     * header. Compaction needs a real Relation: biscuit_pendlog_compact()
     * reaches the buffer manager and takes a heavyweight page lock, so it
     * needs relation_open(), and relation_open() is not safe this late in
     * backend shutdown. That is precisely why the old
     * biscuit_persist_save() flush was removed from this callback rather
     * than fixed. Reintroducing the same call under a different name would
     * reintroduce the same crash.
     *
     * Nor does before_shmem_exit() help -- it is later still, not earlier.
     *
     * A working version needs a context that legitimately holds relations
     * open: a background worker with a shutdown callback, or piggybacking
     * on the checkpointer. Both are more than a hook, and neither is
     * required for correctness -- an un-compacted delta is rebuilt on
     * demand, just slowly. Until one exists, the compaction threshold
     * (biscuit.delta_compaction_slots) is what bounds the loss: at 20,000
     * rows the most a restart can cost is roughly 0.6 s of rebuild, which
     * is the same bound §7.5 relies on when it rejects a persisted delta
     * layer.
     */
    elog(DEBUG1, "Biscuit: Module unload - clearing all cache entries");
    biscuit_cache_head          = NULL;
    biscuit_callback_registered = false;
}

/*
 * Register relcache and proc-exit callbacks (idempotent).
 */
void
biscuit_register_callback(void)
{
    if (!biscuit_callback_registered)
    {
        CacheRegisterRelcacheCallback(biscuit_relcache_callback, (Datum) 0);
        on_proc_exit(biscuit_module_unload_callback, (Datum) 0);
        biscuit_callback_registered = true;
        elog(DEBUG1, "Biscuit: Registered cache callbacks");
    }
}
