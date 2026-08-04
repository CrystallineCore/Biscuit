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
 * Unlink a cache entry.  Memory is owned by CacheMemoryContext and
 * will be reclaimed by PostgreSQL — do not pfree here.
 */
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
        biscuit_cache_head = NULL;
        elog(DEBUG1, "Biscuit: Dropped all cache entries (global invalidation)");
        return;
    }

    while (*entry_ptr != NULL)
    {
        entry = *entry_ptr;
        if (entry->indexoid == indexoid)
        {
            *entry_ptr = entry->next;
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
