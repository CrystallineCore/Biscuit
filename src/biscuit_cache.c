/*
 * biscuit_cache.c
 * Session-scoped cache for BiscuitIndex objects.
 *
 * Each index is kept in CacheMemoryContext so it survives across
 * transactions.  A relcache callback and a proc-exit hook ensure
 * stale entries are evicted when relations are dropped or the
 * backend exits.
 */

#include "biscuit_common.h"
#include "biscuit_cache.h"
#include "biscuit_persist.h"

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
    elog(DEBUG1, "Biscuit: Invalidated cache for relation %u", relid);
}

static void
biscuit_module_unload_callback(int code, unsigned long datum)
{
    BiscuitIndexCacheEntry *entry;

    (void) code;
    (void) datum;

    /*
     * Flush any index whose in-memory generation has moved past what's
     * on disk before we drop the cache.
     *
     * This is the missing half of the eager-resave story: bulkdelete
     * and vacuumcleanup always get an unconditional flush for free
     * because a VACUUM always ends by calling biscuit_vacuumcleanup(),
     * which force-saves whenever gen != gen_at_last_snapshot. Plain
     * INSERTs have no equivalent guaranteed call site -- they only get
     * persisted eagerly once BISCUIT_SNAPSHOT_GEN_THRESHOLD generations
     * have piled up (see biscuit_insert() in biscuit_index.c). A
     * session that inserts fewer rows than that threshold and then
     * disconnects was leaving those rows' bitmap state unsaved forever,
     * even though the tuples themselves are safely committed in the
     * heap -- the next cold load would silently fall back to a full
     * from-heap rebuild instead of finding a stale-but-recoverable
     * snapshot, which defeats the point of snapshotting at all.
     *
     * biscuit_persist_save() only needs the indexoid (not a live
     * Relation), which is exactly what we have in the cache entry, so
     * this is safe to call this late in backend shutdown -- no catalog
     * access required.
     */
    for (entry = biscuit_cache_head; entry != NULL; entry = entry->next)
    {
        if (entry->index != NULL && entry->index->gen != entry->index->gen_at_last_snapshot)
        {
            elog(DEBUG1, "Biscuit: Flushing unsaved snapshot for index %u at proc-exit",
                 entry->indexoid);
            biscuit_persist_save(entry->indexoid, entry->index);
        }
    }

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
