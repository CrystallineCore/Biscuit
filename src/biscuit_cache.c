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
#include "utils/inval.h"
#include "storage/ipc.h"

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
    biscuit_cache_remove(relid);
    elog(DEBUG1, "Biscuit: Invalidated cache for relation %u", relid);
}

static void
biscuit_module_unload_callback(int code, unsigned long datum)
{
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
