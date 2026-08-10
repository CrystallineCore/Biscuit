/*
 * biscuit_cache.h
 * In-process index cache: lookup, insert, remove, and invalidation
 * callbacks that keep in-memory BiscuitIndex structures alive across
 * multiple queries within a session.
 */

#ifndef BISCUIT_CACHE_H
#define BISCUIT_CACHE_H

#include "biscuit_common.h"

/* Look up a cached index by relation OID. Returns NULL on miss. */
extern BiscuitIndex *biscuit_cache_lookup(Oid indexoid);

/* Insert (or replace) a BiscuitIndex in the session cache. */
extern void biscuit_cache_insert(Oid indexoid, BiscuitIndex *idx);

/* Remove a cache entry (e.g. after DROP INDEX). */
extern void biscuit_cache_remove(Oid indexoid);

/* Register rel-cache and proc-exit callbacks (idempotent). */
extern void biscuit_register_callback(void);

/* Safe cleanup of a BiscuitIndex structure (nulls pointers). */
extern void biscuit_cleanup_index(BiscuitIndex *idx);

/*
 * biscuit_index_free_bitmaps
 *
 * Explicitly free every RoaringBitmap a BiscuitIndex owns (POS/NEG/CACHE
 * per character per column, both case variants, the LEN/LEN_GE ladder,
 * and tombstones). Required before MemoryContextDelete()-ing (or
 * otherwise discarding) any context holding a BiscuitIndex: under
 * HAVE_ROARING, every RoaringBitmap* is allocated by CRoaring's own
 * allocator, not by palloc, so a bare MemoryContextDelete() never reaches
 * it -- see the definition in biscuit_cache.c for the full explanation.
 * idx may be NULL, and may be partially built (any zero-initialized
 * BiscuitIndex, e.g. straight out of palloc0(), is safe to pass here).
 *
 * Exported for biscuit_persist.c's biscuit_persist_load(), which must
 * apply the same treatment to a discarded/distrusted load attempt before
 * abandoning or reusing its context -- not just biscuit_cache.c's own
 * eviction path.
 */
extern void biscuit_index_free_bitmaps(BiscuitIndex *idx);

#endif /* BISCUIT_CACHE_H */
