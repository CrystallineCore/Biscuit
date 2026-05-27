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

#endif /* BISCUIT_CACHE_H */
