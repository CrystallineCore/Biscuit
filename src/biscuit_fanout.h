/*
 * biscuit_fanout.h
 *
 * ONE FAN-OUT, THREE CALLERS  (base/delta design §4.1)
 *
 * "What structures does this string belong to?" used to be answered in
 * three places that had to agree by inspection:
 *
 *   - biscuit_index_single_record()   (legacy single-column write path)
 *   - biscuit_index_column_record()   (multi-column write path)
 *   - the LEN / LEN_GE blocks open-coded inside biscuit_insert()
 *
 * The base/delta design adds a fourth consumer -- the delta builder, which
 * must reproduce the *identical* set of (col, is_lower, kind, ch, position)
 * identities from the same bytes, or reads silently disagree with what a
 * drain later writes into the base blobs. Four independent copies of that
 * rule is not a maintainable proposition, so it lives here once and every
 * caller drives it through a callback.
 *
 * The emitted identity tuple is deliberately the same one that
 * BiscuitDirEntry, BiscuitPendLogKey and the delta hash all key on, so an
 * emitted identity can be used directly as a lookup key with no
 * translation layer in between.
 *
 * LOCALE SAFETY (§4.2)
 * --------------------
 * lower_str is a PARAMETER. It is never computed in here, and this file
 * must never call biscuit_str_tolower(). PostgreSQL's lower() is
 * collation-dependent, so a delta builder that recomputed it could derive
 * different lowercase bytes from the same row than the writer did --
 * across two backends, or across a primary and a standby running
 * different ICU/libc versions. Callers on the write path pass the bytes
 * they are about to persist; callers on the delta/drain side pass the
 * bytes they read back out of STRCACHE. Both therefore fan out over
 * exactly the bytes that are durable.
 */
#ifndef BISCUIT_FANOUT_H
#define BISCUIT_FANOUT_H

#include "biscuit_common.h"

/*
 * Emit callback.
 *
 * col       -- BiscuitDirEntry.col addressing: BISCUIT_DIR_COL_LEGACY (-1)
 *              for the single-column layout, 0-based otherwise.
 * is_lower  -- which case-mode family the structure belongs to.
 * kind      -- BISCUIT_DIR_KIND_POS / _NEG / _CACHE / _LEN / _LEN_GE.
 * ch        -- character byte for POS/NEG/CACHE, -1 for LEN/LEN_GE.
 * position  -- character position for POS, negative offset for NEG,
 *              length for LEN/LEN_GE, -1 for CACHE.
 * slot      -- the record slot (rec_idx) being fanned out. Passed through
 *              from biscuit_fanout_string() so a stateless ctx can be
 *              reused across slots.
 * op        -- BISCUIT_PENDING_OP_ADD / _REMOVE, passed through unchanged.
 *
 * NOTE (deviation from the design doc): the doc's sketch of this typedef
 * omits `slot`. It is threaded through here because every consumer needs
 * it and the alternative -- mutating a field on ctx between calls -- makes
 * the callback order-dependent for no gain.
 */
typedef void (*BiscuitFanoutEmit)(void *ctx,
                                  int32 col, bool is_lower, uint8 kind,
                                  int32 ch, int32 position,
                                  uint32 slot, uint8 op);

/*
 * biscuit_fanout_string
 *
 * Emit every structure identity that the string (str, byte_len) implies
 * for column `col` at record slot `slot`.
 *
 * mode          -- BISCUIT_MODE_LIKE gates the case-sensitive pass,
 *                  BISCUIT_MODE_ILIKE gates the lowercase pass. This is
 *                  the same gating the write path applies, so a column
 *                  whose opclass is biscuit_like_ops fans out to exactly
 *                  the structures that path would have created.
 * lower_str /
 * lower_len     -- the lowercased bytes, or NULL/0. A NULL lower_str
 *                  suppresses the lowercase pass regardless of `mode`,
 *                  matching the write path's "data_cache_lower[slot] is
 *                  NULL when ILIKE is off" convention.
 * len_ge_bound  -- upper bound (exclusive) on LEN_GE positions, or -1 for
 *                  no bound. The write path caps the LEN_GE loop at the
 *                  live allocated capacity of length_ge_bitmaps[]; the
 *                  delta builder has no such array and passes -1.
 * op            -- stamped onto every emitted identity.
 *
 * Emission order matches the write path's: the case-sensitive character
 * pass, then the case-sensitive LEN/LEN_GE, then the lowercase character
 * pass, then the lowercase LEN/LEN_GE. Consumers must not depend on that
 * order for correctness (the delta is a set), but keeping it identical
 * makes a diff of old-vs-new pending logs readable.
 */
extern void biscuit_fanout_string(const char *str, int byte_len,
                                  const char *lower_str, int lower_len,
                                  int32 col, uint8 mode, uint32 slot,
                                  int32 len_ge_bound,
                                  uint8 op,
                                  BiscuitFanoutEmit emit, void *ctx);

/*
 * Number of identities biscuit_fanout_string() would emit for the same
 * arguments, without emitting them. Used for delta bitmap presizing and
 * for the rebuild-cost estimate behind the compaction threshold; cheap
 * enough (one pass over the bytes) to call speculatively.
 */
extern uint64 biscuit_fanout_count(const char *str, int byte_len,
                                   const char *lower_str, int lower_len,
                                   uint8 mode, int32 len_ge_bound);

#endif /* BISCUIT_FANOUT_H */
