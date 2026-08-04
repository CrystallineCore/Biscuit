/*
 * biscuit_delta.h
 *
 * SLOT -> IDENTITY EXPANSION  (base/delta design §4)
 *
 * The pending log no longer carries derived bitmap records. It carries
 * the fact of a row write: {slot, ADD|REMOVE}, eight bytes, no payload.
 * Something has to turn that back into the (col, is_lower, kind, ch,
 * position) identities the read path and the drain both work in, and this
 * is it.
 *
 * The text is NOT in the log record and must not be put there (§3.1): it
 * is already durable. biscuit_persist_row_identity_write_record() writes,
 * per row, in place, both the raw and the lowercased bytes of every
 * indexed column into STRCACHE. Logging it again would write the same
 * bytes to WAL twice, which is the entire cost this design exists to
 * remove. So expansion reads text back out of STRCACHE.
 *
 * That has a second benefit that is worth as much as the WAL saving:
 * because the lowercased bytes are read rather than recomputed, expansion
 * never calls PostgreSQL's collation-dependent lower(). Two backends, or
 * a primary and a standby on different ICU/libc builds, cannot derive
 * different structures from the same row. See biscuit_fanout.h §4.2.
 *
 * WHAT DRIVES THE COLUMN LIST
 * ---------------------------
 * Not a BiscuitIndex. Expansion runs in two contexts that have no cached
 * index available -- a cold backend building its first snapshot, and
 * VACUUM draining -- so the column set is read from durable state: the
 * directory's STRCACHE entries say which (col, is_lower) string arrays
 * exist, and biscuit_get_column_case_mode() re-derives the opclass gating
 * from the catalog. Both are the same sources the write path used, so the
 * two agree by construction rather than by convention.
 */
#ifndef BISCUIT_DELTA_H
#define BISCUIT_DELTA_H

#include "biscuit_common.h"
#include "biscuit_fanout.h"

/*
 * Expand `nslots` record slots into structure identities, invoking `emit`
 * once per identity.
 *
 * slots  -- ascending, deduplicated. Ascending is not merely tidy: the
 *           STRCACHE read walks the pointer-array page directory once and
 *           reuses the value-heap buffer across consecutive slots (the
 *           heap is bump-allocated in slot order), so an unsorted array
 *           degrades a single ordered walk into random I/O.
 * op     -- stamped onto every emitted identity. Callers expand their ADD
 *           set and their REMOVE set in two separate calls rather than
 *           interleaving, because a slot's identities depend only on its
 *           current text, never on which op is being applied to it.
 *
 * Slots whose STRCACHE entry is NULL (a NULL column value, or a slot
 * whose logical page was never allocated) emit nothing for that column.
 * That is a legitimate state, not a short read -- it is exactly how the
 * write path represents a NULL -- so it is silently skipped.
 *
 * Allocates scratch in a private context that is destroyed before return;
 * anything `emit` wants to keep, it must copy into its own context.
 */
extern void biscuit_delta_expand_slots(Relation index,
                                       const uint32 *slots, int nslots,
                                       uint8 op,
                                       BiscuitFanoutEmit emit, void *ctx);

/*
 * Number of slots at which biscuit_delta_expand_slots() is expected to
 * take longer than the target rebuild time. Reads the
 * biscuit.delta_compaction_slots GUC; exposed as a function so callers
 * do not each have to know the GUC's name or its clamping rules.
 */
extern int biscuit_delta_compaction_threshold(void);

/*
 * GUC backing variable for biscuit.delta_compaction_slots, defined in
 * biscuit_delta.c and registered in _PG_init(). Exposed so biscuit.c can
 * hand its address to DefineCustomIntVariable(); everything else should go
 * through biscuit_delta_compaction_threshold().
 */
extern int biscuit_delta_compaction_slots;

#endif /* BISCUIT_DELTA_H */
