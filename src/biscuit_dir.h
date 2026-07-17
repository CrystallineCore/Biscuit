/*
 * biscuit_dir.h
 *
 * Directory chain primitives ("Phase 0" of the pending-list design doc,
 * §5): per-column chains of BISCUIT_PAGE_DIR pages, each holding a packed
 * BiscuitDirEntry[] array, rooted at BiscuitMetaPageData.dir_roots[slot].
 * A directory entry maps a structure's identity --
 * (col, is_lower, kind, ch, position) -- to the pair of chain heads
 * (compacted blob, pending list) that biscuit_blob.c's primitives read
 * and write.
 *
 * This layer owns:
 *   - allocating a column's chain root lazily, on first entry;
 *   - finding an entry by identity;
 *   - appending a new entry (packed, in chain-append order, per §5's
 *     "entries are packed and appended... a page fills up... and a new
 *     tail page is linked" -- but unlike the pending-list's O(1)-tail
 *     requirement, directory inserts are rare (bounded by the number of
 *     distinct structures an index ever has, not by row count), so this
 *     walks the chain to find the actual last page each time rather than
 *     caching a tail pointer -- see biscuit_common.h's BiscuitDirEntry
 *     comment: "Container-level addressing... is explicitly out of scope"
 *     -- directory-entry count is bounded the same way);
 *   - updating an existing entry's blob_head/pending_* fields in place,
 *     by (page, index) reference, per §5: "directory entries are mutated
 *     in place... entries are never relocated once written";
 *   - walking every entry in a column's chain (used by biscuit_persist.c's
 *     load/drop to enumerate the whole directory);
 *   - dropping every directory page across every populated column slot
 *     (index drop).
 *
 * What this layer does NOT own: the §3 size-threshold drain decision, any
 * CRUD call-site wiring, or anything about what a BiscuitDirEntry's
 * blob_head/pending_head chains actually contain -- that's
 * biscuit_blob.c's and (for whole-structure save/load) biscuit_persist.c's
 * job.
 */

#ifndef BISCUIT_DIR_H
#define BISCUIT_DIR_H

#include "biscuit_common.h"

/*
 * A stable handle to an existing directory entry's on-disk location,
 * returned by biscuit_dir_find() and consumed by biscuit_dir_update() to
 * mutate that same entry in place without re-searching the chain. Valid
 * only within the lifetime described by biscuit_dir_update()'s comment --
 * entries are never relocated once written (§5), so a ref taken now
 * remains valid for the entry's whole lifetime, but concurrent inserts by
 * other backends can still be landing on other pages in the same chain
 * without invalidating it.
 */
typedef struct BiscuitDirEntryRef
{
    BlockNumber blkno;
    int         index;      /* entry's position within that page's packed array */
} BiscuitDirEntryRef;

/*
 * biscuit_dir_slot_for_col
 * Maps a BiscuitDirEntry.col value to a BiscuitMetaPageData.dir_roots[]
 * array index: real column indices (>= 0) map to themselves; both
 * sentinels (BISCUIT_DIR_COL_LEGACY, BISCUIT_DIR_COL_SINGLETON) map to
 * slot 0 (see biscuit_common.h's comment on the sentinels for why sharing
 * slot 0 is safe).
 */
extern int biscuit_dir_slot_for_col(int32 col);

/*
 * biscuit_dir_find
 * Look up the directory entry with this exact identity. Returns false
 * (out_entry/out_ref untouched) if no such entry exists yet, or if the
 * column's chain hasn't been allocated at all. out_ref may be NULL if the
 * caller only needs the entry's current contents (e.g. a plain read),
 * not a handle for a later update.
 */
extern bool biscuit_dir_find(Relation index,
                              int32 col, bool is_lower, uint8 kind,
                              int32 ch, int32 position,
                              BiscuitDirEntry *out_entry,
                              BiscuitDirEntryRef *out_ref);

/*
 * biscuit_dir_update
 * Overwrite the entry at ref with *new_entry (identity fields included --
 * callers normally pass back the same identity plus updated blob_head/
 * pending_* fields, but this does not itself re-validate identity
 * equality; that's the caller's responsibility, exactly like handing back
 * an ItemPointer to update a known tuple). Single small GenericXLog
 * transaction registering only that one page.
 */
extern void biscuit_dir_update(Relation index,
                                const BiscuitDirEntryRef *ref,
                                const BiscuitDirEntry *new_entry);

/*
 * biscuit_dir_insert
 * Append a brand new entry (entry->col/is_lower/kind/ch/position give its
 * identity; caller must have already confirmed via biscuit_dir_find()
 * that no entry with this identity exists, since this never checks for
 * duplicates itself -- mirrors GIN/btree-style "caller does the
 * find-then-act split" rather than hiding a redundant search in here).
 * Allocates the column's chain root on first use, and a new tail page
 * when the current last page is full. out_ref, if non-NULL, is filled in
 * with a handle to the newly-inserted entry.
 */
extern void biscuit_dir_insert(Relation index,
                                const BiscuitDirEntry *entry,
                                BiscuitDirEntryRef *out_ref);

/*
 * biscuit_dir_upsert
 * find-then-(update|insert) convenience wrapper: if an entry with this
 * identity already exists, overwrite it with *entry's blob_head/pending_*
 * fields via biscuit_dir_update(); otherwise biscuit_dir_insert() it.
 * This is the primary entry point biscuit_persist.c's save path uses --
 * it doesn't need to know or care whether a structure already had a
 * directory entry.
 */
extern void biscuit_dir_upsert(Relation index, const BiscuitDirEntry *entry);

/*
 * biscuit_dir_foreach_column
 * Walk every entry in the chain rooted at dir_roots[slot] (lock-coupled
 * share-lock reads), calling cb(entry, state) for each one, in on-disk
 * chain order. slot must already be a valid BiscuitMetaPageData.dir_roots[]
 * index (see biscuit_dir_slot_for_col()); if dir_roots[slot] is
 * InvalidBlockNumber (nothing ever inserted for that slot), this is a
 * silent no-op.
 */
typedef void (*BiscuitDirWalkCallback)(const BiscuitDirEntry *entry, void *state);

extern void biscuit_dir_foreach_column(Relation index, int slot,
                                        BiscuitDirWalkCallback cb, void *state);

/*
 * biscuit_dir_num_slots
 * Returns the metapage's current num_dir_columns (how many dir_roots[]
 * slots have ever been populated) -- the upper bound callers should loop
 * slot 0..num_slots-1 over when they need to visit every column's chain
 * (biscuit_persist_load()/biscuit_persist_drop()).
 */
extern int biscuit_dir_num_slots(Relation index);

/*
 * biscuit_dir_drop_all
 * Retire every directory page across every populated dir_roots[] slot
 * (does NOT free the blob/pending chains the entries point at -- callers
 * that need "free everything a whole index owns" must walk the directory
 * with biscuit_dir_foreach_column() *before* calling this, freeing each
 * entry's blob_head/pending_head chains themselves, since this function
 * only knows about BISCUIT_PAGE_DIR pages). Resets every dir_roots[] slot
 * to InvalidBlockNumber and num_dir_columns to 0 in the metapage.
 */
extern void biscuit_dir_drop_all(Relation index);

#endif /* BISCUIT_DIR_H */
