/*
 * biscuit_rowstore.h
 *
 * In-place, single-page/single-slot storage for BiscuitIndex's
 * "row identity" state: the TID array (slot_idx -> heap ItemPointer),
 * the per-(column, is_lower) string cache (slot_idx -> C string), and the
 * scalar HEADER blob.
 *
 * Why this file exists
 * ---------------------
 * Before this module, all three of these were persisted the same way as
 * the bitmap structures' compacted blobs: biscuit_page_write_blob()
 * serializes the *entire* array/blob into a brand new chunk chain and
 * retires the old one. That is the right primitive for something that is
 * genuinely re-encoded as a whole (a RoaringBitmap after a drain merges
 * its pending deltas), but TIDS/STRCACHE are just flat per-record arrays
 * where a steady-state INSERT touches exactly one element -- rewriting
 * the whole array on every commit made per-commit cost O(num_records)
 * instead of O(1), which is the root cause the "Biscuit Row-Identity
 * In-Place Storage" implementation summary measured directly (300
 * single-row transactions cost 70x more WAL/row than the same 300 rows
 * batched into one transaction).
 *
 * This module fixes that by giving TIDS and STRCACHE genuine random-access
 * storage:
 *
 *   - TIDS: a directory of fixed-size BISCUIT_PAGE_TIDSLOT pages, each
 *     holding a dense ItemPointerData[] array. slot_idx maps to
 *     (logical_page, offset) = (slot_idx / N, slot_idx % N); logical_page
 *     maps to a physical BlockNumber via an append-only
 *     BISCUIT_PAGE_PAGEDIR chain (see biscuit_common.h). Writing one slot
 *     touches exactly one TIDSLOT page (plus, only the first time a new
 *     logical page is needed, one PAGEDIR append) -- O(1) per write,
 *     never a rewrite of anything else.
 *
 *   - STRCACHE: the same PAGEDIR-of-fixed-slot-pages scheme, but each slot
 *     is a BiscuitStrPtr (blkno/offset/length) pointing into a companion
 *     append-only value heap (BISCUIT_PAGE_STRHEAP, bump-allocated at a
 *     tracked tail, same pattern as biscuit_pendlog_append()'s tail-page
 *     handling). A write is always: append the new bytes to the heap
 *     (or, if they don't fit on one heap page, spill to a dedicated
 *     biscuit_page_write_blob() chain), then overwrite one pointer slot
 *     in place. O(1) amortized, no rewrite-shaped operation anywhere.
 *     Freelist-slot reuse orphans the *old* bytes in the heap (dead
 *     space); reclaiming that is the deferred "STRCACHE value-heap
 *     compaction" work, out of scope here.
 *
 *   - HEADER: small enough that its cost was never the problem, but once
 *     the single-page in-place write primitive exists for TIDS/STRCACHE
 *     it costs nothing extra to give HEADER the same treatment instead of
 *     leaving it on the chain-reallocate-every-commit path -- it becomes
 *     one fixed page, overwritten in place, never re-chained.
 *
 * All three new logical-page-directory-based structures reuse
 * biscuit_blob.h's biscuit_page_alloc()/biscuit_ensure_synchronous_commit()
 * for page (de)allocation and durability, exactly like biscuit_dir.c does
 * for its own packed-entry chain -- no new allocation or WAL-durability
 * primitive is introduced here, only a new page layout built on the
 * existing ones.
 *
 * What this file does NOT own: which BiscuitDirEntry identity
 * (col/is_lower/kind) a given TIDS/STRCACHE/HEADER structure lives under,
 * or when to call these functions -- that's biscuit_persist.c (directory
 * bookkeeping + the read side used by cold load) and biscuit_index.c (the
 * exact CRUD call sites: fresh insert, freelist-slot reuse, and the
 * page-boundary-crossing allocation case -- see biscuit_persist.h's
 * biscuit_persist_row_identity_write_record() for the entry point those
 * call sites actually use).
 *
 * Format cutover
 * ---------------
 * The on-disk layout of TIDS/STRCACHE/HEADER changes incompatibly here,
 * with no dual-format reader and no BISCUIT_VERSION bump. Both of those
 * are deliberate and have the same single justification: version 3
 * (BISCUIT_LIBRARY_VERSION "3.0.0 - Player") was never shipped. There is
 * no deployed index anywhere written by the old TIDS/STRCACHE blob layout,
 * so there is nothing for a version bump to distinguish this format
 * *from*, and nothing for a compatibility path to read. The only readers
 * of the old layout that ever existed live in this same unreleased tree,
 * and this change replaces them rather than keeping them alongside. An
 * index built from an intermediate development checkout must be
 * REINDEXed -- the same expectation every prior format change in this
 * tree has carried.
 */

#ifndef BISCUIT_ROWSTORE_H
#define BISCUIT_ROWSTORE_H

#include "biscuit_common.h"

/* ==================== TIDS ==================== */

/*
 * biscuit_rowstore_tid_write
 *
 * Durably write idx->tids[slot_idx] equivalent data (*tid) into its
 * logical slot, in place. *pagedir_root is the TIDS directory entry's
 * blob_head (repurposed as a BISCUIT_PAGE_PAGEDIR root -- see
 * biscuit_common.h's "Field repurposing" comment); in/out because the
 * very first write for a fresh index allocates the root and the caller
 * (biscuit_persist_row_identity_write_record()) must persist the new
 * value back into the directory entry.
 *
 * Covers all three mutation cases the implementation summary calls out
 * uniformly: a slot on an already-allocated logical page is a single
 * in-place GenericXLog write to that one TIDSLOT page; a slot whose
 * logical page doesn't exist yet allocates a fresh TIDSLOT page and
 * appends it to the PAGEDIR chain. There is no separate "fresh insert"
 * vs "freelist-slot reuse" distinction at this layer -- both look
 * identical here (overwrite slot_idx), the distinction only matters one
 * layer up for which in-memory bitmaps also need updating.
 */
extern void biscuit_rowstore_tid_write(Relation index,
                                        BlockNumber *pagedir_root,
                                        uint32 slot_idx,
                                        const ItemPointerData *tid);

/*
 * biscuit_rowstore_tid_read
 * Read a single slot. Returns an invalid ItemPointer (via
 * ItemPointerSetInvalid()) if pagedir_root is InvalidBlockNumber or the
 * slot's logical page was never allocated (never written).
 */
extern void biscuit_rowstore_tid_read(Relation index,
                                       BlockNumber pagedir_root,
                                       uint32 slot_idx,
                                       ItemPointerData *out_tid);

/*
 * biscuit_rowstore_tid_read_all
 * Bulk read of slots [0, num_records) into a caller-supplied, already
 * appropriately-sized out_tids array -- the biscuit_persist_load() cold
 * load path. Walks the PAGEDIR chain once and each referenced TIDSLOT
 * page once (sequential, proportional to num_records -- no replay, no
 * rewrite, matching the implementation summary's "cold load should be
 * trivially flat" validation requirement). ERRORs if the directory
 * doesn't have enough slots to cover num_records (corrupt/truncated
 * directory).
 */
extern void biscuit_rowstore_tid_read_all(Relation index,
                                           BlockNumber pagedir_root,
                                           uint32 num_records,
                                           ItemPointerData *out_tids);

/* ==================== STRCACHE ==================== */

/*
 * biscuit_rowstore_str_write
 *
 * Durably write the string cache entry for slot_idx, in place.
 * *ptr_pagedir_root / *heap_head / *heap_tail are the STRCACHE directory
 * entry's blob_head / strheap_head / strheap_tail fields (see
 * biscuit_common.h); all three are in/out for the same reason as
 * biscuit_rowstore_tid_write()'s pagedir_root.
 *
 * len < 0 means NULL (absent) -- writes a BiscuitStrPtr with
 * blkno == InvalidBlockNumber and touches neither the pointer-array
 * PAGEDIR nor the value heap beyond the one pointer-slot write. len >= 0
 * (str non-NULL, including the empty string) appends `len` bytes to the
 * value heap (or, if larger than one heap page's payload, to a dedicated
 * biscuit_page_write_blob() chain -- see BiscuitStrPtr's
 * BISCUIT_STRPTR_OVERSIZE_SENTINEL comment) and then writes the resulting
 * BiscuitStrPtr into its slot, same "logical page exists vs needs
 * allocating" split as TIDS.
 */
extern void biscuit_rowstore_str_write(Relation index,
                                        BlockNumber *ptr_pagedir_root,
                                        BlockNumber *heap_head,
                                        BlockNumber *heap_tail,
                                        uint32 slot_idx,
                                        const char *str,
                                        int32 len);

/*
 * biscuit_rowstore_str_read
 * Read a single slot's string, palloc'd in cxt. Returns NULL for an
 * absent/NULL entry or an unallocated pointer-array logical page (never
 * written). out_len, if non-NULL, receives the byte length (0 for both
 * NULL and a legal empty string -- use the return value, not *out_len, to
 * tell them apart).
 */
extern char *biscuit_rowstore_str_read(Relation index,
                                        BlockNumber ptr_pagedir_root,
                                        uint32 slot_idx,
                                        MemoryContext cxt,
                                        int *out_len);

/*
 * biscuit_rowstore_str_read_all
 *
 * Bulk read of slots [0, num_records) into a caller-supplied, already
 * appropriately-sized out_arr (each element palloc'd in cxt; NULL for an
 * absent entry) -- the biscuit_persist_load() cold-load path, and the
 * direct counterpart to biscuit_rowstore_tid_read_all().
 *
 * Exists because calling biscuit_rowstore_str_read() in a loop is
 * measurably slower than the single-blob read it replaced (~1.8x on cold
 * load): per slot it re-walks the pointer-array page directory from its
 * root, re-reads that slot's STRPTR page, and then reads the value-heap
 * page -- three buffer acquisitions per record where the old concatenated
 * blob needed one sequential pass in total. This function amortizes all
 * three: it walks the directory chain once, reads each STRPTR page once,
 * and holds the most recently used value-heap buffer across slots, which
 * matters because the heap is bump-allocated in slot order, so runs of
 * consecutive slots almost always resolve to the same heap page.
 *
 * Oversized values (BISCUIT_STRPTR_OVERSIZE_SENTINEL) still take their own
 * blob-chain read, as they must -- they are rare by construction.
 */
extern void biscuit_rowstore_str_read_all(Relation index,
                                           BlockNumber ptr_pagedir_root,
                                           uint32 num_records,
                                           MemoryContext cxt,
                                           char **out_arr);

/* ==================== HEADER ==================== */

/*
 * biscuit_rowstore_header_write
 * Overwrite the single fixed HEADER page in place, allocating it on the
 * first call (*head_inout starts as InvalidBlockNumber). ERRORs if len
 * exceeds BiscuitHeaderMaxPayload(BLCKSZ) (only realistically reachable
 * with an extreme number of indexed columns -- see the macro's comment).
 */
extern void biscuit_rowstore_header_write(Relation index,
                                           BlockNumber *head_inout,
                                           const char *data,
                                           uint32 len);

/*
 * biscuit_rowstore_header_read
 * Read the HEADER page back into a freshly palloc'd buffer in the current
 * memory context. head == InvalidBlockNumber yields *out_data = NULL,
 * *out_len = 0 (matches biscuit_page_read_blob()'s empty-blob contract).
 */
extern void biscuit_rowstore_header_read(Relation index,
                                          BlockNumber head,
                                          char **out_data,
                                          uint32 *out_len);

/* ==================== TEARDOWN ==================== */

/*
 * biscuit_rowstore_free_tid_chain / _free_str_chains
 *
 * Retire every page a TIDS/STRCACHE structure owns -- not just the
 * directory-entry-visible root(s), but everything reachable from them
 * (the PAGEDIR chain itself, every TIDSLOT/STRPTR page it points at, and
 * for STRCACHE, the value-heap chain too). Used by biscuit_persist.c's
 * drop path in place of a bare biscuit_page_free_blob(entry->blob_head)
 * call, since for these two kinds blob_head is only the *directory*, not
 * the whole structure.
 *
 * Known gap, flagged rather than silently swallowed: a STRPTR slot
 * pointing at an oversized-value blob chain (BISCUIT_STRPTR_OVERSIZE_SENTINEL)
 * is not walked and freed here -- doing so would require reading every
 * STRPTR page and branching per-slot during teardown. Rare in practice
 * (only strings wider than one heap page's payload, a few KB) and,
 * because these pages are process-invisible once the owning relation is
 * gone, only matters for a REINDEX/DROP INDEX that reuses the relation's
 * own storage rather than unlinking the file outright. Same category of
 * deferred cleanup as the STRCACHE value-heap compaction work.
 */
extern void biscuit_rowstore_free_tid_chain(Relation index, BlockNumber pagedir_root);
extern void biscuit_rowstore_free_str_chains(Relation index,
                                              BlockNumber ptr_pagedir_root,
                                              BlockNumber heap_head);
extern void biscuit_rowstore_free_header(Relation index, BlockNumber head);

#endif /* BISCUIT_ROWSTORE_H */
