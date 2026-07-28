/*
 * biscuit_blob.h
 *
 * WAL-logged page storage primitives: chunked compacted-blob chain I/O
 * plus the shared page allocator/retirement machinery every other
 * on-disk structure in Biscuit is built on.
 *
 * What lives here:
 *   - biscuit_page_write_blob() / biscuit_page_read_blob(): chunked,
 *     GenericXLog-wrapped compacted-blob chain I/O, FSM-integrated
 *     free/truncate of a superseded chain.
 *   - biscuit_page_alloc() / biscuit_page_free_blob(): page allocation
 *     from, and retirement to, Biscuit's own recycle_xid-gated freelist.
 *     Used by biscuit_dir.c, biscuit_pendlog.c and biscuit_rowstore.c for
 *     their own page kinds, not just by the blob chain.
 *   - biscuit_ensure_synchronous_commit(): forces xid assignment so
 *     GenericXLogFinish() gets a synchronous commit rather than silently
 *     taking the async path. Every durable mutation path calls this.
 *
 * Explicitly NOT here:
 *   - BiscuitDirEntry lookup/insert/update -- biscuit_dir.h.
 *   - The pending-delta log, its append path, its read-time snapshot and
 *     its drain -- biscuit_pendlog.h. Note that the per-structure pending
 *     chain primitives that used to live in this file
 *     (biscuit_pending_append(), biscuit_pending_append_with_dir(),
 *     biscuit_pending_drain(), and the BiscuitDrainStats they reported
 *     through) are GONE, along with the BISCUIT_PAGE_PENDING page format
 *     itself. They were superseded by the single index-wide shared log in
 *     biscuit_pendlog.c and were dead code by the time they were removed;
 *     see biscuit_pendlog.h's header for why the shared log replaced them.
 *   - The in-place row-identity storage (TIDS/STRCACHE/HEADER) --
 *     biscuit_rowstore.h.
 *
 * Locking: these primitives take only the page-level buffer locks they
 * need for their own chain mutation. Callers needing cross-structure
 * coordination own that themselves. The one ordering rule imposed here is
 * biscuit_page_alloc()'s: it takes the metapage lock internally, so no
 * caller may hold the metapage lock across a call into it (see
 * biscuit_pendlog_append()'s locking comment, which cost a self-deadlock
 * to get right).
 */

#ifndef BISCUIT_BLOB_H
#define BISCUIT_BLOB_H

/*
 * Durability note (all functions below): every durable mutation in this
 * file forces its own transaction to be assigned a real TransactionId
 * before entering its critical section (see biscuit_blob.c's
 * biscuit_ensure_synchronous_commit()). This is required for correctness:
 * PostgreSQL only performs a synchronous WAL flush at commit for
 * transactions that were assigned a real xid -- a transaction that wrote
 * WAL but has no xid silently takes the *asynchronous* commit path
 * instead (correct for genuinely-optional WAL like HOT pruning, wrong
 * for a durable index mutation). In production use, every call into this
 * file already happens inside a real DML/VACUUM operation that has
 * already assigned an xid via the enclosing heap operation, so this is
 * normally a no-op -- but each primitive guarantees it for itself rather
 * than assuming the caller did.
 */

/*
 * biscuit_ensure_synchronous_commit
 * Exported so other translation units performing their own durable
 * GenericXLog-based page mutations (biscuit_dir.c, biscuit_persist.c) can
 * apply the same guarantee without duplicating the reasoning above --
 * see this file's biscuit_blob.c definition for the full explanation.
 */
extern void biscuit_ensure_synchronous_commit(void);

#include "biscuit_common.h"
#include "biscuit_bitmap.h"

/* ==================== COMPACTED-BLOB CHUNK CHAIN ==================== */

/*
 * biscuit_page_write_blob
 *
 * Serialize `len` bytes of `data` (the caller's already-roaring-serialized
 * payload -- this function has no bitmap-specific logic, per §1) into a
 * brand new chunk chain, one BiscuitBlobChunkHeader-prefixed page per
 * chunk, linked via opaque.next.  Compacted chains are never mutated in
 * place (the design doc is explicit: a drain always writes a *new* chain
 * and swings the directory's blob_head to it -- see §3 step 5), so this
 * always allocates fresh pages; it never accepts an existing head to
 * overwrite.
 *
 * *out_head is set to the first chunk's BlockNumber on success. When
 * len == 0, *out_head is set to InvalidBlockNumber and no pages are
 * allocated (an empty/absent structure has no chain at all -- matches
 * BiscuitDirEntry.blob_head's InvalidBlockNumber-means-absent contract).
 *
 * Each chunk page is written as its own small GenericXLog transaction
 * (full-page image -- these are freshly initialized pages, so there is no
 * meaningful "before" image to diff against, same reasoning
 * biscuit_write_metadata_to_disk() already applies to its own P_NEW case).
 * A crash between chunk N and chunk N+1 leaves an orphaned partial chain
 * (some pages allocated, opaque.next of the last one is still
 * InvalidBlockNumber) that nothing points at yet, since *out_head is only
 * returned to the caller -- and therefore only becomes reachable via a
 * directory entry -- after this function returns successfully. An
 * orphaned partial chain from a crash mid-write is inert garbage (never
 * linked from anywhere) rather than a torn structure a reader could ever
 * reach; reclaiming that garbage is future FSM/directory-bootstrap work,
 * not a correctness requirement of this primitive.
 *
 * bytes_written, if non-NULL, is incremented by the total bytes physically
 * written across all chunk pages (headers + payload) -- instrumentation
 * for Phase 5's drain-threshold sizing.
 */
extern void biscuit_page_write_blob(Relation index,
                                     const char *data,
                                     uint32 len,
                                     BlockNumber *out_head,
                                     uint32 *bytes_written);

/*
 * biscuit_page_read_blob
 *
 * Walk the chunk chain rooted at `head` and reassemble it into a single
 * palloc'd buffer in the current memory context. head == InvalidBlockNumber
 * is treated as "empty blob": *out_data is set to NULL and *out_len to 0,
 * matching biscuit_page_write_blob()'s len==0 contract.
 *
 * Each chunk is read under BUFFER_LOCK_SHARE, lock-coupled: the next
 * chunk's lock is acquired before the current one is released, so a
 * concurrent drain can never recycle a page this reader is mid-walk
 * through out from under it (design doc, "Addressing review feedback"
 * point 1). total_len/total_chunks are cross-checked against every chunk
 * visited (design doc's "sanity-check itself" rationale for duplicating
 * them on every chunk); a mismatch raises ERROR rather than silently
 * returning a truncated/corrupt blob, since that indicates either a bug
 * or a directory entry pointing at a page that isn't the chain it claims
 * to be.
 */
extern void biscuit_page_read_blob(Relation index,
                                    BlockNumber head,
                                    char **out_data,
                                    uint32 *out_len);

/*
 * biscuit_page_free_blob
 *
 * Retire an entire superseded blob chain: walk it, stamp every page's
 * opaque.recycle_xid with the current transaction ID, and link each page
 * into the index's own deferred-recycle freelist (BiscuitMetaPageData
 * .fsm_root -- see its comment; NOT the ordinary Postgres index FSM
 * directly, since a page with recycle_xid set must not be handed out to a
 * new chain until biscuit_vacuumcleanup()'s horizon check clears it --
 * design doc, "Addressing review feedback" point 1). Safe to call with
 * head == InvalidBlockNumber (no-op).
 *
 * This performs its own small GenericXLog transaction per retired page
 * (each page's opaque area plus the metapage's fsm_root/fsm_page_count
 * link-in), matching the granularity biscuit_page_free_blob() (below)
 * uses for the same reason.
 */
extern void biscuit_page_free_blob(Relation index, BlockNumber head);

/*
 * biscuit_page_free_chain
 *
 * Alias for biscuit_page_free_blob() under a name that doesn't imply
 * "blob chains only" -- the retirement walk (biscuit_free_chain() in
 * biscuit_blob.c) never inspects page_kind, so it works identically on a
 * pendlog chain, a directory chain, or a blob chain. Callers outside this
 * file that are retiring a non-blob chain (e.g. the directory layer
 * freeing a drained pendlog chain, or a whole BISCUIT_PAGE_DIR chain on
 * index drop) should call this name instead, purely for readability at
 * the call site.
 */
extern void biscuit_page_free_chain(Relation index, BlockNumber head);

/*
 * biscuit_page_alloc
 *
 * Return a fresh, BUFFER_LOCK_EXCLUSIVE-locked buffer for the caller to
 * PageInit() as page_kind (one of the BISCUIT_PAGE_* tags). First tries
 * to pop a page off the deferred-recycle freelist that
 * biscuit_page_free_blob()/biscuit_page_free_chain() push retired pages
 * onto (BiscuitMetaPageData.fsm_root), reusing it once its
 * opaque.recycle_xid clears the oldest-still-running-transaction
 * horizon; falls back to extending the relation (P_NEW) when the
 * freelist is empty or its head isn't old enough yet.
 *
 * This is the counterpart to biscuit_page_free_blob()/_free_chain(): use
 * it at every allocation site that used to call
 * ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL) for a
 * blob chunk, pendlog, or directory page, EXCEPT where the caller
 * already holds the metapage buffer lock itself (this function acquires
 * that lock internally to peek/pop the freelist, so calling it while
 * already holding that same lock will self-deadlock -- see
 * biscuit_dir_ensure_root() in biscuit_dir.c for the one such site,
 * which deliberately keeps plain P_NEW instead).
 *
 * Locking: only ever inspects the freelist head (LIFO), never walks
 * deeper; see biscuit_blob.c for the full target-then-metapage lock
 * ordering rationale relative to the retirement path.
 */
extern Buffer biscuit_page_alloc(Relation index, uint16 page_kind);

#endif /* BISCUIT_BLOB_H */
