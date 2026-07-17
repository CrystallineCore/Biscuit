/*
 * biscuit_blob.h
 *
 * WAL-logged page storage primitives: compacted-blob chunk chain I/O and
 * pending-delta list append/drain.  See the design doc ("Biscuit WAL-Logged
 * Storage: Pending-List Design") for the full picture; this header covers
 * exactly the two on-disk components it describes in §1 -- nothing about
 * the directory (§5), the per-structure size trigger (§3's *decision* to
 * drain), or the delete fan-out (Round 4) lives here.  Callers own that
 * layer; these are the primitives it will be built on.
 *
 * Scope of this phase, deliberately:
 *   - biscuit_page_write_blob() / biscuit_page_read_blob(): chunked,
 *     GenericXLog-wrapped compacted-blob chain I/O, FSM-integrated
 *     free/truncate of a superseded chain.
 *   - biscuit_pending_append(): single small GenericXLog-registered write
 *     per call, appending one BiscuitPendingRecord to a pending chain's
 *     tail page, allocating a new tail page only when required.
 *   - biscuit_pending_drain(): apply every pending record to a caller-
 *     supplied in-memory RoaringBitmap, optionally re-serialize it as a
 *     new compacted-blob chain, and free the drained pending chain (and,
 *     if a blob rewrite was requested, the superseded blob chain) back to
 *     Biscuit's own recycle_xid-gated freelist.
 *
 * Explicitly NOT in scope here (future phases, per the design doc):
 *   - BiscuitDirEntry lookup/insert/update (§5).
 *   - The §3 size-threshold *decision* to call biscuit_pending_drain() --
 *     that belongs to the CRUD call sites in biscuit_index.c, which this
 *     phase does not touch.
 *   - biscuit_delete_record_and_free_slot() and the multi-structure lock
 *     ordering it requires (Round 4) -- single-structure primitives only.
 *   - biscuit_persist.c and any CRUD call sites -- untouched in this phase.
 *
 * Locking (§6): callers that need cross-structure coordination (e.g. the
 * future directory layer) are responsible for whatever directory-entry
 * lock protects BiscuitDirEntry.pending_tail / blob_head; these primitives
 * only take the page-level buffer locks they need for their own chain
 * mutation, nested exactly as §6 specifies for append (page-exclusive
 * lock held across the head/tail pointer update) and for drain (exclusive
 * across the full page set touched by that one structure's critical
 * section).
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
 * link-in), matching the granularity biscuit_pending_free_chain() (below)
 * uses for the same reason.
 */
extern void biscuit_page_free_blob(Relation index, BlockNumber head);

/*
 * biscuit_page_free_chain
 *
 * Alias for biscuit_page_free_blob() under a name that doesn't imply
 * "blob chains only" -- the retirement walk (biscuit_free_chain() in
 * biscuit_blob.c) never inspects page_kind, so it works identically on a
 * pending chain, a directory chain, or a blob chain. Callers outside this
 * file that are retiring a non-blob chain (e.g. the directory layer
 * freeing a drained pending chain, or a whole BISCUIT_PAGE_DIR chain on
 * index drop) should call this name instead, purely for readability at
 * the call site.
 */
extern void biscuit_page_free_chain(Relation index, BlockNumber head);

/* ==================== PENDING-DELTA LIST CHAIN ==================== */

/*
 * biscuit_pending_append
 *
 * Append one (value, op) delta to the pending chain identified by
 * *head / *tail, allocating the chain's first page (if *head is
 * InvalidBlockNumber) or a new tail page (if the current tail is full).
 * *head and *tail are both in/out: the caller (the future directory
 * layer) owns persisting BiscuitDirEntry.pending_head/pending_tail after
 * this call returns -- this primitive only tells you what they became,
 * it does not itself update any directory page, since the directory
 * doesn't exist yet in this phase (see file header "Scope").
 *
 * This is the hot path (§2: every CRUD mutation touching a structure's
 * bitmap does exactly this, and a single row's fan-out is on the order of
 * 2 * strlen(value) calls), so it is deliberately a single small
 * GenericXLog transaction registering only the one page being mutated
 * (§6's WAL-logging decision, "Addressing review feedback" point 3/4) --
 * never the metapage, never another structure's pages. Locking follows
 * §6's nested pattern exactly: the tail page's BUFFER_LOCK_EXCLUSIVE is
 * acquired first and space is re-checked *under* that lock (not before
 * it), so two backends racing to append to the same now-full page
 * serialize on that page's lock and the second one re-checks against the
 * (by-then-already-extended) page rather than acting on a stale
 * observation -- this closes the concurrent tail-allocation race the
 * design doc's Round 1 review flagged. If a new tail page is allocated,
 * it is linked via the old tail's opaque.next and *this function's own
 * exclusive lock on the old tail page is held across that step*, matching
 * §6's requirement that the old page's lock not be released until the
 * new page is fully linked.
 *
 * bytes_written, if non-NULL, is set to sizeof(BiscuitPendingRecord) plus
 * (when a new page was allocated) the page-header overhead -- Phase 5
 * drain-threshold-sizing instrumentation, per the append side of the
 * requested instrumentation.
 */
extern void biscuit_pending_append(Relation index,
                                    BlockNumber *head,
                                    BlockNumber *tail,
                                    uint32 value,
                                    uint8 op,
                                    uint32 *bytes_written);

/*
 * BiscuitDrainStats
 * Instrumentation filled in by biscuit_pending_drain() -- the data Phase 5
 * uses to size pending_list_limit / autovacuum drain cadence.
 */
typedef struct BiscuitDrainStats
{
    uint32 records_drained;        /* pending records applied to target   */
    uint32 pending_pages_freed;    /* pending-chain pages retired         */
    uint32 blob_bytes_written;     /* bytes of newly-written compacted
                                     * blob, 0 if no rewrite was requested */
    uint32 old_blob_pages_freed;   /* superseded blob-chain pages retired */
} BiscuitDrainStats;

/*
 * biscuit_pending_drain
 *
 * Apply every record in the pending chain rooted at `pending_head`, in
 * chain order, to `target` via the existing biscuit_roaring_add()/
 * biscuit_roaring_remove() (§3 step 2: no new mutation logic). The caller
 * is responsible for having already decoded the structure's *old*
 * compacted blob (if any) into `target` before calling this (e.g. via
 * biscuit_page_read_blob() + the existing roaring deserialize path) --
 * this function only ever appends deltas on top of whatever `target`
 * already holds, it never reads the blob chain itself.
 *
 * If do_blob_rewrite is true, this additionally performs §3 steps 3-5 as
 * one operation: re-serializes `target` (now fully merged) into a brand
 * new compacted-blob chain via biscuit_page_write_blob(), retires the
 * chain previously pointed at by *blob_head via biscuit_page_free_blob(),
 * and updates *blob_head to the new chain's head. If do_blob_rewrite is
 * false, *blob_head is left untouched and the caller is responsible for
 * persisting the merged `target` itself (e.g. a read-time merge-at-scan
 * caller per §4, which never rewrites anything).
 *
 * Either way, the drained pending chain's pages are always retired via
 * biscuit_page_free_blob()-equivalent handling (biscuit_pending_free_chain
 * internally) and *head_inout / *tail_inout (the pending chain's own
 * head/tail, distinct from *blob_head) are reset to InvalidBlockNumber --
 * again, it is the caller's job to persist that into the future
 * directory's pending_head/pending_tail/pending_count/pending_bytes.
 *
 * stats, if non-NULL, is filled in with counts for Phase 5 sizing.
 *
 * Crash safety: pending-record application to `target` is a pure
 * in-memory operation (no WAL, nothing durable yet). The durable part is
 * the optional blob rewrite and the two chains' retirement, each done
 * under its own critical section exactly like biscuit_page_write_blob()/
 * biscuit_page_free_blob() already guarantee individually. This means a
 * crash between "blob rewritten" and "pending chain retired" is possible
 * and is safe by construction: the pending chain, if the crash happens
 * before its retirement lands, is simply drained *again* on the next
 * attempt (idempotent -- reapplying the same ADD/REMOVE sequence to a
 * *freshly re-read* old blob yields the same result, since target is
 * never partially written back until the new blob chain's
 * GenericXLogFinish() has completed). A crash before the blob rewrite
 * lands leaves the old blob + full pending chain untouched, which is
 * exactly "drain didn't happen yet" -- also safe. There is no window
 * where a reader can observe a new blob with the pending records it came
 * from still also applied on top of it a second time from a *stale*
 * directory entry, because the directory's blob_head/pending_head swing
 * (making either of those states externally visible) is the future
 * directory layer's responsibility, entered only after this function
 * returns successfully -- see file header "Scope".
 */
extern void biscuit_pending_drain(Relation index,
                                   BlockNumber pending_head,
                                   BlockNumber *head_inout,
                                   BlockNumber *tail_inout,
                                   BlockNumber *blob_head,
                                   bool do_blob_rewrite,
                                   RoaringBitmap *target,
                                   BiscuitDrainStats *stats);

#endif /* BISCUIT_BLOB_H */
