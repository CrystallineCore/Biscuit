/*
 * biscuit_blob.c
 *
 * See biscuit_blob.h for the full contract of every function here, and
 * the design doc ("Biscuit WAL-Logged Storage: Pending-List Design") for
 * the protocol these primitives implement (§1-§3, §6).
 *
 * Page layout used by the compacted-blob chunk chain: standard Postgres
 * page header, then a small fixed BiscuitBlobChunkHeader written directly
 * at the start of the data area, followed by its variable-length raw
 * serialized payload, and finally a PageAddSpecial-style
 * BiscuitPageOpaqueData footer. This is deliberately not using
 * PageAddItem()/item pointers at all -- see the
 * BiscuitBlobChunkMaxPayload() macro in
 * biscuit_common.h, which sizes against exactly this layout. pd_lower is
 * kept in sync with how much of the data area is actually used purely so
 * page-level tooling (pg_filedump-style inspection, amcheck) sees a
 * sane-looking page, matching biscuit_common.h's own stated goal for
 * BiscuitPageOpaqueData; nothing in this file itself relies on pd_lower
 * for correctness, since it never uses the item-pointer machinery.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_blob.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"      /* LockRelationForExtension() /
                                  UnlockRelationForExtension(), used by
                                  biscuit_page_alloc()'s P_NEW path below */
#include "storage/procarray.h"   /* GetOldestNonRemovableTransactionId(), used
                                   * by biscuit_page_alloc()'s recycle_xid
                                   * horizon check below */

/*
 * biscuit_ensure_synchronous_commit
 *
 * PostgreSQL's RecordTransactionCommit() only performs a synchronous
 * XLogFlush() -- i.e. only actually waits for the WAL this transaction
 * wrote to reach durable storage before returning control to the caller
 * -- when the transaction was assigned a real TransactionId ("markXidCommitted").
 * A transaction that wrote WAL (via GenericXLogFinish(), in our case) but
 * was never assigned an xid takes the *asynchronous* commit path instead
 * (see xact.c: "This enables possible committed transaction loss in the
 * case of a postmaster crash because WAL buffers are left unwritten.").
 * That path exists deliberately for genuinely-optional WAL like HOT
 * pruning, where losing it after a crash is harmless -- but every
 * mutation in this file is a real, caller-visible durable change (a
 * compacted-blob write or a page allocation/retirement), so silently
 * downgrading to async commit would violate this file's whole contract.
 *
 * In production use every call into this file happens from inside
 * biscuit_insert()/biscuit_bulkdelete()/etc., which already assign a real
 * xid via the enclosing heap operation, so this would likely never fire
 * in practice -- but a page-storage primitive shouldn't rely on its
 * caller happening to have done that for unrelated reasons. Calling
 * GetCurrentTransactionId() forces one to be assigned (idempotent within
 * a transaction) *before* the critical section below, which is exactly
 * where it needs to happen -- assigning an xid is itself not something
 * that can be done inside a critical section.
 */
void
biscuit_ensure_synchronous_commit(void)
{
    (void) GetCurrentTransactionId();
}

/* ==================== PAGE LAYOUT HELPERS ==================== */

#define BiscuitPageDataPtr(page)  ((char *) (page) + SizeOfPageHeaderData)

static inline void
BiscuitPageSetLower(Page page, Size used)
{
    ((PageHeader) page)->pd_lower = (LocationIndex) (SizeOfPageHeaderData + used);
}

/* ==================== DEFERRED-RECYCLE RETIREMENT ====================
 *
 * Stamps a page dead (recycle_xid) and links it into the index's own
 * BiscuitMetaPageData.fsm_root chain -- NOT the ordinary Postgres index
 * FSM directly. See biscuit_blob.h's biscuit_page_free_blob() comment and
 * the metapage's fsm_root field comment in biscuit_common.h for why: a
 * page with recycle_xid set must stay off-limits to new chains until
 * biscuit_vacuumcleanup()'s horizon check clears it (future phase; not
 * implemented here -- this file only ever pushes onto that freelist, it
 * never pops from it).
 *
 * Lock ordering: caller must already hold BUFFER_LOCK_EXCLUSIVE on
 * target_buf before calling. This function then acquires the metapage's
 * lock -- always target-page-then-metapage, the only order this file ever
 * uses for this pair, so there is no cross-order deadlock risk between
 * concurrent retirements.
 */
static void
biscuit_retire_page_locked(Relation index, Buffer target_buf)
{
    Buffer               mbuf;
    Page                 mpage;
    Page                 tpage;
    BiscuitMetaPageData *meta;
    BiscuitPageOpaque    opaque;
    GenericXLogState    *state;
    BlockNumber          target_blkno = BufferGetBlockNumber(target_buf);

    biscuit_ensure_synchronous_commit();

    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);

    state = GenericXLogStart(index);
    tpage = GenericXLogRegisterBuffer(state, target_buf, 0);
    mpage = GenericXLogRegisterBuffer(state, mbuf, 0);

    meta = (BiscuitMetaPageData *) PageGetSpecialPointer(mpage);

    opaque              = (BiscuitPageOpaque) PageGetSpecialPointer(tpage);
    opaque->recycle_xid = GetCurrentTransactionId();
    opaque->next        = meta->fsm_root;     /* opaque.next repurposed as
                                                * the freelist link -- this
                                                * page is no longer part of
                                                * whatever chain it used to
                                                * belong to. */

    meta->fsm_root = target_blkno;
    meta->fsm_page_count++;

    GenericXLogFinish(state);
    UnlockReleaseBuffer(mbuf);
}

/*
 * biscuit_page_alloc
 *
 * Return a fresh, BUFFER_LOCK_EXCLUSIVE-locked buffer for the caller to
 * PageInit() and populate as page_kind (BISCUIT_PAGE_BLOB/_PENDING/_DIR).
 * First tries to pop a page off BiscuitMetaPageData.fsm_root -- the
 * deferred-recycle freelist biscuit_retire_page_locked() pushes onto --
 * and only falls back to extending the relation (P_NEW) when the
 * freelist is empty or its head isn't old enough yet to be safely
 * reused.
 *
 * Only the *head* of the freelist is ever considered. The list is a
 * LIFO stack (retirement pushes at the head), so the head is always the
 * most-recently-retired page and thus the one *least* likely to have
 * cleared the recycle_xid horizon; older, already-safe pages can be
 * sitting deeper in the chain. This is a deliberate simplicity
 * trade-off, not an oversight: walking/splicing an arbitrary interior
 * node would need a prev-pointer walk under the metapage lock, and
 * since retirement and consumption roughly balance out over time (every
 * drain retires pages, every subsequent allocation tries to reclaim
 * one), the freelist stays bounded rather than growing unboundedly the
 * way it does today, even without picking the globally-oldest
 * candidate. A future pass could walk deeper if the head is still too
 * new.
 *
 * Locking: never holds the metapage lock and a candidate page's lock at
 * the same time in the metapage-then-target order, to avoid deadlocking
 * against biscuit_retire_page_locked()'s target-then-metapage order.
 * Instead: peek the head under a short metapage lock, drop it, lock the
 * candidate page on its own (target-first), then re-acquire the
 * metapage lock (now target-then-metapage, consistent with retire) and
 * re-validate that the candidate is still the head before splicing it
 * out. Loses the race and falls through to P_NEW on any mismatch or
 * horizon miss, rather than looping/spinning.
 *
 * Recycle safety: a candidate's recycle_xid must both predate
 * GetOldestNonRemovableTransactionId()'s horizon *and* not be our own
 * current transaction id. The second check is not redundant with the
 * first -- see the comment at the check itself for why VACUUM
 * (PROC_IN_VACUUM) can make the horizon function return a value past our
 * own still-open xid, which would otherwise let a single drain hand a
 * page it just retired for one structure straight back out for another
 * before the first structure's directory entry -- still pointing at the
 * old chain until *that* entry's own turn in the drain -- has been
 * caught up.
 */
Buffer
biscuit_page_alloc(Relation index, uint16 page_kind)
{
    Buffer       mbuf;
    Page         mpage;
    BiscuitMetaPageData *meta;
    BlockNumber  candidate;
    Buffer       cbuf;
    Page         cpage;
    BiscuitPageOpaque copaque;
    TransactionId horizon;
    BlockNumber  saved_next;

    /* 1. Peek the freelist head. */
    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_SHARE);
    mpage = BufferGetPage(mbuf);
    meta  = (BiscuitMetaPageData *) PageGetSpecialPointer(mpage);
    candidate = meta->fsm_root;
    UnlockReleaseBuffer(mbuf);

    if (candidate == InvalidBlockNumber)
        goto extend;

    /* 2. Lock the candidate on its own (target-first, no metapage held). */
    cbuf = ReadBuffer(index, candidate);
    LockBuffer(cbuf, BUFFER_LOCK_EXCLUSIVE);
    cpage   = BufferGetPage(cbuf);
    copaque = (BiscuitPageOpaque) PageGetSpecialPointer(cpage);

    /*
     * Is it old enough that no concurrent scan could still be on it?
     *
     * GetOldestNonRemovableTransactionId()'s result is *not* guaranteed to
     * be <= our own transaction's xid. During VACUUM -- the only caller
     * that reaches this path with wait=true, via biscuit_pendlog_drain_all()
     * -- the backend has PROC_IN_VACUUM set for the whole run, and
     * ComputeXidHorizons() (which this function calls under the hood)
     * deliberately skips any PGPROC entry with that flag set, including
     * our own, precisely so a long VACUUM isn't held back by its own xmin.
     * The intended effect is that VACUUM can prune rows it itself deleted
     * earlier in the same run; the side effect here is that the "horizon"
     * this function returns is no longer bounded above by our own
     * in-progress xid the way it would be for an ordinary backend, and can
     * run right past it.
     *
     * That matters because biscuit_pendlog_drain_all() retires a
     * structure's old blob chain (biscuit_retire_page_locked(), which
     * stamps recycle_xid = GetCurrentTransactionId()) and then goes on to
     * write compacted blobs for every *other* structure in the same
     * drain, each write routing through this function. Without the extra
     * check below, a page this very drain retired moments ago -- for a
     * directory entry biscuit_pendlog_drain_all() hasn't reached yet, and
     * whose old blob_head still points at it -- can look "old enough" and
     * get handed straight back out and overwritten for an unrelated
     * structure's new chunk before the entry that still owns it has had a
     * chance to read it. That is exactly the "chunk page recycled off the
     * freelist while a live chain still references it" pattern behind the
     * intermittent "blob chunk chain inconsistency" corruption (D1):
     * silent within the drain that causes it, only surfacing later --
     * sometimes in that same VACUUM when a not-yet-processed entry is
     * finally read, sometimes only in a later reader -- as a chunk whose
     * seq/total_len/total_chunks no longer match what the chain's owner
     * expects.
     *
     * TransactionIdIsCurrentTransactionId() is a hard backstop that does
     * not depend on what ComputeXidHorizons() decides: a page we ourselves
     * retired in this same still-open transaction is never eligible for
     * reuse by us, full stop, regardless of how the horizon function
     * treats our own PGPROC entry. Cross-transaction reuse (the normal
     * case -- a page freed by an earlier, already-committed VACUUM) is
     * unaffected, since recycle_xid there is never our current xid.
     */
    horizon = GetOldestNonRemovableTransactionId(NULL);
    if (!TransactionIdIsValid(copaque->recycle_xid) ||
        TransactionIdIsCurrentTransactionId(copaque->recycle_xid) ||
        !TransactionIdPrecedes(copaque->recycle_xid, horizon))
    {
        UnlockReleaseBuffer(cbuf);
        goto extend;
    }
    saved_next = copaque->next;   /* the freelist link, captured before we
                                    * touch anything under the metapage
                                    * lock below */

    /* 3. Re-lock the metapage (target-then-metapage) and re-validate. */
    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
    mpage = BufferGetPage(mbuf);
    meta  = (BiscuitMetaPageData *) PageGetSpecialPointer(mpage);

    if (meta->fsm_root != candidate)
    {
        /* Lost the race to another allocator; give up and extend. */
        UnlockReleaseBuffer(mbuf);
        UnlockReleaseBuffer(cbuf);
        goto extend;
    }

    {
        GenericXLogState *state;

        biscuit_ensure_synchronous_commit();
        state = GenericXLogStart(index);
        mpage = GenericXLogRegisterBuffer(state, mbuf, 0);
        cpage = GenericXLogRegisterBuffer(state, cbuf, GENERIC_XLOG_FULL_IMAGE);

        meta = (BiscuitMetaPageData *) PageGetSpecialPointer(mpage);
        meta->fsm_root = saved_next;
        meta->fsm_page_count--;

        /*
         * Neutral placeholder state; the caller PageInit()s this page
         * (in its own subsequent GenericXLog transaction) before writing
         * real content, so this only matters if something inspects the
         * page in between -- keep it looking like a normal, non-dead
         * page of the requested kind rather than a half-retired one.
         */
        copaque              = (BiscuitPageOpaque) PageGetSpecialPointer(cpage);
        copaque->recycle_xid = InvalidTransactionId;
        copaque->next        = InvalidBlockNumber;
        copaque->page_kind   = page_kind;
        copaque->flags       = 0;

        GenericXLogFinish(state);
        UnlockReleaseBuffer(mbuf);
    }

    /* Caller gets cbuf back still EXCLUSIVE-locked, ready for PageInit(). */
    return cbuf;

extend:
    /*
     * Extending the relation needs the standard extension lock, the same
     * way every other in-core index AM's P_NEW path does (see e.g.
     * _bt_getbuf(), ginNewBuffer(), _hash_getnewbuf()). Without it, two
     * backends racing to extend at the same moment -- a foreground
     * inserter and autovacuum, or two concurrent sessions -- can both be
     * handed the *same* new block number: nothing here was serializing
     * that decision. Whoever writes second silently overwrites the
     * first's chunk, which is indistinguishable, from a later reader's
     * point of view, from the WAL-atomicity gap this file used to have
     * in biscuit_page_write_blob() -- same "expected seq" mismatch,
     * different cause. This one only fires when a second backend
     * genuinely overlaps the extension instant, which is why it surfaces
     * far less often than the bug that used to dominate here, but it's
     * not zero.
     *
     * RBM_ZERO_AND_LOCK (rather than RBM_NORMAL + a separate LockBuffer()
     * call) makes "zero-fill the new page" and "take the exclusive
     * content lock" a single atomic step with respect to other backends,
     * instead of two, closing the window between them.
     */
    LockRelationForExtension(index, ExclusiveLock);
    cbuf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_ZERO_AND_LOCK, NULL);
    UnlockRelationForExtension(index, ExclusiveLock);
    return cbuf;
}

/*
 * Walk a chain (the retirement logic doesn't care about page kind)
 * from head to its end via the *original* opaque.next links,
 * retiring every page. Increments *pages_freed (if non-NULL) by the
 * number of pages retired. No-op if head is InvalidBlockNumber.
 */
static void
biscuit_free_chain(Relation index, BlockNumber head, uint32 *pages_freed)
{
    BlockNumber cur   = head;
    uint32      count = 0;

    while (cur != InvalidBlockNumber)
    {
        Buffer            buf = ReadBuffer(index, cur);
        Page              page;
        BiscuitPageOpaque opaque;
        BlockNumber       next;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page   = BufferGetPage(buf);
        opaque = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        next   = opaque->next;   /* capture before retire repurposes it */

        biscuit_retire_page_locked(index, buf);

        UnlockReleaseBuffer(buf);
        cur = next;
        count++;
    }

    if (pages_freed)
        *pages_freed += count;
}

void
biscuit_page_free_blob(Relation index, BlockNumber head)
{
    if (head == InvalidBlockNumber)
        return;
    biscuit_free_chain(index, head, NULL);
}

void
biscuit_page_free_chain(Relation index, BlockNumber head)
{
    biscuit_page_free_blob(index, head);
}

/* ==================== COMPACTED-BLOB CHUNK CHAIN ==================== */

void
biscuit_page_write_blob(Relation index, const char *data, uint32 len,
                         BlockNumber *out_head, uint32 *bytes_written)
{
    uint32       maxpayload = BiscuitBlobChunkMaxPayload(BLCKSZ);
    uint32       nchunks;
    BlockNumber *blocks;
    uint32       total_bytes = 0;
    uint32       i;
    Size         specialSize = MAXALIGN(sizeof(BiscuitPageOpaqueData));
    Buffer       prevbuf     = InvalidBuffer;

    if (len == 0)
    {
        *out_head = InvalidBlockNumber;
        if (bytes_written)
            *bytes_written = 0;
        return;
    }

    biscuit_ensure_synchronous_commit();

    nchunks = (len + maxpayload - 1) / maxpayload;
    blocks  = (BlockNumber *) palloc(nchunks * sizeof(BlockNumber));

    /*
     * prevbuf is chunk i-1's buffer, held EXCLUSIVE-locked across loop
     * iterations (not released the moment it's written) specifically so
     * its opaque.next can be filled in as part of the *same* Generic WAL
     * record that initializes chunk i, rather than a separate record
     * afterwards.
     *
     * The previous version registered and finished each chunk's own
     * FULL_IMAGE init as one record, then -- only once the buffer had
     * been fully unlocked and released -- opened a second, independent
     * record to patch the *previous* chunk's opaque.next to point at it.
     * Generic WAL records are atomic individually but not as a pair: a
     * crash landing between those two records replays the first (chunk i
     * exists, initialized) but not the second (chunk i-1's next-pointer
     * update is lost). That is invisible as long as nothing yet points
     * at chunk i-1 -- true so far, since the chain is still under
     * construction -- but it stops being true the moment this function
     * returns *out_head to a caller that immediately links the *whole*
     * chain into a directory entry via biscuit_dir_update(), racing
     * nothing else. The real hazard was never a concurrent reader; it
     * was ordinary relation extension. `biscuit_page_alloc()`'s P_NEW
     * fallback (extending the relation) hands back a buffer full of
     * whatever bytes the OS/kernel happened to have there -- not
     * necessarily zeroes -- until this function's own FULL_IMAGE record
     * for that block is replayed. If chunk i's init record is durable
     * but chunk i-1's link-patch record is not (crash in between, or the
     * two records land on different sides of a recovery target), a
     * reader walking the chain later can dereference chunk i's raw,
     * pre-init buffer contents as if they were already a valid
     * BiscuitPageOpaqueData -- which is exactly how a stray run of bytes
     * that happens to spell the page magic ("BISC") gets read back as a
     * next-pointer (block number 0x42495343) instead of a real link, and
     * why the corruption clustered at the *end* of the relation (freshly
     * extended pages) with `expected seq` always 0 or 1 (the break is at
     * the first or second link, i.e. right where a brand-new page enters
     * the chain) rather than being spread across recycled interior pages.
     *
     * Registering both buffers in one GenericXLogStart/Finish makes
     * "initialize chunk i" and "link chunk i-1 to it" a single atomic
     * unit: replay either applies both or neither, so a chain can never
     * reference a page whose own initializing record hasn't also been
     * applied. Two buffers is well within Generic WAL's four-buffer
     * limit (MAX_GENERIC_XLOG_PAGES).
     */
    for (i = 0; i < nchunks; i++)
    {
        Buffer                   buf;
        Page                     page;
        Page                     pprev     = NULL;
        GenericXLogState        *state;
        BiscuitBlobChunkHeader  *hdr;
        BiscuitPageOpaque        opaque;
        uint32                   offset    = i * maxpayload;
        uint32                   chunk_len = Min(maxpayload, len - offset);

        buf = biscuit_page_alloc(index, BISCUIT_PAGE_BLOB);

        state = GenericXLogStart(index);
        page  = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
        if (prevbuf != InvalidBuffer)
            pprev = GenericXLogRegisterBuffer(state, prevbuf, 0);

        PageInit(page, BufferGetPageSize(buf), specialSize);

        hdr              = (BiscuitBlobChunkHeader *) BiscuitPageDataPtr(page);
        hdr->total_len    = len;
        hdr->total_chunks = nchunks;
        hdr->chunk_seq    = i;
        hdr->chunk_len    = chunk_len;
        memcpy((char *) hdr + MAXALIGN(sizeof(BiscuitBlobChunkHeader)),
               data + offset, chunk_len);

        BiscuitPageSetLower(page, MAXALIGN(sizeof(BiscuitBlobChunkHeader)) + chunk_len);

        opaque              = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        opaque->next        = InvalidBlockNumber;   /* patched next iteration,
                                                       * atomically with i+1's
                                                       * own init, once it
                                                       * exists */
        opaque->page_kind   = BISCUIT_PAGE_BLOB;
        opaque->flags       = 0;
        opaque->recycle_xid = InvalidTransactionId;

        if (pprev != NULL)
        {
            BiscuitPageOpaque popaque = (BiscuitPageOpaque) PageGetSpecialPointer(pprev);

            popaque->next = BufferGetBlockNumber(buf);
        }

        GenericXLogFinish(state);

        if (prevbuf != InvalidBuffer)
            UnlockReleaseBuffer(prevbuf);

        blocks[i]    = BufferGetBlockNumber(buf);
        total_bytes += MAXALIGN(sizeof(BiscuitBlobChunkHeader)) + chunk_len;

        /*
         * Keep buf locked -- it becomes next iteration's prevbuf, linked
         * in the same record as chunk i+1's init. The final chunk's
         * buffer is released after the loop.
         */
        prevbuf = buf;
    }

    if (prevbuf != InvalidBuffer)
        UnlockReleaseBuffer(prevbuf);

    *out_head = blocks[0];
    if (bytes_written)
        *bytes_written = total_bytes;
    pfree(blocks);
}

void
biscuit_page_read_blob(Relation index, BlockNumber head, char **out_data, uint32 *out_len)
{
    Buffer   buf;
    Page     page;
    BiscuitBlobChunkHeader *hdr;
    char    *result;
    uint32   total_len;
    uint32   total_chunks;
    uint32   written = 0;
    uint32   seq     = 0;
    BlockNumber cur  = head;

    if (head == InvalidBlockNumber)
    {
        *out_data = NULL;
        *out_len  = 0;
        return;
    }

    buf  = ReadBuffer(index, head);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    hdr  = (BiscuitBlobChunkHeader *) BiscuitPageDataPtr(page);

    total_len    = hdr->total_len;
    total_chunks = hdr->total_chunks;

    if (total_len == 0 || total_chunks == 0)
    {
        UnlockReleaseBuffer(buf);
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("biscuit: corrupt blob chunk chain at block %u (total_len=%u total_chunks=%u)",
                        head, total_len, total_chunks)));
    }

    result = (char *) palloc(total_len);

    for (;;)
    {
        BiscuitPageOpaque opaque;
        BlockNumber       next;

        page = BufferGetPage(buf);
        hdr  = (BiscuitBlobChunkHeader *) BiscuitPageDataPtr(page);

        if (hdr->total_len != total_len ||
            hdr->total_chunks != total_chunks ||
            hdr->chunk_seq != seq)
        {
            UnlockReleaseBuffer(buf);
            pfree(result);
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("biscuit: blob chunk chain inconsistency at block %u (expected seq %u)",
                            cur, seq)));
        }

        if (written + hdr->chunk_len > total_len)
        {
            UnlockReleaseBuffer(buf);
            pfree(result);
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("biscuit: blob chunk chain overflow at block %u", cur)));
        }

        memcpy(result + written,
               (char *) hdr + MAXALIGN(sizeof(BiscuitBlobChunkHeader)),
               hdr->chunk_len);
        written += hdr->chunk_len;

        opaque = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        next   = opaque->next;

        if (next == InvalidBlockNumber)
        {
            UnlockReleaseBuffer(buf);
            break;
        }

        /* Lock-coupled walk: acquire the next page before releasing the
         * current one, so a concurrent drain can never recycle a page
         * this read is mid-walk through (design doc, review point 1). */
        {
            Buffer nextbuf = ReadBuffer(index, next);

            LockBuffer(nextbuf, BUFFER_LOCK_SHARE);
            UnlockReleaseBuffer(buf);
            buf = nextbuf;
        }
        cur = next;
        seq++;
    }

    if (written != total_len)
    {
        pfree(result);
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("biscuit: blob chunk chain truncated (got %u of %u bytes, head block %u)",
                        written, total_len, head)));
    }

    *out_data = result;
    *out_len  = total_len;
}
