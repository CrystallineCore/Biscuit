/*
 * biscuit_blob.c
 *
 * See biscuit_blob.h for the full contract of every function here, and
 * the design doc ("Biscuit WAL-Logged Storage: Pending-List Design") for
 * the protocol these primitives implement (§1-§3, §6).
 *
 * Page layout used by both chain types (blob chunk pages and pending
 * pages): standard Postgres page header, then a small fixed struct
 * (BiscuitBlobChunkHeader or BiscuitPendingPageHeader) written directly
 * at the start of the data area, followed by its variable-length payload
 * (raw serialized bytes, or a packed BiscuitPendingRecord[] array), and
 * finally a PageAddSpecial-style BiscuitPageOpaqueData footer. This is
 * deliberately not using PageAddItem()/item pointers at all -- see the
 * BiscuitBlobChunkMaxPayload()/BiscuitPendingPageMaxRecords() macros in
 * biscuit_common.h, which size against exactly this layout. pd_lower is
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
 * pending-list append or a compacted-blob rewrite), so silently
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

static inline Size
BiscuitPendingUsedBytes(uint32 num_records)
{
    return MAXALIGN(sizeof(BiscuitPendingPageHeader))
         + (Size) num_records * sizeof(BiscuitPendingRecord);
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

    /* Is it old enough that no concurrent scan could still be on it? */
    horizon = GetOldestNonRemovableTransactionId(NULL);
    if (!TransactionIdIsValid(copaque->recycle_xid) ||
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
    cbuf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(cbuf, BUFFER_LOCK_EXCLUSIVE);
    return cbuf;
}

/*
 * Walk a chain (blob or pending -- the retirement logic doesn't care
 * which) from head to its end via the *original* opaque.next links,
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

    for (i = 0; i < nchunks; i++)
    {
        Buffer                   buf;
        Page                     page;
        GenericXLogState        *state;
        BiscuitBlobChunkHeader  *hdr;
        BiscuitPageOpaque        opaque;
        uint32                   offset    = i * maxpayload;
        uint32                   chunk_len = Min(maxpayload, len - offset);

        buf = biscuit_page_alloc(index, BISCUIT_PAGE_BLOB);

        state = GenericXLogStart(index);
        page  = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

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
        opaque->next        = InvalidBlockNumber;   /* patched below once i+1 exists */
        opaque->page_kind   = BISCUIT_PAGE_BLOB;
        opaque->flags       = 0;
        opaque->recycle_xid = InvalidTransactionId;

        GenericXLogFinish(state);

        blocks[i]    = BufferGetBlockNumber(buf);
        total_bytes += MAXALIGN(sizeof(BiscuitBlobChunkHeader)) + chunk_len;

        UnlockReleaseBuffer(buf);

        if (i > 0)
        {
            /*
             * Nobody can be reading this brand-new chain yet (nothing
             * points at blocks[0] until this function returns -- see the
             * "orphaned partial chain" note in biscuit_blob.h), so this
             * second small transaction patching the previous chunk's
             * opaque.next needs no coordination with any concurrent
             * reader -- it's purely finishing this chain's own
             * construction.
             */
            Buffer             pbuf   = ReadBuffer(index, blocks[i - 1]);
            Page               ppage;
            GenericXLogState  *pstate;
            BiscuitPageOpaque  popaque;

            LockBuffer(pbuf, BUFFER_LOCK_EXCLUSIVE);
            pstate  = GenericXLogStart(index);
            ppage   = GenericXLogRegisterBuffer(pstate, pbuf, 0);
            popaque = (BiscuitPageOpaque) PageGetSpecialPointer(ppage);
            popaque->next = blocks[i];
            GenericXLogFinish(pstate);
            UnlockReleaseBuffer(pbuf);
        }
    }

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

/* ==================== PENDING-DELTA LIST CHAIN ==================== */

void
biscuit_pending_append(Relation index, BlockNumber *head, BlockNumber *tail,
                        uint32 value, uint8 op, uint32 *bytes_written)
{
    Buffer   buf;
    Page     page;
    BiscuitPendingPageHeader *hdr;
    Size     specialSize = MAXALIGN(sizeof(BiscuitPageOpaqueData));

    Assert(op == BISCUIT_PENDING_OP_ADD || op == BISCUIT_PENDING_OP_REMOVE);

    biscuit_ensure_synchronous_commit();

    if (*head == InvalidBlockNumber)
    {
        /* Brand new chain: allocate its first (and, for now, only) page. */
        GenericXLogState     *state;
        BiscuitPendingRecord *rec;
        BiscuitPageOpaque     opaque;

        buf = biscuit_page_alloc(index, BISCUIT_PAGE_PENDING);

        state = GenericXLogStart(index);
        page  = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

        PageInit(page, BufferGetPageSize(buf), specialSize);

        hdr             = (BiscuitPendingPageHeader *) BiscuitPageDataPtr(page);
        hdr->max_records = BiscuitPendingPageMaxRecords(BufferGetPageSize(buf));
        hdr->num_records = 0;

        rec = (BiscuitPendingRecord *) ((char *) hdr + MAXALIGN(sizeof(BiscuitPendingPageHeader)));
        rec[0].value = value;
        rec[0].op    = op;
        memset(rec[0].reserved, 0, sizeof(rec[0].reserved));
        hdr->num_records = 1;

        BiscuitPageSetLower(page, BiscuitPendingUsedBytes(1));

        opaque              = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        opaque->next        = InvalidBlockNumber;
        opaque->page_kind   = BISCUIT_PAGE_PENDING;
        opaque->flags       = BISCUIT_PENDING_FLAG_TAIL;
        opaque->recycle_xid = InvalidTransactionId;

        GenericXLogFinish(state);

        *head = *tail = BufferGetBlockNumber(buf);
        UnlockReleaseBuffer(buf);

        if (bytes_written)
            *bytes_written = MAXALIGN(sizeof(BiscuitPendingPageHeader)) + sizeof(BiscuitPendingRecord);
        return;
    }

    /*
     * Existing chain: lock the current tail page first, and re-check
     * space *under* that lock -- not before it (§6). This is what closes
     * the concurrent tail-allocation race: a second backend racing to
     * append to the same now-full page blocks on this lock and, once it
     * acquires it, sees whatever the winner left behind (either room on
     * this same page, or an already-linked new tail page to follow).
     */
    buf = ReadBuffer(index, *tail);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);
    hdr  = (BiscuitPendingPageHeader *) BiscuitPageDataPtr(page);

    if (hdr->num_records < hdr->max_records)
    {
        /* Room on the current tail page: append in place. */
        GenericXLogState     *state = GenericXLogStart(index);
        BiscuitPendingRecord *rec;

        page = GenericXLogRegisterBuffer(state, buf, 0);
        hdr  = (BiscuitPendingPageHeader *) BiscuitPageDataPtr(page);

        rec = (BiscuitPendingRecord *) ((char *) hdr + MAXALIGN(sizeof(BiscuitPendingPageHeader)));
        rec[hdr->num_records].value = value;
        rec[hdr->num_records].op    = op;
        memset(rec[hdr->num_records].reserved, 0, sizeof(rec[hdr->num_records].reserved));
        hdr->num_records++;

        BiscuitPageSetLower(page, BiscuitPendingUsedBytes(hdr->num_records));

        GenericXLogFinish(state);
        UnlockReleaseBuffer(buf);

        if (bytes_written)
            *bytes_written = sizeof(BiscuitPendingRecord);
        return;
    }

    /*
     * Tail page is full: allocate a new tail page, link it via the old
     * tail's opaque.next, and swing *tail -- all while still holding the
     * exclusive lock on the old tail page, and all inside a single
     * GenericXLog transaction covering both pages, so the link and the
     * pointer swing land atomically (§6's nested-lock requirement,
     * satisfied here even more strongly than the minimum it asks for).
     */
    {
        Buffer                 newbuf;
        Page                   newpage;
        GenericXLogState      *state;
        BiscuitPendingPageHeader *newhdr;
        BiscuitPendingRecord  *rec;
        BiscuitPageOpaque      newopaque;
        BiscuitPageOpaque      oldopaque;

        newbuf = biscuit_page_alloc(index, BISCUIT_PAGE_PENDING);

        state   = GenericXLogStart(index);
        page    = GenericXLogRegisterBuffer(state, buf, 0);
        newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);

        PageInit(newpage, BufferGetPageSize(newbuf), specialSize);
        newhdr             = (BiscuitPendingPageHeader *) BiscuitPageDataPtr(newpage);
        newhdr->max_records = BiscuitPendingPageMaxRecords(BufferGetPageSize(newbuf));
        newhdr->num_records = 1;

        rec = (BiscuitPendingRecord *) ((char *) newhdr + MAXALIGN(sizeof(BiscuitPendingPageHeader)));
        rec[0].value = value;
        rec[0].op    = op;
        memset(rec[0].reserved, 0, sizeof(rec[0].reserved));

        BiscuitPageSetLower(newpage, BiscuitPendingUsedBytes(1));

        newopaque              = (BiscuitPageOpaque) PageGetSpecialPointer(newpage);
        newopaque->next        = InvalidBlockNumber;
        newopaque->page_kind   = BISCUIT_PAGE_PENDING;
        newopaque->flags       = BISCUIT_PENDING_FLAG_TAIL;
        newopaque->recycle_xid = InvalidTransactionId;

        oldopaque         = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        oldopaque->next   = BufferGetBlockNumber(newbuf);
        oldopaque->flags &= ~BISCUIT_PENDING_FLAG_TAIL;

        GenericXLogFinish(state);

        *tail = BufferGetBlockNumber(newbuf);

        UnlockReleaseBuffer(newbuf);
        UnlockReleaseBuffer(buf);   /* old tail's lock released only now,
                                     * after *tail has already swung */

        if (bytes_written)
            *bytes_written = MAXALIGN(sizeof(BiscuitPendingPageHeader)) + sizeof(BiscuitPendingRecord);
    }
}

void
biscuit_pending_drain(Relation index,
                       BlockNumber pending_head,
                       BlockNumber *head_inout,
                       BlockNumber *tail_inout,
                       BlockNumber *blob_head,
                       bool do_blob_rewrite,
                       RoaringBitmap *target,
                       BiscuitDrainStats *stats)
{
    BlockNumber cur             = pending_head;
    uint32      records_drained = 0;
    uint32      pending_freed   = 0;

    Assert(target != NULL);

    biscuit_ensure_synchronous_commit();

    /*
     * §3 step 2: walk the pending chain in order, applying each record
     * via the existing biscuit_roaring_add()/biscuit_roaring_remove() --
     * no new mutation logic. Reads are share-locked (this phase does not
     * yet have a directory-entry lock to hold off concurrent appenders
     * during the walk -- see biscuit_blob.h's crash-safety note: the
     * walk-then-retire below is safe even so, because retiring is a
     * separate, later step that only touches pages via their *captured*
     * next-pointers, and a page that gained new appended records between
     * this read and the retire is simply retired anyway, per the
     * documented "future directory layer enters only after this function
     * returns" contract -- concurrent-append-during-drain coordination is
     * the future directory layer's job, not this primitive's).
     */
    while (cur != InvalidBlockNumber)
    {
        Buffer                     buf = ReadBuffer(index, cur);
        Page                       page;
        BiscuitPendingPageHeader  *hdr;
        BiscuitPendingRecord      *rec;
        BiscuitPageOpaque          opaque;
        BlockNumber                next;
        uint32                     i;

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        hdr  = (BiscuitPendingPageHeader *) BiscuitPageDataPtr(page);
        rec  = (BiscuitPendingRecord *) ((char *) hdr + MAXALIGN(sizeof(BiscuitPendingPageHeader)));

        for (i = 0; i < hdr->num_records; i++)
        {
            switch (rec[i].op)
            {
                case BISCUIT_PENDING_OP_ADD:
                    biscuit_roaring_add(target, rec[i].value);
                    break;
                case BISCUIT_PENDING_OP_REMOVE:
                    biscuit_roaring_remove(target, rec[i].value);
                    break;
                default:
                    UnlockReleaseBuffer(buf);
                    ereport(ERROR,
                            (errcode(ERRCODE_DATA_CORRUPTED),
                             errmsg("biscuit: corrupt pending record op %u at block %u",
                                    rec[i].op, cur)));
            }
        }
        records_drained += hdr->num_records;

        opaque = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        next   = opaque->next;

        UnlockReleaseBuffer(buf);
        cur = next;
    }

    /*
     * §3 step 4: truncate the pending chain -- but only on a real drain
     * (do_blob_rewrite = true). A read-time reconciliation call
     * (do_blob_rewrite = false, see biscuit_pattern.c's
     * biscuit_reconcile_pending() / "Biscuit WAL-Logged Storage: Phase 1
     * Contract" §3) must leave both the pending chain and the compacted
     * blob completely untouched -- it only applies pending records to
     * the in-memory `target` bitmap so a query sees a consistent result,
     * without doing any of the durable bookkeeping (chain truncation,
     * blob rewrite, directory update) that a real drain performs. The
     * caller is responsible for NOT persisting *head_inout* tail_inout
     * back to the directory in that case -- biscuit_reconcile_pending()
     * doesn't call biscuit_dir_update() at all, so the untouched
     * pending_head/pending_tail this function leaves in entry.pending_head/
     * entry.pending_tail (unchanged from what the caller passed in) are
     * simply discarded along with the rest of that stack-local
     * BiscuitDirEntry copy.
     */
    if (do_blob_rewrite)
    {
        biscuit_free_chain(index, pending_head, &pending_freed);
        *head_inout = InvalidBlockNumber;
        *tail_inout = InvalidBlockNumber;

        /* §3 steps 3 & 5: re-encode once, write the new compacted chain,
         * retire the old one, and hand back the new head. */
        {
        BlockNumber old_blob_head = *blob_head;
        char       *serialized;
        uint32      serialized_len = 0;
        BlockNumber new_head       = InvalidBlockNumber;
        uint32      blob_bytes     = 0;
        uint32      old_freed      = 0;

        serialized = biscuit_roaring_serialize(target, &serialized_len);
        biscuit_page_write_blob(index, serialized, serialized_len, &new_head, &blob_bytes);
        if (serialized)
            pfree(serialized);

        if (old_blob_head != InvalidBlockNumber)
            biscuit_free_chain(index, old_blob_head, &old_freed);

        *blob_head = new_head;

        if (stats)
        {
            stats->blob_bytes_written   = blob_bytes;
            stats->old_blob_pages_freed = old_freed;
        }
        }
    }
    else if (stats)
    {
        stats->blob_bytes_written   = 0;
        stats->old_blob_pages_freed = 0;
    }

    if (stats)
    {
        stats->records_drained     = records_drained;
        stats->pending_pages_freed = pending_freed;
    }
}
