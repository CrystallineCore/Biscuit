/*
 * biscuit_pendlog.c
 * See biscuit_pendlog.h for the design and the measurements that motivated it.
 */

#include "biscuit_common.h"
#include "biscuit_blob.h"
#include "biscuit_dir.h"
#include "biscuit_bitmap.h"
#include "biscuit_pendlog.h"
#include "biscuit_fanout.h"
#include "biscuit_delta.h"
#include "biscuit_persist.h"   /* biscuit_rowstore_alloc_lock()/_unlock() --
                                 * see pendlog_drain_internal()'s call site
                                 * for why the drain now takes this too */
#include "storage/bufpage.h"
#include "utils/memutils.h"
#include "access/xact.h"

#define BiscuitPageDataPtr(page)  ((char *) (page) + SizeOfPageHeaderData)

static inline void
PendLogSetLower(Page page, Size used)
{
    ((PageHeader) page)->pd_lower = (LocationIndex) (SizeOfPageHeaderData + used);
}

static inline BiscuitPendLogRecord *
PendLogRecords(BiscuitPendLogPageHeader *hdr)
{
    return (BiscuitPendLogRecord *)
        ((char *) hdr + MAXALIGN(sizeof(BiscuitPendLogPageHeader)));
}

/* ================================================================
 * ROW BATCHING
 *
 * Why this exists
 * ---------------
 * Indexing one string of N characters emits, per case mode, POS/NEG/CACHE
 * per character plus LEN and one LEN_GE append per length threshold --
 * on the order of 8N+4 pendlog records for a single row. Before batching,
 * each of those was its own GenericXLogStart/Finish pair, so each carried
 * a full XLogRecord header plus a block reference even though its payload
 * is a ~24-byte BiscuitPendLogRecord. The fixed per-record overhead, not
 * the payload, dominated the WAL cost of an insert.
 *
 * All of a row's appends already land on the SAME pendlog tail page (that
 * is the whole point of the shared log). So a batch keeps that one page
 * exclusively locked with a single GenericXLog transaction open across the
 * whole row, writes every record into it, and finishes once. N records
 * become one WAL record carrying the accumulated page delta.
 *
 * Two rules make this safe:
 *
 *   1. NEVER drain while a batch is open. biscuit_pendlog_drain_all()
 *      takes the metapage and other page locks; taking them while holding
 *      the tail page's content lock is a deadlock. The drain trigger is
 *      therefore deferred to biscuit_pendlog_batch_end(), which runs with
 *      no locks of ours held.
 *
 *   2. NEVER hold the tail lock across biscuit_page_alloc(). If the tail
 *      fills mid-row, the batch finishes and releases first, then falls
 *      back to pendlog_append_single() for that one record -- which owns
 *      all the rollover/allocation logic -- and re-opens a batch on the
 *      new tail for the records that follow.
 *
 * The batch is process-local and non-reentrant by construction: it wraps
 * one row's write inside one backend. begin/end must be paired; end() is
 * idempotent, and the buffer content lock and GenericXLogState are both
 * released by resource-owner cleanup if an ERROR unwinds without end()
 * running.
 *
 * That resource-owner cleanup is NOT enough on its own, though: it frees
 * the buffer pin/lock and the GenericXLogState's memory, but
 * biscuit_batch/biscuit_batch_open are plain process-local statics that
 * resource-owner cleanup knows nothing about, so they keep pointing at
 * those now-invalid resources after an abort. See the abort-callback
 * block below (biscuit_batch_xact_callback / biscuit_batch_subxact_
 * callback) for why that mismatch is dangerous and how it is closed.
 * ================================================================ */

typedef struct BiscuitPendLogBatch
{
    Relation            index;      /* relation this batch is bound to */
    GenericXLogState   *state;      /* open xlog txn, or NULL */
    Buffer              buf;        /* exclusive-locked tail, or InvalidBuffer */
    Page                scratch;    /* GenericXLog page image for buf -- ALL
                                     * writes must go here, never to
                                     * BufferGetPage(buf), or they bypass WAL */
    uint64              last_bytes; /* log size reported by the last append */
    bool                want_drain; /* a deferred drain trigger fired */
    SubTransactionId    owner_subid; /* subxact that acquired buf/state, or
                                      * InvalidSubTransactionId if neither is
                                      * currently held */
} BiscuitPendLogBatch;

static BiscuitPendLogBatch biscuit_batch      = { NULL, NULL, InvalidBuffer, NULL, 0, false, InvalidSubTransactionId };
static bool                biscuit_batch_open = false;

bool
biscuit_pendlog_batch_active(void)
{
    return biscuit_batch_open;
}

/* ================================================================
 * BATCH ABORT SAFETY
 *
 * A batch is meant to be opened and closed strictly within one row's
 * write. But if an ERROR is raised anywhere in between -- from any of the
 * fan-out calls the batch wraps -- the transaction (or subtransaction)
 * aborts before biscuit_pendlog_batch_end() ever runs. Postgres's
 * resource-owner and memory-context machinery correctly release the tail
 * buffer's pin/lock and free the GenericXLogState's backing memory on
 * that abort. What that cleanup does NOT do is touch biscuit_batch /
 * biscuit_batch_open, because those are plain process-local statics with
 * no resource-owner registration of their own.
 *
 * Left alone, biscuit_batch_open would stay true and biscuit_batch.buf /
 * .state would keep pointing at resources that no longer belong to this
 * backend. The next biscuit_pendlog_batch_begin() would then see
 * biscuit_batch_open and call batch_flush(), which would call
 * GenericXLogFinish() on a dangling GenericXLogState pointer and
 * UnlockReleaseBuffer() on a buffer this backend no longer holds a pin
 * on -- undefined behaviour at best, and in a non-assert build, silent
 * release of a buffer pin that may since have been reused by something
 * else entirely.
 *
 * The fix is to reset the bookkeeping on abort, NOT to call
 * batch_flush(): by the time either callback below runs, the buffer
 * pin/lock and the GenericXLogState are already gone (that is exactly
 * the problem), so touching them again would just be a second use of
 * already-freed resources. We only need to make biscuit_batch_open /
 * biscuit_batch agree with reality again.
 *
 * Both a top-level and a subtransaction callback are registered:
 * SUBXACT_EVENT_ABORT_SUB catches a batch whose tail buffer was acquired
 * inside a subtransaction (e.g. a PL/pgSQL exception block) that aborts
 * on its own, and XACT_EVENT_ABORT / XACT_EVENT_PARALLEL_ABORT catch a
 * top-level abort. Registration is idempotent and happens lazily from
 * biscuit_pendlog_batch_begin(), so a backend that never opens a batch
 * never pays for it.
 *
 * A subtransaction abort only releases resources owned by THAT
 * subtransaction's resource owner -- not an ancestor's. A batch can be
 * opened at one subxact level and still be open when code further down
 * (inside one of the fan-out calls it wraps) enters and aborts a *deeper*,
 * unrelated subtransaction that gets caught (again, the PL/pgSQL
 * exception-block case). If we reset unconditionally on any
 * SUBXACT_EVENT_ABORT_SUB while a batch happens to be open, we would
 * discard biscuit_batch.buf/.state in exactly that case even though the
 * buffer's pin/lock and the GenericXLogState were acquired at the
 * surviving outer level and are still perfectly valid -- orphaning a live
 * content lock for the rest of the transaction instead of freeing a dead
 * one, which is the same class of bug this whole block exists to prevent.
 * So biscuit_batch.owner_subid records which subxact actually acquired
 * the buffer (set in batch_page_with_room(), cleared by batch_flush()),
 * and the subxact callback only resets when the aborting subxact matches.
 * ================================================================ */

static bool biscuit_batch_abort_callbacks_registered = false;

static inline void
biscuit_batch_reset_bookkeeping(void)
{
    biscuit_batch.index       = NULL;
    biscuit_batch.state       = NULL;
    biscuit_batch.buf         = InvalidBuffer;
    biscuit_batch.scratch     = NULL;
    biscuit_batch.last_bytes  = 0;
    biscuit_batch.want_drain  = false;
    biscuit_batch.owner_subid = InvalidSubTransactionId;
    biscuit_batch_open        = false;
}

static void
biscuit_batch_xact_callback(XactEvent event, void *arg)
{
    (void) arg;

    if (!biscuit_batch_open)
        return;

    switch (event)
    {
        case XACT_EVENT_ABORT:
        case XACT_EVENT_PARALLEL_ABORT:
            biscuit_batch_reset_bookkeeping();
            break;
        default:
            break;
    }
}

static void
biscuit_batch_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
                                SubTransactionId parentSubid, void *arg)
{
    (void) parentSubid;
    (void) arg;

    if (!biscuit_batch_open)
        return;

    if (event != SUBXACT_EVENT_ABORT_SUB)
        return;

    /*
     * Nothing live to protect -- either no buffer is currently held (the
     * batch is between rollovers, or hasn't acquired a tail page yet), or
     * the buffer we do hold belongs to a different subxact than the one
     * that just aborted. In the latter case resource-owner cleanup for
     * THIS subxact did not touch it: it's still ours, still valid, and
     * resetting here would leak its pin and content lock. Only reset when
     * the aborting subxact is the one that actually acquired it.
     */
    if (BufferIsValid(biscuit_batch.buf) &&
        biscuit_batch.owner_subid == mySubid)
        biscuit_batch_reset_bookkeeping();
}

/* Idempotent; safe to call from biscuit_pendlog_batch_begin() every time. */
static void
biscuit_batch_register_abort_callbacks(void)
{
    if (!biscuit_batch_abort_callbacks_registered)
    {
        RegisterXactCallback(biscuit_batch_xact_callback, NULL);
        RegisterSubXactCallback(biscuit_batch_subxact_callback, NULL);
        biscuit_batch_abort_callbacks_registered = true;
    }
}

/* Finish the open GenericXLog transaction, if any, and release the page. */
static void
batch_flush(void)
{
    if (biscuit_batch.state != NULL)
    {
        GenericXLogFinish(biscuit_batch.state);
        biscuit_batch.state = NULL;
    }
    if (BufferIsValid(biscuit_batch.buf))
    {
        UnlockReleaseBuffer(biscuit_batch.buf);
        biscuit_batch.buf = InvalidBuffer;
    }
    biscuit_batch.scratch     = NULL;
    biscuit_batch.owner_subid = InvalidSubTransactionId;
}

/*
 * Try to open (or keep) a tail page with room for one more record.
 * Returns the page header on success with biscuit_batch.state/buf set,
 * or NULL if the caller must fall back to pendlog_append_single().
 */
static BiscuitPendLogPageHeader *
batch_page_with_room(Relation index)
{
    Buffer                    mbuf;
    BiscuitMetaPageData      *meta;
    BlockNumber               tailblk;
    BiscuitPendLogPageHeader *hdr;
    BiscuitPageOpaque         topaque;
    Page                      lpage;

    /* Already have one open with room? */
    if (biscuit_batch.state != NULL && BufferIsValid(biscuit_batch.buf))
    {
        /*
         * Re-registering an already-registered buffer is defined to return
         * the same page image (generic_xlog.c), so this is a cheap lookup
         * rather than a second registration.
         */
        biscuit_batch.scratch =
            GenericXLogRegisterBuffer(biscuit_batch.state, biscuit_batch.buf, 0);
        hdr = (BiscuitPendLogPageHeader *) BiscuitPageDataPtr(biscuit_batch.scratch);
        if (hdr->num_records < hdr->max_records)
            return hdr;
        batch_flush();          /* full -- roll over below */
    }

    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_SHARE);
    meta    = (BiscuitMetaPageData *) PageGetSpecialPointer(BufferGetPage(mbuf));
    tailblk = meta->pendlog_tail;
    /*
     * Keep the log-size estimate fresh while batched. We only re-read it
     * on rollover, not per append, which is exactly the point: the size
     * only changes when a page is added, and the drain trigger is a
     * threshold test that tolerates being a page stale.
     */
    biscuit_batch.last_bytes = (uint64) meta->pendlog_npages * BLCKSZ;
    UnlockReleaseBuffer(mbuf);

    if (tailblk == InvalidBlockNumber)
        return NULL;            /* fresh log: single-record path allocates */

    biscuit_batch.buf = ReadBuffer(index, tailblk);
    LockBuffer(biscuit_batch.buf, BUFFER_LOCK_EXCLUSIVE);

    hdr     = (BiscuitPendLogPageHeader *)
                  BiscuitPageDataPtr(BufferGetPage(biscuit_batch.buf));
    topaque = (BiscuitPageOpaque)
                  PageGetSpecialPointer(BufferGetPage(biscuit_batch.buf));

    /* Same detach check as the unbatched fast path -- see its comment. */
    if ((topaque->flags & BISCUIT_PENDING_FLAG_TAIL) == 0 ||
        hdr->num_records >= hdr->max_records)
    {
        UnlockReleaseBuffer(biscuit_batch.buf);
        biscuit_batch.buf = InvalidBuffer;
        return NULL;
    }

    /*
     * Record the subxact that owns this pin/lock so the abort callback
     * can tell "mine, and now gone" apart from "an unrelated nested
     * subxact aborted, this is still live" -- see the BATCH ABORT SAFETY
     * comment above.
     */
    biscuit_batch.owner_subid = GetCurrentSubTransactionId();
    biscuit_batch.state       = GenericXLogStart(index);
    lpage                     = GenericXLogRegisterBuffer(biscuit_batch.state,
                                                           biscuit_batch.buf, 0);
    biscuit_batch.scratch     = lpage;
    return (BiscuitPendLogPageHeader *) BiscuitPageDataPtr(lpage);
}

void
biscuit_pendlog_batch_begin(Relation index)
{
    /*
     * Nested begin would silently orphan the outer batch's locked page.
     * Callers pair begin/end around one row, so this should not happen;
     * flush defensively rather than assert, so a stray call cannot leave
     * a content lock held.
     */
    if (biscuit_batch_open)
        batch_flush();

    biscuit_batch_register_abort_callbacks();

    biscuit_batch.index       = index;
    biscuit_batch.state       = NULL;
    biscuit_batch.buf         = InvalidBuffer;
    biscuit_batch.scratch     = NULL;
    biscuit_batch.last_bytes  = 0;
    biscuit_batch.want_drain  = false;
    biscuit_batch.owner_subid = InvalidSubTransactionId;
    biscuit_batch_open        = true;
}

void
biscuit_pendlog_batch_end(void)
{
    Relation index      = biscuit_batch.index;
    bool     want_drain = biscuit_batch.want_drain;

    if (!biscuit_batch_open)
        return;                 /* idempotent */

    batch_flush();
    biscuit_batch_open       = false;
    biscuit_batch.index      = NULL;
    biscuit_batch.want_drain = false;

    /*
     * Deferred drain, now that we hold none of our own locks. Doing this
     * inside the batch would deadlock against the tail page lock.
     */
    if (want_drain && index != NULL)
        biscuit_pendlog_drain_all(index, false);
}

/* ================================================================
 * APPEND
 * ================================================================ */

static uint64
pendlog_append_single(Relation index, uint32 slot, uint8 op)
{
    Buffer                    mbuf;
    Buffer                    tbuf;
    Page                      mpage, lpage;
    BiscuitMetaPageData      *meta;
    GenericXLogState         *state;
    Size                      specialSize = MAXALIGN(sizeof(BiscuitPageOpaqueData));
    BiscuitPendLogRecord      rec;
    BlockNumber               tailblk;
    uint32                    npages;

    Assert(op == BISCUIT_PENDING_OP_ADD || op == BISCUIT_PENDING_OP_REMOVE);

    biscuit_ensure_synchronous_commit();

    memset(&rec, 0, sizeof(rec));   /* zero-fills flags/pad too */
    rec.slot = slot;
    rec.op   = op;

    /*
     * LOCKING (this cost a self-deadlock to get right, so it is spelled out).
     *
     * biscuit_page_alloc() takes the metapage lock itself -- share, then
     * exclusive, to consult and update the FSM root. So this function must
     * NEVER hold the metapage lock across a biscuit_page_alloc() call:
     * doing so self-deadlocks on the buffer content lock, deterministically,
     * on the very first append an index ever takes.
     *
     * Hence: read the metapage under a SHARE lock and release it before
     * doing anything else. The common path then touches only the log tail
     * page and never re-locks the metapage at all.
     *
     * That last point is also why the metapage is no longer updated per
     * append. An earlier version bumped pendlog_count/pendlog_bytes on
     * every append, which put the metapage into every single WAL record
     * and serialized all writers on one exclusive lock. The record count
     * lives on the log page itself instead, and the metapage is touched
     * only on page rollover (rare). This halves the pages-per-append from
     * two to one -- which matters, because pages-per-append is exactly the
     * quantity this whole redesign exists to minimize.
     */
    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_SHARE);
    meta    = (BiscuitMetaPageData *) PageGetSpecialPointer(BufferGetPage(mbuf));
    tailblk = meta->pendlog_tail;
    npages  = meta->pendlog_npages;
    UnlockReleaseBuffer(mbuf);

    /* ---- fast path: there is a tail and it has room ---- */
    if (tailblk != InvalidBlockNumber)
    {
        BiscuitPendLogPageHeader *hdr;
        BiscuitPageOpaque         topaque;

        tbuf = ReadBuffer(index, tailblk);
        LockBuffer(tbuf, BUFFER_LOCK_EXCLUSIVE);
        hdr     = (BiscuitPendLogPageHeader *) BiscuitPageDataPtr(BufferGetPage(tbuf));
        topaque = (BiscuitPageOpaque) PageGetSpecialPointer(BufferGetPage(tbuf));

        /*
         * DETACH CHECK -- do not remove.
         *
         * We read tailblk from the metapage under a SHARE lock and then
         * released it, so a drain may have detached the whole log in the
         * gap (pendlog_detach(), below). A detached chain is unreachable
         * from the metapage and is about to be freed, so appending to it
         * would durably WAL-log a record that is then thrown away with the
         * chain -- a committed insert whose index entries silently vanish.
         *
         * pendlog_detach() clears BISCUIT_PENDING_FLAG_TAIL on the tail
         * page under that page's exclusive lock, which is the same lock we
         * now hold. So if the flag is still set, this page is reachable
         * from the metapage and will stay so until we release; if it is
         * clear, the page is detached and we must start over on whatever
         * the log looks like now.
         */
        if ((topaque->flags & BISCUIT_PENDING_FLAG_TAIL) == 0)
        {
            UnlockReleaseBuffer(tbuf);

            mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
            LockBuffer(mbuf, BUFFER_LOCK_SHARE);
            meta    = (BiscuitMetaPageData *) PageGetSpecialPointer(BufferGetPage(mbuf));
            tailblk = meta->pendlog_tail;
            npages  = meta->pendlog_npages;
            UnlockReleaseBuffer(mbuf);

            /*
             * Re-read done. Fall through to the slow path rather than
             * retrying the fast path: the slow path handles both "log is
             * empty again" (the common case right after a detach) and
             * "another appender already started a fresh tail", and it
             * re-reads the metapage under an exclusive lock anyway, so a
             * second stale observation here cannot cause a lost record.
             */
            goto slow_path;
        }

        if (hdr->num_records < hdr->max_records)
        {
            /*
             * The overwhelmingly common case: ONE page enters WAL. Every
             * append a row makes lands here, on the same page, so a row
             * touching K structures dirties one page rather than K.
             */
            state = GenericXLogStart(index);
            lpage = GenericXLogRegisterBuffer(state, tbuf, 0);
            hdr   = (BiscuitPendLogPageHeader *) BiscuitPageDataPtr(lpage);

            PendLogRecords(hdr)[hdr->num_records] = rec;
            hdr->num_records++;
            PendLogSetLower(lpage, BiscuitPendLogUsedBytes(hdr->num_records));

            GenericXLogFinish(state);
            UnlockReleaseBuffer(tbuf);

            return (uint64) npages * BLCKSZ;
        }

        /* Tail is full. Release before allocating -- see the locking note. */
        UnlockReleaseBuffer(tbuf);
    }

slow_path:
    /*
     * ---- slow path: the log needs a new page ----
     *
     * Allocate FIRST, holding no locks of ours, because biscuit_page_alloc()
     * wants the metapage. Then take the metapage exclusively and splice the
     * new page in as the tail, whatever the tail happens to be by then.
     *
     * Racing backends: if another appender rolled the tail over between our
     * share-lock read and here, we simply append our page after *its* new
     * tail rather than the one we saw. That wastes the tail slot the winner
     * left (its page ends up holding fewer records than it could), which is
     * a bounded, rare inefficiency -- not a correctness problem -- and it
     * buys a design with no retry loop and no need to hand back an
     * already-allocated page.
     */
    tbuf = biscuit_page_alloc(index, BISCUIT_PAGE_PENDLOG);

    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
    meta    = (BiscuitMetaPageData *) PageGetSpecialPointer(BufferGetPage(mbuf));
    tailblk = meta->pendlog_tail;

    if (tailblk == InvalidBlockNumber)
    {
        /* First page of a fresh (or freshly drained) log. */
        BiscuitPendLogPageHeader *hdr;
        BiscuitPageOpaque         opaque;

        state = GenericXLogStart(index);
        lpage = GenericXLogRegisterBuffer(state, tbuf, GENERIC_XLOG_FULL_IMAGE);
        mpage = GenericXLogRegisterBuffer(state, mbuf, 0);

        PageInit(lpage, BufferGetPageSize(tbuf), specialSize);
        hdr              = (BiscuitPendLogPageHeader *) BiscuitPageDataPtr(lpage);
        hdr->max_records = BiscuitPendLogMaxRecords(BufferGetPageSize(tbuf));
        PendLogRecords(hdr)[0] = rec;
        hdr->num_records = 1;
        PendLogSetLower(lpage, BiscuitPendLogUsedBytes(1));

        opaque              = (BiscuitPageOpaque) PageGetSpecialPointer(lpage);
        opaque->next        = InvalidBlockNumber;
        opaque->page_kind   = BISCUIT_PAGE_PENDLOG;
        opaque->flags       = BISCUIT_PENDING_FLAG_TAIL;
        opaque->recycle_xid = InvalidTransactionId;

        meta = (BiscuitMetaPageData *) PageGetSpecialPointer(mpage);
        meta->pendlog_head   = BufferGetBlockNumber(tbuf);
        meta->pendlog_tail   = meta->pendlog_head;
        meta->pendlog_npages = 1;
        npages               = 1;

        GenericXLogFinish(state);
        UnlockReleaseBuffer(mbuf);
        UnlockReleaseBuffer(tbuf);

        return (uint64) npages * BLCKSZ;
    }

    /* Link the new page after the current tail and swing the pointer. */
    {
        Buffer                     obuf;
        Page                       opage;
        BiscuitPendLogPageHeader  *nhdr;
        BiscuitPageOpaque          nopaque, oopaque;

        obuf = ReadBuffer(index, tailblk);
        LockBuffer(obuf, BUFFER_LOCK_EXCLUSIVE);

        state = GenericXLogStart(index);
        opage = GenericXLogRegisterBuffer(state, obuf, 0);
        lpage = GenericXLogRegisterBuffer(state, tbuf, GENERIC_XLOG_FULL_IMAGE);
        mpage = GenericXLogRegisterBuffer(state, mbuf, 0);

        PageInit(lpage, BufferGetPageSize(tbuf), specialSize);
        nhdr              = (BiscuitPendLogPageHeader *) BiscuitPageDataPtr(lpage);
        nhdr->max_records = BiscuitPendLogMaxRecords(BufferGetPageSize(tbuf));
        PendLogRecords(nhdr)[0] = rec;
        nhdr->num_records = 1;
        PendLogSetLower(lpage, BiscuitPendLogUsedBytes(1));

        nopaque              = (BiscuitPageOpaque) PageGetSpecialPointer(lpage);
        nopaque->next        = InvalidBlockNumber;
        nopaque->page_kind   = BISCUIT_PAGE_PENDLOG;
        nopaque->flags       = BISCUIT_PENDING_FLAG_TAIL;
        nopaque->recycle_xid = InvalidTransactionId;

        oopaque         = (BiscuitPageOpaque) PageGetSpecialPointer(opage);
        oopaque->next   = BufferGetBlockNumber(tbuf);
        oopaque->flags &= ~BISCUIT_PENDING_FLAG_TAIL;

        meta = (BiscuitMetaPageData *) PageGetSpecialPointer(mpage);
        meta->pendlog_tail = BufferGetBlockNumber(tbuf);
        meta->pendlog_npages++;
        npages = meta->pendlog_npages;

        GenericXLogFinish(state);
        UnlockReleaseBuffer(mbuf);
        UnlockReleaseBuffer(tbuf);
        UnlockReleaseBuffer(obuf);
    }

    return (uint64) npages * BLCKSZ;
}

/*
 * biscuit_pendlog_append -- public entry point.
 *
 * Routes through the open row batch when there is one, so that a row's
 * whole fan-out lands in a single WAL record instead of one per
 * structure. Falls back to the single-record path whenever the batch
 * cannot take the record on its current page (fresh log, tail full,
 * or tail detached by a concurrent drain).
 *
 * The return value keeps the pre-batching contract: the caller uses it
 * only to decide whether to trigger a drain. While batched we report the
 * last known size and record that a drain is wanted, so the trigger
 * fires once in biscuit_pendlog_batch_end() rather than mid-row.
 */
uint64
biscuit_pendlog_append(Relation index, uint32 slot, uint8 op)
{
    BiscuitPendLogPageHeader *hdr;
    BiscuitPendLogRecord      rec;
    Page                      lpage;

    if (!biscuit_batch_open || biscuit_batch.index != index)
        return pendlog_append_single(index, slot, op);

    Assert(op == BISCUIT_PENDING_OP_ADD || op == BISCUIT_PENDING_OP_REMOVE);

    hdr = batch_page_with_room(index);
    if (hdr == NULL)
    {
        /*
         * No batchable page. The single-record path owns rollover and
         * allocation; let it write this record, and leave the batch closed
         * so the next append re-opens on whatever tail now exists.
         */
        biscuit_batch.last_bytes = pendlog_append_single(index, slot, op);
        if (biscuit_batch.last_bytes > biscuit_pendlog_drain_trigger_bytes())
            biscuit_batch.want_drain = true;
        return biscuit_batch.last_bytes;
    }

    biscuit_ensure_synchronous_commit();

    memset(&rec, 0, sizeof(rec));   /* zero-fills flags/pad too */
    rec.slot = slot;
    rec.op   = op;

    PendLogRecords(hdr)[hdr->num_records] = rec;
    hdr->num_records++;

    /* hdr already points into the scratch image; keep pd_lower in sync there */
    lpage = biscuit_batch.scratch;
    PendLogSetLower(lpage, BiscuitPendLogUsedBytes(hdr->num_records));

    /*
     * Arm the deferred drain rather than running it here -- we are holding
     * the tail page's content lock, and the drain wants the metapage.
     * biscuit_pendlog_batch_end() runs it once the lock is released.
     */
    if (biscuit_batch.last_bytes > biscuit_pendlog_drain_trigger_bytes())
        biscuit_batch.want_drain = true;

    return biscuit_batch.last_bytes;
}

/* ================================================================
 * STATS
 * ================================================================ */

/*
 * Log size (in bytes) at which the append path should compact.
 *
 * The threshold is stated in ROWS -- biscuit.delta_compaction_slots -- and
 * converted here, because rows are what set delta rebuild time and bytes
 * are what the append path cheaply knows. With a fixed 8-byte record the
 * conversion is exact rather than an estimate: one row is one record, and
 * a page holds BiscuitPendLogMaxRecords(BLCKSZ) of them.
 *
 * BISCUIT_PENDLOG_DRAIN_PAGES survives as a hard ceiling underneath. It is
 * no longer the tuning knob -- 4 MB of 8-byte records is roughly half a
 * million rows, far past any sane rebuild budget -- but it bounds the
 * damage if the GUC is set absurdly high.
 */
uint64
biscuit_pendlog_drain_trigger_bytes(void)
{
    uint64 recs_per_page = (uint64) BiscuitPendLogMaxRecords(BLCKSZ);
    uint64 rows          = (uint64) biscuit_delta_compaction_threshold();
    uint64 pages;

    if (recs_per_page == 0)
        recs_per_page = 1;

    pages = (rows + recs_per_page - 1) / recs_per_page;
    if (pages < 1)
        pages = 1;
    if (pages > (uint64) BISCUIT_PENDLOG_DRAIN_PAGES)
        pages = (uint64) BISCUIT_PENDLOG_DRAIN_PAGES;

    return pages * (uint64) BLCKSZ;
}

void
biscuit_pendlog_stats(Relation index, uint64 *out_count, uint64 *out_bytes)
{
    Buffer               buf;
    BiscuitMetaPageData *meta;

    if (RelationGetNumberOfBlocks(index) == 0)
    {
        if (out_count) *out_count = 0;
        if (out_bytes) *out_bytes = 0;
        return;
    }

    buf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    meta = (BiscuitMetaPageData *) PageGetSpecialPointer(BufferGetPage(buf));
    if (out_count) *out_count = meta->pendlog_npages;
    if (out_bytes) *out_bytes = (uint64) meta->pendlog_npages * BLCKSZ;
    UnlockReleaseBuffer(buf);
}

/* ================================================================
 * READ-PATH SNAPSHOT
 * ================================================================ */

/*
 * Snapshot cache.
 *
 * This was a single static slot, which quietly cost a full log
 * re-materialization every time a backend alternated between two Biscuit
 * indexes: building any snapshot evicted whatever was cached, so a
 * per-column index layout thrashed 1:1 with column switches. Measured on
 * a read-only benchmark, first-touch-per-column showed up as 20-70ms
 * spikes against a ~1.5ms warm baseline, with every repeat on an
 * already-touched column fast -- the signature of an evict-and-rebuild
 * cache, not of a growing log.
 *
 * A small fixed array is enough: a backend touches few Biscuit indexes,
 * lookup is a linear scan over 8 pointers, and LRU eviction needs one
 * counter. Sized generously rather than tuned -- an idle slot costs a
 * pointer.
 */
#define BISCUIT_PENDLOG_SNAPSHOT_SLOTS  8

typedef struct PendLogSnapshotSlot
{
    BiscuitPendLogSnapshot *snap;   /* NULL == empty slot */
    uint64                  lru;    /* higher == more recently used */

    /*
     * meta->pendlog_draining as observed when this slot's snapshot was
     * last (re)built. total_drains alone does not distinguish "a drain
     * just detached the log, draining now points at it" from "that same
     * drain finished its merge, draining is Invalid again" -- both can
     * be observed under the same total_drains value, since
     * pendlog_clear_draining() does not bump the counter. Comparing this
     * against the freshly-read pendlog_draining is what lets
     * biscuit_pendlog_snapshot() notice the transition and rebuild
     * instead of serving a snapshot that silently drops the abandoned
     * chain's deltas once they've been merged into the blobs (or, in the
     * other direction, keeps re-ingesting a chain that no longer needs
     * it -- harmless but wasteful).
     */
    BlockNumber              built_at_draining;
} PendLogSnapshotSlot;

static PendLogSnapshotSlot snapshot_cache[BISCUIT_PENDLOG_SNAPSHOT_SLOTS];
static uint64              snapshot_lru_clock = 0;

static void pendlog_ingest_from(Relation index, BiscuitPendLogSnapshot *snap,
                                 BlockNumber start_blk, uint32 start_off);
static void pendlog_expand_touched(Relation index, BiscuitPendLogSnapshot *snap);

static void
pendlog_key_init(BiscuitPendLogKey *k, int32 col, bool is_lower, uint8 kind,
                  int32 ch, int32 position)
{
    memset(k, 0, sizeof(*k));   /* pad[] must be zeroed: dynahash memcmp's the key */
    k->col      = col;
    k->ch       = ch;
    k->position = position;
    k->kind     = kind;
    k->is_lower = is_lower ? 1 : 0;
}

/*
 * Free a snapshot.
 *
 * The per-structure `adds` bitmaps and the four snapshot-wide sets are
 * freed EXPLICITLY, before the context goes, rather than being left to
 * MemoryContextDelete(). Under HAVE_ROARING a RoaringBitmap is allocated
 * by CRoaring's own allocator, not by palloc, so deleting the context
 * reclaims the hash entries while leaking every bitmap they point at. The
 * fallback bitset does use palloc and would survive either way; freeing
 * explicitly is the behaviour that is correct under both builds, which is
 * the only kind worth having.
 */
static void
pendlog_snapshot_free(BiscuitPendLogSnapshot *snap)
{
    HASH_SEQ_STATUS      seq;
    BiscuitPendLogEntry *e;

    if (snap == NULL)
        return;

    if (snap->htab != NULL)
    {
        hash_seq_init(&seq, snap->htab);
        while ((e = (BiscuitPendLogEntry *) hash_seq_search(&seq)) != NULL)
        {
            biscuit_roaring_free(e->adds);
            e->adds = NULL;
        }
    }

    biscuit_roaring_free(snap->kill);
    biscuit_roaring_free(snap->live);
    biscuit_roaring_free(snap->seen);
    biscuit_roaring_free(snap->expanded);

    MemoryContextDelete(snap->cxt);
}

/*
 * Allocate an empty snapshot in its own context. Shared by the read-path
 * cache and by the drain, which needs the identical structure but must not
 * install it in the process-wide cache.
 */
static BiscuitPendLogSnapshot *
pendlog_snapshot_create(Oid indexoid, MemoryContext parent, const char *name)
{
    MemoryContext           cxt;
    MemoryContext           old;
    HASHCTL                 ctl;
    BiscuitPendLogSnapshot *snap;

    /*
     * The context name must be a compile-time constant -- AllocSetContextCreate
     * static-asserts on it, because the context stores the pointer rather
     * than a copy. So `name` distinguishes the two callers in the hash's
     * name (which does copy) and the context takes a fixed literal.
     */
    cxt = AllocSetContextCreate(parent, "biscuit pendlog snapshot",
                                 ALLOCSET_DEFAULT_SIZES);

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = sizeof(BiscuitPendLogKey);
    ctl.entrysize = sizeof(BiscuitPendLogEntry);
    ctl.hcxt      = cxt;

    snap = (BiscuitPendLogSnapshot *) MemoryContextAllocZero(cxt, sizeof(*snap));
    snap->cxt        = cxt;
    snap->indexoid   = indexoid;
    snap->resume_blk = InvalidBlockNumber;   /* a zeroed BlockNumber would
                                              * read as block 0, the metapage */
    snap->resume_off = 0;
    snap->htab       = hash_create(name, 256, &ctl,
                                    HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    old = MemoryContextSwitchTo(cxt);
    snap->kill     = biscuit_roaring_create();
    snap->live     = biscuit_roaring_create();
    snap->seen     = biscuit_roaring_create();
    snap->expanded = biscuit_roaring_create();
    MemoryContextSwitchTo(old);

    return snap;
}

/* Drop one slot's snapshot (freeing its context) and mark the slot empty. */
static void
pendlog_slot_clear(PendLogSnapshotSlot *slot)
{
    if (slot->snap != NULL)
    {
        pendlog_snapshot_free(slot->snap);
        slot->snap = NULL;
    }
    slot->lru               = 0;
    slot->built_at_draining = InvalidBlockNumber;
}

void
biscuit_pendlog_invalidate(Oid indexoid)
{
    int i;

    for (i = 0; i < BISCUIT_PENDLOG_SNAPSHOT_SLOTS; i++)
    {
        if (snapshot_cache[i].snap == NULL)
            continue;
        if (OidIsValid(indexoid) && snapshot_cache[i].snap->indexoid != indexoid)
            continue;
        pendlog_slot_clear(&snapshot_cache[i]);
    }
}

/* Find this index's cached snapshot, or NULL. */
static PendLogSnapshotSlot *
pendlog_slot_find(Oid indexoid)
{
    int i;

    for (i = 0; i < BISCUIT_PENDLOG_SNAPSHOT_SLOTS; i++)
        if (snapshot_cache[i].snap != NULL &&
            snapshot_cache[i].snap->indexoid == indexoid)
            return &snapshot_cache[i];
    return NULL;
}

/* An empty slot, or the least-recently-used one (evicting its snapshot). */
static PendLogSnapshotSlot *
pendlog_slot_acquire(void)
{
    int i, victim = 0;

    for (i = 0; i < BISCUIT_PENDLOG_SNAPSHOT_SLOTS; i++)
        if (snapshot_cache[i].snap == NULL)
            return &snapshot_cache[i];

    for (i = 1; i < BISCUIT_PENDLOG_SNAPSHOT_SLOTS; i++)
        if (snapshot_cache[i].lru < snapshot_cache[victim].lru)
            victim = i;

    pendlog_slot_clear(&snapshot_cache[victim]);
    return &snapshot_cache[victim];
}

BiscuitPendLogSnapshot *
biscuit_pendlog_snapshot(Relation index)
{
    Oid                     indexoid = RelationGetRelid(index);
    Buffer                  mbuf;
    BiscuitMetaPageData    *meta;
    BlockNumber             head;
    BlockNumber             tail;
    BlockNumber             draining;
    uint64                  count;
    uint64                  drains;
    uint32                  npages;
    BiscuitPendLogSnapshot *snap;
    PendLogSnapshotSlot    *slot;

    if (RelationGetNumberOfBlocks(index) == 0)
        return NULL;

    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_SHARE);
    meta     = (BiscuitMetaPageData *) PageGetSpecialPointer(BufferGetPage(mbuf));
    head     = meta->pendlog_head;
    tail     = meta->pendlog_tail;
    npages   = meta->pendlog_npages;
    drains   = meta->total_drains;
    draining = meta->pendlog_draining;
    UnlockReleaseBuffer(mbuf);

    /*
     * Staleness key = (total_drains, npages, records on the tail page).
     *
     * The drain counter is NOT optional. (npages, tail_records) alone is
     * only monotonic *within* one drain cycle, and it silently repeats
     * across cycles: a log at 1 page / 98 records, drained, then refilled
     * by one identically-shaped row, is again 1 page / 98 records. A
     * backend holding a snapshot from before that drain would compare
     * equal and reuse it, which is wrong in both directions -- it misses
     * every delta appended after the drain, and it re-applies pre-drain
     * deltas that the drain already folded into the blobs. The second half
     * is not harmlessly idempotent once slot reuse is in play: a stale
     * "add rec_idx 7" resurrects a recycled slot and the scan returns a
     * row that does not match the pattern.
     *
     * Uniform workloads make that repeat likely rather than exotic, since
     * identical row shapes append identical record counts. total_drains is
     * bumped under the metapage's exclusive lock by pendlog_detach() on
     * every drain and never decreases, so including it makes the whole key
     * monotonic and the collision unreachable.
     */
    count = (uint64) npages << 32;
    if (tail != InvalidBlockNumber)
    {
        Buffer                    tbuf = ReadBuffer(index, tail);
        BiscuitPendLogPageHeader *thdr;

        LockBuffer(tbuf, BUFFER_LOCK_SHARE);
        thdr = (BiscuitPendLogPageHeader *) BiscuitPageDataPtr(BufferGetPage(tbuf));
        count |= (uint64) thdr->num_records;
        UnlockReleaseBuffer(tbuf);
    }

    /*
     * Empty steady state: nothing in the live log AND no abandoned drain
     * chain left over from an interrupted merge. Only then can callers
     * skip reconciliation entirely.
     *
     * The old check here was "head == InvalidBlockNumber || count == 0".
     * head/tail/npages are all reset to Invalid/0 by pendlog_detach() in
     * the SAME transaction that publishes meta->pendlog_draining, so the
     * instant a drain detaches the log, head is Invalid and count is 0 --
     * indistinguishable, under the old check, from "fully drained,
     * nothing pending". Every query landing between that detach and the
     * matching pendlog_clear_draining() (a crash or a PITR/replica stop
     * anywhere in that window makes it permanent) took the early return
     * and silently treated an unmerged chain's deltas as absent. See the
     * fix in biscuit_pendlog_drain_all(), which already re-ingests this
     * same "abandoned" chain (there called `abandoned`) for exactly this
     * reason -- this is the read-path counterpart of that.
     */
    if (head == InvalidBlockNumber && draining == InvalidBlockNumber)
    {
        biscuit_pendlog_invalidate(indexoid);
        return NULL;
    }

    slot = pendlog_slot_find(indexoid);
    if (slot != NULL)
    {
        snap = slot->snap;

        /*
         * Same drain cycle AND the same draining chain (or lack of one).
         *
         * total_drains alone is not enough: it is bumped once, by
         * pendlog_detach(), at the *start* of a drain. It is NOT bumped
         * again when pendlog_clear_draining() clears the marker at the
         * *end* of that same drain's merge. So "draining went from H to
         * Invalid because the merge finished and its deltas are now in
         * the blobs" and "draining is still H because nothing happened"
         * are two different, non-idempotent-to-conflate states that can
         * both be observed at the same total_drains value. Comparing
         * built_at_draining catches the transition either way:
         *   - H -> Invalid: the chain we ingested is now redundant with
         *     the blobs; still correct to keep applying (replay is
         *     idempotent) but not safe to treat as "nothing changed"
         *     forever, since a *later* transition (see next point) must
         *     still be caught.
         *   - Invalid -> H': a new drain started (and possibly finished)
         *     entirely within one total_drains-unchanged window is not
         *     actually possible (detach always bumps total_drains), so
         *     the only real transition here is H -> Invalid; the check
         *     is kept symmetric because it costs nothing and does not
         *     rely on that invariant holding forever.
         */
        if (snap->built_at_drains == drains &&
            slot->built_at_draining == draining)
        {
            slot->lru = ++snapshot_lru_clock;

            if (snap->built_at_count == count)
                return snap;    /* nothing appended since we last looked */

            /*
             * INCREMENTAL EXTEND. Resume from where ingestion stopped and
             * absorb only what is new, rather than rebuilding the whole
             * hash.
             *
             * This is the difference between O(records appended since this
             * backend last asked) and O(entire log) per query. The old code
             * rebuilt from scratch on every append, so in any read/write
             * mix every query paid for the full log -- a cost that grows
             * until a drain resets it, then grows again.
             */
            pendlog_ingest_from(index, snap, snap->resume_blk, snap->resume_off);

            /*
             * A slot mentioned again after we already expanded it is the
             * one case incremental extension cannot absorb, and it is
             * worth being precise about why.
             *
             * Expansion reads the row's text from STRCACHE, which always
             * holds the CURRENT text for a slot. That is what makes the
             * scheme order-independent: replaying "ADD@42" twice, or in
             * either order relative to "REMOVE@42", yields the same
             * identities, because both readings consult the same bytes.
             * It stops being true across time. If this snapshot expanded
             * slot 42 an hour ago, its `adds` bitmaps hold identities
             * derived from the text slot 42 had THEN; an UPDATE since has
             * overwritten STRCACHE, so those identities are stale and
             * there is no record of which structures to withdraw them
             * from -- the old text is gone.
             *
             * So: rebuild from scratch. This is not the common path. A
             * pure-insert stream never re-mentions a slot (biscuit_insert()
             * claims a fresh slot every time -- see its freelist comment),
             * so appends extend incrementally as before. Only UPDATE and
             * DELETE of rows written since the last drain land here, and
             * they pay one full rebuild rather than a wrong answer.
             */
            if (!snap->need_rebuild)
            {
                /*
                 * CROSS-BACKEND LOCK-ORDER FIX (second call site).
                 *
                 * pendlog_expand_touched() reads each touched slot's text
                 * back out of STRCACHE via plain LockBuffer(SHARE) -- an
                 * LWLock, invisible to the deadlock detector. A concurrent
                 * row-identity write (biscuit_persist_row_identity_write_
                 * record(), biscuit_persist.c) can be holding one of those
                 * very STRCACHE pages EXCLUSIVE, pinned open in its
                 * BiscuitXlogBatch for the span of the whole row.
                 *
                 * pendlog_drain_internal() already closes this race for its
                 * own call to pendlog_expand_touched() further down in this
                 * file by taking biscuit_rowstore_alloc_lock() around it --
                 * see that call site for the full mechanism. This is the
                 * *other* caller: the ordinary per-query snapshot path that
                 * every index scan goes through whenever there are pending
                 * inserts to reconcile, which is the far more common of the
                 * two and was left unprotected. Without this lock, an
                 * ordinary SELECT racing an ordinary INSERT can hang
                 * exactly the same way a drain racing an INSERT used to.
                 *
                 * Same ordering argument applies unchanged: this function
                 * never holds BISCUIT_METAPAGE_BLKNO, so there is no path
                 * back the other way for a cycle to close, and by the time
                 * this lock is granted no writer's batch can still be
                 * holding a STRCACHE page open (a writer flushes its batch
                 * before releasing this same lock).
                 */
                biscuit_rowstore_alloc_lock(index);
                pendlog_expand_touched(index, snap);
                biscuit_rowstore_alloc_unlock(index);
                snap->built_at_count = count;
                return snap;
            }

            elog(DEBUG1,
                 "Biscuit: pendlog snapshot rebuild forced (slot re-mentioned after expansion)");
            pendlog_slot_clear(slot);
        }
        else
        {

            /* A drain (or a drain's completion) happened: start over. */
            pendlog_slot_clear(slot);
        }
    }

    snap = pendlog_snapshot_create(indexoid, CacheMemoryContext,
                                    "biscuit pendlog snapshot");
    snap->built_at_count  = count;
    snap->built_at_drains = drains;

    /*
     * Ingest the abandoned chain (if any) BEFORE the live head, matching
     * the order biscuit_pendlog_drain_all() uses at its own two
     * pendlog_ingest_from() calls. Both chains are absorbed into the same
     * seen/live/kill sets before ANY expansion happens, which is why
     * expansion is a separate step rather than the tail of ingest: a slot
     * that appears in both chains must reach its final state before its
     * text is read, or the first chain's reading would be superseded by
     * the second with no way to withdraw it.
     */
    if (draining != InvalidBlockNumber && draining != head)
        pendlog_ingest_from(index, snap, draining, 0);

    if (head != InvalidBlockNumber)
        pendlog_ingest_from(index, snap, head, 0);

    /*
     * CROSS-BACKEND LOCK-ORDER FIX (third call site, same hazard).
     * See the incremental-extend branch above for the full explanation;
     * this is the fresh-build path taken the first time a backend asks
     * for this index's snapshot (or after a drain invalidated the cached
     * one). Same fix, same reasoning.
     */
    biscuit_rowstore_alloc_lock(index);
    pendlog_expand_touched(index, snap);
    biscuit_rowstore_alloc_unlock(index);

    slot                    = pendlog_slot_acquire();
    slot->snap              = snap;
    slot->lru               = ++snapshot_lru_clock;
    slot->built_at_draining = draining;

    return snap;
}

/*
 * Append a slot to the snapshot's "touched since last expansion" list.
 * A plain growable array, not a set: duplicates are cheap here and are
 * removed by the sort in pendlog_expand_touched(), whereas keeping it
 * deduplicated would mean a membership test per record on the hot ingest
 * path for no benefit.
 */
static void
pendlog_touch_slot(BiscuitPendLogSnapshot *snap, uint32 slot)
{
    if (snap->ntouched == snap->cap_touched)
    {
        int newcap = snap->cap_touched ? snap->cap_touched * 2 : 256;

        if (snap->touched == NULL)
            snap->touched = (uint32 *) MemoryContextAlloc(snap->cxt,
                                                          newcap * sizeof(uint32));
        else
            snap->touched = (uint32 *) repalloc(snap->touched,
                                                 newcap * sizeof(uint32));
        snap->cap_touched = newcap;
    }
    snap->touched[snap->ntouched++] = slot;
}

static int
pendlog_slot_cmp(const void *a, const void *b)
{
    uint32 x = *(const uint32 *) a;
    uint32 y = *(const uint32 *) b;

    return (x < y) ? -1 : ((x > y) ? 1 : 0);
}

/*
 * pendlog_ingest_from  -- PHASE 1 of delta construction
 *
 * Walk the log chain from start_blk, skipping the first start_off records
 * of that first page, and fold every record into the snapshot's three
 * slot-level sets. NO text is read and NO identity is produced here; that
 * is pendlog_expand_touched()'s job, and the separation is load-bearing
 * (see the comment there).
 *
 * The three sets:
 *
 *   seen -- every slot the log mentions at all. Used only to notice a
 *           second mention.
 *
 *   live -- slots whose LAST record is an ADD. These are the slots that
 *           currently have text worth fanning out. A slot inserted and
 *           then deleted inside one drain window leaves `live` and is
 *           never expanded, which is exactly right: it contributes
 *           nothing to any structure.
 *
 *   kill -- slots whose membership in the BASE blobs may be stale, and
 *           which must therefore be subtracted from every base bitmap
 *           before the delta's additions are applied. A slot lands here
 *           if it was ever REMOVEd, or if it was ADDed more than once.
 *
 *           It deliberately does NOT contain slots that were ADDed
 *           exactly once and never removed, and that exclusion is what
 *           keeps the common case fast. A base blob can only contain a
 *           slot if some earlier, already-drained record put it there;
 *           and every path that rewrites a live slot (biscuit_insert()'s
 *           UPDATE branch, biscuit_bulkdelete()) emits a REMOVE first.
 *           So a single unaccompanied ADD is necessarily a slot the base
 *           has never seen, and subtracting it would be a no-op bought
 *           with a bitmap copy per structure per query. A pure-insert
 *           workload therefore leaves `kill` empty and pays nothing.
 *
 * On return snap->resume_blk/resume_off point just past the last record
 * consumed, so a later call resumes exactly there. Within one drain cycle
 * the log is strictly append-only -- pages already in the chain are
 * immutable, only the tail's num_records grows -- so re-reading from the
 * resume point yields precisely what is new.
 *
 * Allocates into snap->cxt.
 */
static void
pendlog_ingest_from(Relation index, BiscuitPendLogSnapshot *snap,
                     BlockNumber start_blk, uint32 start_off)
{
    MemoryContext old = MemoryContextSwitchTo(snap->cxt);
    BlockNumber   cur = start_blk;
    uint32        off = start_off;

    while (cur != InvalidBlockNumber)
    {
        Buffer                     buf = ReadBuffer(index, cur);
        Page                       page;
        BiscuitPendLogPageHeader  *hdr;
        BiscuitPendLogRecord      *recs;
        BiscuitPageOpaque          opaque;
        BlockNumber                next;
        uint32                     i, n;

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        hdr  = (BiscuitPendLogPageHeader *) BiscuitPageDataPtr(page);
        recs = PendLogRecords(hdr);
        n    = hdr->num_records;

        for (i = off; i < n; i++)
        {
            uint32 rslot = recs[i].slot;
            bool   seen_before;

            seen_before = biscuit_roaring_contains(snap->seen, rslot);

            /*
             * Already expanded in an earlier pass over this same snapshot?
             * Then its identities were derived from text that has since
             * been overwritten, and there is no way to withdraw them.
             * Flag it and let the caller rebuild; keep walking so
             * resume_blk/resume_off stay honest either way.
             */
            if (biscuit_roaring_contains(snap->expanded, rslot))
                snap->need_rebuild = true;

            if (recs[i].op == BISCUIT_PENDING_OP_REMOVE)
            {
                biscuit_roaring_add(snap->kill, rslot);
                biscuit_roaring_remove(snap->live, rslot);
            }
            else
            {
                if (seen_before)
                    biscuit_roaring_add(snap->kill, rslot);
                biscuit_roaring_add(snap->live, rslot);
            }

            biscuit_roaring_add(snap->seen, rslot);
            pendlog_touch_slot(snap, rslot);
            snap->nrecords++;
        }

        /*
         * Resume point: this page, everything up to n consumed. If the
         * chain ends here this is the tail, and a later call re-reads this
         * same page and picks up any records appended in the meantime.
         */
        snap->resume_blk = cur;
        snap->resume_off = n;

        opaque = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        next   = opaque->next;
        UnlockReleaseBuffer(buf);

        cur = next;
        off = 0;    /* only the first page of this walk is partially consumed */
    }

    MemoryContextSwitchTo(old);
}

/* ---- delta expansion sink ---- */

typedef struct DeltaEmitCtx
{
    BiscuitPendLogSnapshot *snap;
    uint32                  max_slot;   /* for fallback-bitset presizing */
} DeltaEmitCtx;

/*
 * Record one (structure identity, slot) pair into the delta hash.
 *
 * Keyed by the same BiscuitPendLogKey the directory and the old derived
 * records used, so a delta entry is directly comparable to a base
 * structure with no translation. Sparse by construction: only identities
 * the delta's rows actually produce get an entry, so memory scales with
 * delta size rather than with the index's key space (a dense mirror would
 * be 256 x max_length x 2 x 2 pointers, almost all empty).
 */
static void
pendlog_delta_emit(void *ctxp,
                   int32 col, bool is_lower, uint8 kind,
                   int32 ch, int32 position, uint32 slot, uint8 op)
{
    DeltaEmitCtx            *ectx = (DeltaEmitCtx *) ctxp;
    BiscuitPendLogSnapshot  *snap = ectx->snap;
    BiscuitPendLogKey        key;
    BiscuitPendLogEntry     *e;
    bool                     found;
    MemoryContext            old;

    /* Expansion only ever produces additions; removals are the kill set. */
    Assert(op == BISCUIT_PENDING_OP_ADD);
    (void) op;

    pendlog_key_init(&key, col, is_lower, kind, ch, position);

    old = MemoryContextSwitchTo(snap->cxt);

    e = (BiscuitPendLogEntry *) hash_search(snap->htab, &key, HASH_ENTER, &found);
    if (!found)
        e->adds = biscuit_roaring_create_sized(ectx->max_slot);

    biscuit_roaring_add(e->adds, slot);
    snap->nidentities++;

    MemoryContextSwitchTo(old);
}

/*
 * pendlog_expand_touched  -- PHASE 2 of delta construction
 *
 * Take the slots ingested since the last expansion, keep the ones that are
 * still live, read their text from STRCACHE, and fan it out into the delta
 * hash.
 *
 * Why this is separate from ingest, and must stay separate: expansion
 * reads whatever text a slot currently has. A slot must therefore reach
 * its FINAL state within the ingested range before its text is consulted,
 * or an early reading gets superseded with no way to withdraw it. Ingest
 * can be called twice in one build (the abandoned chain, then the live
 * one); expansion runs once, afterwards.
 */
static void
pendlog_expand_touched(Relation index, BiscuitPendLogSnapshot *snap)
{
    DeltaEmitCtx  ectx;
    uint32       *slots;
    int           nslots = 0;
    int           i;
    uint32        max_slot = 0;

    if (snap->ntouched == 0)
        return;

    /*
     * Filter to live-and-not-yet-expanded, in ascending order.
     *
     * Ascending matters twice over. biscuit_delta_expand_slots() walks the
     * STRCACHE pointer directory once and reuses the value-heap buffer
     * across consecutive slots -- the heap is bump-allocated in slot
     * order, so a sorted run is a handful of buffer reads and an unsorted
     * one is random I/O. And on the fallback bitset, adding in ascending
     * order means each delta bitmap grows at most once, to its final size.
     *
     * The touched list is already ascending per ingest pass (the log is
     * append-only and slots are claimed monotonically), but it is sorted
     * explicitly rather than assumed: an UPDATE re-mentions an older slot,
     * and the freelist could reintroduce out-of-order slots if slot reuse
     * is ever restored to the insert path.
     */
    qsort(snap->touched, snap->ntouched, sizeof(uint32), pendlog_slot_cmp);

    slots = (uint32 *) palloc(snap->ntouched * sizeof(uint32));
    for (i = 0; i < snap->ntouched; i++)
    {
        uint32 sl = snap->touched[i];

        if (i > 0 && sl == snap->touched[i - 1])
            continue;                                   /* dedupe */
        if (!biscuit_roaring_contains(snap->live, sl))
            continue;                                   /* finally removed */
        if (biscuit_roaring_contains(snap->expanded, sl))
            continue;                                   /* already done */

        slots[nslots++] = sl;
        if (sl > max_slot)
            max_slot = sl;
    }

    snap->ntouched = 0;

    if (nslots == 0)
    {
        pfree(slots);
        return;
    }

    /*
     * SIZE FROM max(slot), NOT FROM THE COUNT.
     *
     * Slots are recycled and claimed monotonically, so a 500-record delta
     * can perfectly well contain slot 900,000. On the fallback bitset,
     * presizing from the record count is not a mis-size -- it is a buffer
     * overrun. One pass over the filtered list makes it exact, and it is
     * the same pass that already had to run.
     */
    ectx.snap     = snap;
    ectx.max_slot = max_slot;

    biscuit_delta_expand_slots(index, slots, nslots,
                                BISCUIT_PENDING_OP_ADD,
                                pendlog_delta_emit, &ectx);

    for (i = 0; i < nslots; i++)
        biscuit_roaring_add(snap->expanded, slots[i]);

    pfree(slots);
}

BiscuitPendLogEntry *
biscuit_pendlog_lookup(BiscuitPendLogSnapshot *snap,
                        int32 col, bool is_lower, uint8 kind,
                        int32 ch, int32 position)
{
    BiscuitPendLogKey key;

    if (snap == NULL || snap->htab == NULL)
        return NULL;

    pendlog_key_init(&key, col, is_lower, kind, ch, position);
    return (BiscuitPendLogEntry *) hash_search(snap->htab, &key, HASH_FIND, NULL);
}

/*
 * Does this snapshot change ANY structure, whether or not it has an entry
 * for it?
 *
 * The kill set makes that a real question. Under the old derived-record
 * scheme a structure with no pending entry was untouched by definition,
 * so a hash miss meant "return the cached bitmap, borrowed, zero copies".
 * That is no longer true: a deleted row must vanish from every base
 * bitmap it was in, and the log records the deletion once, against the
 * slot, not once per structure. So a structure with no entry may still
 * need the kill set subtracted from it.
 *
 * Callers use this to keep the old fast path where it is still valid: an
 * empty kill set means no base membership is stale, which is the case for
 * any workload that has not deleted or updated a row since the last drain.
 */
bool
biscuit_pendlog_has_kills(const BiscuitPendLogSnapshot *snap)
{
    return snap != NULL && snap->kill != NULL &&
           !biscuit_roaring_is_empty(snap->kill);
}

/*
 * Reconcile one structure: target := (target \ kill) | entry->adds.
 *
 * ORDER IS NOT NEGOTIABLE, and the reason is worth stating because the
 * arithmetic looks commutative and is not. A slot that was updated in
 * place appears in BOTH kill (its base membership is stale) and, under
 * its new text's identities, in adds. Subtracting after adding would
 * remove the row from the structures it now belongs to. Subtract first.
 *
 * There is no per-structure removal list and there must not be one. A
 * REMOVE is recorded against a slot, and the set of structures that slot
 * belonged to is a property of text that may no longer exist -- an UPDATE
 * has already overwritten STRCACHE by the time anything reads the log. The
 * kill set sidesteps that entirely: withdraw the slot from whatever the
 * base says, then re-add it from whatever it says now.
 *
 * MVCC does not make a mistake here survivable, and no part of this may
 * lean on it. Because xs_recheck is false, nothing re-tests the predicate
 * after the index yields a TID. MVCC filters DEAD rows; it does not filter
 * WRONG rows. A stale membership that survives this reconciliation is a
 * live, visible tuple returned for a pattern it does not match, and the
 * executor will happily hand it to the user. In the other direction, a row
 * wrongly missing from a negation's base set is simply absent from the
 * result -- there is no tuple for anything downstream to filter at all.
 */
void
biscuit_pendlog_apply(const BiscuitPendLogSnapshot *snap,
                       const BiscuitPendLogEntry *entry, RoaringBitmap *target)
{
    if (target == NULL)
        return;

    if (biscuit_pendlog_has_kills(snap))
        biscuit_roaring_andnot_inplace(target, snap->kill);

    if (entry != NULL && entry->adds != NULL)
        biscuit_roaring_or_inplace(target, entry->adds);
}

/* ================================================================
 * DRAIN
 * ================================================================ */

/* Read the abandoned-drain marker (see BiscuitMetaPageData.pendlog_draining). */
static BlockNumber
pendlog_read_draining(Relation index)
{
    Buffer               mbuf;
    BiscuitMetaPageData *meta;
    BlockNumber          blk;

    if (RelationGetNumberOfBlocks(index) == 0)
        return InvalidBlockNumber;

    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_SHARE);
    meta = (BiscuitMetaPageData *) PageGetSpecialPointer(BufferGetPage(mbuf));
    blk  = meta->pendlog_draining;
    UnlockReleaseBuffer(mbuf);
    return blk;
}

/* Clear the marker once a drain's merge has completed successfully. */
static void
pendlog_clear_draining(Relation index)
{
    Buffer               mbuf;
    Page                 mpage;
    BiscuitMetaPageData *meta;
    GenericXLogState    *state;

    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
    biscuit_ensure_synchronous_commit();
    state = GenericXLogStart(index);
    mpage = GenericXLogRegisterBuffer(state, mbuf, 0);
    meta  = (BiscuitMetaPageData *) PageGetSpecialPointer(mpage);
    meta->pendlog_draining = InvalidBlockNumber;

    /*
     * The merge is now durable in the blobs and this chain is about to
     * become unreachable. A backend that reloaded *during* the drain
     * window holds pre-merge blobs and was ingesting the abandoned chain
     * to compensate; the moment the marker clears, that chain stops being
     * ingested, so its copy must be re-read. See the matching bump in
     * pendlog_detach() for the full rationale.
     */
    meta->gen++;

    GenericXLogFinish(state);
    UnlockReleaseBuffer(mbuf);
}

/*
 * pendlog_detach
 *
 * Atomically make the current log chain unreachable and hand its head to
 * the caller, who then owns it exclusively.
 *
 * Under one GenericXLog transaction holding BOTH the metapage and the tail
 * page exclusively:
 *   - metapage: head/tail = Invalid, npages = 0, total_drains++
 *   - tail page: clear BISCUIT_PENDING_FLAG_TAIL
 *
 * Clearing that flag is what makes the whole thing safe against
 * concurrent appenders. An appender reads pendlog_tail under a SHARE lock
 * and releases it before locking the tail page, so it can arrive at a page
 * we have already detached. It re-checks the flag under the tail page's
 * exclusive lock -- the same lock we hold here -- and restarts if the flag
 * is gone. Without that handshake an append could land on a detached page
 * and be freed along with the chain: a committed transaction whose index
 * entries silently disappear.
 *
 * Returns InvalidBlockNumber if the log was already empty (or another
 * drainer detached it first), in which case there is nothing to do.
 *
 * Lock order is metapage-then-tail-page, matching
 * biscuit_pendlog_append()'s rollover branch, which is the only other site
 * holding both.
 */
static BlockNumber
pendlog_detach(Relation index)
{
    Buffer               mbuf, tbuf;
    Page                 mpage, tpage;
    BiscuitMetaPageData *meta;
    GenericXLogState    *state;
    BlockNumber          head, tail, draining;
    BiscuitPageOpaque    topaque;

    if (RelationGetNumberOfBlocks(index) == 0)
        return InvalidBlockNumber;

    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
    meta     = (BiscuitMetaPageData *) PageGetSpecialPointer(BufferGetPage(mbuf));
    head     = meta->pendlog_head;
    tail     = meta->pendlog_tail;
    draining = meta->pendlog_draining;

    /*
     * An earlier drain already detached a chain (durably recorded in
     * pendlog_draining) and then died -- crash, error, or backend kill --
     * somewhere between that detach and pendlog_clear_draining(). That
     * chain's deltas are only safe once some drain has ingested, merged
     * and freed it. pendlog_draining is a single BlockNumber, not a list:
     * it can only ever describe ONE outstanding chain at a time.
     *
     * Bug this fixes: this function used to only check pendlog_draining
     * when head == InvalidBlockNumber, and otherwise fell through and
     * unconditionally overwrote it with the newly detached head a few
     * lines down (meta->pendlog_draining = head). That is safe within a
     * single successful drain call -- the caller (biscuit_pendlog_drain_all)
     * already captured the old value in its own local `abandoned` before
     * calling this, and correctly ingests/frees both chains. But if a
     * *second* drain then also died before finishing its merge, the
     * on-disk marker by then pointed only at the second drain's chain --
     * the first, still-unrecovered chain was silently orphaned: no longer
     * referenced by pendlog_draining, never freed (biscuit_page_free_blob()
     * is what stamps recycle_xid, and it never got to run on it), so
     * biscuit_page_alloc() would never reclaim it either. Any structure
     * whose deltas lived only in that first chain and hadn't yet been
     * merged into a blob when the second drain died was permanently lost
     * on recovery -- not corrupted, just silently gone.
     *
     * Fix: if a chain is already outstanding, adopt it and return
     * immediately WITHOUT touching the live log at all, even if the live
     * log also has pages. That guarantees pendlog_draining is only ever
     * written while it is InvalidBlockNumber, so it can never be
     * clobbered. The live log is left completely alone -- still reachable
     * via meta->pendlog_head/tail, still growing if appenders keep
     * writing to it -- and gets its own turn safely on a later call, once
     * this one has cleared the marker.
     */
    if (draining != InvalidBlockNumber)
    {
        UnlockReleaseBuffer(mbuf);
        return draining;
    }

    if (head == InvalidBlockNumber)
    {
        UnlockReleaseBuffer(mbuf);
        return InvalidBlockNumber;
    }

    tbuf = ReadBuffer(index, tail);
    LockBuffer(tbuf, BUFFER_LOCK_EXCLUSIVE);

    biscuit_ensure_synchronous_commit();
    state = GenericXLogStart(index);
    mpage = GenericXLogRegisterBuffer(state, mbuf, 0);
    tpage = GenericXLogRegisterBuffer(state, tbuf, 0);

    meta = (BiscuitMetaPageData *) PageGetSpecialPointer(mpage);
    meta->pendlog_head   = InvalidBlockNumber;
    meta->pendlog_tail   = InvalidBlockNumber;
    meta->pendlog_npages = 0;
    meta->total_drains++;

    /*
     * BLOCKER-1, drain half.
     *
     * A drain relocates deltas from the shared log into the compacted
     * blobs. Any other backend holding a cached BiscuitIndex built from
     * the *pre-drain* blobs was relying on this log to supply exactly
     * those deltas at query time; once they are merged and the log is
     * gone, biscuit_pendlog_snapshot() correctly returns NULL and that
     * backend's bitmaps quietly lose every membership the drain absorbed.
     * Same symptom as the insert case, different trigger -- and this one
     * fires from autovacuum, with no user action at all.
     *
     * meta->gen is the one counter every backend already re-checks on
     * beginscan/rescan (biscuit_get_current_index()), so bumping it here
     * forces the reload. Free: we are already inside this function's
     * GenericXLog transaction with the metapage exclusively locked.
     *
     * Bumped at both ends of the drain -- here, and again in
     * pendlog_clear_draining() -- because both transitions change what a
     * given set of blobs plus a given log state add up to. Over-
     * invalidation costs one reload; under-invalidation is wrong answers.
     */
    meta->gen++;

    /*
     * Publish the detached chain so it is recoverable if this merge dies.
     * Set in the SAME transaction that unlinks it from pendlog_head, so
     * there is no instant in which the chain is referenced by neither
     * field. See BiscuitMetaPageData.pendlog_draining.
     *
     * Safe to write unconditionally here: the guard above already
     * established meta->pendlog_draining == InvalidBlockNumber before we
     * got this far, so this cannot overwrite a still-outstanding marker
     * from an earlier drain that hasn't been recovered yet.
     */
    meta->pendlog_draining = head;

    topaque         = (BiscuitPageOpaque) PageGetSpecialPointer(tpage);
    topaque->flags &= ~BISCUIT_PENDING_FLAG_TAIL;

    GenericXLogFinish(state);
    UnlockReleaseBuffer(tbuf);
    UnlockReleaseBuffer(mbuf);

    return head;
}

/*
 * pendlog_detach_prefix
 *
 * Detach the first `max_pages` pages of the log as a standalone chain and
 * hand the caller its head. max_pages == 0 means "the whole log", which
 * degrades exactly to pendlog_detach() and is the VACUUM path.
 *
 * The prefix case is what makes incremental compaction possible: the rest
 * of the log stays reachable from pendlog_head, keeps its BISCUIT_PENDING_
 * FLAG_TAIL page, and remains appendable for the entire duration of the
 * merge. Only the shipped pages become unreachable.
 *
 * Three things happen in ONE GenericXLog transaction, and they have to:
 *   - metapage: pendlog_head advances past the prefix, npages drops by the
 *     number shipped, total_drains and gen are bumped, and pendlog_draining
 *     is set to the prefix head so an interrupted merge is recoverable.
 *   - the prefix's last page: opaque->next is severed, so nothing walking
 *     the detached chain wanders into the still-live remainder. Getting
 *     this wrong in the other order would let the merge ingest records
 *     that are still reachable from pendlog_head and then free the pages
 *     holding them.
 *
 * If an earlier drain died mid-merge, its chain is adopted and returned
 * untouched, exactly as pendlog_detach() does -- pendlog_draining is a
 * single BlockNumber and can only ever describe one outstanding chain, so
 * it must never be overwritten while set.
 */
static BlockNumber
pendlog_detach_prefix(Relation index, uint32 max_pages)
{
    Buffer               mbuf, lbuf;
    Page                 mpage, lpage;
    BiscuitMetaPageData *meta;
    GenericXLogState    *state;
    BlockNumber          head, tail, draining;
    BlockNumber          cur, next_after;
    BiscuitPageOpaque    lopaque;
    uint32               shipped;

    if (max_pages == 0)
        return pendlog_detach(index);

    if (RelationGetNumberOfBlocks(index) == 0)
        return InvalidBlockNumber;

    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
    meta     = (BiscuitMetaPageData *) PageGetSpecialPointer(BufferGetPage(mbuf));
    head     = meta->pendlog_head;
    tail     = meta->pendlog_tail;
    draining = meta->pendlog_draining;

    if (draining != InvalidBlockNumber)
    {
        UnlockReleaseBuffer(mbuf);
        return draining;
    }

    if (head == InvalidBlockNumber)
    {
        UnlockReleaseBuffer(mbuf);
        return InvalidBlockNumber;
    }

    /*
     * Walk the prefix to find its last page and what follows. Buffer reads
     * under the metapage's exclusive lock are safe here specifically
     * because nothing in this walk allocates -- biscuit_page_alloc() takes
     * the metapage lock itself, and holding it across an allocation is the
     * self-deadlock the append path's comment warns about.
     */
    cur        = head;
    shipped    = 0;
    next_after = InvalidBlockNumber;

    while (cur != InvalidBlockNumber && shipped < max_pages)
    {
        Buffer            b = ReadBuffer(index, cur);
        BiscuitPageOpaque o;
        BlockNumber       nxt;

        LockBuffer(b, BUFFER_LOCK_SHARE);
        o   = (BiscuitPageOpaque) PageGetSpecialPointer(BufferGetPage(b));
        nxt = o->next;
        UnlockReleaseBuffer(b);

        shipped++;
        if (shipped == max_pages)
            next_after = nxt;

        if (cur == tail)
        {
            /*
             * The prefix reached the tail, so there is no remainder to
             * keep. Fall back to a whole-log detach, which additionally
             * clears BISCUIT_PENDING_FLAG_TAIL -- the handshake that stops
             * a concurrent appender from writing into a chain we are about
             * to free.
             */
            UnlockReleaseBuffer(mbuf);
            return pendlog_detach(index);
        }
        cur = nxt;
    }

    if (next_after == InvalidBlockNumber)
    {
        /* Shorter than max_pages and never hit the tail: nothing sane to
         * ship a prefix of. Take the whole thing. */
        UnlockReleaseBuffer(mbuf);
        return pendlog_detach(index);
    }

    /* Re-walk to the prefix's last page so we can sever it under xlog. */
    cur = head;
    {
        uint32 i;

        for (i = 1; i < shipped; i++)
        {
            Buffer            b = ReadBuffer(index, cur);
            BiscuitPageOpaque o;
            BlockNumber       nxt;

            LockBuffer(b, BUFFER_LOCK_SHARE);
            o   = (BiscuitPageOpaque) PageGetSpecialPointer(BufferGetPage(b));
            nxt = o->next;
            UnlockReleaseBuffer(b);
            cur = nxt;
        }
    }

    lbuf = ReadBuffer(index, cur);
    LockBuffer(lbuf, BUFFER_LOCK_EXCLUSIVE);

    biscuit_ensure_synchronous_commit();
    state = GenericXLogStart(index);
    mpage = GenericXLogRegisterBuffer(state, mbuf, 0);
    lpage = GenericXLogRegisterBuffer(state, lbuf, 0);

    meta = (BiscuitMetaPageData *) PageGetSpecialPointer(mpage);
    meta->pendlog_head = next_after;
    meta->pendlog_npages = (meta->pendlog_npages > shipped)
                            ? meta->pendlog_npages - shipped : 0;

    /*
     * total_drains and gen both move for the same reason they do in a full
     * detach: the log's identity changed, and every backend's staleness
     * key and cached index have to notice. Over-invalidation costs a
     * reload; under-invalidation is wrong answers.
     */
    meta->total_drains++;
    meta->gen++;
    meta->pendlog_draining = head;

    lopaque       = (BiscuitPageOpaque) PageGetSpecialPointer(lpage);
    lopaque->next = InvalidBlockNumber;

    GenericXLogFinish(state);
    UnlockReleaseBuffer(lbuf);
    UnlockReleaseBuffer(mbuf);

    return head;
}

/* ================================================================
 * KILL SWEEP
 *
 * The one genuinely new cost this design introduces, and the one place
 * where "log the fact of the row" is more expensive than logging derived
 * records. It is worth being explicit about the trade rather than letting
 * it be discovered.
 *
 * Under the old scheme, deleting a row emitted an explicit REMOVE record
 * against every structure it belonged to -- biscuit_remove_from_all_indices()
 * literally walked the in-memory index and appended ~8N+4 records. That
 * made deletes enormously expensive in WAL, and made the drain cheap:
 * every structure needing an update was named in the log.
 *
 * Now a delete is eight bytes and names no structures at all, because the
 * set of structures it affects is a property of text that may already have
 * been overwritten. Reads handle this by subtracting the kill set from
 * every base bitmap they touch (biscuit_pendlog_apply()). But the drain
 * has to make that subtraction durable, and it cannot know which blobs
 * contain a killed slot without reading them. So when the kill set is
 * non-empty, the drain sweeps every bitmap-kind directory entry that the
 * merge loop did not already rewrite.
 *
 * The cost moved rather than appeared: a drain over a delete-bearing log
 * is O(total structures) instead of O(structures the log touched). A
 * pure-insert log leaves the kill set empty and skips this entirely, which
 * is why the check is on emptiness rather than on whether any REMOVE was
 * seen.
 *
 * Only bitmap kinds are swept. TIDS, TOMBSTONES, FREELIST, STRCACHE and
 * HEADER are row-identity structures whose element domain is not "slots
 * matching a pattern", and biscuit_rowstore.c owns their durability; a
 * kill applied to them would corrupt them.
 */

static bool
pendlog_kind_is_bitmap(uint8 kind)
{
    return kind == BISCUIT_DIR_KIND_POS ||
           kind == BISCUIT_DIR_KIND_NEG ||
           kind == BISCUIT_DIR_KIND_CACHE ||
           kind == BISCUIT_DIR_KIND_LEN ||
           kind == BISCUIT_DIR_KIND_LEN_GE;
}

typedef struct KillSweepState
{
    Relation                index;
    BiscuitPendLogSnapshot *snap;
    int                     swept;
} KillSweepState;

static void
pendlog_kill_sweep_cb(const BiscuitDirEntry *entry, void *state)
{
    KillSweepState     *st = (KillSweepState *) state;
    BiscuitPendLogKey   key;
    RoaringBitmap      *bm;
    BiscuitDirEntry     fresh;
    BiscuitDirEntryRef  ref;
    char               *blob;
    uint32              bloblen;
    char               *buf;
    uint32              len = 0;
    BlockNumber         newhead;
    uint32              written;

    if (!pendlog_kind_is_bitmap(entry->kind))
        return;
    if (entry->blob_head == InvalidBlockNumber)
        return;   /* nothing durable to subtract from */

    /*
     * Already handled by the merge loop, which applied the kill set to
     * this structure on its way past. Rewriting it again would be correct
     * but would cost a second blob rewrite per structure.
     */
    pendlog_key_init(&key, entry->col, entry->is_lower != 0,
                      entry->kind, entry->ch, entry->position);
    if (hash_search(st->snap->htab, &key, HASH_FIND, NULL) != NULL)
        return;

    /*
     * Re-find under our own reference rather than mutating the caller's
     * snapshot copy: biscuit_dir_foreach_column() hands out a copy with
     * the page lock already released, precisely so a callback can take its
     * own exclusive reference without self-deadlocking.
     */
    if (!biscuit_dir_find(st->index, entry->col, entry->is_lower != 0,
                           entry->kind, entry->ch, entry->position,
                           &fresh, &ref))
        return;
    if (fresh.blob_head == InvalidBlockNumber)
        return;

    biscuit_page_read_blob(st->index, fresh.blob_head, &blob, &bloblen);
    bm = biscuit_roaring_deserialize(blob, bloblen);
    if (blob)
        pfree(blob);

    biscuit_roaring_andnot_inplace(bm, st->snap->kill);

    buf = biscuit_roaring_serialize(bm, &len);
    biscuit_page_write_blob(st->index, buf, len, &newhead, &written);
    if (buf)
        pfree(buf);
    biscuit_roaring_free(bm);

    /*
     * Repoint before freeing, for the same reason the merge loop does:
     * biscuit_page_free_blob() rewrites each freed page's opaque->next
     * into a freelist link, so a reader following a still-stale blob_head
     * would walk off into recycled pages.
     */
    {
        BlockNumber oldhead = fresh.blob_head;

        fresh.blob_head = newhead;
        biscuit_dir_update(st->index, &ref, &fresh);

        if (oldhead != InvalidBlockNumber)
            biscuit_page_free_blob(st->index, oldhead);
    }

    st->swept++;
}

static int
pendlog_kill_sweep(Relation index, BiscuitPendLogSnapshot *snap)
{
    KillSweepState st;
    int            nslots;
    int            i;

    if (!biscuit_pendlog_has_kills(snap))
        return 0;

    st.index = index;
    st.snap  = snap;
    st.swept = 0;

    nslots = biscuit_dir_num_slots(index);
    for (i = 0; i < nslots; i++)
        biscuit_dir_foreach_column(index, i, pendlog_kill_sweep_cb, &st);

    elog(DEBUG1, "Biscuit: kill sweep rewrote %d structure(s)", st.swept);
    return st.swept;
}

static int
pendlog_drain_internal(Relation index, bool wait, uint32 max_pages)
{
    BiscuitPendLogSnapshot *snap;
    HASH_SEQ_STATUS         seq;
    BiscuitPendLogEntry    *e;
    int                     drained = 0;
    BlockNumber             head;
    BlockNumber             abandoned;

    if (RelationGetNumberOfBlocks(index) == 0)
        return 0;

    /*
     * DRAIN LOCK -- serializes drainers against each other.
     *
     * Two drainers running concurrently (two appenders both crossing
     * BISCUIT_PENDLOG_DRAIN_PAGES, or an appender racing VACUUM) each do
     * read-blob / apply / write-new-blob / free-old-blob per structure. On
     * a structure both touch, both read the same blob_head and both free
     * it: the same pages land on the recycle freelist twice, which
     * corrupts the freelist itself, since the second push rewrites
     * opaque.next on a page already linked into it. The later dir_update
     * also silently discards the other drainer's merged result.
     *
     * Detaching does not fix this on its own -- two drainers can detach
     * two *different* chains and still collide on the structures those
     * chains have in common -- so the merge phase needs real mutual
     * exclusion. A heavyweight page lock on the metapage gives it, and is
     * the same mechanism and same lock target GIN uses to serialize
     * ginInsertCleanup().
     *
     * wait = false is the opportunistic append-path caller: if someone
     * else is already draining, that is exactly the work we wanted done,
     * so decline rather than queue behind an O(index size) merge while
     * holding up a user INSERT. wait = true is VACUUM, which must not
     * silently skip its unconditional pass.
     */
    if (wait)
        LockPage(index, BISCUIT_METAPAGE_BLKNO, ExclusiveLock);
    else if (!ConditionalLockPage(index, BISCUIT_METAPAGE_BLKNO, ExclusiveLock))
        return 0;

    /*
     * Detach first, merge second. Everything after this point operates on
     * a chain no other backend can reach or append to, which is what makes
     * the merge safe to run without blocking writers: appenders that
     * arrive now build a fresh log from scratch and their records are
     * simply drained next time.
     *
     * The previous version merged straight from the live log and only
     * cleared the metapage afterwards, so every append landing during the
     * merge -- an O(index size) window -- was folded into nothing and then
     * freed with the chain.
     */
    abandoned = pendlog_read_draining(index);
    head      = pendlog_detach_prefix(index, max_pages);
    if (head == InvalidBlockNumber)
    {
        UnlockPage(index, BISCUIT_METAPAGE_BLKNO, ExclusiveLock);
        return 0;   /* nothing pending, and nothing abandoned */
    }

    /*
     * Any cached read snapshot describes the log we just detached, and its
     * staleness key is now stale by construction (total_drains moved). Drop
     * it before we start rewriting the blobs it was meant to complement.
     */
    biscuit_pendlog_invalidate(RelationGetRelid(index));

    /*
     * Build a private, uncached materialization of the detached chain. Not
     * biscuit_pendlog_snapshot(): that reads the metapage, which no longer
     * points at this chain, and it would install the result as the
     * process-wide read cache, which must stay keyed to the *live* log.
     */
    snap = pendlog_snapshot_create(RelationGetRelid(index), CurrentMemoryContext,
                                    "biscuit pendlog drain");

    /*
     * Ingest the abandoned chain (if any) BEFORE the freshly detached one,
     * so both chains reach the same slot-level final state before any text
     * is read.
     */
    if (abandoned != InvalidBlockNumber && abandoned != head)
        pendlog_ingest_from(index, snap, abandoned, 0);

    pendlog_ingest_from(index, snap, head, 0);

    /*
     * Expand once, after both chains are in. The drain builds a fresh
     * snapshot every time, so need_rebuild cannot fire here -- nothing was
     * expanded before this call.
     *
     * CROSS-BACKEND LOCK-ORDER FIX.
     *
     * pendlog_expand_touched() calls biscuit_delta_expand_slots(), which
     * reads each touched slot's text back out of STRCACHE via plain
     * LockBuffer(SHARE) -- an LWLock, invisible to the deadlock detector.
     * A concurrent row-identity write (biscuit_persist_row_identity_
     * write_record(), biscuit_persist.c) can be holding one of those very
     * STRCACHE pages EXCLUSIVE, pinned open in its BiscuitXlogBatch for
     * the span of the whole row, while it is itself blocked -- via that
     * same backend's own alloc_lock hold -- behind nothing this drain
     * holds... UNLESS some third backend's wait chain threads the two
     * together. With no ordering rule between "a batch is open" and "a
     * drain is reading STRCACHE", that combination hangs rather than gets
     * caught: PG's deadlock detector only walks the heavyweight lock
     * wait-for graph, and a backend blocked on an LWLock is invisible to
     * it, so a cycle that closes through even one LWLock hop never fires
     * the detector.
     *
     * Fix: take the same heavyweight lock biscuit_persist_row_identity_
     * write_record() takes around its whole batch (biscuit_rowstore_
     * alloc_lock() / biscuit_persist.c) for the span in which this drain
     * reads STRCACHE. That makes "a batch is open" and "a drain is
     * reading STRCACHE" mutually exclusive via one heavyweight primitive
     * instead of racing through independent LWLocks -- by the time this
     * lock is granted, no writer's batch can still be holding a STRCACHE
     * page open, because a writer flushes its batch before releasing this
     * same lock (see biscuit_persist_row_identity_write_record()).
     *
     * Ordering stays total and cannot itself introduce a new cycle: this
     * is the only place that nests alloc_lock inside BISCUIT_METAPAGE_BLKNO
     * (already held above), and an ordinary row-identity write never takes
     * BISCUIT_METAPAGE_BLKNO at all -- so there is no path back the other
     * way for a cycle to close. See biscuit_rowstore_alloc_lock()'s comment
     * in biscuit_persist.c for the other half of this invariant.
     *
     * Scoped tightly to just this call, not the whole drain: the merge
     * loop below only touches directory pages and blobs, never the
     * specific STRCACHE row pages a batch could be holding, so holding
     * writers off for the whole O(index size) merge would cost far more
     * concurrency than the actual hazard requires.
     */
    biscuit_rowstore_alloc_lock(index);
    pendlog_expand_touched(index, snap);
    biscuit_rowstore_alloc_unlock(index);
    Assert(!snap->need_rebuild);

    /*
     * Merge each structure's deltas into its compacted blob. A structure
     * that has never been written before has no directory entry yet --
     * the append path deliberately doesn't create one -- so create it
     * here, which is the only place that needs to know that.
     */
    hash_seq_init(&seq, snap->htab);
    while ((e = (BiscuitPendLogEntry *) hash_seq_search(&seq)) != NULL)
    {
        BiscuitDirEntry     entry;
        BiscuitDirEntryRef  ref;
        RoaringBitmap      *bm;
        char               *buf;
        uint32              len = 0;
        BlockNumber         newhead;
        uint32              written;
        bool                exists;

        exists = biscuit_dir_find(index, e->key.col, e->key.is_lower != 0,
                                   e->key.kind, e->key.ch, e->key.position,
                                   &entry, &ref);
        if (!exists)
        {
            /*
             * First-ever write for this structure: create its directory
             * entry lazily, here at drain time. The append path never
             * touches the directory, which is precisely why it stays off
             * the hot path.
             */
            BiscuitDirEntryInit(&entry, e->key.col, e->key.is_lower != 0,
                                 e->key.kind, e->key.ch, e->key.position);
            biscuit_dir_insert(index, &entry, &ref);
        }

        /* Load, apply, write back. */
        if (entry.blob_head != InvalidBlockNumber)
        {
            char   *blob;
            uint32  bloblen;

            biscuit_page_read_blob(index, entry.blob_head, &blob, &bloblen);
            bm = biscuit_roaring_deserialize(blob, bloblen);
            if (blob)
                pfree(blob);
        }
        else
            bm = biscuit_roaring_create();

        biscuit_pendlog_apply(snap, e, bm);

        buf = biscuit_roaring_serialize(bm, &len);
        biscuit_page_write_blob(index, buf, len, &newhead, &written);
        if (buf)
            pfree(buf);
        biscuit_roaring_free(bm);

        /*
         * Repoint the directory at the new blob BEFORE freeing the old
         * one. biscuit_page_free_blob() doesn't just mark old pages dead:
         * biscuit_retire_page_locked() repurposes each freed page's
         * opaque->next as a freelist link, overwriting the real
         * chunk-chain pointer. Freeing the old chain first left a window
         * where the directory still pointed at it while its "next" links
         * had already been rewritten into the freelist, so a concurrent
         * (or even later, same-drain) reader following the still-stale
         * blob_head would walk off into recycled pages -- exactly the
         * "blob chunk chain inconsistency" corruption reproduced above.
         * Updating the directory first means any reader from here on sees
         * either the old, still-intact chain or the new one -- never one
         * mid-retirement.
         *
         * blob_head is the only field a bitmap-kind entry owns; there is
         * no per-entry pending state left to reset (the detached chain is
         * freed below) and strheap_* belongs to STRCACHE, which never
         * appears in this log. Leave everything else exactly as found.
         */
        {
            BlockNumber oldhead = entry.blob_head;

            entry.blob_head = newhead;
            biscuit_dir_update(index, &ref, &entry);

            if (oldhead != InvalidBlockNumber)
                biscuit_page_free_blob(index, oldhead);
        }

        drained++;
    }

    /*
     * Make the removals durable in the structures the merge loop did not
     * visit. Runs INSIDE the drain lock and before the recovery marker is
     * cleared, so a crash part-way leaves the marker set and the whole
     * chain -- kills included -- is re-ingested and re-applied by the next
     * drain. Re-applying a kill is idempotent (subtracting a slot that is
     * already absent is a no-op), which is what makes that safe.
     */
    drained += pendlog_kill_sweep(index, snap);

    /*
     * The chain is already unreachable, so freeing it last is safe in
     * either crash direction: a crash before this point leaks the chain's
     * pages (recovered by nothing until the index is dropped) but loses no
     * data, since every delta it held is already in a blob. There is no
     * ordering hazard with the metapage any more -- pendlog_detach()
     * cleared it before the merge began.
     */
    /*
     * Merge succeeded: clear the recovery marker first, then free. Order
     * matters -- clearing before freeing means a crash in between leaks
     * pages, while freeing before clearing would leave the marker pointing
     * at freed pages that a later drain would try to read as live records.
     */
    pendlog_clear_draining(index);

    biscuit_page_free_chain(index, head);
    if (abandoned != InvalidBlockNumber && abandoned != head)
        biscuit_page_free_chain(index, abandoned);

    pendlog_snapshot_free(snap);
    UnlockPage(index, BISCUIT_METAPAGE_BLKNO, ExclusiveLock);

    return drained;
}

int
biscuit_pendlog_drain_all(Relation index, bool wait)
{
    return pendlog_drain_internal(index, wait, 0 /* whole log */);
}

/*
 * INCREMENTAL COMPACTION (design §7.2)
 *
 * Waiting for VACUUM lets the delta grow without bound between vacuums,
 * and delta size is what sets both read latency and cold-start rebuild
 * cost. Compaction bounds it without a full drain: ship the oldest portion
 * of the log into base and drop those records.
 *
 * COMPACTION SHIPS A STRICT PREFIX OF THE LOG, NEVER A SELECTION OF
 * SLOTS. This is a rule, not an implementation detail, and it survives
 * even though the kill/add formulation happens to make out-of-order
 * application harmless for the *current* set of operations. The reason to
 * keep it stated and enforced is that the property it protects is not
 * local: for any slot, either every record up to the compaction point is
 * applied to base, or none is. Shipping "the significant slots" and
 * leaving others behind reorders a slot's history, and slot recycling
 * makes that concretely wrong -- a slot may hold ADD, REMOVE, ADD for
 * three different rows, and applying the first ADD without the REMOVE
 * leaves base claiming a row that no longer matches.
 *
 * It is tempting to reason that MVCC covers such a mistake. It does not.
 * MVCC filters dead rows, not wrong ones, and because xs_recheck is false
 * nothing re-tests the predicate on the way out. See
 * biscuit_pendlog_apply().
 *
 * Mechanically this is the existing drain applied to a prefix: detach the
 * first max_pages pages as their own chain, merge them exactly as a full
 * drain would, retire them, and leave the rest of the log live and
 * appendable throughout.
 */
int
biscuit_pendlog_compact(Relation index, bool wait, uint32 max_pages)
{
    if (max_pages == 0)
        max_pages = 1;
    return pendlog_drain_internal(index, wait, max_pages);
}

void
biscuit_pendlog_free_chain(Relation index)
{
    Buffer               mbuf;
    Page                 mpage;
    BiscuitMetaPageData *meta;
    GenericXLogState    *state;
    BlockNumber          head;

    if (RelationGetNumberOfBlocks(index) == 0)
        return;

    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
    biscuit_ensure_synchronous_commit();
    state = GenericXLogStart(index);
    mpage = GenericXLogRegisterBuffer(state, mbuf, 0);
    meta  = (BiscuitMetaPageData *) PageGetSpecialPointer(mpage);
    head  = meta->pendlog_head;
    meta->pendlog_head   = InvalidBlockNumber;
    meta->pendlog_tail   = InvalidBlockNumber;
    meta->pendlog_npages = 0;
    /*
     * Bump total_drains here too, even though this is the index-drop path
     * and not a real drain. It keeps the invariant the read-path staleness
     * key depends on -- "the log's identity changed" is always visible as a
     * total_drains change -- rather than leaving one caller that resets the
     * log without moving the counter.
     */
    meta->total_drains++;
    GenericXLogFinish(state);
    UnlockReleaseBuffer(mbuf);

    biscuit_pendlog_invalidate(RelationGetRelid(index));

    if (head != InvalidBlockNumber)
        biscuit_page_free_chain(index, head);
}
