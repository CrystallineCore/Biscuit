/*
 * biscuit_pendlog.c
 * See biscuit_pendlog.h for the design and the measurements that motivated it.
 */

#include "biscuit_common.h"
#include "biscuit_blob.h"
#include "biscuit_dir.h"
#include "biscuit_bitmap.h"
#include "biscuit_pendlog.h"
#include "storage/bufpage.h"
#include "utils/memutils.h"

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
 * APPEND
 * ================================================================ */

uint64
biscuit_pendlog_append(Relation index,
                        int32 col, bool is_lower, uint8 kind,
                        int32 ch, int32 position,
                        uint32 rec_idx, uint8 op)
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

    memset(&rec, 0, sizeof(rec));   /* zero-fills reserved[] too */
    rec.col      = (int16) col;
    rec.is_lower = is_lower ? 1 : 0;
    rec.kind     = kind;
    rec.ch       = ch;
    rec.position = position;
    rec.rec_idx  = rec_idx;
    rec.op       = op;

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

/* ================================================================
 * STATS
 * ================================================================ */

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
} PendLogSnapshotSlot;

static PendLogSnapshotSlot snapshot_cache[BISCUIT_PENDLOG_SNAPSHOT_SLOTS];
static uint64              snapshot_lru_clock = 0;

static void pendlog_ingest_from(Relation index, BiscuitPendLogSnapshot *snap,
                                 BlockNumber start_blk, uint32 start_off);

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

/* Drop one slot's snapshot (freeing its context) and mark the slot empty. */
static void
pendlog_slot_clear(PendLogSnapshotSlot *slot)
{
    if (slot->snap != NULL)
    {
        MemoryContextDelete(slot->snap->cxt);
        slot->snap = NULL;
    }
    slot->lru = 0;
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
    uint64                  count;
    uint64                  drains;
    uint32                  npages;
    MemoryContext           cxt;
    HASHCTL                 ctl;
    BiscuitPendLogSnapshot *snap;
    PendLogSnapshotSlot    *slot;

    if (RelationGetNumberOfBlocks(index) == 0)
        return NULL;

    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_SHARE);
    meta   = (BiscuitMetaPageData *) PageGetSpecialPointer(BufferGetPage(mbuf));
    head   = meta->pendlog_head;
    tail   = meta->pendlog_tail;
    npages = meta->pendlog_npages;
    drains = meta->total_drains;
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

    /* Empty log: the fully-drained steady state. No hash, no allocation,
     * and callers skip reconciliation entirely. */
    if (head == InvalidBlockNumber || count == 0)
    {
        biscuit_pendlog_invalidate(indexoid);
        return NULL;
    }

    slot = pendlog_slot_find(indexoid);
    if (slot != NULL)
    {
        snap = slot->snap;

        /*
         * Same drain cycle, so everything already ingested is still valid:
         * the log is append-only within a cycle, existing pages are
         * immutable, and only the tail's record count grows.
         */
        if (snap->built_at_drains == drains)
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
            snap->built_at_count = count;
            return snap;
        }

        /* A drain happened: the blobs absorbed these deltas. Start over. */
        pendlog_slot_clear(slot);
    }

    cxt = AllocSetContextCreate(CacheMemoryContext,
                                 "biscuit pendlog snapshot",
                                 ALLOCSET_DEFAULT_SIZES);

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = sizeof(BiscuitPendLogKey);
    ctl.entrysize = sizeof(BiscuitPendLogEntry);
    ctl.hcxt      = cxt;

    snap = (BiscuitPendLogSnapshot *) MemoryContextAllocZero(cxt, sizeof(*snap));
    snap->cxt             = cxt;
    snap->indexoid        = indexoid;
    snap->built_at_count  = count;
    snap->built_at_drains = drains;
    snap->resume_blk      = InvalidBlockNumber;
    snap->resume_off      = 0;
    snap->htab = hash_create("biscuit pendlog", 256, &ctl,
                              HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    pendlog_ingest_from(index, snap, head, 0);

    slot       = pendlog_slot_acquire();
    slot->snap = snap;
    slot->lru  = ++snapshot_lru_clock;

    return snap;
}

/*
 * pendlog_ingest_from
 *
 * Walk the log chain starting at start_blk, skipping the first start_off
 * records of that first page, and bucket everything else into snap's hash
 * by structure identity. Records are appended per structure in chain
 * order, which biscuit_pendlog_apply() relies on (add-then-remove of the
 * same rec_idx is not the same as remove-then-add).
 *
 * On return snap->resume_blk/resume_off point just past the last record
 * consumed, so a later call can pick up exactly where this one stopped.
 * That is what makes incremental extension possible: within one drain
 * cycle the log is strictly append-only -- pages already in the chain are
 * immutable and only the tail page's num_records grows -- so re-reading
 * from the resume point yields precisely the records that are new.
 *
 * Two callers, two uses of the same walk:
 *   - biscuit_pendlog_snapshot(), with (head, 0) for a cold build or
 *     (resume_blk, resume_off) to extend.
 *   - biscuit_pendlog_drain_all(), with (chain, 0) on a chain it has
 *     *detached* from the metapage, which by definition cannot be found by
 *     re-reading the metapage.
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
            BiscuitPendLogKey    key;
            BiscuitPendLogEntry *e;
            bool                 found;

            pendlog_key_init(&key, recs[i].col, recs[i].is_lower != 0,
                              recs[i].kind, recs[i].ch, recs[i].position);

            e = (BiscuitPendLogEntry *) hash_search(snap->htab, &key, HASH_ENTER, &found);
            if (!found)
            {
                e->capacity = 8;
                e->ndeltas  = 0;
                e->deltas   = (BiscuitPendLogDelta *)
                    MemoryContextAlloc(snap->cxt, e->capacity * sizeof(BiscuitPendLogDelta));
            }
            else if (e->ndeltas == e->capacity)
            {
                int newcap = e->capacity * 2;
                BiscuitPendLogDelta *nd = (BiscuitPendLogDelta *)
                    MemoryContextAlloc(snap->cxt, newcap * sizeof(BiscuitPendLogDelta));
                memcpy(nd, e->deltas, e->ndeltas * sizeof(BiscuitPendLogDelta));
                pfree(e->deltas);
                e->deltas   = nd;
                e->capacity = newcap;
            }

            e->deltas[e->ndeltas].rec_idx = recs[i].rec_idx;
            e->deltas[e->ndeltas].op      = recs[i].op;
            e->ndeltas++;
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

void
biscuit_pendlog_apply(const BiscuitPendLogEntry *entry, RoaringBitmap *target)
{
    int i;

    if (entry == NULL || target == NULL)
        return;

    /* In log order: the same rec_idx can be added and later removed (or
     * the reverse) within one undrained window, and only the last one
     * counts. */
    for (i = 0; i < entry->ndeltas; i++)
    {
        if (entry->deltas[i].op == BISCUIT_PENDING_OP_ADD)
            biscuit_roaring_add(target, entry->deltas[i].rec_idx);
        else
            biscuit_roaring_remove(target, entry->deltas[i].rec_idx);
    }
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
    BlockNumber          head, tail;
    BiscuitPageOpaque    topaque;

    if (RelationGetNumberOfBlocks(index) == 0)
        return InvalidBlockNumber;

    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
    meta = (BiscuitMetaPageData *) PageGetSpecialPointer(BufferGetPage(mbuf));
    head = meta->pendlog_head;
    tail = meta->pendlog_tail;

    if (head == InvalidBlockNumber)
    {
        /*
         * Live log is empty -- but an earlier drain may have died between
         * detaching and finishing. If so, adopt its chain: we hold the
         * drain lock, so no other drainer can be working on it.
         */
        BlockNumber abandoned = meta->pendlog_draining;

        UnlockReleaseBuffer(mbuf);
        return abandoned;
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
     * Publish the detached chain so it is recoverable if this merge dies.
     * Set in the SAME transaction that unlinks it from pendlog_head, so
     * there is no instant in which the chain is referenced by neither
     * field. See BiscuitMetaPageData.pendlog_draining.
     */
    meta->pendlog_draining = head;

    topaque         = (BiscuitPageOpaque) PageGetSpecialPointer(tpage);
    topaque->flags &= ~BISCUIT_PENDING_FLAG_TAIL;

    GenericXLogFinish(state);
    UnlockReleaseBuffer(tbuf);
    UnlockReleaseBuffer(mbuf);

    return head;
}

int
biscuit_pendlog_drain_all(Relation index, bool wait)
{
    BiscuitPendLogSnapshot *snap;
    MemoryContext           cxt;
    HASHCTL                 ctl;
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
    head      = pendlog_detach(index);
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
    cxt = AllocSetContextCreate(CurrentMemoryContext,
                                 "biscuit pendlog drain",
                                 ALLOCSET_DEFAULT_SIZES);

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = sizeof(BiscuitPendLogKey);
    ctl.entrysize = sizeof(BiscuitPendLogEntry);
    ctl.hcxt      = cxt;

    snap = (BiscuitPendLogSnapshot *) MemoryContextAllocZero(cxt, sizeof(*snap));
    snap->cxt        = cxt;
    snap->indexoid   = RelationGetRelid(index);
    snap->resume_blk = InvalidBlockNumber;   /* never resumed; a zeroed
                                              * BlockNumber would read as
                                              * block 0, the metapage */
    snap->resume_off = 0;
    snap->htab     = hash_create("biscuit pendlog drain", 256, &ctl,
                                  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    /*
     * Ingest the abandoned chain (if any) BEFORE the freshly detached one,
     * so replay order matches original append order across the two.
     */
    if (abandoned != InvalidBlockNumber && abandoned != head)
        pendlog_ingest_from(index, snap, abandoned, 0);

    pendlog_ingest_from(index, snap, head, 0);

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

        biscuit_pendlog_apply(e, bm);

        buf = biscuit_roaring_serialize(bm, &len);
        biscuit_page_write_blob(index, buf, len, &newhead, &written);
        if (buf)
            pfree(buf);
        biscuit_roaring_free(bm);

        if (entry.blob_head != InvalidBlockNumber)
            biscuit_page_free_blob(index, entry.blob_head);

        /*
         * blob_head is the only field a bitmap-kind entry owns; there is
         * no per-entry pending state left to reset (the detached chain is
         * freed below) and strheap_* belongs to STRCACHE, which never
         * appears in this log. Leave everything else exactly as found.
         */
        entry.blob_head = newhead;
        biscuit_dir_update(index, &ref, &entry);

        drained++;
    }

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

    MemoryContextDelete(cxt);
    UnlockPage(index, BISCUIT_METAPAGE_BLKNO, ExclusiveLock);

    return drained;
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
