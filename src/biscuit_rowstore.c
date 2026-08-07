/*
 * biscuit_rowstore.c
 * See biscuit_rowstore.h for the full design.
 *
 * Page layout conventions match biscuit_blob.c/biscuit_dir.c exactly:
 * standard page header, a small fixed struct (or nothing, for TIDSLOT/
 * STRPTR) written directly at the start of the data area, its payload
 * immediately following, and a BiscuitPageOpaqueData footer. pd_lower is
 * kept in sync purely for page-level tooling, same rationale as
 * biscuit_blob.c's file header.
 */

#include "biscuit_common.h"
#include "biscuit_blob.h"
#include "biscuit_rowstore.h"
#include "storage/bufpage.h"

#define BiscuitPageDataPtr(page)  ((char *) (page) + SizeOfPageHeaderData)

/* ================================================================
 * XLOG BATCHING -- see biscuit_common.h's BiscuitXlogBatch comment for
 * the full rationale. init()/flush() are the two calls a driving caller
 * (biscuit_persist.c) needs; biscuit_xlog_batch_register() is the one
 * every batch-aware steady-state write in this file funnels through, and
 * stays static/internal to this translation unit.
 * ================================================================ */

void
biscuit_xlog_batch_init(BiscuitXlogBatch *batch, Relation index)
{
    batch->index = index;
    batch->state = NULL;
    batch->nbufs = 0;
}

void
biscuit_xlog_batch_flush(BiscuitXlogBatch *batch)
{
    int i;

    if (batch->state == NULL)
        return;

    GenericXLogFinish(batch->state);
    for (i = 0; i < batch->nbufs; i++)
        UnlockReleaseBuffer(batch->bufs[i]);

    batch->state = NULL;
    batch->nbufs = 0;
}

/*
 * Register buf -- already pinned and exclusive-locked by the caller --
 * into batch, returning the writable page. Starts a new GenericXLogState
 * if batch is empty; flushes and starts a fresh one first if batch is
 * already holding MAX_GENERIC_XLOG_PAGES buffers.
 *
 * Ownership transfers to the batch on a successful call: the caller must
 * NOT call GenericXLogFinish() or UnlockReleaseBuffer() on buf itself.
 * biscuit_xlog_batch_flush() does both, once, for every buffer the batch
 * is holding, whenever it is next called (explicitly by the driving
 * caller, or implicitly by a later registration that overflows the
 * batch).
 */
static Page
biscuit_xlog_batch_register(BiscuitXlogBatch *batch, Buffer buf, int flags)
{
    if (batch->nbufs >= MAX_GENERIC_XLOG_PAGES)
        biscuit_xlog_batch_flush(batch);

    if (batch->state == NULL)
        batch->state = GenericXLogStart(batch->index);

    batch->bufs[batch->nbufs++] = buf;
    return GenericXLogRegisterBuffer(batch->state, buf, flags);
}

static inline void
BiscuitPageSetLower(Page page, Size used)
{
    ((PageHeader) page)->pd_lower = (LocationIndex) (SizeOfPageHeaderData + used);
}

static inline void
BiscuitPageBumpLower(Page page, Size used)
{
    LocationIndex needed = (LocationIndex) (SizeOfPageHeaderData + used);

    if (((PageHeader) page)->pd_lower < needed)
        ((PageHeader) page)->pd_lower = needed;
}

/* ================================================================
 * PAGEDIR: logical page number -> physical BlockNumber, append-only.
 *
 * Shared verbatim by TIDS (slot = ItemPointerData) and STRCACHE's
 * pointer array (slot = BiscuitStrPtr) -- this layer only ever moves
 * BlockNumbers around, it has no idea what the logical pages it points
 * at actually contain.
 * ================================================================ */

static inline Size
BiscuitPageDirUsedBytes(uint32 num_entries)
{
    return MAXALIGN(sizeof(BiscuitPageDirHeader))
         + (Size) num_entries * sizeof(BlockNumber);
}

/*
 * biscuit_pagedir_lookup
 * Walk the chain from root, returning the BlockNumber stored at logical
 * index `logical`, or InvalidBlockNumber if root is InvalidBlockNumber or
 * the chain doesn't reach that far yet (logical page never allocated).
 */
static BlockNumber
biscuit_pagedir_lookup(Relation index, BlockNumber root, uint32 logical)
{
    BlockNumber cur = root;
    uint32      skip = logical;

    while (cur != InvalidBlockNumber)
    {
        Buffer                buf = ReadBuffer(index, cur);
        Page                  page;
        BiscuitPageDirHeader *hdr;
        BlockNumber          *slots;
        BiscuitPageOpaque     opaque;
        BlockNumber           next;
        uint32                n;

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page  = BufferGetPage(buf);
        hdr   = (BiscuitPageDirHeader *) BiscuitPageDataPtr(page);
        slots = (BlockNumber *) ((char *) hdr + MAXALIGN(sizeof(BiscuitPageDirHeader)));
        n     = hdr->num_entries;

        if (skip < n)
        {
            BlockNumber result = slots[skip];

            UnlockReleaseBuffer(buf);
            return result;
        }
        skip -= n;

        opaque = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        next   = opaque->next;
        UnlockReleaseBuffer(buf);
        cur = next;
    }

    return InvalidBlockNumber;
}

/*
 * biscuit_pagedir_append
 * Append new_blockno as logical entry number `expect_logical`, allocating
 * the chain's first page (if *root is InvalidBlockNumber) or a new tail
 * page (if the current last page is full).
 *
 * DENSE IN-ORDER INVARIANT. This directory is a positional array, not a
 * keyed map: logical page L is "the L'th entry in chain order", so entries
 * must be appended in strictly increasing L with no gaps. `expect_logical`
 * exists to enforce that rather than trust it -- if a caller ever appended
 * page 2 before page 1, every subsequent lookup would silently return the
 * wrong physical block for every logical page from that point on, and the
 * corruption would surface far away from its cause (as garbage TIDs or
 * strings, not as an error).
 *
 * The invariant used to hold only "because a new logical page is only ever
 * needed by the fresh-append insert path, where slot_idx == num_records++
 * is strictly increasing". That reasoning was single-backend reasoning and
 * it did not survive concurrency: two backends whose slots landed on the
 * same not-yet-existing logical page both arrived here with the same
 * expect_logical, and the loser got the error below instead of an insert.
 *
 * This function now has exactly one caller, biscuit_pagedir_ensure(),
 * which computes expect_logical from the chain's live length while holding
 * the per-index row-identity allocation lock. The two checks below are
 * therefore unreachable by construction rather than merely unlikely, and
 * are retained as assertions against a future caller reintroducing the old
 * "caller supplies the number it thinks is next" contract. Do not call
 * this directly; call biscuit_pagedir_ensure().
 *
 * Unlike TIDSLOT/STRPTR writes, this always walks from the root to find
 * the true last page. Directory-chain growth is bounded by
 * (num_records / slots-per-page / entries-per-dir-page), i.e. millions of
 * rows per chain page, so this walk is a handful of page reads in
 * practice, not a hot-path cost (the same trade-off biscuit_dir.c's own
 * chain walk already makes -- see its file header).
 */
static void
biscuit_pagedir_append(Relation index, BlockNumber *root,
                        uint32 expect_logical, BlockNumber new_blockno)
{
    Size specialSize = MAXALIGN(sizeof(BiscuitPageOpaqueData));
    BlockNumber cur;
    uint32      seen = 0;

    if (*root == InvalidBlockNumber)
    {
        Buffer                buf;
        GenericXLogState     *state;
        Page                  page;
        BiscuitPageDirHeader *hdr;
        BlockNumber          *slots;
        BiscuitPageOpaque     opaque;

        if (expect_logical != 0)
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("biscuit: row-identity page directory would start at logical page %u, not 0",
                            expect_logical)));

        buf   = biscuit_page_alloc(index, BISCUIT_PAGE_PAGEDIR);
        state = GenericXLogStart(index);
        page  = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

        PageInit(page, BufferGetPageSize(buf), specialSize);
        hdr = (BiscuitPageDirHeader *) BiscuitPageDataPtr(page);
        hdr->max_entries = BiscuitPageDirMaxEntries(BufferGetPageSize(buf));
        hdr->num_entries = 1;

        slots    = (BlockNumber *) ((char *) hdr + MAXALIGN(sizeof(BiscuitPageDirHeader)));
        slots[0] = new_blockno;

        BiscuitPageSetLower(page, BiscuitPageDirUsedBytes(1));

        opaque              = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        opaque->next        = InvalidBlockNumber;
        opaque->page_kind   = BISCUIT_PAGE_PAGEDIR;
        opaque->flags       = 0;
        opaque->recycle_xid = InvalidTransactionId;

        GenericXLogFinish(state);
        *root = BufferGetBlockNumber(buf);
        UnlockReleaseBuffer(buf);
        return;
    }

    cur = *root;
    for (;;)
    {
        Buffer                buf = ReadBuffer(index, cur);
        Page                  page;
        BiscuitPageDirHeader *hdr;
        BiscuitPageOpaque     opaque;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page   = BufferGetPage(buf);
        hdr    = (BiscuitPageDirHeader *) BiscuitPageDataPtr(page);
        opaque = (BiscuitPageOpaque) PageGetSpecialPointer(page);

        if (hdr->num_entries < hdr->max_entries)
        {
            GenericXLogState     *state;
            Page                   p2;
            BiscuitPageDirHeader  *h2;
            BlockNumber           *slots;

            if (seen + hdr->num_entries != expect_logical)
            {
                UnlockReleaseBuffer(buf);
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("biscuit: row-identity page directory append out of order (next logical page is %u, caller supplied %u)",
                                seen + hdr->num_entries, expect_logical)));
            }

            state = GenericXLogStart(index);
            p2    = GenericXLogRegisterBuffer(state, buf, 0);
            h2    = (BiscuitPageDirHeader *) BiscuitPageDataPtr(p2);
            slots = (BlockNumber *) ((char *) h2 + MAXALIGN(sizeof(BiscuitPageDirHeader)));

            slots[h2->num_entries] = new_blockno;
            h2->num_entries++;
            BiscuitPageSetLower(p2, BiscuitPageDirUsedBytes(h2->num_entries));

            GenericXLogFinish(state);
            UnlockReleaseBuffer(buf);
            return;
        }

        if (opaque->next == InvalidBlockNumber)
        {
            Buffer                 newbuf;
            GenericXLogState      *state;
            Page                   p2, newpage;
            BiscuitPageDirHeader  *newhdr;
            BlockNumber           *slots;
            BiscuitPageOpaque      newopaque, oldopaque;

            if (seen + hdr->num_entries != expect_logical)
            {
                UnlockReleaseBuffer(buf);
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("biscuit: row-identity page directory append out of order (next logical page is %u, caller supplied %u)",
                                seen + hdr->num_entries, expect_logical)));
            }

            newbuf  = biscuit_page_alloc(index, BISCUIT_PAGE_PAGEDIR);
            state   = GenericXLogStart(index);
            p2      = GenericXLogRegisterBuffer(state, buf, 0);
            newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);

            PageInit(newpage, BufferGetPageSize(newbuf), specialSize);
            newhdr = (BiscuitPageDirHeader *) BiscuitPageDataPtr(newpage);
            newhdr->max_entries = BiscuitPageDirMaxEntries(BufferGetPageSize(newbuf));
            newhdr->num_entries = 1;

            slots    = (BlockNumber *) ((char *) newhdr + MAXALIGN(sizeof(BiscuitPageDirHeader)));
            slots[0] = new_blockno;
            BiscuitPageSetLower(newpage, BiscuitPageDirUsedBytes(1));

            newopaque              = (BiscuitPageOpaque) PageGetSpecialPointer(newpage);
            newopaque->next        = InvalidBlockNumber;
            newopaque->page_kind   = BISCUIT_PAGE_PAGEDIR;
            newopaque->flags       = 0;
            newopaque->recycle_xid = InvalidTransactionId;

            oldopaque       = (BiscuitPageOpaque) PageGetSpecialPointer(p2);
            oldopaque->next = BufferGetBlockNumber(newbuf);

            GenericXLogFinish(state);
            UnlockReleaseBuffer(newbuf);
            UnlockReleaseBuffer(buf);
            return;
        }

        {
            BlockNumber next = opaque->next;

            seen += hdr->num_entries;
            UnlockReleaseBuffer(buf);
            cur = next;
        }
    }
}

/*
 * biscuit_pagedir_count
 * Total number of logical entries currently in the chain, i.e. the
 * logical page number the next append would occupy.
 */
static uint32
biscuit_pagedir_count(Relation index, BlockNumber root)
{
    BlockNumber cur   = root;
    uint32      total = 0;

    while (cur != InvalidBlockNumber)
    {
        Buffer                buf = ReadBuffer(index, cur);
        Page                  page;
        BiscuitPageDirHeader *hdr;
        BiscuitPageOpaque     opaque;
        BlockNumber           next;

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page   = BufferGetPage(buf);
        hdr    = (BiscuitPageDirHeader *) BiscuitPageDataPtr(page);
        total += hdr->num_entries;

        opaque = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        next   = opaque->next;
        UnlockReleaseBuffer(buf);
        cur = next;
    }

    return total;
}

/*
 * biscuit_pagedir_ensure
 *
 * "Give me the physical block backing logical page `logical`, allocating
 * and linking a blank one of `page_kind` if it doesn't exist yet."
 *
 * This replaces the old lookup-then-allocate-then-append sequence that
 * biscuit_rowstore_tid_write()/biscuit_strptr_write() each open-coded, and
 * it exists because that sequence was not atomic across backends. Two
 * backends whose freshly-claimed slots happened to land on the same
 * not-yet-existing logical page would BOTH see InvalidBlockNumber from the
 * lookup, BOTH allocate a physical page, and both then call
 * biscuit_pagedir_append() with the same expect_logical. The first won;
 * the second hit the dense-in-order check and raised
 *
 *     biscuit: row-identity page directory append out of order
 *     (next logical page is 38, caller supplied 37)
 *
 * aborting an otherwise valid INSERT, and orphaning the slot page it had
 * already allocated. Because the losing backends were consistently the
 * same ones, this presented as writer starvation rather than as ordinary
 * contention: retrying did not help, because the retry raced exactly the
 * same way.
 *
 * Two things fix that, and both are needed:
 *
 *   1. Callers hold the per-index row-identity allocation lock (see
 *      biscuit_persist.c's biscuit_rowstore_alloc_lock()), so only one
 *      backend is ever inside this function for a given index at a time.
 *      That is what actually eliminates the race.
 *
 *   2. This function does the lookup and the allocation together, so
 *      "already exists" is a normal, silent return rather than a
 *      collision. Even without (1) this degrades to "someone beat me to
 *      it, use theirs" instead of an error plus a leaked page.
 *
 * The strict gap check is deliberately kept: `logical` beyond the end of
 * the chain by more than one is still a hard ERROR, because the directory
 * is a positional array and a gap really would silently mis-resolve every
 * later logical page. What is no longer treated as an error is `logical`
 * pointing at an entry that already exists -- that is now the expected
 * outcome of a benign race, not corruption.
 */
static BlockNumber
biscuit_pagedir_ensure(Relation index, BlockNumber *root,
                        uint32 logical, uint16 page_kind)
{
    BlockNumber blkno;
    uint32      have;
    uint32      i;

    blkno = biscuit_pagedir_lookup(index, *root, logical);
    if (blkno != InvalidBlockNumber)
        return blkno;

    /*
     * Fill forward from the chain's current length. Normally that means
     * allocating exactly one page, because slot numbers are handed out
     * contiguously by biscuit_claim_new_slot() and so logical pages are
     * needed in order.
     *
     * The loop is not decoration, though. A slot claim is deliberately
     * non-transactional -- the metapage counter advances at claim time and
     * never retreats -- so a transaction that claims a slot and then aborts
     * leaves that slot number permanently spoken for and never written.
     * Enough consecutive aborted claims (a full page's worth, ~1300 slots
     * at the default BLCKSZ) would skip a logical page entirely, and the
     * next successful insert would then be asking for a page one beyond the
     * end of the chain. The old code raised a hard error there, which would
     * have wedged every subsequent insert into that index permanently.
     * Allocating the skipped pages blank costs one page each and is
     * self-healing: a zeroed TIDSLOT page reads as entirely unoccupied,
     * which is exactly what those abandoned slots are.
     */
    have = biscuit_pagedir_count(index, *root);

    if (logical < have)
    {
        /*
         * The lookup missed but the entry is within the chain's length:
         * that means the directory itself is damaged, not merely short.
         */
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("biscuit: row-identity page directory in index \"%s\" has no block for logical page %u despite holding %u entries",
                        RelationGetRelationName(index), logical, have)));
    }

    for (i = have; i <= logical; i++)
    {
        Buffer             buf;
        GenericXLogState  *state;
        Page               page;
        Size               specialSize = MAXALIGN(sizeof(BiscuitPageOpaqueData));
        BiscuitPageOpaque  opaque;

        /*
         * Allocate the slot page blank. Both callers then perform exactly
         * the same in-place slot write they would perform on a pre-existing
         * page, which is why this returns a zeroed page rather than taking
         * the payload: it collapses the old "new logical page" and
         * "existing logical page" branches into one code path per caller. A
         * zeroed TIDSLOT page also reads as entirely unoccupied, which is
         * what the BISCUIT_SLOT_WRITE_FRESH guard in
         * biscuit_rowstore_tid_write() expects.
         */
        buf   = biscuit_page_alloc(index, page_kind);
        state = GenericXLogStart(index);
        page  = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

        PageInit(page, BufferGetPageSize(buf), specialSize);

        opaque              = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        opaque->next        = InvalidBlockNumber;
        opaque->page_kind   = page_kind;
        opaque->flags       = 0;
        opaque->recycle_xid = InvalidTransactionId;

        GenericXLogFinish(state);
        blkno = BufferGetBlockNumber(buf);
        UnlockReleaseBuffer(buf);

        biscuit_pagedir_append(index, root, i, blkno);
    }

    /* blkno is the last one allocated, i.e. logical's. */
    return blkno;
}

/*
 * biscuit_pagedir_free_all
 * Retire the PAGEDIR chain itself AND every page it points at (each
 * entry is a TIDSLOT or STRPTR page, depending on caller). Used only by
 * teardown (biscuit_rowstore_free_tid_chain/_free_str_chains).
 */static void
biscuit_pagedir_free_all(Relation index, BlockNumber root)
{
    BlockNumber cur = root;

    while (cur != InvalidBlockNumber)
    {
        Buffer                buf = ReadBuffer(index, cur);
        Page                  page;
        BiscuitPageDirHeader *hdr;
        BlockNumber          *slots;
        BiscuitPageOpaque     opaque;
        BlockNumber           next;
        uint32                i, n;

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page  = BufferGetPage(buf);
        hdr   = (BiscuitPageDirHeader *) BiscuitPageDataPtr(page);
        slots = (BlockNumber *) ((char *) hdr + MAXALIGN(sizeof(BiscuitPageDirHeader)));
        n     = hdr->num_entries;

        for (i = 0; i < n; i++)
            biscuit_page_free_chain(index, slots[i]);

        opaque = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        next   = opaque->next;
        UnlockReleaseBuffer(buf);

        biscuit_page_free_chain(index, cur);
        cur = next;
    }
}

/* ================================================================
 * TIDS
 * ================================================================ */

void
biscuit_rowstore_tid_write(Relation index, BlockNumber *pagedir_root,
                            uint32 slot_idx, const ItemPointerData *tid,
                            BiscuitSlotWriteMode mode, BiscuitXlogBatch *batch)
{
    uint32      slots_per_page = BiscuitTidSlotsPerPage(BLCKSZ);
    uint32      logical        = slot_idx / slots_per_page;
    uint32      offset         = slot_idx % slots_per_page;
    BlockNumber blkno;

    biscuit_ensure_synchronous_commit();

    /*
     * Resolve (allocating and linking a blank page if this logical page
     * doesn't exist yet) and then write the slot in place. There is no
     * longer a separate "new logical page" branch here: allocation is a
     * detail of biscuit_pagedir_ensure(), which is idempotent under a
     * concurrent racer, whereas the old open-coded
     * lookup-allocate-then-append could raise "page directory append out
     * of order" and starve the losing backend. See that function.
     */
    blkno = biscuit_pagedir_ensure(index, pagedir_root, logical,
                                    BISCUIT_PAGE_TIDSLOT);

    {
        Buffer            buf = ReadBuffer(index, blkno);
        GenericXLogState *state;
        Page              page;
        ItemPointerData  *slots;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        /*
         * DEFENSE IN DEPTH against a slot-allocation regression.
         *
         * The buffer lock we just took serializes concurrent writers to
         * this page, but it cannot tell us whether two of them were handed
         * the *same* slot number -- that decision was made further up, in
         * biscuit_claim_new_slot(). Before this check existed, a duplicate
         * claim reached exactly here and simply overwrote the previous
         * occupant, losing a row with no error anywhere. The only guard in
         * this file, biscuit_pagedir_append()'s dense-in-order check, fires
         * only when a whole new logical page must be allocated, which is
         * the rare case -- hence a loud failure in a minority of runs and
         * silent loss in all of them.
         *
         * A caller claiming a fresh slot asserts the slot has never been
         * written. A freshly PageInit'd TIDSLOT page is all zeroes and
         * ItemPointerIsValid() is false for a zeroed pointer, so "occupied"
         * is exactly ItemPointerIsValid(). Deleted slots are also cleared
         * to invalid (biscuit_bulkdelete() calls ItemPointerSetInvalid()
         * then rewrites the slot), so a recycled slot reads as free too.
         *
         * The read is done on the real buffer page before GenericXLogStart()
         * so we never open a WAL transaction we are about to abort out of.
         */
        if (mode == BISCUIT_SLOT_WRITE_FRESH)
        {
            ItemPointerData *cur_slots =
                (ItemPointerData *) BiscuitPageDataPtr(BufferGetPage(buf));

            if (ItemPointerIsValid(&cur_slots[offset]))
            {
                ItemPointerData occupant = cur_slots[offset];

                UnlockReleaseBuffer(buf);
                ereport(ERROR,
                        (errcode(ERRCODE_DATA_CORRUPTED),
                         errmsg("biscuit: row slot %u in index \"%s\" was already occupied on a fresh claim",
                                slot_idx, RelationGetRelationName(index)),
                         errdetail("Slot holds TID (%u,%u); the insert tried to store TID (%u,%u). "
                                   "Two writers were handed the same slot number.",
                                   ItemPointerGetBlockNumber(&occupant),
                                   ItemPointerGetOffsetNumber(&occupant),
                                   ItemPointerGetBlockNumber((ItemPointer) tid),
                                   ItemPointerGetOffsetNumber((ItemPointer) tid)),
                         errhint("This indicates a slot-allocation defect, not on-disk damage. "
                                 "REINDEX will restore the index; please report the occurrence.")));
            }
        }

        if (batch != NULL)
        {
            /*
             * Batched: register into the caller's shared transaction and
             * leave buf pinned+locked. biscuit_xlog_batch_flush() (called
             * by the driving caller once the whole row is written, or
             * earlier if the batch overflows) owns GenericXLogFinish() and
             * UnlockReleaseBuffer() for this buffer.
             */
            page = biscuit_xlog_batch_register(batch, buf, 0);

            slots         = (ItemPointerData *) BiscuitPageDataPtr(page);
            slots[offset] = *tid;

            BiscuitPageBumpLower(page, (Size) (offset + 1) * sizeof(ItemPointerData));
        }
        else
        {
            state = GenericXLogStart(index);
            page  = GenericXLogRegisterBuffer(state, buf, 0);

            slots         = (ItemPointerData *) BiscuitPageDataPtr(page);
            slots[offset] = *tid;

            BiscuitPageBumpLower(page, (Size) (offset + 1) * sizeof(ItemPointerData));

            GenericXLogFinish(state);
            UnlockReleaseBuffer(buf);
        }
    }
}

void
biscuit_rowstore_tid_read(Relation index, BlockNumber pagedir_root,
                           uint32 slot_idx, ItemPointerData *out_tid)
{
    uint32      slots_per_page = BiscuitTidSlotsPerPage(BLCKSZ);
    uint32      logical        = slot_idx / slots_per_page;
    uint32      offset         = slot_idx % slots_per_page;
    BlockNumber blkno          = biscuit_pagedir_lookup(index, pagedir_root, logical);
    Buffer            buf;
    Page              page;
    ItemPointerData  *slots;

    if (blkno == InvalidBlockNumber)
    {
        ItemPointerSetInvalid(out_tid);
        return;
    }

    buf = ReadBuffer(index, blkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page  = BufferGetPage(buf);
    slots = (ItemPointerData *) BiscuitPageDataPtr(page);
    *out_tid = slots[offset];
    UnlockReleaseBuffer(buf);
}

void
biscuit_rowstore_tid_read_all(Relation index, BlockNumber pagedir_root,
                               uint32 num_records, ItemPointerData *out_tids)
{
    uint32      slots_per_page = BiscuitTidSlotsPerPage(BLCKSZ);
    uint32      remaining      = num_records;
    uint32      written        = 0;
    BlockNumber cur            = pagedir_root;

    while (cur != InvalidBlockNumber && remaining > 0)
    {
        Buffer                buf = ReadBuffer(index, cur);
        Page                  page;
        BiscuitPageDirHeader *hdr;
        BlockNumber          *slots;
        BiscuitPageOpaque     opaque;
        BlockNumber           next;
        uint32                i, n;

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page  = BufferGetPage(buf);
        hdr   = (BiscuitPageDirHeader *) BiscuitPageDataPtr(page);
        slots = (BlockNumber *) ((char *) hdr + MAXALIGN(sizeof(BiscuitPageDirHeader)));
        n     = hdr->num_entries;

        for (i = 0; i < n && remaining > 0; i++)
        {
            Buffer            tbuf = ReadBuffer(index, slots[i]);
            Page              tpage;
            ItemPointerData  *tslots;
            uint32            take = Min(remaining, slots_per_page);

            LockBuffer(tbuf, BUFFER_LOCK_SHARE);
            tpage  = BufferGetPage(tbuf);
            tslots = (ItemPointerData *) BiscuitPageDataPtr(tpage);
            memcpy(out_tids + written, tslots, (Size) take * sizeof(ItemPointerData));
            UnlockReleaseBuffer(tbuf);

            written   += take;
            remaining -= take;
        }

        opaque = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        next   = opaque->next;
        UnlockReleaseBuffer(buf);
        cur = next;
    }

    if (remaining > 0)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("biscuit: tid page-directory has fewer slots than num_records (%u short)",
                        remaining)));
}

void
biscuit_rowstore_free_tid_chain(Relation index, BlockNumber pagedir_root)
{
    if (pagedir_root == InvalidBlockNumber)
        return;
    biscuit_pagedir_free_all(index, pagedir_root);
}

/* ================================================================
 * STRCACHE -- pointer array
 * ================================================================ */

/*
 * LOCK ORDER, WRITE SIDE: STRHEAP before STRPTR.
 *
 * biscuit_rowstore_str_write() calls biscuit_strheap_append() (which, under
 * an open BiscuitXlogBatch, leaves the STRHEAP tail page pinned+locked --
 * ownership transferred into the batch, not released) and only then calls
 * this function, which locks the STRPTR page below while that STRHEAP lock
 * is still held. So a batched row write takes STRHEAP-then-STRPTR.
 *
 * The read side (biscuit_rowstore_str_read_all() / _str_read_slots() in
 * this file) must never take STRPTR-then-STRHEAP as a result -- it used to,
 * via biscuit_strptr_materialize() being called while the caller's STRPTR
 * page was still locked, and that was an exact ABBA inversion of this
 * order: two ordinary buffer-content LWLocks, invisible to PostgreSQL's
 * deadlock detector, so a reader and a writer converging on the same
 * (STRPTR page, STRHEAP page) pair simply hung forever instead of one
 * being aborted. Both read functions now copy pointers out and release
 * their STRPTR page before calling biscuit_strptr_materialize(), so they
 * never hold both locks at once and there is nothing left to invert
 * against this order. Do not reintroduce a caller that holds a STRPTR
 * page locked across a call to biscuit_strptr_materialize().
 */
static void
biscuit_strptr_write(Relation index, BlockNumber *pagedir_root,
                      uint32 slot_idx, const BiscuitStrPtr *sp,
                      BiscuitXlogBatch *batch)
{
    uint32      ptrs_per_page = BiscuitStrPtrSlotsPerPage(BLCKSZ);
    uint32      logical       = slot_idx / ptrs_per_page;
    uint32      offset        = slot_idx % ptrs_per_page;
    BlockNumber blkno;

    /* Same collapse as biscuit_rowstore_tid_write() above -- see
     * biscuit_pagedir_ensure() for why the allocate-then-append branch
     * had to go. */
    blkno = biscuit_pagedir_ensure(index, pagedir_root, logical,
                                    BISCUIT_PAGE_STRPTR);

    {
        Buffer            buf = ReadBuffer(index, blkno);
        GenericXLogState *state;
        Page              page;
        BiscuitStrPtr    *slots;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        if (batch != NULL)
        {
            /* See biscuit_rowstore_tid_write()'s batched branch above --
             * same ownership transfer, buf stays pinned+locked. */
            page = biscuit_xlog_batch_register(batch, buf, 0);

            slots         = (BiscuitStrPtr *) BiscuitPageDataPtr(page);
            slots[offset] = *sp;

            BiscuitPageBumpLower(page, (Size) (offset + 1) * sizeof(BiscuitStrPtr));
        }
        else
        {
            state = GenericXLogStart(index);
            page  = GenericXLogRegisterBuffer(state, buf, 0);

            slots         = (BiscuitStrPtr *) BiscuitPageDataPtr(page);
            slots[offset] = *sp;

            BiscuitPageBumpLower(page, (Size) (offset + 1) * sizeof(BiscuitStrPtr));

            GenericXLogFinish(state);
            UnlockReleaseBuffer(buf);
        }
    }
}

static BiscuitStrPtr
biscuit_strptr_read(Relation index, BlockNumber pagedir_root, uint32 slot_idx)
{
    uint32        ptrs_per_page = BiscuitStrPtrSlotsPerPage(BLCKSZ);
    uint32        logical       = slot_idx / ptrs_per_page;
    uint32        offset        = slot_idx % ptrs_per_page;
    BlockNumber   blkno         = biscuit_pagedir_lookup(index, pagedir_root, logical);
    BiscuitStrPtr sp;

    if (blkno == InvalidBlockNumber)
    {
        sp.blkno  = InvalidBlockNumber;
        sp.offset = 0;
        sp.length = 0;
        return sp;
    }

    {
        Buffer          buf = ReadBuffer(index, blkno);
        Page            page;
        BiscuitStrPtr  *slots;

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page  = BufferGetPage(buf);
        slots = (BiscuitStrPtr *) BiscuitPageDataPtr(page);
        sp    = slots[offset];
        UnlockReleaseBuffer(buf);
    }

    return sp;
}

/* ================================================================
 * STRCACHE -- append-only value heap
 * ================================================================ */

static void
biscuit_strheap_append(Relation index, BlockNumber *head, BlockNumber *tail,
                        const char *data, uint32 len,
                        BlockNumber *out_blkno, uint32 *out_offset,
                        BiscuitXlogBatch *batch)
{
    Size   specialSize = MAXALIGN(sizeof(BiscuitPageOpaqueData));
    uint32 payload_max = BiscuitStrHeapMaxPayload(BLCKSZ);

    Assert(len <= payload_max);

    if (*head == InvalidBlockNumber)
    {
        /*
         * Allocating the chain's first page, same as biscuit_pagedir_
         * ensure()'s fresh-logical-page branch and the rollover branch
         * further down: its own self-contained transaction, not folded
         * into an in-progress batch (see BiscuitXlogBatch's comment in
         * biscuit_common.h). Flush first so the two stay disjoint.
         */
        Buffer                buf;
        GenericXLogState     *state;
        Page                  page;
        BiscuitStrHeapHeader *hdr;
        BiscuitPageOpaque     opaque;

        if (batch != NULL)
            biscuit_xlog_batch_flush(batch);

        buf   = biscuit_page_alloc(index, BISCUIT_PAGE_STRHEAP);
        state = GenericXLogStart(index);
        page  = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

        PageInit(page, BufferGetPageSize(buf), specialSize);
        hdr = (BiscuitStrHeapHeader *) BiscuitPageDataPtr(page);
        hdr->avail = payload_max;
        hdr->used  = 0;
        if (len > 0)
            memcpy((char *) hdr + MAXALIGN(sizeof(BiscuitStrHeapHeader)), data, len);
        hdr->used = len;

        BiscuitPageSetLower(page, MAXALIGN(sizeof(BiscuitStrHeapHeader)) + len);

        opaque              = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        opaque->next        = InvalidBlockNumber;
        opaque->page_kind   = BISCUIT_PAGE_STRHEAP;
        opaque->flags       = BISCUIT_PENDING_FLAG_TAIL;
        opaque->recycle_xid = InvalidTransactionId;

        GenericXLogFinish(state);

        *head = *tail = BufferGetBlockNumber(buf);
        *out_blkno  = *head;
        *out_offset = 0;
        UnlockReleaseBuffer(buf);
        return;
    }

    {
        Buffer                buf = ReadBuffer(index, *tail);
        Page                  page;
        BiscuitStrHeapHeader *hdr;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buf);
        hdr  = (BiscuitStrHeapHeader *) BiscuitPageDataPtr(page);

        if ((Size) hdr->used + len <= hdr->avail)
        {
            if (batch != NULL)
            {
                /* See biscuit_rowstore_tid_write()'s batched branch --
                 * same ownership transfer, buf stays pinned+locked. */
                Page                   p2  = biscuit_xlog_batch_register(batch, buf, 0);
                BiscuitStrHeapHeader  *h2  = (BiscuitStrHeapHeader *) BiscuitPageDataPtr(p2);
                uint32                 off = h2->used;

                if (len > 0)
                    memcpy((char *) h2 + MAXALIGN(sizeof(BiscuitStrHeapHeader)) + off, data, len);
                h2->used += len;

                BiscuitPageSetLower(p2, MAXALIGN(sizeof(BiscuitStrHeapHeader)) + h2->used);

                *out_blkno  = *tail;
                *out_offset = off;
                return;
            }
            else
            {
                GenericXLogState     *state = GenericXLogStart(index);
                Page                   p2   = GenericXLogRegisterBuffer(state, buf, 0);
                BiscuitStrHeapHeader  *h2   = (BiscuitStrHeapHeader *) BiscuitPageDataPtr(p2);
                uint32                 off  = h2->used;

                if (len > 0)
                    memcpy((char *) h2 + MAXALIGN(sizeof(BiscuitStrHeapHeader)) + off, data, len);
                h2->used += len;

                BiscuitPageSetLower(p2, MAXALIGN(sizeof(BiscuitStrHeapHeader)) + h2->used);

                GenericXLogFinish(state);
                *out_blkno  = *tail;
                *out_offset = off;
                UnlockReleaseBuffer(buf);
                return;
            }
        }

        /*
         * Tail full: allocate + link a new tail page, same nested-lock
         * pattern as biscuit_pendlog_append()'s page-rollover branch --
         * its own self-contained transaction, not folded into an
         * in-progress batch (see BiscuitXlogBatch's comment in
         * biscuit_common.h). Flush first so the two stay disjoint.
         */
        if (batch != NULL)
            biscuit_xlog_batch_flush(batch);
        {
            Buffer                 newbuf = biscuit_page_alloc(index, BISCUIT_PAGE_STRHEAP);
            GenericXLogState      *state;
            Page                   p2, newpage;
            BiscuitStrHeapHeader  *newhdr;
            BiscuitPageOpaque      newopaque, oldopaque;

            state   = GenericXLogStart(index);
            p2      = GenericXLogRegisterBuffer(state, buf, 0);
            newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);

            PageInit(newpage, BufferGetPageSize(newbuf), specialSize);
            newhdr = (BiscuitStrHeapHeader *) BiscuitPageDataPtr(newpage);
            newhdr->avail = payload_max;
            newhdr->used  = len;
            if (len > 0)
                memcpy((char *) newhdr + MAXALIGN(sizeof(BiscuitStrHeapHeader)), data, len);

            BiscuitPageSetLower(newpage, MAXALIGN(sizeof(BiscuitStrHeapHeader)) + len);

            newopaque              = (BiscuitPageOpaque) PageGetSpecialPointer(newpage);
            newopaque->next        = InvalidBlockNumber;
            newopaque->page_kind   = BISCUIT_PAGE_STRHEAP;
            newopaque->flags       = BISCUIT_PENDING_FLAG_TAIL;
            newopaque->recycle_xid = InvalidTransactionId;

            oldopaque         = (BiscuitPageOpaque) PageGetSpecialPointer(p2);
            oldopaque->next   = BufferGetBlockNumber(newbuf);
            oldopaque->flags &= ~BISCUIT_PENDING_FLAG_TAIL;

            GenericXLogFinish(state);

            *tail = BufferGetBlockNumber(newbuf);
            *out_blkno  = *tail;
            *out_offset = 0;

            UnlockReleaseBuffer(newbuf);
            UnlockReleaseBuffer(buf);
            return;
        }
    }
}

/* ================================================================
 * STRCACHE -- public entry points
 * ================================================================ */

void
biscuit_rowstore_str_write(Relation index, BlockNumber *ptr_pagedir_root,
                            BlockNumber *heap_head, BlockNumber *heap_tail,
                            uint32 slot_idx, const char *str, int32 len,
                            BiscuitXlogBatch *batch)
{
    BiscuitStrPtr sp;

    biscuit_ensure_synchronous_commit();

    if (len < 0)
    {
        sp.blkno  = InvalidBlockNumber;
        sp.offset = 0;
        sp.length = 0;
    }
    else
    {
        uint32 payload_max = BiscuitStrHeapMaxPayload(BLCKSZ);

        if ((uint32) len > payload_max)
        {
            /*
             * Oversized value: dedicated chunked blob chain (existing
             * biscuit_page_write_blob() primitive already handles
             * arbitrary length, and already combines its own chunk-chain
             * buffers per-transaction -- see biscuit_blob.c). Not
             * batch-aware: this is not the steady-state single-page write
             * the batch targets, and an oversized value is rare enough
             * (and its own chunk chain already amortized) that folding it
             * in would add complexity for no measurable gain.
             */
            BlockNumber blob_head;
            uint32      written;

            biscuit_page_write_blob(index, str, (uint32) len, &blob_head, &written);
            sp.blkno  = blob_head;
            sp.offset = BISCUIT_STRPTR_OVERSIZE_SENTINEL;
            sp.length = (uint32) len;
        }
        else
        {
            biscuit_strheap_append(index, heap_head, heap_tail, str, (uint32) len,
                                    &sp.blkno, &sp.offset, batch);
            sp.length = (uint32) len;
        }
    }

    biscuit_strptr_write(index, ptr_pagedir_root, slot_idx, &sp, batch);
}

/*
 * biscuit_rowstore_read_oversize_retry
 *
 * Same race as biscuit_persist_read_blob_retry() in biscuit_persist.c
 * (see that function's comment for the full mechanism), just resolved by
 * (ptr_pagedir_root, slot_idx) via biscuit_strptr_read() instead of a
 * directory identity: a concurrent compaction drain can retire an
 * oversized STRCACHE blob's chain between when we read the pointer and
 * when biscuit_page_read_blob() takes its first lock. Re-reading the
 * pointer gets the current one; biscuit_strptr_read() always reads fresh
 * from disk, so this costs nothing when there's nothing to retry.
 */
static void
biscuit_rowstore_read_oversize_retry(Relation index, BlockNumber ptr_pagedir_root,
                                      uint32 slot_idx, BiscuitStrPtr sp,
                                      char **out_data, uint32 *out_len)
{
    int attempt;

    for (attempt = 0; ; attempt++)
    {
        MemoryContext oldcxt = CurrentMemoryContext;

        PG_TRY();
        {
            biscuit_page_read_blob(index, sp.blkno, out_data, out_len);
            return;
        }
        PG_CATCH();
        {
            ErrorData *edata;

            MemoryContextSwitchTo(oldcxt);
            edata = CopyErrorData();

            if (edata->sqlerrcode != ERRCODE_T_R_SERIALIZATION_FAILURE ||
                attempt >= 4)
                ReThrowError(edata);
            FlushErrorState();

            sp = biscuit_strptr_read(index, ptr_pagedir_root, slot_idx);
            if (sp.blkno == InvalidBlockNumber)
            {
                *out_data = NULL;
                *out_len  = 0;
                return;
            }
            /* loop and retry with the freshly-resolved pointer */
        }
        PG_END_TRY();
    }
}

char *
biscuit_rowstore_str_read(Relation index, BlockNumber ptr_pagedir_root,
                           uint32 slot_idx, MemoryContext cxt, int *out_len)
{
    BiscuitStrPtr sp = biscuit_strptr_read(index, ptr_pagedir_root, slot_idx);
    char         *result;
    MemoryContext old;

    if (sp.blkno == InvalidBlockNumber)
    {
        if (out_len)
            *out_len = 0;
        return NULL;
    }

    if (sp.offset == BISCUIT_STRPTR_OVERSIZE_SENTINEL)
    {
        char   *data;
        uint32  len;

        biscuit_rowstore_read_oversize_retry(index, ptr_pagedir_root, slot_idx, sp,
                                              &data, &len);
        old    = MemoryContextSwitchTo(cxt);
        result = pnstrdup(data ? data : "", len);
        MemoryContextSwitchTo(old);
        if (data)
            pfree(data);
        if (out_len)
            *out_len = (int) len;
        return result;
    }

    {
        Buffer                 buf = ReadBuffer(index, sp.blkno);
        Page                   page;
        BiscuitStrHeapHeader  *hdr;
        const char            *base;

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        hdr  = (BiscuitStrHeapHeader *) BiscuitPageDataPtr(page);
        base = (const char *) hdr + MAXALIGN(sizeof(BiscuitStrHeapHeader)) + sp.offset;

        old    = MemoryContextSwitchTo(cxt);
        result = pnstrdup(base, sp.length);
        MemoryContextSwitchTo(old);

        UnlockReleaseBuffer(buf);
    }

    if (out_len)
        *out_len = (int) sp.length;
    return result;
}

void
biscuit_rowstore_free_str_chains(Relation index, BlockNumber ptr_pagedir_root,
                                  BlockNumber heap_head)
{
    if (ptr_pagedir_root != InvalidBlockNumber)
        biscuit_pagedir_free_all(index, ptr_pagedir_root);
    if (heap_head != InvalidBlockNumber)
        biscuit_page_free_chain(index, heap_head);
    /* Oversized-value blob chains referenced from individual STRPTR slots
     * are not walked here -- see biscuit_rowstore.h's teardown comment. */
}

/*
 * Materialize one BiscuitStrPtr into a palloc'd C string in cxt, reusing
 * *heap_buf (a pinned+share-locked value-heap buffer for block
 * *heap_blk) when this pointer resolves to that same page. Callers must
 * release *heap_buf when the run is finished.
 */
static char *
biscuit_strptr_materialize(Relation index, const BiscuitStrPtr *sp,
                            MemoryContext cxt,
                            Buffer *heap_buf, BlockNumber *heap_blk)
{
    char         *result;
    MemoryContext old;

    if (sp->blkno == InvalidBlockNumber)
        return NULL;

    if (sp->offset == BISCUIT_STRPTR_OVERSIZE_SENTINEL)
    {
        char   *data;
        uint32  len;

        biscuit_page_read_blob(index, sp->blkno, &data, &len);
        old    = MemoryContextSwitchTo(cxt);
        result = pnstrdup(data ? data : "", len);
        MemoryContextSwitchTo(old);
        if (data)
            pfree(data);
        return result;
    }

    /*
     * Reuse the currently-held heap buffer when this slot lands on the
     * same page -- the common case, since the value heap is bump-allocated
     * in slot order. Only pay for a new ReadBuffer/LockBuffer when the run
     * actually crosses onto a different heap page.
     */
    if (*heap_buf == InvalidBuffer || *heap_blk != sp->blkno)
    {
        if (*heap_buf != InvalidBuffer)
            UnlockReleaseBuffer(*heap_buf);
        *heap_buf = ReadBuffer(index, sp->blkno);
        LockBuffer(*heap_buf, BUFFER_LOCK_SHARE);
        *heap_blk = sp->blkno;
    }

    {
        Page                  page = BufferGetPage(*heap_buf);
        BiscuitStrHeapHeader *hdr  = (BiscuitStrHeapHeader *) BiscuitPageDataPtr(page);
        const char           *base = (const char *) hdr
                                     + MAXALIGN(sizeof(BiscuitStrHeapHeader))
                                     + sp->offset;

        old    = MemoryContextSwitchTo(cxt);
        result = pnstrdup(base, sp->length);
        MemoryContextSwitchTo(old);
    }

    return result;
}

void
biscuit_rowstore_str_read_all(Relation index, BlockNumber ptr_pagedir_root,
                               uint32 num_records, MemoryContext cxt,
                               char **out_arr)
{
    uint32      ptrs_per_page = BiscuitStrPtrSlotsPerPage(BLCKSZ);
    uint32      written       = 0;
    BlockNumber cur           = ptr_pagedir_root;
    Buffer      heap_buf      = InvalidBuffer;
    BlockNumber heap_blk      = InvalidBlockNumber;

    if (ptr_pagedir_root == InvalidBlockNumber || num_records == 0)
        return;   /* nothing ever written -- caller's palloc0'd array stands */

    /* Walk the pointer-array page directory once. */
    while (cur != InvalidBlockNumber && written < num_records)
    {
        Buffer                dbuf = ReadBuffer(index, cur);
        Page                  dpage;
        BiscuitPageDirHeader *dhdr;
        BlockNumber          *dslots;
        BlockNumber          *dslots_copy;
        BiscuitPageOpaque     dopaque;
        BlockNumber           next;
        uint32                i, n;

        LockBuffer(dbuf, BUFFER_LOCK_SHARE);
        dpage  = BufferGetPage(dbuf);
        dhdr   = (BiscuitPageDirHeader *) BiscuitPageDataPtr(dpage);
        dslots = (BlockNumber *) ((char *) dhdr + MAXALIGN(sizeof(BiscuitPageDirHeader)));
        n      = dhdr->num_entries;

        /*
         * Copy this directory page's entries (and its chain-next pointer)
         * out and release dbuf BEFORE touching any STRPTR/STRHEAP page.
         *
         * The previous version held this PAGEDIR page's share lock across
         * the whole entries loop below, including the calls into
         * biscuit_strptr_materialize() (via the STRPTR-copy path further
         * down), on the theory that a directory page's lock is cheap to
         * hold. It is not: biscuit_strptr_materialize() acquires a
         * value-heap (STRHEAP) page's buffer lock, and the write path
         * takes the two locks in the opposite order. When a
         * BiscuitXlogBatch is open, biscuit_strheap_append() leaves its
         * STRHEAP page pinned+locked (ownership transferred into the
         * batch), and biscuit_rowstore_str_write() then calls
         * biscuit_strptr_write(), which resolves its STRPTR page via
         * biscuit_pagedir_ensure()/biscuit_pagedir_append() -- taking a
         * PAGEDIR page's buffer lock while that STRHEAP lock is still
         * held. So the write path takes STRHEAP-then-PAGEDIR while this
         * function, unpatched, took PAGEDIR-then-STRHEAP -- an exact ABBA
         * inversion on two ordinary buffer-content LWLocks, invisible to
         * PostgreSQL's (heavyweight-lock-only) deadlock detector, so a
         * reader and a writer converging on the same (PAGEDIR page,
         * STRHEAP page) pair simply hang forever instead of one being
         * aborted. This is the same class of bug already fixed once below
         * for STRPTR-vs-STRHEAP (see that comment) -- it just recurred one
         * level up, at the PAGEDIR page this loop was still holding.
         *
         * The fix is the same shape: never hold the PAGEDIR page's lock
         * at the same time as anything that can reach STRHEAP. Copy the
         * (small, fixed-size) BlockNumber array of STRPTR page pointers
         * out while dbuf is locked, drop dbuf immediately, and only then
         * walk those STRPTR pages (which themselves apply the very same
         * treatment before calling materialize()).
         */
        dslots_copy = (BlockNumber *) palloc(n * sizeof(BlockNumber));
        memcpy(dslots_copy, dslots, (Size) n * sizeof(BlockNumber));

        dopaque = (BiscuitPageOpaque) PageGetSpecialPointer(dpage);
        next    = dopaque->next;
        UnlockReleaseBuffer(dbuf);

        for (i = 0; i < n && written < num_records; i++)
        {
            Buffer         pbuf = ReadBuffer(index, dslots_copy[i]);
            Page           ppage;
            BiscuitStrPtr *pslots;
            BiscuitStrPtr *sp_copy;
            uint32         take = Min(num_records - written, ptrs_per_page);
            uint32         k;

            LockBuffer(pbuf, BUFFER_LOCK_SHARE);
            ppage  = BufferGetPage(pbuf);
            pslots = (BiscuitStrPtr *) BiscuitPageDataPtr(ppage);

            /*
             * Copy every pointer out and release this STRPTR page BEFORE
             * materializing any of them. Same ABBA hazard as the PAGEDIR
             * page above, one level down: biscuit_strptr_materialize()
             * acquires a STRHEAP page's buffer lock, and the write path
             * takes STRHEAP-then-STRPTR (biscuit_strheap_append() leaves
             * its STRHEAP page pinned+locked under an open batch, then
             * biscuit_strptr_write() locks the STRPTR page). So this
             * function must never hold the STRPTR page locked across a
             * materialize() call either.
             */
            sp_copy = (BiscuitStrPtr *) palloc(take * sizeof(BiscuitStrPtr));
            memcpy(sp_copy, pslots, (Size) take * sizeof(BiscuitStrPtr));
            UnlockReleaseBuffer(pbuf);

            for (k = 0; k < take; k++)
            {
                out_arr[written + k] =
                    biscuit_strptr_materialize(index, &sp_copy[k], cxt, &heap_buf, &heap_blk);
            }

            pfree(sp_copy);
            written += take;
        }

        pfree(dslots_copy);
        cur = next;
    }

    if (heap_buf != InvalidBuffer)
        UnlockReleaseBuffer(heap_buf);

    /*
     * Slots beyond what the directory covers stay NULL (the caller
     * palloc0's the array). That is a legitimate state -- a structure
     * whose logical pages were never allocated because nothing was ever
     * written there -- not a short read, so unlike
     * biscuit_rowstore_tid_read_all() there is no error to raise here.
     */
}

/*
 * biscuit_rowstore_str_read_slots
 *
 * Materialize a SPARSE, ascending set of slots, rather than the dense
 * [0, num_records) range biscuit_rowstore_str_read_all() reads.
 *
 * The delta builder needs this. A delta covering 500 rows may span slots
 * 0 through 900,000, because slots are claimed monotonically and the log
 * holds only what has been written since the last drain. Reading densely
 * to the highest slot present would materialize the entire string cache to
 * expand a handful of rows -- and materializing means a pnstrdup per slot,
 * so the cost is real memory, not just wasted reads.
 *
 * The walk is still ONE ordered pass over the pointer-array page
 * directory, which is why `slots` must be ascending. Logical pointer page
 * L covers slots [L * ptrs_per_page, (L+1) * ptrs_per_page), so an
 * ascending request lets the walk skip whole directory entries without
 * reading them and lets the value-heap buffer be reused across
 * consecutive slots (the heap is bump-allocated in slot order, so runs of
 * slots share a page). An unsorted request would degrade all of that into
 * random I/O and re-reads.
 *
 * out_arr[i] receives the string for slots[i], or stays NULL. A NULL is
 * not a short read: it is either an explicitly-NULL column value or a slot
 * whose logical page was never allocated, and those are indistinguishable
 * here and identical in effect -- exactly as in
 * biscuit_rowstore_str_read_all(), which likewise has no truncation
 * condition to detect.
 */
void
biscuit_rowstore_str_read_slots(Relation index, BlockNumber ptr_pagedir_root,
                                 const uint32 *slots, int nslots,
                                 MemoryContext cxt, char **out_arr)
{
    uint32      ptrs_per_page = BiscuitStrPtrSlotsPerPage(BLCKSZ);
    BlockNumber cur           = ptr_pagedir_root;
    Buffer      heap_buf      = InvalidBuffer;
    BlockNumber heap_blk      = InvalidBlockNumber;
    uint32      logical_page  = 0;   /* logical pointer page index */
    int         next_slot     = 0;   /* index into slots[] still unserved */

    if (ptr_pagedir_root == InvalidBlockNumber || nslots <= 0)
        return;

    while (cur != InvalidBlockNumber && next_slot < nslots)
    {
        Buffer                dbuf = ReadBuffer(index, cur);
        Page                  dpage;
        BiscuitPageDirHeader *dhdr;
        BlockNumber          *dslots;
        BlockNumber          *dslots_copy;
        BiscuitPageOpaque     dopaque;
        BlockNumber           next;
        uint32                i, n;

        LockBuffer(dbuf, BUFFER_LOCK_SHARE);
        dpage  = BufferGetPage(dbuf);
        dhdr   = (BiscuitPageDirHeader *) BiscuitPageDataPtr(dpage);
        dslots = (BlockNumber *) ((char *) dhdr + MAXALIGN(sizeof(BiscuitPageDirHeader)));
        n      = dhdr->num_entries;

        /*
         * Copy this directory page's entries (and its chain-next pointer)
         * out and release dbuf BEFORE touching any STRPTR/STRHEAP page.
         * Same ABBA hazard as biscuit_rowstore_str_read_all() -- see that
         * function's comment. The write path takes STRHEAP-then-PAGEDIR
         * (via biscuit_pagedir_ensure()/biscuit_pagedir_append() while an
         * open batch still holds a STRHEAP page locked); holding this
         * PAGEDIR page locked across the entries loop below, which can
         * reach STRHEAP via biscuit_strptr_materialize(), inverted that
         * order.
         */
        dslots_copy = (BlockNumber *) palloc(n * sizeof(BlockNumber));
        memcpy(dslots_copy, dslots, (Size) n * sizeof(BlockNumber));

        dopaque = (BiscuitPageOpaque) PageGetSpecialPointer(dpage);
        next    = dopaque->next;
        UnlockReleaseBuffer(dbuf);

        for (i = 0; i < n && next_slot < nslots; i++, logical_page++)
        {
            uint32 first = logical_page * ptrs_per_page;
            uint32 past  = first + ptrs_per_page;
            Buffer pbuf;
            Page   ppage;
            BiscuitStrPtr *pslots;
            BiscuitStrPtr *sp_copy;
            int    *idx_copy;
            int     ncopy = 0;
            int     j;

            /*
             * Skip this pointer page entirely if no requested slot falls
             * in it. This is the whole point of the sparse variant: no
             * ReadBuffer, no lock, no materialization.
             */
            if (slots[next_slot] >= past)
                continue;

            pbuf = ReadBuffer(index, dslots_copy[i]);
            LockBuffer(pbuf, BUFFER_LOCK_SHARE);
            ppage  = BufferGetPage(pbuf);
            pslots = (BiscuitStrPtr *) BiscuitPageDataPtr(ppage);

            /*
             * Copy every pointer this page will serve out, and release
             * pbuf, BEFORE materializing any of them.
             *
             * Same ABBA hazard as biscuit_rowstore_str_read_all(): holding
             * this STRPTR page's share lock while biscuit_strptr_materialize()
             * reaches for a STRHEAP page's lock inverts against the write
             * path's STRHEAP-then-STRPTR order under an open
             * BiscuitXlogBatch (biscuit_strheap_append() leaves its STRHEAP
             * page pinned+locked, then biscuit_strptr_write() locks STRPTR).
             * Both sides are ordinary buffer-content LWLocks, so the
             * inversion is invisible to the deadlock detector -- see
             * biscuit_strptr_write()'s comment. Bounded by ptrs_per_page,
             * so the copy is small and worst-case one BLCKSZ page's worth.
             */
            sp_copy  = (BiscuitStrPtr *) palloc(ptrs_per_page * sizeof(BiscuitStrPtr));
            idx_copy = (int *) palloc(ptrs_per_page * sizeof(int));

            while (next_slot < nslots && slots[next_slot] < past)
            {
                Assert(ncopy < (int) ptrs_per_page);   /* slots[] is ascending
                                                          * and drawn from a
                                                          * bitmap, so this
                                                          * page can serve at
                                                          * most ptrs_per_page
                                                          * of them */
                sp_copy[ncopy]  = pslots[slots[next_slot] - first];
                idx_copy[ncopy] = next_slot;
                ncopy++;
                next_slot++;
            }

            UnlockReleaseBuffer(pbuf);

            for (j = 0; j < ncopy; j++)
                out_arr[idx_copy[j]] =
                    biscuit_strptr_materialize(index, &sp_copy[j], cxt, &heap_buf, &heap_blk);

            pfree(sp_copy);
            pfree(idx_copy);
        }

        pfree(dslots_copy);
        cur = next;
    }

    if (heap_buf != InvalidBuffer)
        UnlockReleaseBuffer(heap_buf);
}

/* ================================================================
 * HEADER
 * ================================================================ */

void
biscuit_rowstore_header_write(Relation index, BlockNumber *head_inout,
                               const char *data, uint32 len)
{
    Size   specialSize = MAXALIGN(sizeof(BiscuitPageOpaqueData));
    uint32 payload_max = BiscuitHeaderMaxPayload(BLCKSZ);

    if (len > payload_max)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("biscuit: HEADER blob too large (%u bytes, max %u)",
                        len, payload_max),
                 errhint("This index likely has an unusually large number of indexed columns.")));

    biscuit_ensure_synchronous_commit();

    if (*head_inout == InvalidBlockNumber)
    {
        Buffer             buf = biscuit_page_alloc(index, BISCUIT_PAGE_HEADER);
        GenericXLogState  *state;
        Page               page;
        BiscuitPageOpaque  opaque;
        uint32            *lenp;

        state = GenericXLogStart(index);
        page  = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

        PageInit(page, BufferGetPageSize(buf), specialSize);

        lenp  = (uint32 *) BiscuitPageDataPtr(page);
        *lenp = len;
        if (len > 0)
            memcpy((char *) lenp + MAXALIGN(sizeof(uint32)), data, len);

        BiscuitPageSetLower(page, MAXALIGN(sizeof(uint32)) + len);

        opaque              = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        opaque->next        = InvalidBlockNumber;
        opaque->page_kind   = BISCUIT_PAGE_HEADER;
        opaque->flags       = 0;
        opaque->recycle_xid = InvalidTransactionId;

        GenericXLogFinish(state);
        *head_inout = BufferGetBlockNumber(buf);
        UnlockReleaseBuffer(buf);
        return;
    }

    {
        Buffer            buf = ReadBuffer(index, *head_inout);
        GenericXLogState *state;
        Page              page;
        uint32           *lenp;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        state = GenericXLogStart(index);
        page  = GenericXLogRegisterBuffer(state, buf, 0);

        lenp  = (uint32 *) BiscuitPageDataPtr(page);
        *lenp = len;
        if (len > 0)
            memcpy((char *) lenp + MAXALIGN(sizeof(uint32)), data, len);

        BiscuitPageSetLower(page, MAXALIGN(sizeof(uint32)) + len);

        GenericXLogFinish(state);
        UnlockReleaseBuffer(buf);
    }
}

void
biscuit_rowstore_header_read(Relation index, BlockNumber head,
                              char **out_data, uint32 *out_len)
{
    Buffer   buf;
    Page     page;
    uint32  *lenp;
    uint32   len;
    char    *result;

    if (head == InvalidBlockNumber)
    {
        *out_data = NULL;
        *out_len  = 0;
        return;
    }

    buf = ReadBuffer(index, head);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    lenp = (uint32 *) BiscuitPageDataPtr(page);
    len  = *lenp;

    result = (len > 0) ? (char *) palloc(len) : NULL;
    if (len > 0)
        memcpy(result, (char *) lenp + MAXALIGN(sizeof(uint32)), len);

    UnlockReleaseBuffer(buf);

    *out_data = result;
    *out_len  = len;
}

void
biscuit_rowstore_free_header(Relation index, BlockNumber head)
{
    if (head != InvalidBlockNumber)
        biscuit_page_free_chain(index, head);
}
