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
 * The invariant holds today because a new logical page is only ever needed
 * by the fresh-append insert path, where slot_idx == num_records++ is
 * strictly increasing; the freelist-reuse path can only produce a
 * slot_idx below num_records, whose logical page necessarily already
 * exists. That is a real property of the CRUD call sites rather than
 * something this layer can arrange for itself, which is exactly why it is
 * checked here instead of assumed -- it is the "new slot allocation as an
 * explicit third case" the implementation plan calls out, and the same
 * shape of implicit-assumption bug as the original NULL-guard crash.
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
 * biscuit_pagedir_free_all
 * Retire the PAGEDIR chain itself AND every page it points at (each
 * entry is a TIDSLOT or STRPTR page, depending on caller). Used only by
 * teardown (biscuit_rowstore_free_tid_chain/_free_str_chains).
 */
static void
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
                            uint32 slot_idx, const ItemPointerData *tid)
{
    uint32      slots_per_page = BiscuitTidSlotsPerPage(BLCKSZ);
    uint32      logical        = slot_idx / slots_per_page;
    uint32      offset         = slot_idx % slots_per_page;
    BlockNumber blkno;

    biscuit_ensure_synchronous_commit();

    blkno = biscuit_pagedir_lookup(index, *pagedir_root, logical);

    if (blkno == InvalidBlockNumber)
    {
        /* New logical page: allocate, write the one slot, link into the
         * directory. This is the "new slot allocation" case the
         * implementation summary calls out as its own named case -- the
         * only place this primitive allocates rather than purely
         * overwrites. */
        Buffer             buf = biscuit_page_alloc(index, BISCUIT_PAGE_TIDSLOT);
        GenericXLogState  *state;
        Page               page;
        Size               specialSize = MAXALIGN(sizeof(BiscuitPageOpaqueData));
        BiscuitPageOpaque  opaque;
        ItemPointerData   *slots;

        state = GenericXLogStart(index);
        page  = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

        PageInit(page, BufferGetPageSize(buf), specialSize);

        opaque              = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        opaque->next        = InvalidBlockNumber;
        opaque->page_kind   = BISCUIT_PAGE_TIDSLOT;
        opaque->flags       = 0;
        opaque->recycle_xid = InvalidTransactionId;

        slots         = (ItemPointerData *) BiscuitPageDataPtr(page);
        slots[offset] = *tid;

        BiscuitPageSetLower(page, (Size) (offset + 1) * sizeof(ItemPointerData));

        GenericXLogFinish(state);
        blkno = BufferGetBlockNumber(buf);
        UnlockReleaseBuffer(buf);

        biscuit_pagedir_append(index, pagedir_root, logical, blkno);
        return;
    }

    /* Existing logical page: single in-place write, fresh insert or
     * freelist-slot reuse both land here identically. */
    {
        Buffer            buf = ReadBuffer(index, blkno);
        GenericXLogState *state;
        Page              page;
        ItemPointerData  *slots;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        state = GenericXLogStart(index);
        page  = GenericXLogRegisterBuffer(state, buf, 0);

        slots         = (ItemPointerData *) BiscuitPageDataPtr(page);
        slots[offset] = *tid;

        BiscuitPageBumpLower(page, (Size) (offset + 1) * sizeof(ItemPointerData));

        GenericXLogFinish(state);
        UnlockReleaseBuffer(buf);
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

static void
biscuit_strptr_write(Relation index, BlockNumber *pagedir_root,
                      uint32 slot_idx, const BiscuitStrPtr *sp)
{
    uint32      ptrs_per_page = BiscuitStrPtrSlotsPerPage(BLCKSZ);
    uint32      logical       = slot_idx / ptrs_per_page;
    uint32      offset        = slot_idx % ptrs_per_page;
    BlockNumber blkno         = biscuit_pagedir_lookup(index, *pagedir_root, logical);

    if (blkno == InvalidBlockNumber)
    {
        Buffer             buf = biscuit_page_alloc(index, BISCUIT_PAGE_STRPTR);
        GenericXLogState  *state;
        Page               page;
        Size               specialSize = MAXALIGN(sizeof(BiscuitPageOpaqueData));
        BiscuitPageOpaque  opaque;
        BiscuitStrPtr     *slots;

        state = GenericXLogStart(index);
        page  = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

        PageInit(page, BufferGetPageSize(buf), specialSize);

        opaque              = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        opaque->next        = InvalidBlockNumber;
        opaque->page_kind   = BISCUIT_PAGE_STRPTR;
        opaque->flags       = 0;
        opaque->recycle_xid = InvalidTransactionId;

        slots         = (BiscuitStrPtr *) BiscuitPageDataPtr(page);
        slots[offset] = *sp;

        BiscuitPageSetLower(page, (Size) (offset + 1) * sizeof(BiscuitStrPtr));

        GenericXLogFinish(state);
        blkno = BufferGetBlockNumber(buf);
        UnlockReleaseBuffer(buf);

        biscuit_pagedir_append(index, pagedir_root, logical, blkno);
        return;
    }

    {
        Buffer            buf = ReadBuffer(index, blkno);
        GenericXLogState *state;
        Page              page;
        BiscuitStrPtr    *slots;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        state = GenericXLogStart(index);
        page  = GenericXLogRegisterBuffer(state, buf, 0);

        slots         = (BiscuitStrPtr *) BiscuitPageDataPtr(page);
        slots[offset] = *sp;

        BiscuitPageBumpLower(page, (Size) (offset + 1) * sizeof(BiscuitStrPtr));

        GenericXLogFinish(state);
        UnlockReleaseBuffer(buf);
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
                        BlockNumber *out_blkno, uint32 *out_offset)
{
    Size   specialSize = MAXALIGN(sizeof(BiscuitPageOpaqueData));
    uint32 payload_max = BiscuitStrHeapMaxPayload(BLCKSZ);

    Assert(len <= payload_max);

    if (*head == InvalidBlockNumber)
    {
        Buffer                buf = biscuit_page_alloc(index, BISCUIT_PAGE_STRHEAP);
        GenericXLogState     *state;
        Page                  page;
        BiscuitStrHeapHeader *hdr;
        BiscuitPageOpaque     opaque;

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

        /* Tail full: allocate + link a new tail page, same nested-lock
         * pattern as biscuit_pendlog_append()'s page-rollover branch. */
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
                            uint32 slot_idx, const char *str, int32 len)
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
            /* Oversized value: dedicated chunked blob chain (existing
             * biscuit_page_write_blob() primitive already handles
             * arbitrary length). */
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
                                    &sp.blkno, &sp.offset);
            sp.length = (uint32) len;
        }
    }

    biscuit_strptr_write(index, ptr_pagedir_root, slot_idx, &sp);
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

        biscuit_page_read_blob(index, sp.blkno, &data, &len);
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
        BiscuitPageOpaque     dopaque;
        BlockNumber           next;
        uint32                i, n;

        LockBuffer(dbuf, BUFFER_LOCK_SHARE);
        dpage  = BufferGetPage(dbuf);
        dhdr   = (BiscuitPageDirHeader *) BiscuitPageDataPtr(dpage);
        dslots = (BlockNumber *) ((char *) dhdr + MAXALIGN(sizeof(BiscuitPageDirHeader)));
        n      = dhdr->num_entries;

        for (i = 0; i < n && written < num_records; i++)
        {
            Buffer         pbuf = ReadBuffer(index, dslots[i]);
            Page           ppage;
            BiscuitStrPtr *pslots;
            uint32         take = Min(num_records - written, ptrs_per_page);
            uint32         k;

            LockBuffer(pbuf, BUFFER_LOCK_SHARE);
            ppage  = BufferGetPage(pbuf);
            pslots = (BiscuitStrPtr *) BiscuitPageDataPtr(ppage);

            for (k = 0; k < take; k++)
            {
                /*
                 * Copy the pointer out before materializing: materializing
                 * may acquire a different value-heap buffer, and holding
                 * this STRPTR page's share lock across that is fine, but
                 * relying on `pslots` staying valid afterwards is not worth
                 * the subtlety -- the struct is 12 bytes.
                 */
                BiscuitStrPtr sp = pslots[k];

                out_arr[written + k] =
                    biscuit_strptr_materialize(index, &sp, cxt, &heap_buf, &heap_blk);
            }

            UnlockReleaseBuffer(pbuf);
            written += take;
        }

        dopaque = (BiscuitPageOpaque) PageGetSpecialPointer(dpage);
        next    = dopaque->next;
        UnlockReleaseBuffer(dbuf);
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
