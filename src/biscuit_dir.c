/*
 * biscuit_dir.c
 * See biscuit_dir.h for the full contract of every function here.
 *
 * Page layout matches biscuit_blob.c's BISCUIT_PAGE_BLOB/BISCUIT_PAGE_PENDING
 * pages exactly (standard page header, a small fixed struct written
 * directly at the start of the data area, its payload -- here a packed
 * BiscuitDirEntry[] array -- immediately following, and a
 * BiscuitPageOpaqueData footer) -- see biscuit_blob.c's file header
 * comment for why (BiscuitDirPageMaxEntries() in biscuit_common.h sizes
 * against exactly this layout, same as the blob/pending macros).
 */

#include "biscuit_common.h"
#include "biscuit_blob.h"
#include "biscuit_dir.h"
#include "storage/bufpage.h"

/* ==================== PAGE LAYOUT HELPERS ====================
 * Duplicated from biscuit_blob.c (file-local there) rather than shared,
 * since both are one-line macros/functions and this file has no other
 * reason to depend on biscuit_blob.c's internals.
 */

#define BiscuitPageDataPtr(page)  ((char *) (page) + SizeOfPageHeaderData)

static inline void
BiscuitPageSetLower(Page page, Size used)
{
    ((PageHeader) page)->pd_lower = (LocationIndex) (SizeOfPageHeaderData + used);
}

static inline Size
BiscuitDirUsedBytes(uint32 num_entries)
{
    return MAXALIGN(sizeof(BiscuitDirPageHeader))
         + (Size) num_entries * sizeof(BiscuitDirEntry);
}

/* ==================== COLUMN SLOT MAPPING ==================== */

int
biscuit_dir_slot_for_col(int32 col)
{
    return (col < 0) ? 0 : (int) col;
}

/* ==================== ROOT BOOTSTRAP ==================== */

/*
 * Return dir_roots[slot], allocating and linking a fresh empty
 * BISCUIT_PAGE_DIR page for it first if this is the first entry ever
 * inserted for this slot. The whole check-then-allocate sequence holds a
 * single continuous BUFFER_LOCK_EXCLUSIVE on the metapage throughout (no
 * unlock/relock window), so there is no double-allocation race between
 * concurrent first-inserts into the same slot: the second caller simply
 * blocks on the metapage lock and, on acquiring it, sees the first
 * caller's already-populated dir_roots[slot] and returns that instead of
 * allocating a second root.
 */
static BlockNumber
biscuit_dir_ensure_root(Relation index, int slot)
{
    Buffer               mbuf;
    Page                 mpage;
    BiscuitMetaPageData *meta;
    BlockNumber          existing;

    mbuf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
    mpage = BufferGetPage(mbuf);
    meta  = (BiscuitMetaPageData *) PageGetSpecialPointer(mpage);
    existing = meta->dir_roots[slot];

    if (existing != InvalidBlockNumber)
    {
        UnlockReleaseBuffer(mbuf);
        return existing;
    }

    {
        Buffer                newbuf;
        Page                  newpage;
        GenericXLogState     *state;
        BiscuitDirPageHeader *hdr;
        BiscuitPageOpaque     opaque;
        BlockNumber           new_blkno;

        biscuit_ensure_synchronous_commit();

        newbuf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);

        state   = GenericXLogStart(index);
        mpage   = GenericXLogRegisterBuffer(state, mbuf, 0);
        newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);

        PageInit(newpage, BufferGetPageSize(newbuf), MAXALIGN(sizeof(BiscuitPageOpaqueData)));
        hdr = (BiscuitDirPageHeader *) BiscuitPageDataPtr(newpage);
        hdr->num_entries = 0;
        hdr->max_entries = BiscuitDirPageMaxEntries(BufferGetPageSize(newbuf));
        BiscuitPageSetLower(newpage, BiscuitDirUsedBytes(0));

        opaque              = (BiscuitPageOpaque) PageGetSpecialPointer(newpage);
        opaque->next        = InvalidBlockNumber;
        opaque->page_kind   = BISCUIT_PAGE_DIR;
        opaque->flags       = 0;
        opaque->recycle_xid = InvalidTransactionId;

        new_blkno = BufferGetBlockNumber(newbuf);

        meta = (BiscuitMetaPageData *) PageGetSpecialPointer(mpage);
        meta->dir_roots[slot] = new_blkno;
        if (slot + 1 > meta->num_dir_columns)
            meta->num_dir_columns = slot + 1;

        GenericXLogFinish(state);

        UnlockReleaseBuffer(newbuf);
        UnlockReleaseBuffer(mbuf);

        return new_blkno;
    }
}

static BlockNumber
biscuit_dir_read_root(Relation index, int slot)
{
    Buffer               buf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    Page                 page;
    BiscuitMetaPageData *meta;
    BlockNumber          root;

    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    meta = (BiscuitMetaPageData *) PageGetSpecialPointer(page);
    root = meta->dir_roots[slot];
    UnlockReleaseBuffer(buf);

    return root;
}

/* ==================== FIND ==================== */

bool
biscuit_dir_find(Relation index,
                  int32 col, bool is_lower, uint8 kind,
                  int32 ch, int32 position,
                  BiscuitDirEntry *out_entry,
                  BiscuitDirEntryRef *out_ref)
{
    int         slot = biscuit_dir_slot_for_col(col);
    BlockNumber root = biscuit_dir_read_root(index, slot);
    BlockNumber cur  = root;

    if (root == InvalidBlockNumber)
        return false;

    while (cur != InvalidBlockNumber)
    {
        Buffer                 buf = ReadBuffer(index, cur);
        Page                   page;
        BiscuitDirPageHeader  *hdr;
        BiscuitDirEntry       *entries;
        BiscuitPageOpaque      opaque;
        BlockNumber            next;
        uint32                 i;

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page    = BufferGetPage(buf);
        hdr     = (BiscuitDirPageHeader *) BiscuitPageDataPtr(page);
        entries = (BiscuitDirEntry *) ((char *) hdr + MAXALIGN(sizeof(BiscuitDirPageHeader)));

        for (i = 0; i < hdr->num_entries; i++)
        {
            if (entries[i].col == col && entries[i].is_lower == is_lower &&
                entries[i].kind == kind && entries[i].ch == ch &&
                entries[i].position == position)
            {
                *out_entry = entries[i];
                if (out_ref)
                {
                    out_ref->blkno = cur;
                    out_ref->index = (int) i;
                }
                UnlockReleaseBuffer(buf);
                return true;
            }
        }

        opaque = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        next   = opaque->next;
        UnlockReleaseBuffer(buf);
        cur = next;
    }

    return false;
}

/* ==================== UPDATE ==================== */

void
biscuit_dir_update(Relation index,
                    const BiscuitDirEntryRef *ref,
                    const BiscuitDirEntry *new_entry)
{
    Buffer                 buf = ReadBuffer(index, ref->blkno);
    Page                   page;
    GenericXLogState      *state;
    BiscuitDirPageHeader  *hdr;
    BiscuitDirEntry       *entries;

    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    biscuit_ensure_synchronous_commit();
    state   = GenericXLogStart(index);
    page    = GenericXLogRegisterBuffer(state, buf, 0);
    hdr     = (BiscuitDirPageHeader *) BiscuitPageDataPtr(page);
    entries = (BiscuitDirEntry *) ((char *) hdr + MAXALIGN(sizeof(BiscuitDirPageHeader)));

    Assert((uint32) ref->index < hdr->num_entries);
    entries[ref->index] = *new_entry;

    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

/* ==================== INSERT ==================== */

/*
 * Walk from root, acquiring an exclusive lock on each page in turn (never
 * more than one at a time), until finding one whose opaque.next is
 * InvalidBlockNumber under that same lock -- i.e. the chain's current
 * tail, confirmed durably rather than assumed from a stale earlier read.
 * Returns that buffer still exclusively locked; caller is responsible for
 * unlocking/releasing it.
 */
static Buffer
biscuit_dir_lock_tail(Relation index, BlockNumber root)
{
    BlockNumber cur = root;

    for (;;)
    {
        Buffer            buf = ReadBuffer(index, cur);
        Page              page;
        BiscuitPageOpaque opaque;
        BlockNumber       next;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page   = BufferGetPage(buf);
        opaque = (BiscuitPageOpaque) PageGetSpecialPointer(page);

        if (opaque->next == InvalidBlockNumber)
            return buf;   /* still holding the exclusive lock */

        next = opaque->next;
        UnlockReleaseBuffer(buf);
        cur = next;
    }
}

void
biscuit_dir_insert(Relation index, const BiscuitDirEntry *entry, BiscuitDirEntryRef *out_ref)
{
    int         slot = biscuit_dir_slot_for_col(entry->col);
    BlockNumber root = biscuit_dir_ensure_root(index, slot);
    Buffer      buf  = biscuit_dir_lock_tail(index, root);
    Page        page = BufferGetPage(buf);
    BiscuitDirPageHeader *hdr = (BiscuitDirPageHeader *) BiscuitPageDataPtr(page);

    biscuit_ensure_synchronous_commit();

    if (hdr->num_entries < hdr->max_entries)
    {
        /* Room on the current tail page: append in place. */
        GenericXLogState *state = GenericXLogStart(index);
        BiscuitDirEntry   *entries;

        page    = GenericXLogRegisterBuffer(state, buf, 0);
        hdr     = (BiscuitDirPageHeader *) BiscuitPageDataPtr(page);
        entries = (BiscuitDirEntry *) ((char *) hdr + MAXALIGN(sizeof(BiscuitDirPageHeader)));

        entries[hdr->num_entries] = *entry;
        if (out_ref)
        {
            out_ref->blkno = BufferGetBlockNumber(buf);
            out_ref->index = (int) hdr->num_entries;
        }
        hdr->num_entries++;

        BiscuitPageSetLower(page, BiscuitDirUsedBytes(hdr->num_entries));

        GenericXLogFinish(state);
        UnlockReleaseBuffer(buf);
        return;
    }

    /*
     * Tail page full: allocate a new tail page, link it via the old
     * tail's opaque.next, and write the new entry there -- both pages in
     * one GenericXLog transaction, same nested-lock pattern as
     * biscuit_pending_append()'s overflow branch.
     */
    {
        Buffer                 newbuf;
        Page                   newpage;
        GenericXLogState      *state;
        BiscuitDirPageHeader  *newhdr;
        BiscuitDirEntry       *entries;
        BiscuitPageOpaque      newopaque;
        BiscuitPageOpaque      oldopaque;

        newbuf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);

        state   = GenericXLogStart(index);
        page    = GenericXLogRegisterBuffer(state, buf, 0);
        newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);

        PageInit(newpage, BufferGetPageSize(newbuf), MAXALIGN(sizeof(BiscuitPageOpaqueData)));
        newhdr = (BiscuitDirPageHeader *) BiscuitPageDataPtr(newpage);
        newhdr->num_entries = 0;
        newhdr->max_entries = BiscuitDirPageMaxEntries(BufferGetPageSize(newbuf));

        entries = (BiscuitDirEntry *) ((char *) newhdr + MAXALIGN(sizeof(BiscuitDirPageHeader)));
        entries[0] = *entry;
        newhdr->num_entries = 1;

        BiscuitPageSetLower(newpage, BiscuitDirUsedBytes(1));

        newopaque              = (BiscuitPageOpaque) PageGetSpecialPointer(newpage);
        newopaque->next        = InvalidBlockNumber;
        newopaque->page_kind   = BISCUIT_PAGE_DIR;
        newopaque->flags       = 0;
        newopaque->recycle_xid = InvalidTransactionId;

        oldopaque       = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        oldopaque->next = BufferGetBlockNumber(newbuf);

        if (out_ref)
        {
            out_ref->blkno = BufferGetBlockNumber(newbuf);
            out_ref->index = 0;
        }

        GenericXLogFinish(state);

        UnlockReleaseBuffer(newbuf);
        UnlockReleaseBuffer(buf);
    }
}

/* ==================== UPSERT ==================== */

void
biscuit_dir_upsert(Relation index, const BiscuitDirEntry *entry)
{
    BiscuitDirEntry    existing;
    BiscuitDirEntryRef ref;

    if (biscuit_dir_find(index, entry->col, entry->is_lower, entry->kind,
                          entry->ch, entry->position, &existing, &ref))
        biscuit_dir_update(index, &ref, entry);
    else
        biscuit_dir_insert(index, entry, NULL);
}

/* ==================== WALK ==================== */

void
biscuit_dir_foreach_column(Relation index, int slot, BiscuitDirWalkCallback cb, void *state)
{
    BlockNumber root = biscuit_dir_read_root(index, slot);
    BlockNumber cur  = root;

    while (cur != InvalidBlockNumber)
    {
        Buffer                buf = ReadBuffer(index, cur);
        Page                  page;
        BiscuitDirPageHeader *hdr;
        BiscuitDirEntry      *entries;
        BiscuitPageOpaque     opaque;
        BlockNumber           next;
        uint32                i;

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page    = BufferGetPage(buf);
        hdr     = (BiscuitDirPageHeader *) BiscuitPageDataPtr(page);
        entries = (BiscuitDirEntry *) ((char *) hdr + MAXALIGN(sizeof(BiscuitDirPageHeader)));

        for (i = 0; i < hdr->num_entries; i++)
            cb(&entries[i], state);

        opaque = (BiscuitPageOpaque) PageGetSpecialPointer(page);
        next   = opaque->next;
        UnlockReleaseBuffer(buf);
        cur = next;
    }
}

int
biscuit_dir_num_slots(Relation index)
{
    Buffer               buf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
    Page                 page;
    BiscuitMetaPageData *meta;
    int                  n;

    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    meta = (BiscuitMetaPageData *) PageGetSpecialPointer(page);
    n    = meta->num_dir_columns;
    UnlockReleaseBuffer(buf);

    return n;
}

/* ==================== DROP ==================== */

void
biscuit_dir_drop_all(Relation index)
{
    BlockNumber roots[BISCUIT_MAX_DIR_COLUMNS];
    int32       num_slots;
    int         i;

    {
        Buffer               buf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
        Page                 page;
        BiscuitMetaPageData *meta;

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page      = BufferGetPage(buf);
        meta      = (BiscuitMetaPageData *) PageGetSpecialPointer(page);
        num_slots = meta->num_dir_columns;
        memcpy(roots, meta->dir_roots, sizeof(roots));
        UnlockReleaseBuffer(buf);
    }

    for (i = 0; i < num_slots; i++)
        if (roots[i] != InvalidBlockNumber)
            biscuit_page_free_chain(index, roots[i]);

    {
        Buffer               buf = ReadBuffer(index, BISCUIT_METAPAGE_BLKNO);
        Page                 page;
        GenericXLogState    *state;
        BiscuitMetaPageData *meta;
        int                  j;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        biscuit_ensure_synchronous_commit();
        state = GenericXLogStart(index);
        page  = GenericXLogRegisterBuffer(state, buf, 0);
        meta  = (BiscuitMetaPageData *) PageGetSpecialPointer(page);

        for (j = 0; j < BISCUIT_MAX_DIR_COLUMNS; j++)
            meta->dir_roots[j] = InvalidBlockNumber;
        meta->num_dir_columns = 0;

        GenericXLogFinish(state);
        UnlockReleaseBuffer(buf);
    }
}
