/*
 * biscuit_pattern.c
 * LIKE / ILIKE pattern parsing, bitmap-level position matching,
 * recursive windowed matching, and the multi-column query optimizer.
 *
 * The public interface is declared in biscuit_pattern.h.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_utf8.h"
#include "biscuit_pattern.h"
#include "biscuit_pendlog.h"
#include "biscuit_dir.h"    /* biscuit_dir_find, BiscuitDirEntry */

/*
 * Defined in biscuit_scan.c, registered as biscuit.diag_scan_trace in
 * _PG_init(). Declared here rather than in a header because the headers are
 * not in this working set; fold it into biscuit_scan.h when convenient.
 */
extern bool biscuit_diag_scan_trace;

/*
 * biscuit_reconcile_scratch_cxt -- storage for the extern declared in
 * biscuit_pattern.h; see that declaration for the full contract.
 */
MemoryContext biscuit_reconcile_scratch_cxt = NULL;

/* ================================================================
 * SECTION 0 – Read-time shared-log reconciliation
 * ================================================================
 *
 * Chosen strategy (Phase 1 Contract §3): in-memory merge at scan time,
 * not a forced pre-scan drain. Every bitmap-fetch helper below
 * (biscuit_get_pos_bitmap et al.) routes its result through
 * biscuit_reconcile_pending() before returning, so callers throughout
 * this file (biscuit_match_part_at_pos, biscuit_recursive_windowed_match,
 * their multi-column/ILIKE counterparts, and biscuit_query_pattern*
 * biscuit_query_column_pattern*) see an already-reconciled bitmap and
 * need no reconciliation logic of their own -- they only had to start
 * passing the scan's Relation down to the fetch helpers.
 *
 * OWNERSHIP CONTRACT: the returned pointer is either (a) the same live,
 * borrowed `cached` pointer unchanged (no undrained pending records --
 * the common, zero-copy case), or (b) a fresh RoaringBitmap whose
 * cleanup has already been registered against CurrentMemoryContext (see
 * biscuit_reconcile_register_cleanup() just below) -- it will be freed
 * automatically the moment that context is reset or deleted. Either way,
 * THE CALLER MUST NOT CALL biscuit_roaring_free() ON THIS RETURN VALUE.
 * Every caller in this file either uses the result immediately (AND/OR
 * it into an accumulator the caller *does* own) or makes its own
 * biscuit_roaring_copy() before doing anything else with it -- callers
 * were already written this way (the right instinct, even before there
 * was a mechanism backing it), so this contract asks nothing new of
 * them.
 *
 * WHY A MEMORY-CONTEXT CALLBACK RATHER THAN, SAY, THE SNAPSHOT ITSELF:
 * an earlier version of this fix registered the fresh copy with the
 * pendlog snapshot it was reconciled against, freeing it alongside
 * kill/live/seen/expanded when that snapshot was next rebuilt or
 * evicted. That is bounded in principle but not in practice: a
 * snapshot's common-case read path is to incrementally *extend* in
 * place (biscuit_pendlog_snapshot(), biscuit_pendlog.c) rather than
 * rebuild, and a workload whose query mix rarely forces a full rebuild
 * (no drain, no relid contention for one of the 8 cache slots) could
 * leave that snapshot -- and every fresh copy ever attached to it --
 * alive for the rest of the session. That is a real, observed leak
 * (BISCUIT-3.0.0-GA-Report.md's v63->v65 series: the slope dropped but
 * never plateaued), just a shallower one than the original.
 *
 * A context reset callback fixes this by tying the fresh copy's
 * lifetime to something that resets on a bounded, predictable cadence
 * regardless of snapshot behavior -- CurrentMemoryContext here is
 * whatever short-lived context the executor has set up for the AM
 * callback currently running (scan-lifetime at the outside, often
 * per-tuple), which is reset/deleted on its own schedule independent of
 * pendlog snapshot churn.
 *
 * biscuit_get_length_ge() and its three siblings are the one place that
 * breaks the "caller never frees" half of this pattern on purpose:
 * their own long-standing contract with THEIR callers is "always return
 * an owned, freely mutable/freeable copy". They honor that by always
 * making one more biscuit_roaring_copy() before returning -- a plain,
 * unregistered copy nobody but their own immediate caller ever sees --
 * regardless of which case (a)/(b) they got back from this function.
 * See their own comments.
 *
 * (This function used to say a fresh copy was "context-scoped, reclaimed
 * by ordinary memory-context cleanup" -- true of the *scaffolding*
 * pointing at it, never of the RoaringBitmap itself: under HAVE_ROARING a
 * RoaringBitmap is CRoaring-allocated, not palloc'd, so no ordinary
 * MemoryContextDelete/Reset ever reclaims it on its own -- it takes an
 * explicit callback, which is exactly what
 * biscuit_reconcile_register_cleanup() sets up.)
 */

/*
 * biscuit_reconcile_register_cleanup
 *
 * Free `bm` (via biscuit_roaring_free()) the moment CurrentMemoryContext
 * is reset or deleted, using PostgreSQL's standard
 * MemoryContextRegisterResetCallback() mechanism. The MemoryContextCallback
 * node itself is palloc'd IN that same context, so it shares its fate
 * exactly -- no separate cleanup of the node itself is needed.
 */
static void
biscuit_reconcile_scratch_free_cb(void *arg)
{
    biscuit_roaring_free((RoaringBitmap *) arg);
}

static void
biscuit_reconcile_register_cleanup(RoaringBitmap *bm)
{
    MemoryContext           target;
    MemoryContextCallback  *cb;

    if (bm == NULL)
        return;

    /*
     * Prefer the current scan's own scratch context (explicit,
     * scan-owned lifetime -- reset every rescan, deleted at endscan; see
     * biscuit_reconcile_scratch_cxt's declaration in biscuit_pattern.h)
     * over the ambient CurrentMemoryContext, whose lifetime this file
     * has no way to verify and which an earlier version of this fix
     * learned not to trust (BISCUIT-3.0.0-GA-Report.md's v65->v66 §10
     * finding: flat under read-only load, still climbing under
     * write-concurrent load -- consistent with CurrentMemoryContext
     * living far longer than one statement on at least one access
     * pattern this codebase exercises).
     */
    target = biscuit_reconcile_scratch_cxt ? biscuit_reconcile_scratch_cxt : CurrentMemoryContext;

    cb = (MemoryContextCallback *) MemoryContextAlloc(target, sizeof(MemoryContextCallback));
    cb->func = biscuit_reconcile_scratch_free_cb;
    cb->arg  = bm;
    MemoryContextRegisterResetCallback(target, cb);
}

static RoaringBitmap *
biscuit_reconcile_pending(Relation index, RoaringBitmap *cached,
                           int32 col, bool is_lower, uint8 kind,
                           int32 ch, int32 position)
{
    BiscuitPendLogSnapshot *snap;
    BiscuitPendLogEntry    *pend;
    RoaringBitmap          *merged;

    if (index == NULL)
        return cached;   /* no backing Relation available -- tolerate
                           * defensively rather than crash; every real
                           * query-path caller has one (biscuit_scan.c
                           * always has scan->indexRelation) */

    /*
     * Since the per-structure pending chains were replaced by one shared
     * log, a structure's undrained deltas are scattered through that log
     * rather than sitting in a chain of its own. Scanning the log per
     * structure would be O(structures x log size) for a query that touches
     * hundreds of structures, so instead the log is materialized once into
     * a hash keyed by structure identity and cached until the log moves
     * (biscuit_pendlog_snapshot()). Per-structure reconciliation is then a
     * hash probe.
     *
     * Both early exits below are the common cases and both are cheap: a
     * fully-drained index returns NULL from the snapshot without
     * allocating anything, and a structure with nothing pending misses the
     * hash. Only a structure that actually has undrained deltas pays for a
     * bitmap copy.
     */
    snap = biscuit_pendlog_snapshot(index);
    if (snap == NULL)
    {
        if (unlikely(biscuit_diag_scan_trace))
            ereport(WARNING,
                    (errmsg("biscuit: diag reconcile col=%d lower=%d kind=%u ch=%d pos=%d "
                            "-> no snapshot (log empty)",
                            col, is_lower ? 1 : 0, kind, ch, position)));
        return cached;   /* log empty -- fully drained, the steady state */
    }

    pend = biscuit_pendlog_lookup(snap, col, is_lower, kind, ch, position);

    /*
     * Per-structure reconcile accounting.
     *
     * This is the measurement that distinguishes "the delta produced these
     * identities" from "the query found them". The v39 guards proved the
     * first: every live slot reached expansion and produced identities. They
     * say nothing about the second, because expansion writes into the hash
     * under whatever key biscuit_delta.c derived, and the query reads out
     * under whatever key this call site passes. A mismatch between those two
     * keys is a total, silent miss for every post-build row while base-
     * resident build rows stay correct -- which is the profile this defect
     * has had since v31.
     *
     * pendlog_key_init() does NOT normalize col: BISCUIT_DIR_COL_LEGACY (-1)
     * and 0 hash to different keys and memcmp unequal. biscuit_get_pos_bitmap()
     * and its siblings pass -1 for the single-column layout;
     * biscuit_delta_expand_slots() passes the directory entry's own col
     * field, and biscuit_dir_slot_for_col() maps both -1 and 0 onto directory
     * slot 0, so an entry written under 0-based addressing in a
     * single-column index is indistinguishable by slot but not by key.
     *
     * Logging `col` on both sides is what settles it. If this trace shows
     * pend=none for structures the delta demonstrably populated, compare the
     * col values; if they differ, that is the defect and it needs no further
     * localization.
     */
    if (unlikely(biscuit_diag_scan_trace))
        ereport(WARNING,
                (errmsg("biscuit: diag reconcile col=%d lower=%d kind=%u ch=%d pos=%d "
                        "-> pend=%s kills=%s cached=" UINT64_FORMAT,
                        col, is_lower ? 1 : 0, kind, ch, position,
                        pend ? "hit" : "none",
                        biscuit_pendlog_has_kills(snap) ? "yes" : "no",
                        cached ? biscuit_roaring_count(cached) : 0)));

    /*
     * THE MISS IS NO LONGER AUTOMATICALLY A NO-OP.
     *
     * Under the old derived-record scheme, a structure with no entry in
     * the log was untouched by definition -- every mutation named the
     * structure it applied to, so a hash miss meant "nothing pending here"
     * and the cached bitmap could be returned borrowed, with zero copies.
     *
     * The kill set breaks that implication in one direction. A DELETE now
     * records only that a slot was retired; it names no structures,
     * because the set of structures the slot belonged to is a function of
     * text that an UPDATE may already have overwritten. So a structure
     * with no entry may still be holding a slot that has been retired out
     * from under it, and returning the cached bitmap unmodified would
     * yield a TID for a row that no longer matches the pattern. Nothing
     * downstream would catch it: xs_recheck is false, so the predicate is
     * never re-tested, and MVCC filters dead rows, not wrong ones.
     *
     * The fast path survives where it is still valid. An empty kill set
     * means no base membership anywhere is stale, which is the case for
     * any workload that has not deleted or updated a row since the last
     * drain -- and biscuit_pendlog.c deliberately keeps single
     * unaccompanied ADDs out of the kill set precisely so that a
     * pure-insert stream keeps hitting this branch.
     */
    if (pend == NULL && !biscuit_pendlog_has_kills(snap))
        return cached;   /* nothing pending, and nothing retired */

    /*
     * Copy before applying: `cached` is the caller's live, borrowed
     * bitmap and this is a read, not a drain -- mutating it in place would
     * corrupt the cached structure with deltas that are already durably
     * recorded and will be applied again at the next real drain.
     */
    merged = cached ? biscuit_roaring_copy(cached) : biscuit_roaring_create();
    biscuit_pendlog_apply(snap, pend, merged);
    biscuit_reconcile_register_cleanup(merged);

    if (unlikely(biscuit_diag_scan_trace))
        ereport(WARNING,
                (errmsg("biscuit: diag reconcile col=%d kind=%u ch=%d pos=%d "
                        "-> merged " UINT64_FORMAT " (from " UINT64_FORMAT ")",
                        col, kind, ch, position,
                        biscuit_roaring_count(merged),
                        cached ? biscuit_roaring_count(cached) : 0)));

    return merged;
}

/* ================================================================
 * SECTION 1 – CharIndex bitmap accessor helpers
 * ================================================================
 *
 * Sorted PosEntry arrays use binary search for O(log n) lookup and
 * insertion-sort to keep entries ordered.
 */

/* ---------- single-column (legacy) case-sensitive ---------- */

RoaringBitmap *
biscuit_get_pos_bitmap(Relation index, BiscuitIndex *idx, unsigned char ch, int pos)
{
    CharIndex     *cidx   = &idx->pos_idx_legacy[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < pos) left  = mid + 1;
        else                                    right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, -1, false,
                                      BISCUIT_DIR_KIND_POS, ch, pos);
}

RoaringBitmap *
biscuit_get_neg_bitmap(Relation index, BiscuitIndex *idx, unsigned char ch, int neg_offset)
{
    CharIndex     *cidx   = &idx->neg_idx_legacy[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < neg_offset) left  = mid + 1;
        else                                           right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, -1, false,
                                      BISCUIT_DIR_KIND_NEG, ch, neg_offset);
}

void
biscuit_set_pos_bitmap(BiscuitIndex *idx, unsigned char ch, int pos, RoaringBitmap *bm)
{
    CharIndex *cidx       = &idx->pos_idx_legacy[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < pos) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_neg_bitmap(BiscuitIndex *idx, unsigned char ch, int neg_offset, RoaringBitmap *bm)
{
    CharIndex *cidx       = &idx->neg_idx_legacy[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < neg_offset) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

/* ---------- single-column case-insensitive (lower) ---------- */

RoaringBitmap *
biscuit_get_pos_bitmap_lower(Relation index, BiscuitIndex *idx, unsigned char ch, int pos)
{
    CharIndex     *cidx   = &idx->pos_idx_lower[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < pos) left  = mid + 1;
        else                                    right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, -1, true,
                                      BISCUIT_DIR_KIND_POS, ch, pos);
}

RoaringBitmap *
biscuit_get_neg_bitmap_lower(Relation index, BiscuitIndex *idx, unsigned char ch, int neg_offset)
{
    CharIndex     *cidx   = &idx->neg_idx_lower[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < neg_offset) left  = mid + 1;
        else                                           right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, -1, true,
                                      BISCUIT_DIR_KIND_NEG, ch, neg_offset);
}

void
biscuit_set_pos_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int pos, RoaringBitmap *bm)
{
    CharIndex *cidx       = &idx->pos_idx_lower[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < pos) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_neg_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int neg_offset, RoaringBitmap *bm)
{
    CharIndex *cidx       = &idx->neg_idx_lower[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < neg_offset) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

/* ---------- multi-column accessors ---------- */

RoaringBitmap *
biscuit_get_col_pos_bitmap(Relation index, ColumnIndex *col, int col_idx,
                            unsigned char ch, int pos)
{
    CharIndex     *cidx   = &col->pos_idx[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < pos) left  = mid + 1;
        else                                    right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, col_idx, false,
                                      BISCUIT_DIR_KIND_POS, ch, pos);
}

RoaringBitmap *
biscuit_get_col_neg_bitmap(Relation index, ColumnIndex *col, int col_idx,
                            unsigned char ch, int neg_offset)
{
    CharIndex     *cidx   = &col->neg_idx[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < neg_offset) left  = mid + 1;
        else                                           right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, col_idx, false,
                                      BISCUIT_DIR_KIND_NEG, ch, neg_offset);
}

RoaringBitmap *
biscuit_get_col_pos_bitmap_lower(Relation index, ColumnIndex *col, int col_idx,
                                  unsigned char ch, int pos)
{
    CharIndex     *cidx   = &col->pos_idx_lower[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < pos) left  = mid + 1;
        else                                    right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, col_idx, true,
                                      BISCUIT_DIR_KIND_POS, ch, pos);
}

RoaringBitmap *
biscuit_get_col_neg_bitmap_lower(Relation index, ColumnIndex *col, int col_idx,
                                  unsigned char ch, int neg_offset)
{
    CharIndex     *cidx   = &col->neg_idx_lower[ch];
    RoaringBitmap *cached = NULL;
    int            left   = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cached = cidx->entries[mid].bitmap; break; }
        else if (cidx->entries[mid].pos < neg_offset) left  = mid + 1;
        else                                           right = mid - 1;
    }
    return biscuit_reconcile_pending(index, cached, col_idx, true,
                                      BISCUIT_DIR_KIND_NEG, ch, neg_offset);
}

void
biscuit_set_col_pos_bitmap(ColumnIndex *col, unsigned char ch, int pos, RoaringBitmap *bm)
{
    CharIndex *cidx       = &col->pos_idx[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < pos) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_col_neg_bitmap(ColumnIndex *col, unsigned char ch, int neg_offset, RoaringBitmap *bm)
{
    CharIndex *cidx       = &col->neg_idx[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < neg_offset) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_col_pos_bitmap_lower(ColumnIndex *col, unsigned char ch, int pos, RoaringBitmap *bm)
{
    CharIndex *cidx       = &col->pos_idx_lower[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < pos) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_col_neg_bitmap_lower(ColumnIndex *col, unsigned char ch, int neg_offset, RoaringBitmap *bm)
{
    CharIndex *cidx       = &col->neg_idx_lower[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < neg_offset) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

/* ================================================================
 * SECTION 2 – Length bitmap helpers
 * ================================================================ */

static RoaringBitmap *
biscuit_get_length_ge(Relation index, BiscuitIndex *idx, int min_len)
{
    RoaringBitmap *cached = NULL;
    int            position = (min_len <= 0) ? 0 : min_len;

    if (min_len <= 0 && idx->length_ge_bitmaps_legacy && idx->length_ge_bitmaps_legacy[0])
        cached = idx->length_ge_bitmaps_legacy[0];
    else if (min_len < idx->max_length_legacy && idx->length_ge_bitmaps_legacy &&
             idx->length_ge_bitmaps_legacy[min_len])
        cached = idx->length_ge_bitmaps_legacy[min_len];

    /*
     * biscuit_reconcile_pending() now always registers a fresh copy for
     * automatic cleanup when CurrentMemoryContext resets/deletes (see
     * biscuit_reconcile_register_cleanup()) -- that copy must NOT also be
     * freed by us or by our caller. This function's own contract with
     * ITS caller has always been "always hand back an owned copy you may
     * mutate/free" (see biscuit_query_pattern_masked() etc., which do
     * exactly that on every call site). Honor that by always making one
     * more independent copy here, regardless of which case we got back --
     * unlike case (a) alone, this no longer special-cases "reconciled ==
     * cached" to skip the copy, because in case (b) `reconciled` is no
     * longer ours to hand off uncopied.
     */
    {
        RoaringBitmap *reconciled = biscuit_reconcile_pending(index, cached, -1, false,
                                                                BISCUIT_DIR_KIND_LEN_GE,
                                                                -1, position);
        return reconciled ? biscuit_roaring_copy(reconciled) : biscuit_roaring_create();
    }
}

static RoaringBitmap *
biscuit_get_length_ge_lower(Relation index, BiscuitIndex *idx, int min_len)
{
    RoaringBitmap *cached = NULL;
    int            position = (min_len <= 0) ? 0 : min_len;

    if (min_len <= 0 && idx->length_ge_bitmaps_lower && idx->length_ge_bitmaps_lower[0])
        cached = idx->length_ge_bitmaps_lower[0];
    else if (min_len < idx->max_length_lower && idx->length_ge_bitmaps_lower &&
             idx->length_ge_bitmaps_lower[min_len])
        cached = idx->length_ge_bitmaps_lower[min_len];

    /* See biscuit_get_length_ge()'s comment: always return an
     * independent copy now that a fresh `reconciled` may be
     * snapshot-owned rather than ours to hand off. */
    {
        RoaringBitmap *reconciled = biscuit_reconcile_pending(index, cached, -1, true,
                                                                BISCUIT_DIR_KIND_LEN_GE,
                                                                -1, position);
        return reconciled ? biscuit_roaring_copy(reconciled) : biscuit_roaring_create();
    }
}

static RoaringBitmap *
biscuit_get_col_length_ge(Relation index, ColumnIndex *col, int col_idx, int min_len)
{
    RoaringBitmap *cached = NULL;
    int            position = (min_len <= 0) ? 0 : min_len;

    if (min_len <= 0 && col->length_ge_bitmaps && col->length_ge_bitmaps[0])
        cached = col->length_ge_bitmaps[0];
    else if (min_len < col->max_length && col->length_ge_bitmaps &&
             col->length_ge_bitmaps[min_len])
        cached = col->length_ge_bitmaps[min_len];

    /* See biscuit_get_length_ge()'s comment. */
    {
        RoaringBitmap *reconciled = biscuit_reconcile_pending(index, cached, col_idx, false,
                                                                BISCUIT_DIR_KIND_LEN_GE,
                                                                -1, position);
        return reconciled ? biscuit_roaring_copy(reconciled) : biscuit_roaring_create();
    }
}

static RoaringBitmap *
biscuit_get_col_length_ge_lower(Relation index, ColumnIndex *col, int col_idx, int min_len)
{
    RoaringBitmap *cached = NULL;
    int            position = (min_len <= 0) ? 0 : min_len;

    if (min_len <= 0 && col->length_ge_bitmaps_lower && col->length_ge_bitmaps_lower[0])
        cached = col->length_ge_bitmaps_lower[0];
    else if (min_len < col->max_length_lower && col->length_ge_bitmaps_lower &&
             col->length_ge_bitmaps_lower[min_len])
        cached = col->length_ge_bitmaps_lower[min_len];

    /* See biscuit_get_length_ge()'s comment. */
    {
        RoaringBitmap *reconciled = biscuit_reconcile_pending(index, cached, col_idx, true,
                                                                BISCUIT_DIR_KIND_LEN_GE,
                                                                -1, position);
        return reconciled ? biscuit_roaring_copy(reconciled) : biscuit_roaring_create();
    }
}

/*
 * biscuit_get_negation_base_set
 *
 * The "all indexed, non-null rows of this column" set that NOT LIKE /
 * NOT ILIKE inverts against: length_ge[0], reconciled against the shared
 * pending log. Returns an owned bitmap the caller must free; falls back to
 * a dense [0, num_records) range when the column has no length_ge array
 * (a column built before length bitmaps existed, or one where they were
 * never populated).
 *
 * WHY THIS EXISTS -- this is a correctness fix, not a refactor.
 *
 * biscuit_scan.c used to build this set by reading
 * col->length_ge_bitmaps[0] directly out of the in-memory ColumnIndex,
 * with no reconciliation, and then subtract from it a col_result that HAD
 * been reconciled. While per-structure pending chains existed that was
 * survivable in practice; once the shared pending log deferred drains far
 * longer (BISCUIT_PENDLOG_DRAIN_PAGES = 4MB, deliberately, for OLAP bulk
 * loads) the two sides routinely disagreed about which slots exist.
 *
 * The observable failure: insert 600 rows, delete 80, VACUUM, insert 80
 * more (which recycle the freed slots via the free list). The recycled
 * slots' LEN_GE membership is sitting undrained in the log, so the
 * unreconciled base set omits them while the reconciled positive side
 * includes them. `a NOT LIKE 'Item%'` returned 0 where the heap says 80 --
 * every recycled row silently missing from the complement. It only
 * reproduced with autovacuum off; an autovacuum-triggered drain hid it
 * completely, which is exactly what makes this class of bug dangerous.
 *
 * The rule this encodes: ANY bitmap that participates in a query must come
 * from a reconciling accessor. Mixing a raw cached bitmap with a
 * reconciled one in the same expression is always a bug, and the two-sided
 * nature of negation makes it a silent one.
 */
/*
 * biscuit_get_negation_base_set_legacy
 *
 * Single-column (legacy field) equivalent of
 * biscuit_get_negation_base_set(); same reconciliation requirement and the
 * same bug if it is bypassed. The dense fallback here filters on
 * data_cache[j] rather than using a plain range, matching what the
 * single-column scan path did before -- a NULL-valued row has no
 * data_cache entry and must not appear in the complement.
 */
RoaringBitmap *
biscuit_get_negation_base_set_legacy(Relation index, BiscuitIndex *idx,
                                      bool is_lower)
{
    RoaringBitmap **ge_arr = is_lower ? idx->length_ge_bitmaps_lower
                                       : idx->length_ge_bitmaps_legacy;

    if (ge_arr && ge_arr[0])
        return is_lower ? biscuit_get_length_ge_lower(index, idx, 0)
                        : biscuit_get_length_ge(index, idx, 0);

    {
        /*
         * Dense fallback, filtered on data_cache[j] rather than a plain
         * range: a NULL-valued row has no data_cache entry and must not
         * appear in the complement. That much is unchanged.
         *
         * What is new is the reconciliation. data_cache is this backend's
         * in-memory mirror, so on its own it misses every row written
         * since this backend last loaded, and retains every row retired
         * since -- the same two errors the multi-column fallback makes,
         * and with the same silent symptom, since a row missing from
         * all_rows is simply absent from the result with no dead tuple for
         * anything downstream to filter. Route it through the ordinary
         * accessor so the kill set is subtracted and the delta's LEN_GE[0]
         * additions are merged.
         */
        RoaringBitmap *all = biscuit_roaring_create_sized((uint32) idx->num_records);
        int            j;

        for (j = 0; j < idx->num_records; j++)
            if (idx->data_cache[j])
                biscuit_roaring_add(all, j);

        {
            RoaringBitmap *reconciled = biscuit_reconcile_pending(index, all, BISCUIT_DIR_COL_LEGACY,
                                                                    is_lower, BISCUIT_DIR_KIND_LEN_GE,
                                                                    -1, 0);

            if (reconciled == all)
                return all;   /* nothing pending: `all` is already ours, uniquely */

            /*
             * A fresh copy was made from `all` and is now registered for
             * automatic cleanup when CurrentMemoryContext resets/deletes
             * (biscuit_reconcile_register_cleanup(), inside
             * biscuit_reconcile_pending()) -- `all` itself was fully
             * absorbed into that copy and is now dead weight; free it
             * rather than leaving it an orphan (it is a real,
             * CRoaring-allocated bitmap like any other, not
             * context-scoped scratch, whatever an earlier version of
             * this comment claimed). And since our own caller's contract
             * is "you own what this returns, free it whenever", hand
             * back an independent copy of our own rather than the
             * registered-for-callback `reconciled` -- returning that
             * directly would double-free it once its own context resets.
             */
            biscuit_roaring_free(all);
            return biscuit_roaring_copy(reconciled);
        }
    }
}

/*
 * NEGATION UNDER BASE + DELTA (design §6.1)
 *
 * NOT LIKE p is NOT (NOT base) OR (NOT delta). Negation does not
 * distribute over the merge. It has to be:
 *
 *     all_rows \ (base_match(p) | delta_match(p))
 *
 * where all_rows includes delta rows and excludes tombstones. The
 * subtrahend is handled by the caller, which builds it from the same
 * reconciling accessors as any positive scan. What this function owes it
 * is an all_rows that has been through the identical reconciliation --
 * which is exactly what the primary path below does, because
 * biscuit_get_col_length_ge(index, ..., 0) is a reconciling accessor and
 * LEN_GE[0] is by construction "every indexed, non-null row of this
 * column". A row added since the last drain appears in the delta's
 * LEN_GE[0] entry; a row retired since then is in the kill set and is
 * subtracted from base's. Both happen inside the accessor.
 *
 * MVCC DOES NOT COVER A MISTAKE HERE, and the failure is the quiet kind.
 * A row wrongly missing from all_rows is simply absent from the result --
 * it is a live, visible tuple that was never returned, so there is no dead
 * tuple for the executor's visibility check to filter and nothing to
 * notice. This is the same shape as the bug that motivated this function's
 * existence: mixing an unreconciled base with a reconciled subtrahend
 * returned 0 rows where the heap had 80, reproducible only with autovacuum
 * off, because any drain hid it completely.
 */
RoaringBitmap *
biscuit_get_negation_base_set(Relation index, ColumnIndex *col, int col_idx,
                               bool is_lower, int num_records)
{
    RoaringBitmap  **ge_arr = is_lower ? col->length_ge_bitmaps_lower
                                        : col->length_ge_bitmaps;

    if (ge_arr && ge_arr[0])
        return is_lower ? biscuit_get_col_length_ge_lower(index, col, col_idx, 0)
                        : biscuit_get_col_length_ge(index, col, col_idx, 0);

    {
        /*
         * DENSE FALLBACK -- a column with no LEN_GE array at all (built
         * before length bitmaps existed, or never populated).
         *
         * num_records is this backend's process-local high-water mark, so
         * the raw range is "every slot this backend believes exists". Two
         * corrections are needed before it can stand in for all_rows, and
         * neither was applied before, because before the base/delta change
         * this path could not see undrained state at all.
         *
         * 1. Add delta rows. A row inserted since the last drain may sit
         *    at a slot beyond num_records as this backend last loaded it.
         *    Omitting it silently drops it from every negation result.
         *
         * 2. Subtract tombstones AND the kill set. A deleted row's slot
         *    stays within [0, num_records) -- it goes on the free list, it
         *    does not shrink the range -- so a plain range readmits every
         *    deleted row into the complement.
         *
         * Both are done by reconciling an empty structure through the
         * ordinary accessor path: the kill set is subtracted and the
         * delta's own LEN_GE[0] additions are merged in, which is the
         * same treatment the primary path above receives.
         */
        RoaringBitmap *all = biscuit_roaring_create_sized((uint32) num_records);
        RoaringBitmap *reconciled;

#ifdef HAVE_ROARING
        roaring_bitmap_add_range(all, 0, num_records);
#else
        int j;
        for (j = 0; j < num_records; j++)
            biscuit_roaring_add(all, j);
#endif

        reconciled = biscuit_reconcile_pending(index, all, col_idx, is_lower,
                                                BISCUIT_DIR_KIND_LEN_GE, -1, 0);

        if (reconciled == all)
            return all;   /* nothing pending: `all` is already ours, uniquely */

        /*
         * A fresh copy was made from `all` and is now registered for
         * automatic cleanup when CurrentMemoryContext resets/deletes
         * (biscuit_reconcile_register_cleanup(), inside
         * biscuit_reconcile_pending()) -- `all` itself was fully
         * absorbed into that copy and is now dead weight, not
         * "context-scoped scratch reclaimed with the rest" as an earlier
         * version of this comment claimed (it is a real,
         * CRoaring-allocated bitmap like any other -- see
         * biscuit_index_free_bitmaps()'s comment in biscuit_cache.c for
         * why nothing but an explicit free ever reaches one of these).
         * Free it, and hand our own caller an independent copy rather
         * than the registered-for-callback `reconciled`: this function's
         * documented contract is "returns an owned bitmap the caller
         * must free", and returning that directly would double-free it
         * once its own context resets.
         */
        biscuit_roaring_free(all);
        return biscuit_roaring_copy(reconciled);
    }
}

/* ================================================================
 * SECTION 3 – Pattern parsing
 * ================================================================
 *
 * BISCUIT_LITERAL_ESC is a sentinel byte (SOH, 0x01) stored in
 * ParsedPattern part strings to indicate that the following byte is
 * a literal character rather than a LIKE wildcard.  It is used to
 * distinguish a literal '_' (escaped as '\\_' in SQL) from the
 * single-character wildcard '_'.
 *
 * 0x01 (SOH) is chosen because:
 *  - It cannot appear in valid UTF-8 text stored in PostgreSQL.
 *  - It cannot be a LIKE wildcard ('%' or '_').
 *  - It is distinct from any real data byte the matchers would see.
 */
#define BISCUIT_LITERAL_ESC  '\x01'

/*
 * biscuit_part_char_count
 * Count the number of LIKE pattern characters represented by a part
 * string that may contain BISCUIT_LITERAL_ESC + byte sentinel pairs.
 * Each sentinel pair counts as one character.  Regular UTF-8 sequences
 * are counted normally.
 */
static int
biscuit_part_char_count(const char *part, int byte_len)
{
    int count = 0;
    int i     = 0;
    while (i < byte_len)
    {
        unsigned char c = (unsigned char) part[i];
        if (c == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            i += 2; /* sentinel + literal byte = 1 char */
        }
        else
        {
            i += biscuit_utf8_char_length(c);
        }
        count++;
    }
    return count;
}

/*
 * biscuit_part_seed_byte
 * ----------------------
 * Return the first concrete (non-wildcard) byte from a part string for
 * use as a char-cache lookup seed.  Skips:
 *   - bare '_' bytes (single-char wildcard)
 *   - BISCUIT_LITERAL_ESC prefixes, but returns the byte AFTER the prefix
 *     (that byte is the literal character to seed from)
 * Returns 0 (NUL) if the part is all wildcards.
 */
static unsigned char
biscuit_part_seed_byte(const char *part, int byte_len)
{
    int i = 0;
    while (i < byte_len)
    {
        unsigned char c = (unsigned char) part[i];
        if (c == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            if (i + 1 < byte_len)
                return (unsigned char) part[i + 1]; /* literal char */
            i += 2;
        }
        else if (c == '_')
        {
            i++;  /* skip single-char wildcard */
        }
        else
        {
            return c;  /* first concrete character */
        }
    }
    return 0;  /* all wildcards */
}

/*
 * biscuit_part_match_substr
 * -------------------------
 * Check whether the sentinel-encoded part string matches a substring of
 * haystack starting at byte offset hay_off.
 *
 * Rules:
 *   - A bare '_' in the part matches exactly one UTF-8 character in hay.
 *   - A BISCUIT_LITERAL_ESC + byte pair matches that exact literal byte.
 *   - All other bytes match themselves exactly.
 *
 * Returns true on a full match of the part, false otherwise.
 * hay/hay_byte_len: the full string and its byte length.
 * hay_off: byte offset within hay where matching starts.
 */
static bool
biscuit_part_match_substr(const char *hay, int hay_byte_len, int hay_off,
                           const char *part, int part_byte_len)
{
    int pi = 0;  /* part byte position */
    int hi = hay_off;  /* haystack byte position */

    while (pi < part_byte_len)
    {
        unsigned char pc;
        if (hi >= hay_byte_len)
            return false;

        pc = (unsigned char) part[pi];

        if (pc == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal;
            /* Literal match: part[pi+1] must equal hay[hi] exactly */
            if (pi + 1 >= part_byte_len)
                return false;
            literal = (unsigned char) part[pi + 1];
            if ((unsigned char) hay[hi] != literal)
                return false;
            pi += 2;
            hi += 1;
        }
        else if (pc == '_')
        {
            /* Single-char wildcard: consume one UTF-8 char from haystack */
            int cl = biscuit_utf8_char_length((unsigned char) hay[hi]);
            if (hi + cl > hay_byte_len)
                return false;
            pi += 1;
            hi += cl;
        }
        else
        {
            /* Literal byte from pattern: must match exactly */
            if ((unsigned char) hay[hi] != pc)
                return false;
            pi += 1;
            hi += 1;
        }
    }
    return true;  /* all part bytes consumed */
}

/*
 * biscuit_wildcard_contains
 * --------------------------
 * Wildcard-aware replacement for strstr(): returns true if the
 * sentinel-encoded `part` (which may contain bare '_' single-char
 * wildcards and/or BISCUIT_LITERAL_ESC-escaped literal underscores)
 * matches a substring of `hay` starting at some UTF-8 character
 * boundary.
 *
 * A plain strstr() treats '_' as a literal byte, which is wrong for
 * SQL LIKE/ILIKE semantics ('_' must match any single character).
 * This function instead tries biscuit_part_match_substr() — which
 * already implements correct '_'/escape handling — at every valid
 * starting offset, exactly like strstr() would scan every starting
 * offset for a literal needle.
 */
static bool
biscuit_wildcard_contains(const char *hay, int hay_byte_len,
                           const char *part, int part_byte_len)
{
    int hay_off = 0;

    if (part_byte_len == 0)
        return true;

    while (hay_off <= hay_byte_len)
    {
        if (biscuit_part_match_substr(hay, hay_byte_len, hay_off,
                                       part, part_byte_len))
            return true;
        if (hay_off >= hay_byte_len)
            break;
        /* Advance by one whole UTF-8 character, not one raw byte, so we
         * never attempt a match starting mid-way through a multi-byte
         * sequence. */
        hay_off += biscuit_utf8_char_length((unsigned char) hay[hay_off]);
    }
    return false;
}


/* Splits a LIKE/ILIKE pattern on unescaped '%' wildcards.
 *
 * SQL escape convention (backslash, matching what PostgreSQL passes to
 * index AMs after processing the ESCAPE clause):
 *   '\%'  -> literal '%'  (not a wildcard separator)
 *   '\_'  -> literal '_'  (not a single-char wildcard)
 *   '\\'  -> literal '\'
 *
 * Each resulting part string has escape sequences collapsed to the
 * actual character they represent, so the downstream bitmap matchers
 * see real bytes and never mis-treat them as wildcards.
 *
 * starts_percent / ends_percent reflect whether the *unescaped* pattern
 * starts/ends with a wildcard '%'.
 */
ParsedPattern *
biscuit_parse_pattern(const char *pattern)
{
    ParsedPattern *parsed;
    int            plen      = strlen(pattern);
    int            max_parts = plen + 1;
    int            part_count;
    char          *buf;      /* unescaped part accumulator */
    int            buf_len;
    bool           in_part;
    int            i;

    buf      = (char *) palloc(2 * plen + 1); /* 2× for literal-_ sentinel pairs */
    buf_len  = 0;
    in_part  = false;

    parsed                 = (ParsedPattern *) palloc(sizeof(ParsedPattern));
    parsed->parts          = (char **) palloc(max_parts * sizeof(char *));
    parsed->part_lens      = (int *)   palloc(max_parts * sizeof(int));
    parsed->part_byte_lens = (int *)   palloc(max_parts * sizeof(int));
    parsed->part_count     = 0;
    parsed->starts_percent = false;
    parsed->ends_percent   = false;
    part_count             = 0;

    /*
     * Determine starts_percent: true only when the very first character
     * is an unescaped '%'.
     */
    if (plen > 0)
    {
        if (pattern[0] == '\\' && plen > 1)
            parsed->starts_percent = false;  /* escaped char — not a wildcard */
        else
            parsed->starts_percent = (pattern[0] == '%');
    }

    /*
     * Determine ends_percent: true only when the last character is an
     * unescaped '%'.  Count consecutive backslashes immediately before
     * it; an odd count means the '%' is escaped.
     */
    if (plen > 0 && pattern[plen - 1] == '%')
    {
        int bs = 0, j = plen - 2;
        while (j >= 0 && pattern[j] == '\\') { bs++; j--; }
        parsed->ends_percent = ((bs % 2) == 0);
    }

    /* Main scan: walk pattern, unescape on the fly, split on bare '%'. */
    for (i = 0; i <= plen; )
    {
        if (i == plen)
        {
            /* End of pattern: flush current part if any. */
            if (in_part)
            {
                char *part_str             = pnstrdup(buf, buf_len);
                parsed->parts[part_count]          = part_str;
                parsed->part_byte_lens[part_count]  = buf_len;
                parsed->part_lens[part_count]       =
                    biscuit_part_char_count(part_str, buf_len);
                part_count++;
            }
            break;
        }

        if (pattern[i] == '\\' && i + 1 < plen)
        {
            char escaped = pattern[i + 1];
            /*
             * Backslash escapes any following character:
             *   '\%'  -> literal '%'  (not a wildcard separator)
             *   '\_'  -> literal '_'  (emit sentinel so matchers don't
             *                          treat it as a single-char wildcard)
             *   '\\'  -> literal '\'
             *   '\X'  -> literal 'X'  (for any other X, e.g. '\.')
             *
             * For '_' we store BISCUIT_LITERAL_ESC + '_' in the part
             * buffer so downstream matchers can distinguish literal '_'
             * from wildcard '_'.  All other escaped chars are stored as
             * their literal byte (they have no wildcard meaning anyway).
             */
            if (!in_part)
                in_part = true;
            if (escaped == '_')
            {
                buf[buf_len++] = BISCUIT_LITERAL_ESC;
                buf[buf_len++] = '_';
            }
            else
            {
                /* '%', '\\', '.', '-', etc. — just the literal byte */
                buf[buf_len++] = escaped;
            }
            i += 2;
        }
        else if (pattern[i] == '%')
        {
            /* Unescaped '%': wildcard separator — flush current part. */
            if (in_part)
            {
                char *part_str             = pnstrdup(buf, buf_len);
                parsed->parts[part_count]          = part_str;
                parsed->part_byte_lens[part_count]  = buf_len;
                parsed->part_lens[part_count]       =
                    biscuit_part_char_count(part_str, buf_len);
                part_count++;
                in_part = false;
                buf_len = 0;
            }
            i++;
        }
        else
        {
            /* Regular character (including unescaped '_'): copy verbatim.
             * Multi-byte UTF-8 sequences are copied whole. */
            int char_len = biscuit_utf8_char_length((unsigned char) pattern[i]);
            if (i + char_len > plen)
                char_len = plen - i;
            if (!in_part)
                in_part = true;
            memcpy(buf + buf_len, pattern + i, char_len);
            buf_len += char_len;
            i       += char_len;
        }
    }

    pfree(buf);
    parsed->part_count = part_count;
    return parsed;
}

void
biscuit_free_parsed_pattern(ParsedPattern *parsed)
{
    int i;

    if (!parsed)
        return;

    if (parsed->parts)
    {
        for (i = 0; i < parsed->part_count; i++)
        {
            if (parsed->parts[i])
            {
                pfree(parsed->parts[i]);
                parsed->parts[i] = NULL;
            }
        }
        pfree(parsed->parts);
    }
    if (parsed->part_lens)     pfree(parsed->part_lens);
    if (parsed->part_byte_lens) pfree(parsed->part_byte_lens);
    pfree(parsed);
}

/* ================================================================
 * SECTION 4 – Low-level part matching (single-column)
 * ================================================================ */

/*
 * Match part at a specific character position (from start) – case-sensitive.
 * Handles underscore (_) wildcards and multi-byte UTF-8 sequences.
 */
static RoaringBitmap *
biscuit_match_part_at_pos(Relation index, BiscuitIndex *idx, const char *part,
                          int part_byte_len, int start_pos)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int            part_byte_pos  = 0;
    int            char_pos       = start_pos;
    int            concrete_chars = 0;
    bool           first_char     = true;

    while (part_byte_pos < part_byte_len)
    {
        unsigned char first_byte = (unsigned char) part[part_byte_pos];

        /* BISCUIT_LITERAL_ESC prefix: next byte is a literal character,
         * not a wildcard.  Consume both bytes and treat the second as a
         * concrete 1-byte character for bitmap lookup. */
        if (first_byte == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal_byte;
            RoaringBitmap *char_bm;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            literal_byte = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            char_bm = biscuit_get_pos_bitmap(index, idx, literal_byte, char_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2;
            char_pos++;
            continue;
        }

        if (first_byte == '_') { part_byte_pos++; char_pos++; continue; }

        {
        int char_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_len > part_byte_len)
            char_len = part_byte_len - part_byte_pos;

        concrete_chars++;

        if (char_len == 1)
        {
            RoaringBitmap *char_bm = biscuit_get_pos_bitmap(index, idx, first_byte, char_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
        }
        else
        {
            RoaringBitmap *multibyte = NULL;
            int b;
            for (b = 0; b < char_len; b++)
            {
                unsigned char bv = (unsigned char) part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_pos_bitmap(index, idx, bv, char_pos);
                if (!byte_bm) {
                    if (multibyte) biscuit_roaring_free(multibyte);
                    if (result)   biscuit_roaring_free(result);
                    return biscuit_roaring_create();
                }
                if (b == 0) multibyte = biscuit_roaring_copy(byte_bm);
                else { biscuit_roaring_and_inplace(multibyte, byte_bm); if (biscuit_roaring_is_empty(multibyte)) { biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } }
            }
            if (first_char) { result = multibyte; first_char = false; }
            else { biscuit_roaring_and_inplace(result, multibyte); biscuit_roaring_free(multibyte); if (biscuit_roaring_is_empty(result)) return result; }
        }

        part_byte_pos += char_len;
        char_pos++;
        } /* end char_len block */
    }

    {
        int pattern_char_count = biscuit_part_char_count(part, part_byte_len);
        if (concrete_chars == 0) {
            if (result) biscuit_roaring_free(result);
            result = biscuit_get_length_ge(index, idx, start_pos + pattern_char_count);
        } else {
            len_filter = biscuit_get_length_ge(index, idx, start_pos + pattern_char_count);
            if (len_filter) {
                if (result) biscuit_roaring_and_inplace(result, len_filter);
                else        { result = len_filter; len_filter = NULL; }
                if (len_filter) biscuit_roaring_free(len_filter);
            }
        }
    }

    return result ? result : biscuit_roaring_create();
}

/* Match part anchored at the end of string – case-sensitive. */
static RoaringBitmap *
biscuit_match_part_at_end(Relation index, BiscuitIndex *idx, const char *part, int part_byte_len)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int            concrete_chars        = 0;
    bool           first_char            = true;
    int            pattern_char_count    = biscuit_part_char_count(part, part_byte_len);
    int            part_byte_pos         = 0;
    int            char_offset_from_end  = 0;

    while (part_byte_pos < part_byte_len)
    {
        unsigned char first_byte = (unsigned char) part[part_byte_pos];

        if (first_byte == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal_byte;
            RoaringBitmap *char_bm;
            int neg_pos;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            literal_byte = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            neg_pos = -(pattern_char_count - char_offset_from_end);
            char_bm = biscuit_get_neg_bitmap(index, idx, literal_byte, neg_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2;
            char_offset_from_end++;
            continue;
        }

        if (first_byte == '_') { part_byte_pos++; char_offset_from_end++; continue; }

        {
        int char_len = biscuit_utf8_char_length(first_byte);
        int neg_pos;
        if (part_byte_pos + char_len > part_byte_len)
            char_len = part_byte_len - part_byte_pos;

        concrete_chars++;
        neg_pos = -(pattern_char_count - char_offset_from_end);

        if (char_len == 1)
        {
            RoaringBitmap *char_bm = biscuit_get_neg_bitmap(index, idx, first_byte, neg_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
        }
        else
        {
            RoaringBitmap *multibyte = NULL;
            int b;
            for (b = 0; b < char_len; b++)
            {
                unsigned char bv = (unsigned char) part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_neg_bitmap(index, idx, bv, neg_pos);
                if (!byte_bm) { if (multibyte) biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
                if (b == 0) multibyte = biscuit_roaring_copy(byte_bm);
                else { biscuit_roaring_and_inplace(multibyte, byte_bm); if (biscuit_roaring_is_empty(multibyte)) { biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } }
            }
            if (first_char) { result = multibyte; first_char = false; }
            else { biscuit_roaring_and_inplace(result, multibyte); biscuit_roaring_free(multibyte); if (biscuit_roaring_is_empty(result)) return result; }
        }

        part_byte_pos += char_len;
        char_offset_from_end++;
        } /* end char_len block */
    }

    if (concrete_chars == 0) {
        if (result) biscuit_roaring_free(result);
        result = biscuit_get_length_ge(index, idx, pattern_char_count);
    } else {
        len_filter = biscuit_get_length_ge(index, idx, pattern_char_count);
        if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); }
    }

    return result ? result : biscuit_roaring_create();
}

/* Case-insensitive variants reuse the same logic via _lower accessors */
static RoaringBitmap *
biscuit_match_part_at_pos_ilike(Relation index, BiscuitIndex *idx, const char *part,
                                int part_byte_len, int start_pos)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int            part_byte_pos  = 0;
    int            char_pos       = start_pos;
    int            concrete_chars = 0;
    bool           first_char     = true;

    while (part_byte_pos < part_byte_len)
    {
        unsigned char first_byte = (unsigned char) part[part_byte_pos];
        if (first_byte == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal_byte;
            RoaringBitmap *char_bm;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            literal_byte = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            char_bm = biscuit_get_pos_bitmap_lower(index, idx, literal_byte, char_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2;
            char_pos++;
            continue;
        }
        if (first_byte == '_') { part_byte_pos++; char_pos++; continue; }

        {
        int char_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_len > part_byte_len)
            char_len = part_byte_len - part_byte_pos;
        concrete_chars++;

        if (char_len == 1)
        {
            RoaringBitmap *char_bm = biscuit_get_pos_bitmap_lower(index, idx, first_byte, char_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
        }
        else
        {
            RoaringBitmap *multibyte = NULL; int b;
            for (b = 0; b < char_len; b++) {
                unsigned char bv = (unsigned char) part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_pos_bitmap_lower(index, idx, bv, char_pos);
                if (!byte_bm) { if (multibyte) biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
                if (b == 0) multibyte = biscuit_roaring_copy(byte_bm);
                else { biscuit_roaring_and_inplace(multibyte, byte_bm); if (biscuit_roaring_is_empty(multibyte)) { biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } }
            }
            if (first_char) { result = multibyte; first_char = false; }
            else { biscuit_roaring_and_inplace(result, multibyte); biscuit_roaring_free(multibyte); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += char_len;
        char_pos++;
        } /* end char_len block */
    }

    {
        int pattern_char_count = biscuit_part_char_count(part, part_byte_len);
        if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_length_ge_lower(index, idx, start_pos + pattern_char_count); }
        else { len_filter = biscuit_get_length_ge_lower(index, idx, start_pos + pattern_char_count); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } }
    }

    return result ? result : biscuit_roaring_create();
}

static RoaringBitmap *
biscuit_match_part_at_end_ilike(Relation index, BiscuitIndex *idx, const char *part, int part_byte_len)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int            concrete_chars       = 0;
    bool           first_char           = true;
    int            pattern_char_count   = biscuit_part_char_count(part, part_byte_len);
    int            part_byte_pos        = 0;
    int            char_offset_from_end = 0;

    while (part_byte_pos < part_byte_len)
    {
        unsigned char first_byte = (unsigned char) part[part_byte_pos];
        if (first_byte == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal_byte;
            RoaringBitmap *char_bm;
            int neg_pos;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            literal_byte = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            neg_pos = -(pattern_char_count - char_offset_from_end);
            char_bm = biscuit_get_neg_bitmap_lower(index, idx, literal_byte, neg_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2;
            char_offset_from_end++;
            continue;
        }
        if (first_byte == '_') { part_byte_pos++; char_offset_from_end++; continue; }
        {
        int char_len = biscuit_utf8_char_length(first_byte);
        int neg_pos;
        if (part_byte_pos + char_len > part_byte_len) char_len = part_byte_len - part_byte_pos;
        concrete_chars++;
        neg_pos = -(pattern_char_count - char_offset_from_end);
        if (char_len == 1) {
            RoaringBitmap *char_bm = biscuit_get_neg_bitmap_lower(index, idx, first_byte, neg_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *multibyte = NULL; int b;
            for (b = 0; b < char_len; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *byte_bm = biscuit_get_neg_bitmap_lower(index, idx, bv, neg_pos); if (!byte_bm) { if (multibyte) biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) multibyte = biscuit_roaring_copy(byte_bm); else { biscuit_roaring_and_inplace(multibyte, byte_bm); if (biscuit_roaring_is_empty(multibyte)) { biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = multibyte; first_char = false; }
            else { biscuit_roaring_and_inplace(result, multibyte); biscuit_roaring_free(multibyte); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += char_len;
        char_offset_from_end++;
        } /* end char_len block */
    }

    if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_length_ge_lower(index, idx, pattern_char_count); }
    else { len_filter = biscuit_get_length_ge_lower(index, idx, pattern_char_count); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } }

    return result ? result : biscuit_roaring_create();
}


/* ================================================================
 * SECTION 5 – Iterative windowed matching (single-column)
 *
 * Replaces the former recursive implementations to eliminate both
 * stack-overflow risk and the O(positions^parts) allocation pattern
 * that caused OOM crashes on long-text columns.
 *
 * Algorithm: explicit work-stack of (part_idx, min_pos, candidates)
 * frames.  Children are pushed instead of recursed into.  Each frame
 * owns its candidates bitmap and frees it when the frame is done.
 * This bounds live bitmap count to O(part_count * max_len) instead of
 * the former exponential.
 * ================================================================ */

typedef struct WMFrame {
    int            part_idx;
    int            min_pos;
    RoaringBitmap *candidates;
} WMFrame;

#define WM_MAX_STACK 512

/* ================================================================
 * SCALAR FALLBACK for the windowed sweep's stack overflow.
 * ================================================================
 *
 * biscuit_recursive_windowed_match*() explores, position by position, every
 * way the pattern's literal parts could line up in a candidate row, via a
 * bounded (WM_MAX_STACK) explicit stack. On an adversarial or highly
 * repetitive string -- e.g. repeat('ab', 500) against '%a%b%c%' -- a single
 * part ("a") can match at hundreds of positions in hundreds of rows
 * simultaneously, and the stack fills before every combination has been
 * tried.
 *
 * The code used to treat a full stack as "this candidate matches": any row
 * that reached here was OR'd straight into `result`, without ever checking
 * whether the *remaining* parts (the ones that would have been explored had
 * there been stack room) actually occur in the row's text. For
 * repeat('ab',500) against '%a%b%c%', a row that matched "a" then "b" got
 * treated as matching '%a%b%c%' even though the row has no 'c' at all. That
 * is a genuine false positive, not a lossy approximation the executor
 * cleans up afterward: BISCUIT's amgetbitmap() always reports
 * recheck = false (see biscuit_scan.c), so PostgreSQL never re-evaluates
 * the qual against the heap tuple, and the wrong row reaches the client. A
 * NOT LIKE query over the same pattern shows the exact mirror image: the
 * same row is wrongly excluded from the complement.
 *
 * The fix: when the stack can't hold another frame, don't guess. Fall back
 * to a direct, scalar verification of the remaining parts against this
 * frame's actual candidate rows, and only add a row to `result` if it
 * genuinely matches. Greedy leftmost matching is correct (not merely a
 * heuristic) for this: for literal parts separated by unanchored '%',
 * matching each part as early as possible after the previous one never
 * forecloses a later part's match, so if any valid assignment of positions
 * exists, the greedy one is among them. The only spot that needs special
 * handling is the final part when the pattern does not end in '%', which
 * must land exactly at the end of the string rather than merely somewhere
 * in it -- handled the same way biscuit_match_part_at_end() is used
 * elsewhere in this file.
 */

/*
 * Find the leftmost occurrence of `part` in `hay` at or after character
 * position from_char_pos. Returns the character position immediately past
 * the match (so the caller can resume searching for the next part from
 * there), or -1 if `part` does not occur anywhere from that position on.
 */
static int
wm_find_part_from_char(const char *hay, int hay_byte_len, int hay_char_len,
                        const char *part, int part_byte_len, int part_char_len,
                        int from_char_pos)
{
    int cp;
    int byte_off;

    if (part_char_len == 0)
        return from_char_pos;

    if (from_char_pos < 0 || from_char_pos > hay_char_len - part_char_len)
        return -1;

    byte_off = biscuit_utf8_char_to_byte_offset(hay, hay_byte_len, from_char_pos);
    if (byte_off < 0)
        return -1;

    for (cp = from_char_pos; cp <= hay_char_len - part_char_len; cp++)
    {
        if (biscuit_part_match_substr(hay, hay_byte_len, byte_off, part, part_byte_len))
            return cp + part_char_len;
        if (byte_off >= hay_byte_len)
            break;
        byte_off += biscuit_utf8_char_length((unsigned char) hay[byte_off]);
    }
    return -1;
}

/*
 * Verify parts[start_part_idx .. part_count-1] all occur, in order, in
 * `hay`, with the first one starting no earlier than character position
 * start_char_pos. Mirrors exactly what the windowed sweep would have
 * proven bitmap-side had it had stack room to keep exploring.
 */
static bool
wm_scalar_verify_remaining(const char *hay, int hay_byte_len,
                            const char **parts, int *part_byte_lens,
                            int part_count, int start_part_idx,
                            int start_char_pos, bool ends_percent)
{
    int hay_char_len = biscuit_utf8_char_count(hay, hay_byte_len);
    int cur_pos       = start_char_pos;
    int pidx;

    for (pidx = start_part_idx; pidx < part_count; pidx++)
    {
        int  part_cl = biscuit_part_char_count(parts[pidx], part_byte_lens[pidx]);
        bool is_last  = (pidx == part_count - 1);

        if (is_last && !ends_percent)
        {
            int anchor_pos = hay_char_len - part_cl;
            int byte_off;

            if (anchor_pos < cur_pos)
                return false;
            byte_off = biscuit_utf8_char_to_byte_offset(hay, hay_byte_len, anchor_pos);
            if (byte_off < 0)
                return false;
            if (!biscuit_part_match_substr(hay, hay_byte_len, byte_off,
                                            parts[pidx], part_byte_lens[pidx]))
                return false;
            cur_pos = hay_char_len;
        }
        else
        {
            int next_pos = wm_find_part_from_char(hay, hay_byte_len, hay_char_len,
                                                   parts[pidx], part_byte_lens[pidx],
                                                   part_cl, cur_pos);
            if (next_pos < 0)
                return false;
            cur_pos = next_pos;
        }
    }
    return true;
}

/*
 * Fetch-and-verify: for every row in `nc`, look up its text via `fetch` and
 * add it to `result` only if wm_scalar_verify_remaining() confirms the
 * remaining parts actually occur. `fetch` returning NULL (no cached text,
 * e.g. a NULL column value slipping through as a candidate) is treated as
 * "does not match", matching how the bitmap-side helpers already behave
 * for slots with no data.
 */
typedef const char *(*WMHayFetch) (void *fetch_ctx, uint32_t slot);

static void
wm_verify_and_add(RoaringBitmap *result, RoaringBitmap *nc,
                   WMHayFetch fetch, void *fetch_ctx,
                   const char **parts, int *part_byte_lens, int part_count,
                   int start_part_idx, int start_char_pos, bool ends_percent)
{
#ifdef HAVE_ROARING
    roaring_uint32_iterator_t *iter = roaring_iterator_create(nc);
    while (iter->has_value)
    {
        uint32_t    rec = iter->current_value;
        const char *hay = fetch(fetch_ctx, rec);
        if (hay && wm_scalar_verify_remaining(hay, strlen(hay), parts, part_byte_lens,
                                               part_count, start_part_idx,
                                               start_char_pos, ends_percent))
            biscuit_roaring_add(result, rec);
        roaring_uint32_iterator_advance(iter);
    }
    roaring_uint32_iterator_free(iter);
#else
    {
        uint64_t  cnt;
        uint32_t *indices = biscuit_roaring_to_array(nc, &cnt);
        if (indices)
        {
            int j;
            for (j = 0; j < (int) cnt; j++)
            {
                uint32_t    rec = indices[j];
                const char *hay = fetch(fetch_ctx, rec);
                if (hay && wm_scalar_verify_remaining(hay, strlen(hay), parts, part_byte_lens,
                                                       part_count, start_part_idx,
                                                       start_char_pos, ends_percent))
                    biscuit_roaring_add(result, rec);
            }
            pfree(indices);
        }
    }
#endif
}

/* Hay-fetch callbacks: one per data source the four windowed-match
 * variants below draw row text from. */
static const char *
wm_hay_fetch_legacy(void *ctx, uint32_t slot)
{
    BiscuitIndex *idx = (BiscuitIndex *) ctx;
    return (slot < (uint32_t) idx->num_records) ? idx->data_cache[slot] : NULL;
}

static const char *
wm_hay_fetch_legacy_lower(void *ctx, uint32_t slot)
{
    BiscuitIndex *idx = (BiscuitIndex *) ctx;
    return (slot < (uint32_t) idx->num_records) ? idx->data_cache_lower[slot] : NULL;
}

typedef struct WMColFetchCtx
{
    BiscuitIndex *idx;
    int           col_idx;
} WMColFetchCtx;

static const char *
wm_hay_fetch_col(void *ctx, uint32_t slot)
{
    WMColFetchCtx *c = (WMColFetchCtx *) ctx;
    if (slot >= (uint32_t) c->idx->num_records)
        return NULL;
    return c->idx->column_data_cache[c->col_idx][slot];
}

static const char *
wm_hay_fetch_col_lower(void *ctx, uint32_t slot)
{
    WMColFetchCtx *c = (WMColFetchCtx *) ctx;
    if (slot >= (uint32_t) c->idx->num_records || !c->idx->column_data_cache_lower)
        return NULL;
    return c->idx->column_data_cache_lower[c->col_idx][slot];
}

static void
biscuit_recursive_windowed_match(
    Relation index, RoaringBitmap *result, BiscuitIndex *idx,
    const char **parts, int *part_lens, int part_count,
    bool ends_percent, int part_idx, int min_pos,
    RoaringBitmap *current_candidates, int max_len)
{
    int i;
    WMFrame *stack     = (WMFrame *) palloc(WM_MAX_STACK * sizeof(WMFrame));
    int      stack_top = 0;
    /* FIX 5 (position-loop half): once `result` already contains every
     * candidate row, no further position/part probe can add anything
     * back -- stop draining the stack instead of continuing to probe. */
    uint64_t total_candidates = biscuit_roaring_count(current_candidates);
    /*
     * Upper bound on result's cardinality, cheap to maintain (just an
     * addition) vs. calling biscuit_roaring_count(result) -- itself
     * O(containers), not O(1) -- on every single OR in a wide sweep.
     * cands frames can overlap (the same row can satisfy the pattern at
     * more than one position), so this sum can overcount the true
     * result cardinality; that's fine, it's only used to skip the exact
     * check below until saturation is actually plausible.
     */
    uint64_t result_count_upper_bound = 0;
    int *pcl = (int *) palloc(part_count * sizeof(int));
    int *suf = (int *) palloc(part_count * sizeof(int));
    for (i = 0; i < part_count; i++)
        pcl[i] = biscuit_part_char_count(parts[i], part_lens[i]);
    suf[part_count - 1] = 0;
    for (i = part_count - 2; i >= 0; i--)
        suf[i] = suf[i + 1] + pcl[i + 1];

    stack[stack_top].part_idx   = part_idx;
    stack[stack_top].min_pos    = min_pos;
    stack[stack_top].candidates = biscuit_roaring_copy(current_candidates);
    stack_top++;

    while (stack_top > 0)
    {
        int            pidx;
        int            mpos;
        RoaringBitmap *cands;
        int            part_cl;
        int            suf_len;
        int            max_pos;
        stack_top--;
        pidx  = stack[stack_top].part_idx;
        mpos  = stack[stack_top].min_pos;
        cands = stack[stack_top].candidates;

        if (pidx >= part_count)
        {
            biscuit_roaring_or_inplace(result, cands);
            result_count_upper_bound += biscuit_roaring_count(cands);
            biscuit_roaring_free(cands);
            if (total_candidates > 0 && result_count_upper_bound >= total_candidates &&
                biscuit_roaring_count(result) == total_candidates)
            {
#ifdef USE_ASSERT_CHECKING
                /*
                 * Cardinality equality only implies set equality under
                 * result ⊆ current_candidates. That containment holds
                 * because every frame's candidate set is derived from
                 * current_candidates by intersection only (never OR'd
                 * with anything external) before landing in `result` --
                 * but that's an invariant of the code around this loop,
                 * not something this check can see, so verify it
                 * explicitly rather than trusting the count alone.
                 */
                {
                    RoaringBitmap *escapees = biscuit_roaring_copy(result);
                    biscuit_roaring_andnot_inplace(escapees, current_candidates);
                    Assert(biscuit_roaring_is_empty(escapees));
                    biscuit_roaring_free(escapees);
                }
#endif
                while (stack_top > 0)
                {
                    stack_top--;
                    biscuit_roaring_free(stack[stack_top].candidates);
                }
                break;
            }
            continue;
        }

        part_cl = pcl[pidx];
        suf_len = suf[pidx];
        max_pos = max_len - part_cl - suf_len;

        if (pidx == part_count - 1 && !ends_percent)
        {
            RoaringBitmap *em = biscuit_match_part_at_end(index, idx, parts[pidx], part_lens[pidx]);
            if (em)
            {
                int mrl;
                RoaringBitmap *lc;
                biscuit_roaring_and_inplace(em, cands);
                mrl = mpos + part_cl;
                lc = biscuit_get_length_ge(index, idx, mrl);
                if (lc) { biscuit_roaring_and_inplace(em, lc); biscuit_roaring_free(lc); }
                if (!biscuit_roaring_is_empty(em))
                    biscuit_roaring_or_inplace(result, em);
                biscuit_roaring_free(em);
            }
            biscuit_roaring_free(cands);
            continue;
        }

        if (mpos > max_pos) { biscuit_roaring_free(cands); continue; }

        for (i = mpos; i <= max_pos; i++)
        {
            int pos;
            RoaringBitmap *pm;
            RoaringBitmap *nc;
            pos = i;
            pm = biscuit_match_part_at_pos(index, idx, parts[pidx],
                                           part_lens[pidx], pos);
            if (!pm)
                continue;
            nc = biscuit_roaring_copy(cands);
            biscuit_roaring_and_inplace(nc, pm);
            biscuit_roaring_free(pm);
            if (biscuit_roaring_is_empty(nc))
            {
                biscuit_roaring_free(nc);
                continue;
            }
            if (stack_top >= WM_MAX_STACK)
            {
                /* See "SCALAR FALLBACK" above biscuit_recursive_windowed_match():
                 * the stack has no room to keep exploring this candidate
                 * set bitmap-side, so verify it directly against each
                 * row's text instead of assuming a match. */
                wm_verify_and_add(result, nc, wm_hay_fetch_legacy, idx,
                                   parts, part_lens, part_count,
                                   pidx + 1, pos + part_cl, ends_percent);
                biscuit_roaring_free(nc);
                continue;
            }
            stack[stack_top].part_idx   = pidx + 1;
            stack[stack_top].min_pos    = pos + part_cl;
            stack[stack_top].candidates = nc;
            stack_top++;
        }
        biscuit_roaring_free(cands);
    }

    pfree(stack);
    pfree(pcl);
    pfree(suf);
}

static void
biscuit_recursive_windowed_match_ilike(
    Relation index, RoaringBitmap *result, BiscuitIndex *idx,
    const char **parts, int *part_lens, int part_count,
    bool ends_percent, int part_idx, int min_pos,
    RoaringBitmap *current_candidates, int max_len)
{
    int i;
    WMFrame *stack     = (WMFrame *) palloc(WM_MAX_STACK * sizeof(WMFrame));
    int      stack_top = 0;
    /* FIX 5 (position-loop half): once `result` already contains every
     * candidate row, no further position/part probe can add anything
     * back -- stop draining the stack instead of continuing to probe. */
    uint64_t total_candidates = biscuit_roaring_count(current_candidates);
    /*
     * Upper bound on result's cardinality, cheap to maintain (just an
     * addition) vs. calling biscuit_roaring_count(result) -- itself
     * O(containers), not O(1) -- on every single OR in a wide sweep.
     * cands frames can overlap (the same row can satisfy the pattern at
     * more than one position), so this sum can overcount the true
     * result cardinality; that's fine, it's only used to skip the exact
     * check below until saturation is actually plausible.
     */
    uint64_t result_count_upper_bound = 0;
    int *pcl = (int *) palloc(part_count * sizeof(int));
    int *suf = (int *) palloc(part_count * sizeof(int));
    for (i = 0; i < part_count; i++)
        pcl[i] = biscuit_part_char_count(parts[i], part_lens[i]);
    suf[part_count - 1] = 0;
    for (i = part_count - 2; i >= 0; i--)
        suf[i] = suf[i + 1] + pcl[i + 1];

    stack[stack_top].part_idx   = part_idx;
    stack[stack_top].min_pos    = min_pos;
    stack[stack_top].candidates = biscuit_roaring_copy(current_candidates);
    stack_top++;

    while (stack_top > 0)
    {
        int            pidx;
        int            mpos;
        RoaringBitmap *cands;
        int            part_cl;
        int            suf_len;
        int            max_pos;
        stack_top--;
        pidx  = stack[stack_top].part_idx;
        mpos  = stack[stack_top].min_pos;
        cands = stack[stack_top].candidates;

        if (pidx >= part_count)
        {
            biscuit_roaring_or_inplace(result, cands);
            result_count_upper_bound += biscuit_roaring_count(cands);
            biscuit_roaring_free(cands);
            if (total_candidates > 0 && result_count_upper_bound >= total_candidates &&
                biscuit_roaring_count(result) == total_candidates)
            {
#ifdef USE_ASSERT_CHECKING
                /*
                 * Cardinality equality only implies set equality under
                 * result ⊆ current_candidates. That containment holds
                 * because every frame's candidate set is derived from
                 * current_candidates by intersection only (never OR'd
                 * with anything external) before landing in `result` --
                 * but that's an invariant of the code around this loop,
                 * not something this check can see, so verify it
                 * explicitly rather than trusting the count alone.
                 */
                {
                    RoaringBitmap *escapees = biscuit_roaring_copy(result);
                    biscuit_roaring_andnot_inplace(escapees, current_candidates);
                    Assert(biscuit_roaring_is_empty(escapees));
                    biscuit_roaring_free(escapees);
                }
#endif
                while (stack_top > 0)
                {
                    stack_top--;
                    biscuit_roaring_free(stack[stack_top].candidates);
                }
                break;
            }
            continue;
        }

        part_cl = pcl[pidx];
        suf_len = suf[pidx];
        max_pos = max_len - part_cl - suf_len;

        if (pidx == part_count - 1 && !ends_percent)
        {
            RoaringBitmap *em = biscuit_match_part_at_end_ilike(index, idx, parts[pidx], part_lens[pidx]);
            if (em)
            {
                int mrl;
                RoaringBitmap *lc;
                biscuit_roaring_and_inplace(em, cands);
                mrl = mpos + part_cl;
                lc = biscuit_get_length_ge_lower(index, idx, mrl);
                if (lc) { biscuit_roaring_and_inplace(em, lc); biscuit_roaring_free(lc); }
                if (!biscuit_roaring_is_empty(em))
                    biscuit_roaring_or_inplace(result, em);
                biscuit_roaring_free(em);
            }
            biscuit_roaring_free(cands);
            continue;
        }

        if (mpos > max_pos) { biscuit_roaring_free(cands); continue; }

        for (i = mpos; i <= max_pos; i++)
        {
            int pos;
            RoaringBitmap *pm;
            RoaringBitmap *nc;
            pos = i;
            pm = biscuit_match_part_at_pos_ilike(index, idx, parts[pidx], part_lens[pidx], pos);
            if (!pm) continue;
            nc = biscuit_roaring_copy(cands);
            biscuit_roaring_and_inplace(nc, pm);
            biscuit_roaring_free(pm);
            if (biscuit_roaring_is_empty(nc)) { biscuit_roaring_free(nc); continue; }
            if (stack_top >= WM_MAX_STACK)
            {
                /* See "SCALAR FALLBACK" above biscuit_recursive_windowed_match(). */
                wm_verify_and_add(result, nc, wm_hay_fetch_legacy_lower, idx,
                                   parts, part_lens, part_count,
                                   pidx + 1, pos + part_cl, ends_percent);
                biscuit_roaring_free(nc);
                continue;
            }
            stack[stack_top].part_idx   = pidx + 1;
            stack[stack_top].min_pos    = pos + part_cl;
            stack[stack_top].candidates = nc;
            stack_top++;
        }
        biscuit_roaring_free(cands);
    }

    pfree(stack);
    pfree(pcl);
    pfree(suf);
}

/* ================================================================
 * SECTION 6 – Multi-column part matching helpers
 * ================================================================ */

static RoaringBitmap *
biscuit_match_col_part_at_pos(Relation index, ColumnIndex *col, int col_idx,
                              const char *part,
                              int part_byte_len, int start_pos)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int part_byte_pos = 0, char_pos = start_pos, concrete_chars = 0;
    bool first_char = true;

    while (part_byte_pos < part_byte_len) {
        unsigned char fb = (unsigned char) part[part_byte_pos];
        if (fb == (unsigned char) BISCUIT_LITERAL_ESC) {
            unsigned char lb; RoaringBitmap *bm;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            lb = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            bm = biscuit_get_col_pos_bitmap(index, col, col_idx, lb, char_pos);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2; char_pos++; continue;
        }
        if (fb == '_') { part_byte_pos++; char_pos++; continue; }
        {
        int cl = biscuit_utf8_char_length(fb);
        if (part_byte_pos + cl > part_byte_len) cl = part_byte_len - part_byte_pos;
        concrete_chars++;
        if (cl == 1) {
            RoaringBitmap *bm = biscuit_get_col_pos_bitmap(index, col, col_idx, fb, char_pos);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *mb = NULL; int b;
            for (b = 0; b < cl; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *bm = biscuit_get_col_pos_bitmap(index, col, col_idx, bv, char_pos); if (!bm) { if (mb) biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) mb = biscuit_roaring_copy(bm); else { biscuit_roaring_and_inplace(mb, bm); if (biscuit_roaring_is_empty(mb)) { biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = mb; first_char = false; } else { biscuit_roaring_and_inplace(result, mb); biscuit_roaring_free(mb); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += cl; char_pos++;
        } /* end cl block */
    }

    { int pcc = biscuit_part_char_count(part, part_byte_len);
      if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_col_length_ge(index, col, col_idx, start_pos + pcc); }
      else { len_filter = biscuit_get_col_length_ge(index, col, col_idx, start_pos + pcc); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } } }
    return result ? result : biscuit_roaring_create();
}

static RoaringBitmap *
biscuit_match_col_part_at_end(Relation index, ColumnIndex *col, int col_idx,
                              const char *part, int part_byte_len)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int concrete_chars = 0, pcc = biscuit_part_char_count(part, part_byte_len);
    int part_byte_pos = 0, cfe = 0;
    bool first_char = true;

    while (part_byte_pos < part_byte_len) {
        unsigned char fb = (unsigned char) part[part_byte_pos];
        if (fb == (unsigned char) BISCUIT_LITERAL_ESC) {
            unsigned char lb; RoaringBitmap *bm; int neg;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            lb = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            neg = -(pcc - cfe);
            bm = biscuit_get_col_neg_bitmap(index, col, col_idx, lb, neg);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2; cfe++; continue;
        }
        if (fb == '_') { part_byte_pos++; cfe++; continue; }
        {
        int cl = biscuit_utf8_char_length(fb);
        int neg;
        if (part_byte_pos + cl > part_byte_len) cl = part_byte_len - part_byte_pos;
        concrete_chars++;
        neg = -(pcc - cfe);
        if (cl == 1) {
            RoaringBitmap *bm = biscuit_get_col_neg_bitmap(index, col, col_idx, fb, neg);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *mb = NULL; int b;
            for (b = 0; b < cl; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *bm = biscuit_get_col_neg_bitmap(index, col, col_idx, bv, neg); if (!bm) { if (mb) biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) mb = biscuit_roaring_copy(bm); else { biscuit_roaring_and_inplace(mb, bm); if (biscuit_roaring_is_empty(mb)) { biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = mb; first_char = false; } else { biscuit_roaring_and_inplace(result, mb); biscuit_roaring_free(mb); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += cl; cfe++;
        } /* end cl block */
    }

    if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_col_length_ge(index, col, col_idx, pcc); }
    else { len_filter = biscuit_get_col_length_ge(index, col, col_idx, pcc); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } }
    return result ? result : biscuit_roaring_create();
}

/* Case-insensitive multi-column variants */
static RoaringBitmap *
biscuit_match_col_part_at_pos_ilike(Relation index, ColumnIndex *col, int col_idx,
                                    const char *part,
                                    int part_byte_len, int start_pos)
{
    /* Identical to biscuit_match_col_part_at_pos but uses _lower accessors */
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int part_byte_pos = 0, char_pos = start_pos, concrete_chars = 0;
    bool first_char = true;

    while (part_byte_pos < part_byte_len) {
        unsigned char fb = (unsigned char) part[part_byte_pos];
        if (fb == (unsigned char) BISCUIT_LITERAL_ESC) {
            unsigned char lb; RoaringBitmap *bm;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            lb = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            bm = biscuit_get_col_pos_bitmap_lower(index, col, col_idx, lb, char_pos);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2; char_pos++; continue;
        }
        if (fb == '_') { part_byte_pos++; char_pos++; continue; }
        {
        int cl = biscuit_utf8_char_length(fb);
        if (part_byte_pos + cl > part_byte_len) cl = part_byte_len - part_byte_pos;
        concrete_chars++;
        if (cl == 1) {
            RoaringBitmap *bm = biscuit_get_col_pos_bitmap_lower(index, col, col_idx, fb, char_pos);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *mb = NULL; int b;
            for (b = 0; b < cl; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *bm = biscuit_get_col_pos_bitmap_lower(index, col, col_idx, bv, char_pos); if (!bm) { if (mb) biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) mb = biscuit_roaring_copy(bm); else { biscuit_roaring_and_inplace(mb, bm); if (biscuit_roaring_is_empty(mb)) { biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = mb; first_char = false; } else { biscuit_roaring_and_inplace(result, mb); biscuit_roaring_free(mb); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += cl; char_pos++;
        } /* end cl block */
    }

    { int pcc = biscuit_part_char_count(part, part_byte_len);
      if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_col_length_ge_lower(index, col, col_idx, start_pos + pcc); }
      else { len_filter = biscuit_get_col_length_ge_lower(index, col, col_idx, start_pos + pcc); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } } }
    return result ? result : biscuit_roaring_create();
}

static RoaringBitmap *
biscuit_match_col_part_at_end_ilike(Relation index, ColumnIndex *col, int col_idx,
                                    const char *part, int part_byte_len)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int concrete_chars = 0, pcc = biscuit_part_char_count(part, part_byte_len);
    int part_byte_pos = 0, cfe = 0;
    bool first_char = true;

    while (part_byte_pos < part_byte_len) {
        unsigned char fb = (unsigned char) part[part_byte_pos];
        if (fb == (unsigned char) BISCUIT_LITERAL_ESC) {
            unsigned char lb; RoaringBitmap *bm; int neg;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            lb = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            neg = -(pcc - cfe);
            bm = biscuit_get_col_neg_bitmap_lower(index, col, col_idx, lb, neg);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2; cfe++; continue;
        }
        if (fb == '_') { part_byte_pos++; cfe++; continue; }
        {
        int cl = biscuit_utf8_char_length(fb);
        int neg;
        if (part_byte_pos + cl > part_byte_len) cl = part_byte_len - part_byte_pos;
        concrete_chars++;
        neg = -(pcc - cfe);
        if (cl == 1) {
            RoaringBitmap *bm = biscuit_get_col_neg_bitmap_lower(index, col, col_idx, fb, neg);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *mb = NULL; int b;
            for (b = 0; b < cl; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *bm = biscuit_get_col_neg_bitmap_lower(index, col, col_idx, bv, neg); if (!bm) { if (mb) biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) mb = biscuit_roaring_copy(bm); else { biscuit_roaring_and_inplace(mb, bm); if (biscuit_roaring_is_empty(mb)) { biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = mb; first_char = false; } else { biscuit_roaring_and_inplace(result, mb); biscuit_roaring_free(mb); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += cl; cfe++;
        } /* end cl block */
    }

    if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_col_length_ge_lower(index, col, col_idx, pcc); }
    else { len_filter = biscuit_get_col_length_ge_lower(index, col, col_idx, pcc); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } }
    return result ? result : biscuit_roaring_create();
}


/* ================================================================
 * SECTION 7 – Iterative windowed matching (multi-column)
 *
 * Same stack-based approach as Section 5 but operating on a
 * ColumnIndex pointer and using the col-specific bitmap accessors.
 * ================================================================ */

static void
biscuit_recursive_windowed_match_col(
    Relation index, RoaringBitmap *result, BiscuitIndex *idx, ColumnIndex *col, int col_idx,
    const char **parts, int *part_byte_lens, int part_count,
    bool ends_percent, int part_idx, int min_char_pos,
    RoaringBitmap *current_candidates, int max_char_len)
{
    int i;
    WMFrame *stack     = (WMFrame *) palloc(WM_MAX_STACK * sizeof(WMFrame));
    int      stack_top = 0;
    /* FIX 5 (position-loop half): once `result` already contains every
     * candidate row, no further position/part probe can add anything
     * back -- stop draining the stack instead of continuing to probe. */
    uint64_t total_candidates = biscuit_roaring_count(current_candidates);
    /*
     * Upper bound on result's cardinality, cheap to maintain (just an
     * addition) vs. calling biscuit_roaring_count(result) -- itself
     * O(containers), not O(1) -- on every single OR in a wide sweep.
     * cands frames can overlap (the same row can satisfy the pattern at
     * more than one position), so this sum can overcount the true
     * result cardinality; that's fine, it's only used to skip the exact
     * check below until saturation is actually plausible.
     */
    uint64_t result_count_upper_bound = 0;
    int *pcl = (int *) palloc(part_count * sizeof(int));
    int *suf = (int *) palloc(part_count * sizeof(int));
    for (i = 0; i < part_count; i++)
        pcl[i] = biscuit_part_char_count(parts[i], part_byte_lens[i]);
    suf[part_count - 1] = 0;
    for (i = part_count - 2; i >= 0; i--)
        suf[i] = suf[i + 1] + pcl[i + 1];

    stack[stack_top].part_idx   = part_idx;
    stack[stack_top].min_pos    = min_char_pos;
    stack[stack_top].candidates = biscuit_roaring_copy(current_candidates);
    stack_top++;

    while (stack_top > 0)
    {
        int            pidx;
        int            mpos;
        RoaringBitmap *cands;
        int            part_cl;
        int            suf_len;
        int            max_pos;
        stack_top--;
        pidx  = stack[stack_top].part_idx;
        mpos  = stack[stack_top].min_pos;
        cands = stack[stack_top].candidates;

        if (pidx >= part_count)
        {
            biscuit_roaring_or_inplace(result, cands);
            result_count_upper_bound += biscuit_roaring_count(cands);
            biscuit_roaring_free(cands);
            if (total_candidates > 0 && result_count_upper_bound >= total_candidates &&
                biscuit_roaring_count(result) == total_candidates)
            {
#ifdef USE_ASSERT_CHECKING
                /*
                 * Cardinality equality only implies set equality under
                 * result ⊆ current_candidates. That containment holds
                 * because every frame's candidate set is derived from
                 * current_candidates by intersection only (never OR'd
                 * with anything external) before landing in `result` --
                 * but that's an invariant of the code around this loop,
                 * not something this check can see, so verify it
                 * explicitly rather than trusting the count alone.
                 */
                {
                    RoaringBitmap *escapees = biscuit_roaring_copy(result);
                    biscuit_roaring_andnot_inplace(escapees, current_candidates);
                    Assert(biscuit_roaring_is_empty(escapees));
                    biscuit_roaring_free(escapees);
                }
#endif
                while (stack_top > 0)
                {
                    stack_top--;
                    biscuit_roaring_free(stack[stack_top].candidates);
                }
                break;
            }
            continue;
        }

        part_cl = pcl[pidx];
        suf_len = suf[pidx];
        max_pos = max_char_len - part_cl - suf_len;

        if (pidx == part_count - 1 && !ends_percent)
        {
            RoaringBitmap *em = biscuit_match_col_part_at_end(index, col, col_idx, parts[pidx], part_byte_lens[pidx]);
            if (em)
            {
                int mrl;
                RoaringBitmap *lc;
                biscuit_roaring_and_inplace(em, cands);
                mrl = mpos + part_cl;
                lc = biscuit_get_col_length_ge(index, col, col_idx, mrl);
                if (lc) { biscuit_roaring_and_inplace(em, lc); biscuit_roaring_free(lc); }
                if (!biscuit_roaring_is_empty(em))
                    biscuit_roaring_or_inplace(result, em);
                biscuit_roaring_free(em);
            }
            biscuit_roaring_free(cands);
            continue;
        }

        if (mpos > max_pos) { biscuit_roaring_free(cands); continue; }

        for (i = mpos; i <= max_pos; i++)
        {
            int pos;
            RoaringBitmap *pm;
            RoaringBitmap *nc;
            pos = i;
            pm = biscuit_match_col_part_at_pos(index, col, col_idx, parts[pidx], part_byte_lens[pidx], pos);
            if (!pm) continue;
            nc = biscuit_roaring_copy(cands);
            biscuit_roaring_and_inplace(nc, pm);
            biscuit_roaring_free(pm);
            if (biscuit_roaring_is_empty(nc)) { biscuit_roaring_free(nc); continue; }
            if (stack_top >= WM_MAX_STACK)
            {
                /* See "SCALAR FALLBACK" above biscuit_recursive_windowed_match(). */
                WMColFetchCtx fctx;
                fctx.idx = idx;
                fctx.col_idx = col_idx;
                wm_verify_and_add(result, nc, wm_hay_fetch_col, &fctx,
                                   parts, part_byte_lens, part_count,
                                   pidx + 1, pos + part_cl, ends_percent);
                biscuit_roaring_free(nc);
                continue;
            }
            stack[stack_top].part_idx   = pidx + 1;
            stack[stack_top].min_pos    = pos + part_cl;
            stack[stack_top].candidates = nc;
            stack_top++;
        }
        biscuit_roaring_free(cands);
    }

    pfree(stack);
    pfree(pcl);
    pfree(suf);
}

static void
biscuit_recursive_windowed_match_col_ilike(
    Relation index, RoaringBitmap *result, BiscuitIndex *idx, ColumnIndex *col, int col_idx,
    const char **parts, int *part_byte_lens, int part_count,
    bool ends_percent, int part_idx, int min_char_pos,
    RoaringBitmap *current_candidates, int max_char_len)
{
    int i;
    WMFrame *stack     = (WMFrame *) palloc(WM_MAX_STACK * sizeof(WMFrame));
    int      stack_top = 0;
    /* FIX 5 (position-loop half): once `result` already contains every
     * candidate row, no further position/part probe can add anything
     * back -- stop draining the stack instead of continuing to probe. */
    uint64_t total_candidates = biscuit_roaring_count(current_candidates);
    /*
     * Upper bound on result's cardinality, cheap to maintain (just an
     * addition) vs. calling biscuit_roaring_count(result) -- itself
     * O(containers), not O(1) -- on every single OR in a wide sweep.
     * cands frames can overlap (the same row can satisfy the pattern at
     * more than one position), so this sum can overcount the true
     * result cardinality; that's fine, it's only used to skip the exact
     * check below until saturation is actually plausible.
     */
    uint64_t result_count_upper_bound = 0;
    int *pcl = (int *) palloc(part_count * sizeof(int));
    int *suf = (int *) palloc(part_count * sizeof(int));
    for (i = 0; i < part_count; i++)
        pcl[i] = biscuit_part_char_count(parts[i], part_byte_lens[i]);
    suf[part_count - 1] = 0;
    for (i = part_count - 2; i >= 0; i--)
        suf[i] = suf[i + 1] + pcl[i + 1];

    stack[stack_top].part_idx   = part_idx;
    stack[stack_top].min_pos    = min_char_pos;
    stack[stack_top].candidates = biscuit_roaring_copy(current_candidates);
    stack_top++;

    while (stack_top > 0)
    {
        int            pidx;
        int            mpos;
        RoaringBitmap *cands;
        int            part_cl;
        int            suf_len;
        int            max_pos;
        stack_top--;
        pidx  = stack[stack_top].part_idx;
        mpos  = stack[stack_top].min_pos;
        cands = stack[stack_top].candidates;

        if (pidx >= part_count)
        {
            biscuit_roaring_or_inplace(result, cands);
            result_count_upper_bound += biscuit_roaring_count(cands);
            biscuit_roaring_free(cands);
            if (total_candidates > 0 && result_count_upper_bound >= total_candidates &&
                biscuit_roaring_count(result) == total_candidates)
            {
#ifdef USE_ASSERT_CHECKING
                /*
                 * Cardinality equality only implies set equality under
                 * result ⊆ current_candidates. That containment holds
                 * because every frame's candidate set is derived from
                 * current_candidates by intersection only (never OR'd
                 * with anything external) before landing in `result` --
                 * but that's an invariant of the code around this loop,
                 * not something this check can see, so verify it
                 * explicitly rather than trusting the count alone.
                 */
                {
                    RoaringBitmap *escapees = biscuit_roaring_copy(result);
                    biscuit_roaring_andnot_inplace(escapees, current_candidates);
                    Assert(biscuit_roaring_is_empty(escapees));
                    biscuit_roaring_free(escapees);
                }
#endif
                while (stack_top > 0)
                {
                    stack_top--;
                    biscuit_roaring_free(stack[stack_top].candidates);
                }
                break;
            }
            continue;
        }

        part_cl = pcl[pidx];
        suf_len = suf[pidx];
        max_pos = max_char_len - part_cl - suf_len;

        if (pidx == part_count - 1 && !ends_percent)
        {
            RoaringBitmap *em = biscuit_match_col_part_at_end_ilike(index, col, col_idx, parts[pidx], part_byte_lens[pidx]);
            if (em)
            {
                int mrl;
                RoaringBitmap *lc;
                biscuit_roaring_and_inplace(em, cands);
                mrl = mpos + part_cl;
                lc = biscuit_get_col_length_ge_lower(index, col, col_idx, mrl);
                if (lc) { biscuit_roaring_and_inplace(em, lc); biscuit_roaring_free(lc); }
                if (!biscuit_roaring_is_empty(em))
                    biscuit_roaring_or_inplace(result, em);
                biscuit_roaring_free(em);
            }
            biscuit_roaring_free(cands);
            continue;
        }

        if (mpos > max_pos) { biscuit_roaring_free(cands); continue; }

        for (i = mpos; i <= max_pos; i++)
        {
            int pos;
            RoaringBitmap *pm;
            RoaringBitmap *nc;
            pos = i;
            pm = biscuit_match_col_part_at_pos_ilike(index, col, col_idx, parts[pidx], part_byte_lens[pidx], pos);
            if (!pm) continue;
            nc = biscuit_roaring_copy(cands);
            biscuit_roaring_and_inplace(nc, pm);
            biscuit_roaring_free(pm);
            if (biscuit_roaring_is_empty(nc)) { biscuit_roaring_free(nc); continue; }
            if (stack_top >= WM_MAX_STACK)
            {
                /* See "SCALAR FALLBACK" above biscuit_recursive_windowed_match(). */
                WMColFetchCtx fctx;
                fctx.idx = idx;
                fctx.col_idx = col_idx;
                wm_verify_and_add(result, nc, wm_hay_fetch_col_lower, &fctx,
                                   parts, part_byte_lens, part_count,
                                   pidx + 1, pos + part_cl, ends_percent);
                biscuit_roaring_free(nc);
                continue;
            }
            stack[stack_top].part_idx   = pidx + 1;
            stack[stack_top].min_pos    = pos + part_cl;
            stack[stack_top].candidates = nc;
            stack_top++;
        }
        biscuit_roaring_free(cands);
    }

    pfree(stack);
    pfree(pcl);
    pfree(suf);
}

/* ================================================================
 * SECTION 8 – Public query entry points (single-column)
 * ================================================================ */

/*
 * biscuit_query_pattern_masked
 *
 * Same as biscuit_query_pattern(), but accepts an optional candidate
 * mask -- the intersection of every cheaper scan key already evaluated
 * for this AND-conjunction (see biscuit_rescan() / biscuit_rescan_multicolumn()
 * in biscuit_scan.c). When mask is non-NULL:
 *
 *   - An empty mask short-circuits immediately (no cheap key survived,
 *     so this key can't add anything back).
 *   - The two expensive branches -- the char_cache+scalar-verify path
 *     for pure single-segment infix patterns ('%abc%'), and the
 *     multi-segment windowed sweep (biscuit_recursive_windowed_match)
 *     -- intersect their candidate set with mask *before* doing the
 *     expensive work, instead of computing the full-table result and
 *     relying on the caller to AND it down afterward. That's the whole
 *     fix: a 26-row mask means the scalar verify touches 26 strings and
 *     the windowed sweep's per-position bitmap ANDs touch a 26-row
 *     roaring container, not the full row set.
 *
 * biscuit_query_pattern() below is now a thin mask=NULL wrapper so every
 * existing caller keeps working unchanged.
 */
RoaringBitmap *
biscuit_query_pattern_masked(Relation index, BiscuitIndex *idx, const char *pattern,
                              const RoaringBitmap *mask)
{
    int            plen = strlen(pattern);
    ParsedPattern *parsed = NULL;
    int            min_len, i;
    RoaringBitmap *result = NULL;
    int            wildcard_count = 0, percent_count = 0;
    bool           only_wildcards = true;

    /* A mask restricts what this key can possibly add back; an empty
     * mask means every cheaper co-predicate already eliminated every
     * row, so there is nothing left to compute. */
    if (mask && biscuit_roaring_is_empty(mask))
        return biscuit_roaring_create();

    /*
     * Defensive gate: an index built with biscuit_ilike_ops never
     * populates the case-sensitive structures this function depends on.
     * The planner should never route a LIKE/NOT LIKE qual to such an
     * index (biscuit_ilike_ops's opfamily doesn't register those
     * operators), so reaching this with legacy_case_mode missing
     * BISCUIT_MODE_LIKE indicates a programming error, not a normal
     * query.
     */
    if (!(idx->legacy_case_mode & BISCUIT_MODE_LIKE))
        ereport(ERROR,
                (errmsg("biscuit: this index was not built with LIKE support"),
                 errhint("The index's opclass (biscuit_ilike_ops) only supports ILIKE/NOT ILIKE.")));

    if (plen == 0) {
        /* Empty pattern '' matches only records with an empty string value */
        RoaringBitmap *lb = (idx->length_bitmaps_legacy)
            ? biscuit_reconcile_pending(index, idx->length_bitmaps_legacy[0], -1, false,
                                          BISCUIT_DIR_KIND_LEN, -1, 0)
            : NULL;
        return lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
    }

    if (plen == 1 && pattern[0] == '%') {
        /* '%' matches every non-tombstoned, non-NULL record */
        RoaringBitmap *lgb = (idx->length_ge_bitmaps_legacy)
            ? biscuit_reconcile_pending(index, idx->length_ge_bitmaps_legacy[0], -1, false,
                                          BISCUIT_DIR_KIND_LEN_GE, -1, 0)
            : NULL;
        if (lgb)
            result = biscuit_roaring_copy(lgb);
        else {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                bool ts = false;
                #ifdef HAVE_ROARING
                ts = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
                #else
                { uint32_t bl = i >> 6, bt = i & 63; ts = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
                #endif
                if (!ts && idx->data_cache[i]) biscuit_roaring_add(result, i);
            }
        }
        return result;
    }

    for (i = 0; i < plen; i++) {
        if (pattern[i] == '%') percent_count++;
        else if (pattern[i] == '_') wildcard_count++;
        else { only_wildcards = false; break; }
    }

    if (only_wildcards) {
        if (percent_count > 0) return biscuit_get_length_ge(index, idx, wildcard_count);
        if (wildcard_count < idx->max_length_legacy && idx->length_bitmaps_legacy[wildcard_count])
        {
            RoaringBitmap *lb = biscuit_reconcile_pending(index, idx->length_bitmaps_legacy[wildcard_count],
                                                            -1, false, BISCUIT_DIR_KIND_LEN, -1, wildcard_count);
            return lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
        }
        return biscuit_roaring_create();
    }

    PG_TRY();
    {
        parsed = biscuit_parse_pattern(pattern);

        if (parsed->part_count == 0) {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                bool ts = false;
                #ifdef HAVE_ROARING
                ts = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
                #else
                { uint32_t bl = i >> 6, bt = i & 63; ts = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
                #endif
                if (!ts) biscuit_roaring_add(result, i);
            }
            biscuit_free_parsed_pattern(parsed);
            parsed = NULL; /* FIX 5: prevent double-free in PG_CATCH */
            return result;
        }

        min_len = 0;
        for (i = 0; i < parsed->part_count; i++)
            min_len += parsed->part_lens[i];

        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                result = biscuit_match_part_at_pos(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (result && min_len < idx->max_length_legacy && idx->length_bitmaps_legacy[min_len])
                {
                    RoaringBitmap *lb = biscuit_reconcile_pending(index, idx->length_bitmaps_legacy[min_len],
                                                                    -1, false, BISCUIT_DIR_KIND_LEN, -1, min_len);
                    biscuit_roaring_and_inplace(result, lb);
                }
                else if (!result || min_len >= idx->max_length_legacy) { if (result) biscuit_roaring_free(result); result = biscuit_roaring_create(); }
            } else if (!parsed->starts_percent) {
                result = biscuit_match_part_at_pos(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                result = biscuit_match_part_at_end(index, idx, parsed->parts[0], parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                /* '%abc%' – substring via char-cache and brute verification */
                result = biscuit_roaring_create();
                if (parsed->part_byte_lens[0] > 0) {
                    unsigned char first_byte = biscuit_part_seed_byte(
                        parsed->parts[0], parsed->part_byte_lens[0]);
                    if (first_byte != 0) {
                        RoaringBitmap *cc = biscuit_reconcile_pending(index, idx->char_cache_legacy[first_byte],
                                                                        -1, false, BISCUIT_DIR_KIND_CACHE,
                                                                        first_byte, -1);
                        RoaringBitmap *candidates = cc
                            ? biscuit_roaring_copy(cc)
                            : biscuit_roaring_create();
                        int part_char_len = parsed->part_lens[0];
                        RoaringBitmap *lf = biscuit_get_length_ge(index, idx, part_char_len);
                        if (lf) { biscuit_roaring_and_inplace(candidates, lf); biscuit_roaring_free(lf); }
                        /* FIX 1: shrink the verify set with the caller's
                         * mask before the O(candidates x string_length)
                         * scalar substring scan below, instead of
                         * scanning every char_cache hit in the table. */
                        if (mask) biscuit_roaring_and_inplace(candidates, mask);

                        #ifdef HAVE_ROARING
                        {
                            roaring_uint32_iterator_t *iter = roaring_iterator_create(candidates);
                            while (iter->has_value) {
                                uint32_t rec = iter->current_value;
                                if (rec < (uint32_t) idx->num_records && idx->data_cache[rec]) {
                                    const char *hay = idx->data_cache[rec];
                                    int hbl = strlen(hay), hcl = biscuit_utf8_char_count(hay, hbl);
                                    bool found = false;
                                    for (int cp = 0; cp <= hcl - part_char_len && !found; cp++) {
                                        int bo = biscuit_utf8_char_to_byte_offset(hay, hbl, cp);
                                        if (bo < 0) continue;
                                        if (biscuit_part_match_substr(hay, hbl, bo,
                                                parsed->parts[0], parsed->part_byte_lens[0]))
                                            found = true;
                                    }
                                    if (found) biscuit_roaring_add(result, rec);
                                }
                                roaring_uint32_iterator_advance(iter);
                            }
                            roaring_uint32_iterator_free(iter);
                        }
                        #else
                        {
                            uint64_t cnt;
                            uint32_t *indices = biscuit_roaring_to_array(candidates, &cnt);
                            if (indices) {
                                for (int j = 0; j < (int) cnt; j++) {
                                    uint32_t rec = indices[j];
                                    if (rec < (uint32_t) idx->num_records && idx->data_cache[rec]) {
                                        const char *hay = idx->data_cache[rec];
                                        int hbl = strlen(hay), hcl = biscuit_utf8_char_count(hay, hbl);
                                        bool found = false;
                                        for (int cp = 0; cp <= hcl - part_char_len && !found; cp++) {
                                            int bo = biscuit_utf8_char_to_byte_offset(hay, hbl, cp);
                                            if (bo < 0) continue;
                                            if (biscuit_part_match_substr(hay, hbl, bo,
                                                    parsed->parts[0], parsed->part_byte_lens[0]))
                                                found = true;
                                        }
                                        if (found) biscuit_roaring_add(result, rec);
                                    }
                                }
                                pfree(indices);
                            }
                        }
                        #endif
                        biscuit_roaring_free(candidates);
                    }
                }
            }
        } else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            RoaringBitmap *prefix = biscuit_match_part_at_pos(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
            RoaringBitmap *suffix = biscuit_match_part_at_end(index, idx, parsed->parts[1], parsed->part_byte_lens[1]);
            if (!prefix || !suffix) { if (prefix) biscuit_roaring_free(prefix); if (suffix) biscuit_roaring_free(suffix); result = biscuit_roaring_create(); }
            else {
                RoaringBitmap *lf;
                biscuit_roaring_and_inplace(prefix, suffix); biscuit_roaring_free(suffix);
                lf = biscuit_get_length_ge(index, idx, min_len);
                if (lf) { biscuit_roaring_and_inplace(prefix, lf); biscuit_roaring_free(lf); }
                result = prefix;
            }
        } else {
            RoaringBitmap *candidates;
            result = biscuit_roaring_create();
            candidates = biscuit_get_length_ge(index, idx, min_len);
            /* FIX 1: seed the sweep from the caller's mask too, so a
             * small mask (e.g. from a cheap sibling predicate already
             * evaluated in biscuit_rescan()) collapses the per-position
             * bitmap ANDs in biscuit_recursive_windowed_match() to a
             * handful of rows instead of the full table. */
            if (mask && candidates) biscuit_roaring_and_inplace(candidates, mask);
            if (candidates && !biscuit_roaring_is_empty(candidates)) {
                if (!parsed->starts_percent) {
                    RoaringBitmap *first = biscuit_match_part_at_pos(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                    if (first) { biscuit_roaring_and_inplace(first, candidates); biscuit_roaring_free(candidates); candidates = first; }
                }
                if (!biscuit_roaring_is_empty(candidates)) {
                    biscuit_recursive_windowed_match(index, result, idx,
                        (const char **) parsed->parts, parsed->part_byte_lens, parsed->part_count,
                        parsed->ends_percent, 0, 0, candidates, idx->max_len);
                }
                biscuit_roaring_free(candidates);
            } else if (candidates) biscuit_roaring_free(candidates);
        }

        biscuit_free_parsed_pattern(parsed);
        parsed = NULL; /* FIX 5: prevent double-free in PG_CATCH */
    }
    PG_CATCH();
    {
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return result ? result : biscuit_roaring_create();
}

/*
 * biscuit_query_pattern -- unchanged public signature, kept for every
 * existing caller. Note this does NOT guarantee the returned bitmap is
 * fully ANDed with a mask on every code path (masking above is a
 * performance shortcut applied only in the two expensive branches);
 * callers that pass a mask via biscuit_query_pattern_masked() directly
 * must still AND the result with their mask afterward, same as they
 * would with the unmasked result today.
 */
RoaringBitmap *
biscuit_query_pattern(Relation index, BiscuitIndex *idx, const char *pattern)
{
    return biscuit_query_pattern_masked(index, idx, pattern, NULL);
}

/* ILIKE single-column: lower-case the pattern first, then reuse logic */

/* See biscuit_query_pattern_masked() above for the mask contract. */
RoaringBitmap *
biscuit_query_pattern_ilike_masked(Relation index, BiscuitIndex *idx, const char *pattern,
                                    const RoaringBitmap *mask)
{
    int            plen = strlen(pattern);
    char          *pl;
    RoaringBitmap *result;
    ParsedPattern *parsed = NULL;
    int            min_len, i;
    int            wildcard_count = 0, percent_count = 0;
    bool           only_wildcards = true;

    if (mask && biscuit_roaring_is_empty(mask))
        return biscuit_roaring_create();

    /*
     * Defensive gate: an index built with biscuit_like_ops never
     * populates the "_lower" structures this function depends on. The
     * planner should never route an ILIKE/NOT ILIKE qual to such an
     * index in the first place (biscuit_like_ops's opfamily doesn't
     * register those operators -- see biscuit.sql), so reaching this
     * with legacy_case_mode missing BISCUIT_MODE_ILIKE indicates a
     * programming error rather than a normal query, and we fail loudly
     * instead of dereferencing NULL/empty bitmap arrays.
     */
    if (!(idx->legacy_case_mode & BISCUIT_MODE_ILIKE))
        ereport(ERROR,
                (errmsg("biscuit: this index was not built with ILIKE support"),
                 errhint("The index's opclass (biscuit_like_ops) only supports LIKE/NOT LIKE.")));

    pl = biscuit_str_tolower(pattern, plen);

    /* delegate with lowercased pattern, using _lower accessors implicitly
       via biscuit_match_part_at_pos_ilike / _end_ilike / get_length_ge_lower */

    plen = strlen(pl);

    if (plen == 0) {
        /*
         * Empty pattern '' matches only records with an empty string
         * value. Use the case-insensitive length bitmap here, not the
         * case-sensitive legacy one -- a column built with
         * biscuit_ilike_ops only (no LIKE support) never populates
         * length_bitmaps_legacy, and even when both are built, going
         * through the _lower structures is the correct, mode-consistent
         * choice for an ILIKE entry point.
         */
        RoaringBitmap *lb = (idx->length_bitmaps_lower)
            ? biscuit_reconcile_pending(index, idx->length_bitmaps_lower[0], -1, true,
                                          BISCUIT_DIR_KIND_LEN, -1, 0)
            : NULL;
        result = lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
        pfree(pl); return result;
    }

    if (plen == 1 && pl[0] == '%') {
        /* '%' matches every non-tombstoned, non-NULL record */
        RoaringBitmap *lgb = (idx->length_ge_bitmaps_lower)
            ? biscuit_reconcile_pending(index, idx->length_ge_bitmaps_lower[0], -1, true,
                                          BISCUIT_DIR_KIND_LEN_GE, -1, 0)
            : NULL;
        if (lgb)
            result = biscuit_roaring_copy(lgb);
        else {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                bool ts = false;
#ifdef HAVE_ROARING
                ts = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
#else
                { uint32_t bl = i >> 6, bt = i & 63; ts = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
#endif
                if (!ts && idx->data_cache_lower && idx->data_cache_lower[i]) biscuit_roaring_add(result, i);
            }
        }
        pfree(pl); return result;
    }

    for (i = 0; i < plen; i++) {
        if (pl[i] == '%') percent_count++;
        else if (pl[i] == '_') wildcard_count++;
        else { only_wildcards = false; break; }
    }

    if (only_wildcards) {
        if (percent_count > 0) result = biscuit_get_length_ge_lower(index, idx, wildcard_count);
        else if (wildcard_count < idx->max_length_lower && idx->length_bitmaps_lower && idx->length_bitmaps_lower[wildcard_count])
        {
            RoaringBitmap *lb = biscuit_reconcile_pending(index, idx->length_bitmaps_lower[wildcard_count],
                                                            -1, true, BISCUIT_DIR_KIND_LEN, -1, wildcard_count);
            result = lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
        }
        else result = biscuit_roaring_create();
        pfree(pl); return result;
    }

    PG_TRY();
    {
        parsed = biscuit_parse_pattern(pl);
        if (parsed->part_count == 0) {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                bool ts = false;
#ifdef HAVE_ROARING
                ts = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
#else
                { uint32_t bl = i >> 6, bt = i & 63; ts = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
#endif
                if (!ts) biscuit_roaring_add(result, i);
            }
            biscuit_free_parsed_pattern(parsed); parsed = NULL; /* FIX 5 */ pfree(pl); return result;
        }
        min_len = 0;
        for (i = 0; i < parsed->part_count; i++) min_len += parsed->part_lens[i];

        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                result = biscuit_match_part_at_pos_ilike(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (result && min_len < idx->max_length_lower && idx->length_bitmaps_lower && idx->length_bitmaps_lower[min_len])
                {
                    RoaringBitmap *lb = biscuit_reconcile_pending(index, idx->length_bitmaps_lower[min_len],
                                                                    -1, true, BISCUIT_DIR_KIND_LEN, -1, min_len);
                    biscuit_roaring_and_inplace(result, lb);
                }
                else if (!result || min_len >= idx->max_length_lower) { if (result) biscuit_roaring_free(result); result = biscuit_roaring_create(); }
            } else if (!parsed->starts_percent) {
                result = biscuit_match_part_at_pos_ilike(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                result = biscuit_match_part_at_end_ilike(index, idx, parsed->parts[0], parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                result = biscuit_roaring_create();
                /* substring ILIKE – similar brute verification via lowercase cache */
                if (parsed->part_byte_lens[0] > 0) {
                    unsigned char first_byte = biscuit_part_seed_byte(
                        parsed->parts[0], parsed->part_byte_lens[0]);
                    RoaringBitmap *cc = (first_byte != 0)
                        ? biscuit_reconcile_pending(index, idx->char_cache_lower[first_byte], -1, true,
                                                      BISCUIT_DIR_KIND_CACHE, first_byte, -1)
                        : NULL;
                    RoaringBitmap *candidates = cc
                        ? biscuit_roaring_copy(cc)
                        : biscuit_roaring_create();
                    int pcl = parsed->part_lens[0];
                    RoaringBitmap *lf = biscuit_get_length_ge_lower(index, idx, pcl);
                    if (lf) { biscuit_roaring_and_inplace(candidates, lf); biscuit_roaring_free(lf); }
                    /* FIX 1 */
                    if (mask) biscuit_roaring_and_inplace(candidates, mask);
                    #ifdef HAVE_ROARING
                    { roaring_uint32_iterator_t *iter = roaring_iterator_create(candidates);
                      while (iter->has_value) { uint32_t rec = iter->current_value;
                        if (rec < (uint32_t) idx->num_records && idx->data_cache_lower && idx->data_cache_lower[rec]) {
                            const char *hay = idx->data_cache_lower[rec];
                            int hbl = strlen(hay), hcl = biscuit_utf8_char_count(hay, hbl);
                            bool found = false;
                            for (int cp = 0; cp <= hcl - pcl && !found; cp++) {
                                int bo = biscuit_utf8_char_to_byte_offset(hay, hbl, cp);
                                if (bo >= 0 && biscuit_part_match_substr(hay, hbl, bo,
                                        parsed->parts[0], parsed->part_byte_lens[0]))
                                    found = true;
                            }
                            if (found) biscuit_roaring_add(result, rec);
                        }
                        roaring_uint32_iterator_advance(iter); }
                      roaring_uint32_iterator_free(iter); }
                    #else
                    { uint64_t cnt; uint32_t *indices = biscuit_roaring_to_array(candidates, &cnt);
                      if (indices) { for (int j = 0; j < (int) cnt; j++) { uint32_t rec = indices[j];
                            if (rec < (uint32_t) idx->num_records && idx->data_cache_lower && idx->data_cache_lower[rec]) {
                                const char *hay = idx->data_cache_lower[rec];
                                int hbl = strlen(hay), hcl = biscuit_utf8_char_count(hay, hbl);
                                bool found = false;
                                for (int cp = 0; cp <= hcl - pcl && !found; cp++) {
                                    int bo = biscuit_utf8_char_to_byte_offset(hay, hbl, cp);
                                    if (bo >= 0 && biscuit_part_match_substr(hay, hbl, bo,
                                            parsed->parts[0], parsed->part_byte_lens[0]))
                                        found = true;
                                }
                                if (found) biscuit_roaring_add(result, rec); } }
                          pfree(indices); } }
                    #endif
                    biscuit_roaring_free(candidates);
                }
            }
        } else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            RoaringBitmap *prefix = biscuit_match_part_at_pos_ilike(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
            RoaringBitmap *suffix = biscuit_match_part_at_end_ilike(index, idx, parsed->parts[1], parsed->part_byte_lens[1]);
            if (!prefix || !suffix) { if (prefix) biscuit_roaring_free(prefix); if (suffix) biscuit_roaring_free(suffix); result = biscuit_roaring_create(); }
            else { RoaringBitmap *lf;
                   biscuit_roaring_and_inplace(prefix, suffix); biscuit_roaring_free(suffix);
                   lf = biscuit_get_length_ge_lower(index, idx, min_len);
                   if (lf) { biscuit_roaring_and_inplace(prefix, lf); biscuit_roaring_free(lf); }
                   result = prefix; }
        } else {
            RoaringBitmap *candidates;
            result = biscuit_roaring_create();
            candidates = biscuit_get_length_ge_lower(index, idx, min_len);
            /* FIX 1 */
            if (mask && candidates) biscuit_roaring_and_inplace(candidates, mask);
            if (candidates && !biscuit_roaring_is_empty(candidates)) {
                if (!parsed->starts_percent) { RoaringBitmap *first = biscuit_match_part_at_pos_ilike(index, idx, parsed->parts[0], parsed->part_byte_lens[0], 0); if (first) { biscuit_roaring_and_inplace(first, candidates); biscuit_roaring_free(candidates); candidates = first; } }
                if (!biscuit_roaring_is_empty(candidates))
                    biscuit_recursive_windowed_match_ilike(index, result, idx, (const char **) parsed->parts, parsed->part_byte_lens, parsed->part_count, parsed->ends_percent, 0, 0, candidates, idx->max_length_lower);
                biscuit_roaring_free(candidates);
            } else if (candidates) biscuit_roaring_free(candidates);
        }

        biscuit_free_parsed_pattern(parsed);
        parsed = NULL; /* FIX 5 */
    }
    PG_CATCH();
    {
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        pfree(pl);
        PG_RE_THROW();
    }
    PG_END_TRY();

    pfree(pl);
    return result ? result : biscuit_roaring_create();
}

RoaringBitmap *
biscuit_query_pattern_ilike(Relation index, BiscuitIndex *idx, const char *pattern)
{
    return biscuit_query_pattern_ilike_masked(index, idx, pattern, NULL);
}

/* ================================================================
 * SECTION 9 – Public multi-column query entry points
 * ================================================================ */

/* See biscuit_query_pattern_masked() above for the mask contract. */
RoaringBitmap *
biscuit_query_column_pattern_masked(Relation index, BiscuitIndex *idx, int col_idx,
                                     const char *pattern, const RoaringBitmap *mask)
{
    ColumnIndex   *col;
    int            plen = strlen(pattern);
    ParsedPattern *parsed = NULL;
    int            min_len, i;
    RoaringBitmap *result = NULL;
    int            wildcard_count = 0, percent_count = 0;
    bool           only_wildcards = true;

    if (!idx || col_idx < 0 || col_idx >= idx->num_columns || !idx->column_indices)
        return biscuit_roaring_create();

    if (mask && biscuit_roaring_is_empty(mask))
        return biscuit_roaring_create();

    /*
     * Defensive gate: a column built with biscuit_ilike_ops never
     * populates the case-sensitive structures this function needs. The
     * planner should never route a LIKE/NOT LIKE qual to such a column
     * (biscuit_ilike_ops's opfamily doesn't register those operators),
     * so reaching this indicates a programming error rather than a
     * normal query.
     */
    if (idx->column_case_mode && !(idx->column_case_mode[col_idx] & BISCUIT_MODE_LIKE))
        ereport(ERROR,
                (errmsg("biscuit: column %d of this index was not built with LIKE support",
                        col_idx),
                 errhint("This column's opclass (biscuit_ilike_ops) only supports ILIKE/NOT ILIKE.")));

    col = &idx->column_indices[col_idx];

    if (!col->length_bitmaps || !col->length_ge_bitmaps || col->max_length <= 0)
        return biscuit_roaring_create();

    if (plen == 0) {
        RoaringBitmap *lb = biscuit_reconcile_pending(index, col->length_bitmaps[0],
                                                        col_idx, false, BISCUIT_DIR_KIND_LEN, -1, 0);
        return lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
    }
    if (plen == 1 && pattern[0] == '%') {
        RoaringBitmap *lgb = biscuit_reconcile_pending(index, col->length_ge_bitmaps[0],
                                                         col_idx, false, BISCUIT_DIR_KIND_LEN_GE, -1, 0);
        return lgb ? biscuit_roaring_copy(lgb) : biscuit_roaring_create();
    }

    for (i = 0; i < plen; i++) {
        if (pattern[i] == '%') percent_count++;
        else if (pattern[i] == '_') wildcard_count++;
        else { only_wildcards = false; break; }
    }

    if (only_wildcards) {
        /*
         * col->max_length is the allocated size of length_bitmaps /
         * length_ge_bitmaps (valid indices 0..max_length-1), so this must
         * be a strict "<". "<=" let wildcard_count == max_length slip
         * through, reading one RoaringBitmap* past the end of the
         * palloc'd array on an ordinary LIKE/ILIKE query whose all-wildcard
         * pattern length equals the column's max indexed length.
         */
        if (percent_count > 0 && wildcard_count < col->max_length && col->length_ge_bitmaps[wildcard_count])
        {
            RoaringBitmap *lgb = biscuit_reconcile_pending(index, col->length_ge_bitmaps[wildcard_count],
                                                             col_idx, false, BISCUIT_DIR_KIND_LEN_GE,
                                                             -1, wildcard_count);
            return lgb ? biscuit_roaring_copy(lgb) : biscuit_roaring_create();
        }
        if (!percent_count && wildcard_count < col->max_length && col->length_bitmaps[wildcard_count])
        {
            RoaringBitmap *lb = biscuit_reconcile_pending(index, col->length_bitmaps[wildcard_count],
                                                            col_idx, false, BISCUIT_DIR_KIND_LEN,
                                                            -1, wildcard_count);
            return lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
        }
        return biscuit_roaring_create();
    }

    PG_TRY();
    {
        parsed = biscuit_parse_pattern(pattern);
        if (parsed->part_count == 0) {
            RoaringBitmap *lgb = biscuit_reconcile_pending(index, col->length_ge_bitmaps[0],
                                                             col_idx, false, BISCUIT_DIR_KIND_LEN_GE, -1, 0);
            result = lgb ? biscuit_roaring_copy(lgb) : biscuit_roaring_create();
            biscuit_free_parsed_pattern(parsed); parsed = NULL; /* FIX 5 */ return result;
        }
        min_len = 0;
        for (i = 0; i < parsed->part_count; i++) min_len += parsed->part_lens[i];

        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                result = biscuit_match_col_part_at_pos(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                /* col->max_length is the array size (valid indices 0..max_length-1); "<=" read one past the end */
                if (result && min_len < col->max_length && col->length_bitmaps[min_len])
                {
                    RoaringBitmap *lb = biscuit_reconcile_pending(index, col->length_bitmaps[min_len],
                                                                    col_idx, false, BISCUIT_DIR_KIND_LEN,
                                                                    -1, min_len);
                    biscuit_roaring_and_inplace(result, lb);
                }
                else if (result) { biscuit_roaring_free(result); result = biscuit_roaring_create(); }
                else result = biscuit_roaring_create();
            } else if (!parsed->starts_percent) {
                result = biscuit_match_col_part_at_pos(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                result = biscuit_match_col_part_at_end(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                /*
                 * Substring LIKE: %needle%
                 * Seed candidates from char_cache[first_concrete_byte]
                 * (case-sensitive) — biscuit_part_seed_byte() skips any
                 * leading '_' wildcards / BISCUIT_LITERAL_ESC prefixes so
                 * a pattern like '%_lex%' seeds from 'l', not '_'.
                 * Filter by minimum length, then verify with a
                 * wildcard-aware match.
                 */
                result = biscuit_roaring_create();
                if (parsed->part_byte_lens[0] > 0)
                {
                    unsigned char  fb    = biscuit_part_seed_byte(parsed->parts[0], parsed->part_byte_lens[0]);
                    RoaringBitmap *cc    = fb != 0
                                          ? biscuit_reconcile_pending(index, col->char_cache[fb], col_idx,
                                                                       false, BISCUIT_DIR_KIND_CACHE, fb, -1)
                                          : NULL;
                    RoaringBitmap *lgb0  = biscuit_reconcile_pending(index, col->length_ge_bitmaps[0], col_idx,
                                                                       false, BISCUIT_DIR_KIND_LEN_GE, -1, 0);
                    RoaringBitmap *cands = (fb != 0 && cc)
                                          ? biscuit_roaring_copy(cc)
                                          : (fb != 0
                                             ? biscuit_roaring_create()
                                             : (lgb0
                                                ? biscuit_roaring_copy(lgb0)
                                                : biscuit_roaring_create()));
                    int            pcl   = parsed->part_lens[0];
                    RoaringBitmap *lf    = biscuit_get_col_length_ge(index, col, col_idx, pcl);

                    if (lf) { biscuit_roaring_and_inplace(cands, lf); biscuit_roaring_free(lf); }
                    /* FIX 1 */
                    if (mask) biscuit_roaring_and_inplace(cands, mask);

#ifdef HAVE_ROARING
                    {
                        roaring_uint32_iterator_t *iter = roaring_iterator_create(cands);
                        while (iter->has_value)
                        {
                            uint32_t    rec = iter->current_value;
                            const char *hay;
                            if (rec < (uint32_t) idx->num_records &&
                                idx->column_data_cache &&
                                idx->column_data_cache[col_idx] &&
                                (hay = idx->column_data_cache[col_idx][rec]) != NULL)
                            {
                                if (biscuit_wildcard_contains(hay, strlen(hay),
                                                               parsed->parts[0],
                                                               parsed->part_byte_lens[0]))
                                    biscuit_roaring_add(result, rec);
                            }
                            roaring_uint32_iterator_advance(iter);
                        }
                        roaring_uint32_iterator_free(iter);
                    }
#else
                    {
                        uint64_t  cnt;
                        uint32_t *indices = biscuit_roaring_to_array(cands, &cnt);
                        if (indices)
                        {
                            int j;
                            for (j = 0; j < (int) cnt; j++)
                            {
                                uint32_t    rec = indices[j];
                                const char *hay;
                                if (rec < (uint32_t) idx->num_records &&
                                    idx->column_data_cache &&
                                    idx->column_data_cache[col_idx] &&
                                    (hay = idx->column_data_cache[col_idx][rec]) != NULL)
                                {
                                    if (biscuit_wildcard_contains(hay, strlen(hay),
                                                                   parsed->parts[0],
                                                                   parsed->part_byte_lens[0]))
                                        biscuit_roaring_add(result, rec);
                                }
                            }
                            pfree(indices);
                        }
                    }
#endif
                    biscuit_roaring_free(cands);
                }
            }
        } else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            RoaringBitmap *prefix = biscuit_match_col_part_at_pos(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
            RoaringBitmap *suffix = biscuit_match_col_part_at_end(index, col, col_idx, parsed->parts[1], parsed->part_byte_lens[1]);
            if (!prefix || !suffix) { if (prefix) biscuit_roaring_free(prefix); if (suffix) biscuit_roaring_free(suffix); result = biscuit_roaring_create(); }
            else { RoaringBitmap *lf;
                   biscuit_roaring_and_inplace(prefix, suffix); biscuit_roaring_free(suffix);
                   lf = biscuit_get_col_length_ge(index, col, col_idx, min_len);
                   if (lf) { biscuit_roaring_and_inplace(prefix, lf); biscuit_roaring_free(lf); }
                   result = prefix; }
        } else {
            RoaringBitmap *cands;
            result = biscuit_roaring_create();
            cands = biscuit_get_col_length_ge(index, col, col_idx, min_len);
            /* FIX 1 */
            if (mask && cands) biscuit_roaring_and_inplace(cands, mask);
            if (cands && !biscuit_roaring_is_empty(cands)) {
                if (!parsed->starts_percent) { RoaringBitmap *first = biscuit_match_col_part_at_pos(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0); if (first) { biscuit_roaring_and_inplace(first, cands); biscuit_roaring_free(cands); cands = first; } }
                if (!biscuit_roaring_is_empty(cands))
                    biscuit_recursive_windowed_match_col(index, result, idx, col, col_idx, (const char **) parsed->parts, parsed->part_byte_lens, parsed->part_count, parsed->ends_percent, 0, 0, cands, col->max_length);
                biscuit_roaring_free(cands);
            } else if (cands) biscuit_roaring_free(cands);
        }
        biscuit_free_parsed_pattern(parsed);
        parsed = NULL; /* FIX 5 */
    }
    PG_CATCH();
    {
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return result ? result : biscuit_roaring_create();
}

RoaringBitmap *
biscuit_query_column_pattern(Relation index, BiscuitIndex *idx, int col_idx, const char *pattern)
{
    return biscuit_query_column_pattern_masked(index, idx, col_idx, pattern, NULL);
}

/* ILIKE variant for multi-column: lowercase pattern first. See
 * biscuit_query_pattern_masked() above for the mask contract. */
RoaringBitmap *
biscuit_query_column_pattern_ilike_masked(Relation index, BiscuitIndex *idx, int col_idx,
                                           const char *pattern, const RoaringBitmap *mask)
{
    char          *pl;
    RoaringBitmap *result;
    ColumnIndex   *col;
    int            plen = strlen(pattern);

    if (!idx || col_idx < 0 || col_idx >= idx->num_columns || !idx->column_indices)
        return biscuit_roaring_create();

    if (mask && biscuit_roaring_is_empty(mask))
        return biscuit_roaring_create();

    /*
     * Defensive gate: a column built with biscuit_like_ops never
     * populates the case-insensitive "_lower" structures this function
     * needs. The planner should never route an ILIKE/NOT ILIKE qual to
     * such a column (biscuit_like_ops's opfamily doesn't register those
     * operators), so reaching this indicates a programming error rather
     * than a normal query.
     */
    if (idx->column_case_mode && !(idx->column_case_mode[col_idx] & BISCUIT_MODE_ILIKE))
        ereport(ERROR,
                (errmsg("biscuit: column %d of this index was not built with ILIKE support",
                        col_idx),
                 errhint("This column's opclass (biscuit_like_ops) only supports LIKE/NOT LIKE.")));

    col = &idx->column_indices[col_idx];
    pl  = biscuit_str_tolower(pattern, plen);

    /* delegate via case-insensitive (lower) accessors in a similar flow */
    {
    ParsedPattern *parsed = NULL;
    int            min_len, i;
    int            wc = 0, pc = 0;
    bool           ow = true;

    plen = strlen(pl);
    result = NULL;

    if (plen == 0) {
        RoaringBitmap *lb = (col->length_bitmaps_lower)
            ? biscuit_reconcile_pending(index, col->length_bitmaps_lower[0], col_idx, true,
                                          BISCUIT_DIR_KIND_LEN, -1, 0)
            : NULL;
        result = lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
        pfree(pl); return result;
    }
    if (plen == 1 && pl[0] == '%') {
        RoaringBitmap *lgb = (col->length_ge_bitmaps_lower)
            ? biscuit_reconcile_pending(index, col->length_ge_bitmaps_lower[0], col_idx, true,
                                          BISCUIT_DIR_KIND_LEN_GE, -1, 0)
            : NULL;
        result = lgb ? biscuit_roaring_copy(lgb) : biscuit_roaring_create();
        pfree(pl); return result;
    }

    for (i = 0; i < plen; i++) { if (pl[i] == '%') pc++; else if (pl[i] == '_') wc++; else { ow = false; break; } }
    if (ow) {
        if (pc > 0) result = biscuit_get_col_length_ge_lower(index, col, col_idx, wc);
        /* col->max_length_lower is the array size (valid indices 0..max_length_lower-1); "<=" read one past the end */
        else if (wc < col->max_length_lower && col->length_bitmaps_lower && col->length_bitmaps_lower[wc])
        {
            RoaringBitmap *lb = biscuit_reconcile_pending(index, col->length_bitmaps_lower[wc], col_idx, true,
                                                            BISCUIT_DIR_KIND_LEN, -1, wc);
            result = lb ? biscuit_roaring_copy(lb) : biscuit_roaring_create();
        }
        else result = biscuit_roaring_create();
        pfree(pl); return result;
    }

    PG_TRY();
    {
        parsed = biscuit_parse_pattern(pl);
        if (parsed->part_count == 0) {
            RoaringBitmap *lgb = (col->length_ge_bitmaps_lower)
                ? biscuit_reconcile_pending(index, col->length_ge_bitmaps_lower[0], col_idx, true,
                                              BISCUIT_DIR_KIND_LEN_GE, -1, 0)
                : NULL;
            result = lgb ? biscuit_roaring_copy(lgb) : biscuit_roaring_create();
            biscuit_free_parsed_pattern(parsed); parsed = NULL; /* FIX 5 */ pfree(pl); return result;
        }
        min_len = 0;
        for (i = 0; i < parsed->part_count; i++) min_len += parsed->part_lens[i];

        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                result = biscuit_match_col_part_at_pos_ilike(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                /* col->max_length_lower is the array size (valid indices 0..max_length_lower-1); "<=" read one past the end */
                if (result && min_len < col->max_length_lower && col->length_bitmaps_lower && col->length_bitmaps_lower[min_len])
                {
                    RoaringBitmap *lb = biscuit_reconcile_pending(index, col->length_bitmaps_lower[min_len], col_idx,
                                                                    true, BISCUIT_DIR_KIND_LEN, -1, min_len);
                    biscuit_roaring_and_inplace(result, lb);
                }
                else if (result) { biscuit_roaring_free(result); result = biscuit_roaring_create(); }
                else result = biscuit_roaring_create();
            } else if (!parsed->starts_percent) {
                result = biscuit_match_col_part_at_pos_ilike(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                result = biscuit_match_col_part_at_end_ilike(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                /*
                 * Substring ILIKE: %needle%
                 * pl (the pattern) has already been fully lowercased above.
                 * parsed->parts[0] is therefore also lowercase.
                 * Seed candidates from char_cache_lower[first_concrete_byte]
                 * — biscuit_part_seed_byte() skips leading '_' wildcards /
                 * BISCUIT_LITERAL_ESC prefixes so a pattern like '%_lex%'
                 * seeds from 'l', not '_'. Filter by minimum length, then
                 * verify with a wildcard-aware match against the
                 * lowercased data cache.
                 */
                result = biscuit_roaring_create();
                if (parsed->part_byte_lens[0] > 0)
                {
                    unsigned char  fb   = biscuit_part_seed_byte(parsed->parts[0], parsed->part_byte_lens[0]); /* lowercase first concrete byte */
                    RoaringBitmap *cc   = fb != 0
                                        ? biscuit_reconcile_pending(index, col->char_cache_lower[fb], col_idx,
                                                                     true, BISCUIT_DIR_KIND_CACHE, fb, -1)
                                        : NULL;
                    RoaringBitmap *lgb0 = col->length_ge_bitmaps_lower
                                        ? biscuit_reconcile_pending(index, col->length_ge_bitmaps_lower[0], col_idx,
                                                                     true, BISCUIT_DIR_KIND_LEN_GE, -1, 0)
                                        : NULL;
                    RoaringBitmap *cands = (fb != 0 && cc)
                                          ? biscuit_roaring_copy(cc)
                                          : (fb != 0
                                             ? biscuit_roaring_create()
                                             : (lgb0
                                                ? biscuit_roaring_copy(lgb0)
                                                : biscuit_roaring_create()));
                    int            pcl   = parsed->part_lens[0];
                    RoaringBitmap *lf    = biscuit_get_col_length_ge_lower(index, col, col_idx, pcl);

                    if (lf) { biscuit_roaring_and_inplace(cands, lf); biscuit_roaring_free(lf); }
                    /* FIX 1 */
                    if (mask) biscuit_roaring_and_inplace(cands, mask);

#ifdef HAVE_ROARING
                    {
                        roaring_uint32_iterator_t *iter = roaring_iterator_create(cands);
                        while (iter->has_value)
                        {
                            uint32_t    rec = iter->current_value;
                            const char *hay;
                            /*
                             * Use the pre-lowercased cache populated at build
                             * / insert time.  This avoids a
                             * palloc + tolower + pfree per candidate — the hot
                             * path inside an already bitmap-pruned set.
                             * column_data_cache_lower mirrors column_data_cache
                             * slot-for-slot; a NULL entry means the source value
                             * was NULL, so the same guard applies.
                             */
                            if (rec < (uint32_t) idx->num_records &&
                                idx->column_data_cache_lower &&
                                idx->column_data_cache_lower[col_idx] &&
                                (hay = idx->column_data_cache_lower[col_idx][rec]) != NULL)
                            {
                                if (biscuit_wildcard_contains(hay, strlen(hay),
                                                               parsed->parts[0],
                                                               parsed->part_byte_lens[0]))
                                    biscuit_roaring_add(result, rec);
                            }
                            roaring_uint32_iterator_advance(iter);
                        }
                        roaring_uint32_iterator_free(iter);
                    }
#else
                    {
                        uint64_t  cnt;
                        uint32_t *indices = biscuit_roaring_to_array(cands, &cnt);
                        if (indices)
                        {
                            int j;
                            for (j = 0; j < (int) cnt; j++)
                            {
                                uint32_t    rec = indices[j];
                                const char *hay;
                                if (rec < (uint32_t) idx->num_records &&
                                    idx->column_data_cache_lower &&
                                    idx->column_data_cache_lower[col_idx] &&
                                    (hay = idx->column_data_cache_lower[col_idx][rec]) != NULL)
                                {
                                    if (biscuit_wildcard_contains(hay, strlen(hay),
                                                                   parsed->parts[0],
                                                                   parsed->part_byte_lens[0]))
                                        biscuit_roaring_add(result, rec);
                                }
                            }
                            pfree(indices);
                        }
                    }
#endif
                    biscuit_roaring_free(cands);
                }
            }
        } else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            RoaringBitmap *prefix = biscuit_match_col_part_at_pos_ilike(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
            RoaringBitmap *suffix = biscuit_match_col_part_at_end_ilike(index, col, col_idx, parsed->parts[1], parsed->part_byte_lens[1]);
            if (!prefix || !suffix) { if (prefix) biscuit_roaring_free(prefix); if (suffix) biscuit_roaring_free(suffix); result = biscuit_roaring_create(); }
            else { RoaringBitmap *lf; biscuit_roaring_and_inplace(prefix, suffix); biscuit_roaring_free(suffix); lf = biscuit_get_col_length_ge_lower(index, col, col_idx, min_len); if (lf) { biscuit_roaring_and_inplace(prefix, lf); biscuit_roaring_free(lf); } result = prefix; }
        } else {
            RoaringBitmap *cands;
            result = biscuit_roaring_create();
            cands = biscuit_get_col_length_ge_lower(index, col, col_idx, min_len);
            /* FIX 1 */
            if (mask && cands) biscuit_roaring_and_inplace(cands, mask);
            if (cands && !biscuit_roaring_is_empty(cands)) {
                if (!parsed->starts_percent) { RoaringBitmap *first = biscuit_match_col_part_at_pos_ilike(index, col, col_idx, parsed->parts[0], parsed->part_byte_lens[0], 0); if (first) { biscuit_roaring_and_inplace(first, cands); biscuit_roaring_free(cands); cands = first; } }
                if (!biscuit_roaring_is_empty(cands))
                    biscuit_recursive_windowed_match_col_ilike(index, result, idx, col, col_idx, (const char **) parsed->parts, parsed->part_byte_lens, parsed->part_count, parsed->ends_percent, 0, 0, cands, col->max_length_lower);
                biscuit_roaring_free(cands);
            } else if (cands) biscuit_roaring_free(cands);
        }
        biscuit_free_parsed_pattern(parsed);
        parsed = NULL; /* FIX 5 */
    }
    PG_CATCH();
    {
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        pfree(pl);
        PG_RE_THROW();
    }
    PG_END_TRY();

    pfree(pl);
    return result ? result : biscuit_roaring_create();
    } /* end parsed block */
}

RoaringBitmap *
biscuit_query_column_pattern_ilike(Relation index, BiscuitIndex *idx, int col_idx, const char *pattern)
{
    return biscuit_query_column_pattern_ilike_masked(index, idx, col_idx, pattern, NULL);
}

/* ================================================================
 * SECTION 10 – Query plan / optimizer
 * ================================================================ */

static int
calculate_anchor_strength(const char *pattern, bool is_prefix, bool is_suffix)
{
    int strength = 0, i, len = strlen(pattern);
    if (!is_prefix && !is_suffix) return 0;
    if (is_prefix) { for (i = 0; i < len && pattern[i] != '%'; i++) strength += (pattern[i] != '_') ? 10 : 3; }
    if (is_suffix) { int ss = len; for (i = len - 1; i >= 0 && pattern[i] != '%'; i--) ss = i; for (i = ss; i < len; i++) strength += (pattern[i] != '_') ? 10 : 3; }
    return Min(strength, 100);
}

/*
 * KNOWN OPEN ISSUE (not fixed in this changeset): this is a second,
 * independent implementation of "how selective/anchored is this LIKE
 * pattern" alongside biscuit_classify_pattern()/BiscuitPatternShape in
 * biscuit_index.c, which feeds costestimate. The two can disagree about
 * which key is cheap -- costestimate might tell the planner one key is
 * cheaper while this function orders rescan's evaluation the other way.
 * One concrete symptom of the divergence was fixed directly below (NOT
 * LIKE/NOT ILIKE scoring, see the comment at the end of this function) --
 * but that's a patch on this classifier, not a fix to the underlying
 * duplication. Consolidating the two into one shared classifier is still
 * recommended; now that biscuit_index.h and biscuit_pattern.h are both
 * available, the header-plumbing obstacle that used to block that is
 * gone, but the merge itself is a real design decision (the two
 * classifiers compute different things -- BiscuitPatternShape is built
 * for cost-model math, QueryPredicate for ordering) that deserves its
 * own reviewed change rather than being folded in here unrequested.
 */
static void
analyze_pattern(QueryPredicate *pred)
{
    const char *p = pred->pattern;
    int len = strlen(p), i;
    bool in_pct = false;

    pred->concrete_chars = pred->underscore_count = pred->percent_count = 0;
    pred->partition_count = 0; pred->has_percent = false;

    for (i = 0; i < len; i++) {
        if (p[i] == '%') { pred->has_percent = true; if (!in_pct) { pred->percent_count++; in_pct = true; } }
        else { if (in_pct) pred->partition_count++; in_pct = false; if (p[i] == '_') pred->underscore_count++; else pred->concrete_chars++; }
    }
    if (!in_pct && len > 0) pred->partition_count++;

    pred->is_exact  = !pred->has_percent && pred->underscore_count == 0;
    pred->is_prefix = (len > 0 && p[0] != '%') && pred->has_percent;
    pred->is_suffix = (len > 0 && p[len - 1] != '%') && pred->has_percent;
    pred->is_substring = pred->starts_percent = pred->ends_percent = false;
    if (pred->has_percent) {
        pred->starts_percent  = (p[0] == '%');
        pred->ends_percent    = (p[len - 1] == '%');
        pred->is_substring    = pred->starts_percent && pred->ends_percent && pred->percent_count >= 2;
    }
    pred->anchor_strength = calculate_anchor_strength(p, pred->is_prefix, pred->is_suffix);
    pred->selectivity_score = pred->is_exact ? 0.0
        : pred->is_prefix || pred->is_suffix ? 0.1 + (pred->percent_count * 0.1)
        : pred->is_substring ? 0.5 : 0.8;
    pred->selectivity_score -= (pred->concrete_chars * 0.05);
    if (pred->selectivity_score < 0.0) pred->selectivity_score = 0.01;
    if (pred->selectivity_score > 1.0) pred->selectivity_score = 1.0;

    /*
     * FIX 3b: this score measures how selective the *pattern* is, not
     * how selective the *predicate* is. For NOT LIKE / NOT ILIKE the
     * predicate returns the complement of the pattern match, so a
     * strongly-anchored pattern ('usr\_1234\_%', score ~0.0, "looks
     * cheap") is actually the LEAST selective predicate available --
     * NOT LIKE 'usr\_1234\_%' returns ~99.99% of the table. Evaluating
     * it first would seed the mask with almost every row, defeating the
     * whole point of ordering by cost. Invert the score for negated
     * strategies so the sort reflects what the predicate actually
     * returns, not what its pattern looks like in isolation.
     *
     * This is a symptom of the classifier divergence documented above
     * analyze_pattern() and on biscuit_classify_pattern() in
     * biscuit_index.c -- consolidating the two would fix this as a
     * side effect, but until then this predicate-level correction is
     * applied here directly.
     */
    if (pred->scan_key &&
        (pred->scan_key->sk_strategy == BISCUIT_NOT_LIKE_STRATEGY ||
         pred->scan_key->sk_strategy == BISCUIT_NOT_ILIKE_STRATEGY))
        pred->selectivity_score = 1.0 - pred->selectivity_score;
}

static int
compare_predicates(const void *a, const void *b)
{
    const QueryPredicate *pa = (const QueryPredicate *) a;
    const QueryPredicate *pb = (const QueryPredicate *) b;
    if (pa->selectivity_score < pb->selectivity_score) return -1;
    if (pa->selectivity_score > pb->selectivity_score) return  1;
    return 0;
}

QueryPlan *
biscuit_build_query_plan(BiscuitIndex *idx, ScanKey keys, int nkeys)
{
    QueryPlan *plan;
    int        i;

    (void) idx;  /* reserved for future cardinality-aware planning */

    plan            = (QueryPlan *) palloc(sizeof(QueryPlan));
    plan->predicates = (QueryPredicate *) palloc(nkeys * sizeof(QueryPredicate));
    plan->count     = 0;
    plan->capacity  = nkeys;

    for (i = 0; i < nkeys; i++)
    {
        ScanKey        key  = &keys[i];
        QueryPredicate *pred = &plan->predicates[plan->count];

        if (key->sk_flags & SK_ISNULL)
            continue;

        pred->column_index = key->sk_attno - 1;
        pred->scan_key     = key;

        {
            text *pt = DatumGetTextPP(key->sk_argument);
            pred->pattern = pstrdup(text_to_cstring(pt));
        }

        analyze_pattern(pred);
        plan->count++;
    }

    /* Sort by selectivity: most selective first */
    if (plan->count > 1)
        qsort(plan->predicates, plan->count, sizeof(QueryPredicate), compare_predicates);

    return plan;
}

void
biscuit_free_query_plan(QueryPlan *plan)
{
    int i;

    if (!plan)
        return;

    PG_TRY();
    {
        if (plan->predicates) {
            for (i = 0; i < plan->count; i++) {
                if (plan->predicates[i].pattern) {
                    pfree(plan->predicates[i].pattern);
                    plan->predicates[i].pattern = NULL;
                }
            }
            pfree(plan->predicates);
            plan->predicates = NULL;
        }
        pfree(plan);
    }
    PG_CATCH();
    {
        FlushErrorState();
    }
    PG_END_TRY();
}
