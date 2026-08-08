/*
 * biscuit_pendlog.h
 *
 * The single index-wide append-only pending log that replaced the old
 * per-structure pending chains.
 *
 * The problem it solves
 * ----------------------
 * Every steady-state bitmap mutation used to append to its own
 * structure's private pending chain and then update that structure's
 * directory entry. A structure is a (col, is_lower, kind, ch, position)
 * tuple, and a single indexed row touches a lot of them -- positional and
 * negative-offset bitmaps for every character, per-character caches,
 * length and length-ge bitmaps, all doubled for the lowercase set. Each
 * of those structures had its own chain, hence its own page.
 *
 * PostgreSQL charges a full-page image for a page's first modification
 * after each checkpoint, so WAL volume tracked *page count*, and page
 * count tracked structure count. Measured with pg_waldump on one insert
 * of a 9-character string: 199 WAL records, 82 distinct pages, 84 FPIs,
 * 60KB of WAL for a single row. WAL per row grew linearly with string
 * length at roughly 1KB per character, independent of index size.
 *
 * The fix
 * -------
 * One log per index. Each record carries its own structure identity, so
 * all K records a row produces land on the *same* tail page. Page count
 * per row collapses from ~K to ~1, and the FPI bill collapses with it.
 * This is GIN's fastupdate pending-list design applied to the same
 * problem.
 *
 * The cost, and where it lands
 * -----------------------------
 * Records widen from 8 to 20 bytes (BiscuitPendLogRecord vs the retired
 * per-structure pending record) because identity is now explicit rather than
 * implied by which chain a record sits in. That is a delta-size cost, not
 * a page-count cost, so it is charged against the cheap term.
 *
 * More significantly, reads get harder. Reconciling one structure used to
 * mean walking that structure's own short chain; now the relevant records
 * are scattered through a shared log. Scanning the whole log per structure
 * would be O(structures x log size) per query -- catastrophic. So the read
 * path materializes the log into an in-memory hash keyed by structure
 * identity, once per statement, and reconciles from that
 * (biscuit_pendlog_snapshot()). The snapshot is invalidated by the
 * metapage's pendlog_npages plus the tail page's record count, so a log
 * that has not changed is not rebuilt.
 *
 * Concurrency note (honest): appends now serialize on one tail page and
 * on the metapage counter, where before they serialized per structure.
 * That is a real reduction in write concurrency, and it is the same
 * trade-off GIN's fastupdate makes for the same reason. Workloads with
 * many concurrent writers to one index will feel it; the alternative was
 * paying ~1KB of WAL per indexed character forever.
 */

#ifndef BISCUIT_PENDLOG_H
#define BISCUIT_PENDLOG_H

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "utils/hsearch.h"

/* ==================== APPEND (hot path) ==================== */

/*
 * biscuit_pendlog_append
 *
 * Durably record one ROW WRITE -- not one bitmap delta. In the common case
 * this is a single
 * GenericXLog transaction registering exactly ONE page: the log tail.
 *
 * The metapage is read under a SHARE lock to find the tail and released
 * again *before* the WAL transaction opens, so it is not part of the
 * append's WAL record at all. It is only registered on the rare
 * page-rollover path, where the tail pointer and page count have to
 * advance. This is deliberate and is the single most important property
 * of this function: pages-per-append is the quantity the whole design
 * exists to minimize, and an earlier version that maintained an exact
 * record count on the metapage paid for it twice -- every append carried
 * the metapage into its WAL record (doubling pages-per-append) and every
 * writer serialized on one exclusive metapage lock.
 *
 * So the steady-state cost of an append is one page, and it is the same
 * page for every append in the index, which is what collapses the
 * full-page-image bill: one FPI per checkpoint interval no matter how
 * many rows or structures go through.
 *
 * There is NO directory lookup and NO directory update here. Entries are
 * created lazily at drain time for whatever structures actually appear.
 *
 * WHAT CHANGED, AND WHY THE SIGNATURE SHRANK SO MUCH
 * --------------------------------------------------
 * This used to take a full structure identity plus a rec_idx, and was
 * called once per structure a row touched -- roughly 8N+4 times for an
 * N-character string, since every character contributes POS, NEG and CACHE
 * per case mode on top of LEN and the LEN_GE ladder. Collapsing those onto
 * one page fixed the full-page-image bill but left the record count alone,
 * and record count is where the remaining amplification lived: ~196 records
 * and ~3.9 kB of derived data per 24-character row, measured at 5131 B of
 * WAL per row against 158 B for the heap alone.
 *
 * All of it was derivation. The set of structures a row belongs to is a
 * function of the row's text, and the text is already durable and already
 * WAL-logged, once, by biscuit_persist_row_identity_write_record(). So the
 * log now records only WHICH SLOT changed and IN WHICH DIRECTION, and
 * biscuit_delta.c reconstructs the identities on demand by reading the text
 * back out of STRCACHE and running biscuit_fanout_string() over it.
 *
 * The fan-out did not disappear; it left the write path and became
 * delta-build work, paid on the first read after a write burst rather than
 * on every write. For load-then-query it is never paid at all.
 *
 * op is BISCUIT_PENDING_OP_ADD or _REMOVE. It is mandatory: slots are
 * recycled, so a log may legitimately hold ADD@42, REMOVE@42, ADD@42 for
 * three different rows, and DELETE and the delete-half of UPDATE have no
 * other representation.
 *
 * Returns the log's undrained byte count after the append, so the caller can
 * apply the compaction trigger without a second metapage read.
 */
extern uint64 biscuit_pendlog_append(Relation index, uint32 slot, uint8 op);

/*
 * biscuit_pendlog_drain_trigger_bytes
 *
 * Log size (in bytes) at which the append path should compact.
 *
 * The threshold is configured in ROWS (biscuit.delta_compaction_slots) and
 * converted here, because rows are what set delta rebuild time while bytes
 * are what the append path cheaply knows. With a fixed 8-byte record the
 * conversion is exact rather than an estimate.
 *
 * BISCUIT_PENDLOG_DRAIN_PAGES survives only as a hard ceiling underneath:
 * 4 MB of 8-byte records is roughly half a million rows, far past any sane
 * rebuild budget, so it no longer functions as a tuning knob.
 */
extern uint64 biscuit_pendlog_drain_trigger_bytes(void);

/* ==================== ROW BATCHING ==================== */

/*
 * A row's fan-out is large: indexing one N-character string emits
 * POS/NEG/CACHE per character per case mode, plus LEN and one LEN_GE
 * append per length threshold -- on the order of 8N+4 appends. They all
 * target the same tail page, but before batching each one opened its own
 * GenericXLog transaction, so each paid a full XLogRecord header and
 * block reference for a ~20-byte payload. The fixed per-record overhead,
 * not the payload, dominated an insert's WAL.
 *
 * A batch holds that one tail page exclusively locked with a single
 * GenericXLog transaction open across the whole row, so N appends produce
 * one WAL record carrying the accumulated page delta.
 *
 * Contract:
 *   - begin/end must be paired around one row's write. end() is
 *     idempotent, and on an error unwind the buffer content lock and the
 *     GenericXLogState are both released by resource-owner cleanup.
 * NOTE: biscuit_insert() no longer opens a batch. A row is now ONE record,
 * so a batch would open a GenericXLog transaction, write eight bytes, and
 * close it again. The machinery is retained -- it is correct, costs nothing
 * unused, and a change reintroducing multiple appends per row would want it
 * back -- but the only live caller of biscuit_pendlog_batch_active() is now
 * the compaction trigger, defensively.
 *
 *   - While a batch is open, callers must NOT drain: biscuit_pendlog_
 *     drain_all() takes the metapage and other page locks, and taking
 *     them under the tail page's content lock deadlocks. The drain
 *     trigger in biscuit_pending_mutate_structure() therefore tests
 *     biscuit_pendlog_batch_active() and skips; the batch records that a
 *     drain is wanted and runs it from end(), with no locks of ours held.
 *   - Appends that cannot fit the batch's current page (fresh log, tail
 *     full, tail detached by a concurrent drain) transparently fall back
 *     to the unbatched single-record path, which owns all rollover and
 *     allocation logic. Correctness never depends on a batch being open.
 */
extern void biscuit_pendlog_batch_begin(Relation index);
extern void biscuit_pendlog_batch_end(void);
extern bool biscuit_pendlog_batch_active(void);

/* ==================== READ-PATH SNAPSHOT ==================== */

/*
 * BiscuitPendLogSnapshot
 *
 * An in-memory materialization of the whole log, keyed by structure
 * identity, for the read path. Built once and reused for every structure
 * a query touches; see biscuit_pendlog_snapshot().
 */
typedef struct BiscuitPendLogKey
{
    int32   col;
    int32   ch;
    int32   position;
    uint8   kind;
    uint8   is_lower;
    uint8   pad[2];         /* must be zeroed -- hash key is memcmp'd */
} BiscuitPendLogKey;

/*
 * BiscuitPendLogEntry
 *
 * One structure's ADDITIONS in the current delta. There is no removal list,
 * and that absence is the design, not an omission -- see BiscuitPendLogSnapshot
 * .kill below.
 *
 * (BiscuitPendLogDelta, the old ordered (rec_idx, op) array element, is
 * gone with it. Ordering mattered when a structure carried its own
 * interleaved adds and removes; it does not now, because the additions are
 * derived from each slot's CURRENT text and are therefore a set.)
 */
typedef struct BiscuitPendLogEntry
{
    BiscuitPendLogKey  key;     /* must be first (dynahash) */
    RoaringBitmap     *adds;    /* slots this structure gained */
} BiscuitPendLogEntry;

typedef struct BiscuitPendLogSnapshot
{
    HTAB          *htab;
    MemoryContext  cxt;
    Oid            indexoid;
    uint64         built_at_count;  /* (npages << 32 | tail record count)
                                      * when built; a mismatch means stale */
    uint64         built_at_drains; /* BiscuitMetaPageData.total_drains when
                                      * built. Required for correctness, not
                                      * just precision: built_at_count alone
                                      * repeats across a drain cycle (drain,
                                      * then refill by an identically-shaped
                                      * row, reproduces it exactly), and
                                      * reusing a snapshot across that
                                      * boundary both misses post-drain
                                      * deltas and re-applies already-folded
                                      * ones. See biscuit_pendlog_snapshot(). */
    uint64         nrecords;

    /*
     * Resume point for incremental extension: the last log page ingested,
     * and how many of its records were consumed. Within a drain cycle the
     * log is append-only (existing pages immutable, only the tail's
     * num_records grows), so a later call re-reads from here and absorbs
     * exactly the records appended since.
     *
     * Without this, any append invalidated the whole snapshot and the next
     * query re-materialized the entire log -- O(log size) per query in any
     * read/write mix, growing until a drain and then growing again.
     */
    BlockNumber    resume_blk;
    uint32         resume_off;

    /*
     * SLOT-LEVEL STATE -- the half of the delta that is not per-structure.
     *
     * kill -- slots whose membership in the BASE blobs may be stale, and
     *         which must therefore be subtracted from every base bitmap
     *         before a structure's additions are applied.
     *
     *         This exists because a REMOVE names no structures. It cannot:
     *         the set of structures a slot belonged to is a function of
     *         text that an UPDATE may already have overwritten in STRCACHE
     *         by the time anything reads the log. Trying to recover that
     *         list by re-deriving it would expand the REPLACEMENT row's
     *         identities and leave the original's in base forever. So the
     *         reconciler withdraws the slot from base wholesale and re-adds
     *         it from whatever the current text says.
     *
     *         A slot lands here if it was ever REMOVEd, or ADDed more than
     *         once. Deliberately NOT if it was ADDed exactly once and never
     *         removed: base can only contain a slot via an earlier,
     *         already-drained record, and every path that rewrites a live
     *         slot emits a REMOVE first, so a lone ADD is necessarily a slot
     *         base has never seen. That exclusion is what keeps a
     *         pure-insert workload on the zero-copy fast path in
     *         biscuit_reconcile_pending().
     *
     * live     -- slots whose last record is an ADD; the ones worth
     *             expanding. A row inserted and deleted within one drain
     *             window never gets expanded at all.
     * seen     -- every slot mentioned, used only to notice a second
     *             mention.
     * expanded -- slots already fanned out into htab. A record mentioning
     *             one of these sets need_rebuild: its identities came from
     *             text that has since changed, and there is no way to
     *             withdraw them.
     *
     * touched/ntouched/cap_touched -- slots ingested since the last
     * expansion. Expansion is a separate phase precisely so a slot reaches
     * its final state before its text is read.
     */
    RoaringBitmap *kill;
    RoaringBitmap *live;
    RoaringBitmap *seen;
    RoaringBitmap *expanded;

    uint32        *touched;
    int            ntouched;
    int            cap_touched;

    bool           need_rebuild;
    uint64         nidentities;  /* identities emitted; instrumentation */
} BiscuitPendLogSnapshot;

/*
 * biscuit_pendlog_snapshot
 *
 * Return a snapshot of the current log, building it if the cached one is
 * absent or stale. Returns NULL when the log is empty -- the common,
 * fully-drained case -- so callers can skip reconciliation entirely
 * without touching the hash at all.
 *
 * The returned snapshot is owned by this module and lives in its own
 * memory context; callers must not free it.
 *
 * Snapshots for several indexes are cached simultaneously (small LRU
 * array) because a backend alternating between per-column Biscuit indexes
 * would otherwise evict and fully rebuild on every switch. When the log
 * has grown since the cached snapshot was built, it is *extended* from its
 * resume point rather than rebuilt -- see BiscuitPendLogSnapshot's
 * resume_blk/resume_off. A full rebuild happens only when total_drains
 * moves, i.e. when a drain has folded the old deltas into the blobs.
 *
 * NOTE that this is called once per BITMAP FETCH, not once per statement --
 * the metapage is re-read every time -- so successive calls within one scan
 * can legitimately observe different log states. That is fine for appends
 * (they extend incrementally) and NOT fine for drains, which is what the
 * scan-lifetime guard below exists to catch. Every call to this function
 * feeds that guard when it is armed.
 */
extern BiscuitPendLogSnapshot *biscuit_pendlog_snapshot(Relation index);

/*
 * biscuit_pendlog_lookup
 * Find one structure's deltas in a snapshot. Returns NULL if that
 * structure has nothing pending (the common case even when the log is
 * non-empty, since a query's structures are a small subset of a write's).
 */
extern BiscuitPendLogEntry *biscuit_pendlog_lookup(BiscuitPendLogSnapshot *snap,
                                                    int32 col, bool is_lower,
                                                    uint8 kind, int32 ch,
                                                    int32 position);

/*
 * biscuit_pendlog_apply
 *
 * Reconcile one structure:  target := (target \ kill) | entry->adds
 *
 * ORDER IS NOT NEGOTIABLE. A slot updated in place appears in BOTH sets --
 * in kill because its base membership is stale, and in adds under its new
 * text's identities. Adding before subtracting would remove the row from
 * the structures it now belongs to. Subtract first.
 *
 * entry may be NULL: a structure with no additions may still be holding a
 * slot that has been retired out from under it, so the kill set applies
 * whether or not the hash had an entry.
 *
 * MVCC does not make an error here survivable and nothing may lean on it.
 * Because xs_recheck is false, nothing re-tests the predicate after the
 * index yields a TID. MVCC filters DEAD rows; it does not filter WRONG
 * ones. A stale membership that survives this is a live, visible tuple
 * returned for a pattern it does not match.
 */
extern void biscuit_pendlog_apply(const BiscuitPendLogSnapshot *snap,
                                   const BiscuitPendLogEntry *entry,
                                   RoaringBitmap *target);

/*
 * biscuit_pendlog_has_kills
 *
 * Does this snapshot change any structure, whether or not the hash has an
 * entry for it? Callers use this to preserve the old zero-copy fast path
 * where it is still valid -- an empty kill set means no base membership
 * anywhere is stale, which holds for any workload that has not deleted or
 * updated a row since the last drain.
 */
extern bool biscuit_pendlog_has_kills(const BiscuitPendLogSnapshot *snap);

/* Drop any cached snapshot (relcache invalidation, drain, index drop). */
extern void biscuit_pendlog_invalidate(Oid indexoid);

/* ==================== SCAN-LIFETIME DRAIN GUARD ==================== */

/*
 * A scan is not a point in time, and the read path used to assume it was.
 *
 * biscuit_pendlog_snapshot() above is called on EVERY bitmap fetch, and one
 * ILIKE 'alpha%' costs six of them (five biscuit_get_pos_bitmap_lower()
 * calls out of biscuit_match_part_at_pos_ilike(), plus one
 * biscuit_get_length_ge_lower()). The only thing that notices another
 * backend's drain is biscuit_get_current_index()'s meta->gen check
 * (biscuit_index.h), and that runs exactly twice per scan node: at
 * beginscan and at rescan. So a scan validated its base once and then
 * re-read the log five more times without re-validating anything.
 *
 * A drain landing in that window leaves the reader holding in-memory base
 * bitmaps built from the PRE-drain blobs -- nothing updates them, since the
 * drain operates directly on the on-disk directory with no BiscuitIndex
 * involved -- alongside a log that no longer mentions the merged slots. So
 * biscuit_pendlog_snapshot() correctly returns NULL, biscuit_reconcile_
 * pending() takes its "log empty, fully drained" early return, and hands
 * back that stale base verbatim. Every post-build row is then in neither
 * half of the read path for every fetch after the drain, and since those
 * bitmaps are AND-ed together the result collapses to exactly the
 * membership base had at load time -- which is why the observed failures
 * landed on precisely the CREATE INDEX row count whatever the workload
 * size, why every concurrent reader hit it in the same narrow window, and
 * why it cleared by itself when their next beginscan reloaded.
 *
 * The gen bump at pendlog_detach()/pendlog_clear_draining() is correct as
 * far as it goes. It just does not cover a scan already in flight.
 *
 * Contract:
 *   - begin/end must be paired around ONE candidate-set build. end() is
 *     idempotent, and a re-arm resets the state wholesale, so a guard left
 *     armed by an aborted scan cannot make a later one report a false
 *     divergence.
 *
 *   - begin() MUST be called BEFORE biscuit_get_current_index(), never
 *     after. The two metapage reads cannot be made atomic with respect to
 *     each other and the ordering decides which way the gap fails: arming
 *     first can cost one unnecessary rebuild, arming second misses the
 *     drain entirely and reintroduces the defect this exists to close.
 *
 *   - moved() must be read BEFORE end(). A true return means the candidate
 *     set just built mixes pre- and post-drain bitmaps and must be
 *     discarded whole -- not filtered, counted, or collected from. That
 *     includes an EMPTY candidate set: "no rows" reached under a drain is
 *     exactly as untrustworthy as a short count, since an AND against a
 *     post-drain-but-pre-reload bitmap can empty the set outright.
 *
 *   - The caller must then reload (biscuit_get_current_index() will, since
 *     the drain bumped meta->gen) and rebuild. Retry rather than lock: a
 *     drain is rare relative to scans, and excluding one for the duration
 *     of every scan would cost far more than occasionally redoing one.
 *
 * DELIBERATELY TRACKS ONLY (total_drains, pendlog_draining), NOT gen. gen
 * advances on every committed INSERT/UPDATE/DELETE, and ordinary appends
 * arriving mid-scan are handled correctly in flight -- that is exactly what
 * the incremental-extend path in biscuit_pendlog_snapshot() is for.
 * Retrying on those would livelock a scan under any sustained write load
 * while fixing nothing. Only a drain relocates membership out from under a
 * live reader.
 *
 * One guard, not a stack: index scans do not nest within a backend at this
 * layer. The sole caller is biscuit_rescan() (biscuit_scan.c); see its
 * retry loop.
 */
extern void biscuit_pendlog_scan_begin(Relation index);
extern bool biscuit_pendlog_scan_moved(void);
extern void biscuit_pendlog_scan_end(void);

/* ==================== DRAIN ==================== */

/*
 * biscuit_pendlog_drain_all
 *
 * Merge the entire log into the per-structure compacted blobs and reset
 * it. For each distinct structure appearing in the log: load its blob
 * (creating a directory entry if this is the structure's first ever
 * write), apply its deltas, write the blob back, update the entry. Then
 * retire every log page and clear the metapage's pendlog_* fields.
 *
 * Returns the number of structures drained. Called with wait = true from
 * biscuit_vacuumcleanup(), and opportunistically with wait = false from
 * the append path when the log exceeds BISCUIT_PENDLOG_DRAIN_PAGES.
 *
 * Concurrency contract
 * --------------------
 * Drainers are serialized against each other by a heavyweight ExclusiveLock
 * on the metapage -- the same mechanism GIN uses for ginInsertCleanup().
 * `wait` selects the behaviour when another backend already holds it:
 *
 *   wait = false  Return 0 immediately. The append path uses this: someone
 *                 else is already doing the work we wanted, and queueing a
 *                 user INSERT behind an O(index size) merge is worse than
 *                 letting the log grow a little longer.
 *   wait = true   Block until acquired. VACUUM uses this, since its pass
 *                 must not silently skip.
 *
 * Appenders do NOT take this lock and are never blocked by a drain. That
 * works because the drain *detaches* the log chain from the metapage under
 * one atomic GenericXLog transaction before merging anything, so the merge
 * operates on a chain nobody else can reach. Appends arriving mid-drain
 * start a fresh log and get drained next time.
 *
 * Note this is strictly *more* efficient than the per-structure drain it
 * replaces even ignoring the write-path win: a structure touched N times
 * is now blob-rewritten once per drain rather than once per threshold
 * crossing of its own chain.
 *
 * out_gen, if non-NULL, receives the exact meta->gen value this drain
 * produced, captured by pendlog_clear_draining() while still holding the
 * drain's serializing lock -- 0 if the drain found nothing to drain (or
 * did not run, e.g. wait = false and the lock was held elsewhere). Pass
 * NULL when the caller has no in-memory BiscuitIndex to resync (e.g.
 * biscuit_vacuumcleanup()); pass a real pointer and forward it to
 * biscuit_resync_gen_after_self_drain() (biscuit_index.h) when the caller
 * does, so it can keep its own idx->gen in step with a merge it drove
 * itself. See that function's header comment for the full history.
 */
extern int biscuit_pendlog_drain_all(Relation index, bool wait, uint64 *out_gen);

/*
 * biscuit_pendlog_compact
 *
 * Ship the first max_pages pages of the log into the compacted blobs,
 * leaving the remainder live and appendable throughout. Same merge as
 * biscuit_pendlog_drain_all(), same drain lock, same `wait` semantics.
 *
 * Why this exists: waiting for VACUUM lets the delta grow without bound
 * between vacuums, and delta size is what sets read latency and cold-start
 * rebuild cost. A full drain from the append path would rewrite every
 * structure the log touched, which makes a bulk load quadratic; a bounded
 * prefix bounds the work per trigger.
 *
 * COMPACTION SHIPS A STRICT PREFIX, NEVER A SELECTION OF SLOTS. For any
 * slot, either every record up to the compaction point is applied to base
 * or none is. Shipping "the interesting slots" reorders that slot's
 * history, and with slot recycling that is concretely wrong -- a slot may
 * hold ADD, REMOVE, ADD for three different rows, and applying the first
 * ADD without the REMOVE leaves base claiming a row that no longer
 * matches. Nothing downstream catches it (see biscuit_pendlog_apply()).
 *
 * If the prefix would reach the tail, this degrades to a full drain, which
 * additionally clears BISCUIT_PENDING_FLAG_TAIL -- the handshake that stops
 * a concurrent appender writing into a chain about to be freed.
 *
 * out_gen, if non-NULL, receives the exact meta->gen value this compaction
 * produced (0 if nothing was drained) -- see biscuit_pendlog_drain_all()'s
 * out_gen for the full contract; the same rules apply here.
 */
extern int biscuit_pendlog_compact(Relation index, bool wait, uint32 max_pages,
                                   uint64 *out_gen);

/*
 * biscuit_pendlog_free_chain
 * Retire every log page without draining (index drop). Does not touch
 * the blobs or the directory.
 */
extern void biscuit_pendlog_free_chain(Relation index);

/* Read the log's current size: pages, and pages*BLCKSZ. */
extern void biscuit_pendlog_stats(Relation index,
                                   uint64 *out_count, uint64 *out_bytes);

#endif /* BISCUIT_PENDLOG_H */
