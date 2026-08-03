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
 * Durably record one delta. In the common case this is a single
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
 * Unlike the per-structure path this replaced, there is NO directory
 * lookup and NO directory update here -- the record is self-describing,
 * so the structure need not even have a directory entry yet. Entries are
 * created lazily at drain time for whatever structures actually appear in
 * the log. That removes the biscuit_dir_find() chain walk from the hot
 * path entirely, which was a per-structure cost on top of the page cost.
 *
 * Returns the log's undrained byte count after the append, so the caller
 * can apply the pending_list_limit drain trigger without a second
 * metapage read.
 */
extern uint64 biscuit_pendlog_append(Relation index,
                                      int32 col, bool is_lower, uint8 kind,
                                      int32 ch, int32 position,
                                      uint32 rec_idx, uint8 op);

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

typedef struct BiscuitPendLogDelta
{
    uint32  rec_idx;
    uint8   op;
} BiscuitPendLogDelta;

typedef struct BiscuitPendLogEntry
{
    BiscuitPendLogKey     key;      /* must be first (dynahash) */
    BiscuitPendLogDelta  *deltas;
    int                   ndeltas;
    int                   capacity;
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
 * Apply a structure's deltas to a bitmap, in log order (order matters:
 * add-then-remove and remove-then-add of the same rec_idx differ).
 */
extern void biscuit_pendlog_apply(const BiscuitPendLogEntry *entry,
                                   RoaringBitmap *target);

/* Drop any cached snapshot (relcache invalidation, drain, index drop). */
extern void biscuit_pendlog_invalidate(Oid indexoid);

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
 */
extern int biscuit_pendlog_drain_all(Relation index, bool wait);

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
