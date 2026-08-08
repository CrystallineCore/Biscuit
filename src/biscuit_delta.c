/*
 * biscuit_delta.c
 * Turn {slot} back into structure identities, by reading the row's text
 * out of STRCACHE and running the shared fan-out over it.
 * See biscuit_delta.h for why the text is not in the log record.
 */

#include "biscuit_common.h"
#include "biscuit_dir.h"
#include "biscuit_index.h"      /* biscuit_get_column_case_mode() */
#include "biscuit_rowstore.h"   /* biscuit_rowstore_str_read_slots() */
#include "biscuit_fanout.h"
#include "biscuit_delta.h"
#include "utils/memutils.h"

/*
 * Defined in biscuit_scan.c, registered as biscuit.diag_scan_trace in
 * _PG_init(). Same forward declaration biscuit_pattern.c already uses for
 * the same reason -- see that file's comment.
 */
extern bool biscuit_diag_scan_trace;

/*
 * How many zero-identity slots in one expansion before it is worth a
 * WARNING rather than a DEBUG1. See the report block at the end of
 * biscuit_delta_expand_slots() for why this is a threshold and not a
 * plain boolean.
 */
#define BISCUIT_DELTA_WARN_SILENT_SLOTS 8

/* ==================== COMPACTION THRESHOLD GUC ==================== */

/*
 * The old drain trigger was BISCUIT_PENDLOG_DRAIN_PAGES = 512 pages of
 * *derived* records. At ~3.9 kB of derived records per row that was a few
 * thousand rows; at 8 bytes per row the same byte threshold admits roughly
 * 500x more rows before firing (§7.1).
 *
 * Bytes were never the quantity that mattered. What a large log costs is
 * delta rebuild time, and rebuild time scales with the number of ROWS in
 * it -- each row costs one STRCACHE materialization plus one fan-out,
 * regardless of how few bytes its log record occupied. So the threshold is
 * re-derived in rows and made a GUC, because the right value follows from
 * a measurement (§11) that has not been taken yet.
 *
 * The default is a placeholder derived from the design's own estimate of
 * ~30 us/row of rebuild: 20,000 rows is ~0.6 s, which is the stated
 * ceiling on what a cold start should be willing to lose. Treat it as a
 * starting point for that measurement, not as a tuned value.
 */
int biscuit_delta_compaction_slots = 20000;

int
biscuit_delta_compaction_threshold(void)
{
    return biscuit_delta_compaction_slots;
}

/* ==================== COLUMN DISCOVERY ==================== */

/*
 * One indexed column's pair of STRCACHE roots.
 *
 * `col` uses BiscuitDirEntry addressing: BISCUIT_DIR_COL_LEGACY (-1) for
 * the single-column layout, 0-based otherwise. Note that
 * biscuit_dir_slot_for_col() maps both -1 and 0 onto directory slot 0, so
 * the two layouts are distinguished by the entry's own col field, not by
 * which directory slot the entry was found in.
 */
typedef struct DeltaColumn
{
    int32       col;
    BlockNumber raw_root;     /* is_lower = false STRCACHE pointer-dir root */
    BlockNumber lower_root;   /* is_lower = true  STRCACHE pointer-dir root */
} DeltaColumn;

typedef struct DeltaColumnScan
{
    DeltaColumn *cols;
    int          ncols;
    int          capacity;
} DeltaColumnScan;

static DeltaColumn *
delta_column_slot(DeltaColumnScan *scan, int32 col)
{
    int i;

    for (i = 0; i < scan->ncols; i++)
        if (scan->cols[i].col == col)
            return &scan->cols[i];

    if (scan->ncols == scan->capacity)
    {
        int newcap = scan->capacity ? scan->capacity * 2 : 8;

        scan->cols = (DeltaColumn *) repalloc(scan->cols,
                                              newcap * sizeof(DeltaColumn));
        scan->capacity = newcap;
    }

    scan->cols[scan->ncols].col        = col;
    scan->cols[scan->ncols].raw_root   = InvalidBlockNumber;
    scan->cols[scan->ncols].lower_root = InvalidBlockNumber;
    return &scan->cols[scan->ncols++];
}

static void
delta_column_walk_cb(const BiscuitDirEntry *entry, void *state)
{
    DeltaColumnScan *scan = (DeltaColumnScan *) state;
    DeltaColumn     *dc;

    if (entry->kind != BISCUIT_DIR_KIND_STRCACHE)
        return;
    if (entry->blob_head == InvalidBlockNumber)
        return;   /* entry exists but no pointer-array directory yet */

    dc = delta_column_slot(scan, entry->col);
    if (entry->is_lower)
        dc->lower_root = entry->blob_head;
    else
        dc->raw_root = entry->blob_head;
}

/*
 * Discover every column that has durable string state.
 *
 * Deliberately driven from the directory rather than from
 * RelationGetNumberOfAttributes(): a column whose STRCACHE entry does not
 * exist has never had a row written to it, so there is nothing to expand
 * and nothing the fan-out could produce. Reading the catalog would invent
 * columns the log cannot possibly refer to.
 */
static void
delta_discover_columns(Relation index, DeltaColumnScan *scan)
{
    int nslots = biscuit_dir_num_slots(index);
    int s;

    scan->cols     = (DeltaColumn *) palloc(8 * sizeof(DeltaColumn));
    scan->ncols    = 0;
    scan->capacity = 8;

    for (s = 0; s < nslots; s++)
        biscuit_dir_foreach_column(index, s, delta_column_walk_cb, scan);
}

/* ==================== EXPANSION ==================== */

void
biscuit_delta_expand_slots(Relation index,
                           const uint32 *slots, int nslots,
                           uint8 op,
                           BiscuitFanoutEmit emit, void *ctx)
{
    MemoryContext    cxt;
    MemoryContext    old;
    DeltaColumnScan  scan;
    int              c;
    bool            *produced;
    int              silent   = 0;
    uint32           first_silent = 0;

    if (nslots <= 0 || emit == NULL)
        return;

    Assert(op == BISCUIT_PENDING_OP_ADD || op == BISCUIT_PENDING_OP_REMOVE);

    /*
     * Everything below -- the column list, the materialized strings -- is
     * scratch. Putting it in its own context means the caller's context
     * (which for the read path is a long-lived snapshot context in
     * CacheMemoryContext) does not accumulate one copy of every string in
     * the delta for the lifetime of the snapshot. Only the identities the
     * callback chooses to record survive this function.
     */
    cxt = AllocSetContextCreate(CurrentMemoryContext,
                                "biscuit delta expand",
                                ALLOCSET_DEFAULT_SIZES);
    old = MemoryContextSwitchTo(cxt);

    delta_discover_columns(index, &scan);

    /*
     * Per-slot record of whether ANY column produced identities for it.
     *
     * A pending-log record is a bare {slot}: the text lives in STRCACHE and
     * is read back here. When that read comes up empty for every indexed
     * column, the loop below simply `continue`s and the row contributes
     * nothing to the caller's bitmap -- no row, no error, no log line. That
     * is legitimate for a genuinely NULL column value, and it is a lost row
     * if the STRCACHE write did not land (or landed on a page this read
     * cannot reach). The two are indistinguishable at the point of the
     * `continue`, which is why the condition has to be counted here and
     * reported once, rather than judged per slot.
     */
    produced = (bool *) palloc0(nslots * sizeof(bool));

    for (c = 0; c < scan.ncols; c++)
    {
        DeltaColumn *dc = &scan.cols[c];
        char       **raw;
        char       **lower;
        uint8        mode;
        int          i;

        /*
         * DIAGNOSTIC ONLY (biscuit.diag_scan_trace), reset per column --
         * see the report emitted at the bottom of this column's block for
         * what these measure and why.
         */
        int    diag_min_len = -1;
        int    diag_max_len = -1;
        int64  diag_len_sum = 0;
        int    diag_len_n   = 0;
        uint32 diag_short_example_slot  = 0;
        bool   diag_have_short_example  = false;

        /*
         * The opclass gating, re-derived from the catalog exactly as
         * biscuit_load_index() does. This must not be inferred from
         * "is there lowercase text for this slot": biscuit_ilike_ops
         * builds only the lowercase structures but STRCACHE still holds
         * the raw bytes for the row, so inferring LIKE from a non-NULL
         * raw string would fan out into case-sensitive structures the
         * write path never created -- and a drain would then materialize
         * blobs for them.
         *
         * biscuit_get_column_case_mode() indexes columns 0-based; the
         * legacy layout is column 0 of a one-column index, which is how
         * biscuit_persist_load() resolves idx->legacy_case_mode too.
         */
        mode = biscuit_get_column_case_mode(index, dc->col < 0 ? 0 : (int) dc->col);

        raw   = (char **) palloc0(nslots * sizeof(char *));
        lower = (char **) palloc0(nslots * sizeof(char *));

        if (dc->raw_root != InvalidBlockNumber)
            biscuit_rowstore_str_read_slots(index, dc->raw_root,
                                            slots, nslots, cxt, raw);

        if (dc->lower_root != InvalidBlockNumber)
            biscuit_rowstore_str_read_slots(index, dc->lower_root,
                                            slots, nslots, cxt, lower);

        for (i = 0; i < nslots; i++)
        {
            const char *s = raw[i];
            const char *l = lower[i];

            if (s == NULL && l == NULL)
                continue;   /* NULL column value, or slot never written */

            produced[i] = true;

            if (unlikely(biscuit_diag_scan_trace))
            {
                /*
                 * Measure the RAW read length specifically -- the raw
                 * pass is what pos=0/1/2 of a case-sensitive LIKE/ILIKE
                 * anchor probe (e.g. the "alpha%" reconcile trace) reads
                 * from. `l` is tracked into the same min/max only when
                 * there is no raw pass for this mode, so a LIKE-only
                 * column's numbers aren't diluted by a lowercase length
                 * that was never fanned out for it.
                 */
                int len = s ? (int) strlen(s) : (int) strlen(l);

                if (diag_min_len < 0 || len < diag_min_len)
                    diag_min_len = len;
                if (len > diag_max_len)
                    diag_max_len = len;
                diag_len_sum += len;
                diag_len_n++;

                if (len <= 1 && !diag_have_short_example)
                {
                    diag_short_example_slot = slots[i];
                    diag_have_short_example = true;
                }
            }

            /*
             * len_ge_bound = -1: unbounded.
             *
             * The write path clamped its LEN_GE loop to the live allocated
             * capacity of length_ge_bitmaps[], which is an in-memory array
             * size, not a property of the data. The delta has no such
             * array -- it is keyed sparsely by identity (§5) -- so it
             * emits the full ladder. This is the correct direction to
             * differ in: the clamp could only ever have dropped rungs the
             * data called for, and a drain creates whatever directory
             * entries it is handed.
             */
            biscuit_fanout_string(s, s ? (int) strlen(s) : 0,
                                  l, l ? (int) strlen(l) : 0,
                                  dc->col, mode, slots[i],
                                  -1, op, emit, ctx);
        }

        if (unlikely(biscuit_diag_scan_trace) && diag_len_n > 0)
            ereport(WARNING,
                    (errmsg("biscuit: diag delta read-length col=%d n=%d "
                            "min_len=%d max_len=%d avg_len=%.1f",
                            dc->col, diag_len_n, diag_min_len, diag_max_len,
                            (double) diag_len_sum / diag_len_n),
                     diag_have_short_example ?
                         errdetail("First slot with a <=1 byte read: %u.",
                                   diag_short_example_slot) : 0,
                     errhint("min_len==max_len==1 with n close to nslots means "
                             "biscuit_rowstore_str_read_slots() is returning a "
                             "truncated read for this column's post-build rows, "
                             "not that expansion or reconciliation dropped them.")));
    }

    for (c = 0; c < nslots; c++)
    {
        if (!produced[c])
        {
            if (silent == 0)
                first_silent = slots[c];
            silent++;
        }
    }

    MemoryContextSwitchTo(old);
    MemoryContextDelete(cxt);

    /*
     * Report, once per expansion, how many logged slots expanded to nothing.
     *
     * Promoted from DEBUG1 to WARNING. At DEBUG1 this was invisible at any
     * default log level, which meant the v38 run that was meant to test this
     * hypothesis could not have observed it either way -- the absence of the
     * line in that run's logs carries no information.
     *
     * The original reason for DEBUG1 was that a zero-identity expansion is
     * routine on an index over a nullable column, so a WARNING would fire
     * constantly on correct workloads. That is still true, and it is why the
     * check is now gated on BISCUIT_DELTA_WARN_SILENT_SLOTS rather than
     * simply raised: a handful per expansion is noise, and a run of dozens is
     * the failure being hunted. The threshold is a blunt instrument and the
     * right long-term answer is to distinguish "column value was NULL" from
     * "STRCACHE read found nothing" at the point of the read, which needs a
     * signal biscuit_rowstore_str_read_slots() does not currently return.
     *
     * On an all-NOT-NULL workload the expected count is zero and any firing
     * at all is meaningful.
     */
    if (silent >= BISCUIT_DELTA_WARN_SILENT_SLOTS)
        ereport(WARNING,
                (errmsg("biscuit: delta expansion produced no identities for %d of %d logged slot(s)",
                        silent, nslots),
                 errdetail("First such slot is %u (op %u). These rows will be absent "
                           "from the reconciled bitmap.",
                           first_silent, (unsigned) op),
                 errhint("Expected only for NULL column values; on a NOT NULL column "
                         "this indicates the pending log names rows whose text the "
                         "read path cannot find.")));
    else if (silent > 0)
        elog(DEBUG1,
             "biscuit: delta expansion produced no identities for %d of %d logged slot(s) "
             "(first slot %u, op %u)",
             silent, nslots, first_silent, (unsigned) op);
}
