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

    for (c = 0; c < scan.ncols; c++)
    {
        DeltaColumn *dc = &scan.cols[c];
        char       **raw;
        char       **lower;
        uint8        mode;
        int          i;

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
    }

    MemoryContextSwitchTo(old);
    MemoryContextDelete(cxt);
}
