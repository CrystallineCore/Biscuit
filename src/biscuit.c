/*
 * biscuit.c
 * PostgreSQL extension entry point for the Biscuit Index Access Method.
 *
 * Responsibilities:
 *   - PG_MODULE_MAGIC / PG_MODULE_MAGIC_EXT
 *   - biscuit_handler()   – registers the AM callback table
 *   - SQL-callable diagnostic / introspection functions
 *     (biscuit_version, biscuit_has_roaring, biscuit_build_info,
 *      biscuit_build_info_json, biscuit_roaring_version,
 *      biscuit_index_stats, biscuit_index_memory_size,
 *      biscuit_like_support)
 *
 * All heavy lifting is in the sub-modules:
 *   biscuit_bitmap.c   – RoaringBitmap abstraction
 *   biscuit_utf8.c     – UTF-8 helpers & type conversion
 *   biscuit_cache.c    – session index cache
 *   biscuit_tid.c      – TID sorting & collection
 *   biscuit_pattern.c  – LIKE/ILIKE pattern matching
 *   biscuit_index.c    – build, load, CRUD, AM maintenance callbacks
 *   biscuit_scan.c     – beginscan / rescan / gettuple / getbitmap / endscan
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_index.h"
#include "biscuit_scan.h"
#include "biscuit_preload.h"
#include "biscuit_tid.h"

/* ================================================================
 * MODULE MAGIC
 * ================================================================ */

#ifdef PG_MODULE_MAGIC_EXT
    PG_MODULE_MAGIC_EXT(.name = "biscuit", .version = BISCUIT_LIBRARY_VERSION);
#else
    PG_MODULE_MAGIC;
#endif

/* ================================================================
 * _PG_init – called once when the library is loaded.
 * Registers shared-memory hooks and the background preload worker.
 * Without this, biscuit_preload_shmem is always NULL and the
 * background worker is never started.
 * ================================================================ */

void _PG_init(void);

void
_PG_init(void)
{
    biscuit_preload_init();
}

/* ================================================================
 * VERSION / BUILD INFO
 * ================================================================ */

PG_FUNCTION_INFO_V1(biscuit_has_roaring);
Datum
biscuit_has_roaring(PG_FUNCTION_ARGS)
{
    (void) fcinfo;
#ifdef HAVE_ROARING
    PG_RETURN_BOOL(true);
#else
    PG_RETURN_BOOL(false);
#endif
}

PG_FUNCTION_INFO_V1(biscuit_version);
Datum
biscuit_version(PG_FUNCTION_ARGS)
{
    (void) fcinfo;
    PG_RETURN_TEXT_P(cstring_to_text(BISCUIT_LIBRARY_VERSION));
}

/* Set-returning function: returns two rows (roaring support, PG version). */
PG_FUNCTION_INFO_V1(biscuit_build_info);
Datum
biscuit_build_info(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    int              call_cntr;
    int              max_calls;
    TupleDesc        tupdesc;
    AttInMetadata   *attinmeta;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;

        funcctx    = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));

        attinmeta          = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;
        funcctx->max_calls = 2;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx    = SRF_PERCALL_SETUP();
    call_cntr  = funcctx->call_cntr;
    max_calls  = funcctx->max_calls;
    attinmeta  = funcctx->attinmeta;

    if (call_cntr < max_calls)
    {
        char      **values = (char **) palloc(3 * sizeof(char *));
        HeapTuple   tuple;
        Datum       result;

        if (call_cntr == 0)
        {
            values[0] = pstrdup("CRoaring Bitmaps");
#ifdef HAVE_ROARING
            values[1] = pstrdup("true");
            values[2] = pstrdup("High-performance bitmap operations enabled");
#else
            values[1] = pstrdup("false");
            values[2] = pstrdup("Using fallback bitmap implementation (reduced performance)");
#endif
        }
        else
        {
            values[0] = pstrdup("PostgreSQL");
            values[1] = pstrdup("true");
            values[2] = psprintf("Compiled for PostgreSQL %s", PG_VERSION);
        }

        tuple  = BuildTupleFromCStrings(attinmeta, values);
        result = HeapTupleGetDatum(tuple);

        pfree(values[0]); pfree(values[1]); pfree(values[2]); pfree(values);

        SRF_RETURN_NEXT(funcctx, result);
    }
    else
        SRF_RETURN_DONE(funcctx);
}

PG_FUNCTION_INFO_V1(biscuit_build_info_json);
Datum
biscuit_build_info_json(PG_FUNCTION_ARGS)
{
    StringInfoData buf;

    (void) fcinfo;
    initStringInfo(&buf);

    appendStringInfo(&buf, "{");
    appendStringInfo(&buf, "\"version\": \"%s\",", BISCUIT_LIBRARY_VERSION);

#ifdef HAVE_ROARING
    appendStringInfo(&buf, "\"roaring_enabled\": true,");
    appendStringInfo(&buf, "\"roaring_version\": \"%d.%d.%d\",",
                     ROARING_VERSION_MAJOR, ROARING_VERSION_MINOR, ROARING_VERSION_REVISION);
#else
    appendStringInfo(&buf, "\"roaring_enabled\": false,");
#endif

    appendStringInfo(&buf, "\"postgres_version\": \"%s\"", PG_VERSION);
    appendStringInfo(&buf, "}");

    PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

#ifdef HAVE_ROARING
#include <roaring/roaring.h>
PG_FUNCTION_INFO_V1(biscuit_roaring_version);
Datum
biscuit_roaring_version(PG_FUNCTION_ARGS)
{
    char ver[64];
    snprintf(ver, sizeof(ver), "%d.%d.%d",
             ROARING_VERSION_MAJOR, ROARING_VERSION_MINOR, ROARING_VERSION_REVISION);
    PG_RETURN_TEXT_P(cstring_to_text(ver));
}
#else
PG_FUNCTION_INFO_V1(biscuit_roaring_version);
Datum
biscuit_roaring_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_NULL();
}
#endif

/* ================================================================
 * INDEX HANDLER – registers the AM callback table
 * ================================================================ */

PG_FUNCTION_INFO_V1(biscuit_handler);
Datum
biscuit_handler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    (void) fcinfo;

    amroutine->amstrategies          = 4;
    amroutine->amsupport             = 2;
    amroutine->amoptsprocnum         = 0;
    amroutine->amcanorder            = false;
    amroutine->amcanorderbyop        = false;
    amroutine->amcanbackward         = false;
    amroutine->amcanunique           = false;
    amroutine->amcanmulticol         = true;
    amroutine->amoptionalkey         = true;
    amroutine->amsearcharray         = false;
    amroutine->amsearchnulls         = false;
    amroutine->amstorage             = false;
    amroutine->amclusterable         = false;
    amroutine->ampredlocks           = false;
    amroutine->amcaninclude          = false;
    amroutine->amusemaintenanceworkmem = false;
    amroutine->amsummarizing         = false;
    amroutine->amparallelvacuumoptions = 0;
    amroutine->amkeytype             = InvalidOid;

    amroutine->ambuild               = biscuit_build;
    amroutine->ambuildempty          = biscuit_buildempty;
    amroutine->aminsert              = biscuit_insert;
    amroutine->ambulkdelete          = biscuit_bulkdelete;
    amroutine->amvacuumcleanup       = biscuit_vacuumcleanup;
    amroutine->amcanreturn           = biscuit_canreturn;
    amroutine->amcostestimate        = biscuit_costestimate;
    amroutine->amoptions             = biscuit_options;
    amroutine->amproperty            = NULL;
    amroutine->ambuildphasename      = NULL;
    amroutine->amvalidate            = biscuit_validate;
    amroutine->amadjustmembers       = biscuit_adjustmembers;
    amroutine->ambeginscan           = biscuit_beginscan;
    amroutine->amrescan              = biscuit_rescan;
    amroutine->amgettuple            = biscuit_gettuple;
    amroutine->amgetbitmap           = biscuit_getbitmap;
    amroutine->amendscan             = biscuit_endscan;
    amroutine->ammarkpos             = NULL;
    amroutine->amrestrpos            = NULL;
    #if PG_VERSION_NUM >= 180000
    amroutine->amcanparallel            = true;
    amroutine->amestimateparallelscan   = biscuit_estimateparallelscan;
    amroutine->aminitparallelscan       = biscuit_initparallelscan;
    amroutine->amparallelrescan         = biscuit_parallelrescan;
    #else
    amroutine->amcanparallel            = false;
    amroutine->amestimateparallelscan   = NULL;
    amroutine->aminitparallelscan       = NULL;
    amroutine->amparallelrescan         = NULL;
    #endif

    PG_RETURN_POINTER(amroutine);
}

/* ================================================================
 * OPERATOR SUPPORT
 * ================================================================ */

PG_FUNCTION_INFO_V1(biscuit_like_support);
Datum
biscuit_like_support(PG_FUNCTION_ARGS)
{
    (void) fcinfo;
    PG_RETURN_BOOL(true);
}

/* ================================================================
 * INDEX STATISTICS (biscuit_index_stats)
 * ================================================================ */

PG_FUNCTION_INFO_V1(biscuit_index_stats);
Datum
biscuit_index_stats(PG_FUNCTION_ARGS)
{
    Oid           indexoid = PG_GETARG_OID(0);
    Relation      index;
    BiscuitIndex *idx;
    StringInfoData buf;
    int            active_records = 0;
    int            i;

    index = index_open(indexoid, AccessShareLock);

    idx = (BiscuitIndex *) index->rd_amcache;
    if (!idx) { idx = biscuit_load_index(index); index->rd_amcache = idx; }

    for (i = 0; i < idx->num_records; i++)
    {
        bool has_data = (idx->num_columns == 1 && idx->data_cache[i] != NULL) ||
                        (idx->num_columns > 1 && idx->column_data_cache[0][i] != NULL);

        bool is_tombstoned = false;
        if (!has_data) continue;
        
        
#ifdef HAVE_ROARING
        is_tombstoned = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
#else
        { uint32_t bl = i >> 6, bt = i & 63;
          is_tombstoned = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
#endif
        if (!is_tombstoned) active_records++;
    }

    initStringInfo(&buf);
    appendStringInfo(&buf, "Biscuit Index Statistics\n");
    appendStringInfo(&buf, "==========================================\n");
    appendStringInfo(&buf, "Index: %s\n",        RelationGetRelationName(index));
    appendStringInfo(&buf, "Active records: %d\n", active_records);
    appendStringInfo(&buf, "Total slots: %d\n",   idx->num_records);
    appendStringInfo(&buf, "Free slots: %d\n",    idx->free_count);
    appendStringInfo(&buf, "Tombstones: %d\n",    idx->tombstone_count);
    appendStringInfo(&buf, "Max length: %d\n",    idx->max_len);
    appendStringInfo(&buf, "------------------------\n");
    appendStringInfo(&buf, "CRUD Statistics:\n");
    appendStringInfo(&buf, "  Inserts: %lld\n",  (long long) idx->insert_count);
    appendStringInfo(&buf, "  Updates: %lld\n",  (long long) idx->update_count);
    appendStringInfo(&buf, "  Deletes: %lld\n",  (long long) idx->delete_count);
    appendStringInfo(&buf, "------------------------\n");
    appendStringInfo(&buf, "Active Optimizations:\n");
    appendStringInfo(&buf, "  \u2713 1. Skip wildcard intersections\n");
    appendStringInfo(&buf, "  \u2713 2. Early termination on empty\n");
    appendStringInfo(&buf, "  \u2713 3. Avoid redundant copies\n");
    appendStringInfo(&buf, "  \u2713 4. Optimized single-part patterns\n");
    appendStringInfo(&buf, "  \u2713 5. Skip unnecessary length ops\n");
    appendStringInfo(&buf, "  \u2713 6. TID sorting for sequential I/O\n");
    appendStringInfo(&buf, "  \u2713 7. Batch TID insertion\n");
    appendStringInfo(&buf, "  \u2713 8. Direct bitmap iteration\n");
    appendStringInfo(&buf, "  \u2713 9. Parallel bitmap scan support\n");
    appendStringInfo(&buf, "  \u2713 10. Batch cleanup on threshold\n");
    appendStringInfo(&buf, "  \u2713 11. Skip sorting for bitmap scans\n");
    appendStringInfo(&buf, "  \u2713 12. LIMIT-aware TID collection\n");

    index_close(index, AccessShareLock);
    PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/* ================================================================
 * INDEX MEMORY SIZE (biscuit_index_memory_size)
 * ================================================================ */

PG_FUNCTION_INFO_V1(biscuit_index_memory_size);
Datum
biscuit_index_memory_size(PG_FUNCTION_ARGS)
{
    Oid           indexoid = PG_GETARG_OID(0);
    Relation      index;
    BiscuitIndex *idx;
    size_t        total_bytes = 0;
    size_t        string_bytes = 0;
    size_t        bitmap_bytes = 0;
    size_t        metadata_bytes = 0;
    int           i, ch, col;

    index = index_open(indexoid, AccessShareLock);
    if (!index)
        elog(ERROR, "Could not open index with OID %u", indexoid);

    idx = (BiscuitIndex *) index->rd_amcache;
    if (!idx) { idx = biscuit_load_index(index); index->rd_amcache = idx; }
    if (!idx) { index_close(index, AccessShareLock); PG_RETURN_INT64(0); }

    metadata_bytes += sizeof(BiscuitIndex);
    if (idx->tids) metadata_bytes += idx->capacity * sizeof(ItemPointerData);

    if (idx->num_columns == 1)
    {
        if (idx->data_cache)
        {
            metadata_bytes += idx->capacity * sizeof(char *);
            for (i = 0; i < idx->num_records && i < idx->capacity; i++)
                if (idx->data_cache[i]) string_bytes += strlen(idx->data_cache[i]) + 1;
        }
        if (idx->data_cache_lower)
        {
            metadata_bytes += idx->capacity * sizeof(char *);
            for (i = 0; i < idx->num_records && i < idx->capacity; i++)
                if (idx->data_cache_lower[i]) string_bytes += strlen(idx->data_cache_lower[i]) + 1;
        }

        for (ch = 0; ch < CHAR_RANGE; ch++)
        {
            bitmap_bytes += biscuit_charindex_memory_usage(&idx->pos_idx_legacy[ch]);
            bitmap_bytes += biscuit_charindex_memory_usage(&idx->neg_idx_legacy[ch]);
            bitmap_bytes += biscuit_roaring_memory_usage(idx->char_cache_legacy[ch]);
            bitmap_bytes += biscuit_charindex_memory_usage(&idx->pos_idx_lower[ch]);
            bitmap_bytes += biscuit_charindex_memory_usage(&idx->neg_idx_lower[ch]);
            bitmap_bytes += biscuit_roaring_memory_usage(idx->char_cache_lower[ch]);
        }

        if (idx->length_bitmaps_legacy && idx->max_length_legacy > 0)
        {
            metadata_bytes += idx->max_length_legacy * sizeof(RoaringBitmap *);
            for (i = 0; i < idx->max_length_legacy; i++)
                if (idx->length_bitmaps_legacy[i])
                    bitmap_bytes += biscuit_roaring_memory_usage(idx->length_bitmaps_legacy[i]);
        }
        if (idx->length_ge_bitmaps_legacy && idx->max_length_legacy > 0)
        {
            metadata_bytes += idx->max_length_legacy * sizeof(RoaringBitmap *);
            for (i = 0; i < idx->max_length_legacy; i++)
                if (idx->length_ge_bitmaps_legacy[i])
                    bitmap_bytes += biscuit_roaring_memory_usage(idx->length_ge_bitmaps_legacy[i]);
        }
    }
    else if (idx->num_columns > 1)
    {
        metadata_bytes += idx->num_columns * sizeof(Oid);
        metadata_bytes += idx->num_columns * sizeof(FmgrInfo);
        metadata_bytes += idx->num_columns * sizeof(char **);

        if (idx->column_data_cache)
        {
            for (col = 0; col < idx->num_columns; col++)
            {
                if (idx->column_data_cache[col])
                {
                    metadata_bytes += idx->capacity * sizeof(char *);
                    for (i = 0; i < idx->num_records && i < idx->capacity; i++)
                        if (idx->column_data_cache[col][i])
                            string_bytes += strlen(idx->column_data_cache[col][i]) + 1;
                }
            }
        }

        if (idx->column_indices)
        {
            metadata_bytes += idx->num_columns * sizeof(ColumnIndex);
            for (col = 0; col < idx->num_columns; col++)
                bitmap_bytes += biscuit_columnindex_memory_usage(&idx->column_indices[col]);
        }
    }

    if (idx->tombstones) bitmap_bytes += biscuit_roaring_memory_usage(idx->tombstones);
    if (idx->free_list && idx->free_capacity > 0)
        metadata_bytes += idx->free_capacity * sizeof(uint32_t);

    total_bytes = metadata_bytes + string_bytes + bitmap_bytes;

    index_close(index, AccessShareLock);
    PG_RETURN_INT64((int64) total_bytes);
}
