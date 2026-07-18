-- biscuit--2.5.0--3.0.0.sql
-- ALTER EXTENSION upgrade script: biscuit 2.5.0 -> 3.0.0
--
-- What changed and why this cannot be an in-place upgrade
-- ---------------------------------------------------------
-- 3.0.0 is a clean on-disk format cutover (BISCUIT_VERSION bumped 2 -> 3,
-- see biscuit_common.h). Every existing Biscuit index's pages were written
-- by 2.5.0's persistence path, which:
--
--   - could silently swallow a save/load failure as a WARNING and fall
--     back to best-effort behavior (a from-heap rebuild on load, or just
--     giving up on a save and trying again later); and
--   - relied on biscuit_cache.c's proc-exit hook to flush any index whose
--     in-memory generation had drifted past its on-disk snapshot before
--     backend shutdown.
--
-- 3.0.0 deletes both of those unconditionally: biscuit_persist_save() and
-- biscuit_persist_load() failures now propagate as real errors instead of
-- being caught and downgraded, biscuit_load_index() has no from-heap
-- rebuild fallback at all (a missing/corrupt on-disk snapshot is now a
-- hard ERROR with a REINDEX hint), and the proc-exit flush is gone
-- entirely (it can no longer safely run this late in backend shutdown
-- once biscuit_persist_save() requires a real Relation rather than a bare
-- Oid -- see biscuit_persist.c's file header for the full history).
--
-- None of that is a wire-format change to the metapage/directory/blob
-- pages themselves, but it does change what biscuit_load_index() is
-- willing to tolerate when reading them: a 2.5.0 index that happened to
-- be relying on the old best-effort fallback behavior (e.g. one that was
-- never fully flushed before its last backend exited, under the old
-- proc-exit flush) is not guaranteed to load cleanly under 3.0.0's
-- stricter contract. Rather than ship a reader that has to distinguish
-- "cleanly-saved 2.5.0 state" from "state that only worked because 2.5.0
-- silently covered for it", every existing Biscuit index must be rebuilt
-- from scratch under 3.0.0. This mirrors the 2.4.0 -> 2.5.0 cutover's own
-- precedent (see that release's note about biscuit_text_ops) and the
-- BISCUIT_VERSION 1 -> 2 cutover before it (external-file snapshots ->
-- WAL-logged pending-list storage) -- there is no dual-format reader
-- anywhere in this codebase, by design.
--
-- What this script does and does not do
-- ---------------------------------------
-- This script updates the extension's SQL-visible objects (functions,
-- views, opclasses -- anything that lives in the catalog, not in an
-- index's own pages) to their 3.0.0 definitions. It does NOT touch, read,
-- or attempt to migrate any existing Biscuit index's on-disk pages, and
-- it does not attempt to detect which indexes need attention -- every
-- Biscuit index in the database needs the REINDEX below, unconditionally.
--
-- There is no in-place, no-REINDEX upgrade path for the on-disk format,
-- and none is attempted here. Until you REINDEX, an existing Biscuit
-- index will fail its next cold load (fresh backend, or any load-triggering
-- statement after a relcache invalidation) with an ERROR pointing here.
--
-- Required manual step after this script runs, for every Biscuit index:
--
--     REINDEX INDEX CONCURRENTLY <index_name>;   -- preferred: no write lock
--     -- or, if REINDEX CONCURRENTLY isn't available/suitable:
--     REINDEX INDEX <index_name>;
--
-- To find every Biscuit index that needs this:
--
--     SELECT schema_name, index_name FROM biscuit_indexes;

\echo Use "ALTER EXTENSION biscuit UPDATE TO '3.0.0'" to load this file. \quit

-- ==================== NEW OBSERVABILITY OBJECTS ====================

-- Function to get pending-list (unmerged write volume) stats as columns,
-- for use in views/ORDER BY rather than biscuit_index_stats()'s free text.
CREATE FUNCTION biscuit_pending_list_stats(
    index_oid oid,
    OUT pending_list_limit  int,
    OUT total_pending_bytes bigint,
    OUT total_drains        bigint
)
AS 'MODULE_PATHNAME', 'biscuit_pending_list_stats'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_pending_list_stats(oid) IS
'Returns the configured per-structure drain threshold and the (VACUUM-refreshed, so up to one VACUUM cycle stale) total undrained pending-list bytes and lifetime drain count for a Biscuit index -- "how much unmerged write volume is sitting in this index right now".
Usage: SELECT * FROM biscuit_pending_list_stats(''index_name''::regclass::oid);';

GRANT EXECUTE ON FUNCTION biscuit_pending_list_stats(oid) TO PUBLIC;

-- Operational visibility: how much unmerged write volume (pending-list
-- bytes not yet drained into a compacted blob) is sitting in each Biscuit
-- index right now.
CREATE VIEW biscuit_pending_list_usage AS
SELECT
    n.nspname AS schema_name,
    c.relname AS index_name,
    t.relname AS table_name,
    s.pending_list_limit,
    s.total_pending_bytes,
    pg_size_pretty(s.total_pending_bytes) AS total_pending_pretty,
    s.total_drains,
    c.oid AS index_oid
FROM
    pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_am am ON am.oid = c.relam
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_class t ON t.oid = i.indrelid
    CROSS JOIN LATERAL biscuit_pending_list_stats(c.oid) s
WHERE
    am.amname = 'biscuit'
    AND c.relkind = 'i'
ORDER BY
    s.total_pending_bytes DESC;

COMMENT ON VIEW biscuit_pending_list_usage IS
'Shows unmerged write volume (pending-list bytes not yet drained into a compacted blob) per Biscuit index, refreshed once per VACUUM. A large total_pending_bytes relative to index_size, or a low total_drains under heavy write load, suggests biscuit.pending_list_limit may be set too high for this workload.';

GRANT SELECT ON biscuit_pending_list_usage TO PUBLIC;

-- ==================== UPDATED EXISTING OBJECTS ====================

CREATE OR REPLACE FUNCTION biscuit_size_pretty(index_name text)
RETURNS text
AS $$
DECLARE
    bytes bigint;
    kb numeric;
    mb numeric;
    gb numeric;
BEGIN
    bytes := biscuit_index_memory_size(index_name::regclass::oid);

    IF bytes < 1024 THEN
        RETURN bytes || ' bytes';
    ELSIF bytes < 1024 * 1024 THEN
        kb := round((bytes::numeric / 1024), 2);
        RETURN kb || ' KB (' || bytes || ' bytes)';
    ELSIF bytes < 1024 * 1024 * 1024 THEN
        mb := round((bytes::numeric / (1024 * 1024)), 2);
        RETURN mb || ' MB (' || bytes || ' bytes)';
    ELSE
        gb := round((bytes::numeric / (1024 * 1024 * 1024)), 2);
        RETURN gb || ' GB (' || bytes || ' bytes)';
    END IF;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;

-- Add pending-list bytes alongside existing memory/disk size columns.
CREATE OR REPLACE VIEW biscuit_memory_usage AS
SELECT
    i.schemaname,
    i.tablename,
    i.indexname,
    biscuit_index_memory_size(c.oid) AS bytes,
    pg_size_pretty(biscuit_index_memory_size(c.oid)) AS human_readable,
    pg_size_pretty(pg_relation_size(c.oid)) AS disk_size,
    s.total_pending_bytes AS pending_bytes,
    pg_size_pretty(s.total_pending_bytes) AS pending_pretty
FROM pg_indexes i
JOIN pg_class c ON c.relname = i.indexname
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = i.schemaname
JOIN pg_am am ON am.oid = c.relam
CROSS JOIN LATERAL biscuit_pending_list_stats(c.oid) s
WHERE am.amname = 'biscuit'
ORDER BY biscuit_index_memory_size(c.oid) DESC;

COMMENT ON VIEW biscuit_memory_usage IS
'In-memory size, on-disk size, and unmerged pending-list bytes (see biscuit_pending_list_usage for more detail) per Biscuit index.';

COMMENT ON FUNCTION biscuit_index_stats(oid) IS
'Returns detailed statistics for a Biscuit index including CRUD counts, tombstones, pending-list (unmerged write volume), and memory usage.
Usage: SELECT biscuit_index_stats(''index_name''::regclass::oid);';

-- ==================== VERSION HISTORY ====================

INSERT INTO biscuit_version_table (version, description) VALUES
('3.0.0', 'On-disk format cutover to WAL-logged directory + compacted-blob + pending-list storage (BISCUIT_VERSION 3). Removes the old external-file snapshot mechanism entirely, along with its best-effort save/proc-exit-flush machinery -- persistence failures now propagate as errors. Adds biscuit_pending_list_stats()/biscuit_pending_list_usage for pending-list (unmerged write volume) observability. REQUIRES REINDEX of every existing Biscuit index; there is no in-place upgrade for the on-disk format.');

-- ==================== REQUIRED MANUAL STEP ====================

DO $$
DECLARE
    n int;
BEGIN
    SELECT count(*) INTO n FROM biscuit_indexes;
    IF n > 0 THEN
        RAISE WARNING
            'biscuit 3.0.0: % existing Biscuit index(es) found. Their on-disk state was written under the pre-3.0.0 best-effort persistence contract and must be rebuilt: run REINDEX INDEX CONCURRENTLY (or REINDEX INDEX) on each one listed by "SELECT schema_name, index_name FROM biscuit_indexes". Until you do, a cold load of an un-REINDEXed index will fail with an ERROR.',
            n;
    END IF;
END;
$$;
