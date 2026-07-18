-- biscuit--3.0.0.sql
-- SQL installation script for Biscuit Index Access Method
-- PostgreSQL 15+ compatible with full CRUD support and multi-column indexes
--
-- Features:
-- - LIKE and ILIKE support (case-sensitive and case-insensitive), selectable
--   per index via three opclasses: biscuit_ops (both, default),
--   biscuit_like_ops (LIKE only), biscuit_ilike_ops (ILIKE only) -- see
--   the OPERATOR CLASSES section below for details and cost tradeoffs
-- - O(1) lazy deletion with tombstones
-- - Incremental insert/update
-- - Automatic slot reuse
-- - Full VACUUM integration
-- - Multi-column index support
-- - Mixed data type support
-- - WAL-logged directory + compacted-blob + pending-list on-disk storage
--   (BISCUIT_VERSION 3): the old external-file snapshot mechanism and its
--   best-effort/proc-exit-flush machinery are gone. Persistence failures
--   now propagate as errors instead of being silently swallowed.
--
-- NOTE: this is a fresh-install script for new databases. Upgrading an
-- existing 2.5.0 (or earlier) installation MUST go through
-- biscuit--2.5.0--3.0.0.sql via ALTER EXTENSION biscuit UPDATE, which
-- requires REINDEXing every existing Biscuit index -- there is no
-- in-place, no-REINDEX upgrade path for this cutover (see that script's
-- header for why).

-- Complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION biscuit" to load this file. \quit

-- ==================== CORE INDEX ACCESS METHOD ====================

-- Create the index access method handler function
CREATE FUNCTION biscuit_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME', 'biscuit_handler'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_handler(internal) IS
'Index access method handler for Biscuit indexes - provides callbacks for index operations';

-- Create the Biscuit index access method
CREATE ACCESS METHOD biscuit TYPE INDEX HANDLER biscuit_handler;

COMMENT ON ACCESS METHOD biscuit IS
'Biscuit index access method: High-performance pattern matching for LIKE/ILIKE queries with multi-column support';

-- ==================== OPERATOR SUPPORT ====================

-- FIX #1 (was RETURNS bool): Planner support functions must be declared
-- RETURNS internal in SQL.  The C implementation already matches this
-- contract; only the SQL declaration was wrong.
CREATE FUNCTION biscuit_like_support(internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'biscuit_like_support'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_like_support(internal) IS
'Support function that tells the planner Biscuit can handle LIKE/ILIKE pattern matching';

-- ==================== BUILD INFO & DIAGNOSTICS ====================

-- Function to check if Roaring Bitmaps support is enabled
CREATE FUNCTION biscuit_has_roaring()
RETURNS boolean
AS 'MODULE_PATHNAME', 'biscuit_has_roaring'
LANGUAGE C STRICT IMMUTABLE;

COMMENT ON FUNCTION biscuit_has_roaring() IS
'Returns true if the extension was compiled with CRoaring bitmap support, false otherwise.
If false, the extension will use a fallback bitmap implementation with reduced performance.';

-- Function to get build information
CREATE FUNCTION biscuit_build_info()
RETURNS TABLE (
    feature text,
    enabled boolean,
    description text
)
AS 'MODULE_PATHNAME', 'biscuit_build_info'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_build_info() IS
'Returns build-time configuration information including library versions and enabled features.
Usage: SELECT * FROM biscuit_build_info();';

-- Alternative: Get build info as JSON
CREATE FUNCTION biscuit_build_info_json()
RETURNS text
AS 'MODULE_PATHNAME', 'biscuit_build_info_json'
LANGUAGE C STRICT IMMUTABLE;

COMMENT ON FUNCTION biscuit_build_info_json() IS
'Returns build configuration as JSON string.
Usage: SELECT biscuit_build_info_json();';

-- Get Roaring library version (returns NULL if not compiled with Roaring)
CREATE FUNCTION biscuit_roaring_version()
RETURNS text
AS 'MODULE_PATHNAME', 'biscuit_roaring_version'
LANGUAGE C STRICT IMMUTABLE;

COMMENT ON FUNCTION biscuit_roaring_version() IS
'Returns CRoaring library version if available, NULL otherwise.
Usage: SELECT biscuit_roaring_version();';

-- Function to get version information
CREATE FUNCTION biscuit_version()
RETURNS text
AS 'MODULE_PATHNAME', 'biscuit_version'
LANGUAGE C STRICT IMMUTABLE;

COMMENT ON FUNCTION biscuit_version() IS
'Returns the Biscuit extension version string';

-- Function to get index statistics and health information
CREATE FUNCTION biscuit_index_stats(oid)
RETURNS text
AS 'MODULE_PATHNAME', 'biscuit_index_stats'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_index_stats(oid) IS
'Returns detailed statistics for a Biscuit index including CRUD counts, tombstones, pending-list (unmerged write volume), and memory usage.
Usage: SELECT biscuit_index_stats(''index_name''::regclass::oid);';

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

-- ==================== OPERATOR CLASSES ====================
--
-- Three opclasses are provided for TEXT so callers can choose how much
-- build-time/memory cost to pay for the case-insensitive (ILIKE) side of
-- the index. Building the `_lower` position/length/char-cache structures
-- (see biscuit_index.c / biscuit_common.h) roughly doubles per-column
-- memory usage and build time, so a LIKE-only workload should not have
-- to pay for it.
--
-- Each opclass declares its own opfamily so that the *set of operators
-- pg_amop knows about* is what actually gates planner eligibility --
-- e.g. an index built with biscuit_like_ops will never even be
-- considered by the planner for an ILIKE qual, regardless of what
-- biscuit_costestimate() would have said. biscuit_build()/
-- biscuit_load_index() separately introspect which opclass was chosen
-- per column (via the index relation's pg_index/pg_opclass data) to
-- decide whether to allocate/populate the case-sensitive structures,
-- the case-insensitive `_lower` structures, or both.
--
--   biscuit_ops        - LIKE + ILIKE (default; builds both structure sets)
--   biscuit_like_ops    - LIKE / NOT LIKE only (case-sensitive structures only)
--   biscuit_ilike_ops   - ILIKE / NOT ILIKE only (case-insensitive structures only)
--
-- FIX #2 (was FUNCTION 1 biscuit_like_support(internal) with RETURNS bool):
-- Now that biscuit_like_support is correctly declared RETURNS internal above,
-- registering it as opclass support function 1 works as intended.

-- ---- biscuit_ops (default): LIKE + ILIKE, builds both structure sets ----
CREATE OPERATOR FAMILY biscuit_ops USING biscuit;

CREATE OPERATOR CLASS biscuit_ops
DEFAULT FOR TYPE text USING biscuit FAMILY biscuit_ops AS
    OPERATOR 1 ~~ (text, text),      -- LIKE
    OPERATOR 2 !~~ (text, text),     -- NOT LIKE
    OPERATOR 3 ~~* (text, text),     -- ILIKE (case-insensitive)
    OPERATOR 4 !~~* (text, text),    -- NOT ILIKE (case-insensitive)
    FUNCTION 1 biscuit_like_support(internal);

COMMENT ON OPERATOR CLASS biscuit_ops USING biscuit IS
'Default operator class for text types - supports LIKE, NOT LIKE, ILIKE, and
NOT ILIKE. Builds both the case-sensitive and case-insensitive (_lower)
index structures for every column, so it costs roughly 2x the memory/build
time of biscuit_like_ops or biscuit_ilike_ops alone. Use biscuit_like_ops or
biscuit_ilike_ops instead if you only need one case-sensitivity mode.
VARCHAR types will implicitly cast to text to use this class.';

-- ---- biscuit_like_ops: LIKE / NOT LIKE only ----
CREATE OPERATOR FAMILY biscuit_like_ops USING biscuit;

CREATE OPERATOR CLASS biscuit_like_ops
FOR TYPE text USING biscuit FAMILY biscuit_like_ops AS
    OPERATOR 1 ~~ (text, text),      -- LIKE
    OPERATOR 2 !~~ (text, text),     -- NOT LIKE
    FUNCTION 1 biscuit_like_support(internal);

COMMENT ON OPERATOR CLASS biscuit_like_ops USING biscuit IS
'Operator class for text types - supports LIKE and NOT LIKE only. ILIKE and
NOT ILIKE queries cannot use an index built with this opclass; the planner
will fall back to a sequential scan (or another eligible index) for those
quals. Skips building the case-insensitive (_lower) index structures,
roughly halving memory usage and build time versus biscuit_ops.
VARCHAR types will implicitly cast to text to use this class.';

-- ---- biscuit_ilike_ops: ILIKE / NOT ILIKE only ----
CREATE OPERATOR FAMILY biscuit_ilike_ops USING biscuit;

CREATE OPERATOR CLASS biscuit_ilike_ops
FOR TYPE text USING biscuit FAMILY biscuit_ilike_ops AS
    OPERATOR 3 ~~* (text, text),     -- ILIKE (case-insensitive)
    OPERATOR 4 !~~* (text, text),    -- NOT ILIKE (case-insensitive)
    FUNCTION 1 biscuit_like_support(internal);

COMMENT ON OPERATOR CLASS biscuit_ilike_ops USING biscuit IS
'Operator class for text types - supports ILIKE and NOT ILIKE only. LIKE and
NOT LIKE queries cannot use an index built with this opclass; the planner
will fall back to a sequential scan (or another eligible index) for those
quals. Skips building the case-sensitive index structures, roughly halving
memory usage and build time versus biscuit_ops.
VARCHAR types will implicitly cast to text to use this class.';

-- FIX #9: An earlier revision attempted a native biscuit_bpchar_ops
-- operator class for CHAR(n)/bpchar columns, declared as:
--   OPERATOR 1 ~~ (bpchar, bpchar), ...
-- This failed with "operator does not exist: character ~~ character"
-- because PostgreSQL has no ~~/!~~/~~*/!~~* operators defined directly
-- over (bpchar, bpchar) at all -- unlike B-tree's bpchar_pattern_ops,
-- which works by comparing raw bytes and therefore doesn't need a
-- LIKE-specific bpchar operator, Biscuit's pattern matching has no such
-- byte-level equivalence to lean on without dedicated C support.
--
-- The only LIKE/ILIKE operators that exist are over (text, text). When you
-- write `char_col LIKE 'pattern'`, PostgreSQL resolves this by casting the
-- *column* to text -- e.g. `(char_col)::text ~~ 'pattern'::text` -- not by
-- finding some bpchar-flavored operator. A plain index on the bare bpchar
-- column cannot serve that cast expression, regardless of access method.
--
-- There are two real ways to get indexed LIKE/ILIKE on a CHAR(n) column:
--
-- 1. (Available now, no code changes) Build a Biscuit EXPRESSION index on
--    the text cast, which biscuit_ops (or biscuit_like_ops / biscuit_ilike_ops,
--    depending on which operators you need) can serve directly:
--      CREATE INDEX idx_name ON my_table USING biscuit ((char_col::text));
--    Queries using `char_col::text LIKE 'pattern'` or `char_col LIKE
--    'pattern'` (where the planner inserts the same cast) can then use it.
--
-- 2. (Requires new C code, not provided by this script) Implement genuine
--    bpchar-native LIKE/ILIKE operators and matching functions in C, then
--    build a real biscuit_bpchar_ops around them. This is the only way to
--    index the *padded* bpchar representation directly rather than via
--    an expression on the cast value.
--
-- This script ships with option 1 as a documented workaround; CHAR(n)
-- columns work today via an expression index rather than a dedicated
-- opclass on bpchar.

-- ==================== HELPER VIEWS ====================

-- View to show all Biscuit indexes in the database
CREATE VIEW biscuit_indexes AS
SELECT
    n.nspname AS schema_name,
    c.relname AS index_name,
    t.relname AS table_name,
    i.indnatts AS num_columns,
    CASE
        WHEN i.indnatts = 1 THEN
            CASE
                WHEN i.indkey[0] = 0 THEN
                    '(expression)'  -- Expression index
                ELSE
                    (SELECT a.attname FROM pg_attribute a
                     WHERE a.attrelid = t.oid AND a.attnum = i.indkey[0])
            END
        ELSE
            i.indnatts::text || ' columns'
    END AS columns,
    pg_size_pretty(pg_relation_size(c.oid)) AS index_size,
    c.oid AS index_oid
FROM
    pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_am am ON am.oid = c.relam
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_class t ON t.oid = i.indrelid
WHERE
    am.amname = 'biscuit'
    AND c.relkind = 'i'
ORDER BY
    n.nspname, c.relname;

COMMENT ON VIEW biscuit_indexes IS
'Shows all Biscuit indexes in the current database.
For expression indexes (like age::text), shows "(expression)" - use biscuit_indexes_detailed for full definitions.';

-- Detailed multi-column index view (handles expression indexes correctly)
CREATE VIEW biscuit_indexes_detailed AS
SELECT
    n.nspname AS schema_name,
    c.relname AS index_name,
    t.relname AS table_name,
    i.indnatts AS num_columns,
    pg_get_indexdef(c.oid) AS index_definition,
    pg_size_pretty(pg_relation_size(c.oid)) AS index_size,
    c.oid AS index_oid
FROM
    pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_am am ON am.oid = c.relam
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_class t ON t.oid = i.indrelid
WHERE
    am.amname = 'biscuit'
    AND c.relkind = 'i'
ORDER BY
    n.nspname, c.relname;

COMMENT ON VIEW biscuit_indexes_detailed IS
'Detailed view of Biscuit indexes showing full index definitions including expressions.
Use pg_get_indexdef() to see the complete CREATE INDEX statement with all casts.';

-- FIX #3: The original view queried opfname = 'biscuit_ops'.
-- PostgreSQL names an implicitly-created opfamily after the operator class
-- that created it, so the correct name is 'biscuit_text_ops'.
--
-- FIX #7: Filtering on a single hardcoded opfname is fragile if more
-- opclasses/opfamilies are added for this access method later. Filter by
-- access method (am.amname = 'biscuit') instead, and surface which
-- opfamily/type each row belongs to, so this view stays correct without
-- edits if/when additional type support is added.
CREATE VIEW biscuit_operators AS
SELECT
    opf.opfname AS opfamily,
    amop.amopstrategy AS strategy,
    op.oprname AS operator,
    format_type(op.oprleft, NULL) AS left_type,
    format_type(op.oprright, NULL) AS right_type,
    CASE amop.amopstrategy
        WHEN 1 THEN 'LIKE'
        WHEN 2 THEN 'NOT LIKE'
        WHEN 3 THEN 'ILIKE'
        WHEN 4 THEN 'NOT ILIKE'
        ELSE 'UNKNOWN'
    END AS description
FROM pg_amop amop
JOIN pg_operator op ON amop.amopopr = op.oid
JOIN pg_opfamily opf ON amop.amopfamily = opf.oid
JOIN pg_am am ON opf.opfmethod = am.oid
WHERE am.amname = 'biscuit'
ORDER BY opf.opfname, amop.amopstrategy;

COMMENT ON VIEW biscuit_operators IS
'Shows which operators are registered for Biscuit indexes, grouped by
opfamily. Text columns can use biscuit_ops (LIKE+ILIKE, default),
biscuit_like_ops (LIKE/NOT LIKE only), or biscuit_ilike_ops (ILIKE/NOT ILIKE
only) -- each has its own opfamily, so this view will show a separate row
group per opclass actually in use. CHAR(n)/bpchar columns are supported via
an expression index on (col::text), not a native opclass.';

-- FIX #4: The original SUM() returned NULL when no Biscuit indexes exist,
-- causing pg_size_pretty() to show NULL instead of a meaningful value.
-- COALESCE handles the empty-set case.
CREATE VIEW biscuit_status AS
SELECT
    biscuit_version() AS version,
    biscuit_has_roaring() AS roaring_enabled,
    CASE
        WHEN biscuit_has_roaring() THEN 'Optimal (CRoaring)'
        ELSE 'Fallback (reduced performance)'
    END AS bitmap_implementation,
    COUNT(DISTINCT c.oid) AS total_indexes,
    pg_size_pretty(COALESCE(SUM(pg_relation_size(c.oid)), 0)) AS total_index_size
FROM pg_class c
JOIN pg_am am ON am.oid = c.relam
WHERE am.amname = 'biscuit' AND c.relkind = 'i';

COMMENT ON VIEW biscuit_status IS
'Shows current Biscuit extension status including build configuration and index statistics';

-- Operational visibility: how much unmerged write volume (pending-list
-- bytes not yet drained into a compacted blob) is sitting in each Biscuit
-- index right now. total_pending_bytes/total_drains are only refreshed by
-- VACUUM (see biscuit_pending_list_stats()'s comment), so this can lag
-- live state by up to one VACUUM cycle -- treat it as a tuning/monitoring
-- signal, not a transactionally-consistent count.
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

-- ==================== HELPER FUNCTIONS ====================

-- Create the memory size function (takes OID)
CREATE OR REPLACE FUNCTION biscuit_index_memory_size(index_oid oid)
RETURNS bigint
AS 'MODULE_PATHNAME', 'biscuit_index_memory_size'
LANGUAGE C STRICT VOLATILE;

-- FIX #5: The original text overload cast indexname directly to regclass,
-- which fails when the index lives in a non-search-path schema because
-- the bare name is ambiguous.  We join through pg_indexes (which carries
-- the schema) and resolve unambiguously via the OID stored there.
-- Callers who need schema-awareness should pass schema.indexname or use
-- the OID overload directly.
CREATE OR REPLACE FUNCTION biscuit_index_memory_size(index_name text)
RETURNS bigint
AS $$
    SELECT biscuit_index_memory_size(index_name::regclass::oid);
$$ LANGUAGE SQL STRICT VOLATILE;

-- Human-readable version with formatted output
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

-- FIX #6: The original view cast the bare indexname column (no schema) to
-- regclass, which silently resolves against the current search_path and
-- errors or returns the wrong index when names collide across schemas.
-- The fix uses the OID already available via pg_class (joined from
-- pg_indexes) so there is no ambiguity at all.
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

-- Diagnostic function to check configuration
CREATE OR REPLACE FUNCTION biscuit_check_config()
RETURNS TABLE (
    check_name text,
    status text,
    recommendation text
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        'Roaring Support'::text,
        CASE WHEN biscuit_has_roaring() THEN '✓ Enabled' ELSE '✗ Disabled' END,
        CASE WHEN biscuit_has_roaring() THEN
            'Optimal configuration (v' || COALESCE(biscuit_roaring_version(), 'unknown') || ')'
        ELSE
            'Install CRoaring library and rebuild extension for better performance'
        END;

    RETURN QUERY
    SELECT
        'Extension Version'::text,
        biscuit_version(),
        'Current version'::text;

    RETURN QUERY
    SELECT
        'Active Indexes'::text,
        COUNT(*)::text,
        CASE WHEN COUNT(*) = 0 THEN 'No indexes created yet'
             ELSE 'Indexes are active' END
    FROM pg_class c
    JOIN pg_am am ON am.oid = c.relam
    WHERE am.amname = 'biscuit' AND c.relkind = 'i';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION biscuit_check_config() IS
'Performs configuration health check and provides recommendations.
Usage: SELECT * FROM biscuit_check_config();';

-- ==================== VERSION INFO ====================

CREATE TABLE IF NOT EXISTS biscuit_version_table (
    version text PRIMARY KEY,
    installed_at timestamptz DEFAULT now(),
    description text
);

INSERT INTO biscuit_version_table (version, description) VALUES
('2.0.0', 'Multi-column indexing, query optimization, parallel bitmap processing, and persistent caching'),

('2.1.0', 'ILIKE support, unlimited text length indexing, and diagnostic utilities'),

('2.2.0', 'Unicode character-based indexing and UTF-8 correctness improvements'),

('2.3.0', 'Parallel index scans, faster ILIKE execution, and major cache correctness improvements'),

('2.4.0', 'Added expression index support and improved parallel scan and datatype compatibility'),

('2.5.0', 'Split biscuit_text_ops into biscuit_ops (default, LIKE+ILIKE), biscuit_like_ops (LIKE-only), and biscuit_ilike_ops (ILIKE-only) so LIKE-only or ILIKE-only indexes no longer build the unused case-sensitivity structures'),

('3.0.0', 'On-disk format cutover to WAL-logged directory + compacted-blob + pending-list storage (BISCUIT_VERSION 3). Removes the old external-file snapshot mechanism entirely, along with its best-effort save/proc-exit-flush machinery -- persistence failures now propagate as errors. Adds biscuit_pending_list_stats()/biscuit_pending_list_usage for pending-list (unmerged write volume) observability. REQUIRES REINDEX of every existing Biscuit index; there is no in-place upgrade for the on-disk format.');


COMMENT ON TABLE biscuit_version_table IS
'Version history for the Biscuit extension';

-- ==================== GRANT PERMISSIONS ====================

GRANT EXECUTE ON FUNCTION biscuit_has_roaring() TO PUBLIC;
GRANT EXECUTE ON FUNCTION biscuit_build_info() TO PUBLIC;
GRANT EXECUTE ON FUNCTION biscuit_build_info_json() TO PUBLIC;
GRANT EXECUTE ON FUNCTION biscuit_roaring_version() TO PUBLIC;
GRANT EXECUTE ON FUNCTION biscuit_version() TO PUBLIC;
GRANT EXECUTE ON FUNCTION biscuit_index_stats(oid) TO PUBLIC;
GRANT EXECUTE ON FUNCTION biscuit_pending_list_stats(oid) TO PUBLIC;
GRANT EXECUTE ON FUNCTION biscuit_check_config() TO PUBLIC;
GRANT SELECT ON biscuit_indexes TO PUBLIC;
GRANT SELECT ON biscuit_indexes_detailed TO PUBLIC;
GRANT SELECT ON biscuit_operators TO PUBLIC;
GRANT SELECT ON biscuit_status TO PUBLIC;
GRANT SELECT ON biscuit_pending_list_usage TO PUBLIC;

-- ==================== USAGE EXAMPLES (DOCUMENTATION) ====================

COMMENT ON EXTENSION biscuit IS 'bitmap-based index for wildcard pattern matching';
