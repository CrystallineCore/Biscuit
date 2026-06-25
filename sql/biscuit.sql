-- biscuit--1.0.sql
-- SQL installation script for Biscuit Index Access Method
-- PostgreSQL 15+ compatible with full CRUD support and multi-column indexes
--
-- Features:
-- - LIKE and ILIKE support (case-sensitive and case-insensitive)
-- - O(1) lazy deletion with tombstones
-- - Incremental insert/update
-- - Automatic slot reuse
-- - Full VACUUM integration
-- - Multi-column index support
-- - Mixed data type support

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
'Returns detailed statistics for a Biscuit index including CRUD counts, tombstones, and memory usage.
Usage: SELECT biscuit_index_stats(''index_name''::regclass::oid);';

-- ==================== OPERATOR CLASSES ====================

-- Default operator class for TEXT type.
-- Supports: LIKE (~~), NOT LIKE (!~~), ILIKE (~~*), NOT ILIKE (!~~*)
--
-- FIX #2 (was FUNCTION 1 biscuit_like_support(internal) with RETURNS bool):
-- Now that biscuit_like_support is correctly declared RETURNS internal above,
-- registering it as opclass support function 1 works as intended.
CREATE OPERATOR CLASS biscuit_text_ops
DEFAULT FOR TYPE text USING biscuit AS
    OPERATOR 1 ~~ (text, text),      -- LIKE
    OPERATOR 2 !~~ (text, text),     -- NOT LIKE
    OPERATOR 3 ~~* (text, text),     -- ILIKE (case-insensitive)
    OPERATOR 4 !~~* (text, text),    -- NOT ILIKE (case-insensitive)
    FUNCTION 1 biscuit_like_support(internal);

COMMENT ON OPERATOR CLASS biscuit_text_ops USING biscuit IS
'Operator class for text types - supports LIKE, NOT LIKE, ILIKE, NOT ILIKE.
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
--    the text cast, which biscuit_text_ops can serve directly:
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
'Shows which operators are registered for Biscuit indexes.
Currently text (via biscuit_text_ops); CHAR(n)/bpchar columns are
supported via an expression index on (col::text), not a native opclass.';

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
    pg_size_pretty(pg_relation_size(c.oid)) AS disk_size
FROM pg_indexes i
JOIN pg_class c ON c.relname = i.indexname
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = i.schemaname
JOIN pg_am am ON am.oid = c.relam
WHERE am.amname = 'biscuit'
ORDER BY biscuit_index_memory_size(c.oid) DESC;

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
    ('1.0', 'Initial release: LIKE/ILIKE support for text/varchar (CHAR(n) via expression index), multi-column indexes, full CRUD, type conversion, build diagnostics');

COMMENT ON TABLE biscuit_version_table IS
'Version history for the Biscuit extension';

-- ==================== GRANT PERMISSIONS ====================

GRANT EXECUTE ON FUNCTION biscuit_has_roaring() TO PUBLIC;
GRANT EXECUTE ON FUNCTION biscuit_build_info() TO PUBLIC;
GRANT EXECUTE ON FUNCTION biscuit_build_info_json() TO PUBLIC;
GRANT EXECUTE ON FUNCTION biscuit_roaring_version() TO PUBLIC;
GRANT EXECUTE ON FUNCTION biscuit_version() TO PUBLIC;
GRANT EXECUTE ON FUNCTION biscuit_index_stats(oid) TO PUBLIC;
GRANT EXECUTE ON FUNCTION biscuit_check_config() TO PUBLIC;
GRANT SELECT ON biscuit_indexes TO PUBLIC;
GRANT SELECT ON biscuit_indexes_detailed TO PUBLIC;
GRANT SELECT ON biscuit_operators TO PUBLIC;
GRANT SELECT ON biscuit_status TO PUBLIC;

-- ==================== USAGE EXAMPLES (DOCUMENTATION) ====================

COMMENT ON EXTENSION biscuit IS 'bitmap-based index for wildcard pattern matching';
