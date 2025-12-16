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

-- Support function for LIKE operator optimization
CREATE FUNCTION biscuit_like_support(internal)
RETURNS bool
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

-- CRITICAL FIX: Only create ONE operator class per type
-- Each operator class registers operators ONCE in the operator family
-- DO NOT create multiple classes for the same type (no separate varchar/bpchar classes)

-- Default operator class for TEXT type
-- Supports: LIKE (~~), NOT LIKE (!~~), ILIKE (~~*), NOT ILIKE (!~~*)
CREATE OPERATOR CLASS biscuit_text_ops
DEFAULT FOR TYPE text USING biscuit AS
    OPERATOR 1 ~~ (text, text),      -- LIKE
    OPERATOR 2 !~~ (text, text),     -- NOT LIKE
    OPERATOR 3 ~~* (text, text),     -- ILIKE (case-insensitive)
    OPERATOR 4 !~~* (text, text),    -- NOT ILIKE (case-insensitive)
    FUNCTION 1 biscuit_like_support(internal);

COMMENT ON OPERATOR CLASS biscuit_text_ops USING biscuit IS
'Operator class for text types - supports LIKE, NOT LIKE, ILIKE, NOT ILIKE. 
VARCHAR and CHAR types will implicitly cast to text to use this class.';

-- ==================== HELPER VIEWS ====================

-- View to show all Biscuit indexes in the database
CREATE VIEW biscuit_indexes AS
SELECT
    n.nspname AS schema_name,
    c.relname AS index_name,
    t.relname AS table_name,
    CASE 
        WHEN array_length(i.indkey, 1) = 1 THEN 
            (SELECT a.attname FROM pg_attribute a 
             WHERE a.attrelid = t.oid AND a.attnum = i.indkey[0])
        ELSE 
            array_length(i.indkey, 1)::text || ' columns'
    END AS columns,
    array_length(i.indkey, 1) AS num_columns,
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
'Shows all Biscuit indexes in the current database with their tables, columns, and sizes';

-- Detailed multi-column index view
CREATE VIEW biscuit_indexes_detailed AS
SELECT
    n.nspname AS schema_name,
    c.relname AS index_name,
    t.relname AS table_name,
    array_length(i.indkey, 1) AS num_columns,
    array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum)) AS column_names,
    array_agg(format_type(a.atttypid, a.atttypmod) ORDER BY array_position(i.indkey, a.attnum)) AS column_types,
    pg_size_pretty(pg_relation_size(c.oid)) AS index_size,
    c.oid AS index_oid
FROM
    pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_am am ON am.oid = c.relam
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_class t ON t.oid = i.indrelid
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
WHERE
    am.amname = 'biscuit'
    AND c.relkind = 'i'
GROUP BY
    n.nspname, c.relname, t.relname, i.indkey, c.oid
ORDER BY
    n.nspname, c.relname;

COMMENT ON VIEW biscuit_indexes_detailed IS
'Detailed view of Biscuit indexes showing all columns and their types';

-- Verification view for operators
CREATE VIEW biscuit_operators AS
SELECT
    amopstrategy AS strategy,
    oprname AS operator,
    format_type(oprleft, NULL) AS left_type,
    format_type(oprright, NULL) AS right_type,
    CASE amopstrategy
        WHEN 1 THEN 'LIKE'
        WHEN 2 THEN 'NOT LIKE'
        WHEN 3 THEN 'ILIKE'
        WHEN 4 THEN 'NOT ILIKE'
        ELSE 'UNKNOWN'
    END AS description
FROM pg_amop
JOIN pg_operator ON pg_amop.amopopr = pg_operator.oid
JOIN pg_opfamily ON pg_amop.amopfamily = pg_opfamily.oid
WHERE pg_opfamily.opfname = 'biscuit_ops'
ORDER BY amopstrategy;

COMMENT ON VIEW biscuit_operators IS
'Shows which operators are registered for Biscuit indexes';

-- View to show build configuration and runtime status
CREATE VIEW biscuit_status AS
SELECT
    biscuit_version() AS version,
    biscuit_has_roaring() AS roaring_enabled,
    CASE 
        WHEN biscuit_has_roaring() THEN 'Optimal (CRoaring)'
        ELSE 'Fallback (reduced performance)'
    END AS bitmap_implementation,
    COUNT(DISTINCT c.oid) AS total_indexes,
    pg_size_pretty(SUM(pg_relation_size(c.oid))) AS total_index_size
FROM pg_class c
JOIN pg_am am ON am.oid = c.relam
WHERE am.amname = 'biscuit' AND c.relkind = 'i';

COMMENT ON VIEW biscuit_status IS
'Shows current Biscuit extension status including build configuration and index statistics';

-- ==================== HELPER FUNCTIONS ====================

-- Create the memory size function
CREATE OR REPLACE FUNCTION biscuit_index_memory_size(index_oid oid)
RETURNS bigint
AS 'MODULE_PATHNAME', 'biscuit_index_memory_size'
LANGUAGE C STRICT VOLATILE;

-- Convenient wrapper that takes index name instead of OID
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

-- View to show memory usage of all Biscuit indices
CREATE OR REPLACE VIEW biscuit_memory_usage AS
SELECT 
    schemaname,
    tablename,
    indexname,
    biscuit_index_memory_size(indexname::regclass::oid) as bytes,
    biscuit_size_pretty(indexname) as human_readable,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as disk_size
FROM pg_indexes
WHERE indexdef LIKE '%USING biscuit%'
ORDER BY biscuit_index_memory_size(indexname::regclass::oid) DESC;

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
    ('1.0', 'Initial release: LIKE/ILIKE support, multi-column indexes, full CRUD, type conversion, build diagnostics');

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

COMMENT ON EXTENSION biscuit IS 
'Biscuit Index Access Method v1.0

FEATURES:
- Fast LIKE pattern matching (prefix, suffix, contains)
- Case-insensitive ILIKE support
- Multi-column indexes
- Full CRUD support with O(1) deletion
- Automatic type conversion (int, date, timestamp, etc.)
- CRoaring bitmap optimization (when available)

QUICK START:

-- Check configuration
SELECT * FROM biscuit_status;
SELECT * FROM biscuit_check_config();

-- Single column index
CREATE INDEX idx_users_name ON users USING biscuit(name);

-- Multi-column index
CREATE INDEX idx_users_multi ON users USING biscuit(first_name, last_name);

-- Works on VARCHAR and CHAR too (implicit cast to text)
CREATE INDEX idx_users_email ON users USING biscuit(email::varchar);

QUERY EXAMPLES:

-- LIKE queries (case-sensitive)
SELECT * FROM users WHERE name LIKE ''John%'';        -- Prefix
SELECT * FROM users WHERE name LIKE ''%Smith'';       -- Suffix  
SELECT * FROM users WHERE name LIKE ''%admin%'';      -- Contains
SELECT * FROM users WHERE name NOT LIKE ''test%'';    -- Negation

-- ILIKE queries (case-insensitive)
SELECT * FROM users WHERE name ILIKE ''john%'';       -- Case-insensitive prefix
SELECT * FROM users WHERE name ILIKE ''%SMITH'';      -- Case-insensitive suffix
SELECT * FROM users WHERE name NOT ILIKE ''ADMIN%'';  -- Case-insensitive negation

-- Multi-column queries
SELECT * FROM users 
WHERE first_name LIKE ''J%'' AND last_name LIKE ''S%'';

DIAGNOSTICS:

-- View extension status
SELECT * FROM biscuit_status;

-- Configuration health check
SELECT * FROM biscuit_check_config();

-- Build information
SELECT * FROM biscuit_build_info();

-- Check Roaring support
SELECT biscuit_has_roaring();

-- View all Biscuit indexes
SELECT * FROM biscuit_indexes;

-- Get detailed stats
SELECT biscuit_index_stats(''idx_users_name''::regclass);

-- View supported operators
SELECT * FROM biscuit_operators;

MAINTENANCE:

VACUUM ANALYZE users;        -- Cleanup tombstones
REINDEX INDEX idx_users_name; -- Rebuild if needed

PERFORMANCE NOTE:
If biscuit_has_roaring() returns false, the extension is using a fallback
bitmap implementation. Install CRoaring library and rebuild for optimal performance:
  - Debian/Ubuntu: apt-get install libroaring-dev
  - macOS: brew install croaring
  - Then: make clean && make && sudo make install
';