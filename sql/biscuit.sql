-- biscuit--1.0.sql
-- SQL installation script for Biscuit Index Access Method
-- PostgreSQL 15+ compatible with full CRUD support and multi-column indexes
--
-- Features:
-- - O(1) lazy deletion with tombstones
-- - Incremental insert/update
-- - Automatic slot reuse
-- - Full VACUUM integration
-- - Optimized pattern matching for LIKE queries
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
'Biscuit index access method: High-performance pattern matching for LIKE queries with multi-column support';

-- ==================== OPERATOR SUPPORT ====================

-- Support function for LIKE operator optimization
CREATE FUNCTION biscuit_like_support(internal)
RETURNS bool
AS 'MODULE_PATHNAME', 'biscuit_like_support'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_like_support(internal) IS
'Support function that tells the planner Biscuit can handle LIKE pattern matching';

-- ==================== DIAGNOSTIC FUNCTIONS ====================

-- Function to get index statistics and health information
CREATE FUNCTION biscuit_index_stats(oid)
RETURNS text
AS 'MODULE_PATHNAME', 'biscuit_index_stats'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_index_stats(oid) IS
'Returns detailed statistics for a Biscuit index including CRUD counts, tombstones, and memory usage.
Usage: SELECT biscuit_index_stats(''index_name''::regclass::oid);';

-- ==================== OPERATOR CLASSES ====================

-- Default operator class for text types (text, varchar, bpchar)
CREATE OPERATOR CLASS biscuit_text_ops
DEFAULT FOR TYPE text USING biscuit AS
    OPERATOR 1 ~~ (text, text),          -- LIKE operator
    OPERATOR 2 !~~ (text, text),     -- NOT LIKE operator
    FUNCTION 1 biscuit_like_support(internal);

COMMENT ON OPERATOR CLASS biscuit_text_ops USING biscuit IS
'Default operator class for Biscuit indexes on text columns - supports LIKE and ILIKE queries';

-- Operator class for VARCHAR
CREATE OPERATOR CLASS biscuit_varchar_ops
DEFAULT FOR TYPE varchar USING biscuit AS
    OPERATOR 1 ~~ (text, text),
    OPERATOR 2 ~~* (text, text),
    FUNCTION 1 biscuit_like_support(internal);

-- Operator class for CHAR
CREATE OPERATOR CLASS biscuit_bpchar_ops
DEFAULT FOR TYPE bpchar USING biscuit AS
    OPERATOR 1 ~~ (text, text),
    OPERATOR 2 ~~* (text, text),
    FUNCTION 1 biscuit_like_support(internal);

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
'Shows all Biscuit indexes in the current database with their tables, columns, and sizes. Supports multi-column indexes.';

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
'Detailed view of Biscuit indexes showing all columns and their types for multi-column indexes';

-- ==================== USAGE EXAMPLES ====================

-- Example queries (commented out - for documentation)
/*

-- ==================== SINGLE-COLUMN EXAMPLES ====================

-- Basic index creation
CREATE INDEX idx_username ON users USING biscuit(username);
CREATE INDEX idx_email ON users USING biscuit(email);

-- Case-insensitive index (use LOWER())
CREATE INDEX idx_username_lower ON users USING biscuit(LOWER(username));

-- Partial index (only active users)
CREATE INDEX idx_active_users ON users USING biscuit(username)
WHERE status = 'active';

-- Query examples that use the index
SELECT * FROM users WHERE username LIKE 'john%';        -- Prefix
SELECT * FROM users WHERE email LIKE '%@gmail.com';     -- Suffix
SELECT * FROM users WHERE username LIKE '%admin%';      -- Contains
SELECT * FROM users WHERE username LIKE 'user_1%5';     -- Complex

-- ==================== MULTI-COLUMN EXAMPLES ====================

-- Multi-column indexes (NEW in v1.0!)
CREATE INDEX idx_name_email ON users USING biscuit(first_name, last_name);
CREATE INDEX idx_full_contact ON users USING biscuit(first_name, last_name, email);

-- Mixed type multi-column index
CREATE INDEX idx_user_profile ON user_profiles USING biscuit(username, age, city);

-- Query multi-column indexes
SELECT * FROM users 
WHERE first_name LIKE 'John%' AND last_name LIKE 'S%';

SELECT * FROM user_profiles 
WHERE username LIKE 'admin%' AND age::text LIKE '3%' AND city LIKE 'New%';

-- ==================== MAINTENANCE ====================

-- Get index statistics
SELECT biscuit_index_stats('idx_username'::regclass::oid);

-- View all Biscuit indexes
SELECT * FROM biscuit_indexes;

-- View detailed multi-column info
SELECT * FROM biscuit_indexes_detailed;

-- List indexes on a specific table
SELECT * FROM biscuit_indexes WHERE table_name = 'users';

-- Force index usage for testing
SET enable_seqscan = off;
EXPLAIN ANALYZE SELECT * FROM users WHERE username LIKE '%test%';
SET enable_seqscan = on;

-- Maintenance
VACUUM ANALYZE users;           -- Clean up tombstones
REINDEX INDEX idx_username;     -- Rebuild if needed

-- ==================== TYPE CONVERSION EXAMPLES ====================

-- Biscuit automatically handles type conversion for common types:
-- - TEXT, VARCHAR, CHAR (native)
-- - INTEGER, BIGINT (converted to zero-padded sortable strings)
-- - FLOAT, DOUBLE PRECISION (converted to scientific notation)
-- - DATE, TIMESTAMP (converted to sortable integers)
-- - BOOLEAN (converted to 't'/'f')

-- Example: Mixed type index
CREATE TABLE products (
    id SERIAL,
    name TEXT,
    price NUMERIC,
    created_date DATE
);

CREATE INDEX idx_product_search ON products USING biscuit(name, price, created_date);

-- Query will work seamlessly
SELECT * FROM products 
WHERE name LIKE 'Widget%' 
  AND price::text LIKE '9%'
  AND created_date::text LIKE '2024%';

*/

-- ==================== GRANT PERMISSIONS ====================

-- Grant execute on functions to public (read-only diagnostic function)
GRANT EXECUTE ON FUNCTION biscuit_index_stats(oid) TO PUBLIC;

-- ==================== VERSION INFO ====================

-- Store extension version information
CREATE TABLE IF NOT EXISTS biscuit_version (
    version text PRIMARY KEY,
    installed_at timestamptz DEFAULT now(),
    description text
);

INSERT INTO biscuit_version (version, description) VALUES
    ('1.0', 'Initial release with full CRUD support, O(1) deletion, multi-column indexes, and mixed type support');

COMMENT ON TABLE biscuit_version IS
'Version history for the Biscuit IAM extension';

-- ==================== HELPFUL QUERIES ====================

-- Function to check multi-column capability
CREATE FUNCTION biscuit_multicolumn_enabled()
RETURNS boolean AS $$
    SELECT amcanmulticol FROM pg_am WHERE amname = 'biscuit';
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION biscuit_multicolumn_enabled() IS
'Returns true if multi-column indexing is enabled for Biscuit indexes';
