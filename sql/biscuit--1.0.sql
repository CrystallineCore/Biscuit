-- ============================================================================
-- BISCUIT: Bitmap-Indexed String Comparison Using Intelligent Traversal
-- CRUD-ENABLED VERSION - Automatic index updates via triggers
-- ============================================================================

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION biscuit" to load this file. \quit

CREATE FUNCTION biscuit_version()
RETURNS text
AS 'MODULE_PATHNAME', 'biscuit_version'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_version() IS
'Returns the version of the loaded Biscuit C extension';

-- ============================================================================
-- CORE INDEX MANAGEMENT FUNCTIONS
-- ============================================================================

-- Function to build the optimized index from a table column
CREATE FUNCTION biscuit_build_index(
    table_name text,
    column_name text,
    pk_column_name text DEFAULT 'id'
) RETURNS boolean
AS 'MODULE_PATHNAME', 'build_biscuit_index'
LANGUAGE C;

COMMENT ON FUNCTION biscuit_build_index(text, text, text) IS 
'Build a Biscuit bitmap index for wildcard pattern matching on the specified table and column. Requires the primary key column name to accurately identify tuples (defaults to ''id''). Automatically sets up triggers for CRUD operations.';

-- Function to query and return count of matches
CREATE FUNCTION biscuit_match_count(
    pattern text
) RETURNS integer
AS 'MODULE_PATHNAME', 'biscuit_query'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_match_count(text) IS
'Return the count of records matching the given wildcard pattern using the Biscuit index';

-- Function to query and return matching rows (pk + indexed column value)
CREATE FUNCTION biscuit_match_keys(
    pattern text
) RETURNS TABLE(pk text, value text)
AS 'MODULE_PATHNAME', 'biscuit_query_rows'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_match_keys(text) IS
'Return primary keys and indexed column values for records matching the given wildcard pattern';

-- Utility function to check index status
CREATE FUNCTION biscuit_index_status()
RETURNS text
AS 'MODULE_PATHNAME', 'biscuit_status'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_index_status() IS
'Display the current status of the Biscuit index including table, column, primary key information, and CRUD statistics';

-- ============================================================================
-- CONVENIENCE STATUS QUERY FUNCTIONS
-- ============================================================================

-- Get just the free slot count
CREATE OR REPLACE FUNCTION biscuit_get_free_slots()
RETURNS integer
LANGUAGE sql STABLE
AS $$
    SELECT (regexp_matches(biscuit_index_status(), 'Free Slots: ([0-9]+)'))[1]::integer;
$$;

COMMENT ON FUNCTION biscuit_get_free_slots() IS
'Returns the number of available free slots in the Biscuit index for reuse';

-- Get just the active record count
CREATE OR REPLACE FUNCTION biscuit_get_active_count()
RETURNS integer
LANGUAGE sql STABLE
AS $$
    SELECT (regexp_matches(biscuit_index_status(), 'Active Records: ([0-9]+)'))[1]::integer;
$$;

COMMENT ON FUNCTION biscuit_get_active_count() IS
'Returns the count of active (non-deleted) records in the Biscuit index';

-- Get tombstone count
CREATE OR REPLACE FUNCTION biscuit_get_tombstone_count()
RETURNS integer
LANGUAGE sql STABLE
AS $$
    SELECT (regexp_matches(biscuit_index_status(), 'Pending tombstones: ([0-9]+)'))[1]::integer;
$$;

COMMENT ON FUNCTION biscuit_get_tombstone_count() IS
'Returns the number of pending tombstoned records awaiting cleanup';

-- ============================================================================
-- TRIGGER MANAGEMENT
-- ============================================================================

-- Trigger function for automatic index updates
CREATE FUNCTION biscuit_trigger()
RETURNS trigger
AS 'MODULE_PATHNAME', 'biscuit_trigger'
LANGUAGE C;

COMMENT ON FUNCTION biscuit_trigger() IS
'Internal trigger function that maintains the Biscuit index on INSERT, UPDATE, and DELETE operations';

-- Setup triggers automatically
CREATE OR REPLACE FUNCTION biscuit_enable_triggers()
RETURNS text
AS $$
DECLARE
    table_name_val text;
    trigger_exists boolean;
    result_msg text;
BEGIN
    -- Extract table name from status
    SELECT (regexp_matches(biscuit_index_status(), 'Table: ([^\n]+)'))[1]
    INTO table_name_val;
    
    IF table_name_val IS NULL THEN
        RAISE EXCEPTION 'Index not built. Call biscuit_build_index() first.';
    END IF;
    
    -- Check if trigger already exists
    SELECT EXISTS(
        SELECT 1 FROM pg_trigger 
        WHERE tgname = 'biscuit_auto_update'
        AND tgrelid = table_name_val::regclass
    ) INTO trigger_exists;
    
    IF trigger_exists THEN
        RETURN 'Trigger already exists on table: ' || table_name_val;
    END IF;
    
    -- Create trigger
    EXECUTE format($f$
        CREATE TRIGGER biscuit_auto_update
        AFTER INSERT OR UPDATE OR DELETE ON %I
        FOR EACH ROW
        EXECUTE FUNCTION biscuit_trigger();
    $f$, table_name_val);
    
    result_msg := 'Successfully created trigger on table: ' || table_name_val || E'\n';
    result_msg := result_msg || 'The Biscuit index will now automatically update on INSERT, UPDATE, and DELETE operations.';
    
    RETURN result_msg;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION biscuit_enable_triggers() IS
'Creates triggers to automatically maintain the Biscuit index on data changes. Call after building the index.';

-- Remove triggers
CREATE OR REPLACE FUNCTION biscuit_disable_triggers()
RETURNS text
AS $$
DECLARE
    table_name_val text;
    trigger_exists boolean;
BEGIN
    -- Extract table name from status
    SELECT (regexp_matches(biscuit_index_status(), 'Table: ([^\n]+)'))[1]
    INTO table_name_val;
    
    IF table_name_val IS NULL THEN
        RAISE EXCEPTION 'Index not built.';
    END IF;
    
    -- Check if trigger exists
    SELECT EXISTS(
        SELECT 1 FROM pg_trigger 
        WHERE tgname = 'biscuit_auto_update'
        AND tgrelid = table_name_val::regclass
    ) INTO trigger_exists;
    
    IF NOT trigger_exists THEN
        RETURN 'No trigger found on table: ' || table_name_val;
    END IF;
    
    -- Drop trigger
    EXECUTE format($f$
        DROP TRIGGER biscuit_auto_update ON %I;
    $f$, table_name_val);
    
    RETURN 'Successfully removed trigger from table: ' || table_name_val;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION biscuit_disable_triggers() IS
'Removes the automatic update trigger. Use when you want to perform bulk operations without index maintenance overhead.';

-- ============================================================================
-- HIGH-LEVEL QUERY FUNCTIONS
-- ============================================================================

-- Generic tuple function that joins back to the original table
CREATE OR REPLACE FUNCTION biscuit_match_tuples(pattern text)
RETURNS SETOF record
AS $$
DECLARE
    table_name_val text;
    pk_column_val text;
    query_sql text;
BEGIN
    -- Extract table and pk column name from status
    SELECT 
        (regexp_matches(biscuit_index_status(), 'Table: ([^\n]+)'))[1],
        (regexp_matches(biscuit_index_status(), 'Primary Key: ([^\n]+)'))[1]
    INTO table_name_val, pk_column_val;
    
    IF table_name_val IS NULL THEN
        RAISE EXCEPTION 'Index not built. Call biscuit_build_index() first.';
    END IF;
    
    -- Build query that joins matching PKs back to original table
    query_sql := format(
        'SELECT t.* FROM %I t WHERE t.%I IN (SELECT pk FROM biscuit_match_keys(%L))',
        table_name_val,
        pk_column_val,
        pattern
    );
    
    RETURN QUERY EXECUTE query_sql;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION biscuit_match_tuples(text) IS
'Returns complete tuples from the indexed table matching the pattern. Requires column specification: SELECT * FROM biscuit_match_tuples(''%abc%'') AS t(id integer, seq text)';

-- ============================================================================
-- STRONGLY-TYPED WRAPPER CREATION
-- ============================================================================

CREATE OR REPLACE FUNCTION biscuit_create_match_function()
RETURNS text
AS $$
DECLARE
    table_name_val text;
    pk_column_val text;
    col_list text;
    wrapper_sql text;
BEGIN
    -- Get table and pk info
    SELECT 
        (regexp_matches(biscuit_index_status(), 'Table: ([^\n]+)'))[1],
        (regexp_matches(biscuit_index_status(), 'Primary Key: ([^\n]+)'))[1]
    INTO table_name_val, pk_column_val;
    
    IF table_name_val IS NULL THEN
        RAISE EXCEPTION 'Index not built. Call biscuit_build_index() first.';
    END IF;
    
    -- Get column list with types
    SELECT string_agg(
        column_name || ' ' || 
        CASE 
            WHEN data_type = 'character varying' THEN 'text'
            WHEN data_type = 'character' THEN 'text'
            WHEN data_type IN ('integer', 'int', 'int4') THEN 'integer'
            WHEN data_type IN ('bigint', 'int8') THEN 'bigint'
            WHEN data_type IN ('smallint', 'int2') THEN 'smallint'
            WHEN data_type IN ('double precision', 'float8') THEN 'double precision'
            WHEN data_type IN ('real', 'float4') THEN 'real'
            WHEN data_type = 'boolean' THEN 'boolean'
            WHEN data_type = 'timestamp without time zone' THEN 'timestamp'
            WHEN data_type = 'timestamp with time zone' THEN 'timestamptz'
            WHEN data_type = 'date' THEN 'date'
            WHEN data_type = 'numeric' THEN 'numeric'
            WHEN data_type = 'json' THEN 'json'
            WHEN data_type = 'jsonb' THEN 'jsonb'
            WHEN data_type = 'uuid' THEN 'uuid'
            WHEN data_type = 'bytea' THEN 'bytea'
            WHEN data_type = 'text' THEN 'text'
            ELSE data_type
        END,
        ', ' 
        ORDER BY ordinal_position
    )
    INTO col_list
    FROM information_schema.columns
    WHERE table_schema = 'public' 
    AND table_name = table_name_val;
    
    IF col_list IS NULL THEN
        RAISE EXCEPTION 'Could not determine columns for table %', table_name_val;
    END IF;
    
    -- Create strongly-typed wrapper that does the join
    wrapper_sql := format($f$
        CREATE OR REPLACE FUNCTION biscuit_match(pattern text)
        RETURNS TABLE(%s)
        AS $body$
        BEGIN
            RETURN QUERY
            SELECT t.* FROM %I t 
            WHERE t.%I IN (SELECT pk FROM biscuit_match_keys(pattern));
        END;
        $body$ LANGUAGE plpgsql STABLE;
    $f$, col_list, table_name_val, pk_column_val);
    
    EXECUTE wrapper_sql;
    
    -- Also create a SETOF version for flexibility
    EXECUTE format($f$
        CREATE OR REPLACE FUNCTION biscuit_match_rows(pattern text)
        RETURNS SETOF %I
        AS $body$
        BEGIN
            RETURN QUERY
            SELECT t.* FROM %I t 
            WHERE t.%I IN (SELECT pk FROM biscuit_match_keys(pattern));
        END;
        $body$ LANGUAGE plpgsql STABLE;
    $f$, table_name_val, table_name_val, pk_column_val);
    
    RETURN 'Created biscuit_match() and biscuit_match_rows() functions for table: ' || table_name_val || E'\nColumns: ' || col_list;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION biscuit_create_match_function() IS
'Creates strongly-typed biscuit_match() and biscuit_match_rows() wrapper functions. Call after building index. Usage: SELECT * FROM biscuit_match(''%pattern%'')';

-- ============================================================================
-- COMPLETE SETUP HELPER
-- ============================================================================

CREATE OR REPLACE FUNCTION biscuit_setup(
    p_table_name TEXT,
    p_column_name TEXT,
    p_pk_column_name TEXT DEFAULT 'id'
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_pk_type TEXT;
    v_col_type TEXT;
    v_result TEXT := '';
BEGIN
    -- Get the actual types of the columns
    SELECT format_type(a.atttypid, a.atttypmod)
    INTO v_pk_type
    FROM pg_attribute a
    JOIN pg_class c ON a.attrelid = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.relname = p_table_name
      AND a.attname = p_pk_column_name
      AND n.nspname = 'public';
    
    IF v_pk_type IS NULL THEN
        RAISE EXCEPTION 'Primary key column % not found in table %', p_pk_column_name, p_table_name;
    END IF;
    
    SELECT format_type(a.atttypid, a.atttypmod)
    INTO v_col_type
    FROM pg_attribute a
    JOIN pg_class c ON a.attrelid = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.relname = p_table_name
      AND a.attname = p_column_name
      AND n.nspname = 'public';
    
    IF v_col_type IS NULL THEN
        RAISE EXCEPTION 'Column % not found in table %', p_column_name, p_table_name;
    END IF;
    
    -- Build the index
    PERFORM biscuit_build_index(p_table_name, p_column_name, p_pk_column_name);
    v_result := v_result || 'Biscuit index built successfully.' || E'\n';
    
    -- Drop existing functions if they exist
    EXECUTE format('DROP FUNCTION IF EXISTS biscuit_match(TEXT) CASCADE');
    EXECUTE format('DROP FUNCTION IF EXISTS biscuit_match_rows(TEXT) CASCADE');
    
    -- Create biscuit_match() function with proper type casting
    EXECUTE format($f$
        CREATE OR REPLACE FUNCTION biscuit_match(pattern TEXT)
        RETURNS SETOF %I
        LANGUAGE sql STABLE
        AS $func$
            SELECT t.*
            FROM %I t
            WHERE t.%I IN (
                SELECT pk::%s  -- Cast text back to original type
                FROM biscuit_match_keys(pattern)
            )
            ORDER BY t.%I;
        $func$;
    $f$, p_table_name, p_table_name, p_pk_column_name, v_pk_type, p_pk_column_name);
    
    v_result := v_result || format('Created biscuit_match() and biscuit_match_rows() functions for table: %s', p_table_name) || E'\n';
    v_result := v_result || format('Columns: %s %s, %s %s', p_pk_column_name, v_pk_type, p_column_name, v_col_type) || E'\n';
    
    -- Create biscuit_match_rows() function with proper type casting
    EXECUTE format($f$
        CREATE OR REPLACE FUNCTION biscuit_match_rows(pattern TEXT)
        RETURNS TABLE(pk %s, value TEXT)
        LANGUAGE sql STABLE
        AS $func$
            SELECT pk::%s, value  -- Cast text back to original type
            FROM biscuit_match_keys(pattern);
        $func$;
    $f$, v_pk_type, v_pk_type);
    
    -- Setup trigger
    EXECUTE format($f$
        DROP TRIGGER IF EXISTS biscuit_index_trigger ON %I;
    $f$, p_table_name);
    
    EXECUTE format($f$
        CREATE TRIGGER biscuit_index_trigger
        AFTER INSERT OR UPDATE OR DELETE ON %I
        FOR EACH ROW
        EXECUTE FUNCTION biscuit_trigger();
    $f$, p_table_name);
    
    v_result := v_result || format('Successfully created trigger on table: %s', p_table_name) || E'\n';
    v_result := v_result || 'The index will now automatically update on INSERT, UPDATE, and DELETE operations.';
    
    RETURN v_result;
END;
$$;

COMMENT ON FUNCTION biscuit_setup(text, text, text) IS
'Complete one-step setup: builds index, creates match functions, and enables triggers for the specified table and column';

-- ============================================================================
-- EXAMPLE USAGE
-- ============================================================================
-- 
-- COMPLETE SETUP (RECOMMENDED):
-- ==========================================
-- SELECT biscuit_setup('alpha_seq', 'seq');
-- 
-- MANUAL SETUP:
-- ==========================================
-- SELECT biscuit_build_index('alpha_seq', 'seq');
-- SELECT biscuit_create_match_function();
-- SELECT biscuit_enable_triggers();
-- 
-- QUERY METHODS:
-- ==========================================
-- 
-- 1. Full tuples (recommended, no column spec needed):
-- SELECT * FROM biscuit_match('%abc%');
-- SELECT * FROM biscuit_match('%abc%') WHERE id > 100;
-- SELECT id, seq FROM biscuit_match('a%') ORDER BY seq;
-- 
-- 2. Full tuples (alternative):
-- SELECT * FROM biscuit_match_rows('%abc%');
-- 
-- 3. Get count only (fastest):
-- SELECT biscuit_match_count('%abc%');
-- 
-- 4. Get PKs and indexed values only:
-- SELECT * FROM biscuit_match_keys('%abc%');
-- 
-- 5. Check index status:
-- SELECT biscuit_index_status();
-- 
-- 6. Get specific metrics:
-- SELECT biscuit_get_active_count();
-- SELECT biscuit_get_free_slots();
-- SELECT biscuit_get_tombstone_count();
-- 
-- CRUD OPERATIONS:
-- ==========================================
-- Once triggers are enabled, the index automatically updates:
-- 
-- INSERT INTO alpha_seq (id, seq) VALUES (99999, 'newseq');
-- UPDATE alpha_seq SET seq = 'updated' WHERE id = 1;
-- DELETE FROM alpha_seq WHERE id = 2;
-- 
-- All queries will immediately reflect these changes!
-- 
-- TRIGGER MANAGEMENT:
-- ==========================================
-- Remove triggers (for bulk operations):
-- SELECT biscuit_disable_triggers();
-- 
-- Re-enable triggers:
-- SELECT biscuit_enable_triggers();
-- 
-- PATTERN SYNTAX:
-- ==========================================
-- %     = any characters (zero or more)
-- _     = exactly one character
-- 
-- Examples:
-- 'a%'      = starts with 'a'
-- '%z'      = ends with 'z'  
-- '%abc%'   = contains 'abc'
-- '___'     = exactly 3 characters
-- 'a_c'     = 'a', any char, then 'c'
-- 'a%z'     = starts with 'a', ends with 'z'
-- '%a%b%'   = contains 'a' then 'b' (in order)