-- ============================================================================
-- BISCUIT: Bitmap-Indexed String Comparison Using Intelligent Traversal
-- CLEAN VERSION - Essential functions only
-- ============================================================================

-- ============================================================================
-- VERSION INFO
-- ============================================================================

CREATE FUNCTION biscuit_version()
RETURNS text
AS 'MODULE_PATHNAME', 'biscuit_version'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_version() IS
'Returns the version of the Biscuit C extension module';

-- ============================================================================
-- CORE C FUNCTIONS - Direct interface to bitmap index
-- ============================================================================

CREATE FUNCTION biscuit_build_index(
    table_name text,
    column_name text,
    pk_column_name text DEFAULT 'id'
) RETURNS boolean
AS 'MODULE_PATHNAME', 'build_biscuit_index'
LANGUAGE C;

COMMENT ON FUNCTION biscuit_build_index(text, text, text) IS 
'Constructs the bitmap index for the specified table/column. The pk_column_name identifies unique rows. This is a low-level function - use biscuit_setup() for complete initialization.';

CREATE FUNCTION biscuit_match_count(
    pattern text
) RETURNS integer
AS 'MODULE_PATHNAME', 'biscuit_query'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_match_count(text) IS
'Returns only the count of records matching the wildcard pattern. Most efficient for counting without retrieving data.';

CREATE FUNCTION biscuit_match_keys(
    pattern text
) RETURNS TABLE(pk text, value text)
AS 'MODULE_PATHNAME', 'biscuit_query_rows'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_match_keys(text) IS
'Returns (primary_key, indexed_value) pairs for matching records as text. Cast pk to original type if needed.';

CREATE FUNCTION biscuit_index_status()
RETURNS text
AS 'MODULE_PATHNAME', 'biscuit_status'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_index_status() IS
'Returns detailed status: indexed table/column, primary key, active records, free slots, tombstones, and CRUD operation counts.';

CREATE FUNCTION biscuit_cleanup()
RETURNS text
AS 'MODULE_PATHNAME', 'biscuit_cleanup'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_cleanup() IS
'Manually triggers tombstone cleanup. Run when tombstone count is high or before critical operations.';

-- ============================================================================
-- TRIGGER FUNCTION - Maintains index on data changes
-- ============================================================================

CREATE FUNCTION biscuit_trigger()
RETURNS trigger
AS 'MODULE_PATHNAME', 'biscuit_trigger'
LANGUAGE C;

COMMENT ON FUNCTION biscuit_trigger() IS
'Internal trigger function invoked on INSERT/UPDATE/DELETE to keep the bitmap index synchronized with table data.';

-- ============================================================================
-- STATUS QUERY HELPERS - Extract specific metrics
-- ============================================================================

CREATE OR REPLACE FUNCTION biscuit_get_free_slots()
RETURNS integer
LANGUAGE sql STABLE
AS $$
    SELECT COALESCE(
        (regexp_match(biscuit_index_status(), 'Free Slots: ([0-9]+)'))[1]::integer,
        0
    );
$$;

COMMENT ON FUNCTION biscuit_get_free_slots() IS
'Returns the number of reusable slots from deleted records. Higher values indicate fragmentation.';

CREATE OR REPLACE FUNCTION biscuit_get_active_count()
RETURNS integer
LANGUAGE sql STABLE
AS $$
    SELECT COALESCE(
        (regexp_match(biscuit_index_status(), 'Active Records: ([0-9]+)'))[1]::integer,
        0
    );
$$;

COMMENT ON FUNCTION biscuit_get_active_count() IS
'Returns the count of live (non-deleted) records in the index.';

CREATE OR REPLACE FUNCTION biscuit_get_tombstone_count()
RETURNS integer
LANGUAGE sql STABLE
AS $$
    SELECT COALESCE(
        (regexp_match(biscuit_index_status(), 'Pending tombstones: ([0-9]+)'))[1]::integer,
        0
    );
$$;

COMMENT ON FUNCTION biscuit_get_tombstone_count() IS
'Returns the number of tombstoned (soft-deleted) records awaiting cleanup or reuse.';

-- ============================================================================
-- TRIGGER MANAGEMENT - Enable/disable automatic index updates
-- ============================================================================

CREATE OR REPLACE FUNCTION biscuit_enable_triggers()
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name text;
    v_trigger_exists boolean;
BEGIN
    v_table_name := (regexp_match(biscuit_index_status(), 'Table: ([^\n]+)'))[1];
    
    IF v_table_name IS NULL THEN
        RAISE EXCEPTION 'No index found. Call biscuit_setup() or biscuit_build_index() first.';
    END IF;
    
    SELECT EXISTS(
        SELECT 1 FROM pg_trigger 
        WHERE tgname = 'biscuit_auto_update'
        AND tgrelid = v_table_name::regclass
    ) INTO v_trigger_exists;
    
    IF v_trigger_exists THEN
        RETURN format('Trigger already active on table: %s', v_table_name);
    END IF;
    
    EXECUTE format(
        'CREATE TRIGGER biscuit_auto_update '
        'AFTER INSERT OR UPDATE OR DELETE ON %I '
        'FOR EACH ROW EXECUTE FUNCTION biscuit_trigger()',
        v_table_name
    );
    
    RETURN format('✓ Trigger enabled on %s - index will auto-update on data changes', v_table_name);
END;
$$;

COMMENT ON FUNCTION biscuit_enable_triggers() IS
'Activates automatic index maintenance. Call after building the index to keep it synchronized with table modifications.';

CREATE OR REPLACE FUNCTION biscuit_disable_triggers()
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name text;
    v_trigger_exists boolean;
BEGIN
    v_table_name := (regexp_match(biscuit_index_status(), 'Table: ([^\n]+)'))[1];
    
    IF v_table_name IS NULL THEN
        RAISE EXCEPTION 'No index found.';
    END IF;
    
    SELECT EXISTS(
        SELECT 1 FROM pg_trigger 
        WHERE tgname = 'biscuit_auto_update'
        AND tgrelid = v_table_name::regclass
    ) INTO v_trigger_exists;
    
    IF NOT v_trigger_exists THEN
        RETURN format('No trigger found on table: %s', v_table_name);
    END IF;
    
    EXECUTE format('DROP TRIGGER biscuit_auto_update ON %I', v_table_name);
    
    RETURN format('✓ Trigger removed from %s - index updates paused', v_table_name);
END;
$$;

COMMENT ON FUNCTION biscuit_disable_triggers() IS
'Disables automatic index updates. Useful for bulk operations to avoid per-row overhead. Remember to rebuild or re-enable afterwards.';

-- ============================================================================
-- COMPLETE SETUP - One-step initialization with type-safe query function
-- ============================================================================

DROP FUNCTION IF EXISTS biscuit_setup(text, text, text) CASCADE;
DROP FUNCTION IF EXISTS biscuit_match(text) CASCADE;

CREATE OR REPLACE FUNCTION biscuit_setup(
    p_table_name text,
    p_column_name text,
    p_pk_column_name text DEFAULT 'id'
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_result text := '';
    v_col_list_typed text;
    v_pk_type text;
BEGIN
    -- Validate table exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_tables 
        WHERE schemaname = 'public' AND tablename = p_table_name
    ) THEN
        RAISE EXCEPTION 'Table % does not exist', p_table_name;
    END IF;
    
    -- Get PK type
    SELECT CASE 
        WHEN data_type IN ('integer', 'int', 'int4') THEN 'integer'
        WHEN data_type IN ('bigint', 'int8') THEN 'bigint'
        WHEN data_type IN ('smallint', 'int2') THEN 'smallint'
        WHEN data_type IN ('character varying', 'varchar', 'character') THEN 'text'
        WHEN data_type = 'uuid' THEN 'uuid'
        ELSE data_type
    END INTO v_pk_type
    FROM information_schema.columns
    WHERE table_schema = 'public' 
    AND table_name = p_table_name 
    AND column_name = p_pk_column_name;
    
    IF v_pk_type IS NULL THEN
        RAISE EXCEPTION 'PK column % not found', p_pk_column_name;
    END IF;
    
    -- Build bitmap index
    PERFORM biscuit_build_index(p_table_name, p_column_name, p_pk_column_name);
    
    -- Get column definitions for table
    SELECT string_agg(
        column_name || ' ' || 
        CASE 
            WHEN data_type IN ('character varying', 'character') THEN 'text'
            WHEN data_type IN ('integer', 'int', 'int4') THEN 'integer'
            WHEN data_type IN ('bigint', 'int8') THEN 'bigint'
            WHEN data_type IN ('smallint', 'int2') THEN 'smallint'
            WHEN data_type = 'boolean' THEN 'boolean'
            WHEN data_type = 'timestamp without time zone' THEN 'timestamp'
            WHEN data_type = 'uuid' THEN 'uuid'
            ELSE data_type
        END,
        ', '
    ) INTO v_col_list_typed
    FROM information_schema.columns
    WHERE table_schema = 'public' 
    AND table_name = p_table_name;
    
    -- Create query function using PK index
    EXECUTE format(
        'CREATE OR REPLACE FUNCTION biscuit_match(pattern text) '
        'RETURNS TABLE(%s) '
        'LANGUAGE sql STABLE PARALLEL SAFE AS '
        '$func$ '
        '  SELECT t.* FROM %I t '
        '  WHERE t.%I = ANY('
        '    SELECT (pk::%s) FROM biscuit_match_keys($1)'
        '  ) '
        '$func$',
        v_col_list_typed,
        p_table_name,
        p_pk_column_name,
        v_pk_type
    );
    
    -- Enable trigger
    EXECUTE format('DROP TRIGGER IF EXISTS biscuit_auto_update ON %I', p_table_name);
    EXECUTE format(
        'CREATE TRIGGER biscuit_auto_update '
        'AFTER INSERT OR UPDATE OR DELETE ON %I '
        'FOR EACH ROW EXECUTE FUNCTION biscuit_trigger()',
        p_table_name
    );
    
    v_result := '✓ Bitmap index built' || E'\n';
    v_result := v_result || '✓ Created biscuit_match() function' || E'\n';
    v_result := v_result || '✓ Auto-update trigger enabled' || E'\n';
    v_result := v_result || E'\nUsage:\n';
    v_result := v_result || '  SELECT * FROM biscuit_match(''%pattern%'');' || E'\n';
    v_result := v_result || '  SELECT biscuit_match_count(''%pattern%'');' || E'\n';
    
    RETURN v_result;
END;
$$;

COMMENT ON FUNCTION biscuit_setup(text, text, text) IS
'Complete one-step setup: builds index, creates type-safe biscuit_match() function, and enables triggers.';

-- ============================================================================
-- USAGE GUIDE
-- ============================================================================
-- 
-- QUICK START:
-- ============================================================================
-- SELECT biscuit_setup('users', 'email', 'id');
-- 
-- This single command:
--   1. Builds the bitmap index on users.email
--   2. Creates biscuit_match() returning full user records
--   3. Enables triggers for automatic index updates
-- 
-- QUERYING:
-- ============================================================================
-- 
-- Get full table rows (strongly-typed, all columns):
--   SELECT * FROM biscuit_match('%@gmail.com');
--   SELECT * FROM biscuit_match('admin%') WHERE created_at > '2024-01-01';
--   SELECT id, email FROM biscuit_match('%test%') ORDER BY id LIMIT 10;
-- 
-- Get count only (fastest):
--   SELECT biscuit_match_count('%@gmail.com');
-- 
-- Get primary keys and indexed values only:
--   SELECT * FROM biscuit_match_keys('%@gmail.com');
--   SELECT pk::integer AS id FROM biscuit_match_keys('%pattern%');
-- 
-- MONITORING:
-- ============================================================================
-- 
-- Full status report:
--   SELECT biscuit_index_status();
-- 
-- Specific metrics:
--   SELECT biscuit_get_active_count();      -- Live records
--   SELECT biscuit_get_free_slots();        -- Reusable slots
--   SELECT biscuit_get_tombstone_count();   -- Pending deletions
-- 
-- Manual cleanup:
--   SELECT biscuit_cleanup();
-- 
-- PATTERN SYNTAX:
-- ============================================================================
-- %       → Zero or more characters
-- _       → Exactly one character
-- 
-- Examples:
--   'john%'      → Starts with 'john'
--   '%@gmail.com' → Ends with '@gmail.com'
--   '%test%'     → Contains 'test'
--   '____'       → Exactly 4 characters
--   'a_c'        → 'a', any char, 'c'
--   'admin%2024' → Starts 'admin', ends '2024'
-- 
-- CRUD OPERATIONS:
-- ============================================================================
-- Once triggers are enabled, the index auto-updates:
-- 
--   INSERT INTO users (id, email) VALUES (999, 'new@example.com');
--   UPDATE users SET email = 'updated@example.com' WHERE id = 1;
--   DELETE FROM users WHERE id = 2;
-- 
-- Queries immediately reflect these changes!
-- 
-- PERFORMANCE TUNING:
-- ============================================================================
-- 
-- For bulk operations, disable triggers temporarily:
--   SELECT biscuit_disable_triggers();
--   -- Perform bulk INSERT/UPDATE/DELETE
--   SELECT biscuit_build_index('users', 'email', 'id');  -- Rebuild
--   SELECT biscuit_enable_triggers();
-- 
-- Manual cleanup when needed:
--   SELECT biscuit_cleanup();  -- Run when tombstone count is high
-- 
-- Check health:
--   SELECT biscuit_get_tombstone_count();  -- Monitor tombstones
--   SELECT biscuit_get_free_slots();        -- Check fragmentation
-- 
-- CORE FUNCTIONS:
-- ============================================================================
-- Setup:
--   biscuit_setup(table, column, pk)           - Complete initialization
--   biscuit_build_index(table, column, pk)     - Build index only
-- 
-- Query:
--   biscuit_match(pattern)                     - Returns full records
--   biscuit_match_keys(pattern)                - Returns (pk, value) as text
--   biscuit_match_count(pattern)               - Returns count only
-- 
-- Maintenance:
--   biscuit_cleanup()                          - Manual tombstone cleanup
--   biscuit_enable_triggers()                  - Enable auto-updates
--   biscuit_disable_triggers()                 - Disable auto-updates
-- 
-- Monitoring:
--   biscuit_index_status()                     - Full status report
--   biscuit_get_active_count()                 - Active record count
--   biscuit_get_free_slots()                   - Free slot count
--   biscuit_get_tombstone_count()              - Tombstone count
--   biscuit_version()                          - Extension version