-- ============================================================================
-- BISCUIT vs PG_TRGM PERFORMANCE BENCHMARK
-- Realistic hash-based pattern matching comparison
-- ============================================================================

-- Clean slate
DROP TABLE IF EXISTS hash_data CASCADE;
DROP TABLE IF EXISTS benchmark_results CASCADE;

-- Enable pg_trgm extension
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS biscuit;

SET enable_seqscan = OFF;

DO $$ 
BEGIN 
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '     BISCUIT vs PG_TRGM PERFORMANCE BENCHMARK';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;

-- ============================================================================
-- SECTION 1: CREATE TEST DATA
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '📊 Creating test data with random alphanumeric hashes...'; END $$;

-- Main data table
CREATE TABLE hash_data (
    serial_no SERIAL PRIMARY KEY,
    hash TEXT NOT NULL
);

-- Generate random alphanumeric hashes (6-15 characters)
-- Include common patterns to ensure queries return results
INSERT INTO hash_data (hash)
SELECT 
    -- Mix of patterns to ensure searchability
    CASE (g % 10)
        WHEN 0 THEN substring(md5(random()::text), 1, (6 + (random() * 9)::int))
        WHEN 1 THEN substring(md5(random()::text), 1, (6 + (random() * 9)::int))
        WHEN 2 THEN substring(md5(random()::text), 1, (6 + (random() * 9)::int))
        WHEN 3 THEN substring(md5(random()::text), 1, 3) || substring(md5(random()::text), 1, 5)
        WHEN 4 THEN substring(md5(random()::text), 1, (6 + (random() * 9)::int))
        WHEN 5 THEN substring(md5(random()::text), 1, (6 + (random() * 9)::int))
        WHEN 6 THEN substring(md5(random()::text), 1, 4)
        WHEN 7 THEN substring(md5(random()::text), 1, 2) || substring(md5(random()::text), 1, 4)
        WHEN 8 THEN substring(md5(random()::text), 1, (6 + (random() * 9)::int))
        ELSE substring(md5(random()::text), 1, (6 + (random() * 9)::int))
    END
FROM generate_series(1, 5000000) g;

DO $$ 
DECLARE
    row_count INT;
    min_len INT;
    max_len INT;
    avg_len NUMERIC;
BEGIN
    SELECT COUNT(*), MIN(LENGTH(hash)), MAX(LENGTH(hash)), AVG(LENGTH(hash))
    INTO row_count, min_len, max_len, avg_len
    FROM hash_data;
    
    RAISE NOTICE '✓ Generated % rows', row_count;
    RAISE NOTICE '✓ Hash length range: % to % characters (avg: %)', min_len, max_len, ROUND(avg_len, 2);
    RAISE NOTICE '';
END $$;

-- Results table
CREATE TABLE benchmark_results (
    test_id SERIAL PRIMARY KEY,
    test_name TEXT,
    test_pattern TEXT,
    pattern_type TEXT,
    biscuit_count INT,
    biscuit_time_ms NUMERIC(10,3),
    trgm_count INT,
    trgm_time_ms NUMERIC(10,3),
    counts_match BOOLEAN,
    speedup_factor NUMERIC(10,2),
    notes TEXT
);

-- ============================================================================
-- PHASE 1: BISCUIT INDEX TESTS
-- ============================================================================

DO $$ 
BEGIN 
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '🔧 PHASE 1: Testing with BISCUIT Index';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;

DO $$ BEGIN RAISE NOTICE '🔧 Creating BISCUIT index...'; END $$;

CREATE INDEX idx_hash_biscuit ON hash_data USING biscuit(hash);

DO $$ BEGIN RAISE NOTICE '✓ BISCUIT index created'; RAISE NOTICE ''; END $$;

-- Warm up cache
DO $$ 
DECLARE
    dummy INT;
BEGIN 
    RAISE NOTICE '🔥 Warming up cache with dummy query...';
    SELECT COUNT(*) INTO dummy FROM hash_data WHERE hash LIKE '%A%';
    RAISE NOTICE '✓ Cache warmed (% rows touched)', dummy;
    RAISE NOTICE '';
END $$;

DO $$ BEGIN RAISE NOTICE '📋 Running BISCUIT benchmark tests...'; END $$;

-- Test 1: PREFIX - ab%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'ab%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('PREFIX: ab', 'ab%', 'PREFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 1 - PREFIX ab%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 2: PREFIX - abc%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'abc%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('PREFIX: abc', 'abc%', 'PREFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 2 - PREFIX abc%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 3: PREFIX - a%b%c%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'a%b%c%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('PREFIX: a%b%c%', 'a%b%c%', 'PREFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 3 - PREFIX a%%b%%c%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 4: SUFFIX - %a
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%a';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('SUFFIX: %a', '%a', 'SUFFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 4 - SUFFIX %%a: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 5: SUFFIX - %ab
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%ab';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('SUFFIX: %ab', '%ab', 'SUFFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 5 - SUFFIX %%ab: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 6: SUBSTRING - %ab%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%ab%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('SUBSTRING: %ab%', '%ab%', 'SUBSTRING', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 6 - SUBSTRING %%ab%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 7: SUBSTRING - %1%2%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%1%2%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('SUBSTRING: %1%2%', '%1%2%', 'SUBSTRING', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 7 - SUBSTRING %%1%%2%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 8: SUBSTRING - %1%a%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%1%a%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('SUBSTRING: %1%a%', '%1%a%', 'SUBSTRING', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 8 - SUBSTRING %%1%%a%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 9: MULTI-PART - a%b
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'a%b';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('MULTI-PART: a%b', 'a%b', 'MULTI-PART', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 9 - MULTI-PART a%%b: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 10: MULTI-PART - abc%ab%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'abc%ab%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('MULTI-PART: abc%ab%', 'abc%ab%', 'MULTI-PART', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 10 - MULTI-PART abc%%ab%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 11: COMPLEX - %1%a%c%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%1%a%c%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('COMPLEX: %1%a%c%', '%1%a%c%', 'COMPLEX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 11 - COMPLEX %%1%%a%%c%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 12: CASE-INSENSITIVE PREFIX - ab%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash ILIKE 'ab%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('ILIKE PREFIX: ab', 'ab% (ILIKE)', 'ILIKE-PREFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 12 - ILIKE ab%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 13: CASE-INSENSITIVE SUBSTRING - %1%a%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash ILIKE '%1%a%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('ILIKE SUBSTRING: %1%a%', '%1%a% (ILIKE)', 'ILIKE-SUBSTRING', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 13 - ILIKE %%1%%a%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 14: SHORT PREFIX - A%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'A%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('SHORT PREFIX: A', 'A%', 'SHORT-PREFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 14 - SHORT PREFIX A%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 15: COMBINED OR - ab% OR %a
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'ab%' OR hash LIKE '%a';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('OR: ab% OR %a', 'ab% OR %a', 'COMBINED-OR', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 15 - OR ab%% OR %%a: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 16: PREFIX with underscore - a_b%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'a_b%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('PREFIX: a_b', 'a_b%', 'PREFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 16 - PREFIX a_b%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 17: SUFFIX with underscore - %ab_
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%ab_';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('SUFFIX: %ab_', '%ab_', 'SUFFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 17 - SUFFIX %%ab_: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 18: INFIX simple - %abc%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%abc%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('INFIX: %abc%', '%abc%', 'INFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 18 - INFIX %%abc%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 19: INFIX with underscores - %a_b%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%a_b%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('INFIX: %a_b%', '%a_b%', 'INFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 19 - INFIX %%a_b%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 20: MULTI-COMPARTMENT - %abc%def%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%abc%def%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('MULTI-COMPARTMENT: %abc%def%', '%abc%def%', 'MULTI-COMPARTMENT', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 20 - MULTI-COMPARTMENT %%abc%%def%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 21: COMPLEX INFIX with underscores - %a__b__c%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%a__b__c%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('COMPLEX INFIX: %a__b__c%', '%a__b__c%', 'COMPLEX-INFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 21 - COMPLEX INFIX %%a__b__c%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 22: PREFIX with underscore and percent - _ab%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '_ab%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('PREFIX: _ab', '_ab%', 'PREFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 22 - PREFIX _ab%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 23: SUFFIX with underscore and percent - %ab_
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%ab_';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('SUFFIX: %ab_', '%ab_', 'SUFFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 23 - SUFFIX %%ab_: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 24: MULTI-PART with underscore - a_b%c_d%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'a_b%c_d%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('MULTI-PART: a_b%c_d%', 'a_b%c_d%', 'MULTI-PART', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 24 - MULTI-PART a_b%%c_d%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 25: INFIX with starting underscore - _a%b%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '_a%b%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('INFIX: _a%b%', '_a%b%', 'INFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 25 - INFIX _a%%b%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 26: INFIX with ending underscore - %a%b_%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%a%b_%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('INFIX: %a%b_%', '%a%b_%', 'INFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 26 - INFIX %%a%%b_%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 27: COMPLEX MULTI-COMPARTMENT - %a_b%c_d%e%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%a_b%c_d%e%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('COMPLEX: %a_b%c_d%e%', '%a_b%c_d%e%', 'COMPLEX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 27 - COMPLEX %%a_b%%c_d%%e%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 28: PREFIX with multiple underscores - ___abc%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '___abc%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('PREFIX: ___abc', '___abc%', 'PREFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 28 - PREFIX ___abc%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 29: SUFFIX with multiple underscores - %abc___
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%abc___';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('SUFFIX: %abc___', '%abc___', 'SUFFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 29 - SUFFIX %%abc___: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 30: INFIX with underscores and percent - %a__b__c%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%a__b__c%';
    end_time := clock_timestamp();
    
    INSERT INTO benchmark_results (test_name, test_pattern, pattern_type, biscuit_count, biscuit_time_ms)
    VALUES ('INFIX: %a__b__c%', '%a__b__c%', 'INFIX', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 30 - INFIX %%a__b__c%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;



DO $$ BEGIN RAISE NOTICE ''; RAISE NOTICE '✓ BISCUIT tests complete'; RAISE NOTICE ''; END $$;

-- ============================================================================
-- PHASE 2: PG_TRGM INDEX TESTS
-- ============================================================================

DO $$ 
BEGIN 
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '🔧 PHASE 2: Testing with PG_TRGM Index';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;

DO $$ BEGIN RAISE NOTICE '🔥 Dropping BISCUIT index...'; END $$;

DROP INDEX idx_hash_biscuit;

DO $$ BEGIN RAISE NOTICE '✓ BISCUIT index dropped'; RAISE NOTICE ''; END $$;

DO $$ BEGIN RAISE NOTICE '🔧 Creating PG_TRGM GIN index...'; END $$;

CREATE INDEX idx_hash_trgm ON hash_data USING gin(hash gin_trgm_ops);

DO $$ BEGIN RAISE NOTICE '✓ PG_TRGM index created'; RAISE NOTICE ''; END $$;

-- Warm up cache
DO $$ 
DECLARE
    dummy INT;
BEGIN 
    RAISE NOTICE '🔥 Warming up cache with dummy query...';
    SELECT COUNT(*) INTO dummy FROM hash_data WHERE hash LIKE '%A%';
    RAISE NOTICE '✓ Cache warmed (% rows touched)', dummy;
    RAISE NOTICE '';
END $$;

DO $$ BEGIN RAISE NOTICE '📋 Running PG_TRGM benchmark tests...'; END $$;

-- Run all tests again with pg_trgm
-- Test 1
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'ab%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 1;
    
    RAISE NOTICE 'Test 1 - PREFIX ab%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 2
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'abc%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 2;
    
    RAISE NOTICE 'Test 2 - PREFIX abc%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 3
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'a%b%c%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 3;
    
    RAISE NOTICE 'Test 3 - PREFIX a%%b%%c%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 4
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%a';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 4;
    
    RAISE NOTICE 'Test 4 - SUFFIX %%a: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 5
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%ab';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 5;
    
    RAISE NOTICE 'Test 5 - SUFFIX %%ab: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 6
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%ab%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 6;
    
    RAISE NOTICE 'Test 6 - SUBSTRING %%ab%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 7
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%1%2%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 7;
    
    RAISE NOTICE 'Test 7 - SUBSTRING %%1%%2%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 8
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%1%a%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 8;
    
    RAISE NOTICE 'Test 8 - SUBSTRING %%1%%a%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 9
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'a%b';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 9;
    
    RAISE NOTICE 'Test 9 - MULTI-PART a%%b: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 10
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'abc%ab%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 10;
    
    RAISE NOTICE 'Test 10 - MULTI-PART abc%%ab%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 11
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%1%a%c%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 11;
    
    RAISE NOTICE 'Test 11 - COMPLEX %%1%%a%%c%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 12
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash ILIKE 'ab%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 12;
    
    RAISE NOTICE 'Test 12 - ILIKE ab%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 13
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash ILIKE '%1%a%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 13;
    
    RAISE NOTICE 'Test 13 - ILIKE %%1%%a%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 14
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'A%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 14;
    
    RAISE NOTICE 'Test 14 - SHORT PREFIX A%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 15
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'ab%' OR hash LIKE '%a';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 15;
    
    RAISE NOTICE 'Test 15 - OR ab%% OR %%a: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 16: PREFIX with underscore - a_b%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'a_b%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 16;
    
    RAISE NOTICE 'Test 16 - PREFIX a_b%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 17: SUFFIX with underscore - %ab_
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%ab_';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 17;
    
    RAISE NOTICE 'Test 17 - SUFFIX %%ab_: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 18: INFIX simple - %abc%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%abc%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 18;
    
    RAISE NOTICE 'Test 18 - INFIX %%abc%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 19: INFIX with underscores - %a_b%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%a_b%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 19;
    
    RAISE NOTICE 'Test 19 - INFIX %%a_b%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 20: MULTI-COMPARTMENT - %abc%def%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%abc%def%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 20;
    
    RAISE NOTICE 'Test 20 - MULTI-COMPARTMENT %%abc%%def%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 21: COMPLEX INFIX with underscores - %a__b__c%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%a__b__c%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 21;
    
    RAISE NOTICE 'Test 21 - COMPLEX INFIX %%a__b__c%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 22: PREFIX with underscore and percent - _ab%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '_ab%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 22;
    
    RAISE NOTICE 'Test 22 - PREFIX _ab%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 23: SUFFIX with underscore and percent - %ab_
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%ab_';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 23;
    
    RAISE NOTICE 'Test 23 - SUFFIX %%ab_: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 24: MULTI-PART with underscore - a_b%c_d%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE 'a_b%c_d%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 24;
    
    RAISE NOTICE 'Test 24 - MULTI-PART a_b%%c_d%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 25: INFIX with starting underscore - _a%b%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '_a%b%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 25;
    
    RAISE NOTICE 'Test 25 - INFIX _a%%b%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 26: INFIX with ending underscore - %a%b_%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%a%b_%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 26;
    
    RAISE NOTICE 'Test 26 - INFIX %%a%%b_%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 27: COMPLEX MULTI-COMPARTMENT - %a_b%c_d%e%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%a_b%c_d%e%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 27;
    
    RAISE NOTICE 'Test 27 - COMPLEX %%a_b%%c_d%%e%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 28: PREFIX with multiple underscores - ___abc%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '___abc%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 28;
    
    RAISE NOTICE 'Test 28 - PREFIX ___abc%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 29: SUFFIX with multiple underscores - %abc___
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%abc___';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 29;
    
    RAISE NOTICE 'Test 29 - SUFFIX %%abc___: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 30: INFIX with underscores and percent - %a__b__c%
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM hash_data WHERE hash LIKE '%a__b__c%';
    end_time := clock_timestamp();
    
    UPDATE benchmark_results 
    SET trgm_count = row_count, 
        trgm_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_id = 30;
    
    RAISE NOTICE 'Test 30 - INFIX %%a__b__c%%: % rows in % ms', 
        row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;


DO $$ BEGIN RAISE NOTICE ''; RAISE NOTICE '✓ PG_TRGM tests complete'; RAISE NOTICE ''; END $$;

-- ============================================================================
-- PHASE 3: ANALYSIS AND COMPARISON
-- ============================================================================

DO $$ 
BEGIN 
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '📊 PHASE 3: Analysis and Comparison';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;

-- Calculate verification metrics
UPDATE benchmark_results
SET counts_match = (biscuit_count = trgm_count),
    speedup_factor = CASE 
        WHEN biscuit_time_ms > 0 THEN ROUND((trgm_time_ms / biscuit_time_ms)::NUMERIC, 2)
        ELSE NULL
    END,
    notes = CASE 
        WHEN biscuit_count = trgm_count THEN 'Counts match'
        ELSE 'COUNT MISMATCH: Biscuit=' || biscuit_count || ' vs PG_TRGM=' || trgm_count
    END;

-- Overall Summary
DO $$
DECLARE
    total_tests INT;
    passed_tests INT;
    avg_biscuit_speedup NUMERIC;
    avg_trgm_speedup NUMERIC;
    biscuit_wins INT;
    trgm_wins INT;
BEGIN
    SELECT 
        COUNT(*),
        SUM(CASE WHEN counts_match THEN 1 ELSE 0 END),
        AVG(speedup_factor),
        AVG(1.0 / NULLIF(speedup_factor, 0)),
        SUM(CASE WHEN speedup_factor > 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN speedup_factor < 1 THEN 1 ELSE 0 END)
    INTO total_tests, passed_tests, avg_biscuit_speedup, avg_trgm_speedup, biscuit_wins, trgm_wins
    FROM benchmark_results
    WHERE speedup_factor IS NOT NULL;
    
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '📈 OVERALL BENCHMARK SUMMARY';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE '✓ Total Tests Run: %', total_tests;
    RAISE NOTICE '✓ Correctness Verification: % / % tests passed (%%%)', 
        passed_tests, total_tests, (passed_tests::NUMERIC / total_tests * 100);
    RAISE NOTICE '';
    RAISE NOTICE '⚡ Performance Comparison:';
    RAISE NOTICE '   BISCUIT wins: % tests', biscuit_wins;
    RAISE NOTICE '   PG_TRGM wins: % tests', trgm_wins;
    RAISE NOTICE '   Average BISCUIT speedup: %x', COALESCE(avg_biscuit_speedup, 0);
    RAISE NOTICE '';
END $$;

-- Detailed Results Table
DO $$
DECLARE
    rec RECORD;
BEGIN
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '📋 DETAILED TEST RESULTS';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE '%-4s | %-20s | %-12s | %-10s | %-10s | %-10s | %s', 
        'ID', 'Test Name', 'Pattern Type', 'BISCUIT(ms)', 'TRGM(ms)', 'Speedup', 'Winner';
    RAISE NOTICE '%', REPEAT('-', 100);
    
    FOR rec IN 
        SELECT 
            test_id,
            SUBSTRING(test_name, 1, 20) as test_name,
            SUBSTRING(pattern_type, 1, 12) as pattern_type,
            ROUND(biscuit_time_ms, 3) as biscuit_ms,
            ROUND(trgm_time_ms, 3) as trgm_ms,
            speedup_factor,
            CASE 
                WHEN speedup_factor > 1 THEN '🏆 BISCUIT'
                WHEN speedup_factor < 1 THEN '🏆 PG_TRGM'
                ELSE '⚖️  TIE'
            END as winner,
            counts_match
        FROM benchmark_results
        ORDER BY test_id
    LOOP
        RAISE NOTICE '%-4s | %-20s | %-12s | %-10s | %-10s | %-9sx | %', 
            rec.test_id,
            rec.test_name,
            rec.pattern_type,
            rec.biscuit_ms,
            rec.trgm_ms,
            COALESCE(rec.speedup_factor::TEXT, 'N/A'),
            rec.winner;
    END LOOP;
    RAISE NOTICE '';
END $$;

-- Performance by Pattern Type
DO $$
DECLARE
    rec RECORD;
BEGIN
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '📊 PERFORMANCE BY PATTERN TYPE';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE '%-20s | %-6s | %-15s | %-15s | %-10s | %s', 
        'Pattern Type', 'Tests', 'Avg BISCUIT(ms)', 'Avg TRGM(ms)', 'Avg Speedup', 'Winner';
    RAISE NOTICE '%', REPEAT('-', 95);
    
    FOR rec IN 
        SELECT 
            pattern_type,
            COUNT(*) as test_count,
            ROUND(AVG(biscuit_time_ms), 3) as avg_biscuit,
            ROUND(AVG(trgm_time_ms), 3) as avg_trgm,
            ROUND(AVG(speedup_factor), 2) as avg_speedup,
            CASE 
                WHEN AVG(speedup_factor) > 1 THEN '🏆 BISCUIT'
                WHEN AVG(speedup_factor) < 1 THEN '🏆 PG_TRGM'
                ELSE '⚖️  TIE'
            END as winner
        FROM benchmark_results
        WHERE speedup_factor IS NOT NULL
        GROUP BY pattern_type
        ORDER BY avg_speedup DESC
    LOOP
        RAISE NOTICE '%-20s | %-6s | %-15s | %-15s | %-9sx | %', 
            rec.pattern_type,
            rec.test_count,
            rec.avg_biscuit,
            rec.avg_trgm,
            rec.avg_speedup,
            rec.winner;
    END LOOP;
    RAISE NOTICE '';
END $$;

-- Best and Worst Cases
DO $$
DECLARE
    best_rec RECORD;
    worst_rec RECORD;
BEGIN
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '🎯 BEST & WORST CASES';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    
    -- Best case for BISCUIT
    SELECT test_name, test_pattern, speedup_factor
    INTO best_rec
    FROM benchmark_results
    WHERE speedup_factor IS NOT NULL
    ORDER BY speedup_factor DESC
    LIMIT 1;
    
    RAISE NOTICE '🏆 Best Case for BISCUIT:';
    RAISE NOTICE '   Test: %', best_rec.test_name;
    RAISE NOTICE '   Pattern: %', best_rec.test_pattern;
    RAISE NOTICE '   Speedup: %x faster than PG_TRGM', best_rec.speedup_factor;
    RAISE NOTICE '';
    
    -- Worst case for BISCUIT
    SELECT test_name, test_pattern, speedup_factor
    INTO worst_rec
    FROM benchmark_results
    WHERE speedup_factor IS NOT NULL
    ORDER BY speedup_factor ASC
    LIMIT 1;
    
    RAISE NOTICE '⚠️  Worst Case for BISCUIT:';
    RAISE NOTICE '   Test: %', worst_rec.test_name;
    RAISE NOTICE '   Pattern: %', worst_rec.test_pattern;
    RAISE NOTICE '   Speedup: %x (PG_TRGM is %x faster)', 
        worst_rec.speedup_factor, (1.0 / worst_rec.speedup_factor);
    RAISE NOTICE '';
END $$;

-- Final Verdict
DO $$
DECLARE
    biscuit_wins INT;
    trgm_wins INT;
    ties INT;
    avg_speedup NUMERIC;
    correctness_pct NUMERIC;
BEGIN
    SELECT 
        SUM(CASE WHEN speedup_factor > 1.05 THEN 1 ELSE 0 END),
        SUM(CASE WHEN speedup_factor < 0.95 THEN 1 ELSE 0 END),
        SUM(CASE WHEN speedup_factor BETWEEN 0.95 AND 1.05 THEN 1 ELSE 0 END),
        AVG(speedup_factor),
        (SUM(CASE WHEN counts_match THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100)
    INTO biscuit_wins, trgm_wins, ties, avg_speedup, correctness_pct
    FROM benchmark_results
    WHERE speedup_factor IS NOT NULL;
    
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '🏁 FINAL VERDICT';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE '📊 Win/Loss Record (>5%% difference threshold):';
    RAISE NOTICE '   BISCUIT wins: % tests', biscuit_wins;
    RAISE NOTICE '   PG_TRGM wins: % tests', trgm_wins;
    RAISE NOTICE '   Ties: % tests', ties;
    RAISE NOTICE '';
    RAISE NOTICE '📈 Average Performance:';
    RAISE NOTICE '   BISCUIT is %x vs PG_TRGM on average', avg_speedup;
    RAISE NOTICE '';
    RAISE NOTICE '✓ Correctness: %%% of tests have matching counts', correctness_pct;
    RAISE NOTICE '';
    
    IF avg_speedup > 1.2 THEN
        RAISE NOTICE '🎉 CONCLUSION: BISCUIT shows significant performance advantage!';
    ELSIF avg_speedup > 1.0 THEN
        RAISE NOTICE '✅ CONCLUSION: BISCUIT shows modest performance advantage.';
    ELSIF avg_speedup > 0.8 THEN
        RAISE NOTICE '⚖️  CONCLUSION: BISCUIT and PG_TRGM are comparable.';
    ELSE
        RAISE NOTICE '⚠️  CONCLUSION: PG_TRGM shows better performance overall.';
    END IF;
    RAISE NOTICE '';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
END $$;

SELECT * FROM benchmark_results ORDER BY test_id;

DO $$ 
BEGIN 
    RAISE NOTICE '';
    RAISE NOTICE '📁 Full results available in: benchmark_results table';
    RAISE NOTICE '   Query: SELECT * FROM benchmark_results ORDER BY test_id;';
    RAISE NOTICE '';
    RAISE NOTICE '🎉 Benchmark Complete!';
    RAISE NOTICE '';
END $$;