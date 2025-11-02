-- ============================================================================
-- BISCUIT COMPREHENSIVE BENCHMARK SUITE
-- Pure SQL version for reproducible testing
-- Tests: CRUD Performance, Pattern Matching Accuracy, Index Integrity
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'BISCUIT COMPREHENSIVE BENCHMARK SUITE';
    RAISE NOTICE 'Version: 1.0';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;

-- ============================================================================
-- SETUP: Results Tables
-- ============================================================================

DROP TABLE IF EXISTS crud_results CASCADE;
DROP TABLE IF EXISTS accuracy_results CASCADE;

CREATE TABLE crud_results (
    trial INT,
    index_type TEXT,
    operation TEXT,
    time_ms NUMERIC,
    ops_per_sec NUMERIC
);

CREATE TABLE accuracy_results (
    test_id INT,
    test_name TEXT,
    pattern TEXT,
    biscuit_count INT,
    pg_count INT,
    match_status TEXT
);

-- ============================================================================
-- UTILITY FUNCTIONS
-- ============================================================================

CREATE OR REPLACE FUNCTION generate_test_name(id INT) RETURNS TEXT AS $$
BEGIN
    RETURN CASE (id % 10)
        WHEN 0 THEN 'Alexandria_' || id
        WHEN 1 THEN 'Benjamin_' || id
        WHEN 2 THEN 'Charlotte_' || id
        WHEN 3 THEN 'Davidson_' || id
        WHEN 4 THEN 'Elizabeth_' || id
        WHEN 5 THEN 'Frederick_' || id
        WHEN 6 THEN 'Gabriella_' || id
        WHEN 7 THEN 'Harrison_' || id
        WHEN 8 THEN 'Isabella_' || id
        ELSE 'Jefferson_' || id
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- PART 1: CRUD PERFORMANCE BENCHMARK
-- ============================================================================

DO $$
DECLARE
    trial INT;
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
    elapsed_ms NUMERIC;
    ops_count INT := 5000;
    base_rows INT := 100000;
    i INT;
    ops_per_sec NUMERIC;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'PART 1: CRUD PERFORMANCE BENCHMARK';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE 'Configuration:';
    RAISE NOTICE '  - Base dataset: 100,000 rows';
    RAISE NOTICE '  - Operations per trial: 5,000';
    RAISE NOTICE '  - Number of trials: 5';
    RAISE NOTICE '  - Comparing: None, BTree, pg_trgm, Biscuit';
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    
    FOR trial IN 1..5 LOOP
        RAISE NOTICE '';
        RAISE NOTICE '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
        RAISE NOTICE 'TRIAL %', trial;
        RAISE NOTICE '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
        RAISE NOTICE '';
        
        -- TEST 1: No Index (Baseline)
        RAISE NOTICE 'Testing: None (Baseline)';
        RAISE NOTICE '────────────────────────────────────────────────────────────────';
        
        DROP TABLE IF EXISTS test_none CASCADE;
        CREATE TABLE test_none (id SERIAL PRIMARY KEY, name TEXT);
        
        INSERT INTO test_none (name)
        SELECT generate_test_name(generate_series) 
        FROM generate_series(1, base_rows);
        
        RAISE NOTICE '  Testing INSERT...';
        start_time := clock_timestamp();
        FOR i IN 1..ops_count LOOP
            INSERT INTO test_none (name) VALUES ('new_' || i || '_trial' || trial);
        END LOOP;
        end_time := clock_timestamp();
        elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        ops_per_sec := (ops_count / elapsed_ms) * 1000;
        INSERT INTO crud_results VALUES (trial, 'None', 'INSERT', elapsed_ms, ops_per_sec);
        
        RAISE NOTICE '  Testing UPDATE...';
        start_time := clock_timestamp();
        FOR i IN 1..ops_count LOOP
            UPDATE test_none SET name = 'updated_' || i || '_trial' || trial 
            WHERE id = 50000 + (i % 50000);
        END LOOP;
        end_time := clock_timestamp();
        elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        ops_per_sec := (ops_count / elapsed_ms) * 1000;
        INSERT INTO crud_results VALUES (trial, 'None', 'UPDATE', elapsed_ms, ops_per_sec);
        
        RAISE NOTICE '  Testing DELETE...';
        start_time := clock_timestamp();
        FOR i IN 1..ops_count LOOP
            DELETE FROM test_none WHERE id = base_rows + i;
        END LOOP;
        end_time := clock_timestamp();
        elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        ops_per_sec := (ops_count / elapsed_ms) * 1000;
        INSERT INTO crud_results VALUES (trial, 'None', 'DELETE', elapsed_ms, ops_per_sec);
        RAISE NOTICE '';
        
        -- TEST 2: BTree Index
        RAISE NOTICE 'Testing: BTree Index';
        RAISE NOTICE '────────────────────────────────────────────────────────────────';
        
        DROP TABLE IF EXISTS test_btree CASCADE;
        CREATE TABLE test_btree (id SERIAL PRIMARY KEY, name TEXT);
        
        INSERT INTO test_btree (name)
        SELECT generate_test_name(generate_series) 
        FROM generate_series(1, base_rows);
        
        CREATE INDEX idx_btree_name ON test_btree(name);
        
        RAISE NOTICE '  Testing INSERT...';
        start_time := clock_timestamp();
        FOR i IN 1..ops_count LOOP
            INSERT INTO test_btree (name) VALUES ('new_' || i || '_trial' || trial);
        END LOOP;
        end_time := clock_timestamp();
        elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        ops_per_sec := (ops_count / elapsed_ms) * 1000;
        INSERT INTO crud_results VALUES (trial, 'BTree', 'INSERT', elapsed_ms, ops_per_sec);
        
        RAISE NOTICE '  Testing UPDATE...';
        start_time := clock_timestamp();
        FOR i IN 1..ops_count LOOP
            UPDATE test_btree SET name = 'updated_' || i || '_trial' || trial 
            WHERE id = 50000 + (i % 50000);
        END LOOP;
        end_time := clock_timestamp();
        elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        ops_per_sec := (ops_count / elapsed_ms) * 1000;
        INSERT INTO crud_results VALUES (trial, 'BTree', 'UPDATE', elapsed_ms, ops_per_sec);
        
        RAISE NOTICE '  Testing DELETE...';
        start_time := clock_timestamp();
        FOR i IN 1..ops_count LOOP
            DELETE FROM test_btree WHERE id = base_rows + i;
        END LOOP;
        end_time := clock_timestamp();
        elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        ops_per_sec := (ops_count / elapsed_ms) * 1000;
        INSERT INTO crud_results VALUES (trial, 'BTree', 'DELETE', elapsed_ms, ops_per_sec);
        RAISE NOTICE '';
        
        -- TEST 3: pg_trgm GIN Index
        RAISE NOTICE 'Testing: pg_trgm GIN Index';
        RAISE NOTICE '────────────────────────────────────────────────────────────────';
        
        DROP TABLE IF EXISTS test_trgm CASCADE;
        CREATE TABLE test_trgm (id SERIAL PRIMARY KEY, name TEXT);
        
        INSERT INTO test_trgm (name)
        SELECT generate_test_name(generate_series) 
        FROM generate_series(1, base_rows);
        
        CREATE INDEX idx_trgm_name ON test_trgm USING gin(name gin_trgm_ops);
        
        RAISE NOTICE '  Testing INSERT...';
        start_time := clock_timestamp();
        FOR i IN 1..ops_count LOOP
            INSERT INTO test_trgm (name) VALUES ('new_' || i || '_trial' || trial);
        END LOOP;
        end_time := clock_timestamp();
        elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        ops_per_sec := (ops_count / elapsed_ms) * 1000;
        INSERT INTO crud_results VALUES (trial, 'pg_trgm', 'INSERT', elapsed_ms, ops_per_sec);
        
        RAISE NOTICE '  Testing UPDATE...';
        start_time := clock_timestamp();
        FOR i IN 1..ops_count LOOP
            UPDATE test_trgm SET name = 'updated_' || i || '_trial' || trial 
            WHERE id = 50000 + (i % 50000);
        END LOOP;
        end_time := clock_timestamp();
        elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        ops_per_sec := (ops_count / elapsed_ms) * 1000;
        INSERT INTO crud_results VALUES (trial, 'pg_trgm', 'UPDATE', elapsed_ms, ops_per_sec);
        
        RAISE NOTICE '  Testing DELETE...';
        start_time := clock_timestamp();
        FOR i IN 1..ops_count LOOP
            DELETE FROM test_trgm WHERE id = base_rows + i;
        END LOOP;
        end_time := clock_timestamp();
        elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        ops_per_sec := (ops_count / elapsed_ms) * 1000;
        INSERT INTO crud_results VALUES (trial, 'pg_trgm', 'DELETE', elapsed_ms, ops_per_sec);
        RAISE NOTICE '';
        
        -- TEST 4: Biscuit Index
        RAISE NOTICE 'Testing: Biscuit Index';
        RAISE NOTICE '────────────────────────────────────────────────────────────────';
        
        DROP TABLE IF EXISTS test_biscuit CASCADE;
        CREATE TABLE test_biscuit (id SERIAL PRIMARY KEY, name TEXT);
        
        INSERT INTO test_biscuit (name)
        SELECT generate_test_name(generate_series) 
        FROM generate_series(1, base_rows);
        
        PERFORM biscuit_build_index('test_biscuit', 'name', 'id');
        
        CREATE TRIGGER biscuit_crud_trigger
            AFTER INSERT OR UPDATE OR DELETE ON test_biscuit
            FOR EACH ROW EXECUTE FUNCTION biscuit_trigger();
        
        RAISE NOTICE '  Testing INSERT...';
        start_time := clock_timestamp();
        FOR i IN 1..ops_count LOOP
            INSERT INTO test_biscuit (name) VALUES ('new_' || i || '_trial' || trial);
        END LOOP;
        end_time := clock_timestamp();
        elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        ops_per_sec := (ops_count / elapsed_ms) * 1000;
        INSERT INTO crud_results VALUES (trial, 'Biscuit', 'INSERT', elapsed_ms, ops_per_sec);
        
        RAISE NOTICE '  Testing UPDATE...';
        start_time := clock_timestamp();
        FOR i IN 1..ops_count LOOP
            UPDATE test_biscuit SET name = 'updated_' || i || '_trial' || trial 
            WHERE id = 50000 + (i % 50000);
        END LOOP;
        end_time := clock_timestamp();
        elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        ops_per_sec := (ops_count / elapsed_ms) * 1000;
        INSERT INTO crud_results VALUES (trial, 'Biscuit', 'UPDATE', elapsed_ms, ops_per_sec);
        
        RAISE NOTICE '  Testing DELETE...';
        start_time := clock_timestamp();
        FOR i IN 1..ops_count LOOP
            DELETE FROM test_biscuit WHERE id = base_rows + i;
        END LOOP;
        end_time := clock_timestamp();
        elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        ops_per_sec := (ops_count / elapsed_ms) * 1000;
        INSERT INTO crud_results VALUES (trial, 'Biscuit', 'DELETE', elapsed_ms, ops_per_sec);
        RAISE NOTICE '';
    END LOOP;
    
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'CRUD Performance Tests Complete';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;

-- ============================================================================
-- PART 2: PATTERN MATCHING ACCURACY TESTS
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'PART 2: PATTERN MATCHING ACCURACY TESTS';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;

DROP TABLE IF EXISTS test_accuracy CASCADE;
CREATE TABLE test_accuracy (
    id INTEGER PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO test_accuracy (id, value) VALUES
    (1, 'apple'), (2, 'application'), (3, 'apply'),
    (4, 'banana'), (5, 'band'), (6, 'bandana'),
    (7, 'cherry'), (8, 'cheery'), (9, 'xyz'),
    (10, 'xyzabc'), (11, 'abcxyz'), (12, 'abc'),
    (13, 'a'), (14, 'aa'), (15, 'aaa'), (16, ''),
    (17, 'test123'), (18, '123test'), (19, 'te_st'), (20, 'te%st');

SELECT biscuit_setup('test_accuracy', 'value', 'id');

DO $$
DECLARE
    test_counter INT := 0;
    b_count INT;
    p_count INT;
    match_status TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'Running Accuracy Tests...';
    RAISE NOTICE '';
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('app%') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE 'app%';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Prefix: app%', 'app%', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('ban%') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE 'ban%';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Prefix: ban%', 'ban%', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('%xyz') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE '%xyz';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Suffix: %xyz', '%xyz', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('%na') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE '%na';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Suffix: %na', '%na', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('%abc%') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE '%abc%';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Contains: %abc%', '%abc%', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('%an%') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE '%an%';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Contains: %an%', '%an%', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('abc') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE 'abc';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Exact: abc', 'abc', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('xyz') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE 'xyz';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Exact: xyz', 'xyz', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('a_') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE 'a_';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Underscore: a_', 'a_', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('___') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE '___';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Underscore: ___', '___', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('a%a') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE 'a%a';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Complex: a%a', 'a%a', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('%e%y') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE '%e%y';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Complex: %e%y', '%e%y', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('%') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE '%';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Edge: %', '%', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE '';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Edge: empty', '', b_count, p_count, match_status);
    
    test_counter := test_counter + 1;
    SELECT biscuit_match_count('%_%') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE '%_%';
    match_status := CASE WHEN b_count = p_count THEN 'PASS' ELSE 'FAIL' END;
    INSERT INTO accuracy_results VALUES (test_counter, 'Edge: %_%', '%_%', b_count, p_count, match_status);
    
    RAISE NOTICE 'Completed 15 accuracy tests';
    RAISE NOTICE '';
END $$;

-- ============================================================================
-- PART 3: CRUD INTEGRITY TESTS
-- ============================================================================

DO $$
DECLARE
    b_count INT;
    p_count INT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'PART 3: CRUD INTEGRITY TESTS';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    
    RAISE NOTICE 'Testing INSERT operations...';
    INSERT INTO test_accuracy (id, value) VALUES
        (100, 'newapple'), (101, 'newbanana'), (102, 'newcherry');
    SELECT biscuit_match_count('new%') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE 'new%';
    IF b_count = p_count THEN
        RAISE NOTICE 'INSERT test: PASS';
    ELSE
        RAISE NOTICE 'INSERT test: FAIL';
    END IF;
    RAISE NOTICE '';
    
    RAISE NOTICE 'Testing UPDATE operations...';
    UPDATE test_accuracy SET value = 'appla' WHERE id = 1;
    SELECT biscuit_match_count('appla') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE 'appla';
    IF b_count = p_count THEN
        RAISE NOTICE 'UPDATE test: PASS';
    ELSE
        RAISE NOTICE 'UPDATE test: FAIL';
    END IF;
    RAISE NOTICE '';
    
    RAISE NOTICE 'Testing DELETE operations...';
    DELETE FROM test_accuracy WHERE id IN (4, 5, 6);
    SELECT biscuit_match_count('ban%') INTO b_count;
    SELECT COUNT(*) INTO p_count FROM test_accuracy WHERE value LIKE 'ban%';
    IF b_count = p_count THEN
        RAISE NOTICE 'DELETE test: PASS';
    ELSE
        RAISE NOTICE 'DELETE test: FAIL';
    END IF;
    RAISE NOTICE '';
END $$;

-- ============================================================================
-- RESULTS ANALYSIS
-- ============================================================================

DO $$
DECLARE
    biscuit_avg NUMERIC;
    btree_avg NUMERIC;
    trgm_avg NUMERIC;
    none_avg NUMERIC;
    biscuit_overhead_pct NUMERIC;
    btree_overhead_pct NUMERIC;
    trgm_overhead_pct NUMERIC;
    passed_tests INT;
    total_tests INT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'BENCHMARK RESULTS ANALYSIS';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    
    SELECT AVG(time_ms) INTO none_avg FROM crud_results WHERE index_type='None';
    SELECT AVG(time_ms) INTO btree_avg FROM crud_results WHERE index_type='BTree';
    SELECT AVG(time_ms) INTO trgm_avg FROM crud_results WHERE index_type='pg_trgm';
    SELECT AVG(time_ms) INTO biscuit_avg FROM crud_results WHERE index_type='Biscuit';
    
    btree_overhead_pct := ((btree_avg - none_avg) / none_avg) * 100;
    trgm_overhead_pct := ((trgm_avg - none_avg) / none_avg) * 100;
    biscuit_overhead_pct := ((biscuit_avg - none_avg) / none_avg) * 100;
    
    RAISE NOTICE '1. CRUD PERFORMANCE SUMMARY';
    RAISE NOTICE '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
    RAISE NOTICE '';
    RAISE NOTICE 'Average time per operation:';
    RAISE NOTICE '';
    RAISE NOTICE 'Baseline (None): % ms', ROUND(none_avg, 1);
    RAISE NOTICE 'BTree Index: % ms (overhead: %%)', ROUND(btree_avg, 1), ROUND(btree_overhead_pct, 1);
    RAISE NOTICE 'pg_trgm GIN: % ms (overhead: %%)', ROUND(trgm_avg, 1), ROUND(trgm_overhead_pct, 1);
    RAISE NOTICE 'Biscuit: % ms (overhead: %%)', ROUND(biscuit_avg, 1), ROUND(biscuit_overhead_pct, 1);
    RAISE NOTICE '';
    
    SELECT COUNT(*) INTO total_tests FROM accuracy_results;
    SELECT COUNT(*) INTO passed_tests FROM accuracy_results WHERE match_status = 'PASS';
    
    RAISE NOTICE '2. PATTERN MATCHING ACCURACY';
    RAISE NOTICE '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
    RAISE NOTICE '';
    RAISE NOTICE 'Tests completed: %', total_tests;
    RAISE NOTICE 'Tests passed: %', passed_tests;
    RAISE NOTICE '';
    
    IF passed_tests = total_tests THEN
        RAISE NOTICE 'Result: ALL TESTS PASSED';
    ELSE
        RAISE NOTICE 'Result: SOME TESTS FAILED';
    END IF;
    RAISE NOTICE '';
    
    RAISE NOTICE '3. OVERALL ASSESSMENT';
    RAISE NOTICE '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
    RAISE NOTICE '';
    
    IF ABS(biscuit_overhead_pct) < 10 THEN
        RAISE NOTICE 'CRUD Performance: EXCELLENT';
    ELSIF biscuit_overhead_pct < 25 THEN
        RAISE NOTICE 'CRUD Performance: GOOD';
    ELSIF biscuit_overhead_pct < 50 THEN
        RAISE NOTICE 'CRUD Performance: MODERATE';
    ELSIF biscuit_overhead_pct < 100 THEN
        RAISE NOTICE 'CRUD Performance: HIGH';
    ELSE
        RAISE NOTICE 'CRUD Performance: CRITICAL';
    END IF;
    
    IF passed_tests = total_tests THEN
        RAISE NOTICE 'Pattern Matching: PERFECT';
    ELSIF passed_tests >= total_tests * 0.9 THEN
        RAISE NOTICE 'Pattern Matching: GOOD';
    ELSE
        RAISE NOTICE 'Pattern Matching: NEEDS IMPROVEMENT';
    END IF;
    
    RAISE NOTICE '';
END $$;

-- ============================================================================
-- DETAILED RESULTS TABLES
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'DETAILED RESULTS';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE 'Table 1: CRUD Performance by Operation';
    RAISE NOTICE '';
END $$;

SELECT 
    index_type,
    operation,
    ROUND(AVG(time_ms), 2) as avg_ms,
    ROUND(MIN(time_ms), 2) as min_ms,
    ROUND(MAX(time_ms), 2) as max_ms,
    ROUND(STDDEV(time_ms), 2) as stddev_ms,
    ROUND(AVG(ops_per_sec), 0) as avg_ops_per_sec
FROM crud_results
GROUP BY index_type, operation
ORDER BY 
    CASE operation 
        WHEN 'INSERT' THEN 1 
        WHEN 'UPDATE' THEN 2 
        WHEN 'DELETE' THEN 3 
    END,
    index_type;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'Table 2: Pattern Matching Accuracy Details';
    RAISE NOTICE '';
END $$;

SELECT 
    test_id,
    test_name,
    pattern,
    biscuit_count as biscuit,
    pg_count as postgres,
    match_status as status
FROM accuracy_results
ORDER BY test_id;

-- ============================================================================
-- COMPARATIVE ANALYSIS
-- ============================================================================

DO $$
DECLARE
    rec RECORD;
    biscuit_vs_none NUMERIC;
    biscuit_vs_btree NUMERIC;
    biscuit_vs_trgm NUMERIC;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'COMPARATIVE ANALYSIS';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE 'Biscuit vs Other Indexes (by operation):';
    RAISE NOTICE '';
    
    FOR rec IN 
        SELECT 
            operation,
            AVG(CASE WHEN index_type = 'None' THEN time_ms END) as none_avg,
            AVG(CASE WHEN index_type = 'BTree' THEN time_ms END) as btree_avg,
            AVG(CASE WHEN index_type = 'pg_trgm' THEN time_ms END) as trgm_avg,
            AVG(CASE WHEN index_type = 'Biscuit' THEN time_ms END) as biscuit_avg
        FROM crud_results
        GROUP BY operation
        ORDER BY 
            CASE operation 
                WHEN 'INSERT' THEN 1 
                WHEN 'UPDATE' THEN 2 
                WHEN 'DELETE' THEN 3 
            END
    LOOP
        biscuit_vs_none := ((rec.biscuit_avg - rec.none_avg) / rec.none_avg) * 100;
        biscuit_vs_btree := ((rec.biscuit_avg - rec.btree_avg) / rec.btree_avg) * 100;
        biscuit_vs_trgm := ((rec.biscuit_avg - rec.trgm_avg) / rec.trgm_avg) * 100;
        
        RAISE NOTICE '%:', rec.operation;
        RAISE NOTICE '  vs None (baseline): %% overhead', ROUND(biscuit_vs_none, 1);
        RAISE NOTICE '  vs BTree: %% difference', ROUND(biscuit_vs_btree, 1);
        RAISE NOTICE '  vs pg_trgm: %% difference', ROUND(biscuit_vs_trgm, 1);
        RAISE NOTICE '';
    END LOOP;
END $$;

-- ============================================================================
-- PERFORMANCE PERCENTILES
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'PERFORMANCE DISTRIBUTION';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE 'Table 3: Performance Percentiles (milliseconds)';
    RAISE NOTICE '';
END $$;

WITH percentiles AS (
    SELECT 
        index_type,
        operation,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY time_ms) as p50,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY time_ms) as p90,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY time_ms) as p95,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY time_ms) as p99
    FROM crud_results
    GROUP BY index_type, operation
)
SELECT 
    index_type,
    operation,
    ROUND(p50, 2) as median_ms,
    ROUND(p90, 2) as p90_ms,
    ROUND(p95, 2) as p95_ms,
    ROUND(p99, 2) as p99_ms
FROM percentiles
ORDER BY 
    CASE operation 
        WHEN 'INSERT' THEN 1 
        WHEN 'UPDATE' THEN 2 
        WHEN 'DELETE' THEN 3 
    END,
    index_type;

-- ============================================================================
-- OPERATION THROUGHPUT COMPARISON
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'THROUGHPUT ANALYSIS';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE 'Table 4: Average Throughput (operations per second)';
    RAISE NOTICE '';
END $$;

SELECT 
    index_type,
    ROUND(AVG(CASE WHEN operation = 'INSERT' THEN ops_per_sec END), 0) as insert_ops_sec,
    ROUND(AVG(CASE WHEN operation = 'UPDATE' THEN ops_per_sec END), 0) as update_ops_sec,
    ROUND(AVG(CASE WHEN operation = 'DELETE' THEN ops_per_sec END), 0) as delete_ops_sec,
    ROUND(AVG(ops_per_sec), 0) as overall_avg_ops_sec
FROM crud_results
GROUP BY index_type
ORDER BY index_type;

-- ============================================================================
-- ACCURACY TEST DETAILS
-- ============================================================================

DO $$
DECLARE
    failed_count INT;
    failed_tests TEXT := '';
    rec RECORD;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'ACCURACY TEST ANALYSIS';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    
    SELECT COUNT(*) INTO failed_count 
    FROM accuracy_results 
    WHERE match_status = 'FAIL';
    
    IF failed_count > 0 THEN
        RAISE NOTICE 'FAILED TESTS DETAILS:';
        RAISE NOTICE '────────────────────────────────────────────────────────────────';
        RAISE NOTICE '';
        
        FOR rec IN 
            SELECT test_name, pattern, biscuit_count, pg_count 
            FROM accuracy_results 
            WHERE match_status = 'FAIL'
            ORDER BY test_id
        LOOP
            RAISE NOTICE 'Test: %', rec.test_name;
            RAISE NOTICE '  Pattern: %', rec.pattern;
            RAISE NOTICE '  Biscuit count: %', rec.biscuit_count;
            RAISE NOTICE '  Expected count: %', rec.pg_count;
            RAISE NOTICE '  Difference: %', (rec.biscuit_count - rec.pg_count);
            RAISE NOTICE '';
        END LOOP;
    ELSE
        RAISE NOTICE 'All accuracy tests passed successfully!';
        RAISE NOTICE '';
    END IF;
END $$;

-- ============================================================================
-- STATISTICAL SIGNIFICANCE
-- ============================================================================

DO $$
DECLARE
    rec RECORD;
    cv NUMERIC;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'STATISTICAL ANALYSIS';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE 'Coefficient of Variation (CV) by Index Type:';
    RAISE NOTICE '(Lower is better - indicates more consistent performance)';
    RAISE NOTICE '';
    
    FOR rec IN 
        SELECT 
            index_type,
            operation,
            AVG(time_ms) as mean_time,
            STDDEV(time_ms) as std_time
        FROM crud_results
        GROUP BY index_type, operation
        ORDER BY index_type, operation
    LOOP
        IF rec.mean_time > 0 THEN
            cv := (rec.std_time / rec.mean_time) * 100;
            RAISE NOTICE '% - %: CV = %%', 
                RPAD(rec.index_type, 10), 
                RPAD(rec.operation, 10), 
                ROUND(cv, 2);
        END IF;
    END LOOP;
    RAISE NOTICE '';
END $$;

-- ============================================================================
-- INDEX OVERHEAD SUMMARY
-- ============================================================================

DO $$
DECLARE
    rec RECORD;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'INDEX MAINTENANCE OVERHEAD';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE 'Table 5: Overhead vs Baseline (percentage)';
    RAISE NOTICE '';
    
    FOR rec IN 
        WITH baseline AS (
            SELECT operation, AVG(time_ms) as baseline_time
            FROM crud_results
            WHERE index_type = 'None'
            GROUP BY operation
        )
        SELECT 
            c.index_type,
            c.operation,
            AVG(c.time_ms) as avg_time,
            b.baseline_time,
            ((AVG(c.time_ms) - b.baseline_time) / b.baseline_time * 100) as overhead_pct
        FROM crud_results c
        JOIN baseline b ON c.operation = b.operation
        WHERE c.index_type != 'None'
        GROUP BY c.index_type, c.operation, b.baseline_time
        ORDER BY c.operation, c.index_type
    LOOP
        RAISE NOTICE '% - %: %%', 
            RPAD(rec.index_type, 10), 
            RPAD(rec.operation, 10), 
            ROUND(rec.overhead_pct, 1);
    END LOOP;
    RAISE NOTICE '';
END $$;

-- ============================================================================
-- FINAL SUMMARY AND RECOMMENDATIONS
-- ============================================================================

DO $$
DECLARE
    biscuit_avg NUMERIC;
    btree_avg NUMERIC;
    trgm_avg NUMERIC;
    none_avg NUMERIC;
    biscuit_overhead NUMERIC;
    accuracy_rate NUMERIC;
    recommendation TEXT;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'FINAL SUMMARY AND RECOMMENDATIONS';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    
    -- Calculate averages
    SELECT AVG(time_ms) INTO none_avg FROM crud_results WHERE index_type='None';
    SELECT AVG(time_ms) INTO btree_avg FROM crud_results WHERE index_type='BTree';
    SELECT AVG(time_ms) INTO trgm_avg FROM crud_results WHERE index_type='pg_trgm';
    SELECT AVG(time_ms) INTO biscuit_avg FROM crud_results WHERE index_type='Biscuit';
    
    biscuit_overhead := ((biscuit_avg - none_avg) / none_avg) * 100;
    
    SELECT 
        (COUNT(*) FILTER (WHERE match_status = 'PASS')::NUMERIC / COUNT(*) * 100)
    INTO accuracy_rate
    FROM accuracy_results;
    
    RAISE NOTICE 'KEY METRICS:';
    RAISE NOTICE '────────────────────────────────────────────────────────────────';
    RAISE NOTICE 'Biscuit CRUD Overhead: %%', ROUND(biscuit_overhead, 1);
    RAISE NOTICE 'Pattern Matching Accuracy: %%', ROUND(accuracy_rate, 1);
    RAISE NOTICE '';
    
    RAISE NOTICE 'PERFORMANCE RANKING (Lower time is better):';
    RAISE NOTICE '────────────────────────────────────────────────────────────────';
    RAISE NOTICE '1. None (baseline): % ms', ROUND(none_avg, 2);
    
    IF btree_avg <= biscuit_avg AND btree_avg <= trgm_avg THEN
        RAISE NOTICE '2. BTree: % ms', ROUND(btree_avg, 2);
        IF biscuit_avg <= trgm_avg THEN
            RAISE NOTICE '3. Biscuit: % ms', ROUND(biscuit_avg, 2);
            RAISE NOTICE '4. pg_trgm: % ms', ROUND(trgm_avg, 2);
        ELSE
            RAISE NOTICE '3. pg_trgm: % ms', ROUND(trgm_avg, 2);
            RAISE NOTICE '4. Biscuit: % ms', ROUND(biscuit_avg, 2);
        END IF;
    ELSIF biscuit_avg <= btree_avg AND biscuit_avg <= trgm_avg THEN
        RAISE NOTICE '2. Biscuit: % ms', ROUND(biscuit_avg, 2);
        IF btree_avg <= trgm_avg THEN
            RAISE NOTICE '3. BTree: % ms', ROUND(btree_avg, 2);
            RAISE NOTICE '4. pg_trgm: % ms', ROUND(trgm_avg, 2);
        ELSE
            RAISE NOTICE '3. pg_trgm: % ms', ROUND(trgm_avg, 2);
            RAISE NOTICE '4. BTree: % ms', ROUND(btree_avg, 2);
        END IF;
    ELSE
        RAISE NOTICE '2. pg_trgm: % ms', ROUND(trgm_avg, 2);
        IF btree_avg <= biscuit_avg THEN
            RAISE NOTICE '3. BTree: % ms', ROUND(btree_avg, 2);
            RAISE NOTICE '4. Biscuit: % ms', ROUND(biscuit_avg, 2);
        ELSE
            RAISE NOTICE '3. Biscuit: % ms', ROUND(biscuit_avg, 2);
            RAISE NOTICE '4. BTree: % ms', ROUND(btree_avg, 2);
        END IF;
    END IF;
    RAISE NOTICE '';
    
    RAISE NOTICE 'RECOMMENDATION:';
    RAISE NOTICE '────────────────────────────────────────────────────────────────';
    
    IF accuracy_rate < 100 THEN
        recommendation := 'CRITICAL: Fix accuracy issues before production use.';
    ELSIF biscuit_overhead < 15 THEN
        recommendation := 'EXCELLENT: Biscuit shows competitive CRUD performance with accurate pattern matching.';
    ELSIF biscuit_overhead < 30 THEN
        recommendation := 'GOOD: Biscuit is suitable for workloads with moderate write requirements.';
    ELSIF biscuit_overhead < 50 THEN
        recommendation := 'ACCEPTABLE: Consider Biscuit for read-heavy workloads with occasional writes.';
    ELSIF biscuit_overhead < 100 THEN
        recommendation := 'CAUTION: High overhead. Only use for read-dominated workloads.';
    ELSE
        recommendation := 'WARNING: Very high CRUD overhead. Evaluate if benefits justify the cost.';
    END IF;
    
    RAISE NOTICE '%', recommendation;
    RAISE NOTICE '';
    
    RAISE NOTICE 'USE CASES:';
    RAISE NOTICE '────────────────────────────────────────────────────────────────';
    RAISE NOTICE 'Biscuit is best suited for:';
    RAISE NOTICE '  • Wildcard pattern matching queries (%%pattern%%)';
    RAISE NOTICE '  • Read-heavy workloads with complex search patterns';
    RAISE NOTICE '  • Tables with infrequent updates/deletes';
    RAISE NOTICE '';
    RAISE NOTICE 'Consider alternatives if:';
    RAISE NOTICE '  • High-frequency UPDATE/DELETE operations';
    RAISE NOTICE '  • Simple exact-match queries (use BTree)';
    RAISE NOTICE '  • Full-text search requirements (use pg_trgm)';
    RAISE NOTICE '';
END $$;

-- ============================================================================
-- CLEANUP NOTICE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE 'BENCHMARK COMPLETE';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE 'Results have been stored in:';
    RAISE NOTICE '  • crud_results table';
    RAISE NOTICE '  • accuracy_results table';
    RAISE NOTICE '';
    RAISE NOTICE 'To export results:';
    RAISE NOTICE '  \copy crud_results TO ''crud_results.csv'' CSV HEADER';
    RAISE NOTICE '  \copy accuracy_results TO ''accuracy_results.csv'' CSV HEADER';
    RAISE NOTICE '';
    RAISE NOTICE 'To clean up test tables:';
    RAISE NOTICE '  DROP TABLE test_none, test_btree, test_trgm, test_biscuit, test_accuracy CASCADE;';
    RAISE NOTICE '';
    RAISE NOTICE '════════════════════════════════════════════════════════════════';
END $$;
