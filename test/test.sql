-- ============================================================================
-- BISCUIT CRUD VERIFICATION TEST SUITE
-- Comprehensive accuracy verification for all BISCUIT functions
-- ============================================================================


-- ============================================================================
-- TEST SETUP
-- ============================================================================
\timing on
\pset pager off


DROP TABLE IF EXISTS test_sequences CASCADE;
DROP TABLE IF EXISTS test_results CASCADE;

-- Create test table
CREATE TABLE test_sequences (
    id SERIAL PRIMARY KEY,
    seq TEXT NOT NULL,
    category TEXT
);

-- Create results tracking table
CREATE TABLE test_results (
    test_num INTEGER PRIMARY KEY,
    test_name TEXT NOT NULL,
    status TEXT NOT NULL,
    details TEXT,
    timestamp TIMESTAMP DEFAULT NOW()
);

-- Insert initial test data with known patterns
INSERT INTO test_sequences (seq, category) VALUES
    ('alpha', 'group1'),
    ('beta', 'group1'),
    ('gamma', 'group1'),
    ('alphabet', 'group2'),
    ('abc123', 'group2'),
    ('xyz789', 'group2'),
    ('test', 'group3'),
    ('testing', 'group3'),
    ('tester', 'group3'),
    ('production', 'group3');

-- ============================================================================
-- TEST HELPER FUNCTIONS
-- ============================================================================

CREATE OR REPLACE FUNCTION assert_equal(
    test_num INTEGER,
    test_name TEXT,
    expected BIGINT,
    actual BIGINT
) RETURNS VOID AS $$
BEGIN
    IF expected = actual THEN
        INSERT INTO test_results (test_num, test_name, status, details)
        VALUES (test_num, test_name, 'PASS', format('Expected: %s, Got: %s', expected, actual));
    ELSE
        INSERT INTO test_results (test_num, test_name, status, details)
        VALUES (test_num, test_name, 'FAIL', format('Expected: %s, Got: %s', expected, actual));
        RAISE NOTICE 'TEST % FAILED: % - Expected: %, Got: %', test_num, test_name, expected, actual;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assert_sets_equal(
    test_num INTEGER,
    test_name TEXT,
    expected_ids INTEGER[],
    actual_ids INTEGER[]
) RETURNS VOID AS $$
DECLARE
    sorted_expected INTEGER[];
    sorted_actual INTEGER[];
BEGIN
    sorted_expected := (SELECT ARRAY_AGG(x ORDER BY x) FROM UNNEST(expected_ids) x);
    sorted_actual := (SELECT ARRAY_AGG(x ORDER BY x) FROM UNNEST(actual_ids) x);
    
    IF sorted_expected = sorted_actual THEN
        INSERT INTO test_results (test_num, test_name, status, details)
        VALUES (test_num, test_name, 'PASS', format('Expected IDs: %s, Got: %s', sorted_expected, sorted_actual));
    ELSE
        INSERT INTO test_results (test_num, test_name, status, details)
        VALUES (test_num, test_name, 'FAIL', format('Expected IDs: %s, Got: %s', sorted_expected, sorted_actual));
        RAISE NOTICE 'TEST % FAILED: % - Expected: %, Got: %', test_num, test_name, sorted_expected, sorted_actual;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- PHASE 1: INITIAL BUILD AND BASIC QUERIES
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 1: INITIAL BUILD AND BASIC QUERIES'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Build index
SELECT biscuit_setup('test_sequences', 'seq', 'id');

-- Test 1: Verify index status shows correct initial count
SELECT assert_equal(
    1,
    'Initial index build - active count',
    10::BIGINT,
    biscuit_get_active_count()::BIGINT
);

-- Test 2: Match count for 'alpha%' pattern
SELECT assert_equal(
    2,
    'Match count: alpha%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'alpha%'),
    biscuit_match_count('alpha%')::BIGINT
);

-- Test 3: Match count for '%test%' pattern
SELECT assert_equal(
    3,
    'Match count: %test%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%test%'),
    biscuit_match_count('%test%')::BIGINT
);

-- Test 4: Match count for 'beta' exact match
SELECT assert_equal(
    4,
    'Match count: beta',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'beta'),
    biscuit_match_count('beta')::BIGINT
);

-- Test 5: Verify biscuit_match_keys returns correct IDs
SELECT assert_sets_equal(
    5,
    'Match keys: %test% - ID verification',
    ARRAY(SELECT id FROM test_sequences WHERE seq LIKE '%test%' ORDER BY id),
    ARRAY(SELECT pk::INTEGER FROM biscuit_match_keys('%test%') ORDER BY pk)
);

-- Test 6: Verify biscuit_match() returns correct full tuples
SELECT assert_equal(
    6,
    'Match tuples: alpha% - count verification',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'alpha%'),
    (SELECT COUNT(*)::BIGINT FROM biscuit_match('alpha%'))
);

-- Test 7: Verify no false positives
SELECT assert_equal(
    7,
    'No matches for nonexistent pattern',
    0::BIGINT,
    biscuit_match_count('zzz%')::BIGINT
);

-- Test 8: Underscore wildcard '_' (exactly one character)
SELECT assert_equal(
    8,
    'Match count: _eta (underscore wildcard)',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '_eta'),
    biscuit_match_count('_eta')::BIGINT
);

-- ============================================================================
-- PHASE 2: INSERT OPERATIONS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 2: INSERT OPERATIONS'; END $$;
DO $$ BEGIN RAISE NOTICE '================================'; END $$;

-- Test 9: Insert new record and verify index updates
INSERT INTO test_sequences (seq, category) VALUES ('newalpha', 'group4');

SELECT assert_equal(
    9,
    'After INSERT - active count increased',
    11::BIGINT,
    biscuit_get_active_count()::BIGINT
);

-- Test 10: Verify new record is findable
SELECT assert_equal(
    10,
    'After INSERT - new record matches pattern',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%alpha%'),
    biscuit_match_count('%alpha%')::BIGINT
);

-- Test 11: Insert multiple records
INSERT INTO test_sequences (seq, category) VALUES 
    ('prefix_test', 'group5'),
    ('test_suffix', 'group5'),
    ('another_alpha', 'group5');

SELECT assert_equal(
    11,
    'After bulk INSERT - active count',
    14::BIGINT,
    biscuit_get_active_count()::BIGINT
);

-- Test 12: Verify all new records are indexed
SELECT assert_equal(
    12,
    'After bulk INSERT - %test% pattern count',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%test%'),
    biscuit_match_count('%test%')::BIGINT
);

-- Test 13: Verify biscuit_match returns new records
SELECT assert_sets_equal(
    13,
    'After INSERT - biscuit_match returns all alpha records',
    ARRAY(SELECT id FROM test_sequences WHERE seq LIKE '%alpha%' ORDER BY id),
    ARRAY(SELECT id FROM biscuit_match('%alpha%') ORDER BY id)
);

-- ============================================================================
-- PHASE 3: UPDATE OPERATIONS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 3: UPDATE OPERATIONS'; END $$;
DO $$ BEGIN RAISE NOTICE '================================'; END $$;

-- Test 14: Update record - old pattern should no longer match
UPDATE test_sequences SET seq = 'updated_value' WHERE seq = 'beta';

SELECT assert_equal(
    14,
    'After UPDATE - old pattern (beta) should not match',
    0::BIGINT,
    biscuit_match_count('beta')::BIGINT
);

-- Test 15: Verify updated record matches new pattern
SELECT assert_equal(
    15,
    'After UPDATE - new pattern (updated%) should match',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'updated%'),
    biscuit_match_count('updated%')::BIGINT
);

-- Test 16: Active count should remain the same after UPDATE
SELECT assert_equal(
    16,
    'After UPDATE - active count unchanged',
    14::BIGINT,
    biscuit_get_active_count()::BIGINT
);

-- Test 17: Update multiple records
UPDATE test_sequences SET seq = 'modified_' || seq WHERE category = 'group1';

SELECT assert_equal(
    17,
    'After bulk UPDATE - modified_% pattern count',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'modified_%'),
    biscuit_match_count('modified_%')::BIGINT
);

-- Test 18: Verify old patterns no longer match after bulk update
SELECT assert_equal(
    18,
    'After bulk UPDATE - old pattern (gamma) should not match',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'gamma'),
    biscuit_match_count('gamma')::BIGINT
);

-- Test 19: Update to same value should work correctly
UPDATE test_sequences SET seq = seq WHERE id = 1;

SELECT assert_equal(
    19,
    'After no-op UPDATE - active count unchanged',
    14::BIGINT,
    biscuit_get_active_count()::BIGINT
);

-- ============================================================================
-- PHASE 4: DELETE OPERATIONS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 4: DELETE OPERATIONS'; END $$;
DO $$ BEGIN RAISE NOTICE '================================'; END $$;

-- Test 20: Delete single record
DELETE FROM test_sequences WHERE seq = 'test';

SELECT assert_equal(
    20,
    'After DELETE - active count decreased',
    13::BIGINT,
    biscuit_get_active_count()::BIGINT
);

-- Test 21: Verify deleted record no longer matches
SELECT assert_equal(
    21,
    'After DELETE - exact match should not find deleted record',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq = 'test'),
    biscuit_match_count('test')::BIGINT
);

-- Test 22: Delete multiple records
DELETE FROM test_sequences WHERE category = 'group3';

SELECT assert_equal(
    22,
    'After bulk DELETE - active count',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- Test 23: Verify bulk deleted records don't match
SELECT assert_equal(
    23,
    'After bulk DELETE - %test% pattern count',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%test%'),
    biscuit_match_count('%test%')::BIGINT
);

-- Test 24: Verify free slots increased (tombstones created)
DO $$
DECLARE
    free_slots INTEGER;
BEGIN
    free_slots := biscuit_get_free_slots();
    IF free_slots > 0 THEN
        INSERT INTO test_results (test_num, test_name, status, details)
        VALUES (24, 'Free slots after DELETE', 'PASS', format('Free slots: %s', free_slots));
    ELSE
        INSERT INTO test_results (test_num, test_name, status, details)
        VALUES (24, 'Free slots after DELETE', 'WARNING', 'Free slots is 0, may need different implementation');
    END IF;
END $$;

-- ============================================================================
-- PHASE 5: MIXED CRUD OPERATIONS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '===================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 5: MIXED CRUD OPERATIONS'; END $$;
DO $$ BEGIN RAISE NOTICE '===================================='; END $$;

-- Test 25: Insert, Update, Delete in sequence
INSERT INTO test_sequences (seq, category) VALUES ('temp_record', 'temp');
UPDATE test_sequences SET seq = 'temp_modified' WHERE seq = 'temp_record';
DELETE FROM test_sequences WHERE seq = 'temp_modified';

SELECT assert_equal(
    25,
    'After INSERT-UPDATE-DELETE sequence - count unchanged',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- Test 26: Complex pattern after mixed operations
SELECT assert_equal(
    26,
    'Complex pattern %a%a% after mixed ops',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%a%a%'),
    biscuit_match_count('%a%a%')::BIGINT
);

-- Test 27: Verify biscuit_match_keys consistency
SELECT assert_sets_equal(
    27,
    'Match keys consistency after mixed ops',
    ARRAY(SELECT id FROM test_sequences WHERE seq LIKE '%alpha%' ORDER BY id),
    ARRAY(SELECT pk::INTEGER FROM biscuit_match_keys('%alpha%') ORDER BY pk)
);

-- ============================================================================
-- PHASE 6: EDGE CASES
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================'; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 6: EDGE CASES'; END $$;
DO $$ BEGIN RAISE NOTICE '========================'; END $$;

-- Test 28: Empty string pattern
SELECT assert_equal(
    28,
    'Empty string pattern',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE ''),
    biscuit_match_count('')::BIGINT
);

-- Test 29: Single character patterns
INSERT INTO test_sequences (seq, category) VALUES ('a', 'single'), ('z', 'single');

SELECT assert_equal(
    29,
    'Single character pattern: a',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'a'),
    biscuit_match_count('a')::BIGINT
);

-- Test 30: Pattern with multiple wildcards
SELECT assert_equal(
    30,
    'Multiple wildcards: %a%e%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%a%e%'),
    biscuit_match_count('%a%e%')::BIGINT
);

-- Test 31: Update to NULL (if column allows)
-- Skip if NOT NULL constraint exists

-- Test 32: Verify all biscuit_match variants return same IDs
DO $$
DECLARE
    ids_match INTEGER[];
    ids_match_rows INTEGER[];
    ids_match_keys INTEGER[];
    all_equal BOOLEAN;
BEGIN
    ids_match := ARRAY(SELECT id FROM biscuit_match('%alpha%') ORDER BY id);
    ids_match_rows := ARRAY(SELECT (t).id FROM biscuit_match_rows('%alpha%') t ORDER BY (t).id);
    ids_match_keys := ARRAY(SELECT pk::INTEGER FROM biscuit_match_keys('%alpha%') ORDER BY pk);
    
    all_equal := (ids_match = ids_match_rows) AND (ids_match = ids_match_keys);
    
    IF all_equal THEN
        INSERT INTO test_results (test_num, test_name, status, details)
        VALUES (32, 'All match function variants return same IDs', 'PASS', 
                format('All return: %s', ids_match));
    ELSE
        INSERT INTO test_results (test_num, test_name, status, details)
        VALUES (32, 'All match function variants return same IDs', 'FAIL',
                format('match: %s, match_rows: %s, match_keys: %s', 
                       ids_match, ids_match_rows, ids_match_keys));
    END IF;
END $$;

-- ============================================================================
-- PHASE 7: STRESS TEST
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================'; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 7: STRESS TEST'; END $$;
DO $$ BEGIN RAISE NOTICE '========================'; END $$;

-- Test 33: Bulk insert many records
INSERT INTO test_sequences (seq, category)
SELECT 
    'stress_' || i::TEXT || '_' || (i % 10)::TEXT,
    'stress'
FROM generate_series(1, 100) i;

SELECT assert_equal(
    33,
    'After bulk stress insert - active count',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- Test 34: Pattern matching on stress data
SELECT assert_equal(
    34,
    'Stress test pattern: stress_%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'stress_%'),
    biscuit_match_count('stress_%')::BIGINT
);

-- Test 35: Update half of stress records
UPDATE test_sequences SET seq = 'updated_stress_' || id WHERE category = 'stress' AND id % 2 = 0;

SELECT assert_equal(
    35,
    'After stress UPDATE - updated_stress_% count',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'updated_stress_%'),
    biscuit_match_count('updated_stress_%')::BIGINT
);

-- Test 36: Delete half of stress records
DELETE FROM test_sequences WHERE category = 'stress' AND id % 2 = 1;

SELECT assert_equal(
    36,
    'After stress DELETE - final active count',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- ============================================================================
-- FINAL RESULTS SUMMARY
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'TEST RESULTS SUMMARY'; END $$;
DO $$ BEGIN RAISE NOTICE '================================'; END $$;

DO $$
DECLARE
    total_tests INTEGER;
    passed_tests INTEGER;
    failed_tests INTEGER;
    warning_tests INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_tests FROM test_results;
    SELECT COUNT(*) INTO passed_tests FROM test_results WHERE status = 'PASS';
    SELECT COUNT(*) INTO failed_tests FROM test_results WHERE status = 'FAIL';
    SELECT COUNT(*) INTO warning_tests FROM test_results WHERE status = 'WARNING';
    
    RAISE NOTICE 'Total Tests: %', total_tests;
    RAISE NOTICE 'Passed: % (%.1f%%)', passed_tests, (passed_tests::FLOAT / total_tests * 100);
    RAISE NOTICE 'Failed: %', failed_tests;
    RAISE NOTICE 'Warnings: %', warning_tests;
    RAISE NOTICE '';
    
    IF failed_tests > 0 THEN
        RAISE NOTICE 'FAILED TESTS:';
        FOR rec IN SELECT test_num, test_name, details FROM test_results WHERE status = 'FAIL' ORDER BY test_num LOOP
            RAISE NOTICE 'Test %: % - %', rec.test_num, rec.test_name, rec.details;
        END LOOP;
    END IF;
END $$;

-- Display all test results
SELECT 
    test_num,
    test_name,
    status,
    CASE 
        WHEN LENGTH(details) > 80 THEN LEFT(details, 77) || '...'
        ELSE details
    END AS details
FROM test_results
ORDER BY test_num;

-- Display final index status
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'FINAL INDEX STATUS'; END $$;
DO $$ BEGIN RAISE NOTICE '================================'; END $$;

SELECT biscuit_index_status();

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE 'Test suite completed!'; END $$;

-- ============================================================================
-- BISCUIT PATTERN MISMATCH DIAGNOSIS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'DIAGNOSING PATTERN MISMATCHES'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

-- Show tombstone status
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE 'Current tombstone count: %', biscuit_get_tombstone_count(); END $$;
DO $$ BEGIN RAISE NOTICE 'Free slots: %', biscuit_get_free_slots(); END $$;
DO $$ BEGIN RAISE NOTICE 'Active count: %', biscuit_get_active_count(); END $$;

-- Find records that BISCUIT matches but PostgreSQL doesn't for %e%
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'EXTRA RECORDS: BISCUIT found but LIKE did not'; END $$;
DO $$ BEGIN RAISE NOTICE 'Pattern: %%e%%'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

SELECT 
    pk::INTEGER as id, 
    value as seq,
    CASE WHEN value LIKE '%e%' THEN 'SHOULD MATCH' ELSE 'FALSE POSITIVE' END as analysis
FROM biscuit_match_keys('%e%')
WHERE pk::INTEGER NOT IN (
    SELECT id FROM test_sequences WHERE seq LIKE '%e%'
)
ORDER BY pk::INTEGER;

-- Check if these are deleted/tombstoned records
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE 'Checking if extra records exist in table:'; END $$;

WITH biscuit_extras AS (
    SELECT pk::INTEGER as id
    FROM biscuit_match_keys('%e%')
    WHERE pk::INTEGER NOT IN (
        SELECT id FROM test_sequences WHERE seq LIKE '%e%'
    )
)
SELECT 
    be.id,
    CASE 
        WHEN ts.id IS NULL THEN 'NOT IN TABLE (deleted?)'
        ELSE 'EXISTS: ' || ts.seq
    END as status
FROM biscuit_extras be
LEFT JOIN test_sequences ts ON ts.id = be.id
ORDER BY be.id;

-- Same analysis for %a%e%
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'Pattern: %%a%%e%%'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

SELECT 
    pk::INTEGER as id, 
    value as seq,
    CASE 
        WHEN value LIKE '%a%e%' THEN 'SHOULD MATCH' 
        ELSE 'FALSE POSITIVE'
    END as analysis
FROM biscuit_match_keys('%a%e%')
WHERE pk::INTEGER NOT IN (
    SELECT id FROM test_sequences WHERE seq LIKE '%a%e%'
)
ORDER BY pk::INTEGER;

-- Same analysis for a%e%
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'Pattern: a%%e%%'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

SELECT 
    pk::INTEGER as id, 
    value as seq,
    CASE 
        WHEN value LIKE 'a%e%' THEN 'SHOULD MATCH' 
        ELSE 'FALSE POSITIVE'
    END as analysis
FROM biscuit_match_keys('a%e%')
WHERE pk::INTEGER NOT IN (
    SELECT id FROM test_sequences WHERE seq LIKE 'a%e%'
)
ORDER BY pk::INTEGER;

-- Show ALL records to find the pattern
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'ALL RECORDS CONTAINING "e":'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

SELECT 
    id,
    seq,
    CASE WHEN seq LIKE '%e%' THEN '✓ PG' ELSE '' END as pg_match,
    CASE WHEN id IN (SELECT pk::INTEGER FROM biscuit_match_keys('%e%')) 
         THEN '✓ BISCUIT' ELSE '' END as biscuit_match,
    CASE 
        WHEN seq LIKE '%e%' AND id IN (SELECT pk::INTEGER FROM biscuit_match_keys('%e%')) THEN 'BOTH'
        WHEN seq LIKE '%e%' THEN 'PG ONLY'
        WHEN id IN (SELECT pk::INTEGER FROM biscuit_match_keys('%e%')) THEN '⚠ BISCUIT ONLY'
        ELSE ''
    END as status
FROM test_sequences
WHERE seq LIKE '%e%' OR id IN (SELECT pk::INTEGER FROM biscuit_match_keys('%e%'))
ORDER BY id;

-- Check for NULL or empty values that might be confusing things
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'CHECKING FOR EDGE CASES:'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

SELECT 
    COUNT(*) as total,
    COUNT(CASE WHEN seq = '' THEN 1 END) as empty_strings,
    COUNT(CASE WHEN seq IS NULL THEN 1 END) as nulls,
    COUNT(CASE WHEN LENGTH(seq) = 1 THEN 1 END) as single_char
FROM test_sequences;

-- Verify character 'e' specifically
SELECT 
    id, 
    seq,
    LENGTH(seq) as len,
    POSITION('e' IN seq) as e_position
FROM test_sequences  
WHERE id IN (
    SELECT pk::INTEGER FROM biscuit_match_keys('%e%')
    EXCEPT
    SELECT id FROM test_sequences WHERE seq LIKE '%e%'
)
ORDER BY id;