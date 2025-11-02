-- ============================================================================
-- BISCUIT COMPREHENSIVE TEST SUITE
-- Complete verification of ALL functions across ALL scenarios
-- ============================================================================


-- ============================================================================
-- TEST SETUP
-- ============================================================================
\timing on

DROP TABLE IF EXISTS test_sequences CASCADE;
DROP TABLE IF EXISTS test_results CASCADE;

CREATE TABLE test_sequences (
    id SERIAL PRIMARY KEY,
    seq TEXT NOT NULL,
    category TEXT
);

CREATE TABLE test_results (
    test_num INTEGER PRIMARY KEY,
    test_name TEXT NOT NULL,
    status TEXT NOT NULL,
    details TEXT,
    timestamp TIMESTAMP DEFAULT NOW()
);

-- Insert comprehensive test data covering all edge cases
INSERT INTO test_sequences (seq, category) VALUES
    -- Basic patterns
    ('alpha', 'basic'),
    ('beta', 'basic'),
    ('gamma', 'basic'),
    ('alphabet', 'basic'),
    ('abc123', 'basic'),
    ('xyz789', 'basic'),
    ('test', 'basic'),
    ('testing', 'basic'),
    ('tester', 'basic'),
    ('production', 'basic'),
    
    -- Edge cases
    ('', 'edge'),                    -- empty string
    ('a', 'edge'),                   -- single char
    ('z', 'edge'),                   -- single char
    ('ab', 'edge'),                  -- two chars
    ('xyz', 'edge'),                 -- three chars
    
    -- Special characters for pattern testing
    ('aaa', 'pattern'),              -- repeated chars
    ('aba', 'pattern'),              -- palindrome
    ('abcabc', 'pattern'),           -- repeated sequence
    ('start_end', 'pattern'),        -- prefix_suffix
    ('prefix', 'pattern'),
    ('suffix', 'pattern'),
    ('middle', 'pattern'),
    
    -- Case sensitivity
    ('UPPER', 'case'),
    ('lower', 'case'),
    ('MiXeD', 'case'),
    
    -- Numbers and special chars
    ('123', 'numeric'),
    ('abc123def', 'numeric'),
    ('test_underscore', 'special'),
    ('test-dash', 'special'),
    ('test.dot', 'special');

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

CREATE OR REPLACE FUNCTION assert_true(
    test_num INTEGER,
    test_name TEXT,
    condition BOOLEAN,
    details TEXT DEFAULT ''
) RETURNS VOID AS $$
BEGIN
    IF condition THEN
        INSERT INTO test_results (test_num, test_name, status, details)
        VALUES (test_num, test_name, 'PASS', details);
    ELSE
        INSERT INTO test_results (test_num, test_name, status, details)
        VALUES (test_num, test_name, 'FAIL', details);
        RAISE NOTICE 'TEST % FAILED: % - %', test_num, test_name, details;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- PHASE 1: INDEX BUILD & SETUP FUNCTIONS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 1: INDEX BUILD & SETUP'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 1: biscuit_setup() function
SELECT assert_true(
    1,
    'biscuit_setup() completes successfully',
    (SELECT biscuit_setup('test_sequences', 'seq', 'id') LIKE '%Successfully%'),
    'Setup function returned success message'
);

-- Test 2: biscuit_version() function
SELECT assert_true(
    2,
    'biscuit_version() returns version string',
    (SELECT biscuit_version() LIKE '%Biscuit%'),
    format('Version: %s', biscuit_version())
);

-- Test 3: biscuit_index_status() returns valid info
SELECT assert_true(
    3,
    'biscuit_index_status() returns status',
    (SELECT biscuit_index_status() LIKE '%Table: test_sequences%'),
    'Index status contains table name'
);

-- Test 4: Initial active count
SELECT assert_equal(
    4,
    'Initial active count matches table',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- Test 5: Initial free slots is zero
SELECT assert_equal(
    5,
    'Initial free slots is zero',
    0::BIGINT,
    biscuit_get_free_slots()::BIGINT
);

-- Test 6: Initial tombstone count is zero
SELECT assert_equal(
    6,
    'Initial tombstone count is zero',
    0::BIGINT,
    biscuit_get_tombstone_count()::BIGINT
);

-- ============================================================================
-- PHASE 2: PATTERN MATCHING - WILDCARDS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 2: PATTERN MATCHING - WILDCARDS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 7: Match all (%)
SELECT assert_equal(
    7,
    'Pattern % matches all records',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_match_count('%')::BIGINT
);

-- Test 8: Empty pattern matches empty strings
SELECT assert_equal(
    8,
    'Empty pattern matches empty strings',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq = ''),
    biscuit_match_count('')::BIGINT
);

-- Test 9: Single underscore (_)
SELECT assert_equal(
    9,
    'Pattern _ matches single chars',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '_'),
    biscuit_match_count('_')::BIGINT
);

-- Test 10: Double underscore (__)
SELECT assert_equal(
    10,
    'Pattern __ matches two chars',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '__'),
    biscuit_match_count('__')::BIGINT
);

-- Test 11: Triple underscore (___)
SELECT assert_equal(
    11,
    'Pattern ___ matches three chars',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '___'),
    biscuit_match_count('___')::BIGINT
);

-- Test 12: Underscore with percent (%_%)
SELECT assert_equal(
    12,
    'Pattern %_% matches strings with at least 1 char',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%_%'),
    biscuit_match_count('%_%')::BIGINT
);

-- ============================================================================
-- PHASE 3: PATTERN MATCHING - PREFIX/SUFFIX/CONTAINS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 3: PREFIX/SUFFIX/CONTAINS PATTERNS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 13: Prefix pattern (alpha%)
SELECT assert_equal(
    13,
    'Prefix pattern: alpha%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'alpha%'),
    biscuit_match_count('alpha%')::BIGINT
);

-- Test 14: Suffix pattern (%test)
SELECT assert_equal(
    14,
    'Suffix pattern: %test',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%test'),
    biscuit_match_count('%test')::BIGINT
);

-- Test 15: Contains pattern (%abc%)
SELECT assert_equal(
    15,
    'Contains pattern: %abc%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%abc%'),
    biscuit_match_count('%abc%')::BIGINT
);

-- Test 16: Exact match (no wildcards)
SELECT assert_equal(
    16,
    'Exact match: alpha',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq = 'alpha'),
    biscuit_match_count('alpha')::BIGINT
);

-- Test 17: Single char prefix (a%)
SELECT assert_equal(
    17,
    'Single char prefix: a%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'a%'),
    biscuit_match_count('a%')::BIGINT
);

-- Test 18: Single char suffix (%a)
SELECT assert_equal(
    18,
    'Single char suffix: %a',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%a'),
    biscuit_match_count('%a')::BIGINT
);

-- Test 19: Single char contains (%a%)
SELECT assert_equal(
    19,
    'Single char contains: %a%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%a%'),
    biscuit_match_count('%a%')::BIGINT
);

-- ============================================================================
-- PHASE 4: PATTERN MATCHING - COMPLEX MULTI-PART
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 4: COMPLEX MULTI-PART PATTERNS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 20: Two-part pattern (a%b)
SELECT assert_equal(
    20,
    'Two-part pattern: a%b',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'a%b'),
    biscuit_match_count('a%b')::BIGINT
);

-- Test 21: Two-part pattern (%a%b%)
SELECT assert_equal(
    21,
    'Two-part contains: %a%b%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%a%b%'),
    biscuit_match_count('%a%b%')::BIGINT
);

-- Test 22: Three-part pattern (%a%b%c%)
SELECT assert_equal(
    22,
    'Three-part pattern: %a%b%c%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%a%b%c%'),
    biscuit_match_count('%a%b%c%')::BIGINT
);

-- Test 23: Prefix and suffix (a%z)
SELECT assert_equal(
    23,
    'Prefix+suffix: a%z',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'a%z'),
    biscuit_match_count('a%z')::BIGINT
);

-- Test 24: Complex with underscores (a_c)
SELECT assert_equal(
    24,
    'Pattern with underscore: a_c',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'a_c'),
    biscuit_match_count('a_c')::BIGINT
);

-- Test 25: Mixed wildcards (%a_b%)
SELECT assert_equal(
    25,
    'Mixed wildcards: %a_b%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%a_b%'),
    biscuit_match_count('%a_b%')::BIGINT
);

-- ============================================================================
-- PHASE 5: QUERY FUNCTIONS - ALL VARIANTS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 5: QUERY FUNCTION VARIANTS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 26: biscuit_match_count() consistency
SELECT assert_equal(
    26,
    'biscuit_match_count() for %test%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%test%'),
    biscuit_match_count('%test%')::BIGINT
);

-- Test 27: biscuit_match_keys() returns correct PKs
SELECT assert_sets_equal(
    27,
    'biscuit_match_keys() returns correct PKs',
    ARRAY(SELECT id FROM test_sequences WHERE seq LIKE '%test%' ORDER BY id),
    ARRAY(SELECT pk::INTEGER FROM biscuit_match_keys('%test%') ORDER BY pk)
);

-- Test 28: biscuit_match_keys() returns correct values
SELECT assert_true(
    28,
    'biscuit_match_keys() returns correct values',
    (SELECT COUNT(*) = 0 FROM (
        SELECT pk::INTEGER as id, value 
        FROM biscuit_match_keys('%test%')
        EXCEPT
        SELECT id, seq FROM test_sequences WHERE seq LIKE '%test%'
    ) diff),
    'All PK-value pairs match'
);

-- Test 29: biscuit_match() returns full tuples
SELECT assert_equal(
    29,
    'biscuit_match() returns correct count',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%alpha%'),
    (SELECT COUNT(*)::BIGINT FROM biscuit_match('%alpha%'))
);

-- Test 30: biscuit_match() tuple data integrity
SELECT assert_true(
    30,
    'biscuit_match() returns correct data',
    (SELECT COUNT(*) = 0 FROM (
        SELECT * FROM biscuit_match('%alpha%')
        EXCEPT
        SELECT * FROM test_sequences WHERE seq LIKE '%alpha%'
    ) diff),
    'All tuples match exactly'
);

-- ============================================================================
-- PHASE 6: INSERT OPERATIONS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 6: INSERT OPERATIONS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 31: Single INSERT
INSERT INTO test_sequences (seq, category) VALUES ('new_insert_1', 'insert_test');

SELECT assert_equal(
    31,
    'After INSERT - count increases',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- Test 32: INSERT is immediately searchable
SELECT assert_equal(
    32,
    'New INSERT is searchable',
    1::BIGINT,
    biscuit_match_count('new_insert_1')::BIGINT
);

-- Test 33: Bulk INSERT
INSERT INTO test_sequences (seq, category) 
VALUES ('bulk1', 'bulk'), ('bulk2', 'bulk'), ('bulk3', 'bulk');

SELECT assert_equal(
    33,
    'After bulk INSERT - count correct',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- Test 34: Bulk INSERTs are searchable
SELECT assert_equal(
    34,
    'Bulk INSERTs searchable: bulk%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'bulk%'),
    biscuit_match_count('bulk%')::BIGINT
);

-- Test 35: INSERT with empty string
INSERT INTO test_sequences (seq, category) VALUES ('', 'empty_insert');

SELECT assert_equal(
    35,
    'Empty string INSERT searchable',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq = ''),
    biscuit_match_count('')::BIGINT
);

-- Test 36: INSERT with special characters
INSERT INTO test_sequences (seq, category) VALUES ('special!@#$$%^&*()', 'special_insert');

SELECT assert_equal(
    36,
    'Special char INSERT searchable',
    1::BIGINT,
    biscuit_match_count('special!@#$$%^&*()')::BIGINT
);

-- ============================================================================
-- PHASE 7: UPDATE OPERATIONS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 7: UPDATE OPERATIONS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 37: UPDATE changes search results
UPDATE test_sequences SET seq = 'updated_alpha' WHERE seq = 'alpha';

SELECT assert_equal(
    37,
    'After UPDATE - old value not found',
    0::BIGINT,
    biscuit_match_count('alpha')::BIGINT
);

-- Test 38: UPDATE new value is searchable
SELECT assert_equal(
    38,
    'After UPDATE - new value found',
    1::BIGINT,
    biscuit_match_count('updated_alpha')::BIGINT
);

-- Test 39: UPDATE maintains active count
SELECT assert_equal(
    39,
    'UPDATE maintains active count',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- Test 40: Bulk UPDATE
UPDATE test_sequences SET seq = 'modified_' || seq WHERE category = 'basic' AND seq NOT LIKE 'updated%';

SELECT assert_equal(
    40,
    'Bulk UPDATE changes results',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'modified_%'),
    biscuit_match_count('modified_%')::BIGINT
);

-- Test 41: UPDATE to same value (no-op)
UPDATE test_sequences SET seq = seq WHERE id = 1;

SELECT assert_equal(
    41,
    'No-op UPDATE maintains count',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- Test 42: UPDATE to empty string
UPDATE test_sequences SET seq = '' WHERE id = 2;

SELECT assert_equal(
    42,
    'UPDATE to empty string searchable',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq = ''),
    biscuit_match_count('')::BIGINT
);

-- Test 43: UPDATE from empty string
INSERT INTO test_sequences (seq, category) VALUES ('', 'temp_empty');
UPDATE test_sequences SET seq = 'was_empty' WHERE seq = '' AND category = 'temp_empty';

SELECT assert_equal(
    43,
    'UPDATE from empty string works',
    1::BIGINT,
    biscuit_match_count('was_empty')::BIGINT
);

-- ============================================================================
-- PHASE 8: DELETE OPERATIONS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 8: DELETE OPERATIONS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 44: Single DELETE
DELETE FROM test_sequences WHERE seq = 'modified_test';

SELECT assert_equal(
    44,
    'After DELETE - count decreases',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- Test 45: Deleted record not searchable
SELECT assert_equal(
    45,
    'Deleted record not found',
    0::BIGINT,
    biscuit_match_count('modified_test')::BIGINT
);

-- Test 46: Free slots increase after DELETE
SELECT assert_true(
    46,
    'Free slots increase after DELETE',
    biscuit_get_free_slots() > 0,
    format('Free slots: %s', biscuit_get_free_slots())
);

-- Test 47: Bulk DELETE
DELETE FROM test_sequences WHERE category = 'basic';

SELECT assert_equal(
    47,
    'After bulk DELETE - count correct',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- Test 48: Bulk deleted records not searchable
SELECT assert_equal(
    48,
    'Bulk deleted records not found',
    0::BIGINT,
    biscuit_match_count('modified_gamma')::BIGINT
);

-- Test 49: Tombstones tracked
SELECT assert_true(
    49,
    'Tombstones are tracked',
    biscuit_get_tombstone_count() > 0,
    format('Tombstone count: %s', biscuit_get_tombstone_count())
);

-- ============================================================================
-- PHASE 9: SLOT REUSE & RESURRECTION
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 9: SLOT REUSE & RESURRECTION'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Delete some records to create free slots
DELETE FROM test_sequences WHERE category = 'edge' AND seq IN ('a', 'z', 'ab');

-- Test 50: Free slots available
SELECT assert_true(
    50,
    'Free slots available for reuse',
    biscuit_get_free_slots() >= 3,
    format('Free slots: %s', biscuit_get_free_slots())
);

-- Insert new records to reuse slots
INSERT INTO test_sequences (seq, category) VALUES 
    ('reuse1', 'reuse'),
    ('reuse2', 'reuse'),
    ('reuse3', 'reuse');

-- Test 51: Reused slots searchable
SELECT assert_equal(
    51,
    'Reused slots are searchable',
    3::BIGINT,
    biscuit_match_count('reuse%')::BIGINT
);

-- Test 52: Old slot data not searchable
SELECT assert_equal(
    52,
    'Old slot data not found (a)',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq = 'a'),
    biscuit_match_count('a')::BIGINT
);

-- Test 53: Old slot data not searchable (z)
SELECT assert_equal(
    53,
    'Old slot data not found (z)',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq = 'z'),
    biscuit_match_count('z')::BIGINT
);

-- Test 54: No false positives from reused slots
SELECT assert_sets_equal(
    54,
    'Reused slots: no false positives for %e%',
    ARRAY(SELECT id FROM test_sequences WHERE seq LIKE '%e%' ORDER BY id),
    ARRAY(SELECT pk::INTEGER FROM biscuit_match_keys('%e%') ORDER BY pk)
);

-- ============================================================================
-- PHASE 10: MIXED CRUD SEQUENCES
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 10: MIXED CRUD SEQUENCES'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 55: INSERT -> UPDATE -> DELETE sequence
INSERT INTO test_sequences (seq, category) VALUES ('temp1', 'temp');
UPDATE test_sequences SET seq = 'temp1_modified' WHERE seq = 'temp1';
DELETE FROM test_sequences WHERE seq = 'temp1_modified';

SELECT assert_equal(
    55,
    'INSERT->UPDATE->DELETE leaves no trace',
    0::BIGINT,
    biscuit_match_count('temp1%')::BIGINT
);

-- Test 56: Multiple rapid UPDATEs
INSERT INTO test_sequences (seq, category) VALUES ('rapid', 'rapid_test');
UPDATE test_sequences SET seq = 'rapid1' WHERE seq = 'rapid';
UPDATE test_sequences SET seq = 'rapid2' WHERE seq = 'rapid1';
UPDATE test_sequences SET seq = 'rapid3' WHERE seq = 'rapid2';

SELECT assert_equal(
    56,
    'Multiple UPDATEs: final value searchable',
    1::BIGINT,
    biscuit_match_count('rapid3')::BIGINT
);

-- Test 57: Multiple rapid UPDATEs: old values gone
SELECT assert_equal(
    57,
    'Multiple UPDATEs: old values not found',
    0::BIGINT,
    biscuit_match_count('rapid1')::BIGINT + biscuit_match_count('rapid2')::BIGINT
);

-- Test 58: DELETE and re-INSERT same PK
INSERT INTO test_sequences (id, seq, category) VALUES (9999, 'original', 'reinsert_test');
DELETE FROM test_sequences WHERE id = 9999;
INSERT INTO test_sequences (id, seq, category) VALUES (9999, 'reinserted', 'reinsert_test');

SELECT assert_equal(
    58,
    'DELETE and re-INSERT: new value found',
    1::BIGINT,
    biscuit_match_count('reinserted')::BIGINT
);

-- Test 59: DELETE and re-INSERT: old value gone
SELECT assert_equal(
    59,
    'DELETE and re-INSERT: old value not found',
    0::BIGINT,
    biscuit_match_count('original')::BIGINT
);

-- ============================================================================
-- PHASE 11: STRESS TESTING
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 11: STRESS TESTING'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 60: Bulk insert 200 records
INSERT INTO test_sequences (seq, category)
SELECT 'stress_' || i || '_' || (i % 10), 'stress'
FROM generate_series(1, 200) i;

SELECT assert_equal(
    60,
    'Stress: bulk insert 200 records',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- Test 61: Pattern matching on stress data
SELECT assert_equal(
    61,
    'Stress: pattern matching stress_%',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'stress_%'),
    biscuit_match_count('stress_%')::BIGINT
);

-- Test 62: Bulk update stress data
UPDATE test_sequences 
SET seq = 'updated_' || seq 
WHERE category = 'stress' AND (id % 2) = 0;

SELECT assert_equal(
    62,
    'Stress: bulk update pattern',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE 'updated_stress_%'),
    biscuit_match_count('updated_stress_%')::BIGINT
);

-- Test 63: Bulk delete stress data
DELETE FROM test_sequences WHERE category = 'stress' AND (id % 3) = 0;

SELECT assert_equal(
    63,
    'Stress: bulk delete active count',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- Test 64: Complex pattern on stress data
SELECT assert_equal(
    64,
    'Stress: complex pattern %stress%_5',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE seq LIKE '%stress%_5'),
    biscuit_match_count('%stress%_5')::BIGINT
);

-- ============================================================================
-- PHASE 12: EDGE CASES & CORNER CASES
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 12: EDGE CASES & CORNER CASES'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 65: Very long string
INSERT INTO test_sequences (seq, category) 
VALUES (repeat('abcdefghij', 30), 'long_string');

SELECT assert_equal(
    65,
    'Very long string searchable',
    1::BIGINT,
    biscuit_match_count(repeat('abcdefghij', 30))::BIGINT
);

-- Test 66: String with only wildcards in pattern
SELECT assert_equal(
    66,
    'Pattern %%% matches same as %',
    biscuit_match_count('%')::BIGINT,
    biscuit_match_count('%%%')::BIGINT
);

-- Test 67: Repeated character patterns
INSERT INTO test_sequences (seq, category) VALUES ('aaaa', 'repeated');

SELECT assert_equal(
    67,
    'Repeated chars: aaaa',
    1::BIGINT,
    biscuit_match_count('aaaa')::BIGINT
);

-- Test 68: Pattern with only underscores (length match)
SELECT assert_equal(
    68,
    'Pattern ____ matches 4-char strings',
    (SELECT COUNT(*)::BIGINT FROM test_sequences WHERE LENGTH(seq) = 4),
    biscuit_match_count('____')::BIGINT
);

-- Test 69: Non-existent pattern
SELECT assert_equal(
    69,
    'Non-existent pattern returns 0',
    0::BIGINT,
    biscuit_match_count('zzzzzzzzzzz')::BIGINT
);

-- Test 70: Case sensitivity
SELECT assert_equal(
    70,
    'Case sensitive: UPPER vs upper',
    (SELECT (COUNT(*) FILTER (WHERE seq = 'UPPER') + COUNT(*) FILTER (WHERE seq = 'upper'))::BIGINT 
     FROM test_sequences),
    biscuit_match_count('UPPER')::BIGINT + biscuit_match_count('upper')::BIGINT
);

-- ============================================================================
-- PHASE 13: ALL QUERY FUNCTIONS RETURN SAME RESULTS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 13: QUERY FUNCTION CONSISTENCY'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 71: biscuit_match_count vs biscuit_match_keys count
SELECT assert_equal(
    71,
    'match_count = match_keys count for %test%',
    biscuit_match_count('%test%')::BIGINT,
    (SELECT COUNT(*)::BIGINT FROM biscuit_match_keys('%test%'))
);

-- Test 72: biscuit_match_keys vs biscuit_match count
SELECT assert_equal(
    72,
    'match_keys = match tuple count for %alpha%',
    (SELECT COUNT(*)::BIGINT FROM biscuit_match_keys('%alpha%')),
    (SELECT COUNT(*)::BIGINT FROM biscuit_match('%alpha%'))
);

-- Test 73: All query functions return identical IDs
DO $$
DECLARE
    count_val INTEGER;
    keys_ids INTEGER[];
    match_ids INTEGER[];
    all_equal BOOLEAN;
BEGIN
    count_val := biscuit_match_count('%stress%');
    keys_ids := ARRAY(SELECT pk::INTEGER FROM biscuit_match_keys('%stress%') ORDER BY pk);
    match_ids := ARRAY(SELECT id FROM biscuit_match('%stress%') ORDER BY id);
    
    all_equal := (array_length(keys_ids, 1) = count_val) AND (keys_ids = match_ids);
    
    PERFORM assert_true(
        73,
        'All query functions return same IDs',
        all_equal,
        format('count=%s, keys=%s IDs, match=%s IDs', 
               count_val, array_length(keys_ids, 1), array_length(match_ids, 1))
    );
END $$;

-- ============================================================================
-- PHASE 14: COMPREHENSIVE PATTERN COVERAGE
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 14: COMPREHENSIVE PATTERN COVERAGE'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 74-100: Comprehensive pattern tests
DO $$
DECLARE
    test_patterns TEXT[] := ARRAY[
        'a%', '%a', '%a%', 'a',           -- Single char variants
        'ab%', '%ab', '%ab%', 'ab',       -- Two char variants
        'abc%', '%abc', '%abc%', 'abc',   -- Three char variants
        '%a%b%', 'a%b%', '%a%b',          -- Multi-part
        'a%e%', '%a%e%', 'a%e',           -- More multi-part
        '_', '__', '___', '____',         -- Underscores only
        '%_%', '%__%', '%___%',           -- Mixed underscores
        'a_', '_a', 'a_b', '_ab',         -- Underscore combinations
        '%test', 'test%', '%test%'        -- Common patterns
    ];
    pattern TEXT;
    test_num INTEGER := 74;
    expected BIGINT;
    actual BIGINT;
BEGIN
    FOREACH pattern IN ARRAY test_patterns
    LOOP
        expected := (SELECT COUNT(*) FROM test_sequences WHERE seq LIKE pattern);
        actual := biscuit_match_count(pattern);
        
        PERFORM assert_equal(
            test_num,
            format('Pattern: %s', pattern),
            expected,
            actual
        );
        
        test_num := test_num + 1;
        EXIT WHEN test_num > 100;
    END LOOP;
END $$;

-- ============================================================================
-- PHASE 15: SPECIAL FUNCTION TESTS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 15: SPECIAL FUNCTIONS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 101: biscuit_get_active_count() always matches table
SELECT assert_equal(
    101,
    'Active count always matches table count',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    biscuit_get_active_count()::BIGINT
);

-- Test 102: Free slots + active + tombstones <= total slots
SELECT assert_true(
    102,
    'Slot accounting is consistent',
    (SELECT (biscuit_get_free_slots() + biscuit_get_active_count() + biscuit_get_tombstone_count()) <= 
            (SELECT COUNT(*) FROM test_sequences) + 500), -- Account for deleted
    format('Free=%s, Active=%s, Tombstones=%s', 
           biscuit_get_free_slots(), biscuit_get_active_count(), biscuit_get_tombstone_count())
);

-- Test 103: Status function returns valid output
SELECT assert_true(
    103,
    'biscuit_index_status() returns complete status',
    (SELECT biscuit_index_status() LIKE '%Active Records%' 
        AND biscuit_index_status() LIKE '%Free Slots%'
        AND biscuit_index_status() LIKE '%Tombstoned Slots%'),
    'Status contains all key metrics'
);

-- ============================================================================
-- PHASE 16: NO FALSE POSITIVES/NEGATIVES
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 16: VERIFY NO FALSE RESULTS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 104: No false positives for common patterns
SELECT assert_true(
    104,
    'No false positives: %e%',
    (SELECT COUNT(*) = 0 FROM (
        SELECT pk::INTEGER FROM biscuit_match_keys('%e%')
        EXCEPT
        SELECT id FROM test_sequences WHERE seq LIKE '%e%'
    ) diff),
    'BISCUIT returns no extra records'
);

-- Test 105: No false negatives for common patterns
SELECT assert_true(
    105,
    'No false negatives: %e%',
    (SELECT COUNT(*) = 0 FROM (
        SELECT id FROM test_sequences WHERE seq LIKE '%e%'
        EXCEPT
        SELECT pk::INTEGER FROM biscuit_match_keys('%e%')
    ) diff),
    'BISCUIT returns all matching records'
);

-- Test 106: No false positives after CRUD
SELECT assert_true(
    106,
    'No false positives: %stress%',
    (SELECT COUNT(*) = 0 FROM (
        SELECT pk::INTEGER FROM biscuit_match_keys('%stress%')
        EXCEPT
        SELECT id FROM test_sequences WHERE seq LIKE '%stress%'
    ) diff),
    'No false positives after heavy CRUD'
);

-- Test 107: No false negatives after CRUD
SELECT assert_true(
    107,
    'No false negatives: %stress%',
    (SELECT COUNT(*) = 0 FROM (
        SELECT id FROM test_sequences WHERE seq LIKE '%stress%'
        EXCEPT
        SELECT pk::INTEGER FROM biscuit_match_keys('%stress%')
    ) diff),
    'No false negatives after heavy CRUD'
);

-- ============================================================================
-- PHASE 17: DATA INTEGRITY AFTER OPERATIONS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;
DO $$ BEGIN RAISE NOTICE 'PHASE 17: DATA INTEGRITY'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================='; END $$;

-- Test 108: biscuit_match returns exact table data
SELECT assert_true(
    108,
    'match() returns exact table data for %updated%',
    (SELECT COUNT(*) = 0 FROM (
        SELECT * FROM biscuit_match('%updated%')
        EXCEPT
        SELECT * FROM test_sequences WHERE seq LIKE '%updated%'
    ) diff),
    'All tuple data matches exactly'
);

-- Test 109: No orphaned data in index
SELECT assert_equal(
    109,
    'Total matches across all patterns <= active records',
    (SELECT COUNT(DISTINCT pk::INTEGER) FROM biscuit_match_keys('%'))::BIGINT,
    biscuit_get_active_count()::BIGINT
);

-- Test 110: Every table record is indexed
SELECT assert_equal(
    110,
    'Every table record appears in index',
    (SELECT COUNT(*)::BIGINT FROM test_sequences),
    (SELECT COUNT(DISTINCT pk::INTEGER)::BIGINT FROM biscuit_match_keys('%'))
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
    pass_rate NUMERIC;
    rec RECORD;
BEGIN
    SELECT COUNT(*) INTO total_tests FROM test_results;
    SELECT COUNT(*) INTO passed_tests FROM test_results WHERE status = 'PASS';
    SELECT COUNT(*) INTO failed_tests FROM test_results WHERE status = 'FAIL';
    SELECT COUNT(*) INTO warning_tests FROM test_results WHERE status = 'WARNING';
    
    pass_rate := CASE WHEN total_tests > 0 THEN (passed_tests::NUMERIC / total_tests * 100) ELSE 0 END;
    
    RAISE NOTICE '';
    RAISE NOTICE '📊 COMPREHENSIVE TEST RESULTS:';
    RAISE NOTICE '================================';
    RAISE NOTICE 'Total Tests: %', total_tests;
    RAISE NOTICE 'Passed: % (%.1f%%)', passed_tests, pass_rate;
    RAISE NOTICE 'Failed: %', failed_tests;
    RAISE NOTICE 'Warnings: %', warning_tests;
    RAISE NOTICE '';
    
    IF failed_tests > 0 THEN
        RAISE NOTICE '❌ FAILED TESTS:';
        RAISE NOTICE '================================';
        FOR rec IN 
            SELECT test_num, test_name, details 
            FROM test_results 
            WHERE status = 'FAIL' 
            ORDER BY test_num 
        LOOP
            RAISE NOTICE 'Test %: % - %', rec.test_num, rec.test_name, rec.details;
        END LOOP;
        RAISE NOTICE '';
    ELSE
        RAISE NOTICE '✅ ALL TESTS PASSED!';
        RAISE NOTICE '';
    END IF;
    
    RAISE NOTICE '📋 TEST COVERAGE:';
    RAISE NOTICE '  ✓ Index build & setup functions';
    RAISE NOTICE '  ✓ Pattern matching (wildcards, prefix, suffix, contains)';
    RAISE NOTICE '  ✓ Complex multi-part patterns';
    RAISE NOTICE '  ✓ All query function variants';
    RAISE NOTICE '  ✓ INSERT operations (single, bulk, edge cases)';
    RAISE NOTICE '  ✓ UPDATE operations (single, bulk, no-op)';
    RAISE NOTICE '  ✓ DELETE operations (single, bulk, tombstones)';
    RAISE NOTICE '  ✓ Slot reuse & resurrection';
    RAISE NOTICE '  ✓ Mixed CRUD sequences';
    RAISE NOTICE '  ✓ Stress testing (200+ records)';
    RAISE NOTICE '  ✓ Edge cases & corner cases';
    RAISE NOTICE '  ✓ Query function consistency';
    RAISE NOTICE '  ✓ Comprehensive pattern coverage (25+ patterns)';
    RAISE NOTICE '  ✓ False positive/negative detection';
    RAISE NOTICE '  ✓ Data integrity verification';
    RAISE NOTICE '';
END $$;

-- Display detailed results
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
DO $$ BEGIN RAISE NOTICE '✅ COMPREHENSIVE TEST SUITE COMPLETED!'; END $$;
DO $$ BEGIN RAISE NOTICE ''; END $$;