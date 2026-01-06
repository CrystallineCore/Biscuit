-- ============================================================================
-- BISCUIT INDEX VERIFICATION BENCHMARK - FOSDEM 2025 EDITION (FIXED)
-- True verification with proper warmup and accurate timing
-- ============================================================================

-- Clean slate
DROP TABLE IF EXISTS benchmark_single CASCADE;
DROP TABLE IF EXISTS benchmark_multi CASCADE;
DROP TABLE IF EXISTS verification_results CASCADE;

-- Verification results table
CREATE TABLE verification_results (
    test_number INT PRIMARY KEY,
    test_category TEXT,
    test_name TEXT,
    test_query TEXT,
    index_type TEXT,
    biscuit_count INT,
    biscuit_time_ms NUMERIC(10,3),
    actual_count INT,
    actual_time_ms NUMERIC(10,3),
    counts_match BOOLEAN,
    verification_status TEXT,
    speedup NUMERIC(10,2),
    notes TEXT
);

DO $$ 
BEGIN 
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '     BISCUIT INDEX VERIFICATION BENCHMARK - FOSDEM 2025';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;

-- ============================================================================
-- SECTION 1: TEST DATA GENERATION
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '📊 SECTION 1: Generating Test Data...'; END $$;

-- Single-column table
CREATE TABLE benchmark_single (
    id SERIAL PRIMARY KEY,
    email TEXT,
    product_code TEXT,
    description TEXT
);

-- Multi-column table
CREATE TABLE benchmark_multi (
    id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    company TEXT,
    city TEXT
);

-- Insert diverse test data for single-column
INSERT INTO benchmark_single (email, product_code, description)
SELECT 
    'user' || g || '@' || 
    CASE (g % 5)
        WHEN 0 THEN 'gmail.com'
        WHEN 1 THEN 'yahoo.com'
        WHEN 2 THEN 'hotmail.com'
        WHEN 3 THEN 'company.org'
        ELSE 'test.net'
    END,
    'PROD-' || LPAD(g::TEXT, 5, '0') || '-' ||
    CASE (g % 3)
        WHEN 0 THEN 'ALPHA'
        WHEN 1 THEN 'BETA'
        ELSE 'GAMMA'
    END,
    CASE (g % 10)
        WHEN 0 THEN 'Wireless Mouse with Bluetooth'
        WHEN 1 THEN 'Mechanical Keyboard RGB'
        WHEN 2 THEN 'USB-C Cable 2 Meter'
        WHEN 3 THEN 'External Hard Drive 1TB'
        WHEN 4 THEN 'Laptop Cooling Pad'
        WHEN 5 THEN 'Webcam HD 1080p'
        WHEN 6 THEN 'Portable SSD 500GB'
        WHEN 7 THEN 'Gaming Headset Surround'
        WHEN 8 THEN 'Monitor Stand Adjustable'
        ELSE 'Phone Charger Fast Charging'
    END
FROM generate_series(1, 50000) g;

-- Insert diverse test data for multi-column
INSERT INTO benchmark_multi (first_name, last_name, company, city)
SELECT 
    CASE (g % 8)
        WHEN 0 THEN 'John'
        WHEN 1 THEN 'Jane'
        WHEN 2 THEN 'Michael'
        WHEN 3 THEN 'Sarah'
        WHEN 4 THEN 'David'
        WHEN 5 THEN 'Emily'
        WHEN 6 THEN 'Robert'
        ELSE 'Jennifer'
    END,
    CASE (g % 10)
        WHEN 0 THEN 'Smith'
        WHEN 1 THEN 'Johnson'
        WHEN 2 THEN 'Williams'
        WHEN 3 THEN 'Brown'
        WHEN 4 THEN 'Jones'
        WHEN 5 THEN 'Garcia'
        WHEN 6 THEN 'Miller'
        WHEN 7 THEN 'Davis'
        WHEN 8 THEN 'Rodriguez'
        ELSE 'Martinez'
    END,
    'Company-' || (g % 20)::TEXT || '-' ||
    CASE (g % 4)
        WHEN 0 THEN 'Tech'
        WHEN 1 THEN 'Solutions'
        WHEN 2 THEN 'Industries'
        ELSE 'Enterprises'
    END,
    CASE (g % 15)
        WHEN 0 THEN 'New York'
        WHEN 1 THEN 'Los Angeles'
        WHEN 2 THEN 'Chicago'
        WHEN 3 THEN 'Houston'
        WHEN 4 THEN 'Phoenix'
        WHEN 5 THEN 'Philadelphia'
        WHEN 6 THEN 'San Antonio'
        WHEN 7 THEN 'San Diego'
        WHEN 8 THEN 'Dallas'
        WHEN 9 THEN 'San Jose'
        WHEN 10 THEN 'Austin'
        WHEN 11 THEN 'Jacksonville'
        WHEN 12 THEN 'Fort Worth'
        WHEN 13 THEN 'Columbus'
        ELSE 'Charlotte'
    END
FROM generate_series(1, 50000) g;

DO $$ 
BEGIN 
    RAISE NOTICE '✓ Generated 50,000 rows for single-column tests';
    RAISE NOTICE '✓ Generated 50,000 rows for multi-column tests';
    RAISE NOTICE '';
END $$;

-- ============================================================================
-- PHASE 1: CREATE BISCUIT INDEXES AND RUN TESTS
-- ============================================================================

DO $$ 
BEGIN 
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '🔧 PHASE 1: Testing with Biscuit Indexes';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;

DO $$ BEGIN RAISE NOTICE '🔧 Creating Biscuit Indexes...'; END $$;

CREATE INDEX idx_single_email ON benchmark_single USING biscuit(email);
CREATE INDEX idx_single_product ON benchmark_single USING biscuit(product_code);
CREATE INDEX idx_single_desc ON benchmark_single USING biscuit(description);
CREATE INDEX idx_multi_composite ON benchmark_multi USING biscuit(first_name, last_name, company, city);

DO $$ BEGIN RAISE NOTICE '✓ All Biscuit indexes created'; RAISE NOTICE ''; END $$;

-- ============================================================================
-- WARMUP QUERIES TO LOAD INDEXES INTO rd_amcache
-- ============================================================================

DO $$ 
BEGIN 
    RAISE NOTICE '🔥 Warming up indexes (loading into rd_amcache)...';
END $$;

DO $$
DECLARE
    dummy INT;
BEGIN
    SELECT COUNT(*) INTO dummy FROM benchmark_single WHERE email LIKE 'warmup%';
    SELECT COUNT(*) INTO dummy FROM benchmark_single WHERE product_code LIKE 'warmup%';
    SELECT COUNT(*) INTO dummy FROM benchmark_single WHERE description LIKE 'warmup%';
    SELECT COUNT(*) INTO dummy FROM benchmark_multi 
    WHERE first_name LIKE 'warmup%' AND last_name LIKE 'warmup%';
    
    RAISE NOTICE '✓ All indexes loaded into memory (rd_amcache populated)';
    RAISE NOTICE '';
END $$;

-- ============================================================================
-- BISCUIT TESTS - SECTION 2: Single-Column LIKE Tests
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '📋 Running LIKE Tests with Biscuit...'; END $$;

-- Test 1: PREFIX pattern
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE 'user1%';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (1, 'LIKE', 'PREFIX: user1%', 'email LIKE ''user1%''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 1 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 2: SUFFIX pattern
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@gmail.com';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (2, 'LIKE', 'SUFFIX: %@gmail.com', 'email LIKE ''%@gmail.com''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 2 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 3: SUBSTRING pattern
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE product_code LIKE '%ALPHA%';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (3, 'LIKE', 'SUBSTRING: %ALPHA%', 'product_code LIKE ''%ALPHA%''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 3 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 4: EXACT match
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE description LIKE 'Wireless Mouse with Bluetooth';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (4, 'LIKE', 'EXACT: Wireless Mouse with Bluetooth', 'description LIKE ''Wireless Mouse with Bluetooth''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 4 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 5: Complex multi-part pattern
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE product_code LIKE 'PROD-0%5-BETA';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (5, 'LIKE', 'COMPLEX: PROD-0%5-BETA', 'product_code LIKE ''PROD-0%5-BETA''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 5 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- ============================================================================
-- BISCUIT TESTS - SECTION 3: Single-Column ILIKE Tests
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '📋 Running ILIKE Tests with Biscuit...'; END $$;

-- Test 6: ILIKE PREFIX
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email ILIKE 'USER1%';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (6, 'ILIKE', 'PREFIX: USER1%', 'email ILIKE ''USER1%''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 6 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 7: ILIKE SUFFIX
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email ILIKE '%@GMAIL.COM';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (7, 'ILIKE', 'SUFFIX: %@GMAIL.COM', 'email ILIKE ''%@GMAIL.COM''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 7 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 8: ILIKE SUBSTRING
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE description ILIKE '%MOUSE%';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (8, 'ILIKE', 'SUBSTRING: %MOUSE%', 'description ILIKE ''%MOUSE%''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 8 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- ============================================================================
-- BISCUIT TESTS - SECTION 4: Multi-Column Tests
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '📋 Running Multi-Column Tests with Biscuit...'; END $$;

-- Test 9: Two-column LIKE
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_multi WHERE first_name LIKE 'John%' AND last_name LIKE 'Smith%';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (9, 'MULTI-LIKE', 'Two-column: John% AND Smith%', 'first_name LIKE ''John%'' AND last_name LIKE ''Smith%''', 'Multi-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 9 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 10: Three-column LIKE
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_multi WHERE first_name LIKE 'J%' AND last_name LIKE '%son' AND company LIKE '%Tech%';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (10, 'MULTI-LIKE', 'Three-column: J% AND %son AND %Tech%', 'first_name LIKE ''J%'' AND last_name LIKE ''%son'' AND company LIKE ''%Tech%''', 'Multi-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 10 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 11: Two-column ILIKE
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_multi WHERE first_name ILIKE 'JOHN%' AND last_name ILIKE 'SMITH%';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (11, 'MULTI-ILIKE', 'Two-column: JOHN% AND SMITH%', 'first_name ILIKE ''JOHN%'' AND last_name ILIKE ''SMITH%''', 'Multi-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 11 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- ============================================================================
-- BISCUIT TESTS - SECTION 5: AND/OR Tests
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '📋 Running AND/OR Tests with Biscuit...'; END $$;

-- Test 12: Simple AND
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@gmail.com' AND description LIKE '%Mouse%';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (12, 'AND', 'Two predicates: %@gmail.com AND %Mouse%', 'email LIKE ''%@gmail.com'' AND description LIKE ''%Mouse%''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 12 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 13: Simple OR
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@gmail.com' OR email LIKE '%@yahoo.com';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (13, 'OR', 'Two predicates: %@gmail.com OR %@yahoo.com', 'email LIKE ''%@gmail.com'' OR email LIKE ''%@yahoo.com''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 13 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 14: OR with ILIKE
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE description ILIKE '%MOUSE%' OR description ILIKE '%KEYBOARD%';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (14, 'OR', 'ILIKE: %MOUSE% OR %KEYBOARD%', 'description ILIKE ''%MOUSE%'' OR description ILIKE ''%KEYBOARD%''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 14 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 15: Combined (AND) OR (AND)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single 
    WHERE (email LIKE '%@gmail.com' AND product_code LIKE '%ALPHA%')
       OR (email LIKE '%@yahoo.com' AND product_code LIKE '%BETA%');
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (15, 'AND/OR', '(gmail AND ALPHA) OR (yahoo AND BETA)', '(email LIKE ''%@gmail.com'' AND product_code LIKE ''%ALPHA%'') OR (email LIKE ''%@yahoo.com'' AND product_code LIKE ''%BETA%'')', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 15 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- ============================================================================
-- BISCUIT TESTS - SECTION 6: Edge Cases
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '📋 Running Edge Case Tests with Biscuit...'; END $$;

-- Test 16: Empty result
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@nonexistent.xyz';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (16, 'EDGE', 'Empty result set', 'email LIKE ''%@nonexistent.xyz''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 16 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 17: Match all (single %)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (17, 'EDGE', 'Match all (single %)', 'email LIKE ''%''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 17 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 18: Empty pattern
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '';
    end_time := clock_timestamp();
    
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (18, 'EDGE', 'Empty pattern', 'email LIKE ''''', 'Single-column', row_count, elapsed_ms);
    
    RAISE NOTICE 'Test 18 - Biscuit: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

DO $$ BEGIN RAISE NOTICE ''; RAISE NOTICE '✓ Phase 1 Complete - All Biscuit tests recorded'; RAISE NOTICE ''; END $$;

-- ============================================================================
-- PHASE 2: DROP BISCUIT INDEXES AND RUN ACTUAL TESTS
-- ============================================================================

-- Key issue: Phase 2 was using INSERT instead of UPDATE for tests 2-18
-- Solution: Changed all Phase 2 test blocks to use UPDATE instead of INSERT

-- The problematic pattern was:
-- INSERT INTO verification_results (test_number, ...) VALUES (2, ...)

-- Fixed pattern now is:
-- UPDATE verification_results SET actual_count = ..., actual_time_ms = ... WHERE test_number = 2;

-- Here's the complete fixed PHASE 2 section with all 18 tests using UPDATE:

-- ============================================================================
-- PHASE 2: DROP BISCUIT INDEXES AND RUN ACTUAL TESTS (FIXED)
-- ============================================================================

DO $$ 
BEGIN 
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '🔥 PHASE 2: Testing WITHOUT Biscuit (Ground Truth)';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;

DO $$ BEGIN RAISE NOTICE '🔥 Dropping Biscuit Indexes...'; END $$;

DROP INDEX IF EXISTS idx_single_email;
DROP INDEX IF EXISTS idx_single_product;
DROP INDEX IF EXISTS idx_single_desc;
DROP INDEX IF EXISTS idx_multi_composite;

DO $$ BEGIN RAISE NOTICE '✓ All Biscuit indexes dropped'; RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '📋 Running ACTUAL Tests (No Biscuit)...'; END $$;

-- Test 1
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE 'user1%';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 1;
    RAISE NOTICE 'Test 1 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 2
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@gmail.com';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 2;
    RAISE NOTICE 'Test 2 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 3
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE product_code LIKE '%ALPHA%';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 3;
    RAISE NOTICE 'Test 3 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 4
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE description LIKE 'Wireless Mouse with Bluetooth';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 4;
    RAISE NOTICE 'Test 4 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 5
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE product_code LIKE 'PROD-0%5-BETA';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 5;
    RAISE NOTICE 'Test 5 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 6
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email ILIKE 'USER1%';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 6;
    RAISE NOTICE 'Test 6 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 7
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email ILIKE '%@GMAIL.COM';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 7;
    RAISE NOTICE 'Test 7 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 8
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE description ILIKE '%MOUSE%';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 8;
    RAISE NOTICE 'Test 8 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 9
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_multi WHERE first_name LIKE 'John%' AND last_name LIKE 'Smith%';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 9;
    RAISE NOTICE 'Test 9 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 10
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_multi WHERE first_name LIKE 'J%' AND last_name LIKE '%son' AND company LIKE '%Tech%';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 10;
    RAISE NOTICE 'Test 10 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 11
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_multi WHERE first_name ILIKE 'JOHN%' AND last_name ILIKE 'SMITH%';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 11;
    RAISE NOTICE 'Test 11 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 12
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@gmail.com' AND description LIKE '%Mouse%';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 12;
    RAISE NOTICE 'Test 12 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 13
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@gmail.com' OR email LIKE '%@yahoo.com';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 13;
    RAISE NOTICE 'Test 13 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 14
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE description ILIKE '%MOUSE%' OR description ILIKE '%KEYBOARD%';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 14;
    RAISE NOTICE 'Test 14 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 15
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single 
    WHERE (email LIKE '%@gmail.com' AND product_code LIKE '%ALPHA%')
       OR (email LIKE '%@yahoo.com' AND product_code LIKE '%BETA%');
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 15;
    RAISE NOTICE 'Test 15 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 16
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@nonexistent.xyz';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 16;
    RAISE NOTICE 'Test 16 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 17
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 17;
    RAISE NOTICE 'Test 17 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

-- Test 18
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
    elapsed_ms NUMERIC;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '';
    end_time := clock_timestamp();
    elapsed_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    UPDATE verification_results SET actual_count = row_count, actual_time_ms = elapsed_ms WHERE test_number = 18;
    RAISE NOTICE 'Test 18 - Actual: % rows, % ms', row_count, ROUND(elapsed_ms, 3);
END $$;

DO $$ BEGIN RAISE NOTICE ''; RAISE NOTICE '✓ Phase 2 Complete - All actual tests recorded'; RAISE NOTICE ''; END $$;