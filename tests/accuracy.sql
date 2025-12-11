-- ============================================================================
-- BISCUIT INDEX VERIFICATION BENCHMARK - FOSDEM 2025 EDITION
-- True verification: Compare Biscuit results against actual PostgreSQL results
-- ============================================================================

-- Clean slate
DROP TABLE IF EXISTS benchmark_single CASCADE;
DROP TABLE IF EXISTS benchmark_multi CASCADE;
DROP TABLE IF EXISTS verification_results CASCADE;

-- Verification results table (stores both Biscuit and Actual counts)
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
-- BISCUIT TESTS - SECTION 2: Single-Column LIKE Tests
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '📋 Running LIKE Tests with Biscuit...'; END $$;

-- Test 1: PREFIX pattern
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE 'user1%';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (1, 'LIKE', 'PREFIX: user1%', 'email LIKE ''user1%''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 1 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 2: SUFFIX pattern
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@gmail.com';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (2, 'LIKE', 'SUFFIX: %@gmail.com', 'email LIKE ''%@gmail.com''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 2 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 3: SUBSTRING pattern
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE product_code LIKE '%ALPHA%';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (3, 'LIKE', 'SUBSTRING: %ALPHA%', 'product_code LIKE ''%ALPHA%''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 3 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 4: EXACT match
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE description LIKE 'Wireless Mouse with Bluetooth';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (4, 'LIKE', 'EXACT: Wireless Mouse with Bluetooth', 'description LIKE ''Wireless Mouse with Bluetooth''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 4 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 5: Complex multi-part pattern
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE product_code LIKE 'PROD-0%5-BETA';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (5, 'LIKE', 'COMPLEX: PROD-0%5-BETA', 'product_code LIKE ''PROD-0%5-BETA''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 5 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
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
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email ILIKE 'USER1%';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (6, 'ILIKE', 'PREFIX: USER1%', 'email ILIKE ''USER1%''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 6 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 7: ILIKE SUFFIX
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email ILIKE '%@GMAIL.COM';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (7, 'ILIKE', 'SUFFIX: %@GMAIL.COM', 'email ILIKE ''%@GMAIL.COM''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 7 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 8: ILIKE SUBSTRING
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE description ILIKE '%MOUSE%';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (8, 'ILIKE', 'SUBSTRING: %MOUSE%', 'description ILIKE ''%MOUSE%''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 8 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
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
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_multi WHERE first_name LIKE 'John%' AND last_name LIKE 'Smith%';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (9, 'MULTI-LIKE', 'Two-column: John% AND Smith%', 'first_name LIKE ''John%'' AND last_name LIKE ''Smith%''', 'Multi-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 9 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 10: Three-column LIKE
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_multi WHERE first_name LIKE 'J%' AND last_name LIKE '%son' AND company LIKE '%Tech%';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (10, 'MULTI-LIKE', 'Three-column: J% AND %son AND %Tech%', 'first_name LIKE ''J%'' AND last_name LIKE ''%son'' AND company LIKE ''%Tech%''', 'Multi-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 10 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 11: Two-column ILIKE
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_multi WHERE first_name ILIKE 'JOHN%' AND last_name ILIKE 'SMITH%';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (11, 'MULTI-ILIKE', 'Two-column: JOHN% AND SMITH%', 'first_name ILIKE ''JOHN%'' AND last_name ILIKE ''SMITH%''', 'Multi-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 11 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
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
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@gmail.com' AND description LIKE '%Mouse%';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (12, 'AND', 'Two predicates: %@gmail.com AND %Mouse%', 'email LIKE ''%@gmail.com'' AND description LIKE ''%Mouse%''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 12 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 13: Simple OR
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@gmail.com' OR email LIKE '%@yahoo.com';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (13, 'OR', 'Two predicates: %@gmail.com OR %@yahoo.com', 'email LIKE ''%@gmail.com'' OR email LIKE ''%@yahoo.com''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 13 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 14: OR with ILIKE
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE description ILIKE '%MOUSE%' OR description ILIKE '%KEYBOARD%';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (14, 'OR', 'ILIKE: %MOUSE% OR %KEYBOARD%', 'description ILIKE ''%MOUSE%'' OR description ILIKE ''%KEYBOARD%''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 14 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 15: Combined (AND) OR (AND)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single 
    WHERE (email LIKE '%@gmail.com' AND product_code LIKE '%ALPHA%')
       OR (email LIKE '%@yahoo.com' AND product_code LIKE '%BETA%');
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (15, 'AND/OR', '(gmail AND ALPHA) OR (yahoo AND BETA)', '(email LIKE ''%@gmail.com'' AND product_code LIKE ''%ALPHA%'') OR (email LIKE ''%@yahoo.com'' AND product_code LIKE ''%BETA%'')', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 15 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
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
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@nonexistent.xyz';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (16, 'EDGE', 'Empty result set', 'email LIKE ''%@nonexistent.xyz''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 16 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 17: Match all (single %)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (17, 'EDGE', 'Match all (single %)', 'email LIKE ''%''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 17 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 18: Empty pattern
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '';
    end_time := clock_timestamp();
    
    INSERT INTO verification_results (test_number, test_category, test_name, test_query, index_type, biscuit_count, biscuit_time_ms)
    VALUES (18, 'EDGE', 'Empty pattern', 'email LIKE ''''', 'Single-column', row_count, 
            EXTRACT(MILLISECONDS FROM (end_time - start_time)));
    
    RAISE NOTICE 'Test 18 - Biscuit: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

DO $$ BEGIN RAISE NOTICE ''; RAISE NOTICE '✓ Phase 1 Complete - All Biscuit tests recorded'; RAISE NOTICE ''; END $$;

-- ============================================================================
-- PHASE 2: DROP BISCUIT INDEXES AND RUN ACTUAL TESTS
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

-- Now run the same tests WITHOUT Biscuit indexes

DO $$ BEGIN RAISE NOTICE '📋 Running ACTUAL Tests (No Biscuit)...'; END $$;

-- Test 1: PREFIX pattern (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE 'user1%';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 1;
    
    RAISE NOTICE 'Test 1 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 2: SUFFIX pattern (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@gmail.com';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 2;
    
    RAISE NOTICE 'Test 2 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 3: SUBSTRING pattern (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE product_code LIKE '%ALPHA%';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 3;
    
    RAISE NOTICE 'Test 3 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 4: EXACT match (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE description LIKE 'Wireless Mouse with Bluetooth';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 4;
    
    RAISE NOTICE 'Test 4 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 5: Complex multi-part pattern (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE product_code LIKE 'PROD-0%5-BETA';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 5;
    
    RAISE NOTICE 'Test 5 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 6: ILIKE PREFIX (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email ILIKE 'USER1%';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 6;
    
    RAISE NOTICE 'Test 6 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 7: ILIKE SUFFIX (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email ILIKE '%@GMAIL.COM';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 7;
    
    RAISE NOTICE 'Test 7 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 8: ILIKE SUBSTRING (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE description ILIKE '%MOUSE%';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 8;
    
    RAISE NOTICE 'Test 8 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 9: Two-column LIKE (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_multi WHERE first_name LIKE 'John%' AND last_name LIKE 'Smith%';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 9;
    
    RAISE NOTICE 'Test 9 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 10: Three-column LIKE (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_multi WHERE first_name LIKE 'J%' AND last_name LIKE '%son' AND company LIKE '%Tech%';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 10;
    
    RAISE NOTICE 'Test 10 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 11: Two-column ILIKE (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_multi WHERE first_name ILIKE 'JOHN%' AND last_name ILIKE 'SMITH%';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 11;
    
    RAISE NOTICE 'Test 11 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 12: Simple AND (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@gmail.com' AND description LIKE '%Mouse%';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 12;
    
    RAISE NOTICE 'Test 12 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 13: Simple OR (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@gmail.com' OR email LIKE '%@yahoo.com';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 13;
    
    RAISE NOTICE 'Test 13 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 14: OR with ILIKE (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE description ILIKE '%MOUSE%' OR description ILIKE '%KEYBOARD%';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 14;
    
    RAISE NOTICE 'Test 14 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 15: Combined (AND) OR (AND) (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single 
    WHERE (email LIKE '%@gmail.com' AND product_code LIKE '%ALPHA%')
       OR (email LIKE '%@yahoo.com' AND product_code LIKE '%BETA%');
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 15;
    
    RAISE NOTICE 'Test 15 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 16: Empty result (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%@nonexistent.xyz';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 16;
    
    RAISE NOTICE 'Test 16 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 17: Match all (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '%';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 17;
    
    RAISE NOTICE 'Test 17 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

-- Test 18: Empty pattern (ACTUAL)
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count INT;
BEGIN
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO row_count FROM benchmark_single WHERE email LIKE '';
    end_time := clock_timestamp();
    
    UPDATE verification_results 
    SET actual_count = row_count, 
        actual_time_ms = EXTRACT(MILLISECONDS FROM (end_time - start_time))
    WHERE test_number = 18;
    
    RAISE NOTICE 'Test 18 - Actual: % rows, % ms', row_count, ROUND(EXTRACT(MILLISECONDS FROM (end_time - start_time))::NUMERIC, 3);
END $$;

DO $$ BEGIN RAISE NOTICE ''; RAISE NOTICE '✓ Phase 2 Complete - All actual tests recorded'; RAISE NOTICE ''; END $$;

-- ============================================================================
-- PHASE 3: VERIFICATION - Compare Biscuit vs Actual
-- ============================================================================

DO $$ 
BEGIN 
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '🔍 PHASE 3: VERIFICATION - Comparing Results';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;

-- Update verification status for all tests
UPDATE verification_results
SET counts_match = (biscuit_count = actual_count),
    verification_status = CASE 
        WHEN biscuit_count = actual_count THEN '✓ PASS'
        ELSE '✗ FAIL - Count Mismatch'
    END,
    notes = CASE 
        WHEN biscuit_count = actual_count THEN 'Counts match perfectly'
        ELSE 'CRITICAL: Biscuit returned ' || biscuit_count || ' rows but actual is ' || actual_count
    END;

-- ============================================================================
-- FINAL SUMMARY & REPORT
-- ============================================================================

DO $$ 
BEGIN 
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '📊 VERIFICATION SUMMARY - FOSDEM 2025';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;

-- Overall statistics
DO $$
DECLARE
    total_tests INT;
    passed_tests INT;
    failed_tests INT;
    total_speedup NUMERIC;
    avg_speedup NUMERIC;
BEGIN
    SELECT 
        COUNT(*),
        SUM(CASE WHEN counts_match THEN 1 ELSE 0 END),
        SUM(CASE WHEN NOT counts_match THEN 1 ELSE 0 END)
    INTO total_tests, passed_tests, failed_tests
    FROM verification_results;
    
    SELECT 
        SUM(actual_time_ms / NULLIF(biscuit_time_ms, 0)),
        AVG(actual_time_ms / NULLIF(biscuit_time_ms, 0))
    INTO total_speedup, avg_speedup
    FROM verification_results
    WHERE biscuit_time_ms > 0 AND actual_time_ms > 0 AND counts_match;
    
    RAISE NOTICE '📈 Overall Statistics:';
    RAISE NOTICE '   Total Tests: %', total_tests;
    RAISE NOTICE '   ✓ Passed (Counts Match): %', passed_tests;
    RAISE NOTICE '   ✗ Failed (Count Mismatch): %', failed_tests;
    RAISE NOTICE '   Success Rate: %.1f%%', (passed_tests::NUMERIC / total_tests * 100);
    RAISE NOTICE '';
    RAISE NOTICE '⚡ Performance Impact:';
    RAISE NOTICE '   Average Speedup: %.2fx faster with Biscuit', COALESCE(avg_speedup, 0);
    RAISE NOTICE '';
END $$;

-- Detailed comparison table
DO $$
DECLARE
    rec RECORD;
BEGIN
    RAISE NOTICE '📋 Detailed Test Results:';
    RAISE NOTICE '   %-4s | %-15s | %-40s | %-10s | %-10s | %s', 
        'Test', 'Category', 'Name', 'Biscuit', 'Actual', 'Status';
    RAISE NOTICE '   %', REPEAT('-', 110);
    
    FOR rec IN 
        SELECT 
            test_number,
            test_category,
            test_name,
            biscuit_count,
            actual_count,
            verification_status,
            ROUND((actual_time_ms / NULLIF(biscuit_time_ms, 0))::NUMERIC, 2) as speedup
        FROM verification_results
        ORDER BY test_number
    LOOP
        RAISE NOTICE '   %-4s | %-15s | %-40s | %-10s | %-10s | % (%.2fx)', 
            rec.test_number,
            rec.test_category,
            SUBSTRING(rec.test_name, 1, 40),
            rec.biscuit_count,
            rec.actual_count,
            rec.verification_status,
            COALESCE(rec.speedup, 0);
    END LOOP;
    RAISE NOTICE '';
END $$;

-- Failed tests detail (if any)
DO $$
DECLARE
    rec RECORD;
    failure_count INT;
BEGIN
    SELECT COUNT(*) INTO failure_count
    FROM verification_results
    WHERE NOT counts_match;
    
    IF failure_count > 0 THEN
        RAISE NOTICE '⚠️  CRITICAL: FAILED TESTS DETECTED!';
        RAISE NOTICE '';
        
        FOR rec IN 
            SELECT 
                test_number,
                test_category,
                test_name,
                biscuit_count,
                actual_count,
                notes
            FROM verification_results
            WHERE NOT counts_match
            ORDER BY test_number
        LOOP
            RAISE NOTICE '   ✗ Test % [%] %', rec.test_number, rec.test_category, rec.test_name;
            RAISE NOTICE '     Biscuit Count: %', rec.biscuit_count;
            RAISE NOTICE '     Actual Count:  %', rec.actual_count;
            RAISE NOTICE '     Difference:    %', (rec.biscuit_count - rec.actual_count);
            RAISE NOTICE '     Notes: %', rec.notes;
            RAISE NOTICE '';
        END LOOP;
    ELSE
        RAISE NOTICE '✅ All Tests Passed - Counts Match Perfectly!';
        RAISE NOTICE '';
    END IF;
END $$;

-- Performance by category
DO $$
DECLARE
    rec RECORD;
BEGIN
    RAISE NOTICE '📊 Performance by Category:';
    RAISE NOTICE '   %-15s | %-8s | %-15s | %-15s | %-10s', 
        'Category', 'Tests', 'Avg Biscuit(ms)', 'Avg Actual(ms)', 'Speedup';
    RAISE NOTICE '   %', REPEAT('-', 80);
    
    FOR rec IN 
        SELECT 
            test_category,
            COUNT(*) as test_count,
            ROUND(AVG(biscuit_time_ms), 3) as avg_biscuit,
            ROUND(AVG(actual_time_ms), 3) as avg_actual,
            ROUND(AVG(actual_time_ms / NULLIF(biscuit_time_ms, 0)), 2) as avg_speedup
        FROM verification_results
        WHERE counts_match
        GROUP BY test_category
        ORDER BY test_category
    LOOP
        RAISE NOTICE '   %-15s | %-8s | %-15s | %-15s | %.2fx', 
            rec.test_category,
            rec.test_count,
            rec.avg_biscuit,
            rec.avg_actual,
            COALESCE(rec.avg_speedup, 0);
    END LOOP;
    RAISE NOTICE '';
END $$;

-- Final verdict
DO $$
DECLARE
    passed_tests INT;
    total_tests INT;
BEGIN
    SELECT 
        SUM(CASE WHEN counts_match THEN 1 ELSE 0 END),
        COUNT(*)
    INTO passed_tests, total_tests
    FROM verification_results;
    
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    IF passed_tests = total_tests THEN
        RAISE NOTICE '   ✅ VERIFICATION COMPLETE - ALL TESTS PASSED!';
        RAISE NOTICE '   Biscuit Index is ACCURATE and ready for publication!';
    ELSE
        RAISE NOTICE '   ❌ VERIFICATION FAILED - % OF % TESTS FAILED', (total_tests - passed_tests), total_tests;
        RAISE NOTICE '   DO NOT PUBLISH - Biscuit Index has correctness issues!';
    END IF;
    RAISE NOTICE '═══════════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
END $$;

DO $$ 
BEGIN 
    RAISE NOTICE '📁 Full results available in verification_results table';
    RAISE NOTICE '   Run: SELECT * FROM verification_results ORDER BY test_number;';
    RAISE NOTICE '';
    RAISE NOTICE '🎉 FOSDEM 2025 Biscuit Verification Complete!';
    RAISE NOTICE '';
END $$;