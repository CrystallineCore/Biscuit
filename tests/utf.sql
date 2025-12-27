-- ============================================================================
-- Biscuit UTF-8 Comprehensive Test Suite
-- Tests LIKE/ILIKE with complex UTF-8 patterns
-- Correctly compares sequential scan vs Biscuit index results
-- ============================================================================

-- ============================================================================
-- SETUP: Create test table with diverse UTF-8 data
-- ============================================================================

DROP TABLE IF EXISTS utf8_test_data CASCADE;

CREATE TABLE utf8_test_data (
    id SERIAL PRIMARY KEY,
    description TEXT,
    test_string TEXT,
    category TEXT
);

-- ============================================================================
-- INSERT: Comprehensive UTF-8 test cases
-- ============================================================================

INSERT INTO utf8_test_data (description, test_string, category) VALUES
    -- ASCII baseline
    ('Pure ASCII', 'Hello World', 'ascii'),
    ('ASCII with numbers', 'Test123Data', 'ascii'),
    ('ASCII mixed case', 'ThE QuIcK BrOwN FoX', 'ascii'),
    ('ASCII lowercase', 'hello world test', 'ascii'),
    
    -- Latin Extended (2-byte UTF-8)
    ('French accents', 'Café au lait résumé naïve', 'latin'),
    ('German umlauts', 'Über Österreich Köln Größe', 'latin'),
    ('Spanish tildes', 'Año señor niño mañana', 'latin'),
    ('Nordic characters', 'Åse Øyvind Håkon Bjørn', 'latin'),
    ('Mixed European', 'Zürich Kraków Łódź', 'latin'),
    ('Polish characters', 'Łódź ćwierć źródło', 'latin'),
    
    -- Case folding edge cases
    ('Turkish I problem', 'ISTANBUL istanbul İstanbul', 'case_folding'),
    ('German ß/SS', 'Straße STRASSE Maß', 'case_folding'),
    ('Uppercase É', 'CAFÉ café Café', 'case_folding'),
    ('Greek Σ/σ/ς', 'ΣΕΛΑΣ σελας ςελας', 'case_folding'),
    ('Mixed case café', 'CaFé CAFÉ café', 'case_folding'),
    
    -- Chinese (3-byte UTF-8)
    ('Simplified Chinese', '中国人民解放军', 'cjk'),
    ('Traditional Chinese', '臺灣繁體字測試', 'cjk'),
    ('Japanese Kanji', '日本語漢字東京', 'cjk'),
    ('Korean Hangul', '한국어테스트서울', 'cjk'),
    ('Mixed CJK', '中文English日本語Mix', 'cjk'),
    ('Chinese with spaces', '你好 世界 测试', 'cjk'),
    
    -- Emojis and Symbols (3-4 byte UTF-8)
    ('Common emojis', '😀😁😂🤣😃😄', 'emoji'),
    ('Hearts and symbols', '❤️💙💚💛🧡💜', 'emoji'),
    ('Zodiac signs', '♈♉♊♋♌♍♎♏♐♑♒♓', 'emoji'),
    ('Weather symbols', '☀️☁️⛅🌤️⛈️🌧️', 'emoji'),
    ('Mixed emoji text', 'Hello 👋 World 🌍 Test 🧪', 'emoji'),
    ('Fire emoji', '🔥 Important', 'emoji'),
    ('Multiple fire', '🔥🔥🔥', 'emoji'),
    
    -- Combining characters
    ('Combining diacritics', 'e' || E'\u0301', 'combining'),
    ('Multiple combiners', 'o' || E'\u0308\u0304', 'combining'),
    ('Emoji with modifiers', '👨‍👩‍👧‍👦', 'combining'),
    
    -- Right-to-Left
    ('Arabic', 'مرحبا بك العربية', 'rtl'),
    ('Hebrew', 'שלום עברית בדיקה', 'rtl'),
    ('Mixed LTR/RTL', 'Hello مرحبا World', 'rtl'),
    
    -- Zero-width and special characters
    ('Zero-width joiner', 'Test' || E'\u200D' || 'Data', 'special'),
    ('Zero-width non-joiner', 'Test' || E'\u200C' || 'Data', 'special'),
    ('Soft hyphen', 'Test' || E'\u00AD' || 'Data', 'special'),
    ('Non-breaking space', 'Test' || E'\u00A0' || 'Data', 'special'),
    
    -- Mathematical symbols
    ('Math operators', '∑∏∫√∞≈≠≤≥', 'math'),
    ('Greek letters', 'αβγδεζηθικλμνξοπρστυφχψω', 'math'),
    ('Superscripts', 'x²+y³=z⁴', 'math'),
    
    -- Edge cases
    ('Leading emoji', '🔥Important Message', 'edge'),
    ('Trailing emoji', 'Important Message 🔥', 'edge'),
    ('Middle emoji', 'Important 🔥 Message', 'edge'),
    ('Only emojis', '🔥💯✨⚡', 'edge'),
    ('Empty string', '', 'edge'),
    ('Single char UTF-8', '中', 'edge'),
    ('Repeated UTF-8', '中中中中中', 'edge'),
    ('Spaces only', '   ', 'edge'),
    
    -- Long mixed strings
    ('Mixed everything', 'Hello世界🌍Café©2024™', 'mixed'),
    ('URL with UTF-8', 'https://example.com/中文/测试?q=café', 'mixed'),
    ('URL with UTF-8 suffix', 'https://example.com/中文/测试?q=caféstar', 'mixed'),
    ('Email with UTF-8', 'user@例え.jp', 'mixed'),
    ('Code with UTF-8', 'function 函数(参数) { return "café"; }', 'mixed'),
    
    -- Pattern matching challenges
    ('Percent in text', 'Sale: 50% off', 'pattern_chars'),
    ('Underscore in text', 'my_variable_name', 'pattern_chars'),
    ('Backslash in text', 'C:\Windows\System32', 'pattern_chars'),
    ('Wildcard-like', 'test%value_data', 'pattern_chars');

-- Add generated data for performance testing
INSERT INTO utf8_test_data (description, test_string, category)
SELECT 
    'Generated mixed ' || i,
    '测试' || i || 'Test' || 
    CASE 
        WHEN i % 2 = 0 THEN 'Ö'
        ELSE 'ö'
    END || '数据',
    'generated'
FROM generate_series(1, 1000) i;

-- Analyze table
ANALYZE utf8_test_data;

-- ============================================================================
-- Display sample data with UTF-8 analysis
-- ============================================================================

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '============================================================================';
RAISE NOTICE 'SAMPLE DATA WITH UTF-8 ANALYSIS';
RAISE NOTICE '============================================================================';
END $$;

SELECT 
    id,
    left(description, 25) as description,
    left(test_string, 30) as sample,
    length(test_string) as chars,
    octet_length(test_string) as bytes,
    round(octet_length(test_string)::numeric / NULLIF(length(test_string), 0), 2) as bytes_per_char,
    category
FROM utf8_test_data
WHERE category IN ('emoji', 'cjk', 'latin', 'edge', 'case_folding')
ORDER BY bytes_per_char DESC NULLS LAST, id
LIMIT 25;

-- ============================================================================
-- Define test cases as a table
-- ============================================================================

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '============================================================================';
RAISE NOTICE 'TEST CASE DEFINITIONS';
RAISE NOTICE '============================================================================';
END $$;

DROP TABLE IF EXISTS test_cases;

CREATE TEMP TABLE test_cases (
    test_id SERIAL PRIMARY KEY,
    test_name TEXT UNIQUE NOT NULL,
    test_pattern TEXT NOT NULL,
    operator TEXT NOT NULL CHECK (operator IN ('LIKE', 'ILIKE', 'NOT LIKE', 'NOT ILIKE')),
    description TEXT
);

INSERT INTO test_cases (test_name, test_pattern, operator, description) VALUES
    ('ascii_like_hello', '%Hello%', 'LIKE', 'Simple ASCII pattern'),
    ('ascii_ilike_hello', '%HELLO%', 'ILIKE', 'Case-insensitive ASCII'),
    ('ascii_like_test', '%Test%', 'LIKE', 'ASCII Test pattern'),
    ('ascii_ilike_test', '%test%', 'ILIKE', 'Case-insensitive test'),
    
    ('chinese_like_zhong', '%中%', 'LIKE', 'Chinese character 中'),
    ('chinese_ilike_zhong', '%中%', 'ILIKE', 'Chinese ILIKE (no case)'),
    ('chinese_like_world', '%世界%', 'LIKE', 'Multi-char Chinese'),
    ('chinese_like_hello', '%你好%', 'LIKE', 'Chinese hello'),
    
    ('emoji_like_fire', '%🔥%', 'LIKE', 'Fire emoji'),
    ('emoji_like_wave', '%👋%', 'LIKE', 'Wave emoji'),
    ('emoji_like_earth', '%🌍%', 'LIKE', 'Earth emoji'),
    
    ('french_like_cafe_upper', '%Café%', 'LIKE', 'Café with uppercase C'),
    ('french_like_cafe_lower', '%café%', 'LIKE', 'café all lowercase'),
    ('french_ilike_cafe', '%café%', 'ILIKE', 'café case-insensitive'),
    ('french_like_resume', '%résumé%', 'LIKE', 'French résumé'),
    
    ('german_like_o_upper', '%Ö%', 'LIKE', 'German Ö uppercase'),
    ('german_like_o_lower', '%ö%', 'LIKE', 'German ö lowercase'),
    ('german_ilike_o', '%ö%', 'ILIKE', 'German ö case-insensitive'),
    ('german_like_uber', '%Über%', 'LIKE', 'Über pattern'),
    
    ('greek_like_alpha', '%α%', 'LIKE', 'Greek alpha'),
    ('greek_like_sigma', '%σ%', 'LIKE', 'Greek sigma'),
    
    ('math_like_sum', '%∑%', 'LIKE', 'Summation symbol'),
    ('math_like_sqrt', '%√%', 'LIKE', 'Square root symbol'),
    
    ('zodiac_like_aries', '%♈%', 'LIKE', 'Aries zodiac sign'),
    
    ('prefix_hello', 'Hello%', 'LIKE', 'Prefix pattern'),
    ('suffix_world', '%World', 'LIKE', 'Suffix pattern'),
    ('underscore_test', 'Test___Data', 'LIKE', 'Underscore wildcard'),
    
    ('complex_chinese_test', '%测试%Test%', 'LIKE', 'Complex multi-part'),
    ('complex_mixed', '%世界%Café%', 'LIKE', 'Mixed CJK and Latin'),
    
    ('not_like_emoji', '%emoji%', 'NOT LIKE', 'Exclude emoji word'),
    ('not_ilike_hello', '%HELLO%', 'NOT ILIKE', 'Exclude hello case-insensitive'),
    ('not_like_generated', '%Generated%', 'NOT LIKE', 'Exclude generated'),
    
    ('url_pattern', '%example.com%', 'LIKE', 'URL pattern'),
    ('mixed_code', '%function%', 'LIKE', 'Code pattern');

SELECT test_id, test_name, test_pattern, operator, description
FROM test_cases
ORDER BY test_id;

-- ============================================================================
-- PHASE 1: BASELINE - Sequential Scan (No Index)
-- ============================================================================

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '============================================================================';
RAISE NOTICE 'PHASE 1: BASELINE RESULTS (Sequential Scan - No Index)';
RAISE NOTICE '============================================================================';
END $$;

-- Ensure no Biscuit index exists
DROP INDEX IF EXISTS idx_biscuit_test;

-- Force sequential scans
SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- Create table to store baseline results
DROP TABLE IF EXISTS baseline_results;

CREATE TEMP TABLE baseline_results (
    test_id INTEGER PRIMARY KEY,
    test_name TEXT NOT NULL,
    result_count BIGINT NOT NULL,
    execution_time_ms NUMERIC,
    captured_at TIMESTAMPTZ DEFAULT now()
);

-- Execute each test case and capture results
DO $$
DECLARE
    tc RECORD;
    result_count BIGINT;
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
    query_text TEXT;
BEGIN
    FOR tc IN SELECT * FROM test_cases ORDER BY test_id LOOP
        -- Build the query
        query_text := format(
            'SELECT count(*) FROM utf8_test_data WHERE test_string %s %L',
            tc.operator,
            tc.test_pattern
        );
        
        -- Execute and time the query
        start_time := clock_timestamp();
        EXECUTE query_text INTO result_count;
        end_time := clock_timestamp();
        
        duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        
        -- Store result
        INSERT INTO baseline_results (test_id, test_name, result_count, execution_time_ms)
        VALUES (tc.test_id, tc.test_name, result_count, duration_ms);
        
        RAISE NOTICE 'Baseline % [%]: % rows (%.2f ms)', 
            tc.test_name, tc.operator, result_count, duration_ms;
    END LOOP;
END $$;

-- Re-enable index scans
SET enable_indexscan = on;
SET enable_bitmapscan = on;

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE 'Baseline results captured.';
END $$;

SELECT 
    test_name,
    result_count,
    round(execution_time_ms, 2) as time_ms
FROM baseline_results
ORDER BY test_id;

-- ============================================================================
-- PHASE 2: Create Biscuit Index
-- ============================================================================

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '============================================================================';
RAISE NOTICE 'PHASE 2: Creating Biscuit Index';
RAISE NOTICE '============================================================================';
END $$;

CREATE INDEX idx_biscuit_test ON utf8_test_data USING biscuit(test_string);

ANALYZE utf8_test_data;

-- Verify index exists
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'utf8_test_data'
  AND indexname = 'idx_biscuit_test';

-- ============================================================================
-- PHASE 3: WITH INDEX - Biscuit Index Scan
-- ============================================================================

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '============================================================================';
RAISE NOTICE 'PHASE 3: INDEXED RESULTS (Biscuit Index Scan)';
RAISE NOTICE '============================================================================';
END $$;

-- Create table to store indexed results
DROP TABLE IF EXISTS indexed_results;

CREATE TEMP TABLE indexed_results (
    test_id INTEGER PRIMARY KEY,
    test_name TEXT NOT NULL,
    result_count BIGINT NOT NULL,
    execution_time_ms NUMERIC,
    captured_at TIMESTAMPTZ DEFAULT now()
);

-- Execute each test case with index
DO $$
DECLARE
    tc RECORD;
    result_count BIGINT;
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
    duration_ms NUMERIC;
    query_text TEXT;
BEGIN
    FOR tc IN SELECT * FROM test_cases ORDER BY test_id LOOP
        -- Build the query
        query_text := format(
            'SELECT count(*) FROM utf8_test_data WHERE test_string %s %L',
            tc.operator,
            tc.test_pattern
        );
        
        -- Execute and time the query
        start_time := clock_timestamp();
        EXECUTE query_text INTO result_count;
        end_time := clock_timestamp();
        
        duration_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
        
        -- Store result
        INSERT INTO indexed_results (test_id, test_name, result_count, execution_time_ms)
        VALUES (tc.test_id, tc.test_name, result_count, duration_ms);
        
        RAISE NOTICE 'Indexed % [%]: % rows (%.2f ms)', 
            tc.test_name, tc.operator, result_count, duration_ms;
    END LOOP;
END $$;


SELECT 
    test_name,
    result_count,
    round(execution_time_ms, 2) as time_ms
FROM indexed_results
ORDER BY test_id;

-- ============================================================================
-- COMPARISON AND VERIFICATION
-- ============================================================================

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '============================================================================';
RAISE NOTICE 'VERIFICATION REPORT: Baseline vs Indexed Results';
RAISE NOTICE '============================================================================';
END $$;

SELECT 
    tc.test_name,
    tc.operator,
    tc.test_pattern,
    br.result_count as baseline_count,
    ir.result_count as indexed_count,
    CASE 
        WHEN br.result_count = ir.result_count THEN '✓ PASS'
        ELSE '✗ FAIL'
    END as status,
    (ir.result_count - br.result_count) as difference,
    round(br.execution_time_ms, 2) as baseline_ms,
    round(ir.execution_time_ms, 2) as indexed_ms,
    CASE 
        WHEN ir.execution_time_ms > 0 
        THEN round(br.execution_time_ms / ir.execution_time_ms, 2)
        ELSE NULL
    END as speedup
FROM test_cases tc
JOIN baseline_results br USING (test_id)
JOIN indexed_results ir USING (test_id)
ORDER BY 
    CASE WHEN br.result_count = ir.result_count THEN 1 ELSE 0 END,
    tc.test_id;

-- Summary statistics
DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '--- Summary Statistics ---';
END $$;

SELECT 
    count(*) as total_tests,
    sum(CASE WHEN br.result_count = ir.result_count THEN 1 ELSE 0 END) as passed,
    sum(CASE WHEN br.result_count != ir.result_count THEN 1 ELSE 0 END) as failed,
    round(100.0 * sum(CASE WHEN br.result_count = ir.result_count THEN 1 ELSE 0 END) / count(*), 2) as pass_rate,
    round(avg(br.execution_time_ms), 2) as avg_baseline_ms,
    round(avg(ir.execution_time_ms), 2) as avg_indexed_ms
FROM baseline_results br
JOIN indexed_results ir USING (test_id);

-- ============================================================================
-- DETAILED MISMATCH ANALYSIS
-- ============================================================================
DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '============================================================================';
RAISE NOTICE 'MISMATCH ANALYSIS (if any failures)';
RAISE NOTICE '============================================================================';
END $$;

SELECT 
    tc.test_name,
    tc.operator,
    tc.test_pattern,
    br.result_count as baseline,
    ir.result_count as indexed,
    (ir.result_count - br.result_count) as difference,
    round(100.0 * abs(ir.result_count - br.result_count) / NULLIF(br.result_count, 0), 2) as error_pct
FROM test_cases tc
JOIN baseline_results br USING (test_id)
JOIN indexed_results ir USING (test_id)
WHERE br.result_count != ir.result_count
ORDER BY abs(ir.result_count - br.result_count) DESC;

-- If mismatches exist, show sample data
DO $$
DECLARE
    mismatch_count INTEGER;
BEGIN
    SELECT count(*) INTO mismatch_count
    FROM baseline_results br
    JOIN indexed_results ir USING (test_id)
    WHERE br.result_count != ir.result_count;
    
    IF mismatch_count > 0 THEN
        RAISE NOTICE '';
        RAISE NOTICE '⚠ WARNING: % test(s) show mismatches between baseline and indexed!', mismatch_count;
        RAISE NOTICE 'Investigate the queries above for discrepancies.';
    ELSE
        RAISE NOTICE '';
        RAISE NOTICE '✓ SUCCESS: All tests passed! Biscuit index matches sequential scan exactly.';
    END IF;
END $$;

-- ============================================================================
-- DETAILED QUERY EXAMPLES
-- ============================================================================

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '============================================================================';
RAISE NOTICE 'SAMPLE QUERY RESULTS (French café test)';
RAISE NOTICE '============================================================================';

RAISE NOTICE '';
RAISE NOTICE 'Pattern: %café% (ILIKE - case insensitive)';
END $$;

SELECT 
    id,
    left(description, 20) as description,
    test_string,
    category
FROM utf8_test_data
WHERE test_string ILIKE '%café%'
ORDER BY id;

RAISE NOTICE ''
RAISE NOTICE 'Pattern: %Café% (LIKE - exact case)'

SELECT 
    id,
    left(description, 20) as description,
    test_string,
    category
FROM utf8_test_data
WHERE test_string LIKE '%Café%'
ORDER BY id;

-- ============================================================================
-- EXPLAIN ANALYZE COMPARISON
-- ============================================================================

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '============================================================================';
RAISE NOTICE 'EXPLAIN ANALYZE: Sequential Scan vs Biscuit Index';
RAISE NOTICE '============================================================================';

RAISE NOTICE '';
RAISE NOTICE '--- Without Index (Sequential Scan) ---';
END $$;

DROP INDEX idx_biscuit_test;

EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT count(*) FROM utf8_test_data WHERE test_string LIKE '%中%';

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '--- With Biscuit Index ---';
END $$;

CREATE INDEX idx_biscuit_test ON utf8_test_data USING biscuit(test_string);

EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT count(*) FROM utf8_test_data WHERE test_string LIKE '%中%';

-- ============================================================================
-- INDEX STATISTICS
-- ============================================================================

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '============================================================================';
RAISE NOTICE 'BISCUIT INDEX STATISTICS';
RAISE NOTICE '============================================================================';
END $$;

SELECT * FROM biscuit_index_stats('idx_biscuit_test'::regclass);

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '--- Index Memory Usage ---';
END $$;

SELECT pg_size_pretty(biscuit_index_memory_size('idx_biscuit_test'::regclass)) as memory_size;

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '--- Build Information ---';
END $$;

SELECT * FROM biscuit_build_info();

-- ============================================================================
-- FINAL REPORT
-- ============================================================================

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE '============================================================================';
RAISE NOTICE 'FINAL VERIFICATION SUMMARY';
RAISE NOTICE '============================================================================';
END $$;

SELECT 
    CASE 
        WHEN count(*) = sum(CASE WHEN br.result_count = ir.result_count THEN 1 ELSE 0 END)
        THEN '✓ ALL TESTS PASSED'
        ELSE '✗ SOME TESTS FAILED'
    END as final_status,
    count(*) as total_tests,
    sum(CASE WHEN br.result_count = ir.result_count THEN 1 ELSE 0 END) as passed,
    sum(CASE WHEN br.result_count != ir.result_count THEN 1 ELSE 0 END) as failed,
    round(avg(br.execution_time_ms), 2) as avg_baseline_ms,
    round(avg(ir.execution_time_ms), 2) as avg_indexed_ms,
    round(avg(br.execution_time_ms / NULLIF(ir.execution_time_ms, 0)), 2) as avg_speedup
FROM baseline_results br
JOIN indexed_results ir USING (test_id);

DO $$
BEGIN
RAISE NOTICE '';
RAISE NOTICE 'Test suite completed.';
RAISE NOTICE '';
END $$;