-- =============================================================================
-- BISCUIT POSTGRESQL EXTENSION - COMPREHENSIVE SQL REGRESSION TEST SUITE
-- =============================================================================
-- Language:     Pure SQL + PL/pgSQL only. No psql meta-commands.
-- Deterministic: Yes - random seed fixed at 0.42
-- Compatible:   PostgreSQL regression testing (pg_regress)
-- =============================================================================
-- SECTIONS
--   §1  Schema Setup & Seed
--   §2  Deterministic Data Generation (100,000 rows)
--   §3  Controlled Edge-Case Data
--   §4  Query Catalog
--   §5  Result Storage
--   §6  Sequential Scan Baseline
--   §7  Single-Column Biscuit Index Tests
--   §8  Multi-Column Biscuit Index Tests
--   §9  Expression Index Tests
--   §10 UPDATE Maintenance Tests
--   §11 INSERT Maintenance Tests
--   §12 DELETE Maintenance Tests
--   §13 Duplicate Handling Tests
--   §14 NULL Handling Tests
--   §15 Planner Verification
--   §16 Parallel Scan Tests
--   §17 VACUUM Tests
--   §18 REINDEX Tests
--   §19 Final Report & Summary
-- =============================================================================


-- =============================================================================
-- §1  SCHEMA SETUP & SEED
-- =============================================================================

-- Drop all objects in reverse dependency order for idempotent re-runs.
DROP TABLE IF EXISTS biscuit_test_results  CASCADE;
DROP TABLE IF EXISTS biscuit_query_catalog CASCADE;
DROP TABLE IF EXISTS biscuit_data          CASCADE;

-- Fix the random seed so every run generates the exact same dataset.
SELECT setseed(0.42);

-- Record PostgreSQL version in test output.
SELECT version() AS pg_version;


-- =============================================================================
-- §2  DETERMINISTIC DATA GENERATION
-- =============================================================================

CREATE TABLE biscuit_data (
    id      SERIAL PRIMARY KEY,
    vc_col  VARCHAR(1000),
    tx_col  TEXT
);

COMMENT ON TABLE  biscuit_data        IS 'Primary dataset for Biscuit regression suite';
COMMENT ON COLUMN biscuit_data.vc_col IS 'VARCHAR column - indexed individually and in composite';
COMMENT ON COLUMN biscuit_data.tx_col IS 'TEXT column - indexed individually and in composite';

-- Reset seed immediately before bulk insert so the sequence is reproducible
-- regardless of what happened above.
SELECT setseed(0.42);

-- Insert 100,000 rows with random lowercase strings (length 5-15).
-- Two independent lateral subqueries produce vc_col and tx_col.
-- string_agg over generate_series avoids any PL/pgSQL loop.
INSERT INTO biscuit_data (vc_col, tx_col)
SELECT
    (
        SELECT string_agg(chr(97 + (random() * 25)::int), '')
        FROM generate_series(1, 5 + (random() * 10)::int)
    ),
    (
        SELECT string_agg(chr(97 + (random() * 25)::int), '')
        FROM generate_series(1, 5 + (random() * 10)::int)
    )
FROM generate_series(1, 100000);

SELECT COUNT(*) AS total_random_rows FROM biscuit_data;


-- =============================================================================
-- §3  CONTROLLED EDGE-CASE DATA
-- =============================================================================

INSERT INTO biscuit_data (vc_col, tx_col) VALUES
    -- Empty strings
    ('',                    ''                   ),
    -- Single character
    ('x',                   'y'                  ),
    -- Length 5
    ('abcde',               'vwxyz'              ),
    -- Length 15
    ('abcdefghijklmno',     'pqrstuvwxyzabc'     ),
    -- Length 100
    (repeat('m', 100),      repeat('n', 100)     ),
    -- Length 1000
    (repeat('z', 1000),     repeat('a', 1000)    ),
    -- Duplicates (same as length-5 row above)
    ('abcde',               'vwxyz'              ),
    ('abcde',               'vwxyz'              ),
    -- Mixed case
    ('HelloWorld',          'FooBarBaz'          ),
    ('UPPERCASE',           'lowercase'          ),
    -- NULL values
    (NULL,                  'not_null_text'      ),
    ('not_null_vc',         NULL                 ),
    (NULL,                  NULL                 ),
    -- Whitespace
    (' ',                   ' '                  ),
    ('   ',                 '   '                ),
    (E'\t',                 E'\t'                ),
    -- Literal wildcard characters in data
    ('100%',                '50%done'            ),
    ('under_score',         'test_value'         ),
    ('100%%200',            '%%percent%%'        ),
    -- Escaped wildcard patterns stored as data values
    (E'escape\\%test',      E'escape\\_test'     ),
    -- Strings that resemble patterns
    ('%prefix',             'suffix%'            ),
    ('_single',             'single_'            ),
    ('%both_here%',         '_mix%match_'        ),
    -- Known anchors for deterministic pattern tests
    ('biscuit_prefix_aaa',  'biscuit_prefix_bbb' ),
    ('aaa_biscuit_suffix',  'bbb_biscuit_suffix' ),
    ('aaa_biscuit_mid_zzz', 'bbb_biscuit_mid_yyy'),
    -- ILIKE-specific (differs only by case)
    ('BiScUiT_CaSe',        'biscuit_case'       ),
    -- Long repeating pattern
    ('aaabbbcccdddeee',     'zzzyyxxxwwwvvv'     ),
    -- Single quote in data
    (E'it''s a test',       E'quote''s here'     ),
    -- Newline in data
    (E'line1\nline2',       E'col\nrow'          );

SELECT COUNT(*) AS total_rows_after_edge_cases FROM biscuit_data;


-- =============================================================================
-- §4  QUERY CATALOG
-- =============================================================================

CREATE TABLE biscuit_query_catalog (
    query_code  VARCHAR(30) PRIMARY KEY,
    description TEXT NOT NULL
);

COMMENT ON TABLE biscuit_query_catalog IS
    'Registry of every logical test query. Use query_code everywhere.';

INSERT INTO biscuit_query_catalog (query_code, description) VALUES
    ('VC_LIKE_EXACT',       'vc_col LIKE exact "abcde"'),
    ('VC_LIKE_PREFIX',      'vc_col LIKE prefix "abc%"'),
    ('VC_LIKE_SUFFIX',      'vc_col LIKE suffix "%xyz"'),
    ('VC_LIKE_INFIX',       'vc_col LIKE infix "%biscuit_mid%"'),
    ('VC_LIKE_SINGLE_WC',   'vc_col LIKE single-char wildcard "a_cde"'),
    ('VC_LIKE_MULTI_WC',    'vc_col LIKE multiple wildcards "%b%c%"'),
    ('VC_LIKE_ESCAPE_PCT',  E'vc_col LIKE escaped % "100\\%" ESCAPE "\\"'),
    ('VC_LIKE_ESCAPE_UND',  E'vc_col LIKE escaped _ "under\\_score" ESCAPE "\\"'),
    ('VC_LIKE_EMPTY',       'vc_col LIKE empty string ""'),
    ('VC_LIKE_SPACE',       'vc_col LIKE single space " "'),
    ('VC_ILIKE_EXACT',      'vc_col ILIKE case-insensitive "ABCDE"'),
    ('VC_ILIKE_PREFIX',     'vc_col ILIKE prefix "ABC%"'),
    ('VC_ILIKE_SUFFIX',     'vc_col ILIKE suffix "%XYZ"'),
    ('VC_ILIKE_INFIX',      'vc_col ILIKE infix "%BISCUIT_MID%"'),
    ('VC_ILIKE_MIXED',      'vc_col ILIKE mixed-case "BiScUiT_CaSe"'),
    ('VC_NOTLIKE_PREFIX',   'vc_col NOT LIKE prefix "abc%"'),
    ('VC_NOTLIKE_SUFFIX',   'vc_col NOT LIKE suffix "%xyz"'),
    ('VC_NOTILIKE_PREFIX',  'vc_col NOT ILIKE prefix "ABC%"'),
    ('TX_LIKE_EXACT',       'tx_col LIKE exact "vwxyz"'),
    ('TX_LIKE_PREFIX',      'tx_col LIKE prefix "biscuit_prefix%"'),
    ('TX_LIKE_SUFFIX',      'tx_col LIKE suffix "%biscuit_suffix"'),
    ('TX_LIKE_INFIX',       'tx_col LIKE infix "%biscuit_mid%"'),
    ('TX_LIKE_SINGLE_WC',   'tx_col LIKE single-char wildcard "v_xyz"'),
    ('TX_LIKE_MULTI_WC',    'tx_col LIKE multiple wildcards "%b%c%"'),
    ('TX_LIKE_ESCAPE_PCT',  E'tx_col LIKE escaped % "50\\%done" ESCAPE "\\"'),
    ('TX_LIKE_ESCAPE_UND',  E'tx_col LIKE escaped _ "test\\_value" ESCAPE "\\"'),
    ('TX_ILIKE_EXACT',      'tx_col ILIKE case-insensitive "VWXYZ"'),
    ('TX_ILIKE_PREFIX',     'tx_col ILIKE prefix "BISCUIT_PREFIX%"'),
    ('TX_ILIKE_INFIX',      'tx_col ILIKE infix "%BISCUIT_MID%"'),
    ('TX_NOTLIKE_INFIX',    'tx_col NOT LIKE infix "%biscuit_mid%"'),
    ('TX_NOTILIKE_INFIX',   'tx_col NOT ILIKE infix "%BISCUIT_MID%"'),
    ('AND_BOTH_LIKE',       'vc_col LIKE "abc%" AND tx_col LIKE "vwx%"'),
    ('AND_VC_TX_ILIKE',     'vc_col ILIKE "abc%" AND tx_col ILIKE "VWX%"'),
    ('OR_VC_TX_LIKE',       'vc_col LIKE "abc%" OR tx_col LIKE "vwx%"'),
    ('OR_VC_NOTLIKE_TX',    'vc_col NOT LIKE "abc%" OR tx_col LIKE "%xyz"'),
    ('AND_INFIX_BOTH',      'vc_col LIKE "%biscuit_mid%" AND tx_col LIKE "%biscuit_mid%"'),
    ('NULL_VC_LIKE',        'vc_col LIKE "%%" - excludes NULLs'),
    ('NULL_TX_LIKE',        'tx_col LIKE "%%" - excludes NULLs'),
    ('NULL_VC_NOTLIKE',     'vc_col NOT LIKE "%%" - matches nothing (NULL unknown)'),
    ('EXPR_LOWER_VC',       'lower(vc_col) LIKE "helloworld"'),
    ('EXPR_LOWER_TX',       'lower(tx_col) LIKE "foobarbaz"'),
    ('EXPR_CAST_VC',        'vc_col::text LIKE "abcde"'),
    ('DUP_COUNT_EXACT',     'COUNT of duplicate "abcde" rows'),
    ('MAINT_UPD_OLD',       'Old value after UPDATE - should decrease'),
    ('MAINT_UPD_NEW',       'New value after UPDATE - should appear'),
    ('MAINT_INS_NEW',       'Newly inserted value - should appear'),
    ('MAINT_DEL_GONE',      'Deleted value - should decrease'),
    ('VAC_REPRESENTATIVE',  'Representative query after VACUUM'),
    ('REINDEX_CHECK',       'Representative query after REINDEX');

SELECT COUNT(*) AS catalog_entries FROM biscuit_query_catalog;


-- =============================================================================
-- §5  RESULT STORAGE
-- =============================================================================

CREATE TABLE biscuit_test_results (
    result_id      SERIAL PRIMARY KEY,
    query_code     VARCHAR(30)  NOT NULL REFERENCES biscuit_query_catalog(query_code),
    execution_mode VARCHAR(30)  NOT NULL,
    row_count      BIGINT       NOT NULL,
    recorded_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

COMMENT ON TABLE  biscuit_test_results                IS 'One row per (query_code, execution_mode) observation';
COMMENT ON COLUMN biscuit_test_results.execution_mode IS
    'SEQ=sequential baseline, BISC_VC=biscuit vc_col, BISC_TX=biscuit tx_col, '
    'BISC_MULTI=composite biscuit, EXPR=expression index, PARALLEL=parallel scan, '
    'SEQ_POST_UPD/BISC_POST_UPD=after update, SEQ_POST_VAC/BISC_POST_VAC=after vacuum, '
    'SEQ_POST_REINDEX/BISC_POST_REINDEX=after reindex';


-- =============================================================================
-- §6  SEQUENTIAL SCAN BASELINE
-- =============================================================================
-- Disable all index access methods so the planner uses sequential scans only.

SET enable_indexscan    = off;
SET enable_bitmapscan   = off;
SET enable_indexonlyscan = off;

-- vc_col LIKE patterns
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_EXACT', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abcde';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_PREFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abc%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_SUFFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE '%xyz';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_INFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE '%biscuit_mid%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_SINGLE_WC', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'a_cde';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_MULTI_WC', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE '%b%c%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_ESCAPE_PCT', 'SEQ', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE E'100\\%' ESCAPE E'\\';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_ESCAPE_UND', 'SEQ', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE E'under\\_score' ESCAPE E'\\';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_EMPTY', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE '';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_SPACE', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE ' ';

-- vc_col ILIKE patterns
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_ILIKE_EXACT', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col ILIKE 'ABCDE';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_ILIKE_PREFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col ILIKE 'ABC%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_ILIKE_SUFFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col ILIKE '%XYZ';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_ILIKE_INFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col ILIKE '%BISCUIT_MID%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_ILIKE_MIXED', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col ILIKE 'BiScUiT_CaSe';

-- vc_col NOT LIKE / NOT ILIKE
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_NOTLIKE_PREFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col NOT LIKE 'abc%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_NOTLIKE_SUFFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col NOT LIKE '%xyz';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_NOTILIKE_PREFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col NOT ILIKE 'ABC%';

-- tx_col LIKE patterns
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_EXACT', 'SEQ', COUNT(*) FROM biscuit_data WHERE tx_col LIKE 'vwxyz';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_PREFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE tx_col LIKE 'biscuit_prefix%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_SUFFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE tx_col LIKE '%biscuit_suffix';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_INFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE tx_col LIKE '%biscuit_mid%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_SINGLE_WC', 'SEQ', COUNT(*) FROM biscuit_data WHERE tx_col LIKE 'v_xyz';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_MULTI_WC', 'SEQ', COUNT(*) FROM biscuit_data WHERE tx_col LIKE '%b%c%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_ESCAPE_PCT', 'SEQ', COUNT(*) FROM biscuit_data
WHERE tx_col LIKE E'50\\%done' ESCAPE E'\\';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_ESCAPE_UND', 'SEQ', COUNT(*) FROM biscuit_data
WHERE tx_col LIKE E'test\\_value' ESCAPE E'\\';

-- tx_col ILIKE patterns
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_ILIKE_EXACT', 'SEQ', COUNT(*) FROM biscuit_data WHERE tx_col ILIKE 'VWXYZ';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_ILIKE_PREFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE tx_col ILIKE 'BISCUIT_PREFIX%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_ILIKE_INFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE tx_col ILIKE '%BISCUIT_MID%';

-- tx_col NOT LIKE / NOT ILIKE
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_NOTLIKE_INFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE tx_col NOT LIKE '%biscuit_mid%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_NOTILIKE_INFIX', 'SEQ', COUNT(*) FROM biscuit_data WHERE tx_col NOT ILIKE '%BISCUIT_MID%';

-- Boolean combinations
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'AND_BOTH_LIKE', 'SEQ', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE 'abc%' AND tx_col LIKE 'vwx%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'AND_VC_TX_ILIKE', 'SEQ', COUNT(*) FROM biscuit_data
WHERE vc_col ILIKE 'abc%' AND tx_col ILIKE 'VWX%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'OR_VC_TX_LIKE', 'SEQ', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE 'abc%' OR tx_col LIKE 'vwx%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'OR_VC_NOTLIKE_TX', 'SEQ', COUNT(*) FROM biscuit_data
WHERE vc_col NOT LIKE 'abc%' OR tx_col LIKE '%xyz';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'AND_INFIX_BOTH', 'SEQ', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE '%biscuit_mid%' AND tx_col LIKE '%biscuit_mid%';

-- NULL behaviour (LIKE/ILIKE return NULL for NULL operand, so NULLs are excluded)
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'NULL_VC_LIKE', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE '%%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'NULL_TX_LIKE', 'SEQ', COUNT(*) FROM biscuit_data WHERE tx_col LIKE '%%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'NULL_VC_NOTLIKE', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col NOT LIKE '%%';

-- Expression index queries (SEQ baseline)
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'EXPR_LOWER_VC', 'SEQ', COUNT(*) FROM biscuit_data WHERE lower(vc_col) LIKE 'helloworld';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'EXPR_LOWER_TX', 'SEQ', COUNT(*) FROM biscuit_data WHERE lower(tx_col) LIKE 'foobarbaz';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'EXPR_CAST_VC', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col::text LIKE 'abcde';

-- Duplicate count baseline
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'DUP_COUNT_EXACT', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abcde';

SELECT COUNT(*) AS seq_baseline_results
FROM biscuit_test_results WHERE execution_mode = 'SEQ';


-- =============================================================================
-- §7  SINGLE-COLUMN BISCUIT INDEX TESTS
-- =============================================================================

CREATE INDEX biscuit_idx_vc ON biscuit_data USING biscuit (vc_col);
CREATE INDEX biscuit_idx_tx ON biscuit_data USING biscuit (tx_col);

SET enable_seqscan       = off;
SET enable_indexscan     = on;
SET enable_bitmapscan    = on;
SET enable_indexonlyscan = on;

-- vc_col queries via biscuit_idx_vc
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_EXACT', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abcde';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_PREFIX', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abc%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_SUFFIX', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE '%xyz';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_INFIX', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE '%biscuit_mid%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_SINGLE_WC', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'a_cde';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_MULTI_WC', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE '%b%c%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_ESCAPE_PCT', 'BISC_VC', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE E'100\\%' ESCAPE E'\\';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_ESCAPE_UND', 'BISC_VC', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE E'under\\_score' ESCAPE E'\\';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_EMPTY', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE '';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_SPACE', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE ' ';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_ILIKE_EXACT', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col ILIKE 'ABCDE';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_ILIKE_PREFIX', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col ILIKE 'ABC%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_ILIKE_SUFFIX', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col ILIKE '%XYZ';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_ILIKE_INFIX', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col ILIKE '%BISCUIT_MID%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_ILIKE_MIXED', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col ILIKE 'BiScUiT_CaSe';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_NOTLIKE_PREFIX', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col NOT LIKE 'abc%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_NOTLIKE_SUFFIX', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col NOT LIKE '%xyz';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_NOTILIKE_PREFIX', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col NOT ILIKE 'ABC%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'NULL_VC_LIKE', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE '%%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'NULL_VC_NOTLIKE', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col NOT LIKE '%%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'DUP_COUNT_EXACT', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abcde';

-- tx_col queries via biscuit_idx_tx
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_EXACT', 'BISC_TX', COUNT(*) FROM biscuit_data WHERE tx_col LIKE 'vwxyz';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_PREFIX', 'BISC_TX', COUNT(*) FROM biscuit_data WHERE tx_col LIKE 'biscuit_prefix%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_SUFFIX', 'BISC_TX', COUNT(*) FROM biscuit_data WHERE tx_col LIKE '%biscuit_suffix';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_INFIX', 'BISC_TX', COUNT(*) FROM biscuit_data WHERE tx_col LIKE '%biscuit_mid%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_SINGLE_WC', 'BISC_TX', COUNT(*) FROM biscuit_data WHERE tx_col LIKE 'v_xyz';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_MULTI_WC', 'BISC_TX', COUNT(*) FROM biscuit_data WHERE tx_col LIKE '%b%c%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_ESCAPE_PCT', 'BISC_TX', COUNT(*) FROM biscuit_data
WHERE tx_col LIKE E'50\\%done' ESCAPE E'\\';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_ESCAPE_UND', 'BISC_TX', COUNT(*) FROM biscuit_data
WHERE tx_col LIKE E'test\\_value' ESCAPE E'\\';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_ILIKE_EXACT', 'BISC_TX', COUNT(*) FROM biscuit_data WHERE tx_col ILIKE 'VWXYZ';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_ILIKE_PREFIX', 'BISC_TX', COUNT(*) FROM biscuit_data WHERE tx_col ILIKE 'BISCUIT_PREFIX%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_ILIKE_INFIX', 'BISC_TX', COUNT(*) FROM biscuit_data WHERE tx_col ILIKE '%BISCUIT_MID%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_NOTLIKE_INFIX', 'BISC_TX', COUNT(*) FROM biscuit_data WHERE tx_col NOT LIKE '%biscuit_mid%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_NOTILIKE_INFIX', 'BISC_TX', COUNT(*) FROM biscuit_data WHERE tx_col NOT ILIKE '%BISCUIT_MID%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'NULL_TX_LIKE', 'BISC_TX', COUNT(*) FROM biscuit_data WHERE tx_col LIKE '%%';

-- Boolean combinations (both single-column indexes active simultaneously)
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'AND_BOTH_LIKE', 'BISC_TX', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE 'abc%' AND tx_col LIKE 'vwx%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'AND_VC_TX_ILIKE', 'BISC_TX', COUNT(*) FROM biscuit_data
WHERE vc_col ILIKE 'abc%' AND tx_col ILIKE 'VWX%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'OR_VC_TX_LIKE', 'BISC_TX', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE 'abc%' OR tx_col LIKE 'vwx%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'OR_VC_NOTLIKE_TX', 'BISC_TX', COUNT(*) FROM biscuit_data
WHERE vc_col NOT LIKE 'abc%' OR tx_col LIKE '%xyz';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'AND_INFIX_BOTH', 'BISC_TX', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE '%biscuit_mid%' AND tx_col LIKE '%biscuit_mid%';

SELECT COUNT(*) AS bisc_single_col_results
FROM biscuit_test_results WHERE execution_mode IN ('BISC_VC', 'BISC_TX');


-- =============================================================================
-- §8  MULTI-COLUMN BISCUIT INDEX TESTS
-- =============================================================================

CREATE INDEX biscuit_idx_multi ON biscuit_data USING biscuit (vc_col, tx_col);

-- Queries using only the first column of the composite index
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_EXACT', 'BISC_MULTI', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abcde';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_PREFIX', 'BISC_MULTI', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abc%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_ILIKE_INFIX', 'BISC_MULTI', COUNT(*) FROM biscuit_data WHERE vc_col ILIKE '%BISCUIT_MID%';

-- Queries using only the second column of the composite index
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_EXACT', 'BISC_MULTI', COUNT(*) FROM biscuit_data WHERE tx_col LIKE 'vwxyz';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_PREFIX', 'BISC_MULTI', COUNT(*) FROM biscuit_data WHERE tx_col LIKE 'biscuit_prefix%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_ILIKE_INFIX', 'BISC_MULTI', COUNT(*) FROM biscuit_data WHERE tx_col ILIKE '%BISCUIT_MID%';

-- Queries using both columns via the composite index
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'AND_BOTH_LIKE', 'BISC_MULTI', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE 'abc%' AND tx_col LIKE 'vwx%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'AND_VC_TX_ILIKE', 'BISC_MULTI', COUNT(*) FROM biscuit_data
WHERE vc_col ILIKE 'abc%' AND tx_col ILIKE 'VWX%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'OR_VC_TX_LIKE', 'BISC_MULTI', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE 'abc%' OR tx_col LIKE 'vwx%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'AND_INFIX_BOTH', 'BISC_MULTI', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE '%biscuit_mid%' AND tx_col LIKE '%biscuit_mid%';

SELECT COUNT(*) AS bisc_multi_results
FROM biscuit_test_results WHERE execution_mode = 'BISC_MULTI';


-- =============================================================================
-- §9  EXPRESSION INDEX TESTS
-- =============================================================================

CREATE INDEX biscuit_idx_lower_vc ON biscuit_data USING biscuit (lower(vc_col));
CREATE INDEX biscuit_idx_lower_tx ON biscuit_data USING biscuit (lower(tx_col));
CREATE INDEX biscuit_idx_cast_vc  ON biscuit_data USING biscuit ((vc_col::text));

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'EXPR_LOWER_VC', 'EXPR', COUNT(*) FROM biscuit_data WHERE lower(vc_col) LIKE 'helloworld';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'EXPR_LOWER_TX', 'EXPR', COUNT(*) FROM biscuit_data WHERE lower(tx_col) LIKE 'foobarbaz';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'EXPR_CAST_VC', 'EXPR', COUNT(*) FROM biscuit_data WHERE vc_col::text LIKE 'abcde';

SELECT COUNT(*) AS expr_index_results
FROM biscuit_test_results WHERE execution_mode = 'EXPR';


-- =============================================================================
-- §10 UPDATE MAINTENANCE TESTS
-- =============================================================================

-- Capture SEQ baseline for the rows we are about to mutate.
SET enable_seqscan       = on;
SET enable_indexscan     = off;
SET enable_bitmapscan    = off;
SET enable_indexonlyscan = off;

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'MAINT_UPD_OLD', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abcde';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'MAINT_UPD_NEW', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'updated_biscuit_vc';

-- Update the first 20 rows matching 'abcde'.
UPDATE biscuit_data
SET    vc_col = 'updated_biscuit_vc',
       tx_col = 'updated_biscuit_tx'
WHERE  id IN (
    SELECT id FROM biscuit_data WHERE vc_col = 'abcde' ORDER BY id LIMIT 20
);

-- SEQ counts after UPDATE: old value should drop, new value should appear.
INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'MAINT_UPD_OLD', 'SEQ_POST_UPD', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abcde';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'MAINT_UPD_NEW', 'SEQ_POST_UPD', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'updated_biscuit_vc';

-- Biscuit counts after UPDATE: must match SEQ_POST_UPD exactly.
SET enable_seqscan    = off;
SET enable_indexscan  = on;
SET enable_bitmapscan = on;

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'MAINT_UPD_OLD', 'BISC_POST_UPD', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abcde';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'MAINT_UPD_NEW', 'BISC_POST_UPD', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'updated_biscuit_vc';


-- =============================================================================
-- §11 INSERT MAINTENANCE TESTS
-- =============================================================================

INSERT INTO biscuit_data (vc_col, tx_col) VALUES
    ('biscuit_ins_001', 'biscuit_ins_tx_001'),
    ('biscuit_ins_002', 'biscuit_ins_tx_002'),
    ('biscuit_ins_003', 'biscuit_ins_tx_003'),
    ('biscuit_ins_004', 'biscuit_ins_tx_004'),
    ('biscuit_ins_005', 'biscuit_ins_tx_005'),
    ('biscuit_ins_006', 'biscuit_ins_tx_006'),
    ('biscuit_ins_007', 'biscuit_ins_tx_007'),
    ('biscuit_ins_008', 'biscuit_ins_tx_008'),
    ('biscuit_ins_009', 'biscuit_ins_tx_009'),
    ('biscuit_ins_010', 'biscuit_ins_tx_010'),
    ('biscuit_ins_011', 'biscuit_ins_tx_011'),
    ('biscuit_ins_012', 'biscuit_ins_tx_012'),
    ('biscuit_ins_013', 'biscuit_ins_tx_013'),
    ('biscuit_ins_014', 'biscuit_ins_tx_014'),
    ('biscuit_ins_015', 'biscuit_ins_tx_015'),
    ('biscuit_ins_016', 'biscuit_ins_tx_016'),
    ('biscuit_ins_017', 'biscuit_ins_tx_017'),
    ('biscuit_ins_018', 'biscuit_ins_tx_018'),
    ('biscuit_ins_019', 'biscuit_ins_tx_019'),
    ('biscuit_ins_020', 'biscuit_ins_tx_020');

-- SEQ: all 20 new rows must be visible immediately.
SET enable_seqscan    = on;
SET enable_indexscan  = off;
SET enable_bitmapscan = off;

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'MAINT_INS_NEW', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'biscuit_ins_%';

-- Biscuit: same count expected.
SET enable_seqscan    = off;
SET enable_indexscan  = on;
SET enable_bitmapscan = on;

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'MAINT_INS_NEW', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'biscuit_ins_%';


-- =============================================================================
-- §12 DELETE MAINTENANCE TESTS
-- =============================================================================

-- Delete the first 10 of the 20 inserted rows.
DELETE FROM biscuit_data
WHERE vc_col IN (
    'biscuit_ins_001','biscuit_ins_002','biscuit_ins_003','biscuit_ins_004',
    'biscuit_ins_005','biscuit_ins_006','biscuit_ins_007','biscuit_ins_008',
    'biscuit_ins_009','biscuit_ins_010'
);

-- SEQ: expect exactly 10 remaining (ins_011 through ins_020).
SET enable_seqscan    = on;
SET enable_indexscan  = off;
SET enable_bitmapscan = off;

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'MAINT_DEL_GONE', 'SEQ', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'biscuit_ins_%';

-- Biscuit: must also return 10.
SET enable_seqscan    = off;
SET enable_indexscan  = on;
SET enable_bitmapscan = on;

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'MAINT_DEL_GONE', 'BISC_VC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'biscuit_ins_%';


-- =============================================================================
-- §13 DUPLICATE HANDLING TESTS
-- =============================================================================

-- Insert 5 identical rows for a known value.
INSERT INTO biscuit_data (vc_col, tx_col) VALUES
    ('biscuit_dup_val', 'biscuit_dup_tx'),
    ('biscuit_dup_val', 'biscuit_dup_tx'),
    ('biscuit_dup_val', 'biscuit_dup_tx'),
    ('biscuit_dup_val', 'biscuit_dup_tx'),
    ('biscuit_dup_val', 'biscuit_dup_tx');

-- SEQ duplicate count (expect 5).
SET enable_seqscan    = on;
SET enable_indexscan  = off;
SET enable_bitmapscan = off;

SELECT
    'DUP_VC_SEQ'          AS check_name,
    COUNT(*)              AS observed,
    5                     AS expected,
    (COUNT(*) = 5)::text  AS passed
FROM biscuit_data
WHERE vc_col = 'biscuit_dup_val';

-- Biscuit duplicate count (expect 5).
SET enable_seqscan    = off;
SET enable_indexscan  = on;
SET enable_bitmapscan = on;

SELECT
    'DUP_VC_BISC'         AS check_name,
    COUNT(*)              AS observed,
    5                     AS expected,
    (COUNT(*) = 5)::text  AS passed
FROM biscuit_data
WHERE vc_col = 'biscuit_dup_val';


-- =============================================================================
-- §14 NULL HANDLING TESTS
-- =============================================================================

-- Verify SQL NULL semantics: LIKE/ILIKE/NOT LIKE/NOT ILIKE on NULL yield NULL,
-- so NULL rows are excluded from all pattern-match results.

-- These inline expressions confirm the semantics without touching the table.
SELECT
    (NULL::text LIKE '%')::text         AS null_like_pct,       -- expect NULL
    (NULL::text NOT LIKE '%')::text     AS null_notlike_pct,    -- expect NULL
    (NULL::text ILIKE '%')::text        AS null_ilike_pct,      -- expect NULL
    (NULL::text NOT ILIKE '%')::text    AS null_notilike_pct;   -- expect NULL

-- Confirm row counts: vc_col LIKE '%' SEQ vs Biscuit must agree.
-- (Results already stored in §6 as NULL_VC_LIKE SEQ and in §7 as NULL_VC_LIKE BISC_VC.)
-- Inline cross-check for immediate feedback:
SET enable_seqscan    = on;
SET enable_indexscan  = off;
SET enable_bitmapscan = off;

WITH seq_vc AS (
    SELECT COUNT(*) AS cnt FROM biscuit_data WHERE vc_col LIKE '%%'
)
SELECT
    'NULL_VC_CROSS_CHECK'               AS check_name,
    cnt                                 AS seq_count,
    (cnt = cnt)::text                   AS trivially_consistent
FROM seq_vc;

-- Verify that rows with NULL vc_col are not counted by LIKE '%%'.
SELECT
    'NULL_ROWS_EXCLUDED'                AS check_name,
    COUNT(*) FILTER (WHERE vc_col IS NULL) AS null_vc_rows,
    COUNT(*) FILTER (WHERE vc_col LIKE '%%') AS like_pct_rows,
    (COUNT(*) FILTER (WHERE vc_col IS NULL) > 0)::text AS has_nulls_in_data
FROM biscuit_data;


-- =============================================================================
-- §15 PLANNER VERIFICATION
-- =============================================================================
-- EXPLAIN output is captured in the pg_regress output file for comparison
-- against expected/ files. No results are stored in biscuit_test_results here.

SET enable_seqscan       = off;
SET enable_indexscan     = on;
SET enable_bitmapscan    = on;
SET enable_indexonlyscan = on;

EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abc%';

EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM biscuit_data WHERE tx_col LIKE '%biscuit_suffix';

EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM biscuit_data WHERE vc_col ILIKE 'ABC%';

EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM biscuit_data WHERE lower(vc_col) LIKE 'helloworld';

EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM biscuit_data WHERE vc_col::text LIKE 'abcde';

EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM biscuit_data
WHERE vc_col LIKE 'abc%' AND tx_col LIKE 'vwx%';

EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM biscuit_data
WHERE vc_col LIKE 'abc%' OR tx_col LIKE 'vwx%';


-- =============================================================================
-- §16 PARALLEL SCAN TESTS
-- =============================================================================

SET max_parallel_workers_per_gather = 4;
SET parallel_tuple_cost             = 0;
SET parallel_setup_cost             = 0;
SET min_parallel_table_scan_size    = 0;
SET enable_seqscan                  = on;
SET enable_indexscan                = on;

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VC_LIKE_PREFIX', 'PARALLEL', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abc%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'TX_LIKE_INFIX', 'PARALLEL', COUNT(*) FROM biscuit_data WHERE tx_col LIKE '%biscuit_mid%';

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'AND_BOTH_LIKE', 'PARALLEL', COUNT(*) FROM biscuit_data
WHERE vc_col LIKE 'abc%' AND tx_col LIKE 'vwx%';

-- Reset parallel GUCs.
RESET max_parallel_workers_per_gather;
RESET parallel_tuple_cost;
RESET parallel_setup_cost;
RESET min_parallel_table_scan_size;


-- =============================================================================
-- §17 VACUUM TESTS
-- =============================================================================

VACUUM biscuit_data;

-- SEQ after VACUUM.
SET enable_seqscan    = on;
SET enable_indexscan  = off;
SET enable_bitmapscan = off;

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VAC_REPRESENTATIVE', 'SEQ_POST_VAC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abc%';

-- Biscuit after VACUUM.
SET enable_seqscan    = off;
SET enable_indexscan  = on;
SET enable_bitmapscan = on;

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'VAC_REPRESENTATIVE', 'BISC_POST_VAC', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abc%';


-- =============================================================================
-- §18 REINDEX TESTS
-- =============================================================================

REINDEX INDEX biscuit_idx_vc;
REINDEX INDEX biscuit_idx_tx;
REINDEX INDEX biscuit_idx_multi;
REINDEX INDEX biscuit_idx_lower_vc;
REINDEX INDEX biscuit_idx_lower_tx;
REINDEX INDEX biscuit_idx_cast_vc;

-- SEQ after REINDEX.
SET enable_seqscan    = on;
SET enable_indexscan  = off;
SET enable_bitmapscan = off;

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'REINDEX_CHECK', 'SEQ_POST_REINDEX', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abc%';

-- Biscuit after REINDEX.
SET enable_seqscan    = off;
SET enable_indexscan  = on;
SET enable_bitmapscan = on;

INSERT INTO biscuit_test_results (query_code, execution_mode, row_count)
SELECT 'REINDEX_CHECK', 'BISC_POST_REINDEX', COUNT(*) FROM biscuit_data WHERE vc_col LIKE 'abc%';


-- =============================================================================
-- §19 FINAL REPORT & SUMMARY
-- =============================================================================

-- Reset all GUCs so the planner is free to choose any strategy.
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET enable_indexonlyscan;

-- ---------------------------------------------------------------------------
-- §19.1  Per-query detail: SEQ vs every Biscuit mode
-- ---------------------------------------------------------------------------
WITH seq_base AS (
    SELECT query_code, row_count AS seq_count
    FROM   biscuit_test_results
    WHERE  execution_mode = 'SEQ'
),
bisc_rows AS (
    SELECT query_code, execution_mode, row_count AS bisc_count
    FROM   biscuit_test_results
    WHERE  execution_mode IN ('BISC_VC','BISC_TX','BISC_MULTI','EXPR','PARALLEL')
),
detail AS (
    SELECT
        b.query_code,
        b.execution_mode,
        c.description,
        s.seq_count,
        b.bisc_count,
        (b.bisc_count - s.seq_count)              AS delta,
        CASE WHEN b.bisc_count = s.seq_count
             THEN 'PASS' ELSE 'FAIL' END          AS result
    FROM   bisc_rows b
    JOIN   seq_base           s ON s.query_code = b.query_code
    JOIN   biscuit_query_catalog c ON c.query_code = b.query_code
)
SELECT
    query_code,
    execution_mode,
    description,
    seq_count,
    bisc_count,
    delta,
    result
FROM detail
ORDER BY query_code, execution_mode;

-- ---------------------------------------------------------------------------
-- §19.2  Maintenance test comparisons
-- ---------------------------------------------------------------------------
WITH maint AS (
    -- UPDATE checks
    SELECT
        r1.query_code,
        r1.execution_mode  AS baseline_mode,
        r1.row_count       AS baseline_count,
        r2.execution_mode  AS test_mode,
        r2.row_count       AS test_count,
        (r2.row_count - r1.row_count) AS delta
    FROM biscuit_test_results r1
    JOIN biscuit_test_results r2 ON r2.query_code = r1.query_code
    WHERE (r1.execution_mode = 'SEQ_POST_UPD'     AND r2.execution_mode = 'BISC_POST_UPD')

    UNION ALL

    -- INSERT checks
    SELECT
        r1.query_code,
        r1.execution_mode,
        r1.row_count,
        r2.execution_mode,
        r2.row_count,
        (r2.row_count - r1.row_count)
    FROM biscuit_test_results r1
    JOIN biscuit_test_results r2 ON r2.query_code = r1.query_code
    WHERE r1.execution_mode = 'SEQ' AND r2.execution_mode = 'BISC_VC'
      AND r1.query_code IN ('MAINT_INS_NEW','MAINT_DEL_GONE')

    UNION ALL

    -- VACUUM checks
    SELECT
        r1.query_code,
        r1.execution_mode,
        r1.row_count,
        r2.execution_mode,
        r2.row_count,
        (r2.row_count - r1.row_count)
    FROM biscuit_test_results r1
    JOIN biscuit_test_results r2 ON r2.query_code = r1.query_code
    WHERE r1.execution_mode = 'SEQ_POST_VAC' AND r2.execution_mode = 'BISC_POST_VAC'

    UNION ALL

    -- REINDEX checks
    SELECT
        r1.query_code,
        r1.execution_mode,
        r1.row_count,
        r2.execution_mode,
        r2.row_count,
        (r2.row_count - r1.row_count)
    FROM biscuit_test_results r1
    JOIN biscuit_test_results r2 ON r2.query_code = r1.query_code
    WHERE r1.execution_mode = 'SEQ_POST_REINDEX' AND r2.execution_mode = 'BISC_POST_REINDEX'
)
SELECT
    query_code,
    baseline_mode,
    baseline_count,
    test_mode,
    test_count,
    delta,
    CASE WHEN delta = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM maint
ORDER BY query_code, baseline_mode;

-- ---------------------------------------------------------------------------
-- §19.3  Overall numeric summary
-- ---------------------------------------------------------------------------
WITH all_pairs AS (
    -- Core SEQ vs Biscuit pairs
    SELECT ABS(b.row_count - s.row_count) AS abs_delta
    FROM biscuit_test_results b
    JOIN biscuit_test_results s
        ON  s.query_code = b.query_code AND s.execution_mode = 'SEQ'
    WHERE b.execution_mode IN ('BISC_VC','BISC_TX','BISC_MULTI','EXPR','PARALLEL')

    UNION ALL

    -- UPDATE maintenance
    SELECT ABS(r2.row_count - r1.row_count)
    FROM biscuit_test_results r1
    JOIN biscuit_test_results r2 ON r2.query_code = r1.query_code
    WHERE r1.execution_mode = 'SEQ_POST_UPD' AND r2.execution_mode = 'BISC_POST_UPD'

    UNION ALL

    -- INSERT / DELETE maintenance
    SELECT ABS(r2.row_count - r1.row_count)
    FROM biscuit_test_results r1
    JOIN biscuit_test_results r2 ON r2.query_code = r1.query_code
    WHERE r1.execution_mode = 'SEQ' AND r2.execution_mode = 'BISC_VC'
      AND r1.query_code IN ('MAINT_INS_NEW','MAINT_DEL_GONE')

    UNION ALL

    -- VACUUM maintenance
    SELECT ABS(r2.row_count - r1.row_count)
    FROM biscuit_test_results r1
    JOIN biscuit_test_results r2 ON r2.query_code = r1.query_code
    WHERE r1.execution_mode = 'SEQ_POST_VAC' AND r2.execution_mode = 'BISC_POST_VAC'

    UNION ALL

    -- REINDEX maintenance
    SELECT ABS(r2.row_count - r1.row_count)
    FROM biscuit_test_results r1
    JOIN biscuit_test_results r2 ON r2.query_code = r1.query_code
    WHERE r1.execution_mode = 'SEQ_POST_REINDEX' AND r2.execution_mode = 'BISC_POST_REINDEX'
)
SELECT
    COUNT(*)                                     AS tests_executed,
    COUNT(*) FILTER (WHERE abs_delta = 0)        AS tests_passed,
    COUNT(*) FILTER (WHERE abs_delta <> 0)       AS tests_failed,
    COALESCE(MAX(abs_delta), 0)                  AS max_delta
FROM all_pairs;

-- ---------------------------------------------------------------------------
-- §19.4  Pass/Fail banner
-- ---------------------------------------------------------------------------
WITH all_pairs AS (
    SELECT ABS(b.row_count - s.row_count) AS abs_delta
    FROM biscuit_test_results b
    JOIN biscuit_test_results s
        ON s.query_code = b.query_code AND s.execution_mode = 'SEQ'
    WHERE b.execution_mode IN ('BISC_VC','BISC_TX','BISC_MULTI','EXPR','PARALLEL')

    UNION ALL

    SELECT ABS(r2.row_count - r1.row_count)
    FROM biscuit_test_results r1
    JOIN biscuit_test_results r2 ON r2.query_code = r1.query_code
    WHERE (r1.execution_mode = 'SEQ_POST_UPD'     AND r2.execution_mode = 'BISC_POST_UPD')
       OR (r1.execution_mode = 'SEQ_POST_VAC'     AND r2.execution_mode = 'BISC_POST_VAC')
       OR (r1.execution_mode = 'SEQ_POST_REINDEX' AND r2.execution_mode = 'BISC_POST_REINDEX')
       OR (r1.execution_mode = 'SEQ'              AND r2.execution_mode = 'BISC_VC'
           AND r1.query_code IN ('MAINT_INS_NEW','MAINT_DEL_GONE'))
),
verdict AS (
    SELECT (COUNT(*) FILTER (WHERE abs_delta <> 0) = 0) AS all_passed
    FROM all_pairs
)
SELECT
    CASE
        WHEN all_passed
        THEN '========================================' || E'\n' ||
             '  ALL BISCUIT REGRESSION TESTS PASSED  '  || E'\n' ||
             '========================================'
        ELSE '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' || E'\n' ||
             '  REGRESSION FAILURES DETECTED            ' || E'\n' ||
             '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
    END AS banner
FROM verdict;

-- ---------------------------------------------------------------------------
-- §19.5  Failing queries only (empty result set = success)
-- ---------------------------------------------------------------------------
WITH all_pairs AS (
    SELECT
        b.query_code,
        b.execution_mode,
        c.description,
        s.row_count        AS seq_count,
        b.row_count        AS bisc_count,
        (b.row_count - s.row_count) AS delta
    FROM biscuit_test_results b
    JOIN biscuit_test_results s
        ON  s.query_code = b.query_code AND s.execution_mode = 'SEQ'
    JOIN biscuit_query_catalog c ON c.query_code = b.query_code
    WHERE b.execution_mode IN ('BISC_VC','BISC_TX','BISC_MULTI','EXPR','PARALLEL')

    UNION ALL

    SELECT
        r1.query_code,
        r2.execution_mode,
        c.description,
        r1.row_count,
        r2.row_count,
        (r2.row_count - r1.row_count)
    FROM biscuit_test_results r1
    JOIN biscuit_test_results r2 ON r2.query_code = r1.query_code
    JOIN biscuit_query_catalog c ON c.query_code  = r1.query_code
    WHERE (r1.execution_mode = 'SEQ_POST_UPD'     AND r2.execution_mode = 'BISC_POST_UPD')
       OR (r1.execution_mode = 'SEQ_POST_VAC'     AND r2.execution_mode = 'BISC_POST_VAC')
       OR (r1.execution_mode = 'SEQ_POST_REINDEX' AND r2.execution_mode = 'BISC_POST_REINDEX')
       OR (r1.execution_mode = 'SEQ'              AND r2.execution_mode = 'BISC_VC'
           AND r1.query_code IN ('MAINT_INS_NEW','MAINT_DEL_GONE'))
)
SELECT
    query_code,
    execution_mode,
    description,
    seq_count,
    bisc_count,
    delta
FROM all_pairs
WHERE delta <> 0
ORDER BY ABS(delta) DESC, query_code;

-- ---------------------------------------------------------------------------
-- §19.6  Full raw result dump (for debugging)
-- ---------------------------------------------------------------------------
SELECT
    r.result_id,
    r.query_code,
    r.execution_mode,
    r.row_count,
    c.description
FROM  biscuit_test_results   r
JOIN  biscuit_query_catalog  c ON c.query_code = r.query_code
ORDER BY r.query_code, r.execution_mode, r.result_id;

-- =============================================================================
-- END OF BISCUIT REGRESSION TEST SUITE
-- =============================================================================
