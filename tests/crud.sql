-- =============================================================================
-- BISCUIT INDEX — CRUD MAINTENANCE TEST
-- =============================================================================
-- Verifies that a Biscuit index correctly reflects INSERT, UPDATE, and DELETE
-- operations by comparing sequential-scan counts against index-scan counts
-- after every mutation step.
--
-- Structure
--   §1  Schema setup
--   §2  Seed data (25 rows)
--   §3  Build Biscuit index
--   §4  Baseline verification (SEQ == BISC before any mutation)
--   §5  INSERT 5 rows  — verify index picks them up
--   §6  UPDATE 5 rows  — verify old values vanish, new values appear
--   §7  DELETE 10 rows — verify deleted rows disappear
--   §8  Final summary
-- =============================================================================


-- =============================================================================
-- §1  SCHEMA SETUP
-- =============================================================================

DROP TABLE IF EXISTS biscuit_crud_results CASCADE;
DROP TABLE IF EXISTS biscuit_crud_data    CASCADE;

-- Test data table.
CREATE TABLE biscuit_crud_data (
    id    SERIAL PRIMARY KEY,
    label TEXT NOT NULL
);

-- Results table: one row per (phase, execution_mode) pair.
-- phase         — human-readable stage name
-- execution_mode — SEQ or BISC
-- pattern       — the LIKE pattern used
-- row_count     — COUNT(*) result
CREATE TABLE biscuit_crud_results (
    result_id      SERIAL PRIMARY KEY,
    phase          TEXT   NOT NULL,
    execution_mode TEXT   NOT NULL,   -- 'SEQ' or 'BISC'
    pattern        TEXT   NOT NULL,
    row_count      BIGINT NOT NULL
);


-- =============================================================================
-- §2  SEED DATA — 25 rows
-- =============================================================================
-- Rows fall into five named groups so we have predictable patterns to search.
--   group_alpha_*   (rows  1-5)
--   group_beta_*    (rows  6-10)
--   group_gamma_*   (rows 11-15)
--   group_delta_*   (rows 16-20)
--   group_epsilon_* (rows 21-25)

INSERT INTO biscuit_crud_data (label) VALUES
    ('group_alpha_001'),
    ('group_alpha_002'),
    ('group_alpha_003'),
    ('group_alpha_004'),
    ('group_alpha_005'),
    ('group_beta_001'),
    ('group_beta_002'),
    ('group_beta_003'),
    ('group_beta_004'),
    ('group_beta_005'),
    ('group_gamma_001'),
    ('group_gamma_002'),
    ('group_gamma_003'),
    ('group_gamma_004'),
    ('group_gamma_005'),
    ('group_delta_001'),
    ('group_delta_002'),
    ('group_delta_003'),
    ('group_delta_004'),
    ('group_delta_005'),
    ('group_epsilon_001'),
    ('group_epsilon_002'),
    ('group_epsilon_003'),
    ('group_epsilon_004'),
    ('group_epsilon_005');

-- Confirm exactly 25 rows were inserted.
SELECT COUNT(*) AS seeded_rows FROM biscuit_crud_data;


-- =============================================================================
-- §3  BUILD BISCUIT INDEX
-- =============================================================================

CREATE INDEX biscuit_crud_idx ON biscuit_crud_data USING biscuit (label);


-- =============================================================================
-- §4  BASELINE VERIFICATION  (before any mutation)
-- =============================================================================
-- Every SEQ count must equal the corresponding BISC count.
-- We check each group prefix individually and the wildcard '%group_%'.

-- ── SEQ baseline ─────────────────────────────────────────────────────────────
SET enable_seqscan    = on;
SET enable_indexscan  = off;
SET enable_bitmapscan = off;

INSERT INTO biscuit_crud_results (phase, execution_mode, pattern, row_count)
SELECT 'baseline', 'SEQ', 'group_alpha_%',   COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_alpha_%'   UNION ALL
SELECT 'baseline', 'SEQ', 'group_beta_%',    COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_beta_%'    UNION ALL
SELECT 'baseline', 'SEQ', 'group_gamma_%',   COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_gamma_%'   UNION ALL
SELECT 'baseline', 'SEQ', 'group_delta_%',   COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_delta_%'   UNION ALL
SELECT 'baseline', 'SEQ', 'group_epsilon_%', COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_epsilon_%' UNION ALL
SELECT 'baseline', 'SEQ', '%group_%',        COUNT(*) FROM biscuit_crud_data WHERE label LIKE '%group_%';

-- ── BISC baseline ─────────────────────────────────────────────────────────────
SET enable_seqscan    = off;
SET enable_indexscan  = on;
SET enable_bitmapscan = on;

INSERT INTO biscuit_crud_results (phase, execution_mode, pattern, row_count)
SELECT 'baseline', 'BISC', 'group_alpha_%',   COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_alpha_%'   UNION ALL
SELECT 'baseline', 'BISC', 'group_beta_%',    COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_beta_%'    UNION ALL
SELECT 'baseline', 'BISC', 'group_gamma_%',   COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_gamma_%'   UNION ALL
SELECT 'baseline', 'BISC', 'group_delta_%',   COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_delta_%'   UNION ALL
SELECT 'baseline', 'BISC', 'group_epsilon_%', COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_epsilon_%' UNION ALL
SELECT 'baseline', 'BISC', '%group_%',        COUNT(*) FROM biscuit_crud_data WHERE label LIKE '%group_%';

-- Quick inline check: all 25 rows visible via both paths.
SET enable_seqscan    = on;
SET enable_indexscan  = off;
SET enable_bitmapscan = off;
SELECT 'baseline_seq_total' AS check_name, COUNT(*) AS expected_25 FROM biscuit_crud_data;

SET enable_seqscan    = off;
SET enable_indexscan  = on;
SET enable_bitmapscan = on;
SELECT 'baseline_bisc_total' AS check_name, COUNT(*) AS expected_25 FROM biscuit_crud_data;


-- =============================================================================
-- §5  INSERT 5 NEW ROWS
-- =============================================================================
-- Insert into a new group 'group_zeta_*' so the pattern 'group_zeta_%'
-- has an unambiguous expected count of exactly 5 after this step.

INSERT INTO biscuit_crud_data (label) VALUES
    ('group_zeta_001'),
    ('group_zeta_002'),
    ('group_zeta_003'),
    ('group_zeta_004'),
    ('group_zeta_005');

-- ── SEQ after INSERT ──────────────────────────────────────────────────────────
SET enable_seqscan    = on;
SET enable_indexscan  = off;
SET enable_bitmapscan = off;

INSERT INTO biscuit_crud_results (phase, execution_mode, pattern, row_count)
SELECT 'post_insert', 'SEQ', 'group_zeta_%', COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_zeta_%' UNION ALL
SELECT 'post_insert', 'SEQ', '%group_%',     COUNT(*) FROM biscuit_crud_data WHERE label LIKE '%group_%';

-- ── BISC after INSERT ─────────────────────────────────────────────────────────
SET enable_seqscan    = off;
SET enable_indexscan  = on;
SET enable_bitmapscan = on;

INSERT INTO biscuit_crud_results (phase, execution_mode, pattern, row_count)
SELECT 'post_insert', 'BISC', 'group_zeta_%', COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_zeta_%' UNION ALL
SELECT 'post_insert', 'BISC', '%group_%',     COUNT(*) FROM biscuit_crud_data WHERE label LIKE '%group_%';

-- Inline spot-check: zeta group must have exactly 5 rows via BISC.
SELECT
    'insert_spot_check'                   AS check_name,
    COUNT(*)                              AS observed,
    5                                     AS expected,
    (COUNT(*) = 5)::text                  AS passed
FROM biscuit_crud_data
WHERE label LIKE 'group_zeta_%';


-- =============================================================================
-- §6  UPDATE 5 ROWS
-- =============================================================================
-- Rename the 5 group_alpha rows: 'group_alpha_*' → 'group_alpha_updated_*'.
-- After the update:
--   'group_alpha_%'         should still return 5  (prefix still matches)
--   'group_alpha_updated_%' should return 5         (new infix appears)
--   '%_updated_%'           should return 5         (general infix check)

UPDATE biscuit_crud_data
SET    label = replace(label, 'group_alpha_', 'group_alpha_updated_')
WHERE  label LIKE 'group_alpha_%';

-- ── SEQ after UPDATE ──────────────────────────────────────────────────────────
SET enable_seqscan    = on;
SET enable_indexscan  = off;
SET enable_bitmapscan = off;

INSERT INTO biscuit_crud_results (phase, execution_mode, pattern, row_count)
SELECT 'post_update', 'SEQ', 'group_alpha_%',         COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_alpha_%'         UNION ALL
SELECT 'post_update', 'SEQ', 'group_alpha_updated_%', COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_alpha_updated_%' UNION ALL
SELECT 'post_update', 'SEQ', '%_updated_%',           COUNT(*) FROM biscuit_crud_data WHERE label LIKE '%_updated_%'           UNION ALL
SELECT 'post_update', 'SEQ', '%group_%',              COUNT(*) FROM biscuit_crud_data WHERE label LIKE '%group_%';

-- ── BISC after UPDATE ─────────────────────────────────────────────────────────
SET enable_seqscan    = off;
SET enable_indexscan  = on;
SET enable_bitmapscan = on;

INSERT INTO biscuit_crud_results (phase, execution_mode, pattern, row_count)
SELECT 'post_update', 'BISC', 'group_alpha_%',         COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_alpha_%'         UNION ALL
SELECT 'post_update', 'BISC', 'group_alpha_updated_%', COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_alpha_updated_%' UNION ALL
SELECT 'post_update', 'BISC', '%_updated_%',           COUNT(*) FROM biscuit_crud_data WHERE label LIKE '%_updated_%'           UNION ALL
SELECT 'post_update', 'BISC', '%group_%',              COUNT(*) FROM biscuit_crud_data WHERE label LIKE '%group_%';

-- Inline spot-checks.
SELECT
    'update_new_label_check'              AS check_name,
    COUNT(*)                              AS observed,
    5                                     AS expected,
    (COUNT(*) = 5)::text                  AS passed
FROM biscuit_crud_data
WHERE label LIKE 'group_alpha_updated_%';

SELECT
    'update_old_exact_gone_check'         AS check_name,
    COUNT(*)                              AS observed,
    0                                     AS expected,
    (COUNT(*) = 0)::text                  AS passed
FROM biscuit_crud_data
WHERE label = 'group_alpha_001';   -- original exact value must be gone


-- =============================================================================
-- §7  DELETE 10 ROWS
-- =============================================================================
-- Delete all 5 group_beta rows + all 5 group_gamma rows (10 total).
-- After deletion:
--   'group_beta_%'  must return 0
--   'group_gamma_%' must return 0
--   '%group_%'      must drop by exactly 10

DELETE FROM biscuit_crud_data
WHERE label LIKE 'group_beta_%' OR label LIKE 'group_gamma_%';

-- ── SEQ after DELETE ──────────────────────────────────────────────────────────
SET enable_seqscan    = on;
SET enable_indexscan  = off;
SET enable_bitmapscan = off;

INSERT INTO biscuit_crud_results (phase, execution_mode, pattern, row_count)
SELECT 'post_delete', 'SEQ', 'group_beta_%',  COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_beta_%'  UNION ALL
SELECT 'post_delete', 'SEQ', 'group_gamma_%', COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_gamma_%' UNION ALL
SELECT 'post_delete', 'SEQ', '%group_%',      COUNT(*) FROM biscuit_crud_data WHERE label LIKE '%group_%';

-- ── BISC after DELETE ─────────────────────────────────────────────────────────
SET enable_seqscan    = off;
SET enable_indexscan  = on;
SET enable_bitmapscan = on;

INSERT INTO biscuit_crud_results (phase, execution_mode, pattern, row_count)
SELECT 'post_delete', 'BISC', 'group_beta_%',  COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_beta_%'  UNION ALL
SELECT 'post_delete', 'BISC', 'group_gamma_%', COUNT(*) FROM biscuit_crud_data WHERE label LIKE 'group_gamma_%' UNION ALL
SELECT 'post_delete', 'BISC', '%group_%',      COUNT(*) FROM biscuit_crud_data WHERE label LIKE '%group_%';

-- Inline spot-checks.
SELECT
    'delete_beta_gone_check'              AS check_name,
    COUNT(*)                              AS observed,
    0                                     AS expected,
    (COUNT(*) = 0)::text                  AS passed
FROM biscuit_crud_data
WHERE label LIKE 'group_beta_%';

SELECT
    'delete_gamma_gone_check'             AS check_name,
    COUNT(*)                              AS observed,
    0                                     AS expected,
    (COUNT(*) = 0)::text                  AS passed
FROM biscuit_crud_data
WHERE label LIKE 'group_gamma_%';

-- Surviving rows: delta(25) + zeta(5) + alpha_updated(5) + epsilon(5) = 20.
SET enable_seqscan    = on;
SET enable_indexscan  = off;
SET enable_bitmapscan = off;
SELECT 'final_total_seq'  AS check_name, COUNT(*) AS expected_20 FROM biscuit_crud_data;

SET enable_seqscan    = off;
SET enable_indexscan  = on;
SET enable_bitmapscan = on;
SELECT 'final_total_bisc' AS check_name, COUNT(*) AS expected_20 FROM biscuit_crud_data;


-- =============================================================================
-- §8  FINAL SUMMARY
-- =============================================================================

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

-- ---------------------------------------------------------------------------
-- §8.1  Full comparison table: SEQ vs BISC per phase and pattern
-- ---------------------------------------------------------------------------
WITH seq_side AS (
    SELECT phase, pattern, row_count AS seq_count
    FROM   biscuit_crud_results
    WHERE  execution_mode = 'SEQ'
),
bisc_side AS (
    SELECT phase, pattern, row_count AS bisc_count
    FROM   biscuit_crud_results
    WHERE  execution_mode = 'BISC'
)
SELECT
    s.phase,
    s.pattern,
    s.seq_count,
    b.bisc_count,
    (b.bisc_count - s.seq_count)              AS delta,
    CASE WHEN s.seq_count = b.bisc_count
         THEN 'PASS' ELSE 'FAIL' END          AS result
FROM   seq_side  s
JOIN   bisc_side b USING (phase, pattern)
ORDER BY
    CASE s.phase
        WHEN 'baseline'    THEN 1
        WHEN 'post_insert' THEN 2
        WHEN 'post_update' THEN 3
        WHEN 'post_delete' THEN 4
    END,
    s.pattern;

-- ---------------------------------------------------------------------------
-- §8.2  Numeric summary
-- ---------------------------------------------------------------------------
WITH pairs AS (
    SELECT
        s.phase,
        s.pattern,
        s.row_count                           AS seq_count,
        b.row_count                           AS bisc_count,
        ABS(b.row_count - s.row_count)        AS abs_delta
    FROM biscuit_crud_results s
    JOIN biscuit_crud_results b
        ON  b.phase          = s.phase
        AND b.pattern        = s.pattern
        AND b.execution_mode = 'BISC'
    WHERE s.execution_mode = 'SEQ'
)
SELECT
    COUNT(*)                                  AS comparisons_run,
    COUNT(*) FILTER (WHERE abs_delta = 0)     AS passed,
    COUNT(*) FILTER (WHERE abs_delta <> 0)    AS failed,
    MAX(abs_delta)                            AS max_delta
FROM pairs;

-- ---------------------------------------------------------------------------
-- §8.3  Pass / Fail banner
-- ---------------------------------------------------------------------------
WITH pairs AS (
    SELECT ABS(b.row_count - s.row_count) AS abs_delta
    FROM biscuit_crud_results s
    JOIN biscuit_crud_results b
        ON  b.phase          = s.phase
        AND b.pattern        = s.pattern
        AND b.execution_mode = 'BISC'
    WHERE s.execution_mode = 'SEQ'
),
verdict AS (
    SELECT (COUNT(*) FILTER (WHERE abs_delta <> 0) = 0) AS all_passed
    FROM pairs
)
SELECT
    CASE
        WHEN all_passed
        THEN '============================================' || E'\n' ||
             '  ALL BISCUIT CRUD MAINTENANCE TESTS PASSED' || E'\n' ||
             '============================================'
        ELSE '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' || E'\n' ||
             '  BISCUIT CRUD MAINTENANCE FAILURES DETECTED' || E'\n' ||
             '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
    END AS banner
FROM verdict;

-- ---------------------------------------------------------------------------
-- §8.4  Failing rows only (empty = all passed)
-- ---------------------------------------------------------------------------
WITH pairs AS (
    SELECT
        s.phase,
        s.pattern,
        s.row_count  AS seq_count,
        b.row_count  AS bisc_count,
        (b.row_count - s.row_count) AS delta
    FROM biscuit_crud_results s
    JOIN biscuit_crud_results b
        ON  b.phase          = s.phase
        AND b.pattern        = s.pattern
        AND b.execution_mode = 'BISC'
    WHERE s.execution_mode = 'SEQ'
)
SELECT phase, pattern, seq_count, bisc_count, delta
FROM   pairs
WHERE  delta <> 0
ORDER  BY ABS(delta) DESC;

-- =============================================================================
-- END OF BISCUIT CRUD MAINTENANCE TEST
-- =============================================================================
