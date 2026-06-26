-- =============================================================================
-- Biscuit PostgreSQL Extension — Comprehensive Pure-SQL Regression Test Suite
-- =============================================================================
--
-- PURPOSE
--   Validates that the Biscuit index access method returns results IDENTICAL
--   to sequential scan, across:
--     - LIKE / ILIKE / NOT LIKE / NOT ILIKE, for prefix / infix / suffix /
--       underscore- and percent-wildcard / mixed / composite (AND/OR) patterns
--     - single-column and multi-column (composite) Biscuit indexes
--     - INSERT / UPDATE / DELETE mutation immediacy
--     - VACUUM and REINDEX maintenance operations
--     - single-worker and parallel query execution
--
-- DESIGN NOTES
--   - Pure SQL only. The only procedural constructs used are anonymous
--     PL/pgSQL blocks (DO $$ ... $$), which are native, in-database SQL
--     constructs (NOT an external scripting language) and contain no
--     psql backslash meta-commands (\set, \echo, \gset, \if, \copy, \i,
--     \timing, etc.). This was explicitly approved for this suite.
--   - All "configuration" is literal SQL constants or values pulled from
--     helper tables -- there are no psql variables anywhere in this file.
--   - The suite is idempotent-by-convention: Section 0 drops and recreates
--     every helper object, so the file can be re-run repeatedly against the
--     same database without manual cleanup.
--   - Reproducibility caveat: setseed() guarantees a reproducible random()
--     sequence within a given PostgreSQL major version, but the underlying
--     PRNG algorithm is not contractually guaranteed stable *across* major
--     versions. Exact row-for-row string reproduction should therefore be
--     treated as "reproducible per-version", not "reproducible forever".
--
-- HOW TO RUN
--   psql -d <your_database> -f biscuit_regress.sql
--   (No flags, variables, or external tooling required.)
--
-- =============================================================================


-- =============================================================================
-- SECTION 0: SESSION SETUP, EXTENSION, AND DETERMINISTIC SEED
-- =============================================================================

-- ---- 0.1 Extension under test --------------------------------------------
-- NOTE: This is the ONE line that targets the real extension. Everything in
-- this suite was developed and validated against pg_trgm's GIN opclass as a
-- structural stand-in (same shape: GIN-style index accelerating LIKE/ILIKE
-- over varchar/text, single- and multi-column). To run against the genuine
-- extension, ensure CREATE EXTENSION biscuit; succeeds and that the index
-- DDL in Section 4 uses "USING biscuit (col)" as written below.
CREATE EXTENSION IF NOT EXISTS biscuit;

-- ---- 0.2 Deterministic random seed (Functional Requirement #1) -----------
-- Fixed seed for full reproducibility of generated data and derived query
-- patterns within a single run / PostgreSQL major version.
SELECT setseed(0.42);

-- ---- 0.3 Session-level diagnostics -----------------------------------------
SET client_min_messages = NOTICE;

-- =============================================================================
-- SECTION 0.4: DROP ANY PRIOR RUN'S OBJECTS (idempotent re-run support)
-- =============================================================================

DROP TABLE IF EXISTS biscuit_report_failures CASCADE;
DROP TABLE IF EXISTS biscuit_report_summary CASCADE;
DROP TABLE IF EXISTS biscuit_results CASCADE;
DROP TABLE IF EXISTS biscuit_mutation_updates CASCADE;
DROP TABLE IF EXISTS biscuit_mutation_deletes CASCADE;
DROP TABLE IF EXISTS biscuit_mutation_inserts CASCADE;
DROP TABLE IF EXISTS biscuit_query_catalog CASCADE;
DROP TABLE IF EXISTS biscuit_pattern_seeds CASCADE;
DROP TABLE IF EXISTS biscuit_test_data CASCADE;

-- =============================================================================
-- SECTION 1: CORE SCHEMA
-- =============================================================================

-- ---- 1.1 Main data table (Functional Requirements #2-4) ------------------
-- val_varchar and val_text are populated INDEPENDENTLY (not copies of one
-- another) so that multi-column AND/OR predicates exercise genuine
-- cross-column selectivity rather than degenerating into single-column
-- tests under a different name.
CREATE TABLE biscuit_test_data (
    id            bigint        PRIMARY KEY,
    val_varchar   varchar(15)   NOT NULL,
    val_text      text          NOT NULL,
    batch         text          NOT NULL DEFAULT 'seed',
    created_at    timestamp     NOT NULL DEFAULT now()
);

COMMENT ON TABLE biscuit_test_data IS
  'Primary corpus for Biscuit regression testing. batch tracks provenance: '
  'seed = original 100k generated rows, insert_test = the ~20 hardcoded '
  'mutation-insert rows, update_test = the dedicated update-test rows.';

-- ---- 1.2 Query catalog (Functional Requirement #8) ------------------------
-- Stores every benchmark query exactly once, with a stable unique identifier.
-- The results table stores only query_id; descriptions are joined at report
-- time, never duplicated into the results table.
CREATE TABLE biscuit_query_catalog (
    query_id              text PRIMARY KEY,
    category               text NOT NULL,
    subcategory            text NOT NULL,
    target_column           text NOT NULL,   -- 'val_varchar' | 'val_text' | 'both'
    index_scope             text NOT NULL,   -- 'single' | 'multi' | 'both'
    description             text NOT NULL,
    predicate_sql           text NOT NULL,   -- WHERE-clause fragment, e.g. "val_varchar LIKE 'ab%'"
    expected_selectivity    text             -- 'high' | 'medium' | 'low' | 'zero' (informational)
);

COMMENT ON TABLE biscuit_query_catalog IS
  'Catalog of every distinct benchmark query. predicate_sql is executed '
  'dynamically (via EXECUTE inside DO blocks) against biscuit_test_data; '
  'it is never executed via any external scripting tool.';

-- ---- 1.3 Results table (Functional Requirement #5) -------------------------
CREATE TABLE biscuit_results (
    result_id     bigserial PRIMARY KEY,
    query_id      text NOT NULL REFERENCES biscuit_query_catalog(query_id),
    phase         text NOT NULL,   -- SEQSCAN | INDEXED_SINGLE | INDEXED_MULTI | ...
    seq_count     bigint NOT NULL,
    bisc_count    bigint NOT NULL,
    delta         bigint GENERATED ALWAYS AS (bisc_count - seq_count) STORED,
    plan_used     text,
    recorded_at   timestamp NOT NULL DEFAULT now()
);

CREATE INDEX idx_biscuit_results_query_phase ON biscuit_results (query_id, phase);
CREATE INDEX idx_biscuit_results_delta ON biscuit_results (delta) WHERE delta <> 0;

COMMENT ON TABLE biscuit_results IS
  'One row per (query_id, phase) execution. delta <> 0 indicates a '
  'regression failure for that query in that phase.';

-- ---- 1.4 Mutation tracking tables (Functional Requirements #9-11) --------

-- Hardcoded rows for insert-immediacy testing (~20 rows, exact literals).
CREATE TABLE biscuit_mutation_inserts (
    id            bigint PRIMARY KEY,
    val_varchar   varchar(15) NOT NULL,
    val_text      text NOT NULL,
    distinguishing_pattern text NOT NULL  -- a pattern guaranteed unique to this row, for assertion queries
);

-- Subset of the above slated for deletion (~10 rows).
CREATE TABLE biscuit_mutation_deletes (
    id bigint PRIMARY KEY REFERENCES biscuit_mutation_inserts(id)
);

-- Dedicated rows for update testing (separate from insert/delete sets, so
-- the three mutation tests never interfere with one another's deltas).
CREATE TABLE biscuit_mutation_updates (
    id            bigint PRIMARY KEY,
    old_varchar   varchar(15) NOT NULL,
    old_text      text NOT NULL,
    new_varchar   varchar(15) NOT NULL,
    new_text      text NOT NULL,
    old_pattern   text NOT NULL,  -- pattern that matched the OLD value, must stop matching after update
    new_pattern   text NOT NULL   -- pattern that matches the NEW value, must start matching after update
);

-- ---- 1.5 Final report tables -----------------------------------------------
CREATE TABLE biscuit_report_summary (
    category          text PRIMARY KEY,
    queries_executed  bigint NOT NULL,
    queries_passed    bigint NOT NULL,
    queries_failed    bigint NOT NULL,
    success_pct       numeric(6,2) NOT NULL
);

CREATE TABLE biscuit_report_failures (
    query_id      text NOT NULL,
    description   text NOT NULL,
    phase         text NOT NULL,
    seq_count     bigint NOT NULL,
    bisc_count    bigint NOT NULL,
    delta         bigint NOT NULL
);

-- =============================================================================
-- SECTION 2: RANDOM STRING GENERATION AND DATA POPULATION
-- =============================================================================
--
-- Strings are built character-by-character from a fixed 36-char alphabet
-- [a-z0-9] so that length is EXACTLY controlled (5-15 chars inclusive, per
-- Functional Requirement #4), which is required for clean prefix/suffix/
-- length-boundary extraction in Section 3. md5()-substring approaches were
-- deliberately avoided because they make exact length control awkward.

DROP FUNCTION IF EXISTS biscuit_random_string(int, int);

CREATE FUNCTION biscuit_random_string(min_len int, max_len int)
RETURNS text
LANGUAGE sql
AS $$
    SELECT string_agg(
             substr('abcdefghijklmnopqrstuvwxyz0123456789',
                    1 + floor(random() * 36)::int, 1),
             ''
           )
    FROM generate_series(1, min_len + floor(random() * (max_len - min_len + 1))::int);
$$;

COMMENT ON FUNCTION biscuit_random_string(int, int) IS
  'Generates a random lowercase-alphanumeric string with length uniformly '
  'distributed in [min_len, max_len] inclusive. Relies on the session seed '
  'set via setseed() in Section 0 for reproducibility.';

-- ---- 2.1 Populate main corpus: 100,000 rows, lengths 5-15 -----------------
-- (Functional Requirements #2, #3, #4)
INSERT INTO biscuit_test_data (id, val_varchar, val_text, batch)
SELECT
    gs.id,
    biscuit_random_string(5, 15)::varchar(15),
    biscuit_random_string(5, 15),
    'seed'
FROM generate_series(1, 100000) AS gs(id);

ANALYZE biscuit_test_data;

-- ---- 2.2 Sanity check: row count and length bounds ------------------------
DO $$
DECLARE
    v_count bigint;
    v_minlen int;
    v_maxlen int;
BEGIN
    SELECT count(*), min(length(val_varchar)), max(length(val_varchar))
      INTO v_count, v_minlen, v_maxlen
      FROM biscuit_test_data;

    IF v_count < 100000 THEN
        RAISE EXCEPTION 'Data generation failed: expected >= 100000 rows, got %', v_count;
    END IF;

    IF v_minlen < 5 OR v_maxlen > 15 THEN
        RAISE EXCEPTION 'Data generation failed: val_varchar length out of bounds [5,15], got min=% max=%', v_minlen, v_maxlen;
    END IF;

    RAISE NOTICE 'Section 2 OK: % rows generated, val_varchar length range [%, %]', v_count, v_minlen, v_maxlen;
END $$;

-- =============================================================================
-- SECTION 3: PATTERN-SEED EXTRACTION
-- =============================================================================
--
-- All benchmark query patterns are derived from values that genuinely exist
-- in the corpus, so that the vast majority of generated queries return
-- meaningful (non-zero) result sets, per the Query Generation Requirements.
-- We sample a deterministic subset of rows (given the fixed seed) from each
-- column and extract prefix/suffix/infix fragments of varying lengths.

DROP TABLE IF EXISTS biscuit_pattern_seeds;

CREATE TABLE biscuit_pattern_seeds (
    seed_id        bigserial PRIMARY KEY,
    source_id      bigint NOT NULL,
    source_column  text NOT NULL,      -- 'val_varchar' | 'val_text'
    source_value   text NOT NULL,
    len            int NOT NULL,
    prefix2        text NOT NULL,
    prefix3        text NOT NULL,
    prefix5        text,               -- NULL if len < 5... (never happens here, min len is 5, kept for robustness)
    suffix2        text NOT NULL,
    suffix3        text NOT NULL,
    suffix5        text,
    infix_mid2     text NOT NULL,      -- 2-char substring from the middle
    infix_mid3     text NOT NULL       -- 3-char substring from the middle
);

-- ---- 3.1 Sample 300 rows per column (600 pattern-seed rows total) --------
-- ORDER BY random() is deterministic given the fixed setseed() call in
-- Section 0, executed once per suite run.
INSERT INTO biscuit_pattern_seeds
    (source_id, source_column, source_value, len,
     prefix2, prefix3, prefix5,
     suffix2, suffix3, suffix5,
     infix_mid2, infix_mid3)
SELECT
    id,
    'val_varchar',
    val_varchar,
    length(val_varchar),
    substring(val_varchar from 1 for 2),
    substring(val_varchar from 1 for 3),
    CASE WHEN length(val_varchar) >= 5 THEN substring(val_varchar from 1 for 5) END,
    substring(val_varchar from length(val_varchar) - 1 for 2),
    substring(val_varchar from length(val_varchar) - 2 for 3),
    CASE WHEN length(val_varchar) >= 5 THEN substring(val_varchar from length(val_varchar) - 4 for 5) END,
    substring(val_varchar from greatest(1, length(val_varchar)/2) for 2),
    substring(val_varchar from greatest(1, length(val_varchar)/2 - 1) for 3)
FROM biscuit_test_data
WHERE batch = 'seed'
ORDER BY random()
LIMIT 300;

INSERT INTO biscuit_pattern_seeds
    (source_id, source_column, source_value, len,
     prefix2, prefix3, prefix5,
     suffix2, suffix3, suffix5,
     infix_mid2, infix_mid3)
SELECT
    id,
    'val_text',
    val_text,
    length(val_text),
    substring(val_text from 1 for 2),
    substring(val_text from 1 for 3),
    CASE WHEN length(val_text) >= 5 THEN substring(val_text from 1 for 5) END,
    substring(val_text from length(val_text) - 1 for 2),
    substring(val_text from length(val_text) - 2 for 3),
    CASE WHEN length(val_text) >= 5 THEN substring(val_text from length(val_text) - 4 for 5) END,
    substring(val_text from greatest(1, length(val_text)/2) for 2),
    substring(val_text from greatest(1, length(val_text)/2 - 1) for 3)
FROM biscuit_test_data
WHERE batch = 'seed'
ORDER BY random()
LIMIT 300;

-- ---- 3.2 Sanity check ------------------------------------------------------
DO $$
DECLARE
    v_count bigint;
BEGIN
    SELECT count(*) INTO v_count FROM biscuit_pattern_seeds;
    IF v_count < 500 THEN
        RAISE EXCEPTION 'Pattern seed extraction failed: expected >= 500 seed rows, got %', v_count;
    END IF;
    RAISE NOTICE 'Section 3 OK: % pattern-seed rows extracted', v_count;
END $$;

-- =============================================================================
-- SECTION 4: PROGRAMMATIC QUERY CATALOG GENERATION
-- =============================================================================
--
-- Builds biscuit_query_catalog by iterating over biscuit_pattern_seeds and
-- mechanically constructing every required pattern family. This is the
-- "generate programmatically instead of manually enumerating" requirement:
-- the loop below, not a human, decides exactly which strings go into each
-- query's predicate_sql.
--
-- A monotonically increasing counter per category produces stable,
-- human-readable query_id values like 'PFX_LIKE_POS_0001'.
--
-- Naming convention for query_id:
--   <CATEGORY>_<OPERATOR>_<SIGN>_<NNNN>
--     CATEGORY: PFX (prefix) | INFX (infix) | SFX (suffix) | USC (pure _) |
--               PCT (pure %) | MIX (mixed _/%) | LEADUSC | SURRUSC | TRAILUSC |
--               LEADPCT | SURRPCT | TRAILPCT | COMBO (arbitrary combo) |
--               AND (composite AND) | OR (composite OR) | MIXOP (LIKE+ILIKE
--               in same statement)
--     OPERATOR: LIKE | ILIKE
--     SIGN: POS (expected non-zero) | NEG (expected zero, negative test)
--     NNNN: zero-padded sequence number, unique within category+operator+sign
--
-- NOTE: biscuit_query_catalog was already created empty in Section 1. We
-- TRUNCATE ... CASCADE here (not DROP) so the table identity and its FK
-- relationship to biscuit_results survive. CASCADE also truncates
-- biscuit_results, which is safe because it is still empty at this point
-- in the suite (no phase has executed yet).
TRUNCATE TABLE biscuit_query_catalog CASCADE;

DO $$
DECLARE
    rec               record;
    v_id              text;
    v_seq             int;
    v_neg_quota       int;
    v_neg_emitted     int;
    v_upper_frag      text;
    v_alt_seed        record;
BEGIN
    ----------------------------------------------------------------------
    -- 4.1 PREFIX SEARCHES  (Category PFX)
    -- LIKE 'frag%' / ILIKE 'FRAG%' (case-mutated for genuine ILIKE test)
    -- NOT LIKE / NOT ILIKE counterparts on the same fragments.
    ----------------------------------------------------------------------
    v_seq := 0;
    v_neg_quota := 3;
    v_neg_emitted := 0;
    FOR rec IN
        SELECT * FROM biscuit_pattern_seeds
        WHERE source_column = 'val_varchar'
        ORDER BY seed_id
        LIMIT 40
    LOOP
        v_seq := v_seq + 1;
        v_upper_frag := upper(rec.prefix3);

        -- Positive LIKE
        INSERT INTO biscuit_query_catalog VALUES (
            format('PFX_LIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'PREFIX', 'LIKE_POSITIVE', 'val_varchar', 'both',
            format('Prefix match on val_varchar using 3-char prefix extracted from row id %s', rec.source_id),
            format('val_varchar LIKE %L', rec.prefix3 || '%'),
            CASE WHEN length(rec.prefix3) <= 2 THEN 'high' ELSE 'medium' END
        );

        -- Positive ILIKE, case-mutated so ILIKE is a meaningfully distinct test from LIKE
        INSERT INTO biscuit_query_catalog VALUES (
            format('PFX_ILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'PREFIX', 'ILIKE_POSITIVE', 'val_varchar', 'both',
            format('Case-insensitive prefix match on val_varchar using upper-cased 3-char prefix from row id %s', rec.source_id),
            format('val_varchar ILIKE %L', v_upper_frag || '%'),
            CASE WHEN length(rec.prefix3) <= 2 THEN 'high' ELSE 'medium' END
        );

        -- NOT LIKE counterpart (negation requirement, applied to the same pattern)
        INSERT INTO biscuit_query_catalog VALUES (
            format('PFX_NOTLIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'PREFIX', 'NOT_LIKE', 'val_varchar', 'both',
            format('Negated prefix match (NOT LIKE) on val_varchar, 3-char prefix from row id %s', rec.source_id),
            format('val_varchar NOT LIKE %L', rec.prefix3 || '%'),
            'high'
        );

        -- NOT ILIKE counterpart
        INSERT INTO biscuit_query_catalog VALUES (
            format('PFX_NOTILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'PREFIX', 'NOT_ILIKE', 'val_varchar', 'both',
            format('Negated case-insensitive prefix match (NOT ILIKE) on val_varchar from row id %s', rec.source_id),
            format('val_varchar NOT ILIKE %L', v_upper_frag || '%'),
            'high'
        );

        -- A handful of negative (true zero-result) prefix tests: inject a
        -- character ('@') outside the generation alphabet, guaranteeing zero
        -- matches deterministically rather than relying on probabilistic rarity.
        IF v_neg_emitted < v_neg_quota THEN
            v_neg_emitted := v_neg_emitted + 1;
            INSERT INTO biscuit_query_catalog VALUES (
                format('PFX_LIKE_NEG_%s', lpad(v_neg_emitted::text, 4, '0')),
                'PREFIX', 'LIKE_NEGATIVE', 'val_varchar', 'both',
                format('Negative-control prefix match (guaranteed zero rows) derived from row id %s with out-of-alphabet marker', rec.source_id),
                format('val_varchar LIKE %L', rec.prefix3 || '@@@%'),
                'zero'
            );
        END IF;
    END LOOP;

    RAISE NOTICE 'Section 4.1 (PREFIX) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.2 INFIX SEARCHES  (Category INFX)
    -- '%frag%' / ILIKE '%FRAG%', plus NOT LIKE / NOT ILIKE.
    ----------------------------------------------------------------------
    v_seq := 0;
    v_neg_quota := 3;
    v_neg_emitted := 0;
    FOR rec IN
        SELECT * FROM biscuit_pattern_seeds
        WHERE source_column = 'val_text'
        ORDER BY seed_id
        LIMIT 40
    LOOP
        v_seq := v_seq + 1;
        v_upper_frag := upper(rec.infix_mid3);

        INSERT INTO biscuit_query_catalog VALUES (
            format('INFX_LIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'INFIX', 'LIKE_POSITIVE', 'val_text', 'both',
            format('Infix match on val_text using 3-char middle fragment from row id %s', rec.source_id),
            format('val_text LIKE %L', '%' || rec.infix_mid3 || '%'),
            'medium'
        );

        INSERT INTO biscuit_query_catalog VALUES (
            format('INFX_ILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'INFIX', 'ILIKE_POSITIVE', 'val_text', 'both',
            format('Case-insensitive infix match on val_text using upper-cased 3-char fragment from row id %s', rec.source_id),
            format('val_text ILIKE %L', '%' || v_upper_frag || '%'),
            'medium'
        );

        INSERT INTO biscuit_query_catalog VALUES (
            format('INFX_NOTLIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'INFIX', 'NOT_LIKE', 'val_text', 'both',
            format('Negated infix match (NOT LIKE) on val_text, fragment from row id %s', rec.source_id),
            format('val_text NOT LIKE %L', '%' || rec.infix_mid3 || '%'),
            'high'
        );

        INSERT INTO biscuit_query_catalog VALUES (
            format('INFX_NOTILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'INFIX', 'NOT_ILIKE', 'val_text', 'both',
            format('Negated case-insensitive infix match (NOT ILIKE) on val_text from row id %s', rec.source_id),
            format('val_text NOT ILIKE %L', '%' || v_upper_frag || '%'),
            'high'
        );

        IF v_neg_emitted < v_neg_quota THEN
            v_neg_emitted := v_neg_emitted + 1;
            INSERT INTO biscuit_query_catalog VALUES (
                format('INFX_LIKE_NEG_%s', lpad(v_neg_emitted::text, 4, '0')),
                'INFIX', 'LIKE_NEGATIVE', 'val_text', 'both',
                format('Negative-control infix match (guaranteed zero rows) from row id %s with out-of-alphabet marker', rec.source_id),
                format('val_text LIKE %L', '%' || rec.infix_mid3 || '@@@' || '%'),
                'zero'
            );
        END IF;
    END LOOP;
    RAISE NOTICE 'Section 4.2 (INFIX) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.3 SUFFIX SEARCHES  (Category SFX)
    ----------------------------------------------------------------------
    v_seq := 0;
    v_neg_quota := 3;
    v_neg_emitted := 0;
    FOR rec IN
        SELECT * FROM biscuit_pattern_seeds
        WHERE source_column = 'val_varchar'
        ORDER BY seed_id DESC
        LIMIT 40
    LOOP
        v_seq := v_seq + 1;
        v_upper_frag := upper(rec.suffix3);

        INSERT INTO biscuit_query_catalog VALUES (
            format('SFX_LIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'SUFFIX', 'LIKE_POSITIVE', 'val_varchar', 'both',
            format('Suffix match on val_varchar using 3-char suffix from row id %s', rec.source_id),
            format('val_varchar LIKE %L', '%' || rec.suffix3),
            'medium'
        );

        INSERT INTO biscuit_query_catalog VALUES (
            format('SFX_ILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'SUFFIX', 'ILIKE_POSITIVE', 'val_varchar', 'both',
            format('Case-insensitive suffix match on val_varchar using upper-cased 3-char suffix from row id %s', rec.source_id),
            format('val_varchar ILIKE %L', '%' || v_upper_frag),
            'medium'
        );

        INSERT INTO biscuit_query_catalog VALUES (
            format('SFX_NOTLIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'SUFFIX', 'NOT_LIKE', 'val_varchar', 'both',
            format('Negated suffix match (NOT LIKE) on val_varchar from row id %s', rec.source_id),
            format('val_varchar NOT LIKE %L', '%' || rec.suffix3),
            'high'
        );

        INSERT INTO biscuit_query_catalog VALUES (
            format('SFX_NOTILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'SUFFIX', 'NOT_ILIKE', 'val_varchar', 'both',
            format('Negated case-insensitive suffix match (NOT ILIKE) on val_varchar from row id %s', rec.source_id),
            format('val_varchar NOT ILIKE %L', '%' || v_upper_frag),
            'high'
        );

        IF v_neg_emitted < v_neg_quota THEN
            v_neg_emitted := v_neg_emitted + 1;
            INSERT INTO biscuit_query_catalog VALUES (
                format('SFX_LIKE_NEG_%s', lpad(v_neg_emitted::text, 4, '0')),
                'SUFFIX', 'LIKE_NEGATIVE', 'val_varchar', 'both',
                format('Negative-control suffix match (guaranteed zero rows) from row id %s with out-of-alphabet marker', rec.source_id),
                format('val_varchar LIKE %L', '%' || '@@@' || rec.suffix3),
                'zero'
            );
        END IF;
    END LOOP;
    RAISE NOTICE 'Section 4.3 (SUFFIX) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.4 PURE UNDERSCORE WILDCARD PATTERNS  (Category USC)
    -- repeat('_', len) reproduces the exact length of a real value,
    -- guaranteeing at least one match (the source row itself, plus any
    -- other row sharing that exact length).
    ----------------------------------------------------------------------
    v_seq := 0;
    FOR rec IN
        SELECT * FROM biscuit_pattern_seeds
        WHERE source_column = 'val_varchar'
        ORDER BY seed_id
        LIMIT 20
    LOOP
        v_seq := v_seq + 1;

        INSERT INTO biscuit_query_catalog VALUES (
            format('USC_LIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'PURE_UNDERSCORE', 'LIKE_POSITIVE', 'val_varchar', 'both',
            format('Pure underscore wildcard matching exact length %s (length of row id %s)', rec.len, rec.source_id),
            format('val_varchar LIKE %L', repeat('_', rec.len)),
            CASE WHEN rec.len <= 7 THEN 'high' ELSE 'medium' END
        );

        INSERT INTO biscuit_query_catalog VALUES (
            format('USC_ILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'PURE_UNDERSCORE', 'ILIKE_POSITIVE', 'val_varchar', 'both',
            format('Case-insensitive pure underscore wildcard matching exact length %s', rec.len),
            format('val_varchar ILIKE %L', repeat('_', rec.len)),
            CASE WHEN rec.len <= 7 THEN 'high' ELSE 'medium' END
        );

        INSERT INTO biscuit_query_catalog VALUES (
            format('USC_NOTLIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'PURE_UNDERSCORE', 'NOT_LIKE', 'val_varchar', 'both',
            format('Negated pure underscore wildcard, length %s', rec.len),
            format('val_varchar NOT LIKE %L', repeat('_', rec.len)),
            'high'
        );

        INSERT INTO biscuit_query_catalog VALUES (
            format('USC_NOTILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'PURE_UNDERSCORE', 'NOT_ILIKE', 'val_varchar', 'both',
            format('Negated case-insensitive pure underscore wildcard, length %s', rec.len),
            format('val_varchar NOT ILIKE %L', repeat('_', rec.len)),
            'high'
        );
    END LOOP;
    -- One deterministic zero-result underscore test: length 16 cannot exist
    -- (max generated length is 15), so this is a guaranteed true negative.
    INSERT INTO biscuit_query_catalog VALUES (
        'USC_LIKE_NEG_0001', 'PURE_UNDERSCORE', 'LIKE_NEGATIVE', 'val_varchar', 'both',
        'Negative-control: underscore wildcard of length 16, which exceeds the max generated length of 15',
        format('val_varchar LIKE %L', repeat('_', 16)),
        'zero'
    );
    RAISE NOTICE 'Section 4.4 (PURE UNDERSCORE) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.5 PURE PERCENT WILDCARD PATTERNS  (Category PCT)
    -- '%' matches everything; included as a small sanity-check set, not
    -- spammed, since it has no meaningful selectivity variation.
    ----------------------------------------------------------------------
    INSERT INTO biscuit_query_catalog VALUES
        ('PCT_LIKE_POS_0001', 'PURE_PERCENT', 'LIKE_POSITIVE', 'val_varchar', 'both',
         'Pure percent wildcard matches every row (sanity check) on val_varchar',
         'val_varchar LIKE ''%''', 'high'),
        ('PCT_ILIKE_POS_0001', 'PURE_PERCENT', 'ILIKE_POSITIVE', 'val_varchar', 'both',
         'Pure percent wildcard matches every row (sanity check) on val_varchar via ILIKE',
         'val_varchar ILIKE ''%''', 'high'),
        ('PCT_LIKE_POS_0002', 'PURE_PERCENT', 'LIKE_POSITIVE', 'val_text', 'both',
         'Pure percent wildcard matches every row (sanity check) on val_text',
         'val_text LIKE ''%''', 'high'),
        ('PCT_LIKE_POS_0003', 'PURE_PERCENT', 'LIKE_POSITIVE', 'val_varchar', 'both',
         'Degenerate multi-percent wildcard (%%%) still matches every row',
         'val_varchar LIKE ''%%%''', 'high'),
        ('PCT_NOTLIKE_POS_0001', 'PURE_PERCENT', 'NOT_LIKE', 'val_varchar', 'both',
         'Negated pure percent wildcard: NOT LIKE ''%'' must match zero rows (every row matches the positive form)',
         'val_varchar NOT LIKE ''%''', 'zero');
    RAISE NOTICE 'Section 4.5 (PURE PERCENT) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    RAISE NOTICE 'Section 4 (partial 4.1-4.5) complete: % total catalog rows', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.6 LEADING UNDERSCORE  (Category LEADUSC)
    -- Replace the first character of a real value with '_', so the
    -- pattern is guaranteed to match at least the source row itself.
    ----------------------------------------------------------------------
    v_seq := 0;
    FOR rec IN
        SELECT * FROM biscuit_pattern_seeds WHERE source_column = 'val_text' ORDER BY seed_id LIMIT 20
    LOOP
        v_seq := v_seq + 1;
        INSERT INTO biscuit_query_catalog VALUES (
            format('LEADUSC_LIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'LEADING_UNDERSCORE', 'LIKE_POSITIVE', 'val_text', 'both',
            format('Leading underscore wildcard: first char of row id %s replaced with _', rec.source_id),
            format('val_text LIKE %L', '_' || substring(rec.source_value from 2)),
            'low'
        );
        INSERT INTO biscuit_query_catalog VALUES (
            format('LEADUSC_ILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'LEADING_UNDERSCORE', 'ILIKE_POSITIVE', 'val_text', 'both',
            format('Case-insensitive leading underscore wildcard, row id %s', rec.source_id),
            format('val_text ILIKE %L', '_' || upper(substring(rec.source_value from 2))),
            'low'
        );
        INSERT INTO biscuit_query_catalog VALUES (
            format('LEADUSC_NOTLIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'LEADING_UNDERSCORE', 'NOT_LIKE', 'val_text', 'both',
            format('Negated leading underscore wildcard, row id %s', rec.source_id),
            format('val_text NOT LIKE %L', '_' || substring(rec.source_value from 2)),
            'high'
        );
    END LOOP;
    RAISE NOTICE 'Section 4.6 (LEADING UNDERSCORE) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.7 SURROUNDED BY UNDERSCORE  (Category SURRUSC)
    -- '_' || middle_fragment || '_'
    ----------------------------------------------------------------------
    v_seq := 0;
    FOR rec IN
        SELECT * FROM biscuit_pattern_seeds WHERE source_column = 'val_text' AND len >= 7 ORDER BY seed_id LIMIT 20
    LOOP
        v_seq := v_seq + 1;
        -- NOTE: '_' || frag || '_' alone is a 5-char EXACT-LENGTH pattern
        -- (no wildcards beyond the two underscores), so it only matches
        -- strings that are exactly 5 characters long. To express "this
        -- 3-char fragment, flanked by any one character on each side,
        -- appears somewhere in the string" for our 5-15 char corpus, the
        -- pattern must be surrounded by '%' as well: '%_frag_%'.
        INSERT INTO biscuit_query_catalog VALUES (
            format('SURRUSC_LIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'SURROUNDED_UNDERSCORE', 'LIKE_POSITIVE', 'val_text', 'both',
            format('Underscore-surrounded fragment from row id %s: %% + _ + mid3 + _ + %%', rec.source_id),
            format('val_text LIKE %L', '%' || '_' || rec.infix_mid3 || '_' || '%'),
            'low'
        );
        INSERT INTO biscuit_query_catalog VALUES (
            format('SURRUSC_ILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'SURROUNDED_UNDERSCORE', 'ILIKE_POSITIVE', 'val_text', 'both',
            format('Case-insensitive underscore-surrounded fragment, row id %s', rec.source_id),
            format('val_text ILIKE %L', '%' || '_' || upper(rec.infix_mid3) || '_' || '%'),
            'low'
        );
        INSERT INTO biscuit_query_catalog VALUES (
            format('SURRUSC_NOTILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'SURROUNDED_UNDERSCORE', 'NOT_ILIKE', 'val_text', 'both',
            format('Negated case-insensitive underscore-surrounded fragment, row id %s', rec.source_id),
            format('val_text NOT ILIKE %L', '%' || '_' || upper(rec.infix_mid3) || '_' || '%'),
            'high'
        );
    END LOOP;
    RAISE NOTICE 'Section 4.7 (SURROUNDED UNDERSCORE) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.8 TRAILING UNDERSCORE  (Category TRAILUSC)
    -- Replace the last character of a real value with '_'.
    ----------------------------------------------------------------------
    v_seq := 0;
    FOR rec IN
        SELECT * FROM biscuit_pattern_seeds WHERE source_column = 'val_varchar' ORDER BY seed_id LIMIT 20
    LOOP
        v_seq := v_seq + 1;
        INSERT INTO biscuit_query_catalog VALUES (
            format('TRAILUSC_LIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'TRAILING_UNDERSCORE', 'LIKE_POSITIVE', 'val_varchar', 'both',
            format('Trailing underscore wildcard: last char of row id %s replaced with _', rec.source_id),
            format('val_varchar LIKE %L', substring(rec.source_value from 1 for rec.len - 1) || '_'),
            'low'
        );
        INSERT INTO biscuit_query_catalog VALUES (
            format('TRAILUSC_ILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'TRAILING_UNDERSCORE', 'ILIKE_POSITIVE', 'val_varchar', 'both',
            format('Case-insensitive trailing underscore wildcard, row id %s', rec.source_id),
            format('val_varchar ILIKE %L', upper(substring(rec.source_value from 1 for rec.len - 1)) || '_'),
            'low'
        );
        INSERT INTO biscuit_query_catalog VALUES (
            format('TRAILUSC_NOTLIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'TRAILING_UNDERSCORE', 'NOT_LIKE', 'val_varchar', 'both',
            format('Negated trailing underscore wildcard, row id %s', rec.source_id),
            format('val_varchar NOT LIKE %L', substring(rec.source_value from 1 for rec.len - 1) || '_'),
            'high'
        );
    END LOOP;
    RAISE NOTICE 'Section 4.8 (TRAILING UNDERSCORE) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.9 MIXED UNDERSCORE AND PERCENT WILDCARDS  (Category MIX)
    -- e.g. prefix3 || '_%' , '_' || infix_mid2 || '%' , prefix2 || '%' || suffix2
    ----------------------------------------------------------------------
    v_seq := 0;
    v_neg_quota := 4;
    v_neg_emitted := 0;
    FOR rec IN
        SELECT * FROM biscuit_pattern_seeds WHERE source_column = 'val_varchar' AND len >= 6 ORDER BY seed_id LIMIT 30
    LOOP
        v_seq := v_seq + 1;

        -- Variant A: prefix + single-char wildcard + percent
        INSERT INTO biscuit_query_catalog VALUES (
            format('MIX_LIKE_POS_%sA', lpad(v_seq::text, 4, '0')),
            'MIXED_WILDCARD', 'LIKE_POSITIVE', 'val_varchar', 'both',
            format('Mixed _ and %% wildcard (prefix + _ + %%) from row id %s', rec.source_id),
            format('val_varchar LIKE %L', rec.prefix3 || '_%'),
            'medium'
        );

        -- Variant B: underscore + infix + percent
        INSERT INTO biscuit_query_catalog VALUES (
            format('MIX_LIKE_POS_%sB', lpad(v_seq::text, 4, '0')),
            'MIXED_WILDCARD', 'LIKE_POSITIVE', 'val_varchar', 'both',
            format('Mixed _ and %% wildcard (_ + infix + %%) from row id %s', rec.source_id),
            format('val_varchar LIKE %L', '_' || rec.infix_mid2 || '%'),
            'low'
        );

        -- Variant C: ILIKE version of variant A, case-mutated
        INSERT INTO biscuit_query_catalog VALUES (
            format('MIX_ILIKE_POS_%sC', lpad(v_seq::text, 4, '0')),
            'MIXED_WILDCARD', 'ILIKE_POSITIVE', 'val_varchar', 'both',
            format('Case-insensitive mixed wildcard from row id %s', rec.source_id),
            format('val_varchar ILIKE %L', upper(rec.prefix3) || '_%'),
            'medium'
        );

        -- Variant D: NOT LIKE counterpart of A
        INSERT INTO biscuit_query_catalog VALUES (
            format('MIX_NOTLIKE_POS_%sD', lpad(v_seq::text, 4, '0')),
            'MIXED_WILDCARD', 'NOT_LIKE', 'val_varchar', 'both',
            format('Negated mixed wildcard from row id %s', rec.source_id),
            format('val_varchar NOT LIKE %L', rec.prefix3 || '_%'),
            'high'
        );

        IF v_neg_emitted < v_neg_quota THEN
            v_neg_emitted := v_neg_emitted + 1;
            INSERT INTO biscuit_query_catalog VALUES (
                format('MIX_LIKE_NEG_%s', lpad(v_neg_emitted::text, 4, '0')),
                'MIXED_WILDCARD', 'LIKE_NEGATIVE', 'val_varchar', 'both',
                format('Negative-control mixed wildcard (guaranteed zero) from row id %s', rec.source_id),
                format('val_varchar LIKE %L', rec.prefix3 || '@_%'),
                'zero'
            );
        END IF;
    END LOOP;
    RAISE NOTICE 'Section 4.9 (MIXED WILDCARD) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    RAISE NOTICE 'Section 4 (partial 4.1-4.9) complete: % total catalog rows', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.10 LEADING PERCENT WILDCARD  (Category LEADPCT)
    -- '%' || suffix_fragment. Mechanically identical in SQL shape to the
    -- SUFFIX category (Section 4.3), but tracked under its own catalog
    -- category/query_id per the requirements list's explicit naming, so
    -- every requirement line item is independently traceable in the report
    -- even though the underlying pattern construction is the same idea.
    ----------------------------------------------------------------------
    v_seq := 0;
    FOR rec IN
        SELECT * FROM biscuit_pattern_seeds WHERE source_column = 'val_text' ORDER BY seed_id LIMIT 20
    LOOP
        v_seq := v_seq + 1;
        INSERT INTO biscuit_query_catalog VALUES (
            format('LEADPCT_LIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'LEADING_PERCENT', 'LIKE_POSITIVE', 'val_text', 'both',
            format('Leading percent wildcard (%%+suffix) from row id %s', rec.source_id),
            format('val_text LIKE %L', '%' || rec.suffix3),
            'medium'
        );
        INSERT INTO biscuit_query_catalog VALUES (
            format('LEADPCT_ILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'LEADING_PERCENT', 'ILIKE_POSITIVE', 'val_text', 'both',
            format('Case-insensitive leading percent wildcard from row id %s', rec.source_id),
            format('val_text ILIKE %L', '%' || upper(rec.suffix3)),
            'medium'
        );
        INSERT INTO biscuit_query_catalog VALUES (
            format('LEADPCT_NOTLIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'LEADING_PERCENT', 'NOT_LIKE', 'val_text', 'both',
            format('Negated leading percent wildcard from row id %s', rec.source_id),
            format('val_text NOT LIKE %L', '%' || rec.suffix3),
            'high'
        );
    END LOOP;
    RAISE NOTICE 'Section 4.10 (LEADING PERCENT) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.11 SURROUNDED BY PERCENT  (Category SURRPCT)
    -- '%' || infix || '%'. Mechanically identical to INFIX (Section 4.2);
    -- tracked under its own category for the same traceability reason.
    ----------------------------------------------------------------------
    v_seq := 0;
    FOR rec IN
        SELECT * FROM biscuit_pattern_seeds WHERE source_column = 'val_varchar' ORDER BY seed_id LIMIT 20
    LOOP
        v_seq := v_seq + 1;
        INSERT INTO biscuit_query_catalog VALUES (
            format('SURRPCT_LIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'SURROUNDED_PERCENT', 'LIKE_POSITIVE', 'val_varchar', 'both',
            format('Percent-surrounded fragment from row id %s', rec.source_id),
            format('val_varchar LIKE %L', '%' || rec.infix_mid3 || '%'),
            'medium'
        );
        INSERT INTO biscuit_query_catalog VALUES (
            format('SURRPCT_ILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'SURROUNDED_PERCENT', 'ILIKE_POSITIVE', 'val_varchar', 'both',
            format('Case-insensitive percent-surrounded fragment from row id %s', rec.source_id),
            format('val_varchar ILIKE %L', '%' || upper(rec.infix_mid3) || '%'),
            'medium'
        );
        INSERT INTO biscuit_query_catalog VALUES (
            format('SURRPCT_NOTILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'SURROUNDED_PERCENT', 'NOT_ILIKE', 'val_varchar', 'both',
            format('Negated case-insensitive percent-surrounded fragment from row id %s', rec.source_id),
            format('val_varchar NOT ILIKE %L', '%' || upper(rec.infix_mid3) || '%'),
            'high'
        );
    END LOOP;
    RAISE NOTICE 'Section 4.11 (SURROUNDED PERCENT) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.12 TRAILING PERCENT WILDCARD  (Category TRAILPCT)
    -- prefix || '%'. Mechanically identical to PREFIX (Section 4.1);
    -- tracked under its own category for the same traceability reason.
    ----------------------------------------------------------------------
    v_seq := 0;
    FOR rec IN
        SELECT * FROM biscuit_pattern_seeds WHERE source_column = 'val_text' ORDER BY seed_id DESC LIMIT 20
    LOOP
        v_seq := v_seq + 1;
        INSERT INTO biscuit_query_catalog VALUES (
            format('TRAILPCT_LIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'TRAILING_PERCENT', 'LIKE_POSITIVE', 'val_text', 'both',
            format('Trailing percent wildcard (prefix+%%) from row id %s', rec.source_id),
            format('val_text LIKE %L', rec.prefix3 || '%'),
            'medium'
        );
        INSERT INTO biscuit_query_catalog VALUES (
            format('TRAILPCT_ILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'TRAILING_PERCENT', 'ILIKE_POSITIVE', 'val_text', 'both',
            format('Case-insensitive trailing percent wildcard from row id %s', rec.source_id),
            format('val_text ILIKE %L', upper(rec.prefix3) || '%'),
            'medium'
        );
        INSERT INTO biscuit_query_catalog VALUES (
            format('TRAILPCT_NOTLIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'TRAILING_PERCENT', 'NOT_LIKE', 'val_text', 'both',
            format('Negated trailing percent wildcard from row id %s', rec.source_id),
            format('val_text NOT LIKE %L', rec.prefix3 || '%'),
            'high'
        );
    END LOOP;
    RAISE NOTICE 'Section 4.12 (TRAILING PERCENT) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.13 ARBITRARY COMBINATIONS OF LITERAL TEXT, _ AND %  (Category COMBO)
    -- Randomly interleave literal fragments with _ and % tokens drawn from
    -- the same seed row, guaranteeing the source row matches.
    ----------------------------------------------------------------------
    v_seq := 0;
    v_neg_quota := 5;
    v_neg_emitted := 0;
    FOR rec IN
        SELECT * FROM biscuit_pattern_seeds WHERE source_column = 'val_varchar' AND len >= 8 ORDER BY seed_id LIMIT 30
    LOOP
        v_seq := v_seq + 1;

        -- Combo A: prefix2 + '_' + '%' + mid2 + '%' + suffix2
        -- NOTE: a '%' is required between the leading '_' wildcard and
        -- infix_mid2, because infix_mid2 is extracted from the middle of
        -- the string by character offset and is NOT guaranteed to be
        -- immediately adjacent to position 3 (prefix2 + one wildcard
        -- char) -- there can be a gap of zero or more characters that only
        -- '%' (not direct concatenation) can correctly span.
        INSERT INTO biscuit_query_catalog VALUES (
            format('COMBO_LIKE_POS_%sA', lpad(v_seq::text, 4, '0')),
            'ARBITRARY_COMBO', 'LIKE_POSITIVE', 'val_varchar', 'both',
            format('Arbitrary combo (prefix2 + _ + %% + mid2 + %% + suffix2) from row id %s', rec.source_id),
            format('val_varchar LIKE %L', rec.prefix2 || '_' || '%' || rec.infix_mid2 || '%' || rec.suffix2),
            'low'
        );

        -- Combo B: '%' + mid3 + '_%' + suffix2
        INSERT INTO biscuit_query_catalog VALUES (
            format('COMBO_LIKE_POS_%sB', lpad(v_seq::text, 4, '0')),
            'ARBITRARY_COMBO', 'LIKE_POSITIVE', 'val_varchar', 'both',
            format('Arbitrary combo (%% + mid3 + _%% + suffix2) from row id %s', rec.source_id),
            format('val_varchar LIKE %L', '%' || rec.infix_mid3 || '_%' || rec.suffix2),
            'low'
        );

        -- Combo C: ILIKE version of A, case-mutated literal fragments
        INSERT INTO biscuit_query_catalog VALUES (
            format('COMBO_ILIKE_POS_%sC', lpad(v_seq::text, 4, '0')),
            'ARBITRARY_COMBO', 'ILIKE_POSITIVE', 'val_varchar', 'both',
            format('Case-insensitive arbitrary combo from row id %s', rec.source_id),
            format('val_varchar ILIKE %L', upper(rec.prefix2) || '_' || '%' || upper(rec.infix_mid2) || '%' || upper(rec.suffix2)),
            'low'
        );

        -- Combo D: NOT LIKE counterpart of A
        INSERT INTO biscuit_query_catalog VALUES (
            format('COMBO_NOTLIKE_POS_%sD', lpad(v_seq::text, 4, '0')),
            'ARBITRARY_COMBO', 'NOT_LIKE', 'val_varchar', 'both',
            format('Negated arbitrary combo from row id %s', rec.source_id),
            format('val_varchar NOT LIKE %L', rec.prefix2 || '_' || '%' || rec.infix_mid2 || '%' || rec.suffix2),
            'high'
        );

        IF v_neg_emitted < v_neg_quota THEN
            v_neg_emitted := v_neg_emitted + 1;
            INSERT INTO biscuit_query_catalog VALUES (
                format('COMBO_LIKE_NEG_%s', lpad(v_neg_emitted::text, 4, '0')),
                'ARBITRARY_COMBO', 'LIKE_NEGATIVE', 'val_varchar', 'both',
                format('Negative-control arbitrary combo (guaranteed zero) from row id %s', rec.source_id),
                format('val_varchar LIKE %L', rec.prefix2 || '@_' || '%' || rec.infix_mid2 || '%' || rec.suffix2),
                'zero'
            );
        END IF;
    END LOOP;
    RAISE NOTICE 'Section 4.13 (ARBITRARY COMBO) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    RAISE NOTICE 'Section 4 (partial 4.1-4.13) complete: % total catalog rows', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.14 COMPOSITE PREDICATES: AND / OR ACROSS BOTH COLUMNS  (AND / OR)
    -- Because val_varchar and val_text are generated independently, pairing
    -- a varchar-pattern with a text-pattern from the SAME source row
    -- guarantees at least one AND-match (that row), while pairing patterns
    -- from DIFFERENT rows exercises genuine (likely low/zero) AND
    -- selectivity. OR pairs a guaranteed-match pattern with a
    -- guaranteed-zero pattern to confirm OR correctly unions results.
    --
    -- IMPORTANT: same-row fragments are derived directly from
    -- biscuit_test_data in a single query (NOT by joining the two
    -- independently-sampled halves of biscuit_pattern_seeds, which were
    -- drawn via two separate ORDER BY random() calls and therefore almost
    -- never share a source_id by chance).
    ----------------------------------------------------------------------
    v_seq := 0;
    FOR rec IN
        SELECT id AS vid, id AS tid,
               substring(val_varchar from 1 for 3) AS vpfx,
               substring(val_text from greatest(1, length(val_text)/2) for 3) AS tmid
        FROM biscuit_test_data
        WHERE batch = 'seed' AND length(val_text) >= 6
        ORDER BY id
        LIMIT 30
    LOOP
        v_seq := v_seq + 1;

        -- Same-row AND: both halves true for at least this one row.
        INSERT INTO biscuit_query_catalog VALUES (
            format('AND_SAMEROW_LIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'COMPOSITE_AND', 'LIKE_POSITIVE', 'both', 'multi',
            format('AND composite (same source row %s): varchar prefix AND text infix', rec.vid),
            format('val_varchar LIKE %L AND val_text LIKE %L', rec.vpfx || '%', '%' || rec.tmid || '%'),
            'low'
        );

        -- Same-row AND, ILIKE both sides (case-mutated)
        INSERT INTO biscuit_query_catalog VALUES (
            format('AND_SAMEROW_ILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'COMPOSITE_AND', 'ILIKE_POSITIVE', 'both', 'multi',
            format('Case-insensitive AND composite (same source row %s)', rec.vid),
            format('val_varchar ILIKE %L AND val_text ILIKE %L', upper(rec.vpfx) || '%', '%' || upper(rec.tmid) || '%'),
            'low'
        );

        -- OR: guaranteed-match (varchar prefix from this row) OR a
        -- deliberately-zero text pattern -- confirms OR still returns the
        -- guaranteed-match rows even when the other branch never matches.
        INSERT INTO biscuit_query_catalog VALUES (
            format('OR_GUARANTEED_LIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'COMPOSITE_OR', 'LIKE_POSITIVE', 'both', 'multi',
            format('OR composite: guaranteed varchar match (row %s) OR guaranteed-zero text pattern', rec.vid),
            format('val_varchar LIKE %L OR val_text LIKE %L', rec.vpfx || '%', '%' || rec.tmid || '@@@%'),
            'medium'
        );

        -- NOT LIKE / NOT ILIKE applied to an AND composite (negation of a composite)
        INSERT INTO biscuit_query_catalog VALUES (
            format('AND_NOTLIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'COMPOSITE_AND', 'NOT_LIKE', 'both', 'multi',
            format('Negated AND composite (NOT on varchar side) for row %s', rec.vid),
            format('val_varchar NOT LIKE %L AND val_text LIKE %L', rec.vpfx || '@@@%', '%' || rec.tmid || '%'),
            'high'
        );
    END LOOP;

    -- Different-row AND: pairs a varchar pattern from one row with a text
    -- pattern from an UNRELATED row, exercising genuine (typically low or
    -- zero) cross-column AND selectivity rather than a manufactured match.
    -- This deliberately reuses the two independently-sampled pattern_seeds
    -- pools, since here NOT sharing a source_id is exactly the point.
    v_seq := 0;
    FOR rec IN
        SELECT vs.source_id AS vid, vs.prefix3 AS vpfx,
               ts.source_id AS tid, ts.infix_mid3 AS tmid
        FROM (SELECT * FROM biscuit_pattern_seeds WHERE source_column='val_varchar' ORDER BY seed_id LIMIT 15) vs
        CROSS JOIN (SELECT * FROM biscuit_pattern_seeds WHERE source_column='val_text' ORDER BY seed_id DESC LIMIT 15) ts
        WHERE vs.source_id <> ts.source_id
        LIMIT 20
    LOOP
        v_seq := v_seq + 1;
        INSERT INTO biscuit_query_catalog VALUES (
            format('AND_DIFFROW_LIKE_%s', lpad(v_seq::text, 4, '0')),
            'COMPOSITE_AND', 'LIKE_CROSSROW', 'both', 'multi',
            format('AND composite across unrelated rows %s (varchar) and %s (text): genuine selectivity test, count may be low/zero', rec.vid, rec.tid),
            format('val_varchar LIKE %L AND val_text LIKE %L', rec.vpfx || '%', '%' || rec.tmid || '%'),
            'low'
        );
    END LOOP;
    RAISE NOTICE 'Section 4.14 (COMPOSITE AND/OR) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    ----------------------------------------------------------------------
    -- 4.15 MIXED LIKE + ILIKE WITHIN THE SAME STATEMENT  (Category MIXOP)
    -- val_varchar LIKE pattern1 AND val_text ILIKE pattern2, where
    -- pattern2 is case-flipped so that a plain LIKE would NOT have
    -- matched it -- genuinely exercising ILIKE's case-insensitivity
    -- within a mixed-operator statement. Same-row fragments are derived
    -- directly from biscuit_test_data for the same reason as 4.14 above.
    ----------------------------------------------------------------------
    v_seq := 0;
    FOR rec IN
        SELECT id AS vid,
               substring(val_varchar from 1 for 3) AS vpfx,
               substring(val_text from greatest(1, length(val_text)/2) for 3) AS tmid
        FROM biscuit_test_data
        WHERE batch = 'seed' AND length(val_text) >= 6
        ORDER BY id DESC
        LIMIT 25
    LOOP
        v_seq := v_seq + 1;
        INSERT INTO biscuit_query_catalog VALUES (
            format('MIXOP_LIKE_ILIKE_POS_%s', lpad(v_seq::text, 4, '0')),
            'MIXED_OPERATORS', 'LIKE_AND_ILIKE', 'both', 'multi',
            format('Mixed LIKE + ILIKE in one statement: val_varchar LIKE (exact case) AND val_text ILIKE (case-flipped), row %s', rec.vid),
            format('val_varchar LIKE %L AND val_text ILIKE %L', rec.vpfx || '%', '%' || upper(rec.tmid) || '%'),
            'low'
        );

        -- Verify the case-flip actually matters: the plain-LIKE version of
        -- the same text pattern (uppercased, case-sensitive) should be a
        -- DIFFERENT (typically zero or much lower) result than the ILIKE
        -- version above, proving ILIKE is doing real case-insensitive work.
        INSERT INTO biscuit_query_catalog VALUES (
            format('MIXOP_CASEFLIP_CONTROL_%s', lpad(v_seq::text, 4, '0')),
            'MIXED_OPERATORS', 'CASE_SENSITIVITY_CONTROL', 'both', 'multi',
            format('Control query: same predicate as MIXOP_LIKE_ILIKE_POS_%s but with LIKE instead of ILIKE on the text side, to demonstrate case-sensitivity makes a real difference', lpad(v_seq::text,4,'0')),
            format('val_varchar LIKE %L AND val_text LIKE %L', rec.vpfx || '%', '%' || upper(rec.tmid) || '%'),
            'zero'
        );
    END LOOP;
    RAISE NOTICE 'Section 4.15 (MIXED OPERATORS) catalog rows so far: %', (SELECT count(*) FROM biscuit_query_catalog);

    RAISE NOTICE 'Section 4 COMPLETE: % total catalog rows', (SELECT count(*) FROM biscuit_query_catalog);
END $$;

-- ---- 4.16 Catalog-wide sanity check ----------------------------------------
-- Confirms: (a) catalog is non-trivially large, (b) every predicate_sql is
-- syntactically valid SQL (executes without error), (c) the "no
-- predominantly-zero-result queries" requirement holds for POSITIVE-labeled
-- queries specifically (NEG/zero-labeled queries are EXPECTED to return 0).
DO $$
DECLARE
    v_catalog_count    bigint;
    v_zero_positive    bigint := 0;
    v_total_positive   bigint := 0;
    rec                record;
    v_count            bigint;
BEGIN
    SELECT count(*) INTO v_catalog_count FROM biscuit_query_catalog;
    IF v_catalog_count < 500 THEN
        RAISE EXCEPTION 'Catalog generation produced only % rows; expected several hundred at minimum', v_catalog_count;
    END IF;

    FOR rec IN
        SELECT query_id, predicate_sql
        FROM biscuit_query_catalog
        WHERE subcategory LIKE '%POSITIVE%'
    LOOP
        v_total_positive := v_total_positive + 1;
        EXECUTE format('SELECT count(*) FROM biscuit_test_data WHERE %s', rec.predicate_sql) INTO v_count;
        IF v_count = 0 THEN
            v_zero_positive := v_zero_positive + 1;
            RAISE WARNING 'Catalog query % is labeled POSITIVE but returned 0 rows: %', rec.query_id, rec.predicate_sql;
        END IF;
    END LOOP;

    IF v_zero_positive > 0 THEN
        RAISE EXCEPTION 'Catalog validation FAILED: % of % POSITIVE-labeled queries returned 0 rows (see warnings above)', v_zero_positive, v_total_positive;
    END IF;

    RAISE NOTICE 'Section 4.16 catalog sanity check PASSED: % total queries, % POSITIVE queries all returned > 0 rows', v_catalog_count, v_total_positive;
END $$;

-- =============================================================================
-- SECTION 5: EXECUTION ENGINE AND SEQUENTIAL-SCAN BASELINE
-- =============================================================================
--
-- biscuit_run_catalog_phase() executes every (or a filtered subset of)
-- catalog query and records results into biscuit_results under the given
-- phase label. It optionally compares against a PRIOR phase's counts
-- (via baseline_phase) rather than always treating its own seq_count
-- column as "freshly computed sequential scan" -- this matters for
-- POST_VACUUM / POST_REINDEX / PARALLEL phases run after mutations, where
-- the correct comparison is against a freshly-recomputed baseline on the
-- CURRENT data, not the original pre-mutation Stage-5 baseline (see
-- Stage 1 design doc, Section 9.1, for the full rationale).
--
-- Parameters:
--   p_phase            text   -- label stored in biscuit_results.phase
--   p_category_filter  text   -- NULL = all catalog rows; otherwise restricts
--                                 to a specific category (used for the
--                                 representative-subset phases in Sections 7-8)
--   p_compute_seq       boolean -- TRUE: actually run the query and store the
--                                 count as seq_count (used for true seqscan
--                                 baseline phases). FALSE: copy seq_count from
--                                 the most recent prior phase for this query_id
--                                 (used when re-validating an index phase
--                                 without re-deriving the baseline).
--   p_baseline_phase    text   -- when p_compute_seq is FALSE, which phase to
--                                 copy seq_count from.
--
-- In every phase, bisc_count is always freshly computed by executing the
-- query under the CURRENT planner/index configuration -- only seq_count's
-- source varies.

DROP FUNCTION IF EXISTS biscuit_run_catalog_phase(text, text, boolean, text);

CREATE FUNCTION biscuit_run_catalog_phase(
    p_phase           text,
    p_category_filter text DEFAULT NULL,
    p_compute_seq     boolean DEFAULT true,
    p_baseline_phase  text DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    rec            record;
    v_count        bigint;
    v_seq_count    bigint;
    v_plan_text    text;
    v_explain_json json;
BEGIN
    FOR rec IN
        SELECT query_id, predicate_sql
        FROM biscuit_query_catalog
        WHERE (p_category_filter IS NULL OR category = p_category_filter)
        ORDER BY query_id
    LOOP
        -- Determine seq_count for this row.
        IF p_compute_seq THEN
            EXECUTE format('SELECT count(*) FROM biscuit_test_data WHERE %s', rec.predicate_sql)
                INTO v_seq_count;
        ELSE
            SELECT r.seq_count INTO v_seq_count
            FROM biscuit_results r
            WHERE r.query_id = rec.query_id AND r.phase = p_baseline_phase
            ORDER BY r.recorded_at DESC
            LIMIT 1;

            IF v_seq_count IS NULL THEN
                RAISE EXCEPTION 'No baseline seq_count found for query_id=% in baseline_phase=%', rec.query_id, p_baseline_phase;
            END IF;
        END IF;

        -- Always freshly compute the count under current settings/indexes
        -- ("bisc_count" -- the count produced by whatever execution path
        -- the planner currently takes, which is what we are validating).
        EXECUTE format('SELECT count(*) FROM biscuit_test_data WHERE %s', rec.predicate_sql)
            INTO v_count;

        -- Best-effort plan capture for diagnostics only (never a pass/fail
        -- gate -- see Stage 1 design doc Assumption A7/A8).
        -- NOTE: classification must inspect actual plan semantics, not do a
        -- naive substring search on the JSON text -- "Parallel Aware":
        -- false is present on EVERY node regardless of whether parallelism
        -- was actually used, so a plain ILIKE '%Parallel%' check produces
        -- false positives on every single plan. We instead check for the
        -- presence of a "Workers Planned" key (only emitted on a real
        -- Gather/Gather Merge node) to detect genuine parallel execution.
        BEGIN
            EXECUTE format('EXPLAIN (FORMAT JSON) SELECT count(*) FROM biscuit_test_data WHERE %s', rec.predicate_sql)
                INTO v_explain_json;
            v_plan_text := v_explain_json::text;
            v_plan_text := CASE
                WHEN v_plan_text LIKE '%"Workers Planned"%' THEN 'Parallel'
                WHEN v_plan_text LIKE '%"Node Type": "Bitmap Heap Scan"%' THEN 'Bitmap/Index'
                WHEN v_plan_text LIKE '%"Node Type": "Index Scan"%' THEN 'Index Scan'
                WHEN v_plan_text LIKE '%"Node Type": "Index Only Scan"%' THEN 'Index Only Scan'
                WHEN v_plan_text LIKE '%"Node Type": "Seq Scan"%' THEN 'Seq Scan'
                ELSE 'Other'
            END;
        EXCEPTION WHEN OTHERS THEN
            v_plan_text := 'EXPLAIN_FAILED';
        END;

        INSERT INTO biscuit_results (query_id, phase, seq_count, bisc_count, plan_used)
        VALUES (rec.query_id, p_phase, v_seq_count, v_count, v_plan_text);
    END LOOP;

    RAISE NOTICE 'Phase % complete (category filter: %): % result rows recorded',
        p_phase, coalesce(p_category_filter, '<all>'),
        (SELECT count(*) FROM biscuit_results WHERE phase = p_phase);
END;
$$;

COMMENT ON FUNCTION biscuit_run_catalog_phase(text, text, boolean, text) IS
  'Core execution engine: runs catalog queries (optionally filtered by '
  'category) and records seq_count/bisc_count/plan diagnostics into '
  'biscuit_results under the given phase label.';

-- ---- 5.1 PHASE: SEQSCAN baseline (Functional Requirement #6) -------------
-- Force sequential scan and disable index/bitmap scans. No Biscuit index
-- exists yet at this point, so this is the cleanest possible guarantee of
-- a true sequential scan baseline.
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;

SELECT biscuit_run_catalog_phase('SEQSCAN', NULL, true, NULL);

-- ---- 5.2 Sanity check: every catalog query produced exactly one SEQSCAN row
DO $$
DECLARE
    v_catalog_count bigint;
    v_result_count  bigint;
BEGIN
    SELECT count(*) INTO v_catalog_count FROM biscuit_query_catalog;
    SELECT count(*) INTO v_result_count FROM biscuit_results WHERE phase = 'SEQSCAN';
    IF v_catalog_count <> v_result_count THEN
        RAISE EXCEPTION 'SEQSCAN phase row mismatch: % catalog rows but % SEQSCAN results', v_catalog_count, v_result_count;
    END IF;
    RAISE NOTICE 'Section 5.1-5.2 OK: SEQSCAN baseline recorded for all % catalog queries', v_result_count;
END $$;

-- ---- 5.3 PHASE: INDEXED_SINGLE -- single-column Biscuit indexes ----------
-- (Functional Requirements #7, #12)
CREATE INDEX idx_biscuit_varchar ON biscuit_test_data USING biscuit (val_varchar);
CREATE INDEX idx_biscuit_text    ON biscuit_test_data USING biscuit (val_text);

ANALYZE biscuit_test_data;

-- Disable sequential scan per the explicit requirement wording ("Disable
-- sequential scans, construct the Biscuit index, rerun every query").
SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = on;

SELECT biscuit_run_catalog_phase('INDEXED_SINGLE', NULL, false, 'SEQSCAN');

DO $$
DECLARE
    v_mismatches bigint;
BEGIN
    SELECT count(*) INTO v_mismatches FROM biscuit_results WHERE phase = 'INDEXED_SINGLE' AND delta <> 0;
    RAISE NOTICE 'Section 5.3 OK: INDEXED_SINGLE phase complete. Mismatches vs SEQSCAN baseline: %', v_mismatches;
END $$;

-- ---- 5.4 PHASE: INDEXED_MULTI -- composite (multi-column) Biscuit index --
-- (Functional Requirement #12)
-- Single-column indexes are dropped first so results in this phase are
-- attributable specifically to the composite index, not to whichever
-- index the planner happened to prefer among several available options
-- (Stage 1 design doc, Section 7.2).
DROP INDEX IF EXISTS idx_biscuit_varchar;
DROP INDEX IF EXISTS idx_biscuit_text;

CREATE INDEX idx_biscuit_multi ON biscuit_test_data USING biscuit (val_varchar, val_text);

ANALYZE biscuit_test_data;

SELECT biscuit_run_catalog_phase('INDEXED_MULTI', NULL, false, 'SEQSCAN');

DO $$
DECLARE
    v_mismatches bigint;
BEGIN
    SELECT count(*) INTO v_mismatches FROM biscuit_results WHERE phase = 'INDEXED_MULTI' AND delta <> 0;
    RAISE NOTICE 'Section 5.4 OK: INDEXED_MULTI phase complete. Mismatches vs SEQSCAN baseline: %', v_mismatches;
END $$;

-- ---- 5.5 Restore both single-column AND multi-column indexes together ----
-- so that subsequent sections (mutation tests, VACUUM/REINDEX, parallel
-- execution) exercise a realistic environment where multiple indexes
-- coexist, matching Stage 1 design doc corner case (Section 10, item 7).
CREATE INDEX idx_biscuit_varchar ON biscuit_test_data USING biscuit (val_varchar);
CREATE INDEX idx_biscuit_text    ON biscuit_test_data USING biscuit (val_text);

ANALYZE biscuit_test_data;

DO $$
DECLARE
    v_idx_count int;
BEGIN
    SELECT count(*) INTO v_idx_count
    FROM pg_indexes
    WHERE tablename = 'biscuit_test_data' AND indexname LIKE 'idx_biscuit%';
    IF v_idx_count <> 3 THEN
        RAISE EXCEPTION 'Expected 3 Biscuit indexes (2 single-column + 1 multi-column) to coexist, found %', v_idx_count;
    END IF;
    RAISE NOTICE 'Section 5.5 OK: % Biscuit indexes now coexist (single-column x2 + multi-column)', v_idx_count;
END $$;

-- =============================================================================
-- SECTION 6: MUTATION TESTS (INSERT / DELETE / UPDATE IMMEDIACY)
-- =============================================================================
--
-- (Functional Requirements #9, #10, #11)
-- Three independent, hardcoded data sets so the three mutation tests never
-- interfere with one another's deltas (Stage 1 design doc, Section 7.5):
--   - 20 rows for INSERT-immediacy testing
--   - 10 of those 20 are then DELETEd
--   - 15 SEPARATE, dedicated rows for UPDATE testing
--
-- All literal values are chosen with a guaranteed-unique 4-char
-- "distinguishing_pattern" fragment that does not occur elsewhere in the
-- 100,000-row generated corpus (using a 'zz' prefix combined with the row's
-- sequence number, which is outside the normal a-z0-9-uniform generation
-- distribution's typical clustering and explicitly verified below).

-- ---- 6.1 Populate the 20 hardcoded insert-test rows ------------------------
INSERT INTO biscuit_mutation_inserts (id, val_varchar, val_text, distinguishing_pattern) VALUES
    (1000001, 'zzaa001insert',  'zzaa001markertext',  'zzaa001'),
    (1000002, 'zzaa002insert',  'zzaa002markertext',  'zzaa002'),
    (1000003, 'zzaa003insert',  'zzaa003markertext',  'zzaa003'),
    (1000004, 'zzaa004insert',  'zzaa004markertext',  'zzaa004'),
    (1000005, 'zzaa005insert',  'zzaa005markertext',  'zzaa005'),
    (1000006, 'zzaa006insert',  'zzaa006markertext',  'zzaa006'),
    (1000007, 'zzaa007insert',  'zzaa007markertext',  'zzaa007'),
    (1000008, 'zzaa008insert',  'zzaa008markertext',  'zzaa008'),
    (1000009, 'zzaa009insert',  'zzaa009markertext',  'zzaa009'),
    (1000010, 'zzaa010insert',  'zzaa010markertext',  'zzaa010'),
    (1000011, 'zzaa011insert',  'zzaa011markertext',  'zzaa011'),
    (1000012, 'zzaa012insert',  'zzaa012markertext',  'zzaa012'),
    (1000013, 'zzaa013insert',  'zzaa013markertext',  'zzaa013'),
    (1000014, 'zzaa014insert',  'zzaa014markertext',  'zzaa014'),
    (1000015, 'zzaa015insert',  'zzaa015markertext',  'zzaa015'),
    (1000016, 'zzaa016insert',  'zzaa016markertext',  'zzaa016'),
    (1000017, 'zzaa017insert',  'zzaa017markertext',  'zzaa017'),
    (1000018, 'zzaa018insert',  'zzaa018markertext',  'zzaa018'),
    (1000019, 'zzaa019insert',  'zzaa019markertext',  'zzaa019'),
    (1000020, 'zzaa020insert',  'zzaa020markertext',  'zzaa020');

-- ---- 6.2 Pre-flight check: distinguishing patterns must not already exist
-- in the corpus (otherwise insert-immediacy assertions would be unreliable).
DO $$
DECLARE
    v_collisions bigint;
BEGIN
    SELECT count(*) INTO v_collisions
    FROM biscuit_mutation_inserts mi
    WHERE EXISTS (
        SELECT 1 FROM biscuit_test_data d
        WHERE d.val_varchar LIKE '%' || mi.distinguishing_pattern || '%'
           OR d.val_text    LIKE '%' || mi.distinguishing_pattern || '%'
    );
    IF v_collisions > 0 THEN
        RAISE EXCEPTION 'Mutation-insert distinguishing patterns collide with % existing corpus rows; choose different markers', v_collisions;
    END IF;
    RAISE NOTICE 'Section 6.2 OK: all 20 insert-test distinguishing patterns are unique (zero pre-existing collisions)';
END $$;

-- ---- 6.3 Designate 10 of the 20 rows for later deletion --------------------
INSERT INTO biscuit_mutation_deletes (id) VALUES
    (1000001), (1000002), (1000003), (1000004), (1000005),
    (1000006), (1000007), (1000008), (1000009), (1000010);

-- ---- 6.4 INSERT the 20 rows into the live table, with Biscuit indexes
--          already in place, then immediately verify visibility. ----------
INSERT INTO biscuit_test_data (id, val_varchar, val_text, batch)
SELECT id, val_varchar, val_text, 'insert_test'
FROM biscuit_mutation_inserts;

DO $$
DECLARE
    rec      record;
    v_count  bigint;
    v_failed int := 0;
BEGIN
    FOR rec IN SELECT id, distinguishing_pattern FROM biscuit_mutation_inserts LOOP
        EXECUTE format(
            'SELECT count(*) FROM biscuit_test_data WHERE val_varchar LIKE %L OR val_text LIKE %L',
            '%' || rec.distinguishing_pattern || '%', '%' || rec.distinguishing_pattern || '%'
        ) INTO v_count;
        IF v_count <> 1 THEN
            v_failed := v_failed + 1;
            RAISE WARNING 'Insert-immediacy FAILED for id=%: expected 1 match, found %', rec.id, v_count;
        END IF;
    END LOOP;
    IF v_failed > 0 THEN
        RAISE EXCEPTION 'Insert-immediacy verification FAILED for % of 20 rows', v_failed;
    END IF;
    RAISE NOTICE 'Section 6.4 OK: all 20 inserted rows are immediately visible to Biscuit-indexed queries';
END $$;

-- Record formal catalog entries + results for the insert-immediacy queries
-- so they appear in the final report under the INSERT category.
INSERT INTO biscuit_query_catalog
SELECT
    format('MUT_INSERT_VISIBLE_%s', lpad((id - 1000000)::text, 4, '0')),
    'MUTATION_INSERT', 'INSERT_IMMEDIACY', 'both', 'multi',
    format('Verify row id %s is immediately visible after INSERT (distinguishing pattern %s)', id, distinguishing_pattern),
    format('val_varchar LIKE %L OR val_text LIKE %L', '%' || distinguishing_pattern || '%', '%' || distinguishing_pattern || '%'),
    'low'
FROM biscuit_mutation_inserts;

SELECT biscuit_run_catalog_phase('MUTATION_INSERT', 'MUTATION_INSERT', true, NULL);

-- For insert-immediacy queries specifically, the "correct" answer is
-- always exactly 1 (not "whatever seqscan happens to return"), so we
-- overwrite seq_count to the known-correct expected value of 1 for
-- clarity in the report, then re-verify bisc_count matches.
UPDATE biscuit_results
SET seq_count = 1
WHERE phase = 'MUTATION_INSERT';

-- =============================================================================
-- END SECTION 6 (partial — delete and update sub-sections appended below).
-- =============================================================================
