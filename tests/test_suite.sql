-- =============================================================================
-- BISCUIT INDEX COMPREHENSIVE VALIDATION TEST SUITE
-- PostgreSQL 17+ | Industrial-Grade Accuracy Validation
-- =============================================================================
-- Purpose : Validate that query results using Biscuit indexes are EXACTLY
--           identical to ground-truth sequential scans across 500+ query
--           permutations covering ASCII, Unicode, LIKE, ILIKE, AND, OR,
--           multi-column, NULL, edge-cases, and fuzz scenarios.
-- Usage   : psql -U <user> -d <database> -f biscuit_test_suite.sql
-- =============================================================================

--\timing on
--\set ON_ERROR_STOP on



-- =============================================================================
-- SECTION 0: CONFIGURATION
-- =============================================================================

DO $$
BEGIN
  RAISE NOTICE '=============================================================';
  RAISE NOTICE ' BISCUIT INDEX VALIDATION SUITE - START';
  RAISE NOTICE ' PostgreSQL version: %', version();
  RAISE NOTICE '=============================================================';
END;
$$;

-- Master configuration table
CREATE TEMP TABLE _cfg (key TEXT PRIMARY KEY, val TEXT);
INSERT INTO _cfg VALUES
  ('dataset_rows',      '5000'),   -- rows in main test table
  ('unicode_rows',      '500'),    -- extra unicode-heavy rows
  ('fuzz_iterations',   '50'),     -- randomised fuzz rounds
  ('enable_explain',    'true'),   -- capture EXPLAIN ANALYZE
  ('fail_fast',         'false');  -- stop on first failure

-- =============================================================================
-- SECTION 1: RESULT TRACKING INFRASTRUCTURE
-- =============================================================================

CREATE TEMP TABLE _results (
  id            SERIAL PRIMARY KEY,
  category      TEXT    NOT NULL,
  test_name     TEXT    NOT NULL,
  query_text    TEXT    NOT NULL,
  biscuit_rows  BIGINT,
  seqscan_rows  BIGINT,
  row_match     BOOLEAN,
  biscuit_ms    NUMERIC(12,3),
  seqscan_ms    NUMERIC(12,3),
  index_used    BOOLEAN,
  status        TEXT    NOT NULL DEFAULT 'PENDING',  -- PASS / FAIL / SKIP
  detail        TEXT,
  recorded_at   TIMESTAMPTZ DEFAULT clock_timestamp()
);

-- Diagnostics for failing tests
CREATE TEMP TABLE _failures (
  result_id     INT REFERENCES _results(id),
  missing_ids   TEXT,   -- in seqscan but not biscuit
  extra_ids     TEXT,   -- in biscuit but not seqscan
  detail        TEXT
);

-- =============================================================================
-- SECTION 2: HELPER PLPGSQL FUNCTIONS
-- =============================================================================

-- ----------------------------------------------------------------------------
-- record_result(): low-level INSERT into _results
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _record_result(
    p_category   TEXT,
    p_test_name  TEXT,
    p_query      TEXT,
    p_b_rows     BIGINT,
    p_s_rows     BIGINT,
    p_match      BOOLEAN,
    p_b_ms       NUMERIC,
    p_s_ms       NUMERIC,
    p_idx_used   BOOLEAN,
    p_detail     TEXT DEFAULT NULL
) RETURNS INT LANGUAGE plpgsql AS $$
DECLARE
  v_id  INT;
  v_status TEXT;
BEGIN
  v_status := CASE WHEN p_match THEN 'PASS' ELSE 'FAIL' END;
  INSERT INTO _results
    (category, test_name, query_text, biscuit_rows, seqscan_rows,
     row_match, biscuit_ms, seqscan_ms, index_used, status, detail)
  VALUES
    (p_category, p_test_name, p_query, p_b_rows, p_s_rows,
     p_match, p_b_ms, p_s_ms, p_idx_used, v_status, p_detail)
  RETURNING id INTO v_id;
  RETURN v_id;
END;
$$;

-- ----------------------------------------------------------------------------
-- run_comparison(): core validation engine
--   Runs a query with biscuit indexes enabled, then with seqscan forced,
--   compares results via EXCEPT, records PASS/FAIL.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _run_comparison(
    p_category   TEXT,
    p_test_name  TEXT,
    p_select_sql TEXT,   -- full SELECT … FROM test_data WHERE …
    p_id_col     TEXT DEFAULT 'id'
) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
  v_b_start     TIMESTAMPTZ;
  v_b_end       TIMESTAMPTZ;
  v_s_start     TIMESTAMPTZ;
  v_s_end       TIMESTAMPTZ;
  v_b_rows      BIGINT := 0;
  v_s_rows      BIGINT := 0;
  v_missing     BIGINT := 0;
  v_extra       BIGINT := 0;
  v_match       BOOLEAN;
  v_rid         INT;
  v_detail      TEXT;
  v_plan        TEXT;
  v_index_used  BOOLEAN := false;
BEGIN
  -- ---- BISCUIT RUN ----
  EXECUTE 'SET enable_seqscan = on';
  EXECUTE 'SET enable_indexscan = on';
  EXECUTE 'SET enable_bitmapscan = on';

  v_b_start := clock_timestamp();
  EXECUTE 'CREATE TEMP TABLE _biscuit_res AS ' || p_select_sql;
  v_b_end   := clock_timestamp();
  SELECT COUNT(*) INTO v_b_rows FROM _biscuit_res;

  -- Capture plan
  BEGIN
    SELECT string_agg(row::text, E'\n')
      INTO v_plan
      FROM (SELECT row FROM pg_catalog.pg_query_settings(p_select_sql)) t(row);
  EXCEPTION WHEN OTHERS THEN
    v_plan := '(plan capture skipped)';
  END;

  -- Detect if biscuit index scan appears in plan
  v_index_used := (v_plan ILIKE '%biscuit%' OR v_plan ILIKE '%index%');

  -- ---- SEQSCAN (GROUND TRUTH) RUN ----
  EXECUTE 'SET enable_seqscan = on';
  EXECUTE 'SET enable_indexscan = off';
  EXECUTE 'SET enable_bitmapscan = off';

  v_s_start := clock_timestamp();
  EXECUTE 'CREATE TEMP TABLE _seqscan_res AS ' || p_select_sql;
  v_s_end   := clock_timestamp();
  SELECT COUNT(*) INTO v_s_rows FROM _seqscan_res;

  -- Restore defaults
  EXECUTE 'SET enable_indexscan = on';
  EXECUTE 'SET enable_bitmapscan = on';

  -- ---- COMPARISON via EXCEPT ----
  -- rows in seqscan but NOT in biscuit result => false negatives
  EXECUTE format(
    'SELECT COUNT(*) FROM (SELECT * FROM _seqscan_res EXCEPT SELECT * FROM _biscuit_res) x'
  ) INTO v_missing;

  -- rows in biscuit result but NOT in seqscan => false positives
  EXECUTE format(
    'SELECT COUNT(*) FROM (SELECT * FROM _biscuit_res EXCEPT SELECT * FROM _seqscan_res) x'
  ) INTO v_extra;

  v_match := (v_missing = 0 AND v_extra = 0 AND v_b_rows = v_s_rows);

  v_detail := format(
    'biscuit_rows=%s seqscan_rows=%s missing(FN)=%s extra(FP)=%s',
    v_b_rows, v_s_rows, v_missing, v_extra
  );

  v_rid := _record_result(
    p_category, p_test_name, p_select_sql,
    v_b_rows, v_s_rows, v_match,
    EXTRACT(EPOCH FROM (v_b_end - v_b_start)) * 1000,
    EXTRACT(EPOCH FROM (v_s_end - v_s_start)) * 1000,
    v_index_used, v_detail
  );

  IF NOT v_match THEN
    -- Record failing row IDs for diagnostics
    INSERT INTO _failures (result_id, missing_ids, extra_ids, detail)
    SELECT v_rid,
      (SELECT string_agg(id::text, ',' ORDER BY id)
         FROM (SELECT id FROM _seqscan_res EXCEPT SELECT id FROM _biscuit_res) x),
      (SELECT string_agg(id::text, ',' ORDER BY id)
         FROM (SELECT id FROM _biscuit_res EXCEPT SELECT id FROM _seqscan_res) x),
      v_detail;

    RAISE WARNING 'FAIL [%] % | %', p_category, p_test_name, v_detail;
  ELSE
    RAISE NOTICE 'PASS [%] % | %', p_category, p_test_name, v_detail;
  END IF;

  -- Cleanup temp result tables
  DROP TABLE IF EXISTS _biscuit_res;
  DROP TABLE IF EXISTS _seqscan_res;

EXCEPTION WHEN OTHERS THEN
  DROP TABLE IF EXISTS _biscuit_res;
  DROP TABLE IF EXISTS _seqscan_res;
  RAISE WARNING 'ERROR in test [%] %: %', p_category, p_test_name, SQLERRM;
  PERFORM _record_result(
    p_category, p_test_name, p_select_sql,
    NULL, NULL, false, NULL, NULL, false,
    'EXCEPTION: ' || SQLERRM
  );
END;
$$;

-- =============================================================================
-- SECTION 3: SCHEMA
-- =============================================================================

DROP TABLE IF EXISTS test_data CASCADE;

CREATE TABLE test_data (
  id            BIGSERIAL PRIMARY KEY,

  -- ASCII columns
  first_name    TEXT,
  last_name     TEXT,
  email         TEXT,
  phone         TEXT,
  city          TEXT,
  country       TEXT,
  company       TEXT,
  address       TEXT,
  url           TEXT,
  notes         TEXT,

  -- Unicode columns
  unicode_name  TEXT,   -- Tamil/Japanese/Chinese/Korean/Arabic names
  unicode_city  TEXT,   -- Unicode city names
  unicode_notes TEXT,   -- Unicode freeform text

  -- Mixed columns
  mixed_email   TEXT,   -- e.g. john_தமிழ்@example.com
  mixed_tag     TEXT,   -- e.g. café-東京

  -- Edge-case columns
  empty_col     TEXT,
  null_col      TEXT,
  numeric_str   TEXT,   -- numbers stored as text
  special_chars TEXT,   -- punctuation/symbols
  long_text     TEXT,   -- 500+ char strings

  -- Category flag for targeted queries
  category      TEXT,
  inserted_at   TIMESTAMPTZ DEFAULT clock_timestamp()
);

-- =============================================================================
-- SECTION 4: DATA GENERATION
-- =============================================================================

DO $$
DECLARE
  v_rows         INT  := (SELECT val::int FROM _cfg WHERE key='dataset_rows');
  v_uni_rows     INT  := (SELECT val::int FROM _cfg WHERE key='unicode_rows');

  -- Seed arrays
  first_names    TEXT[] := ARRAY[
    'Alice','Bob','Carol','David','Emma','Frank','Grace','Henry',
    'Irene','Jack','Karen','Liam','Mia','Noah','Olivia','Peter',
    'Quinn','Rose','Sam','Tina','Uma','Victor','Wendy','Xander',
    'Yara','Zoe','Ann','John','Jane','Test','Admin','User',
    'café','résumé','naïve','Ångström','Björk','Łukasz'
  ];
  last_names     TEXT[] := ARRAY[
    'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller',
    'Davis','Wilson','Taylor','Anderson','Thomas','Jackson','White',
    'Harris','Martin','Thompson','Young','Robinson','Walker',
    'Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores'
  ];
  companies      TEXT[] := ARRAY[
    'TechCorp','InnovateLtd','DataSystems','CloudWorks','NetSolutions',
    'AlphaTech','BetaSoft','GammaCorp','DeltaNet','EpsilonIO',
    'café-corp','مرحبا-tech','emoji😊corp','東京Tech','한국Corp',
    'Σystems','Ångström Inc','résumé LLC'
  ];
  cities         TEXT[] := ARRAY[
    'New York','Los Angeles','Chicago','Houston','Phoenix',
    'Philadelphia','San Antonio','San Diego','Dallas','San Jose',
    'London','Paris','Berlin','Tokyo','Sydney',
    'Mumbai','Beijing','Seoul','Cairo','São Paulo',
    '東京','大阪','서울','مكة','दिल्ली',
    'Zürich','Montréal','Kraków','Łódź','Reykjavík'
  ];
  countries      TEXT[] := ARRAY[
    'US','UK','DE','JP','AU','IN','CN','KR','EG','BR',
    'FR','CA','PL','IS','CH','SA','NG','ZA','MX','AR'
  ];
  domains        TEXT[] := ARRAY[
    'gmail.com','yahoo.com','hotmail.com','outlook.com','example.com',
    'corp.net','tech.io','company.org','test.com','mail.co'
  ];
  unicode_names  TEXT[] := ARRAY[
    'தமிழ்','こんにちは','你好','안녕하세요','مرحبا',
    'नमस्ते','Привет','Γεια','שלום','Merhaba',
    '😊நண்பன்','東京太郎','서울사람','القاهرة','दिल्ली वाला',
    'αβγδ','ñoño','Ångström','Björk','Łukasz',
    '🎉Party','🔥Hot','💡Idea','🌍World','🚀Launch'
  ];
  unicode_cities TEXT[] := ARRAY[
    '東京','大阪','서울','부산','北京','上海','台北',
    'மும்பை','دبي','القاهرة','تهران','कोलकाता',
    'Zürich','Montréal','Łódź','Reykjavík','Ångström City'
  ];
  special_arr    TEXT[] := ARRAY[
    '%percent%','under_score','hyphen-ated','dot.ted',
    'at@sign','hash#tag','slash/path','back\slash',
    'quote''s','double"quote','[bracket]','{brace}',
    '(paren)','<angle>','amp&rsand','eq=ual','plus+sign',
    '!exclaim','*star*','?query?','semi;colon','colon:val',
    '   spaces   ','	tab	',E'\nnewline\n',
    'NULL_like','0','1','-1','3.14','1e10'
  ];
  i              INT;
  v_fn           TEXT;
  v_ln           TEXT;
  v_email        TEXT;
  v_uname        TEXT;
  v_ucity        TEXT;
  v_mixed_email  TEXT;
  v_mixed_tag    TEXT;
  v_long_text    TEXT;
  v_special      TEXT;
  v_cat          TEXT;
  categories     TEXT[] := ARRAY[
    'ascii','unicode','mixed','edge','fuzz',
    'selective','broad','null_heavy'
  ];
BEGIN
  RAISE NOTICE 'Generating % main rows + % unicode rows...', v_rows, v_uni_rows;

  -- ---- Main dataset ----
  FOR i IN 1..v_rows LOOP
    v_fn    := first_names[1 + (i % array_length(first_names,1))];
    v_ln    := last_names [1 + (i % array_length(last_names ,1))];
    v_email := lower(v_fn) || '.' || lower(v_ln) || i::text || '@'
               || domains[1 + (i % array_length(domains,1))];
    v_uname := unicode_names[1 + (i % array_length(unicode_names,1))];
    v_ucity := unicode_cities[1 + (i % array_length(unicode_cities,1))];

    -- Mixed: embed Unicode into ASCII-style strings
    v_mixed_email := lower(v_fn) || '_'
                     || CASE WHEN i % 7 = 0 THEN 'தமிழ்'
                             WHEN i % 7 = 1 THEN 'こんにちは'
                             WHEN i % 7 = 2 THEN '你好'
                             WHEN i % 7 = 3 THEN '안녕'
                             WHEN i % 7 = 4 THEN 'مرحبا'
                             WHEN i % 7 = 5 THEN '😊'
                             ELSE            'naïve' END
                     || '@' || domains[1 + ((i+3) % array_length(domains,1))];

    v_mixed_tag := companies[1 + (i % array_length(companies,1))]
                   || '-'
                   || unicode_cities[1 + ((i+2) % array_length(unicode_cities,1))];

    v_special := special_arr[1 + (i % array_length(special_arr,1))];
    v_cat     := categories[1 + (i % array_length(categories,1))];

    -- Build a long text string
    v_long_text := repeat(v_fn || ' ' || v_ln || ' ' || v_uname || ' ', 20);

    INSERT INTO test_data (
      first_name, last_name, email, phone, city, country,
      company, address, url, notes,
      unicode_name, unicode_city, unicode_notes,
      mixed_email, mixed_tag,
      empty_col, null_col, numeric_str, special_chars, long_text,
      category
    ) VALUES (
      v_fn,
      v_ln,
      v_email,
      '+1-' || (100 + (i % 900))::text || '-555-'
              || lpad((i % 10000)::text, 4, '0'),
      cities[1 + (i % array_length(cities,1))],
      countries[1 + (i % array_length(countries,1))],
      companies[1 + ((i+1) % array_length(companies,1))],
      (100 + i)::text || ' Main St, Suite ' || (i % 500)::text,
      'https://www.' || lower(v_fn) || i::text || '.example.com',
      'Note ' || i::text || ': ' || v_fn || ' works at '
                || companies[1 + ((i+2) % array_length(companies,1))],
      v_uname,
      v_ucity,
      'Unicode note: ' || v_uname || ' lives in ' || v_ucity
        || '. Extra: தமிழ் こんにちは 你好 안녕하세요 مرحبا 😊',
      v_mixed_email,
      v_mixed_tag,
      CASE WHEN i % 20 = 0 THEN '' ELSE NULL END,   -- some empty strings
      CASE WHEN i % 15 = 0 THEN NULL ELSE 'notnull' END,
      (i * 3.14159)::numeric(12,5)::text,
      v_special,
      v_long_text,
      v_cat
    );
  END LOOP;

  -- ---- Unicode-dense rows ----
  FOR i IN 1..v_uni_rows LOOP
    INSERT INTO test_data (
      first_name, last_name, email, phone, city, country,
      company, address, url, notes,
      unicode_name, unicode_city, unicode_notes,
      mixed_email, mixed_tag,
      empty_col, null_col, numeric_str, special_chars, long_text,
      category
    ) VALUES (
      unicode_names[1 + (i % array_length(unicode_names,1))],
      unicode_names[1 + ((i+5) % array_length(unicode_names,1))],
      'unicode_' || i::text || '@' || domains[1+(i%array_length(domains,1))],
      '+' || (1 + i % 99)::text || '-' || (1000000 + i)::text,
      unicode_cities[1 + (i % array_length(unicode_cities,1))],
      countries[1 + (i % array_length(countries,1))],
      '会社' || i::text,
      unicode_cities[1 + (i % array_length(unicode_cities,1))] || ' 通り ' || i::text,
      'https://unicode-' || i::text || '.example.com',
      'たまに使う notes ' || i::text || ': '
        || unicode_names[1 + (i % array_length(unicode_names,1))],
      unicode_names[1 + (i % array_length(unicode_names,1))],
      unicode_cities[1 + (i % array_length(unicode_cities,1))],
      '詳細: ' || unicode_names[1+(i%array_length(unicode_names,1))]
        || ' ← ' || unicode_cities[1+((i+1)%array_length(unicode_cities,1))],
      unicode_names[1+(i%array_length(unicode_names,1))] || '@mixed.com',
      unicode_cities[1+(i%array_length(unicode_cities,1))] || '-corp',
      CASE WHEN i % 5 = 0 THEN '' ELSE NULL END,
      CASE WHEN i % 8 = 0 THEN NULL ELSE 'notnull' END,
      i::text,
      '😊🎉🔥💡🌍🚀' || (i % 10)::text,
      repeat(unicode_names[1+(i%array_length(unicode_names,1))] || ' ', 30),
      'unicode'
    );
  END LOOP;

  -- ---- Highly selective unique rows for precision testing ----
  INSERT INTO test_data (first_name, last_name, email, company, city,
    unicode_name, unicode_city, mixed_email, mixed_tag, category, notes)
  VALUES
    ('UniqueAlpha','UniqueZeta','unique_alpha@biscuit-test.com',
     'UniqueCorp','UniqueCity','உnique','유니크',
     'unique_alpha_தமிழ்@biscuit-test.com','UniqueCorp-東京',
     'selective','Highly selective row 1'),
    ('XxXRare','YyYRare','rare_xxy@biscuit-test.com',
     'RareCorp','RareCity','まれ','희귀',
     'rare_xxy_こんにちは@biscuit-test.com','RareCorp-서울',
     'selective','Highly selective row 2'),
    ('NullFirst',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'edge',
     'row with many NULLs'),
    ('EmptyStrings','EmptySurname','empty@biscuit-test.com',
     '','','','','','','edge','row with empty strings'),
    ('LikeWild','Percent%Char','percent%@biscuit-test.com',
     'Wild%Corp','City%1',NULL,NULL,NULL,NULL,'edge',
     'row containing literal % in data'),
    ('Under_Score','Under_Last','under_score@biscuit-test.com',
     'Under_Corp','Under_City',NULL,NULL,NULL,NULL,'edge',
     'row containing literal _ in data'),
    ('CaseSensA','CaseSensB','CaseSens@UPPERCASE.COM',
     'UPPERCASE Corp','NEW YORK','UPPERCASE','UPPERCASE',
     NULL,NULL,'ascii','uppercase row'),
    ('lowercase','onlylower','onlylower@lowercase.com',
     'lowercase corp','lowercase city',NULL,NULL,NULL,NULL,'ascii',
     'all lowercase row');

  RAISE NOTICE 'Data generation complete. Total rows: %',
    (SELECT COUNT(*) FROM test_data);
END;
$$;

-- Analyse for planner accuracy
ANALYZE test_data;

-- =============================================================================
-- SECTION 5: INDEX CREATION
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS biscuit;

DO $$
BEGIN
  RAISE NOTICE 'Creating Biscuit indexes...';
END;
$$;

-- Single-column indexes
CREATE INDEX idx_biscuit_email
  ON test_data USING biscuit(email);

CREATE INDEX idx_biscuit_first_name
  ON test_data USING biscuit(first_name);

CREATE INDEX idx_biscuit_last_name
  ON test_data USING biscuit(last_name);

CREATE INDEX idx_biscuit_company
  ON test_data USING biscuit(company);

CREATE INDEX idx_biscuit_city
  ON test_data USING biscuit(city);

CREATE INDEX idx_biscuit_notes
  ON test_data USING biscuit(notes);

CREATE INDEX idx_biscuit_unicode_name
  ON test_data USING biscuit(unicode_name);

CREATE INDEX idx_biscuit_unicode_city
  ON test_data USING biscuit(unicode_city);

CREATE INDEX idx_biscuit_unicode_notes
  ON test_data USING biscuit(unicode_notes);

CREATE INDEX idx_biscuit_mixed_email
  ON test_data USING biscuit(mixed_email);

CREATE INDEX idx_biscuit_mixed_tag
  ON test_data USING biscuit(mixed_tag);

CREATE INDEX idx_biscuit_special_chars
  ON test_data USING biscuit(special_chars);

CREATE INDEX idx_biscuit_long_text
  ON test_data USING biscuit(long_text);

-- Multi-column indexes (various permutations)
CREATE INDEX idx_biscuit_email_fname
  ON test_data USING biscuit(email, first_name);

CREATE INDEX idx_biscuit_fname_company
  ON test_data USING biscuit(first_name, company);

CREATE INDEX idx_biscuit_fname_lname
  ON test_data USING biscuit(first_name, last_name);

CREATE INDEX idx_biscuit_city_country
  ON test_data USING biscuit(city, country);

CREATE INDEX idx_biscuit_company_city
  ON test_data USING biscuit(company, city);

CREATE INDEX idx_biscuit_unicode_name_city
  ON test_data USING biscuit(unicode_name, unicode_city);

CREATE INDEX idx_biscuit_mixed_email_tag
  ON test_data USING biscuit(mixed_email, mixed_tag);

-- Wide multi-column indexes
CREATE INDEX idx_biscuit_multi4
  ON test_data USING biscuit(email, first_name, last_name, city);

CREATE INDEX idx_biscuit_multi5
  ON test_data USING biscuit(first_name, last_name, company, city, country);

CREATE INDEX idx_biscuit_unicode_multi
  ON test_data USING biscuit(unicode_name, unicode_city, unicode_notes);

CREATE INDEX idx_biscuit_mixed_multi
  ON test_data USING biscuit(mixed_email, mixed_tag, notes);

-- Permuted column-order indexes (same columns, different order)
CREATE INDEX idx_biscuit_lname_fname   -- reversed from fname_lname
  ON test_data USING biscuit(last_name, first_name);

CREATE INDEX idx_biscuit_country_city  -- reversed from city_country
  ON test_data USING biscuit(country, city);

ANALYZE test_data;

DO $$
BEGIN
  RAISE NOTICE 'All Biscuit indexes created.';
  RAISE NOTICE 'Index count: %',
    (SELECT COUNT(*) FROM pg_indexes
      WHERE tablename = 'test_data' AND indexdef ILIKE '%biscuit%');
END;
$$;

-- =============================================================================
-- SECTION 6: TEST EXECUTION
-- =============================================================================
-- Each call to _run_comparison():
--   1. Runs query with all indexes (biscuit path)
--   2. Runs same query with indexscan+bitmapscan OFF (seqscan ground truth)
--   3. Compares via EXCEPT / row counts
--   4. Records PASS/FAIL
-- =============================================================================

-- ============================================================
-- CAT-1: Single-column LIKE — email
-- ============================================================
DO $$
DECLARE cat TEXT := 'single_like_email';
BEGIN RAISE NOTICE '--- Category: % ---', cat; END;
$$;

SELECT _run_comparison('single_like_email','email infix gmail',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%''');
SELECT _run_comparison('single_like_email','email infix yahoo',
  'SELECT id FROM test_data WHERE email LIKE ''%yahoo%''');
SELECT _run_comparison('single_like_email','email infix hotmail',
  'SELECT id FROM test_data WHERE email LIKE ''%hotmail%''');
SELECT _run_comparison('single_like_email','email prefix alice',
  'SELECT id FROM test_data WHERE email LIKE ''alice%''');
SELECT _run_comparison('single_like_email','email prefix bob',
  'SELECT id FROM test_data WHERE email LIKE ''bob%''');
SELECT _run_comparison('single_like_email','email suffix .com',
  'SELECT id FROM test_data WHERE email LIKE ''%.com''');
SELECT _run_comparison('single_like_email','email suffix .org',
  'SELECT id FROM test_data WHERE email LIKE ''%.org''');
SELECT _run_comparison('single_like_email','email full match',
  'SELECT id FROM test_data WHERE email LIKE ''unique_alpha@biscuit-test.com''');
SELECT _run_comparison('single_like_email','email no match',
  'SELECT id FROM test_data WHERE email LIKE ''%zzznomatch%''');
SELECT _run_comparison('single_like_email','email wildcard only',
  'SELECT id FROM test_data WHERE email LIKE ''%''');
SELECT _run_comparison('single_like_email','email at-sign infix',
  'SELECT id FROM test_data WHERE email LIKE ''%@%''');
SELECT _run_comparison('single_like_email','email biscuit domain',
  'SELECT id FROM test_data WHERE email LIKE ''%biscuit-test%''');
SELECT _run_comparison('single_like_email','email unicode mixed',
  'SELECT id FROM test_data WHERE email LIKE ''%தமிழ்%''');
SELECT _run_comparison('single_like_email','email unicode mixed kana',
  'SELECT id FROM test_data WHERE email LIKE ''%こんにちは%''');

-- ============================================================
-- CAT-2: Single-column LIKE — first_name
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: single_like_fname ---'; END; $$;

SELECT _run_comparison('single_like_fname','fname prefix Ali',
  'SELECT id FROM test_data WHERE first_name LIKE ''Ali%''');
SELECT _run_comparison('single_like_fname','fname infix lic',
  'SELECT id FROM test_data WHERE first_name LIKE ''%lic%''');
SELECT _run_comparison('single_like_fname','fname suffix oe',
  'SELECT id FROM test_data WHERE first_name LIKE ''%oe''');
SELECT _run_comparison('single_like_fname','fname exact Alice',
  'SELECT id FROM test_data WHERE first_name LIKE ''Alice''');
SELECT _run_comparison('single_like_fname','fname no match',
  'SELECT id FROM test_data WHERE first_name LIKE ''%Qqq%''');
SELECT _run_comparison('single_like_fname','fname wildcard all',
  'SELECT id FROM test_data WHERE first_name LIKE ''%''');
SELECT _run_comparison('single_like_fname','fname unicode Tamil',
  'SELECT id FROM test_data WHERE first_name LIKE ''%தமிழ்%''');
SELECT _run_comparison('single_like_fname','fname unicode emoji',
  'SELECT id FROM test_data WHERE first_name LIKE ''%😊%''');
SELECT _run_comparison('single_like_fname','fname accented cafe',
  'SELECT id FROM test_data WHERE first_name LIKE ''%café%''');
SELECT _run_comparison('single_like_fname','fname accented resume',
  'SELECT id FROM test_data WHERE first_name LIKE ''%résumé%''');

-- ============================================================
-- CAT-3: Single-column LIKE — city
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: single_like_city ---'; END; $$;

SELECT _run_comparison('single_like_city','city infix ork (New York)',
  'SELECT id FROM test_data WHERE city LIKE ''%ork%''');
SELECT _run_comparison('single_like_city','city prefix New',
  'SELECT id FROM test_data WHERE city LIKE ''New%''');
SELECT _run_comparison('single_like_city','city suffix les (Angeles)',
  'SELECT id FROM test_data WHERE city LIKE ''%les''');
SELECT _run_comparison('single_like_city','city infix Tokyo',
  'SELECT id FROM test_data WHERE city LIKE ''%Tokyo%''');
SELECT _run_comparison('single_like_city','city Unicode Tokyo kanji',
  'SELECT id FROM test_data WHERE city LIKE ''%東京%''');
SELECT _run_comparison('single_like_city','city Unicode Seoul hangul',
  'SELECT id FROM test_data WHERE city LIKE ''%서울%''');
SELECT _run_comparison('single_like_city','city Unicode Mumbai Tamil',
  'SELECT id FROM test_data WHERE city LIKE ''%மும்பை%''');
SELECT _run_comparison('single_like_city','city accented Zürich',
  'SELECT id FROM test_data WHERE city LIKE ''%ürich%''');
SELECT _run_comparison('single_like_city','city accented Montréal',
  'SELECT id FROM test_data WHERE city LIKE ''%ntréal%''');
SELECT _run_comparison('single_like_city','city accented Kraków',
  'SELECT id FROM test_data WHERE city LIKE ''%raków%''');

-- ============================================================
-- CAT-4: Single-column ILIKE — case insensitive
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: single_ilike ---'; END; $$;

SELECT _run_comparison('single_ilike','ilike email uppercase GMAIL',
  'SELECT id FROM test_data WHERE email ILIKE ''%GMAIL%''');
SELECT _run_comparison('single_ilike','ilike email mixed case GmAiL',
  'SELECT id FROM test_data WHERE email ILIKE ''%GmAiL%''');
SELECT _run_comparison('single_ilike','ilike fname uppercase ALICE',
  'SELECT id FROM test_data WHERE first_name ILIKE ''%ALICE%''');
SELECT _run_comparison('single_ilike','ilike fname mixed AlIcE',
  'SELECT id FROM test_data WHERE first_name ILIKE ''%AlIcE%''');
SELECT _run_comparison('single_ilike','ilike fname lowercase alice',
  'SELECT id FROM test_data WHERE first_name ILIKE ''%alice%''');
SELECT _run_comparison('single_ilike','ilike company uppercase TECHCORP',
  'SELECT id FROM test_data WHERE company ILIKE ''%TECHCORP%''');
SELECT _run_comparison('single_ilike','ilike company mixed TeCh',
  'SELECT id FROM test_data WHERE company ILIKE ''%TeCh%''');
SELECT _run_comparison('single_ilike','ilike notes hello uppercase',
  'SELECT id FROM test_data WHERE notes ILIKE ''%HELLO%''');
SELECT _run_comparison('single_ilike','ilike email domain uppercase .COM',
  'SELECT id FROM test_data WHERE email ILIKE ''%.COM''');
SELECT _run_comparison('single_ilike','ilike unicode unicode_name Tamil case',
  'SELECT id FROM test_data WHERE unicode_name ILIKE ''%தமிழ்%''');
SELECT _run_comparison('single_ilike','ilike accented CAFÉ uppercase',
  'SELECT id FROM test_data WHERE company ILIKE ''%CAFÉ%''');
SELECT _run_comparison('single_ilike','ilike accented café lowercase',
  'SELECT id FROM test_data WHERE company ILIKE ''%café%''');
SELECT _run_comparison('single_ilike','ilike unicode city UPPERCASE UNICODE',
  'SELECT id FROM test_data WHERE unicode_city ILIKE ''%UNIQUE%''');
SELECT _run_comparison('single_ilike','ilike notes word NOTE',
  'SELECT id FROM test_data WHERE notes ILIKE ''%NOTE%''');
SELECT _run_comparison('single_ilike','ilike no match',
  'SELECT id FROM test_data WHERE email ILIKE ''%ZZZNOMATCH%''');
SELECT _run_comparison('single_ilike','ilike wildcard all',
  'SELECT id FROM test_data WHERE email ILIKE ''%''');

-- ============================================================
-- CAT-5: Multi-column AND — LIKE + LIKE
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: multi_and_like_like ---'; END; $$;

SELECT _run_comparison('multi_and_like_like','email gmail AND fname alice',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' AND first_name LIKE ''%Alice%''');
SELECT _run_comparison('multi_and_like_like','email yahoo AND company tech',
  'SELECT id FROM test_data WHERE email LIKE ''%yahoo%'' AND company LIKE ''%Tech%''');
SELECT _run_comparison('multi_and_like_like','city New AND country US',
  'SELECT id FROM test_data WHERE city LIKE ''%New%'' AND country LIKE ''%US%''');
SELECT _run_comparison('multi_and_like_like','city Tokyo AND company corp',
  'SELECT id FROM test_data WHERE city LIKE ''%Tokyo%'' AND company LIKE ''%Corp%''');
SELECT _run_comparison('multi_and_like_like','email example AND notes Note',
  'SELECT id FROM test_data WHERE email LIKE ''%example%'' AND notes LIKE ''%Note%''');
SELECT _run_comparison('multi_and_like_like','fname Bob AND lname Smith',
  'SELECT id FROM test_data WHERE first_name LIKE ''%Bob%'' AND last_name LIKE ''%Smith%''');
SELECT _run_comparison('multi_and_like_like','company corp AND city York',
  'SELECT id FROM test_data WHERE company LIKE ''%Corp%'' AND city LIKE ''%York%''');
SELECT _run_comparison('multi_and_like_like','three AND conditions',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' AND first_name LIKE ''%o%'' AND city LIKE ''%o%''');
SELECT _run_comparison('multi_and_like_like','four AND conditions',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' AND first_name LIKE ''%b%'' AND last_name LIKE ''%s%'' AND city LIKE ''%a%''');
SELECT _run_comparison('multi_and_like_like','no match AND',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' AND email LIKE ''%zzznomatch%''');
SELECT _run_comparison('multi_and_like_like','unicode AND ASCII mixed',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%தமிழ்%'' AND email LIKE ''%example%''');
SELECT _run_comparison('multi_and_like_like','unicode city AND unicode name',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%東京%'' AND unicode_name LIKE ''%こんにちは%''');

-- ============================================================
-- CAT-6: Multi-column AND — ILIKE + ILIKE
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: multi_and_ilike_ilike ---'; END; $$;

SELECT _run_comparison('multi_and_ilike_ilike','ilike email GMAIL AND fname ALICE',
  'SELECT id FROM test_data WHERE email ILIKE ''%GMAIL%'' AND first_name ILIKE ''%ALICE%''');
SELECT _run_comparison('multi_and_ilike_ilike','ilike email yahoo AND company TECH',
  'SELECT id FROM test_data WHERE email ILIKE ''%YAHOO%'' AND company ILIKE ''%TECH%''');
SELECT _run_comparison('multi_and_ilike_ilike','ilike city NEW AND country us',
  'SELECT id FROM test_data WHERE city ILIKE ''%NEW%'' AND country ILIKE ''%us%''');
SELECT _run_comparison('multi_and_ilike_ilike','ilike mixed case three cond',
  'SELECT id FROM test_data WHERE email ILIKE ''%gMaIl%'' AND first_name ILIKE ''%bOb%'' AND city ILIKE ''%yOrK%''');
SELECT _run_comparison('multi_and_ilike_ilike','ilike no match',
  'SELECT id FROM test_data WHERE email ILIKE ''%GMAIL%'' AND email ILIKE ''%ZZZNOMATCH%''');
SELECT _run_comparison('multi_and_ilike_ilike','ilike unicode AND ascii',
  'SELECT id FROM test_data WHERE unicode_name ILIKE ''%தமிழ்%'' AND email ILIKE ''%EXAMPLE%''');

-- ============================================================
-- CAT-7: Multi-column AND — LIKE + ILIKE mixed
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: multi_and_like_ilike ---'; END; $$;

SELECT _run_comparison('multi_and_like_ilike','like gmail AND ilike ALICE',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' AND first_name ILIKE ''%ALICE%''');
SELECT _run_comparison('multi_and_like_ilike','like yahoo AND ilike alice',
  'SELECT id FROM test_data WHERE email LIKE ''%yahoo%'' AND first_name ILIKE ''%alice%''');
SELECT _run_comparison('multi_and_like_ilike','ilike GMAIL AND like alice',
  'SELECT id FROM test_data WHERE email ILIKE ''%GMAIL%'' AND first_name LIKE ''%alice%''');
SELECT _run_comparison('multi_and_like_ilike','like corp AND ilike NEW',
  'SELECT id FROM test_data WHERE company LIKE ''%Corp%'' AND city ILIKE ''%NEW%''');
SELECT _run_comparison('multi_and_like_ilike','like note AND ilike BOB',
  'SELECT id FROM test_data WHERE notes LIKE ''%Note%'' AND first_name ILIKE ''%BOB%''');
SELECT _run_comparison('multi_and_like_ilike','unicode like AND ascii ilike',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%Tamil%'' AND email ILIKE ''%EXAMPLE%''');

-- ============================================================
-- CAT-8: OR combinations — LIKE OR LIKE
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: or_like_like ---'; END; $$;

SELECT _run_comparison('or_like_like','email gmail OR yahoo',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' OR email LIKE ''%yahoo%''');
SELECT _run_comparison('or_like_like','email gmail OR hotmail',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' OR email LIKE ''%hotmail%''');
SELECT _run_comparison('or_like_like','fname alice OR fname bob',
  'SELECT id FROM test_data WHERE first_name LIKE ''%Alice%'' OR first_name LIKE ''%Bob%''');
SELECT _run_comparison('or_like_like','city New York OR city Chicago',
  'SELECT id FROM test_data WHERE city LIKE ''%New%'' OR city LIKE ''%Chicago%''');
SELECT _run_comparison('or_like_like','company tech OR company corp',
  'SELECT id FROM test_data WHERE company LIKE ''%Tech%'' OR company LIKE ''%Corp%''');
SELECT _run_comparison('or_like_like','cross column OR',
  'SELECT id FROM test_data WHERE first_name LIKE ''%Ann%'' OR last_name LIKE ''%Ann%''');
SELECT _run_comparison('or_like_like','broad OR selective',
  'SELECT id FROM test_data WHERE email LIKE ''%@%'' OR first_name LIKE ''%UniqueAlpha%''');
SELECT _run_comparison('or_like_like','three OR conditions',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' OR email LIKE ''%yahoo%'' OR email LIKE ''%hotmail%''');
SELECT _run_comparison('or_like_like','unicode OR ascii',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%தமிழ்%'' OR email LIKE ''%unique_alpha%''');
SELECT _run_comparison('or_like_like','no match OR no match',
  'SELECT id FROM test_data WHERE email LIKE ''%zzznomatch1%'' OR email LIKE ''%zzznomatch2%''');

-- ============================================================
-- CAT-9: OR combinations — ILIKE OR ILIKE
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: or_ilike_ilike ---'; END; $$;

SELECT _run_comparison('or_ilike_ilike','ilike email GMAIL OR YAHOO',
  'SELECT id FROM test_data WHERE email ILIKE ''%GMAIL%'' OR email ILIKE ''%YAHOO%''');
SELECT _run_comparison('or_ilike_ilike','ilike fname ALICE OR BOB',
  'SELECT id FROM test_data WHERE first_name ILIKE ''%ALICE%'' OR first_name ILIKE ''%BOB%''');
SELECT _run_comparison('or_ilike_ilike','ilike cross-col fname OR lname',
  'SELECT id FROM test_data WHERE first_name ILIKE ''%ann%'' OR last_name ILIKE ''%ann%''');
SELECT _run_comparison('or_ilike_ilike','ilike company TECH OR CORP',
  'SELECT id FROM test_data WHERE company ILIKE ''%TECH%'' OR company ILIKE ''%CORP%''');
SELECT _run_comparison('or_ilike_ilike','ilike three OR',
  'SELECT id FROM test_data WHERE email ILIKE ''%GMAIL%'' OR email ILIKE ''%YAHOO%'' OR email ILIKE ''%HOTMAIL%''');
SELECT _run_comparison('or_ilike_ilike','ilike no match',
  'SELECT id FROM test_data WHERE email ILIKE ''%ZZZNOMATCH%'' OR company ILIKE ''%ZZZNOMATCH%''');

-- ============================================================
-- CAT-10: OR LIKE + ILIKE mixed
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: or_like_ilike ---'; END; $$;

SELECT _run_comparison('or_like_ilike','like gmail OR ilike YAHOO',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' OR email ILIKE ''%YAHOO%''');
SELECT _run_comparison('or_like_ilike','like alice OR ilike BOB',
  'SELECT id FROM test_data WHERE first_name LIKE ''%alice%'' OR first_name ILIKE ''%BOB%''');
SELECT _run_comparison('or_like_ilike','cross col like OR ilike',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' OR first_name ILIKE ''%CAROL%''');
SELECT _run_comparison('or_like_ilike','unicode like OR ascii ilike',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%こんにちは%'' OR email ILIKE ''%UNIQUE%''');

-- ============================================================
-- CAT-11: Complex AND + OR combinations
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: complex_and_or ---'; END; $$;

SELECT _run_comparison('complex_and_or','AND inside OR',
  'SELECT id FROM test_data WHERE (email LIKE ''%gmail%'' AND first_name LIKE ''%Alice%'') OR (email LIKE ''%yahoo%'' AND first_name LIKE ''%Bob%'')');
SELECT _run_comparison('complex_and_or','OR inside AND',
  'SELECT id FROM test_data WHERE (email LIKE ''%gmail%'' OR email LIKE ''%yahoo%'') AND company LIKE ''%Tech%''');
SELECT _run_comparison('complex_and_or','three-level nesting',
  'SELECT id FROM test_data WHERE ((email LIKE ''%gmail%'' OR email LIKE ''%yahoo%'') AND city LIKE ''%New%'') OR company LIKE ''%Corp%''');
SELECT _run_comparison('complex_and_or','unicode in OR clause',
  'SELECT id FROM test_data WHERE (unicode_name LIKE ''%தமிழ்%'' OR unicode_name LIKE ''%こんにちは%'') AND email LIKE ''%example%''');
SELECT _run_comparison('complex_and_or','ilike in AND/OR combo',
  'SELECT id FROM test_data WHERE email ILIKE ''%GMAIL%'' AND (city ILIKE ''%NEW%'' OR city ILIKE ''%LOS%'')');
SELECT _run_comparison('complex_and_or','four predicates mixed',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' AND (first_name ILIKE ''%alice%'' OR last_name ILIKE ''%smith%'') AND city LIKE ''%%''');

-- ============================================================
-- CAT-12: Unicode LIKE — Tamil
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: unicode_tamil ---'; END; $$;

SELECT _run_comparison('unicode_tamil','unicode_name infix தமிழ்',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%தமிழ்%''');
SELECT _run_comparison('unicode_tamil','unicode_notes infix தமிழ்',
  'SELECT id FROM test_data WHERE unicode_notes LIKE ''%தமிழ்%''');
SELECT _run_comparison('unicode_tamil','mixed_email infix தமிழ்',
  'SELECT id FROM test_data WHERE mixed_email LIKE ''%தமிழ்%''');
SELECT _run_comparison('unicode_tamil','long_text infix தமிழ்',
  'SELECT id FROM test_data WHERE long_text LIKE ''%தமிழ்%''');
SELECT _run_comparison('unicode_tamil','unicode_name ilike தமிழ்',
  'SELECT id FROM test_data WHERE unicode_name ILIKE ''%தமிழ்%''');
SELECT _run_comparison('unicode_tamil','unicode_notes prefix தமிழ்',
  'SELECT id FROM test_data WHERE unicode_notes LIKE ''தமிழ்%''');
SELECT _run_comparison('unicode_tamil','AND: unicode_name + email',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%தமிழ்%'' AND email LIKE ''%example%''');
SELECT _run_comparison('unicode_tamil','OR: unicode_name OR email',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%தமிழ்%'' OR email LIKE ''%unique_alpha%''');

-- ============================================================
-- CAT-13: Unicode LIKE — Japanese
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: unicode_japanese ---'; END; $$;

SELECT _run_comparison('unicode_japanese','unicode_name infix こんにちは',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%こんにちは%''');
SELECT _run_comparison('unicode_japanese','unicode_city Tokyo kanji',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%東京%''');
SELECT _run_comparison('unicode_japanese','unicode_city Osaka kanji',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%大阪%''');
SELECT _run_comparison('unicode_japanese','mixed_tag Tokyo kanji',
  'SELECT id FROM test_data WHERE mixed_tag LIKE ''%東京%''');
SELECT _run_comparison('unicode_japanese','unicode_notes kana infix にち',
  'SELECT id FROM test_data WHERE unicode_notes LIKE ''%にち%''');
SELECT _run_comparison('unicode_japanese','company kanji 会社',
  'SELECT id FROM test_data WHERE company LIKE ''%会社%''');
SELECT _run_comparison('unicode_japanese','address kana 通り',
  'SELECT id FROM test_data WHERE address LIKE ''%通り%''');
SELECT _run_comparison('unicode_japanese','AND unicode city + unicode name',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%東京%'' AND unicode_name LIKE ''%こんにちは%''');

-- ============================================================
-- CAT-14: Unicode LIKE — Chinese
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: unicode_chinese ---'; END; $$;

SELECT _run_comparison('unicode_chinese','unicode_name infix 你好',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%你好%''');
SELECT _run_comparison('unicode_chinese','unicode_city Beijing',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%北京%''');
SELECT _run_comparison('unicode_chinese','unicode_city Shanghai',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%上海%''');
SELECT _run_comparison('unicode_chinese','unicode_city Taipei',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%台北%''');
SELECT _run_comparison('unicode_chinese','AND two Chinese columns',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%北京%'' AND unicode_name LIKE ''%你好%''');

-- ============================================================
-- CAT-15: Unicode LIKE — Korean
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: unicode_korean ---'; END; $$;

SELECT _run_comparison('unicode_korean','unicode_name infix 안녕하세요',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%안녕하세요%''');
SELECT _run_comparison('unicode_korean','unicode_city Seoul',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%서울%''');
SELECT _run_comparison('unicode_korean','unicode_city Busan',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%부산%''');
SELECT _run_comparison('unicode_korean','mixed_email infix 안녕',
  'SELECT id FROM test_data WHERE mixed_email LIKE ''%안녕%''');
SELECT _run_comparison('unicode_korean','unicode_notes hangul infix',
  'SELECT id FROM test_data WHERE unicode_notes LIKE ''%서울%''');

-- ============================================================
-- CAT-16: Unicode LIKE — Arabic
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: unicode_arabic ---'; END; $$;

SELECT _run_comparison('unicode_arabic','unicode_name infix مرحبا',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%مرحبا%''');
SELECT _run_comparison('unicode_arabic','company infix مرحبا',
  'SELECT id FROM test_data WHERE company LIKE ''%مرحبا%''');
SELECT _run_comparison('unicode_arabic','unicode_city Cairo Arabic',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%القاهرة%''');
SELECT _run_comparison('unicode_arabic','unicode_city Tehran Arabic',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%تهران%''');
SELECT _run_comparison('unicode_arabic','mixed_email infix Arabic',
  'SELECT id FROM test_data WHERE mixed_email LIKE ''%مرحبا%''');
SELECT _run_comparison('unicode_arabic','AND Arabic + ASCII',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%مرحبا%'' AND email LIKE ''%example%''');

-- ============================================================
-- CAT-17: Unicode LIKE — Emoji
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: unicode_emoji ---'; END; $$;

SELECT _run_comparison('unicode_emoji','unicode_name emoji 😊',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%😊%''');
SELECT _run_comparison('unicode_emoji','company emoji 😊',
  'SELECT id FROM test_data WHERE company LIKE ''%😊%''');
SELECT _run_comparison('unicode_emoji','mixed_email emoji 😊',
  'SELECT id FROM test_data WHERE mixed_email LIKE ''%😊%''');
SELECT _run_comparison('unicode_emoji','special_chars emoji',
  'SELECT id FROM test_data WHERE special_chars LIKE ''%😊%''');
SELECT _run_comparison('unicode_emoji','unicode_notes emoji 🎉',
  'SELECT id FROM test_data WHERE unicode_notes LIKE ''%🎉%''');
SELECT _run_comparison('unicode_emoji','unicode_notes emoji 🔥',
  'SELECT id FROM test_data WHERE unicode_notes LIKE ''%🔥%''');
SELECT _run_comparison('unicode_emoji','emoji AND ascii',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%😊%'' AND email LIKE ''%example%''');
SELECT _run_comparison('unicode_emoji','emoji OR Tamil',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%😊%'' OR unicode_name LIKE ''%தமிழ்%''');

-- ============================================================
-- CAT-18: Unicode LIKE — Accented Latin / European
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: unicode_accented ---'; END; $$;

SELECT _run_comparison('unicode_accented','fname café',
  'SELECT id FROM test_data WHERE first_name LIKE ''%café%''');
SELECT _run_comparison('unicode_accented','fname résumé',
  'SELECT id FROM test_data WHERE first_name LIKE ''%résumé%''');
SELECT _run_comparison('unicode_accented','fname naïve',
  'SELECT id FROM test_data WHERE first_name LIKE ''%naïve%''');
SELECT _run_comparison('unicode_accented','fname Ångström',
  'SELECT id FROM test_data WHERE first_name LIKE ''%Ångström%''');
SELECT _run_comparison('unicode_accented','fname Björk',
  'SELECT id FROM test_data WHERE first_name LIKE ''%Björk%''');
SELECT _run_comparison('unicode_accented','fname Łukasz',
  'SELECT id FROM test_data WHERE first_name LIKE ''%Łukasz%''');
SELECT _run_comparison('unicode_accented','city Zürich',
  'SELECT id FROM test_data WHERE city LIKE ''%ürich%''');
SELECT _run_comparison('unicode_accented','city Montréal',
  'SELECT id FROM test_data WHERE city LIKE ''%ntréal%''');
SELECT _run_comparison('unicode_accented','city Łódź',
  'SELECT id FROM test_data WHERE city LIKE ''%ódź%''');
SELECT _run_comparison('unicode_accented','city Reykjavík',
  'SELECT id FROM test_data WHERE city LIKE ''%Reykjavík%''');
SELECT _run_comparison('unicode_accented','ilike CAFÉ uppercase',
  'SELECT id FROM test_data WHERE first_name ILIKE ''%CAFÉ%''');
SELECT _run_comparison('unicode_accented','ilike résumé uppercase',
  'SELECT id FROM test_data WHERE first_name ILIKE ''%RÉSUMÉ%''');

-- ============================================================
-- CAT-19: Mixed Unicode+ASCII multi-column AND
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: mixed_unicode_ascii_and ---'; END; $$;

SELECT _run_comparison('mixed_unicode_ascii_and','unicode_name+ilike email',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%தமிழ்%'' AND email ILIKE ''%EXAMPLE%''');
SELECT _run_comparison('mixed_unicode_ascii_and','unicode_city kanji+company',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%東京%'' AND company LIKE ''%Tech%''');
SELECT _run_comparison('mixed_unicode_ascii_and','emoji+ascii fname',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%😊%'' AND first_name LIKE ''%Alice%''');
SELECT _run_comparison('mixed_unicode_ascii_and','arabic+ascii city',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%مرحبا%'' AND city LIKE ''%New%''');
SELECT _run_comparison('mixed_unicode_ascii_and','mixed_email+unicode_city',
  'SELECT id FROM test_data WHERE mixed_email LIKE ''%தமிழ்%'' AND unicode_city LIKE ''%東京%''');
SELECT _run_comparison('mixed_unicode_ascii_and','korean+ascii three cond',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%안녕하세요%'' AND email LIKE ''%example%'' AND city LIKE ''%Seoul%''');

-- ============================================================
-- CAT-20: Mixed Unicode+ASCII multi-column OR
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: mixed_unicode_ascii_or ---'; END; $$;

SELECT _run_comparison('mixed_unicode_ascii_or','tamil OR japanese',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%தமிழ்%'' OR unicode_name LIKE ''%こんにちは%''');
SELECT _run_comparison('mixed_unicode_ascii_or','kanji city OR ascii city',
  'SELECT id FROM test_data WHERE unicode_city LIKE ''%東京%'' OR city LIKE ''%Tokyo%''');
SELECT _run_comparison('mixed_unicode_ascii_or','emoji OR arabic',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%😊%'' OR unicode_name LIKE ''%مرحبا%''');
SELECT _run_comparison('mixed_unicode_ascii_or','all scripts OR',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%தமிழ்%'' OR unicode_name LIKE ''%こんにちは%'' OR unicode_name LIKE ''%你好%'' OR unicode_name LIKE ''%안녕하세요%'' OR unicode_name LIKE ''%مرحبا%''');
SELECT _run_comparison('mixed_unicode_ascii_or','mixed_email OR unicode_city',
  'SELECT id FROM test_data WHERE mixed_email LIKE ''%தமிழ்%'' OR unicode_city LIKE ''%東京%''');

-- ============================================================
-- CAT-21: Escaped wildcard characters
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: escaped_wildcards ---'; END; $$;

SELECT _run_comparison('escaped_wildcards','literal percent sign ESCAPE',
  'SELECT id FROM test_data WHERE special_chars LIKE ''%\%%'' ESCAPE ''\''');
SELECT _run_comparison('escaped_wildcards','literal underscore ESCAPE',
  'SELECT id FROM test_data WHERE special_chars LIKE ''%\_%'' ESCAPE ''\''');
SELECT _run_comparison('escaped_wildcards','email literal dot ESCAPE',
  'SELECT id FROM test_data WHERE email LIKE ''%\.com'' ESCAPE ''\''');
SELECT _run_comparison('escaped_wildcards','email literal percent in data',
  'SELECT id FROM test_data WHERE email LIKE ''%percent%''');
SELECT _run_comparison('escaped_wildcards','special_chars literal at-sign',
  'SELECT id FROM test_data WHERE special_chars LIKE ''%@%''');
SELECT _run_comparison('escaped_wildcards','special_chars hyphen',
  'SELECT id FROM test_data WHERE special_chars LIKE ''%-%''');
SELECT _run_comparison('escaped_wildcards','special_chars dot',
  'SELECT id FROM test_data WHERE special_chars LIKE ''%\.%'' ESCAPE ''\''');
SELECT _run_comparison('escaped_wildcards','special_chars hash',
  'SELECT id FROM test_data WHERE special_chars LIKE ''%#%''');
SELECT _run_comparison('escaped_wildcards','special_chars bracket',
  'SELECT id FROM test_data WHERE special_chars LIKE ''%[%''');
SELECT _run_comparison('escaped_wildcards','special_chars AND literal percent',
  'SELECT id FROM test_data WHERE special_chars LIKE ''%\%%'' ESCAPE ''\'' AND category = ''edge''');
SELECT _run_comparison('escaped_wildcards','ilike with escaped wildcard',
  'SELECT id FROM test_data WHERE special_chars ILIKE ''%\%%'' ESCAPE ''\''');

-- ============================================================
-- CAT-22: NULL and empty string handling
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: null_empty ---'; END; $$;

SELECT _run_comparison('null_empty','null_col IS NULL',
  'SELECT id FROM test_data WHERE null_col IS NULL');
SELECT _run_comparison('null_empty','null_col IS NOT NULL',
  'SELECT id FROM test_data WHERE null_col IS NOT NULL');
SELECT _run_comparison('null_empty','email IS NULL',
  'SELECT id FROM test_data WHERE email IS NULL');
SELECT _run_comparison('null_empty','email IS NOT NULL',
  'SELECT id FROM test_data WHERE email IS NOT NULL');
SELECT _run_comparison('null_empty','empty_col = empty string',
  'SELECT id FROM test_data WHERE empty_col = ''''');
SELECT _run_comparison('null_empty','empty_col IS NULL',
  'SELECT id FROM test_data WHERE empty_col IS NULL');
SELECT _run_comparison('null_empty','empty_col IS NOT NULL',
  'SELECT id FROM test_data WHERE empty_col IS NOT NULL');
SELECT _run_comparison('null_empty','empty_col LIKE wildcard',
  'SELECT id FROM test_data WHERE empty_col LIKE ''%''');
SELECT _run_comparison('null_empty','email LIKE AND null_col IS NULL',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' AND null_col IS NULL');
SELECT _run_comparison('null_empty','email LIKE OR null_col IS NULL',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' OR null_col IS NULL');
SELECT _run_comparison('null_empty','first_name IS NULL',
  'SELECT id FROM test_data WHERE first_name IS NULL');
SELECT _run_comparison('null_empty','unicode_name IS NULL',
  'SELECT id FROM test_data WHERE unicode_name IS NULL');

-- ============================================================
-- CAT-23: Long text search
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: long_text ---'; END; $$;

SELECT _run_comparison('long_text','long_text infix Alice',
  'SELECT id FROM test_data WHERE long_text LIKE ''%Alice%''');
SELECT _run_comparison('long_text','long_text infix Bob',
  'SELECT id FROM test_data WHERE long_text LIKE ''%Bob%''');
SELECT _run_comparison('long_text','long_text ilike ALICE',
  'SELECT id FROM test_data WHERE long_text ILIKE ''%ALICE%''');
SELECT _run_comparison('long_text','long_text unicode Tamil infix',
  'SELECT id FROM test_data WHERE long_text LIKE ''%தமிழ்%''');
SELECT _run_comparison('long_text','long_text unicode Japanese infix',
  'SELECT id FROM test_data WHERE long_text LIKE ''%こんにちは%''');
SELECT _run_comparison('long_text','long_text AND email',
  'SELECT id FROM test_data WHERE long_text LIKE ''%Alice%'' AND email LIKE ''%gmail%''');
SELECT _run_comparison('long_text','long_text ilike AND company',
  'SELECT id FROM test_data WHERE long_text ILIKE ''%bob%'' AND company LIKE ''%Tech%''');

-- ============================================================
-- CAT-24: Selectivity extremes
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: selectivity ---'; END; $$;

-- Highly selective (expect 1 row)
SELECT _run_comparison('selectivity','highly selective unique_alpha email',
  'SELECT id FROM test_data WHERE email LIKE ''%unique_alpha@biscuit-test.com%''');
SELECT _run_comparison('selectivity','highly selective rare_xxy email',
  'SELECT id FROM test_data WHERE email LIKE ''%rare_xxy@biscuit-test.com%''');
SELECT _run_comparison('selectivity','highly selective unique company',
  'SELECT id FROM test_data WHERE company LIKE ''%UniqueCorp%''');

-- Very broad (expect most rows)
SELECT _run_comparison('selectivity','very broad email @',
  'SELECT id FROM test_data WHERE email LIKE ''%@%''');
SELECT _run_comparison('selectivity','very broad fname single char',
  'SELECT id FROM test_data WHERE first_name LIKE ''%a%''');
SELECT _run_comparison('selectivity','very broad notes Note',
  'SELECT id FROM test_data WHERE notes LIKE ''%Note%''');

-- Zero results
SELECT _run_comparison('selectivity','zero results zzznomatch',
  'SELECT id FROM test_data WHERE email LIKE ''%zzznomatch_unique_9999%''');

-- All rows
SELECT _run_comparison('selectivity','all rows wildcard',
  'SELECT id FROM test_data WHERE email LIKE ''%'' OR email IS NULL');

-- ============================================================
-- CAT-25: Numeric string patterns
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: numeric_str ---'; END; $$;

SELECT _run_comparison('numeric_str','numeric_str prefix 3',
  'SELECT id FROM test_data WHERE numeric_str LIKE ''3%''');
SELECT _run_comparison('numeric_str','numeric_str infix .14',
  'SELECT id FROM test_data WHERE numeric_str LIKE ''%.14%''');
SELECT _run_comparison('numeric_str','numeric_str suffix 0',
  'SELECT id FROM test_data WHERE numeric_str LIKE ''%0''');
SELECT _run_comparison('numeric_str','numeric_str AND email',
  'SELECT id FROM test_data WHERE numeric_str LIKE ''3%'' AND email LIKE ''%gmail%''');

-- ============================================================
-- CAT-26: Phone number patterns
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: phone ---'; END; $$;

SELECT _run_comparison('phone','phone prefix +1',
  'SELECT id FROM test_data WHERE phone LIKE ''+1%''');
SELECT _run_comparison('phone','phone infix 555',
  'SELECT id FROM test_data WHERE phone LIKE ''%-555-%''');
SELECT _run_comparison('phone','phone suffix 0000',
  'SELECT id FROM test_data WHERE phone LIKE ''%0000''');
SELECT _run_comparison('phone','phone ilike +1',
  'SELECT id FROM test_data WHERE phone ILIKE ''%+1%''');
SELECT _run_comparison('phone','phone AND city',
  'SELECT id FROM test_data WHERE phone LIKE ''%555%'' AND city LIKE ''%New%''');

-- ============================================================
-- CAT-27: URL patterns
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: url ---'; END; $$;

SELECT _run_comparison('url','url prefix https',
  'SELECT id FROM test_data WHERE url LIKE ''https%''');
SELECT _run_comparison('url','url infix example',
  'SELECT id FROM test_data WHERE url LIKE ''%example%''');
SELECT _run_comparison('url','url suffix .com',
  'SELECT id FROM test_data WHERE url LIKE ''%.com''');
SELECT _run_comparison('url','url ilike HTTPS',
  'SELECT id FROM test_data WHERE url ILIKE ''HTTPS%''');
SELECT _run_comparison('url','url AND email',
  'SELECT id FROM test_data WHERE url LIKE ''%alice%'' AND email LIKE ''%alice%''');

-- ============================================================
-- CAT-28: Index permutation — different column orderings
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: col_order_permutation ---'; END; $$;

-- Same logical query but hits different column-order indexes
SELECT _run_comparison('col_order_permutation','fname+lname order1',
  'SELECT id FROM test_data WHERE first_name LIKE ''%Alice%'' AND last_name LIKE ''%Smith%''');
SELECT _run_comparison('col_order_permutation','lname+fname order2',
  'SELECT id FROM test_data WHERE last_name LIKE ''%Smith%'' AND first_name LIKE ''%Alice%''');
SELECT _run_comparison('col_order_permutation','city+country order1',
  'SELECT id FROM test_data WHERE city LIKE ''%New%'' AND country LIKE ''%US%''');
SELECT _run_comparison('col_order_permutation','country+city order2',
  'SELECT id FROM test_data WHERE country LIKE ''%US%'' AND city LIKE ''%New%''');
SELECT _run_comparison('col_order_permutation','email+fname+lname+city',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' AND first_name LIKE ''%a%'' AND last_name LIKE ''%s%'' AND city LIKE ''%a%''');
SELECT _run_comparison('col_order_permutation','fname+lname+company+city+country',
  'SELECT id FROM test_data WHERE first_name LIKE ''%a%'' AND last_name LIKE ''%s%'' AND company LIKE ''%Tech%'' AND city LIKE ''%a%'' AND country LIKE ''%US%''');

-- ============================================================
-- CAT-29: Duplicate / repeated tokens
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: duplicate_tokens ---'; END; $$;

SELECT _run_comparison('duplicate_tokens','same pattern twice AND',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' AND email LIKE ''%gmail%''');
SELECT _run_comparison('duplicate_tokens','same pattern twice OR',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' OR email LIKE ''%gmail%''');
SELECT _run_comparison('duplicate_tokens','double NOT LIKE',
  'SELECT id FROM test_data WHERE email NOT LIKE ''%yahoo%'' AND email NOT LIKE ''%hotmail%''');
SELECT _run_comparison('duplicate_tokens','same col AND different predicates',
  'SELECT id FROM test_data WHERE email LIKE ''%alice%'' AND email LIKE ''%.com''');

-- ============================================================
-- CAT-30: NOT LIKE patterns
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: not_like ---'; END; $$;

SELECT _run_comparison('not_like','email NOT LIKE gmail',
  'SELECT id FROM test_data WHERE email NOT LIKE ''%gmail%''');
SELECT _run_comparison('not_like','email NOT ILIKE YAHOO',
  'SELECT id FROM test_data WHERE email NOT ILIKE ''%YAHOO%''');
SELECT _run_comparison('not_like','fname NOT LIKE Alice',
  'SELECT id FROM test_data WHERE first_name NOT LIKE ''%Alice%''');
SELECT _run_comparison('not_like','email NOT LIKE AND NOT LIKE',
  'SELECT id FROM test_data WHERE email NOT LIKE ''%gmail%'' AND email NOT LIKE ''%yahoo%''');
SELECT _run_comparison('not_like','NOT LIKE OR NOT LIKE',
  'SELECT id FROM test_data WHERE email NOT LIKE ''%gmail%'' OR email NOT LIKE ''%yahoo%''');
SELECT _run_comparison('not_like','NOT LIKE combined with IS NOT NULL',
  'SELECT id FROM test_data WHERE email NOT LIKE ''%gmail%'' AND email IS NOT NULL');

-- ============================================================
-- CAT-31: Case-sensitivity checks (LIKE vs ILIKE contrast)
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: case_sensitivity ---'; END; $$;

-- These tests verify LIKE is case-sensitive
SELECT _run_comparison('case_sensitivity','LIKE uppercase TEST vs lowercase test',
  'SELECT id FROM test_data WHERE notes LIKE ''%Test%''');
SELECT _run_comparison('case_sensitivity','LIKE lowercase test',
  'SELECT id FROM test_data WHERE notes LIKE ''%test%''');
SELECT _run_comparison('case_sensitivity','LIKE uppercase TEST',
  'SELECT id FROM test_data WHERE notes LIKE ''%TEST%''');
SELECT _run_comparison('case_sensitivity','ILIKE TEST matches all cases',
  'SELECT id FROM test_data WHERE notes ILIKE ''%TEST%''');
SELECT _run_comparison('case_sensitivity','LIKE Corp vs CORP vs corp',
  'SELECT id FROM test_data WHERE company LIKE ''%Corp%''');
SELECT _run_comparison('case_sensitivity','LIKE CORP exact uppercase',
  'SELECT id FROM test_data WHERE company LIKE ''%CORP%''');
SELECT _run_comparison('case_sensitivity','ILIKE CORP case-insensitive',
  'SELECT id FROM test_data WHERE company ILIKE ''%CORP%''');

-- ============================================================
-- CAT-32: Single-character wildcard (_) patterns
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: single_char_wildcard ---'; END; $$;

SELECT _run_comparison('single_char_wildcard','fname _ wildcard one char',
  'SELECT id FROM test_data WHERE first_name LIKE ''_ob%''');
SELECT _run_comparison('single_char_wildcard','email _ wildcard domain',
  'SELECT id FROM test_data WHERE email LIKE ''%@_mail.com''');
SELECT _run_comparison('single_char_wildcard','email four-char prefix _lic',
  'SELECT id FROM test_data WHERE email LIKE ''_lic%''');
SELECT _run_comparison('single_char_wildcard','fname _ middle',
  'SELECT id FROM test_data WHERE first_name LIKE ''A_ice''');
SELECT _run_comparison('single_char_wildcard','ilike _ wildcard',
  'SELECT id FROM test_data WHERE first_name ILIKE ''_OB%''');

-- ============================================================
-- CAT-33: Prefix-only patterns
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: prefix_only ---'; END; $$;

SELECT _run_comparison('prefix_only','email prefix alice',
  'SELECT id FROM test_data WHERE email LIKE ''alice%''');
SELECT _run_comparison('prefix_only','email prefix bob',
  'SELECT id FROM test_data WHERE email LIKE ''bob%''');
SELECT _run_comparison('prefix_only','fname prefix Al',
  'SELECT id FROM test_data WHERE first_name LIKE ''Al%''');
SELECT _run_comparison('prefix_only','company prefix Tech',
  'SELECT id FROM test_data WHERE company LIKE ''Tech%''');
SELECT _run_comparison('prefix_only','url prefix https',
  'SELECT id FROM test_data WHERE url LIKE ''https%''');
SELECT _run_comparison('prefix_only','unicode prefix こんにちは',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''こんにちは%''');
SELECT _run_comparison('prefix_only','unicode prefix 你好',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''你好%''');
SELECT _run_comparison('prefix_only','ilike prefix ALICE',
  'SELECT id FROM test_data WHERE first_name ILIKE ''AL%''');

-- ============================================================
-- CAT-34: Suffix-only patterns
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: suffix_only ---'; END; $$;

SELECT _run_comparison('suffix_only','email suffix .com',
  'SELECT id FROM test_data WHERE email LIKE ''%.com''');
SELECT _run_comparison('suffix_only','email suffix .org',
  'SELECT id FROM test_data WHERE email LIKE ''%.org''');
SELECT _run_comparison('suffix_only','fname suffix ce (Alice)',
  'SELECT id FROM test_data WHERE first_name LIKE ''%ce''');
SELECT _run_comparison('suffix_only','fname suffix ob (Bob)',
  'SELECT id FROM test_data WHERE first_name LIKE ''%ob''');
SELECT _run_comparison('suffix_only','unicode suffix はyo (こんにちは)',
  'SELECT id FROM test_data WHERE unicode_name LIKE ''%ちは''');
SELECT _run_comparison('suffix_only','ilike suffix .COM',
  'SELECT id FROM test_data WHERE email ILIKE ''%.COM''');

-- ============================================================
-- CAT-35: Fuzz / randomised patterns
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: fuzz ---'; END; $$;

DO $$
DECLARE
  v_iters INT := (SELECT val::int FROM _cfg WHERE key='fuzz_iterations');
  i       INT;
  v_chars TEXT[] := ARRAY['a','b','c','d','e','i','o','u','n','s','t',
                           'A','B','C','N','S','T',
                           'こ','に','ち','は','你','好','안','녕',
                           'ம','ா','ழ','்','م','ر','ح','ب',
                           '1','2','3','.','@','-'];
  v_cols  TEXT[] := ARRAY['email','first_name','last_name','company',
                           'city','notes','unicode_name','unicode_city',
                           'mixed_email','mixed_tag'];
  v_col   TEXT;
  v_token TEXT;
  v_sql   TEXT;
  v_op    TEXT;
  v_ops   TEXT[] := ARRAY['LIKE','LIKE','LIKE','ILIKE','ILIKE'];
BEGIN
  FOR i IN 1..v_iters LOOP
    v_col   := v_cols[1 + (i % array_length(v_cols,1))];
    v_token := v_chars[1 + ((i*3) % array_length(v_chars,1))]
            || v_chars[1 + ((i*7) % array_length(v_chars,1))];
    v_op    := v_ops[1 + (i % array_length(v_ops,1))];
    v_sql   := format(
      'SELECT id FROM test_data WHERE %I %s %L',
      v_col, v_op, '%' || v_token || '%'
    );
    PERFORM _run_comparison(
      'fuzz',
      format('fuzz_%s_%s_%s_%s', i, v_col, v_op, v_token),
      v_sql
    );
  END LOOP;
END;
$$;

-- ============================================================
-- CAT-36: Wide multi-column index queries (5 columns)
-- ============================================================
-- DO $$ BEGIN RAISE NOTICE '--- Category: wide_multi_col ---'; END; $$;

-- SELECT _run_comparison('wide_multi_col','five col AND all match',
--   'SELECT id FROM test_data WHERE first_name LIKE ''%a%'' AND last_name LIKE ''%s%'' AND company LIKE ''%Tech%'' AND city LIKE ''%a%'' AND country LIKE ''%US%''');
-- SELECT _run_comparison('wide_multi_col','five col AND partial no-match',
--   'SELECT id FROM test_data WHERE first_name LIKE ''%Alice%'' AND last_name LIKE ''%zzznomatch%'' AND company LIKE ''%Tech%'' AND city LIKE ''%New%'' AND country LIKE ''%US%''');
-- SELECT _run_comparison('wide_multi_col','five col OR broad',
--   'SELECT id FROM test_data WHERE first_name LIKE ''%a%'' OR last_name LIKE ''%s%'' OR company LIKE ''%Tech%'' OR city LIKE ''%a%'' OR country LIKE ''%US%''');
-- SELECT _run_comparison('wide_multi_col','email+fname+lname+city four col',
--   'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' AND first_name LIKE ''%A%'' AND last_name LIKE ''%S%'' AND city LIKE ''%A%''');
-- SELECT _run_comparison('wide_multi_col','unicode three col AND',
--   'SELECT id FROM test_data WHERE unicode_name LIKE ''%தமிழ்%'' AND unicode_city LIKE ''%東京%'' AND unicode_notes LIKE ''%东%''');

-- ============================================================
-- CAT-37: Whitespace and control character patterns
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: whitespace ---'; END; $$;

SELECT _run_comparison('whitespace','special_chars infix space',
  'SELECT id FROM test_data WHERE special_chars LIKE ''%   %''');
SELECT _run_comparison('whitespace','address infix Suite space',
  'SELECT id FROM test_data WHERE address LIKE ''% Suite %''');
SELECT _run_comparison('whitespace','notes infix space works',
  'SELECT id FROM test_data WHERE notes LIKE ''% works %''');
SELECT _run_comparison('whitespace','long_text infix space space',
  'SELECT id FROM test_data WHERE long_text LIKE ''%  %''');

-- ============================================================
-- CAT-38: Chained LIKE on same column
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: chained_same_col ---'; END; $$;

SELECT _run_comparison('chained_same_col','email gmail AND .com same col',
  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' AND email LIKE ''%.com''');
SELECT _run_comparison('chained_same_col','email prefix alice AND suffix .com',
  'SELECT id FROM test_data WHERE email LIKE ''alice%'' AND email LIKE ''%.com''');
SELECT _run_comparison('chained_same_col','fname prefix Al AND suffix ce',
  'SELECT id FROM test_data WHERE first_name LIKE ''Al%'' AND first_name LIKE ''%ce''');
SELECT _run_comparison('chained_same_col','company prefix Tech AND suffix Corp',
  'SELECT id FROM test_data WHERE company LIKE ''Tech%'' AND company LIKE ''%Corp''');
SELECT _run_comparison('chained_same_col','email three LIKE same col',
  'SELECT id FROM test_data WHERE email LIKE ''%alice%'' AND email LIKE ''%gmail%'' AND email LIKE ''%.com''');

-- ============================================================
-- CAT-39: Select column list (not just id)
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: select_cols ---'; END; $$;

SELECT _run_comparison('select_cols','all cols gmail',
  'SELECT id, first_name, last_name, email, city FROM test_data WHERE email LIKE ''%gmail%''');
SELECT _run_comparison('select_cols','all cols unicode',
  'SELECT id, unicode_name, unicode_city, mixed_email FROM test_data WHERE unicode_name LIKE ''%தமிழ்%''');
SELECT _run_comparison('select_cols','count star ilike',
  'SELECT id FROM test_data WHERE company ILIKE ''%corp%''');

-- ============================================================
-- CAT-40: Index drop/recreate consistency check
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: drop_recreate ---'; END; $$;

DO $$
DECLARE
  v_b_rows BIGINT;
  v_s_rows BIGINT;
  v_match  BOOLEAN;
BEGIN
  -- Baseline with indexes
  CREATE TEMP TABLE _dr_base AS
    SELECT id FROM test_data WHERE email LIKE '%gmail%';
  SELECT COUNT(*) INTO v_b_rows FROM _dr_base;

  -- Drop one index
  DROP INDEX IF EXISTS idx_biscuit_email;

  -- Run again (will use other indexes or seqscan)
  CREATE TEMP TABLE _dr_after_drop AS
    SELECT id FROM test_data WHERE email LIKE '%gmail%';
  SELECT COUNT(*) INTO v_s_rows FROM _dr_after_drop;

  -- Compare
  v_match := (v_b_rows = v_s_rows AND
    NOT EXISTS (SELECT id FROM _dr_base EXCEPT SELECT id FROM _dr_after_drop) AND
    NOT EXISTS (SELECT id FROM _dr_after_drop EXCEPT SELECT id FROM _dr_base));

  -- Recreate index
  CREATE INDEX idx_biscuit_email ON test_data USING biscuit(email);
  ANALYZE test_data;

  DROP TABLE _dr_base;
  DROP TABLE _dr_after_drop;

  IF v_match THEN
    INSERT INTO _results (category,test_name,query_text,biscuit_rows,seqscan_rows,row_match,status)
    VALUES ('drop_recreate','drop and recreate idx_biscuit_email',
            'email LIKE gmail before/after drop',v_b_rows,v_s_rows,true,'PASS');
    RAISE NOTICE 'PASS [drop_recreate] drop and recreate email index | rows=%', v_b_rows;
  ELSE
    INSERT INTO _results (category,test_name,query_text,biscuit_rows,seqscan_rows,row_match,status,detail)
    VALUES ('drop_recreate','drop and recreate idx_biscuit_email',
            'email LIKE gmail before/after drop',v_b_rows,v_s_rows,false,'FAIL',
            'Row mismatch after drop/recreate');
    RAISE WARNING 'FAIL [drop_recreate] Mismatch after index drop/recreate!';
  END IF;
END;
$$;

-- ============================================================
-- CAT-41: Bitmap scan / Index scan plan validation
-- ============================================================
DO $$ BEGIN RAISE NOTICE '--- Category: plan_validation ---'; END; $$;

DO $$
DECLARE
  v_plan TEXT;
  v_uses_index BOOLEAN;
  v_sql TEXT;
  tests TEXT[][] := ARRAY[
    ARRAY['email LIKE gmail',       'SELECT id FROM test_data WHERE email LIKE ''%gmail%'''],
    ARRAY['fname LIKE Alice',       'SELECT id FROM test_data WHERE first_name LIKE ''%Alice%'''],
    ARRAY['unicode Tamil',          'SELECT id FROM test_data WHERE unicode_name LIKE ''%தமிழ்%'''],
    ARRAY['multi AND email+fname',  'SELECT id FROM test_data WHERE email LIKE ''%gmail%'' AND first_name LIKE ''%Alice%''']
  ];
  t TEXT[];
BEGIN
  FOREACH t SLICE 1 IN ARRAY tests LOOP
    v_sql := t[2];
    BEGIN
      EXECUTE 'EXPLAIN (FORMAT TEXT) ' || v_sql INTO v_plan;
      v_uses_index := v_plan ILIKE '%biscuit%' OR v_plan ILIKE '%index%' OR v_plan ILIKE '%bitmap%';
    EXCEPTION WHEN OTHERS THEN
      v_uses_index := false;
      v_plan := 'EXPLAIN failed: ' || SQLERRM;
    END;
    INSERT INTO _results
      (category,test_name,query_text,index_used,status,detail,row_match)
    VALUES
      ('plan_validation', 'plan: ' || t[1], v_sql, v_uses_index,
       CASE WHEN v_uses_index THEN 'PASS' ELSE 'FAIL' END,
       left(v_plan, 500), v_uses_index);
    IF v_uses_index THEN
      RAISE NOTICE 'PASS [plan_validation] Index used for: %', t[1];
    ELSE
      RAISE WARNING 'FAIL [plan_validation] No index detected for: % | plan: %', t[1], left(v_plan,300);
    END IF;
  END LOOP;
END;
$$;

-- ============================================================
-- CAT-42: Deterministic permutation explosion
-- Generates 200+ structured permutations across all categories
-- ============================================================
-- DO $$ BEGIN RAISE NOTICE '--- Category: permutation_explosion ---'; END; $$;

-- DO $$
-- DECLARE
--   cols    TEXT[] := ARRAY['email','first_name','last_name','company','city',
--                            'unicode_name','unicode_city','mixed_email','notes'];
--   tokens  TEXT[] := ARRAY['%a%','%e%','%tech%','%corp%','%new%','%ork%',
--                            '%gmail%','%yahoo%','%alice%','%bob%',
--                            '%தமிழ்%','%こんにちは%','%你好%','%안녕%',
--                            '%مرحبا%','%😊%','%ork%','%example%'];
--   ops     TEXT[] := ARRAY['LIKE','LIKE','ILIKE','LIKE','ILIKE'];
--   boolops TEXT[] := ARRAY['AND','OR'];
--   c1 TEXT; c2 TEXT; t1 TEXT; t2 TEXT; o1 TEXT; o2 TEXT; b TEXT;
--   v_sql TEXT;
--   i INT := 0;
--   ci INT; ti INT; oi INT; bi INT; ci2 INT; ti2 INT; oi2 INT;
-- BEGIN
--   FOR ci IN 1..array_length(cols,1) LOOP
--   FOR ci2 IN 1..array_length(cols,1) LOOP
--     CONTINUE WHEN ci2 = ci;
--   FOR ti IN 1..array_length(tokens,1) LOOP
--   FOR ti2 IN 1..array_length(tokens,1) LOOP
--   FOR oi IN 1..array_length(ops,1) LOOP
--   FOR bi IN 1..array_length(boolops,1) LOOP
--     i := i + 1;
--     -- Limit to avoid millions of tests; take a deterministic sparse sample
--     CONTINUE WHEN (ci + ci2 + ti + ti2 + oi + bi) % 7 <> 0;

--     c1 := cols[ci]; c2 := cols[ci2];
--     t1 := tokens[ti]; t2 := tokens[ti2];
--     o1 := ops[oi];
--     o2 := ops[1 + ((oi + bi) % array_length(ops,1))];
--     b  := boolops[bi];

--     v_sql := format(
--       'SELECT id FROM test_data WHERE %I %s %L %s %I %s %L',
--       c1, o1, t1, b, c2, o2, t2
--     );

--     PERFORM _run_comparison(
--       'permutation_explosion',
--       format('perm_%s_%s_%s_%s_%s', i, c1, o1, b, c2),
--       v_sql
--     );
--   END LOOP;
--   END LOOP;
--   END LOOP;
--   END LOOP;
--   END LOOP;
--   END LOOP;
--   RAISE NOTICE 'Permutation explosion: % combinations evaluated', i;
-- END;
-- $$;

-- =============================================================================
-- SECTION 7: FINAL REPORTING
-- =============================================================================

DO $$
DECLARE
  v_total      BIGINT;
  v_pass       BIGINT;
  v_fail       BIGINT;
  v_pct        NUMERIC(6,2);
  v_fp_total   BIGINT;
  v_fn_total   BIGINT;
  r            RECORD;
BEGIN
  SELECT
    COUNT(*),
    COUNT(*) FILTER (WHERE status = 'PASS'),
    COUNT(*) FILTER (WHERE status = 'FAIL')
  INTO v_total, v_pass, v_fail
  FROM _results;

  v_pct := CASE WHEN v_total > 0
                THEN ROUND(v_pass * 100.0 / v_total, 2)
                ELSE 0 END;

  RAISE NOTICE '';
  RAISE NOTICE '=============================================================';
  RAISE NOTICE '  BISCUIT INDEX VALIDATION SUITE — FINAL REPORT';
  RAISE NOTICE '=============================================================';
  RAISE NOTICE '  Total tests  : %', v_total;
  RAISE NOTICE '  PASSED       : %', v_pass;
  RAISE NOTICE '  FAILED       : %', v_fail;
  RAISE NOTICE '  Accuracy     : %% %', v_pct;
  RAISE NOTICE '-------------------------------------------------------------';

  -- Per-category summary
  RAISE NOTICE '  Per-category breakdown:';
  FOR r IN
    SELECT category,
           COUNT(*)                                   AS total,
           COUNT(*) FILTER (WHERE status='PASS')      AS passed,
           COUNT(*) FILTER (WHERE status='FAIL')      AS failed
      FROM _results
     GROUP BY category
     ORDER BY category
  LOOP
    RAISE NOTICE '    %-35s total=% pass=% fail=%',
      r.category, r.total, r.passed, r.failed;
  END LOOP;

  RAISE NOTICE '-------------------------------------------------------------';

  -- Failures detail
  IF v_fail > 0 THEN
    RAISE NOTICE '  FAILING TESTS:';
    FOR r IN
      SELECT res.category, res.test_name, res.status, res.detail,
             res.biscuit_rows, res.seqscan_rows,
             f.missing_ids, f.extra_ids
        FROM _results res
        LEFT JOIN _failures f ON f.result_id = res.id
       WHERE res.status = 'FAIL'
       ORDER BY res.id
    LOOP
      RAISE NOTICE '    [FAIL] [%] %', r.category, r.test_name;
      RAISE NOTICE '           detail     : %', r.detail;
      RAISE NOTICE '           biscuit_rows=%  seqscan_rows=%',
        r.biscuit_rows, r.seqscan_rows;
      IF r.missing_ids IS NOT NULL THEN
        RAISE NOTICE '           false_negatives(IDs): %', left(r.missing_ids, 200);
      END IF;
      IF r.extra_ids IS NOT NULL THEN
        RAISE NOTICE '           false_positives(IDs): %', left(r.extra_ids, 200);
      END IF;
    END LOOP;
  END IF;

  RAISE NOTICE '=============================================================';
  IF v_fail = 0 THEN
    RAISE NOTICE '  *** ALL TESTS PASSED — Biscuit index is CORRECT ***';
  ELSE
    RAISE NOTICE '  *** % TEST(S) FAILED — Investigate Biscuit index ***', v_fail;
  END IF;
  RAISE NOTICE '=============================================================';
END;
$$;

-- Expose result tables for manual inspection
SELECT
  category,
  test_name,
  status,
  biscuit_rows,
  seqscan_rows,
  ROUND(biscuit_ms,2) AS biscuit_ms,
  ROUND(seqscan_ms,2) AS seqscan_ms,
  index_used,
  detail
FROM _results
ORDER BY id;

-- Failure detail rows (empty if all pass)
SELECT
  r.id,
  r.category,
  r.test_name,
  r.biscuit_rows,
  r.seqscan_rows,
  f.missing_ids,
  f.extra_ids,
  f.detail
FROM _results r
JOIN _failures f ON f.result_id = r.id
ORDER BY r.id;

-- =============================================================================
-- SECTION 8: CLEANUP (comment out if you want to retain tables for debugging)
-- =============================================================================

-- DROP TABLE IF EXISTS test_data CASCADE;
-- DROP TABLE IF EXISTS _results;
-- DROP TABLE IF EXISTS _failures;
-- DROP TABLE IF EXISTS _cfg;

DO $$
BEGIN
  RAISE NOTICE 'Suite complete. Results retained in _results and _failures temp tables.';
  RAISE NOTICE 'To clean up: DROP TABLE test_data CASCADE; and drop temp tables.';
END;
$$;