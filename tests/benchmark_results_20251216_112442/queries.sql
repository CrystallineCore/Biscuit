-- ============================================================================
-- COMPREHENSIVE WILDCARD PATTERN MATCHING BENCHMARK
-- Table: interactions (1M rows)
-- Test Coverage: 100+ query patterns across all LIKE/ILIKE combinations
-- ============================================================================

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET work_mem = '256MB';
SET random_page_cost = 1.1;

-- WARMUP: Load index into cache
SELECT COUNT(*) FROM interactions WHERE country LIKE 'United%';

-- ============================================================================
-- SECTION 1: BASIC LIKE PATTERNS (Prefix, Suffix, Infix)
-- ============================================================================

-- PREFIX PATTERNS (pattern%)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Uni%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'So%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'david%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'kelly%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'And%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'i%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE interaction_type LIKE 'com%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE interaction_type LIKE 'post%';

-- SUFFIX PATTERNS (%pattern)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%stan';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%land';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%smith';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%son';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%er';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%oid';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%eb';

-- INFIX PATTERNS (%pattern%)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%united%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%island%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%rica%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%alex%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%john%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%mill%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%dr%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%eb%';

-- ============================================================================
-- SECTION 2: UNDERSCORE WILDCARDS (_)
-- ============================================================================

-- Single underscore
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Ja_an';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '_ndia';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'i_S';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'We_';

-- Multiple underscores
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'S___h%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Ke__a';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'd_v_d%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'k___y__';

-- Mixed wildcards (% and _)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Bo%_a';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%_lex%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%j_nes';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%m_yer%';

-- ============================================================================
-- SECTION 3: CASE INSENSITIVE (ILIKE)
-- ============================================================================

-- ILIKE prefix
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'japan';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'KENYA';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'uNiTeD%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE 'DAVID%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE 'kelly%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE 'android';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE 'IOS';

-- ILIKE suffix
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE '%africa';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE '%STAN';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '%MILLER';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '%smith';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE '%OID';

-- ILIKE infix
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE '%united%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE '%ISLAND%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '%ALEX%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '%john%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE '%web%';

-- ============================================================================
-- SECTION 4: NEGATION (NOT LIKE / NOT ILIKE)
-- ============================================================================

-- NOT LIKE prefix
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE 'Uni%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE 'david%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device NOT LIKE 'And%';

-- NOT LIKE suffix
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE '%stan';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE '%ia';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE '%son';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE '%er';

-- NOT LIKE infix
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE '%land%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE '%Africa%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE '%admin%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE '%test%';

-- NOT ILIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT ILIKE '%africa%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT ILIKE 'united%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device NOT ILIKE 'iOS';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT ILIKE '%admin%';

-- ============================================================================
-- SECTION 5: AND COMBINATIONS (2 predicates)
-- ============================================================================

-- LIKE AND LIKE (same pattern type)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Jap%' AND username LIKE 'david%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' AND device LIKE '%oid';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%ell%' AND device LIKE '%dr%';

-- LIKE AND LIKE (mixed pattern types)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Uni%' AND username LIKE '%smith';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%land' AND username LIKE 'kelly%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'david%' AND device LIKE '%oid';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' AND device LIKE 'iOS';

-- LIKE AND NOT LIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' AND country NOT LIKE 'India';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'da%' AND username NOT LIKE '%vid';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'United%' AND country NOT LIKE '%States%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%oid' AND device NOT LIKE 'Android';

-- ILIKE AND ILIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'japan' AND device ILIKE 'ios';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'SOUTH%' AND device ILIKE '%android%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE 'david%' AND username ILIKE '%john%';

-- LIKE AND ILIKE (mixed)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' AND username ILIKE '%kelly%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'united%' AND device LIKE 'iOS';

-- LIKE AND non-LIKE conditions
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' AND is_premium = 1;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%alex%' AND age BETWEEN 25 AND 35;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'iOS' AND interaction_type = 'post';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Africa%' AND is_premium = 0;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'david%' AND engagement_score > 0.5;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'United%' AND timestamp >= '2025-01-01';

-- NOT LIKE AND NOT LIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE 'United%' AND username NOT LIKE '%admin%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device NOT LIKE 'Web' AND country NOT LIKE '%ia';

-- ============================================================================
-- SECTION 6: AND COMBINATIONS (3+ predicates)
-- ============================================================================

-- Three LIKE predicates
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'S%' AND username LIKE '%a%' AND device LIKE 'iOS';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' AND username LIKE '%son' AND interaction_type LIKE '%st';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'j%' AND country LIKE '%ia' AND device LIKE '%eb';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Uni%' AND username LIKE '%smith' AND device LIKE 'Web';

-- Three LIKE predicates (mixed patterns)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan%' AND username LIKE '%kelly%' AND device LIKE '%oid%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%ell%' AND country LIKE '%land%' AND device LIKE 'i%';

-- LIKE with multiple non-LIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' AND is_premium = 1 AND age > 30;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%alex%' AND age BETWEEN 25 AND 35 AND device = 'iOS';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Africa%' AND interaction_type = 'post' AND engagement_score > 0.7;

-- Four predicates
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' AND username LIKE '%jones%' AND device LIKE '%oid' AND interaction_type LIKE 'com%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'S%' AND username LIKE 's%' AND device LIKE 'iOS' AND interaction_type LIKE '%st';

-- ============================================================================
-- SECTION 7: OR COMBINATIONS (2 predicates)
-- ============================================================================

-- LIKE OR LIKE (same pattern type - prefix)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' OR country LIKE 'Kenya';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'david%' OR username LIKE 'kelly%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'iOS' OR device LIKE 'Android';

-- LIKE OR LIKE (same pattern type - suffix)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' OR country LIKE '%land';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%son' OR username LIKE '%ton';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%oid' OR device LIKE '%eb';

-- LIKE OR LIKE (mixed pattern types)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Uni%' OR country LIKE '%stan';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'david%' OR username LIKE '%miller';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan%' OR country LIKE '%Africa%';

-- LIKE OR NOT LIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'United%' OR country NOT LIKE '%Africa%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'admin%' OR username NOT LIKE '%test%';

-- ILIKE OR ILIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'japan' OR country ILIKE 'india';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE 'ios' OR device ILIKE 'android';

-- LIKE OR non-LIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' OR is_premium = 1;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%alex%' OR age > 50;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'iOS' OR engagement_score > 0.8;

-- ============================================================================
-- SECTION 8: OR COMBINATIONS (3+ predicates)
-- ============================================================================

-- Three LIKE predicates
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' OR country LIKE 'Kenya' OR country LIKE 'Yemen';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'iOS' OR device LIKE 'Android' OR device LIKE 'Web';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'david%' OR username LIKE 'kelly%' OR username LIKE 'alex%';

-- Three mixed patterns
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' OR country LIKE '%Africa%' OR country LIKE '%island%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'admin%' OR username LIKE '%test%' OR username LIKE '%demo%';

-- Four predicates
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' OR country LIKE 'Kenya' OR country LIKE 'India' OR country LIKE 'Yemen';

-- ============================================================================
-- SECTION 9: COMPLEX NESTED CONDITIONS
-- ============================================================================

-- (LIKE OR LIKE) AND LIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country LIKE 'Japan' OR country LIKE 'Kenya') AND username LIKE '%a%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (device LIKE 'iOS' OR device LIKE 'Android') AND interaction_type LIKE 'post';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (username LIKE 'david%' OR username LIKE 'kelly%') AND country LIKE '%ia';

-- LIKE AND (LIKE OR LIKE)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' AND (username LIKE 'david%' OR username LIKE 'kelly%');
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%er' AND (country LIKE 'United%' OR country LIKE 'South%');
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'iOS' AND (country LIKE 'Japan' OR country LIKE 'Kenya');

-- (LIKE AND LIKE) OR (LIKE AND LIKE)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country LIKE 'Japan' AND device LIKE 'iOS') OR (country LIKE 'Kenya' AND device LIKE 'Android');
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (username LIKE 'david%' AND is_premium = 1) OR (username LIKE 'kelly%' AND is_premium = 1);
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country LIKE '%Africa%' AND device LIKE 'Android') OR (country LIKE '%Asia%' AND device LIKE 'iOS');

-- (LIKE OR LIKE) AND (LIKE OR LIKE)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country LIKE 'Japan' OR country LIKE 'Kenya') AND (device LIKE 'iOS' OR device LIKE 'Android');
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (username LIKE 'david%' OR username LIKE 'kelly%') AND (country LIKE '%ia' OR country LIKE '%land');

-- NOT LIKE combinations
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country NOT LIKE 'United%' AND username NOT LIKE '%admin%');
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country LIKE 'Japan' OR country NOT LIKE '%ia') AND (device LIKE 'iOS' OR device NOT LIKE 'Web');

-- Deep nesting
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE ((country LIKE 'Japan' OR country LIKE 'Kenya') AND username LIKE '%a%') OR (device LIKE 'iOS' AND interaction_type LIKE 'post');

-- ============================================================================
-- SECTION 10: EDGE CASES AND SPECIAL PATTERNS
-- ============================================================================

-- Very short patterns (low selectivity - many matches)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'M%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'A%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'a%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 's%';

-- Very broad infix patterns
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%a%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%a%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%e%';

-- Exact match (degenerate LIKE - no wildcards)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Kenya';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'Android';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'Web';

-- Empty result patterns
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Penguin';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'nonexistent%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%zzzzzzz%';

-- Universal patterns (matches all or most rows)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%%';

-- Rare long patterns
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Barthelemy%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'French Southern Territories';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Cocos (Keeling)%';

-- Multiple consecutive underscores
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '_______';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '__________';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '________76';

-- ============================================================================
-- SECTION 11: REAL-WORLD QUERY PATTERNS
-- ============================================================================

-- User search patterns
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%john%' OR username LIKE '%david%' OR username LIKE '%alex%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (username LIKE 'admin%' OR username LIKE 'moderator%') AND is_premium = 1;

-- Geographic searches
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%island%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%United%' OR country LIKE '%Kingdom%' OR country LIKE '%States%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Africa%' AND device LIKE 'Android';

-- Content moderation patterns
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (username LIKE '%spam%' OR username LIKE '%bot%') AND toxicity_score > 0.8;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE interaction_type LIKE 'comment' AND username NOT LIKE '%verified%' AND suspicious_score > 0.5;

-- Analytics queries
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'United%' AND interaction_type LIKE 'post' AND timestamp >= '2025-01-01';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%Android%' AND engagement_score > 0.5 AND is_premium = 1;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%kelly%' AND age BETWEEN 30 AND 50 AND country LIKE '%ia';

-- Multi-device user tracking
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (device LIKE 'iOS' OR device LIKE 'Android') AND interaction_type LIKE 'post' AND timestamp >= '2024-01-01';

-- Premium user analysis
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Africa%' AND is_premium = 1 AND engagement_score > 0.7;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country LIKE 'United%' OR country LIKE 'South%') AND is_premium = 0 AND interaction_type LIKE 'like';

-- ============================================================================
-- SECTION 12: SELECTIVITY SPECTRUM (Critical for fair comparison)
-- ============================================================================

-- Ultra-high selectivity (0.001% - 0.01% of rows) - 1-100 matches
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Saint Barthélemy';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Cocos (Keeling) Islands%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'administrator%';

-- High selectivity (0.01% - 0.1% of rows) - 100-1000 matches
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Uzbek%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'zach%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%istan';

-- Medium selectivity (0.1% - 1% of rows) - 1000-10000 matches
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'United%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'john%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%land';

-- Low selectivity (1% - 10% of rows) - 10000-100000 matches
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'S%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'a%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%a%';

-- Very low selectivity (10%+ of rows) - 100000+ matches
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%e%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%a%' OR country LIKE '%e%';

-- ============================================================================
-- SECTION 13: LONG PATTERNS
-- ============================================================================

EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'United States of America%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%French Southern Territories%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Saint Vincent and the Grenadines%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Democratic People''s Republic%';

-- ============================================================================
-- SECTION 14: SPECIAL CHARACTERS & ESCAPING
-- ============================================================================

EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%(%)%';  -- Parentheses
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%-%';     -- Hyphens
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%.%';     -- Dots
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%''%';    -- Apostrophes

-- ============================================================================
-- SECTION 15: LIMIT + ORDER BY (Real-world pagination)
-- ============================================================================

EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'dav%' ORDER BY username LIMIT 10;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' ORDER BY timestamp LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'iOS' ORDER BY engagement_score DESC LIMIT 100;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'United%' ORDER BY id LIMIT 20 OFFSET 100;

-- ============================================================================
-- END OF COMPREHENSIVE BENCHMARK QUERIES
-- Total: 200+ test cases covering all pattern matching scenarios
-- ============================================================================
