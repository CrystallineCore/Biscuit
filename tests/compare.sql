set enable_seqscan=off;
SET enable_bitmapscan = off;

create index int_bisc on interactions using biscuit(interaction_type, username, country, device);
SELECT biscuit_size_pretty('int_bisc');

DO $$
BEGIN
RAISE NOTICE '---------------------------Biscuit index--------------------------------------------';
END $$;

select * from interactions where country like '_'; -- To warmup
-- ============================================
-- PREFIX QUERIES (LIKE 'pattern%')
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Uni%';
explain (analyze, buffers) select * from interactions where country like 'So%';
explain (analyze, buffers) select * from interactions where username like 'david%';
explain (analyze, buffers) select * from interactions where username like 'sull%';
explain (analyze, buffers) select * from interactions where device like 'And%';
explain (analyze, buffers) select * from interactions where device like 'i%';
explain (analyze, buffers) select * from interactions where country like 'A%';
explain (analyze, buffers) select * from interactions where username like 'j%';

-- ============================================
-- SUFFIX QUERIES (LIKE '%pattern')
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%stan';
explain (analyze, buffers) select * from interactions where country like '%land';
explain (analyze, buffers) select * from interactions where username like '%smith';
explain (analyze, buffers) select * from interactions where username like '%meyer';
explain (analyze, buffers) select * from interactions where username like '%son';
explain (analyze, buffers) select * from interactions where device like '%oid';
explain (analyze, buffers) select * from interactions where device like '%eb';

-- ============================================
-- INFIX QUERIES (LIKE '%pattern%')
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%united%';
explain (analyze, buffers) select * from interactions where country like '%island%';
explain (analyze, buffers) select * from interactions where username like '%alex%';
explain (analyze, buffers) select * from interactions where username like '%pher%';
explain (analyze, buffers) select * from interactions where username like '%mill%';
explain (analyze, buffers) select * from interactions where device like '%dr%';
explain (analyze, buffers) select * from interactions where country like '%rica%';
explain (analyze, buffers) select * from interactions where username like '%ell%';

-- ============================================
-- UNDERSCORE WILDCARD QUERIES (LIKE patterns with _)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Ja_an';
explain (analyze, buffers) select * from interactions where country like 'Ke__a';
explain (analyze, buffers) select * from interactions where username like 'd_vid%';
explain (analyze, buffers) select * from interactions where username like '%j_nes';
explain (analyze, buffers) select * from interactions where username like 's__th';
explain (analyze, buffers) select * from interactions where username like '%m_yer%';
explain (analyze, buffers) select * from interactions where device like '_OS';
explain (analyze, buffers) select * from interactions where device like 'We_';
explain (analyze, buffers) select * from interactions where country like '_ndia';
explain (analyze, buffers) select * from interactions where username like 'k___y__';

-- ============================================
-- CASE-INSENSITIVE QUERIES (ILIKE)
-- ============================================
explain (analyze, buffers) select * from interactions where country ilike 'japan';
explain (analyze, buffers) select * from interactions where country ilike 'KENYA';
explain (analyze, buffers) select * from interactions where country ilike 'sOuTh%';
explain (analyze, buffers) select * from interactions where username ilike 'DAVID%';
explain (analyze, buffers) select * from interactions where username ilike '%JONES%';
explain (analyze, buffers) select * from interactions where device ilike 'android';
explain (analyze, buffers) select * from interactions where device ilike '%WEB%';

-- ============================================
-- AND COMBINATIONS (2 conditions)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Jap%' and username like '%kelly%';
explain (analyze, buffers) select * from interactions where country like '%Africa%' and device like 'And%';
explain (analyze, buffers) select * from interactions where username like 'david%' and device like '%oid';
explain (analyze, buffers) select * from interactions where country like '%ia' and device like 'iOS';
explain (analyze, buffers) select * from interactions where username like '%son' and country like 'United%';
explain (analyze, buffers) select * from interactions where device like 'Web' and username like 's%';
explain (analyze, buffers) select * from interactions where country like '%land' and username like '%er';
explain (analyze, buffers) select * from interactions where username like '%ran%' and device like '%dr%';

-- ============================================
-- AND COMBINATIONS (3 conditions)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'S%' and username like '%a%' and device like 'iOS';
explain (analyze, buffers) select * from interactions where country like '%stan' and username like '%ali%' and device like 'Android';
explain (analyze, buffers) select * from interactions where username like 'j%' and country like '%ia' and device like '%eb';
explain (analyze, buffers) select * from interactions where country like 'Uni%' and username like '%smith' and device like 'Web';
explain (analyze, buffers) select * from interactions where username like '%ell%' and country like '%land%' and device like 'i%';
explain (analyze, buffers) select * from interactions where country like '%Republic%' and username like 'd%' and device like '%oid';

-- ============================================
-- OR COMBINATIONS (2 conditions)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Japan' or username like 'kelly%';
explain (analyze, buffers) select * from interactions where country like '%ia' or device like 'Web';
explain (analyze, buffers) select * from interactions where username like '%david%' or country like 'Kenya';
explain (analyze, buffers) select * from interactions where device like 'iOS' or username like '%miller%';
explain (analyze, buffers) select * from interactions where country like 'So%' or username like '%son';
explain (analyze, buffers) select * from interactions where username like '%ran%' or device like '%droid';
explain (analyze, buffers) select * from interactions where country like '%land' or device like 'And%';

-- ============================================
-- OR COMBINATIONS (3 conditions)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'India' or username like '%jones%' or device like 'Web';
explain (analyze, buffers) select * from interactions where country like '%ia' or username like 's%' or device like 'iOS';
explain (analyze, buffers) select * from interactions where username like '%smith' or country like 'Uni%' or device like '%oid';
explain (analyze, buffers) select * from interactions where device like 'Web' or country like '%Africa%' or username like '%ell%';

-- ============================================
-- MIXED AND/OR COMBINATIONS
-- ============================================
explain (analyze, buffers) select * from interactions where (country like 'Jap%' or country like 'Ken%') and username like '%a%';
explain (analyze, buffers) select * from interactions where country like '%ia' and (username like 'david%' or username like 'kelly%');
explain (analyze, buffers) select * from interactions where (country like 'S%' and device like 'iOS') or username like '%jones%';
explain (analyze, buffers) select * from interactions where username like '%ran%' and (country like '%land' or device like 'Android');
explain (analyze, buffers) select * from interactions where (country like '%ia' or country like '%land') and (username like '%son' or username like '%er');
explain (analyze, buffers) select * from interactions where device like 'Web' or (country like 'Uni%' and username like 's%');
explain (analyze, buffers) select * from interactions where (username like '%smith%' and device like 'iOS') or (country like '%Africa%' and device like 'Android');

-- ============================================
-- NON-EXISTENT PATTERN QUERIES
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Atlantis';
explain (analyze, buffers) select * from interactions where country like 'Xyz%';
explain (analyze, buffers) select * from interactions where username like 'nonexistent%';
explain (analyze, buffers) select * from interactions where username like '%zzzzz%';
explain (analyze, buffers) select * from interactions where device like 'Blackberry';
explain (analyze, buffers) select * from interactions where country like '%Wonderland%';
explain (analyze, buffers) select * from interactions where username like '%qwerty123%';

-- ============================================
-- COMPLEX UNDERSCORE PATTERNS
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'S___h%';
explain (analyze, buffers) select * from interactions where username like '%d_v_d%';
explain (analyze, buffers) select * from interactions where username like 'k____76';
explain (analyze, buffers) select * from interactions where country like '_a__';
explain (analyze, buffers) select * from interactions where username like '%j_n_s';
explain (analyze, buffers) select * from interactions where country like 'B_l_v_a';

-- ============================================
-- VERY SHORT PATTERNS
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'M%';
explain (analyze, buffers) select * from interactions where country like '%i';
explain (analyze, buffers) select * from interactions where username like 'a%';
explain (analyze, buffers) select * from interactions where device like '%S';
explain (analyze, buffers) select * from interactions where username like '%e';

-- ============================================
-- EXACT MATCH (no wildcards for comparison)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Japan';
explain (analyze, buffers) select * from interactions where country like 'Kenya';
explain (analyze, buffers) select * from interactions where device like 'Web';
explain (analyze, buffers) select * from interactions where device like 'Android';

-- ============================================
-- COMBINED WILDCARD TYPES
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%ia' and username like 'd_vid%';
explain (analyze, buffers) select * from interactions where username like '%j_nes%' and device like 'i%';
explain (analyze, buffers) select * from interactions where country like 'S_uth%' and username like '%son';
explain (analyze, buffers) select * from interactions where username like '%ran%' and country like '_ndia';

-- ============================================
-- MULTIPLE AND CONDITIONS (4+)
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%ia' and username like '%jones%' and device like '%oid' and interaction_type like 'com%';
explain (analyze, buffers) select * from interactions where country like 'S%' and username like 's%' and device like 'iOS' and interaction_type like '%st';
explain (analyze, buffers) select * from interactions where username like '%ell%' and country like '%land' and device like 'Web' and interaction_type like '%ke';

-- ============================================
-- PERFORMANCE EDGE CASES
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%%';
explain (analyze, buffers) select * from interactions where username like '%';
explain (analyze, buffers) select * from interactions where country like '%a%' and username like '%a%';
explain (analyze, buffers) select * from interactions where country like '%e%' or username like '%e%' or device like '%e%';
explain (analyze, buffers) select * from interactions where username like '%o%' and country like '%o%' and device like '%o%';
drop index int_bisc;














set enable_seqscan=off;
SET enable_bitmapscan = off;
CREATE INDEX int_trgm ON interactions USING gin ( interaction_type gin_trgm_ops, username gin_trgm_ops, country gin_trgm_ops, device gin_trgm_ops );
SELECT pg_size_pretty(pg_relation_size('int_trgm'));

DO $$
BEGIN
RAISE NOTICE '---------------------------Trigram index--------------------------------------------';
END $$;

select * from interactions where country like '_'; -- To warmup

-- ============================================
-- PREFIX QUERIES (LIKE 'pattern%')
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Uni%';
explain (analyze, buffers) select * from interactions where country like 'So%';
explain (analyze, buffers) select * from interactions where username like 'david%';
explain (analyze, buffers) select * from interactions where username like 'sull%';
explain (analyze, buffers) select * from interactions where device like 'And%';
explain (analyze, buffers) select * from interactions where device like 'i%';
explain (analyze, buffers) select * from interactions where country like 'A%';
explain (analyze, buffers) select * from interactions where username like 'j%';

-- ============================================
-- SUFFIX QUERIES (LIKE '%pattern')
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%stan';
explain (analyze, buffers) select * from interactions where country like '%land';
explain (analyze, buffers) select * from interactions where username like '%smith';
explain (analyze, buffers) select * from interactions where username like '%meyer';
explain (analyze, buffers) select * from interactions where username like '%son';
explain (analyze, buffers) select * from interactions where device like '%oid';
explain (analyze, buffers) select * from interactions where device like '%eb';

-- ============================================
-- INFIX QUERIES (LIKE '%pattern%')
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%united%';
explain (analyze, buffers) select * from interactions where country like '%island%';
explain (analyze, buffers) select * from interactions where username like '%alex%';
explain (analyze, buffers) select * from interactions where username like '%pher%';
explain (analyze, buffers) select * from interactions where username like '%mill%';
explain (analyze, buffers) select * from interactions where device like '%dr%';
explain (analyze, buffers) select * from interactions where country like '%rica%';
explain (analyze, buffers) select * from interactions where username like '%ell%';

-- ============================================
-- UNDERSCORE WILDCARD QUERIES (LIKE patterns with _)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Ja_an';
explain (analyze, buffers) select * from interactions where country like 'Ke__a';
explain (analyze, buffers) select * from interactions where username like 'd_vid%';
explain (analyze, buffers) select * from interactions where username like '%j_nes';
explain (analyze, buffers) select * from interactions where username like 's__th';
explain (analyze, buffers) select * from interactions where username like '%m_yer%';
explain (analyze, buffers) select * from interactions where device like '_OS';
explain (analyze, buffers) select * from interactions where device like 'We_';
explain (analyze, buffers) select * from interactions where country like '_ndia';
explain (analyze, buffers) select * from interactions where username like 'k___y__';

-- ============================================
-- CASE-INSENSITIVE QUERIES (ILIKE)
-- ============================================
explain (analyze, buffers) select * from interactions where country ilike 'japan';
explain (analyze, buffers) select * from interactions where country ilike 'KENYA';
explain (analyze, buffers) select * from interactions where country ilike 'sOuTh%';
explain (analyze, buffers) select * from interactions where username ilike 'DAVID%';
explain (analyze, buffers) select * from interactions where username ilike '%JONES%';
explain (analyze, buffers) select * from interactions where device ilike 'android';
explain (analyze, buffers) select * from interactions where device ilike '%WEB%';

-- ============================================
-- AND COMBINATIONS (2 conditions)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Jap%' and username like '%kelly%';
explain (analyze, buffers) select * from interactions where country like '%Africa%' and device like 'And%';
explain (analyze, buffers) select * from interactions where username like 'david%' and device like '%oid';
explain (analyze, buffers) select * from interactions where country like '%ia' and device like 'iOS';
explain (analyze, buffers) select * from interactions where username like '%son' and country like 'United%';
explain (analyze, buffers) select * from interactions where device like 'Web' and username like 's%';
explain (analyze, buffers) select * from interactions where country like '%land' and username like '%er';
explain (analyze, buffers) select * from interactions where username like '%ran%' and device like '%dr%';

-- ============================================
-- AND COMBINATIONS (3 conditions)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'S%' and username like '%a%' and device like 'iOS';
explain (analyze, buffers) select * from interactions where country like '%stan' and username like '%ali%' and device like 'Android';
explain (analyze, buffers) select * from interactions where username like 'j%' and country like '%ia' and device like '%eb';
explain (analyze, buffers) select * from interactions where country like 'Uni%' and username like '%smith' and device like 'Web';
explain (analyze, buffers) select * from interactions where username like '%ell%' and country like '%land%' and device like 'i%';
explain (analyze, buffers) select * from interactions where country like '%Republic%' and username like 'd%' and device like '%oid';

-- ============================================
-- OR COMBINATIONS (2 conditions)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Japan' or username like 'kelly%';
explain (analyze, buffers) select * from interactions where country like '%ia' or device like 'Web';
explain (analyze, buffers) select * from interactions where username like '%david%' or country like 'Kenya';
explain (analyze, buffers) select * from interactions where device like 'iOS' or username like '%miller%';
explain (analyze, buffers) select * from interactions where country like 'So%' or username like '%son';
explain (analyze, buffers) select * from interactions where username like '%ran%' or device like '%droid';
explain (analyze, buffers) select * from interactions where country like '%land' or device like 'And%';

-- ============================================
-- OR COMBINATIONS (3 conditions)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'India' or username like '%jones%' or device like 'Web';
explain (analyze, buffers) select * from interactions where country like '%ia' or username like 's%' or device like 'iOS';
explain (analyze, buffers) select * from interactions where username like '%smith' or country like 'Uni%' or device like '%oid';
explain (analyze, buffers) select * from interactions where device like 'Web' or country like '%Africa%' or username like '%ell%';

-- ============================================
-- MIXED AND/OR COMBINATIONS
-- ============================================
explain (analyze, buffers) select * from interactions where (country like 'Jap%' or country like 'Ken%') and username like '%a%';
explain (analyze, buffers) select * from interactions where country like '%ia' and (username like 'david%' or username like 'kelly%');
explain (analyze, buffers) select * from interactions where (country like 'S%' and device like 'iOS') or username like '%jones%';
explain (analyze, buffers) select * from interactions where username like '%ran%' and (country like '%land' or device like 'Android');
explain (analyze, buffers) select * from interactions where (country like '%ia' or country like '%land') and (username like '%son' or username like '%er');
explain (analyze, buffers) select * from interactions where device like 'Web' or (country like 'Uni%' and username like 's%');
explain (analyze, buffers) select * from interactions where (username like '%smith%' and device like 'iOS') or (country like '%Africa%' and device like 'Android');

-- ============================================
-- NON-EXISTENT PATTERN QUERIES
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Atlantis';
explain (analyze, buffers) select * from interactions where country like 'Xyz%';
explain (analyze, buffers) select * from interactions where username like 'nonexistent%';
explain (analyze, buffers) select * from interactions where username like '%zzzzz%';
explain (analyze, buffers) select * from interactions where device like 'Blackberry';
explain (analyze, buffers) select * from interactions where country like '%Wonderland%';
explain (analyze, buffers) select * from interactions where username like '%qwerty123%';

-- ============================================
-- COMPLEX UNDERSCORE PATTERNS
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'S___h%';
explain (analyze, buffers) select * from interactions where username like '%d_v_d%';
explain (analyze, buffers) select * from interactions where username like 'k____76';
explain (analyze, buffers) select * from interactions where country like '_a__';
explain (analyze, buffers) select * from interactions where username like '%j_n_s';
explain (analyze, buffers) select * from interactions where country like 'B_l_v_a';

-- ============================================
-- VERY SHORT PATTERNS
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'M%';
explain (analyze, buffers) select * from interactions where country like '%i';
explain (analyze, buffers) select * from interactions where username like 'a%';
explain (analyze, buffers) select * from interactions where device like '%S';
explain (analyze, buffers) select * from interactions where username like '%e';

-- ============================================
-- EXACT MATCH (no wildcards for comparison)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Japan';
explain (analyze, buffers) select * from interactions where country like 'Kenya';
explain (analyze, buffers) select * from interactions where device like 'Web';
explain (analyze, buffers) select * from interactions where device like 'Android';

-- ============================================
-- COMBINED WILDCARD TYPES
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%ia' and username like 'd_vid%';
explain (analyze, buffers) select * from interactions where username like '%j_nes%' and device like 'i%';
explain (analyze, buffers) select * from interactions where country like 'S_uth%' and username like '%son';
explain (analyze, buffers) select * from interactions where username like '%ran%' and country like '_ndia';

-- ============================================
-- MULTIPLE AND CONDITIONS (4+)
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%ia' and username like '%jones%' and device like '%oid' and interaction_type like 'com%';
explain (analyze, buffers) select * from interactions where country like 'S%' and username like 's%' and device like 'iOS' and interaction_type like '%st';
explain (analyze, buffers) select * from interactions where username like '%ell%' and country like '%land' and device like 'Web' and interaction_type like '%ke';

-- ============================================
-- PERFORMANCE EDGE CASES
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%%';
explain (analyze, buffers) select * from interactions where username like '%';
explain (analyze, buffers) select * from interactions where country like '%a%' and username like '%a%';
explain (analyze, buffers) select * from interactions where country like '%e%' or username like '%e%' or device like '%e%';
explain (analyze, buffers) select * from interactions where username like '%o%' and country like '%o%' and device like '%o%';

drop index int_trgm;

















set enable_seqscan=off;
SET enable_bitmapscan = off;
CREATE INDEX int_tree ON interactions( interaction_type text_pattern_ops, username text_pattern_ops, country text_pattern_ops, device text_pattern_ops );
SELECT pg_size_pretty(pg_relation_size('int_tree'));

DO $$
BEGIN
RAISE NOTICE '---------------------------Tree index--------------------------------------------';
END $$;

select * from interactions where country like '_'; -- To warmup

-- ============================================
-- PREFIX QUERIES (LIKE 'pattern%')
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Uni%';
explain (analyze, buffers) select * from interactions where country like 'So%';
explain (analyze, buffers) select * from interactions where username like 'david%';
explain (analyze, buffers) select * from interactions where username like 'sull%';
explain (analyze, buffers) select * from interactions where device like 'And%';
explain (analyze, buffers) select * from interactions where device like 'i%';
explain (analyze, buffers) select * from interactions where country like 'A%';
explain (analyze, buffers) select * from interactions where username like 'j%';

-- ============================================
-- SUFFIX QUERIES (LIKE '%pattern')
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%stan';
explain (analyze, buffers) select * from interactions where country like '%land';
explain (analyze, buffers) select * from interactions where username like '%smith';
explain (analyze, buffers) select * from interactions where username like '%meyer';
explain (analyze, buffers) select * from interactions where username like '%son';
explain (analyze, buffers) select * from interactions where device like '%oid';
explain (analyze, buffers) select * from interactions where device like '%eb';

-- ============================================
-- INFIX QUERIES (LIKE '%pattern%')
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%united%';
explain (analyze, buffers) select * from interactions where country like '%island%';
explain (analyze, buffers) select * from interactions where username like '%alex%';
explain (analyze, buffers) select * from interactions where username like '%pher%';
explain (analyze, buffers) select * from interactions where username like '%mill%';
explain (analyze, buffers) select * from interactions where device like '%dr%';
explain (analyze, buffers) select * from interactions where country like '%rica%';
explain (analyze, buffers) select * from interactions where username like '%ell%';

-- ============================================
-- UNDERSCORE WILDCARD QUERIES (LIKE patterns with _)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Ja_an';
explain (analyze, buffers) select * from interactions where country like 'Ke__a';
explain (analyze, buffers) select * from interactions where username like 'd_vid%';
explain (analyze, buffers) select * from interactions where username like '%j_nes';
explain (analyze, buffers) select * from interactions where username like 's__th';
explain (analyze, buffers) select * from interactions where username like '%m_yer%';
explain (analyze, buffers) select * from interactions where device like '_OS';
explain (analyze, buffers) select * from interactions where device like 'We_';
explain (analyze, buffers) select * from interactions where country like '_ndia';
explain (analyze, buffers) select * from interactions where username like 'k___y__';

-- ============================================
-- CASE-INSENSITIVE QUERIES (ILIKE)
-- ============================================
explain (analyze, buffers) select * from interactions where country ilike 'japan';
explain (analyze, buffers) select * from interactions where country ilike 'KENYA';
explain (analyze, buffers) select * from interactions where country ilike 'sOuTh%';
explain (analyze, buffers) select * from interactions where username ilike 'DAVID%';
explain (analyze, buffers) select * from interactions where username ilike '%JONES%';
explain (analyze, buffers) select * from interactions where device ilike 'android';
explain (analyze, buffers) select * from interactions where device ilike '%WEB%';

-- ============================================
-- AND COMBINATIONS (2 conditions)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Jap%' and username like '%kelly%';
explain (analyze, buffers) select * from interactions where country like '%Africa%' and device like 'And%';
explain (analyze, buffers) select * from interactions where username like 'david%' and device like '%oid';
explain (analyze, buffers) select * from interactions where country like '%ia' and device like 'iOS';
explain (analyze, buffers) select * from interactions where username like '%son' and country like 'United%';
explain (analyze, buffers) select * from interactions where device like 'Web' and username like 's%';
explain (analyze, buffers) select * from interactions where country like '%land' and username like '%er';
explain (analyze, buffers) select * from interactions where username like '%ran%' and device like '%dr%';

-- ============================================
-- AND COMBINATIONS (3 conditions)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'S%' and username like '%a%' and device like 'iOS';
explain (analyze, buffers) select * from interactions where country like '%stan' and username like '%ali%' and device like 'Android';
explain (analyze, buffers) select * from interactions where username like 'j%' and country like '%ia' and device like '%eb';
explain (analyze, buffers) select * from interactions where country like 'Uni%' and username like '%smith' and device like 'Web';
explain (analyze, buffers) select * from interactions where username like '%ell%' and country like '%land%' and device like 'i%';
explain (analyze, buffers) select * from interactions where country like '%Republic%' and username like 'd%' and device like '%oid';

-- ============================================
-- OR COMBINATIONS (2 conditions)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Japan' or username like 'kelly%';
explain (analyze, buffers) select * from interactions where country like '%ia' or device like 'Web';
explain (analyze, buffers) select * from interactions where username like '%david%' or country like 'Kenya';
explain (analyze, buffers) select * from interactions where device like 'iOS' or username like '%miller%';
explain (analyze, buffers) select * from interactions where country like 'So%' or username like '%son';
explain (analyze, buffers) select * from interactions where username like '%ran%' or device like '%droid';
explain (analyze, buffers) select * from interactions where country like '%land' or device like 'And%';

-- ============================================
-- OR COMBINATIONS (3 conditions)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'India' or username like '%jones%' or device like 'Web';
explain (analyze, buffers) select * from interactions where country like '%ia' or username like 's%' or device like 'iOS';
explain (analyze, buffers) select * from interactions where username like '%smith' or country like 'Uni%' or device like '%oid';
explain (analyze, buffers) select * from interactions where device like 'Web' or country like '%Africa%' or username like '%ell%';

-- ============================================
-- MIXED AND/OR COMBINATIONS
-- ============================================
explain (analyze, buffers) select * from interactions where (country like 'Jap%' or country like 'Ken%') and username like '%a%';
explain (analyze, buffers) select * from interactions where country like '%ia' and (username like 'david%' or username like 'kelly%');
explain (analyze, buffers) select * from interactions where (country like 'S%' and device like 'iOS') or username like '%jones%';
explain (analyze, buffers) select * from interactions where username like '%ran%' and (country like '%land' or device like 'Android');
explain (analyze, buffers) select * from interactions where (country like '%ia' or country like '%land') and (username like '%son' or username like '%er');
explain (analyze, buffers) select * from interactions where device like 'Web' or (country like 'Uni%' and username like 's%');
explain (analyze, buffers) select * from interactions where (username like '%smith%' and device like 'iOS') or (country like '%Africa%' and device like 'Android');

-- ============================================
-- NON-EXISTENT PATTERN QUERIES
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Atlantis';
explain (analyze, buffers) select * from interactions where country like 'Xyz%';
explain (analyze, buffers) select * from interactions where username like 'nonexistent%';
explain (analyze, buffers) select * from interactions where username like '%zzzzz%';
explain (analyze, buffers) select * from interactions where device like 'Blackberry';
explain (analyze, buffers) select * from interactions where country like '%Wonderland%';
explain (analyze, buffers) select * from interactions where username like '%qwerty123%';

-- ============================================
-- COMPLEX UNDERSCORE PATTERNS
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'S___h%';
explain (analyze, buffers) select * from interactions where username like '%d_v_d%';
explain (analyze, buffers) select * from interactions where username like 'k____76';
explain (analyze, buffers) select * from interactions where country like '_a__';
explain (analyze, buffers) select * from interactions where username like '%j_n_s';
explain (analyze, buffers) select * from interactions where country like 'B_l_v_a';

-- ============================================
-- VERY SHORT PATTERNS
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'M%';
explain (analyze, buffers) select * from interactions where country like '%i';
explain (analyze, buffers) select * from interactions where username like 'a%';
explain (analyze, buffers) select * from interactions where device like '%S';
explain (analyze, buffers) select * from interactions where username like '%e';

-- ============================================
-- EXACT MATCH (no wildcards for comparison)
-- ============================================
explain (analyze, buffers) select * from interactions where country like 'Japan';
explain (analyze, buffers) select * from interactions where country like 'Kenya';
explain (analyze, buffers) select * from interactions where device like 'Web';
explain (analyze, buffers) select * from interactions where device like 'Android';

-- ============================================
-- COMBINED WILDCARD TYPES
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%ia' and username like 'd_vid%';
explain (analyze, buffers) select * from interactions where username like '%j_nes%' and device like 'i%';
explain (analyze, buffers) select * from interactions where country like 'S_uth%' and username like '%son';
explain (analyze, buffers) select * from interactions where username like '%ran%' and country like '_ndia';

-- ============================================
-- MULTIPLE AND CONDITIONS (4+)
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%ia' and username like '%jones%' and device like '%oid' and interaction_type like 'com%';
explain (analyze, buffers) select * from interactions where country like 'S%' and username like 's%' and device like 'iOS' and interaction_type like '%st';
explain (analyze, buffers) select * from interactions where username like '%ell%' and country like '%land' and device like 'Web' and interaction_type like '%ke';

-- ============================================
-- PERFORMANCE EDGE CASES
-- ============================================
explain (analyze, buffers) select * from interactions where country like '%%';
explain (analyze, buffers) select * from interactions where username like '%';
explain (analyze, buffers) select * from interactions where country like '%a%' and username like '%a%';
explain (analyze, buffers) select * from interactions where country like '%e%' or username like '%e%' or device like '%e%';
explain (analyze, buffers) select * from interactions where username like '%o%' and country like '%o%' and device like '%o%';

drop index int_tree;