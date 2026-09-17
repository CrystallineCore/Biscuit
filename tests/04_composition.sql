-- ===========================================================================
--  04_composition.sql  --  AND / OR / NOT over globs and regexes
--
--  Single-predicate tests exercise one ScanKey. This file exercises the paths
--  that only exist when there is more than one, and they are different paths:
--
--    AND  arrives as multiple ScanKeys on ONE index scan. Biscuit intersects
--         the bitmaps itself, and it also has to decide what to do when one
--         conjunct is outside its subset -- the honest answer is to serve the
--         part it understands and let the recheck filter the rest, NOT to
--         give up and NOT to pretend it matched everything.
--    OR   arrives as separate index scans combined by a BitmapOr node. Here
--         the AM has no say in the union at all, but every branch must be
--         independently indexable or the whole disjunction collapses to a
--         sequential scan -- an OR is only as indexable as its worst arm.
--
--  Mixing operator families in one clause matters too: a regex conjunct is
--  rewritten to a glob before the qsort that orders predicates by cost, so a
--  clause containing both is the case where the rewrite and the ordering
--  interact.
-- ===========================================================================

DELETE FROM bt_result WHERE category = 'composition';

DROP TABLE IF EXISTS cp CASCADE;

CREATE TABLE cp (id int PRIMARY KEY, v text, n int);

INSERT INTO cp (id, v, n)
SELECT g,
       CASE WHEN (g / 24) % 20 = 0 THEN s.stem
            ELSE s.stem || '-' || substr(md5(g::text), 1, 8) END,
       g % 1000
FROM generate_series(1, 40000) AS g
JOIN (VALUES
    ( 0,'alpha-one'),  ( 1,'alpha-two'),  ( 2,'alpha-END'), ( 3,'beta-one'),
    ( 4,'beta-END'),   ( 5,'gamma-one'),  ( 6,'gamma-END'), ( 7,'delta-one'),
    ( 8,'ALPHA-ONE'),  ( 9,'ALPHA-END'),  (10,'Beta-END'),  (11,'aXc-END'),
    (12,'aYc-one'),    (13,'zeta-one'),   (14,'zeta-END'),  (15,'omega-one'),
    (16,'seg-mid-end'),(17,'seg-mid-xxx'),(18,'100%-END'),  (19,'a_b-one'),
    (20,'q'),          (21,'qq'),         (22,'qqq'),       (23,'')
) AS s(k, stem) ON s.k = g % 24;

INSERT INTO cp (id, v, n) VALUES (90001, NULL, 1), (90002, '', 2);

CREATE INDEX cp_ix ON cp USING biscuit (v);
ANALYZE cp;


-- ---- AND: both conjuncts indexable, same operator family -------------------
DO $BT$ BEGIN
    PERFORM bt_run('composition','AND glob+glob',    'cp',
        $$v LIKE 'alpha%' AND v LIKE '%END'$$,                 'must','cp_ix');
    PERFORM bt_run('composition','AND glob+glob wide','cp',
        $$v LIKE '%a%' AND v LIKE '%e%'$$,                     'must','cp_ix');
    PERFORM bt_run('composition','AND three globs',  'cp',
        $$v LIKE 's%' AND v LIKE '%mid%' AND v LIKE '%end'$$,  'must','cp_ix');
    PERFORM bt_run('composition','AND glob+notglob', 'cp',
        $$v LIKE 'alpha%' AND v NOT LIKE '%END'$$,             'must','cp_ix');
    PERFORM bt_run('composition','AND notglob+notglob','cp',
        $$v NOT LIKE 'alpha%' AND v NOT LIKE 'beta%'$$,        'must','cp_ix');
    PERFORM bt_run('composition','AND contradiction','cp',
        $$v LIKE 'alpha%' AND v LIKE 'beta%'$$,                'must','cp_ix');
END $BT$;

-- ---- AND: both conjuncts indexable, MIXED operator families ----------------
-- The regex arm is rewritten to a glob before the predicates are cost-ordered,
-- so these are the clauses where the rewrite and the ordering interact.
DO $BT$ BEGIN
    PERFORM bt_run('composition','AND regex+glob',   'cp',
        $$v ~ '^alpha' AND v LIKE '%END'$$,                    'must','cp_ix');
    PERFORM bt_run('composition','AND glob+regex',   'cp',
        $$v LIKE 'alpha%' AND v ~ 'END$'$$,                    'must','cp_ix');
    PERFORM bt_run('composition','AND regex+regex',  'cp',
        $$v ~ '^alpha' AND v ~ 'END$'$$,                       'must','cp_ix');
    PERFORM bt_run('composition','AND regex+notregex','cp',
        $$v ~ '^alpha' AND v !~ 'END$'$$,                      'must','cp_ix');
    PERFORM bt_run('composition','AND ilike+regex',  'cp',
        $$v ILIKE 'ALPHA%' AND v ~* 'end$'$$,                  'must','cp_ix');
    PERFORM bt_run('composition','AND ilike+glob',   'cp',
        $$v ILIKE 'ALPHA%' AND v LIKE '%END'$$,                'must','cp_ix');
    PERFORM bt_run('composition','AND glob+ilike',   'cp',
        $$v LIKE 'alpha%' AND v ILIKE '%end'$$,                'must','cp_ix');
    PERFORM bt_run('composition','AND dotted regex', 'cp',
        $$v ~ '^a.c' AND v LIKE '%END'$$,                      'must','cp_ix');
END $BT$;

-- ---- AND: one conjunct outside the subset ----------------------------------
-- FINDING, pinned here as current behaviour rather than as an aspiration.
--
-- When one conjunct on the INDEXED COLUMN is outside the decomposable subset,
-- the whole clause loses the index -- including the conjunct that was
-- perfectly indexable on its own. The mechanism is visible in the plans:
--
--   v LIKE 'alpha%'                    -> Bitmap Index Scan on cp_ix
--   v LIKE 'alpha%' AND n < 100        -> Bitmap Index Scan, n < 100 as Filter
--   v LIKE 'alpha%' AND v || '' ~ '[0-9]'
--                                      -> Bitmap Index Scan, regex as Filter
--   v LIKE 'alpha%' AND v ~ '[0-9]'    -> Seq Scan
--
-- The difference is not the regex; it is that '~' is a member of the Biscuit
-- opfamily, so the planner hands BOTH clauses to the index as index clauses.
-- Biscuit then marks the undecomposable one lossy and prices the entire path
-- out, discarding the usable conjunct with it. Wrapping the same regex in
-- 'v || ''''' makes it a non-Var expression, the planner stops offering it to
-- the index, and the path comes straight back -- which is the proof that the
-- index path was viable all along and that this is a costing decision rather
-- than a structural limit.
--
-- Nothing here is a correctness bug: every case returns the right rows. It is
-- a missed optimisation, and the natural fix is to cost the path from the
-- non-lossy keys alone and let the lossy ones fall to the recheck, exactly as
-- happens for 'n < 100'.
DO $BT$ BEGIN
    PERFORM bt_run('composition','AND glob+bracket', 'cp',
        $$v LIKE 'alpha%' AND v ~ '[0-9]'$$,                   'must_not','cp_ix');
    PERFORM bt_run('composition','AND glob+alt',     'cp',
        $$v LIKE '%END' AND v ~ '(one|two)'$$,                 'must_not','cp_ix');
    PERFORM bt_run('composition','AND regex+star',   'cp',
        $$v ~ '^alpha' AND v ~ 'a*b'$$,                        'must_not','cp_ix');
    PERFORM bt_run('composition','AND glob+notci',   'cp',
        $$v LIKE 'alpha%' AND v !~* 'END$'$$,                  'must_not','cp_ix');
    PERFORM bt_run('composition','AND unindexable both','cp',
        $$v ~ '[0-9]' AND v ~ '(one|two)'$$,                   'must_not','cp_ix');
    -- The contrast case. Same predicate, same selectivity, but hidden from
    -- the opfamily behind a concatenation -- and the index comes back.
    PERFORM bt_run('composition','AND glob+opaque regex','cp',
        $$v LIKE 'alpha%' AND v || '' ~ '[0-9]'$$,             'must','cp_ix');
END $BT$;

-- ---- AND: mixed with a predicate on another column -------------------------
-- 'n' has no index, so these force the planner to combine a Biscuit bitmap
-- with a heap-level filter. The risk being checked is that the extra
-- restriction is applied at all, not silently dropped.
DO $BT$ BEGIN
    PERFORM bt_run('composition','AND glob+int',     'cp',
        $$v LIKE 'alpha%' AND n < 100$$,                       'must','cp_ix');
    PERFORM bt_run('composition','AND regex+int',    'cp',
        $$v ~ '^alpha' AND n BETWEEN 200 AND 400$$,            'must','cp_ix');
    PERFORM bt_run('composition','AND glob+isnull',  'cp',
        $$v LIKE 'alpha%' AND v IS NOT NULL$$,                 'must','cp_ix');
    PERFORM bt_run('composition','AND int+glob rev', 'cp',
        $$n = 42 AND v LIKE '%e%'$$,                           'either','cp_ix');
END $BT$;

-- ---- OR: every arm indexable -----------------------------------------------
DO $BT$ BEGIN
    PERFORM bt_run('composition','OR glob+glob',     'cp',
        $$v LIKE 'alpha%' OR v LIKE 'beta%'$$,                 'must','cp_ix');
    PERFORM bt_run('composition','OR three globs',   'cp',
        $$v LIKE 'alpha%' OR v LIKE 'beta%' OR v LIKE 'gamma%'$$,'must','cp_ix');
    PERFORM bt_run('composition','OR regex+glob',    'cp',
        $$v ~ '^alpha' OR v LIKE 'beta%'$$,                    'must','cp_ix');
    PERFORM bt_run('composition','OR regex+regex',   'cp',
        $$v ~ '^alpha' OR v ~ '^beta'$$,                       'must','cp_ix');
    PERFORM bt_run('composition','OR ilike+glob',    'cp',
        $$v ILIKE 'ALPHA%' OR v LIKE 'zeta%'$$,                'must','cp_ix');
    PERFORM bt_run('composition','OR overlapping',   'cp',
        $$v LIKE 'alpha%' OR v LIKE '%one'$$,                  'must','cp_ix');
    PERFORM bt_run('composition','OR disjoint',      'cp',
        $$v LIKE 'alpha%' OR v LIKE 'omega%'$$,                'must','cp_ix');
END $BT$;

-- ---- OR: one arm outside the subset ----------------------------------------
-- A disjunction is only as indexable as its worst arm: if one branch cannot
-- produce a bitmap, there is nothing to union and the whole clause must fall
-- back. Serving only the indexable arm here would DROP rows.
DO $BT$ BEGIN
    PERFORM bt_run('composition','OR glob+bracket',  'cp',
        $$v LIKE 'alpha%' OR v ~ '[0-9]'$$,                    'must_not','cp_ix');
    PERFORM bt_run('composition','OR glob+alt',      'cp',
        $$v LIKE 'alpha%' OR v ~ '(one|two)'$$,                'must_not','cp_ix');
    PERFORM bt_run('composition','OR glob+notci',    'cp',
        $$v LIKE 'alpha%' OR v !~* 'END$'$$,                   'must_not','cp_ix');
    PERFORM bt_run('composition','OR glob+int',      'cp',
        $$v LIKE 'alpha%' OR n < 100$$,                        'must_not','cp_ix');
END $BT$;

-- ---- NOT and nesting -------------------------------------------------------
DO $BT$ BEGIN
    PERFORM bt_run('composition','NOT(glob)',        'cp',
        $$NOT (v LIKE 'alpha%')$$,                             'must','cp_ix');
    PERFORM bt_run('composition','NOT(regex)',       'cp',
        $$NOT (v ~ '^alpha')$$,                                'must','cp_ix');
    PERFORM bt_run('composition','NOT(AND)',         'cp',
        $$NOT (v LIKE 'alpha%' AND v LIKE '%END')$$,           'either','cp_ix');
    PERFORM bt_run('composition','NOT(OR)',          'cp',
        $$NOT (v LIKE 'alpha%' OR v LIKE 'beta%')$$,           'either','cp_ix');
    PERFORM bt_run('composition','(OR) AND glob',    'cp',
        $$(v LIKE 'alpha%' OR v LIKE 'beta%') AND v LIKE '%END'$$,'must','cp_ix');
    PERFORM bt_run('composition','(AND) OR glob',    'cp',
        $$(v LIKE 'alpha%' AND v LIKE '%END') OR v LIKE 'omega%'$$,'must','cp_ix');
    PERFORM bt_run('composition','nested 3 deep',    'cp',
        $$(v LIKE 'a%' OR v LIKE 'b%') AND (v ~ 'END$' OR v ~ 'one$')$$,'must','cp_ix');
END $BT$;


-- ---- Set algebra -----------------------------------------------------------
-- Differential testing proves index and sequential scan agree. These prove
-- that what they agree on is actually the boolean algebra the SQL asked for:
-- a shared error in how conjuncts are combined would pass every case above.
DO $BT$
DECLARE
    a bigint; b bigint; a_and_b bigint; a_or_b bigint;
    n_null int; n_total int; lhs bigint; rhs bigint;
BEGIN
    SET LOCAL enable_seqscan = off;

    -- 1. Inclusion-exclusion: |A ∪ B| = |A| + |B| - |A ∩ B|.
    SELECT count(*) INTO a       FROM cp WHERE v LIKE 'alpha%';
    SELECT count(*) INTO b       FROM cp WHERE v LIKE '%END';
    SELECT count(*) INTO a_and_b FROM cp WHERE v LIKE 'alpha%' AND v LIKE '%END';
    SELECT count(*) INTO a_or_b  FROM cp WHERE v LIKE 'alpha%' OR  v LIKE '%END';
    PERFORM bt_assert(a_or_b = a + b - a_and_b,
        format('inclusion-exclusion: |A∪B|=%s but |A|+|B|-|A∩B|=%s',
               a_or_b, a + b - a_and_b));

    -- 2. Same identity with the two arms drawn from DIFFERENT operator
    --    families, which is the combination the rewrite path touches.
    SELECT count(*) INTO a       FROM cp WHERE v ~ '^alpha';
    SELECT count(*) INTO b       FROM cp WHERE v ILIKE '%end';
    SELECT count(*) INTO a_and_b FROM cp WHERE v ~ '^alpha' AND v ILIKE '%end';
    SELECT count(*) INTO a_or_b  FROM cp WHERE v ~ '^alpha' OR  v ILIKE '%end';
    PERFORM bt_assert(a_or_b = a + b - a_and_b,
        format('mixed-family inclusion-exclusion: %s vs %s',
               a_or_b, a + b - a_and_b));

    -- 3. De Morgan, over the non-NULL rows. NULL satisfies neither a
    --    predicate nor its negation, so the law only holds once they are
    --    excluded -- and getting that wrong is exactly the bug this catches.
    SELECT count(*) INTO n_null  FROM cp WHERE v IS NULL;
    SELECT count(*) INTO n_total FROM cp;
    SELECT count(*) INTO lhs FROM cp
        WHERE v IS NOT NULL AND NOT (v LIKE 'alpha%' AND v LIKE '%END');
    SELECT count(*) INTO rhs FROM cp
        WHERE v IS NOT NULL AND (v NOT LIKE 'alpha%' OR v NOT LIKE '%END');
    PERFORM bt_assert(lhs = rhs,
        format('De Morgan: NOT(A AND B)=%s but (NOT A) OR (NOT B)=%s', lhs, rhs));

    -- 4. A predicate and its negation partition the non-NULL rows, checked
    --    for a COMPOUND clause rather than a single one.
    SELECT count(*) INTO a FROM cp WHERE v LIKE 'alpha%' AND v LIKE '%END';
    SELECT count(*) INTO b FROM cp
        WHERE v IS NOT NULL AND NOT (v LIKE 'alpha%' AND v LIKE '%END');
    PERFORM bt_assert(a + b = n_total - n_null,
        format('compound partition: %s + %s <> %s', a, b, n_total - n_null));

    -- 5. An AND whose arms cannot both hold must be empty, not merely small.
    SELECT count(*) INTO a FROM cp WHERE v LIKE 'alpha%' AND v LIKE 'beta%';
    PERFORM bt_assert(a = 0,
        format('contradictory AND returned %s rows', a));

    -- 6. Adding a conjunct can only shrink a result set, never grow it.
    SELECT count(*) INTO a FROM cp WHERE v LIKE 'alpha%';
    SELECT count(*) INTO b FROM cp WHERE v LIKE 'alpha%' AND v ~ '[0-9]';
    PERFORM bt_assert(b <= a,
        format('AND with an unindexable conjunct grew the result: %s > %s', b, a));
END $BT$;

DO $BT$ BEGIN PERFORM bt_check('composition'); END $BT$;
