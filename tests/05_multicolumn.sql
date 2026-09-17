-- ===========================================================================
--  05_multicolumn.sql  --  multi-column indexes, and how they differ from
--                          several single-column ones
--
--  A multi-column Biscuit index is not a composite key in the B-tree sense.
--  There is no sort order over the tuple and no notion of a "leading column"
--  prefix that must be constrained before the rest become usable: each column
--  has its own bitmaps, and a scan intersects whichever ones the query
--  constrains. That predicts something specific and testable -- a predicate
--  on the THIRD column alone should be just as indexable as one on the first,
--  which is exactly where a B-tree would fall back to a scan.
--
--  This file tests that prediction, and then cross-checks the whole thing
--  against an independent implementation of the same query: an identical
--  table carrying three separate single-column indexes. Two structurally
--  different index layouts computing the same answer is a much stronger
--  statement than either one agreeing with a sequential scan.
-- ===========================================================================

DELETE FROM bt_result WHERE category = 'multicolumn';

DROP TABLE IF EXISTS mc CASCADE;
DROP TABLE IF EXISTS sc CASCADE;

CREATE TABLE mc (id int PRIMARY KEY, a text, b text, c text);

-- The three columns are deliberately given different shapes and different
-- selectivities, so a scan that silently applied a predicate to the wrong
-- column would produce a visibly wrong count rather than a coincidentally
-- similar one.
INSERT INTO mc (id, a, b, c)
SELECT g,
       CASE WHEN (g / 16) % 20 = 0 THEN t.av ELSE t.av || '-' || substr(md5(g::text),1,6) END,
       t.bv,
       CASE WHEN (g / 16) % 7 = 0 THEN t.cv ELSE t.cv || substr(md5((g*7)::text),1,4) END
FROM generate_series(1, 30000) AS g
JOIN (VALUES
    ( 0,'alpha','region-east','svc001'), ( 1,'alpha','region-west','svc002'),
    ( 2,'beta', 'region-east','svc003'), ( 3,'beta', 'region-north','svc004'),
    ( 4,'gamma','region-west','svc005'), ( 5,'gamma','region-south','svc006'),
    ( 6,'delta','region-east','svc007'), ( 7,'delta','region-north','svc008'),
    ( 8,'ALPHA','REGION-EAST','SVC009'), ( 9,'Beta', 'Region-West','Svc010'),
    (10,'omega','region-south','svc011'),(11,'omega','region-east','svc012'),
    (12,'zeta', 'region-west','svc013'), (13,'zeta', 'region-north','svc014'),
    (14,'a_b',  'region%east','svc_015'),(15,'100%', 'region.west','svc.016')
) AS t(k, av, bv, cv) ON t.k = g % 16;

INSERT INTO mc (id, a, b, c) VALUES
    (90001, NULL, 'region-east', 'svc001'),
    (90002, 'alpha', NULL, 'svc001'),
    (90003, 'alpha', 'region-east', NULL),
    (90004, '', '', '');

-- The comparison table: same rows, three independent indexes.
CREATE TABLE sc AS SELECT * FROM mc;
ALTER TABLE sc ADD PRIMARY KEY (id);

CREATE INDEX mc_ix   ON mc USING biscuit (a, b, c);
CREATE INDEX sc_ix_a ON sc USING biscuit (a);
CREATE INDEX sc_ix_b ON sc USING biscuit (b);
CREATE INDEX sc_ix_c ON sc USING biscuit (c);

ANALYZE mc;
ANALYZE sc;


-- ---- multi-column index, one column constrained at a time ------------------
-- The middle and trailing columns are the interesting ones. If Biscuit
-- behaved like a B-tree these would not be indexable without also
-- constraining 'a'.
DO $BT$ BEGIN
    PERFORM bt_run('multicolumn','mc: leading col only',  'mc', $$a LIKE 'alpha%'$$,        'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: middle col only',   'mc', $$b LIKE 'region-east'$$,   'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: trailing col only', 'mc', $$c LIKE 'svc001%'$$,       'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: middle suffix',     'mc', $$b LIKE '%west'$$,         'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: trailing infix',    'mc', $$c LIKE '%01%'$$,          'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: leading regex',     'mc', $$a ~ '^alpha'$$,           'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: trailing regex',    'mc', $$c ~ '^svc0.{2}$'$$,       'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: middle ilike',      'mc', $$b ILIKE 'REGION-EAST%'$$, 'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: leading not like',  'mc', $$a NOT LIKE 'alpha%'$$,    'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: trailing not like', 'mc', $$c NOT LIKE 'svc001%'$$,   'must','mc_ix');
END $BT$;

-- ---- multi-column index, several columns constrained together --------------
DO $BT$ BEGIN
    PERFORM bt_run('multicolumn','mc: a AND b',      'mc',
        $$a LIKE 'alpha%' AND b LIKE 'region-east'$$,                  'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: b AND c',      'mc',
        $$b LIKE 'region-east' AND c LIKE 'svc%'$$,                    'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: a AND c',      'mc',
        $$a LIKE 'beta%' AND c LIKE 'svc00%'$$,                        'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: a AND b AND c','mc',
        $$a LIKE 'alpha%' AND b LIKE 'region%' AND c LIKE 'svc0%'$$,   'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: mixed families','mc',
        $$a ~ '^alpha' AND b ILIKE 'REGION-%' AND c LIKE '%1%'$$,      'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: a OR b',       'mc',
        $$a LIKE 'alpha%' OR b LIKE 'region-south'$$,                  'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: a OR c',       'mc',
        $$a LIKE 'omega%' OR c LIKE 'svc013%'$$,                       'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: (a OR b) AND c','mc',
        $$(a LIKE 'alpha%' OR b LIKE 'region-west') AND c LIKE 'svc%'$$,'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: contradiction','mc',
        $$a LIKE 'alpha%' AND a LIKE 'beta%'$$,                        'must','mc_ix');
    PERFORM bt_run('multicolumn','mc: neg + pos',    'mc',
        $$a NOT LIKE 'alpha%' AND b LIKE 'region-east'$$,              'must','mc_ix');
END $BT$;

-- ---- the same queries against three single-column indexes ------------------
-- Same predicates, structurally different index layout. Each is checked
-- against its own sequential scan here; the cross-layout comparison follows.
DO $BT$ BEGIN
    PERFORM bt_run('multicolumn','sc: col a only',   'sc', $$a LIKE 'alpha%'$$,      'must','sc_ix_a');
    PERFORM bt_run('multicolumn','sc: col b only',   'sc', $$b LIKE 'region-east'$$, 'must','sc_ix_b');
    PERFORM bt_run('multicolumn','sc: col c only',   'sc', $$c LIKE 'svc001%'$$,     'must','sc_ix_c');
    PERFORM bt_run('multicolumn','sc: a AND b',      'sc',
        $$a LIKE 'alpha%' AND b LIKE 'region-east'$$,                  'either','sc_ix_a');
    PERFORM bt_run('multicolumn','sc: a AND b AND c','sc',
        $$a LIKE 'alpha%' AND b LIKE 'region%' AND c LIKE 'svc0%'$$,   'either','sc_ix_a');
    PERFORM bt_run('multicolumn','sc: a OR b',       'sc',
        $$a LIKE 'alpha%' OR b LIKE 'region-south'$$,                  'either',NULL);
    PERFORM bt_run('multicolumn','sc: trailing regex','sc',$$c ~ '^svc0.{2}$'$$,     'must','sc_ix_c');
    PERFORM bt_run('multicolumn','sc: middle ilike', 'sc', $$b ILIKE 'REGION-EAST%'$$,'must','sc_ix_b');
END $BT$;

-- ---- NULL handling per column ----------------------------------------------
-- Each column has exactly one NULL row, and they are on different rows, so a
-- scan that mishandled NULL in a non-leading column would show up as a
-- one-row discrepancy that the fingerprint catches even when counts align.
DO $BT$ BEGIN
    PERFORM bt_run('multicolumn','mc: null in a',    'mc', $$a IS NULL$$,            'either','mc_ix');
    PERFORM bt_run('multicolumn','mc: null in b',    'mc', $$b IS NULL$$,            'either','mc_ix');
    PERFORM bt_run('multicolumn','mc: not like w/ null','mc',$$c NOT LIKE '%'$$,     'either','mc_ix');
    PERFORM bt_run('multicolumn','mc: empty string a','mc', $$a LIKE ''$$,           'either','mc_ix');
END $BT$;


-- ---- Cross-layout equivalence ----------------------------------------------
-- The strongest check in this file. Two different index structures over
-- identical data must produce identical answers for the same predicate --
-- not merely identical counts, but the same rows.
DO $BT$
DECLARE
    preds text[] := ARRAY[
        $q$a LIKE 'alpha%'$q$,
        $q$b LIKE 'region-east'$q$,
        $q$c LIKE 'svc001%'$q$,
        $q$a LIKE 'alpha%' AND b LIKE 'region-east'$q$,
        $q$b LIKE 'region%' AND c LIKE 'svc0%'$q$,
        $q$a LIKE 'alpha%' AND b LIKE 'region%' AND c LIKE 'svc0%'$q$,
        $q$a LIKE 'alpha%' OR b LIKE 'region-south'$q$,
        $q$a ~ '^alpha' AND c ~ 'svc'$q$,
        $q$b ILIKE 'REGION-%'$q$,
        $q$a NOT LIKE 'alpha%' AND b LIKE 'region-east'$q$,
        $q$c ~ '^svc0.{2}$'$q$,
        $q$(a LIKE 'alpha%' OR b LIKE 'region-west') AND c LIKE 'svc%'$q$
    ];
    p text; n_mc bigint; h_mc bigint; n_sc bigint; h_sc bigint;
BEGIN
    SET LOCAL jit = off;
    SET LOCAL max_parallel_workers_per_gather = 0;
    SET LOCAL enable_seqscan = off;

    FOREACH p IN ARRAY preds LOOP
        EXECUTE format('SELECT count(*), coalesce(sum(hashtext(id::text)::bigint),0) '
                       'FROM mc WHERE %s', p) INTO n_mc, h_mc;
        EXECUTE format('SELECT count(*), coalesce(sum(hashtext(id::text)::bigint),0) '
                       'FROM sc WHERE %s', p) INTO n_sc, h_sc;

        PERFORM bt_assert(n_mc = n_sc AND h_mc = h_sc,
            format('multi-column and single-column layouts disagree on [%s]: '
                   'mc=%s rows/%s hash, sc=%s rows/%s hash',
                   p, n_mc, h_mc, n_sc, h_sc));
    END LOOP;

    RAISE NOTICE 'multicolumn : % predicates agree across both index layouts',
                 array_length(preds, 1);
END $BT$;

-- ---- Column independence ---------------------------------------------------
-- A predicate on one column must not be influenced by the others. Checked by
-- constructing an answer two ways: directly, and as the intersection of two
-- single-column results computed separately.
DO $BT$
DECLARE direct bigint; composed bigint;
BEGIN
    SET LOCAL enable_seqscan = off;

    SELECT count(*) INTO direct FROM mc
     WHERE a LIKE 'alpha%' AND b LIKE 'region-east';

    SELECT count(*) INTO composed FROM (
        SELECT id FROM mc WHERE a LIKE 'alpha%'
        INTERSECT
        SELECT id FROM mc WHERE b LIKE 'region-east'
    ) AS x;

    PERFORM bt_assert(direct = composed,
        format('a AND b as one scan (%s) differs from the intersection of two '
               'separate scans (%s)', direct, composed));
END $BT$;

DO $BT$ BEGIN PERFORM bt_check('multicolumn'); END $BT$;
