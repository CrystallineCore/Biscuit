-- ===========================================================================
--  01_like.sql  --  LIKE and NOT LIKE
--
--  The baseline operator pair. Everything else in Biscuit is defined in terms
--  of these: ILIKE is the case-folded structure set, and the regex operators
--  are rewritten onto LIKE globs before they reach the query engine. If this
--  file is wrong, nothing downstream of it can be trusted.
--
--  Dimensions covered: anchoring (prefix / suffix / infix / exact), '_' at
--  every position, multiple and adjacent wildcards, backslash-escaped
--  wildcards, wildcards appearing as DATA rather than syntax, the ESCAPE
--  clause, NULLs, empty strings, case sensitivity, patterns longer than any
--  value, and regex metacharacters that LIKE must treat as literals.
-- ===========================================================================

DELETE FROM bt_result WHERE category = 'like';

DROP TABLE IF EXISTS lk CASCADE;

CREATE TABLE lk (id int PRIMARY KEY, v text);

-- 50,000 rows over 40 hand-chosen stems. One row in twenty is the bare stem
-- so that suffix and exact-match patterns have targets; the rest carry a
-- per-row hex suffix so the column is high-cardinality.
--
-- (g % 40) picks the stem, so the bare-row test uses the INDEPENDENT axis
-- (g / 40). Keying both off the same expression would let only a handful of
-- stems ever appear unsuffixed and would silently starve every suffix case.
INSERT INTO lk (id, v)
SELECT g,
       CASE WHEN (g / 40) % 20 = 0 THEN s.stem
            ELSE s.stem || '-' || substr(md5(g::text), 1, 8) END
FROM generate_series(1, 50000) AS g
JOIN (VALUES
    ( 0,'alpha'),      ( 1,'alphabet'),   ( 2,'beta'),       ( 3,'gamma'),
    ( 4,'delta'),      ( 5,'ALPHA'),      ( 6,'Alpha'),      ( 7,'aLpHa'),
    ( 8,'a_b'),        ( 9,'a%b'),        (10,'100%'),       (11,'%lead'),
    (12,'trail%'),     (13,'under_score'),(14,'two__scores'),(15,'back\slash'),
    (16,'a.c'),        (17,'a*c'),        (18,'[abc]'),      (19,'(a|b)'),
    (20,'x'),          (21,'xy'),         (22,'xyz'),        (23,'xyzw'),
    (24,'node-01'),    (25,'node-42'),    (26,'svc-core'),   (27,'svc-edge'),
    (28,'user_id'),    (29,'order_id'),   (30,'GET /v1'),    (31,'POST /v2'),
    (32,'200 OK'),     (33,'404 MISSING'),(34,'q'),          (35,'qq'),
    (36,'seg-mid-end'),(37,'tailXY'),     (38,'0123456789'), (39,'')
) AS s(k, stem) ON s.k = g % 40;

-- NULLs and an explicit empty string: both are boundary cases the index must
-- agree with a sequential scan about. A NULL never satisfies LIKE or NOT
-- LIKE, so a scan that quietly includes NULL rows in the NOT LIKE result is
-- a bug the row-count check alone would catch, and the fingerprint would
-- catch even if the counts happened to align.
INSERT INTO lk (id, v) VALUES
    (90001, NULL), (90002, NULL), (90003, ''), (90004, ' '),
    (90005, '%'), (90006, '_'), (90007, '\'), (90008, '%_\');

CREATE INDEX lk_ix ON lk USING biscuit (v);
ANALYZE lk;


-- ---- LIKE: anchoring -------------------------------------------------------
DO $BT$ BEGIN
    PERFORM bt_run('like','prefix',            'lk', $$v LIKE 'alpha%'$$,      'must','lk_ix');
    PERFORM bt_run('like','suffix',            'lk', $$v LIKE '%beta'$$,       'must','lk_ix');
    PERFORM bt_run('like','infix',             'lk', $$v LIKE '%gamma%'$$,     'must','lk_ix');
    PERFORM bt_run('like','exact',             'lk', $$v LIKE 'delta'$$,       'must','lk_ix');
    PERFORM bt_run('like','prefix long',       'lk', $$v LIKE '0123456789%'$$, 'must','lk_ix');
    PERFORM bt_run('like','suffix hex',        'lk', $$v LIKE '%abcdef'$$,     'must','lk_ix');
END $BT$;

-- ---- LIKE: underscore at every position -----------------------------------
DO $BT$ BEGIN
    PERFORM bt_run('like','underscore lead',   'lk', $$v LIKE '_lpha'$$,       'must','lk_ix');
    PERFORM bt_run('like','underscore mid',    'lk', $$v LIKE 'a_pha'$$,       'must','lk_ix');
    PERFORM bt_run('like','underscore trail',  'lk', $$v LIKE 'alph_'$$,       'must','lk_ix');
    PERFORM bt_run('like','underscore run',    'lk', $$v LIKE '____'$$,        'must','lk_ix');
    PERFORM bt_run('like','underscore + pct',  'lk', $$v LIKE 'a_%'$$,         'must','lk_ix');
    PERFORM bt_run('like','pct + underscore',  'lk', $$v LIKE '%_d'$$,         'must','lk_ix');
    PERFORM bt_run('like','underscore only',   'lk', $$v LIKE '_'$$,           'must','lk_ix');
END $BT$;

-- ---- LIKE: multiple and adjacent wildcards --------------------------------
DO $BT$ BEGIN
    PERFORM bt_run('like','two segments',      'lk', $$v LIKE 'seg%mid%'$$,    'must','lk_ix');
    PERFORM bt_run('like','three segments',    'lk', $$v LIKE 'seg%mid%end'$$, 'must','lk_ix');
    PERFORM bt_run('like','adjacent percents', 'lk', $$v LIKE 'alpha%%'$$,     'must','lk_ix');
    PERFORM bt_run('like','percent sandwich',  'lk', $$v LIKE '%-%-%'$$,       'must','lk_ix');
END $BT$;
-- A bare '%' matches every non-NULL row. Biscuit declines match-everything
-- globs as a general optimisation -- it does the same for LIKE '%' as for a
-- regex that decomposes to one -- so either outcome is correct here and only
-- the answer is under test.
DO $BT$ BEGIN
    PERFORM bt_run('like','match everything',  'lk', $$v LIKE '%'$$,           'either','lk_ix');
    PERFORM bt_run('like','empty pattern',     'lk', $$v LIKE ''$$,            'either','lk_ix');
END $BT$;

-- ---- LIKE: wildcards escaped in the PATTERN -------------------------------
DO $BT$ BEGIN
    PERFORM bt_run('like','escaped percent',   'lk', $$v LIKE '100\%'$$,       'must','lk_ix');
    PERFORM bt_run('like','escaped underscore','lk', $$v LIKE 'a\_b'$$,        'must','lk_ix');
    PERFORM bt_run('like','escaped backslash', 'lk', $$v LIKE '%\\%'$$,        'must','lk_ix');
    PERFORM bt_run('like','all three escaped', 'lk', $$v LIKE '\%\_\\'$$,      'must','lk_ix');
    PERFORM bt_run('like','esc pct + wildcard','lk', $$v LIKE '%\%%'$$,        'must','lk_ix');
END $BT$;

-- ---- LIKE: wildcards as DATA, not syntax ----------------------------------
-- The stems include literal '%' and '_' characters. These cases check that a
-- row whose VALUE contains a metacharacter is indexed as the character, not
-- as a wildcard -- the mirror image of the escaping cases above.
DO $BT$ BEGIN
    PERFORM bt_run('like','data pct prefix',   'lk', $$v LIKE '\%lead%'$$,     'must','lk_ix');
    PERFORM bt_run('like','data pct suffix',   'lk', $$v LIKE 'trail\%'$$,     'must','lk_ix');
    PERFORM bt_run('like','data underscore',   'lk', $$v LIKE 'under\_score'$$,'must','lk_ix');
    PERFORM bt_run('like','data double under', 'lk', $$v LIKE 'two\_\_scores'$$,'must','lk_ix');
END $BT$;

-- ---- LIKE: regex metacharacters are literals to LIKE ----------------------
DO $BT$ BEGIN
    PERFORM bt_run('like','dot is literal',    'lk', $$v LIKE 'a.c'$$,         'must','lk_ix');
    PERFORM bt_run('like','star is literal',   'lk', $$v LIKE 'a*c'$$,         'must','lk_ix');
    PERFORM bt_run('like','bracket literal',   'lk', $$v LIKE '[abc]'$$,       'must','lk_ix');
    PERFORM bt_run('like','pipe literal',      'lk', $$v LIKE '(a|b)'$$,       'must','lk_ix');
    PERFORM bt_run('like','caret literal',     'lk', $$v LIKE '%^%'$$,         'must','lk_ix');
END $BT$;

-- ---- LIKE: case sensitivity ------------------------------------------------
-- LIKE is case-sensitive. These three patterns differ only in case and must
-- return three different, disjoint-ish answers; if the index were quietly
-- folding case, all three would agree.
DO $BT$ BEGIN
    PERFORM bt_run('like','case lower',        'lk', $$v LIKE 'alpha%'$$,      'must','lk_ix');
    PERFORM bt_run('like','case upper',        'lk', $$v LIKE 'ALPHA%'$$,      'must','lk_ix');
    PERFORM bt_run('like','case mixed',        'lk', $$v LIKE 'aLpHa%'$$,      'must','lk_ix');
END $BT$;

-- ---- LIKE: no-match and degenerate ----------------------------------------
DO $BT$ BEGIN
    PERFORM bt_run('like','no such prefix',    'lk', $$v LIKE 'zzzzzz%'$$,     'must','lk_ix');
    PERFORM bt_run('like','longer than data',  'lk', $$v LIKE repeat('z',200)$$,'must','lk_ix');
    PERFORM bt_run('like','space only',        'lk', $$v LIKE ' '$$,           'must','lk_ix');
END $BT$;

-- ---- NOT LIKE: the same matrix, negated -----------------------------------
-- NOT LIKE is not simply "everything else": NULL rows satisfy neither, so the
-- two answers must not partition the table. That is precisely what makes
-- these worth running separately rather than inferring.
DO $BT$ BEGIN
    PERFORM bt_run('like','not prefix',        'lk', $$v NOT LIKE 'alpha%'$$,  'must','lk_ix');
    PERFORM bt_run('like','not suffix',        'lk', $$v NOT LIKE '%beta'$$,   'must','lk_ix');
    PERFORM bt_run('like','not infix',         'lk', $$v NOT LIKE '%gamma%'$$, 'must','lk_ix');
    PERFORM bt_run('like','not exact',         'lk', $$v NOT LIKE 'delta'$$,   'must','lk_ix');
    PERFORM bt_run('like','not underscore',    'lk', $$v NOT LIKE 'a_pha'$$,   'must','lk_ix');
    PERFORM bt_run('like','not escaped pct',   'lk', $$v NOT LIKE '100\%'$$,   'must','lk_ix');
    PERFORM bt_run('like','not segments',      'lk', $$v NOT LIKE 'seg%mid%end'$$,'must','lk_ix');
    PERFORM bt_run('like','not match-all',     'lk', $$v NOT LIKE '%'$$,       'either','lk_ix');
    PERFORM bt_run('like','not no-match',      'lk', $$v NOT LIKE 'zzzzzz%'$$, 'must','lk_ix');
END $BT$;

-- ---- ESCAPE clause ---------------------------------------------------------
-- LIKE ... ESCAPE parses as like_escape(pattern, escape) fed to ~~, which
-- looks unindexable -- but with both arguments constant the planner folds it
-- back to a plain ~~ against a rewritten literal before the index is ever
-- considered, so these ARE indexable. Asserting 'must' here pins that
-- behaviour: if a future release stopped folding, these would start failing
-- rather than silently losing the index.
DO $BT$ BEGIN
    PERFORM bt_run('like','escape clause pct', 'lk', $$v LIKE '100#%' ESCAPE '#'$$,      'must','lk_ix');
    PERFORM bt_run('like','escape clause und', 'lk', $$v LIKE 'a#_b'  ESCAPE '#'$$,      'must','lk_ix');
    PERFORM bt_run('like','escape clause mix', 'lk', $$v LIKE '%#%%'  ESCAPE '#'$$,      'must','lk_ix');
END $BT$;
-- The same shape with a non-constant escape cannot be folded, so it stays a
-- like_escape() call and no operator class can claim it.
DO $BT$ BEGIN
    PERFORM bt_run('like','escape non-const', 'lk',
        $$v LIKE '100#%' ESCAPE substr(md5('x'), 1, 0) || '#'$$, 'either', 'lk_ix');
END $BT$;

-- ---- NULL semantics --------------------------------------------------------
-- Checked directly rather than differentially: an index that returned NULL
-- rows for either operator would be wrong even if both arms agreed.
DO $$
DECLARE n_null int; n_like int; n_notlike int; n_total int;
BEGIN
    SELECT count(*) INTO n_null    FROM lk WHERE v IS NULL;
    SELECT count(*) INTO n_total   FROM lk;
    SET LOCAL enable_seqscan = off;
    SELECT count(*) INTO n_like    FROM lk WHERE v LIKE '%a%';
    SELECT count(*) INTO n_notlike FROM lk WHERE v NOT LIKE '%a%';

    PERFORM bt_assert(n_null = 2, 'fixture should contain exactly 2 NULL rows');
    PERFORM bt_assert(n_like + n_notlike = n_total - n_null,
        format('LIKE (%s) + NOT LIKE (%s) should equal non-NULL rows (%s), '
               'so NULLs are excluded from both',
               n_like, n_notlike, n_total - n_null));
END;
$$;

DO $BT$ BEGIN PERFORM bt_check('like'); END $BT$;
