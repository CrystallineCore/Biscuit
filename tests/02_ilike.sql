-- ===========================================================================
--  02_ilike.sql  --  ILIKE and NOT ILIKE
--
--  ILIKE is served by a second, case-folded structure set rather than by
--  lowering the query and reusing the LIKE bitmaps, so it is a genuinely
--  separate code path and not a thin wrapper. The interesting question is
--  therefore not "does it fold ASCII" but "does its notion of folding agree
--  with PostgreSQL's in the places where folding is not a simple byte map".
--
--  Dimensions: ASCII folding in both directions, mixed-case patterns against
--  mixed-case data, the full LIKE wildcard matrix repeated under ILIKE,
--  non-ASCII folding (Latin-1 accents, German sharp s, Greek final sigma,
--  Turkish dotted/dotless i), and the algebraic identities that must hold
--  between LIKE, ILIKE and lower().
-- ===========================================================================

DELETE FROM bt_result WHERE category = 'ilike';

DROP TABLE IF EXISTS il CASCADE;

CREATE TABLE il (id int PRIMARY KEY, v text);

INSERT INTO il (id, v)
SELECT g,
       CASE WHEN (g / 32) % 20 = 0 THEN s.stem
            ELSE s.stem || '-' || substr(md5(g::text), 1, 8) END
FROM generate_series(1, 40000) AS g
JOIN (VALUES
    ( 0,'alpha'),   ( 1,'ALPHA'),   ( 2,'Alpha'),   ( 3,'aLpHa'),
    ( 4,'beta'),    ( 5,'BETA'),    ( 6,'Beta'),    ( 7,'bEtA'),
    ( 8,'gamma'),   ( 9,'GAMMA'),   (10,'delta'),   (11,'DELTA'),
    (12,'MiXeD'),   (13,'mixed'),   (14,'MIXED'),   (15,'mIxEd'),
    -- Latin-1 accents: folding is a straightforward one-to-one map here, so
    -- these are the cases ILIKE should get right.
    (16,'Ähnlich'), (17,'ähnlich'), (18,'Über'),    (19,'über'),
    (20,'Élan'),    (21,'élan'),    (22,'GRÜN'),    (23,'grün'),
    -- Folding that is NOT one-to-one: 'ß' upcases to two characters, final
    -- sigma folds to the same letter as medial sigma, and Turkish 'I'/'ı'
    -- fold differently than in any other locale. These are where an index
    -- that folds by its own rules and PostgreSQL's ILIKE can disagree.
    (24,'straße'),  (25,'STRASSE'), (26,'ΣΟΦΟΣ'),   (27,'σοφός'),
    (28,'İstanbul'),(29,'istanbul'),(30,'ıd'),      (31,'ID')
) AS s(k, stem) ON s.k = g % 32;

INSERT INTO il (id, v) VALUES
    (90001, NULL), (90002, ''), (90003, 'A%B'), (90004, 'a_b'), (90005, '100%');

CREATE INDEX il_ix ON il USING biscuit (v);
ANALYZE il;


-- ---- ILIKE: folding in both directions ------------------------------------
-- Each of these four patterns differs only in case. Under ILIKE all four must
-- return the SAME answer; under LIKE (tested in 01) all four differ. Running
-- all four rather than one is what turns this into a folding test.
DO $BT$ BEGIN
    PERFORM bt_run('ilike','fold: lower pattern', 'il', $$v ILIKE 'alpha%'$$, 'must','il_ix');
    PERFORM bt_run('ilike','fold: upper pattern', 'il', $$v ILIKE 'ALPHA%'$$, 'must','il_ix');
    PERFORM bt_run('ilike','fold: title pattern', 'il', $$v ILIKE 'Alpha%'$$, 'must','il_ix');
    PERFORM bt_run('ilike','fold: mixed pattern', 'il', $$v ILIKE 'aLpHa%'$$, 'must','il_ix');
    PERFORM bt_run('ilike','fold: suffix',        'il', $$v ILIKE '%BETA'$$,  'must','il_ix');
    PERFORM bt_run('ilike','fold: infix',         'il', $$v ILIKE '%GaMmA%'$$,'must','il_ix');
    PERFORM bt_run('ilike','fold: exact',         'il', $$v ILIKE 'DeLtA'$$,  'must','il_ix');
END $BT$;

-- ---- ILIKE: the LIKE wildcard matrix, repeated -----------------------------
-- The wildcard machinery is shared between the two operators, but it is
-- driven from a different structure set, so "it works under LIKE" is not
-- evidence that it works under ILIKE.
DO $BT$ BEGIN
    PERFORM bt_run('ilike','underscore lead',   'il', $$v ILIKE '_LPHA'$$,        'must','il_ix');
    PERFORM bt_run('ilike','underscore mid',    'il', $$v ILIKE 'A_PHA'$$,        'must','il_ix');
    PERFORM bt_run('ilike','underscore trail',  'il', $$v ILIKE 'ALPH_'$$,        'must','il_ix');
    PERFORM bt_run('ilike','underscore run',    'il', $$v ILIKE '_____'$$,        'must','il_ix');
    PERFORM bt_run('ilike','two segments',      'il', $$v ILIKE 'MI%ED%'$$,       'must','il_ix');
    PERFORM bt_run('ilike','adjacent percents', 'il', $$v ILIKE 'ALPHA%%'$$,      'must','il_ix');
    PERFORM bt_run('ilike','escaped percent',   'il', $$v ILIKE '100\%'$$,        'must','il_ix');
    PERFORM bt_run('ilike','escaped underscore','il', $$v ILIKE 'A\_B'$$,         'must','il_ix');
    PERFORM bt_run('ilike','data percent',      'il', $$v ILIKE 'a\%b'$$,         'must','il_ix');
    PERFORM bt_run('ilike','match everything',  'il', $$v ILIKE '%'$$,            'either','il_ix');
    PERFORM bt_run('ilike','empty pattern',     'il', $$v ILIKE ''$$,             'either','il_ix');
    PERFORM bt_run('ilike','no match',          'il', $$v ILIKE 'ZZZZZ%'$$,       'must','il_ix');
END $BT$;

-- ---- ILIKE: non-ASCII folding ----------------------------------------------
-- Expectation is deliberately 'either' for this group. Whether the access
-- method serves a non-ASCII ILIKE or declines it is a design choice with a
-- real correctness argument on both sides -- Biscuit already refuses the
-- analogous '~*' rewrite when the emitted glob is non-ASCII, precisely
-- because ILIKE and regex case-insensitivity are not the same relation over
-- non-ASCII text. What is NOT negotiable, and is what these cases assert, is
-- that the answer matches a sequential scan either way.
DO $BT$ BEGIN
    PERFORM bt_run('ilike','accent: A-umlaut',  'il', $$v ILIKE 'ÄHNLICH%'$$,     'either','il_ix');
    PERFORM bt_run('ilike','accent: a-umlaut',  'il', $$v ILIKE 'ähnlich%'$$,     'either','il_ix');
    PERFORM bt_run('ilike','accent: U-umlaut',  'il', $$v ILIKE 'ÜBER%'$$,        'either','il_ix');
    PERFORM bt_run('ilike','accent: E-acute',   'il', $$v ILIKE 'élan%'$$,        'either','il_ix');
    PERFORM bt_run('ilike','accent: infix',     'il', $$v ILIKE '%RÜN%'$$,        'either','il_ix');
    PERFORM bt_run('ilike','sharp s',           'il', $$v ILIKE 'stra%e%'$$,      'either','il_ix');
    PERFORM bt_run('ilike','sharp s expanded',  'il', $$v ILIKE 'STRASSE%'$$,     'either','il_ix');
    PERFORM bt_run('ilike','greek sigma',       'il', $$v ILIKE 'σοφ%'$$,         'either','il_ix');
    PERFORM bt_run('ilike','greek upper',       'il', $$v ILIKE 'ΣΟΦ%'$$,         'either','il_ix');
    PERFORM bt_run('ilike','turkish dotted I',  'il', $$v ILIKE 'istanbul%'$$,    'either','il_ix');
    PERFORM bt_run('ilike','turkish dotless i', 'il', $$v ILIKE 'ID%'$$,          'either','il_ix');
    PERFORM bt_run('ilike','underscore vs mb',  'il', $$v ILIKE '_BER%'$$,        'either','il_ix');
END $BT$;

-- ---- NOT ILIKE -------------------------------------------------------------
DO $BT$ BEGIN
    PERFORM bt_run('ilike','not prefix',      'il', $$v NOT ILIKE 'ALPHA%'$$,   'must','il_ix');
    PERFORM bt_run('ilike','not suffix',      'il', $$v NOT ILIKE '%beta'$$,    'must','il_ix');
    PERFORM bt_run('ilike','not infix',       'il', $$v NOT ILIKE '%GaMmA%'$$,  'must','il_ix');
    PERFORM bt_run('ilike','not exact',       'il', $$v NOT ILIKE 'DeLtA'$$,    'must','il_ix');
    PERFORM bt_run('ilike','not underscore',  'il', $$v NOT ILIKE 'A_PHA'$$,    'must','il_ix');
    PERFORM bt_run('ilike','not escaped pct', 'il', $$v NOT ILIKE '100\%'$$,    'must','il_ix');
    PERFORM bt_run('ilike','not accent',      'il', $$v NOT ILIKE 'ÜBER%'$$,    'either','il_ix');
    PERFORM bt_run('ilike','not match-all',   'il', $$v NOT ILIKE '%'$$,        'either','il_ix');
    PERFORM bt_run('ilike','not no-match',    'il', $$v NOT ILIKE 'ZZZZZ%'$$,   'must','il_ix');
END $BT$;


-- ---- Algebraic identities --------------------------------------------------
-- Differential testing proves the index agrees with a sequential scan. These
-- identities prove that what both of them compute is actually ILIKE: a shared
-- misunderstanding would pass every case above and fail here.
DO $BT$
DECLARE
    a bigint; b bigint; c bigint; d bigint; n_null int; n_total int;
BEGIN
    SET LOCAL enable_seqscan = off;

    -- 1. Case-blindness: four patterns differing only in case, one answer.
    SELECT count(*) INTO a FROM il WHERE v ILIKE 'alpha%';
    SELECT count(*) INTO b FROM il WHERE v ILIKE 'ALPHA%';
    SELECT count(*) INTO c FROM il WHERE v ILIKE 'Alpha%';
    SELECT count(*) INTO d FROM il WHERE v ILIKE 'aLpHa%';
    PERFORM bt_assert(a = b AND b = c AND c = d,
        format('ILIKE must be case-blind, got %s / %s / %s / %s', a, b, c, d));

    -- 2. ILIKE must be a strict superset of LIKE for the same pattern.
    SELECT count(*) INTO a FROM il WHERE v ILIKE 'alpha%';
    SELECT count(*) INTO b FROM il WHERE v LIKE  'alpha%';
    PERFORM bt_assert(a > b,
        format('ILIKE (%s) should match strictly more than LIKE (%s) on a '
               'fixture containing ALPHA, Alpha and aLpHa', a, b));

    -- 3. ILIKE over ASCII must equal LIKE over lower(). Restricted to the
    --    ASCII stems on purpose: this identity is exactly what does NOT hold
    --    over 'ß' and the Turkish dotted i, which is why those are tested
    --    only differentially above.
    SELECT count(*) INTO a FROM il WHERE v ILIKE 'mixed%';
    SELECT count(*) INTO b FROM il WHERE lower(v) LIKE 'mixed%';
    PERFORM bt_assert(a = b,
        format('over ASCII, ILIKE (%s) must equal LIKE over lower() (%s)', a, b));

    -- 4. NULLs satisfy neither ILIKE nor NOT ILIKE.
    SELECT count(*) INTO n_null  FROM il WHERE v IS NULL;
    SELECT count(*) INTO n_total FROM il;
    SELECT count(*) INTO a FROM il WHERE v ILIKE     '%a%';
    SELECT count(*) INTO b FROM il WHERE v NOT ILIKE '%a%';
    PERFORM bt_assert(a + b = n_total - n_null,
        format('ILIKE (%s) + NOT ILIKE (%s) should equal non-NULL rows (%s)',
               a, b, n_total - n_null));
END $BT$;

DO $BT$ BEGIN PERFORM bt_check('ilike'); END $BT$;
