-- ===========================================================================
--  03_regex.sql  --  ~  !~  ~*  !~*
--
--  Biscuit does not implement a regex engine. It decomposes the subset of
--  POSIX ARE that is exactly a LIKE glob in disguise, and refuses everything
--  else. That design has two failure modes and this file is built around
--  them, because they are not symmetrical:
--
--    COMPLETENESS  refusing a regex that IS in the subset. Costs a sequential
--                  scan. Annoying, never wrong.
--    SOUNDNESS     accepting a regex that is NOT exactly a glob. Biscuit is
--                  an exact-match access method -- for the non-case-folded
--                  strategies it leaves xs_recheck false and nothing
--                  downstream re-tests the predicate -- so an approximate
--                  rewrite returns wrong rows silently.
--
--  Every case below therefore carries an explicit 'must' or 'must_not', and
--  a violation in EITHER direction fails the suite. 'either' is reserved for
--  regexes that decompose to a bare '%', which the AM may decline as a
--  match-everything optimisation.
-- ===========================================================================

DELETE FROM bt_result WHERE category = 'regex';

DROP TABLE IF EXISTS rx CASCADE;

CREATE TABLE rx (id int PRIMARY KEY, v text);

INSERT INTO rx (id, v)
SELECT g,
       CASE WHEN (g / 40) % 20 = 0 THEN s.stem
            ELSE s.stem || '-' || substr(md5(g::text), 1, 8) END
FROM generate_series(1, 40000) AS g
JOIN (VALUES
    ( 0,'alpha'),      ( 1,'beta'),       ( 2,'gamma'),      ( 3,'delta'),
    ( 4,'abc'),        ( 5,'axc'),        ( 6,'a.c'),        ( 7,'aQc'),
    ( 8,'12345'),      ( 9,'1234567'),    (10,'xyzzy'),      (11,'x-any-y'),
    (12,'endswith-z'), (13,'p-mid-q'),    (14,'zztop'),      (15,'dot.value'),
    (16,'100%'),       (17,'a_b'),        (18,'back\slash'), (19,'node-042'),
    (20,'svc-core'),   (21,'svcXXXXtail'),(22,'error'),      (23,'errOor'),
    (24,'aaa'),        (25,'qq'),         (26,'user_id'),    (27,'UPPERCASE'),
    (28,'mixedCase'),  (29,'tailXY'),     (30,'$literal'),   (31,'k9-unit'),
    (32,'seg-mid-end'),(33,'Ähnlich'),    (34,'Über'),       (35,'théta'),
    (36,'%_\'),        (37,'0123456789abcdef0123456789abcdef01234567'),
    (38,'a|b'),        (39,'[abc]')
) AS s(k, stem) ON s.k = g % 40;

INSERT INTO rx (id, v) VALUES (90001, NULL), (90002, ''), (90003, '   ');

CREATE INDEX rx_ix ON rx USING biscuit (v);
ANALYZE rx;


-- ---- '~' inside the decomposable subset ------------------------------------
DO $BT$ BEGIN
    PERFORM bt_run('regex','anchor start',      'rx', $$v ~ '^alpha'$$,          'must','rx_ix');
    PERFORM bt_run('regex','anchor end',        'rx', $$v ~ 'beta$'$$,           'must','rx_ix');
    PERFORM bt_run('regex','both anchors',      'rx', $$v ~ '^gamma$'$$,         'must','rx_ix');
    PERFORM bt_run('regex','unanchored',        'rx', $$v ~ 'delta'$$,           'must','rx_ix');
    PERFORM bt_run('regex','dot',               'rx', $$v ~ 'a.c'$$,             'must','rx_ix');
    PERFORM bt_run('regex','dot anchored left', 'rx', $$v ~ '^a.c'$$,            'must','rx_ix');
    PERFORM bt_run('regex','dot anchored right','rx', $$v ~ 'a.c$'$$,            'must','rx_ix');
    PERFORM bt_run('regex','dot run',           'rx', $$v ~ '^.....$'$$,         'must','rx_ix');
    PERFORM bt_run('regex','dot{n}',            'rx', $$v ~ '^.{5}$'$$,          'must','rx_ix');
    PERFORM bt_run('regex','dot{n,}',           'rx', $$v ~ '^.{3,}$'$$,         'must','rx_ix');
    PERFORM bt_run('regex','dot{n} unanchored', 'rx', $$v ~ '.{8}'$$,            'must','rx_ix');
    PERFORM bt_run('regex','dot-star between',  'rx', $$v ~ '^x.*y$'$$,          'must','rx_ix');
    PERFORM bt_run('regex','dot-star trailing', 'rx', $$v ~ '^x.*'$$,            'must','rx_ix');
    PERFORM bt_run('regex','dot-star leading',  'rx', $$v ~ '.*z$'$$,            'must','rx_ix');
    PERFORM bt_run('regex','dot-plus between',  'rx', $$v ~ '^p.+q$'$$,          'must','rx_ix');
    PERFORM bt_run('regex','dot-plus trailing', 'rx', $$v ~ 'zz.+'$$,            'must','rx_ix');
    PERFORM bt_run('regex','multi segment',     'rx', $$v ~ '^seg.*mid.*end$'$$, 'must','rx_ix');
    PERFORM bt_run('regex','literal{n}',        'rx', $$v ~ '^a{3}'$$,           'must','rx_ix');
    PERFORM bt_run('regex','literal{n} infix',  'rx', $$v ~ 'q{2}'$$,            'must','rx_ix');
    PERFORM bt_run('regex','long literal',      'rx', $$v ~ '^0123456789abcdef'$$,'must','rx_ix');
    PERFORM bt_run('regex','exact length 40',   'rx', $$v ~ '^.{40}$'$$,         'must','rx_ix');
END $BT$;

-- ---- '~' escapes: a backslash before a NON-alphanumeric is a plain literal
--      and is safe to unwrap; before an alphanumeric it is a class shorthand
--      or backreference and must be refused (tested in the next group).
DO $BT$ BEGIN
    PERFORM bt_run('regex','escaped dot',       'rx', $$v ~ '\.'$$,              'must','rx_ix');
    PERFORM bt_run('regex','escaped dash',      'rx', $$v ~ '\-'$$,              'must','rx_ix');
    PERFORM bt_run('regex','escaped percent',   'rx', $$v ~ '\%'$$,              'must','rx_ix');
    PERFORM bt_run('regex','escaped underscore','rx', $$v ~ '\_'$$,              'must','rx_ix');
    PERFORM bt_run('regex','escaped backslash', 'rx', $$v ~ '\\'$$,              'must','rx_ix');
    PERFORM bt_run('regex','escaped dollar',    'rx', $$v ~ '^\$'$$,             'must','rx_ix');
    PERFORM bt_run('regex','all metachars',     'rx', $$v ~ '^\%\_\\$'$$,        'must','rx_ix');
    PERFORM bt_run('regex','escaped pct + anchor','rx',$$v ~ '100\%$'$$,         'must','rx_ix');
END $BT$;

-- ---- '~' outside the subset: must be refused -------------------------------
DO $BT$ BEGIN
    PERFORM bt_run('regex','bracket',           'rx', $$v ~ '[abc]'$$,           'must_not','rx_ix');
    PERFORM bt_run('regex','bracket negated',   'rx', $$v ~ '[^abc]'$$,          'must_not','rx_ix');
    PERFORM bt_run('regex','bracket range',     'rx', $$v ~ '[0-9]{2}'$$,        'must_not','rx_ix');
    PERFORM bt_run('regex','posix class',       'rx', $$v ~ '[[:digit:]]'$$,     'must_not','rx_ix');
    PERFORM bt_run('regex','group + alt',       'rx', $$v ~ '(alpha|beta)'$$,    'must_not','rx_ix');
    PERFORM bt_run('regex','bare alternation',  'rx', $$v ~ 'alpha|beta'$$,      'must_not','rx_ix');
    PERFORM bt_run('regex','quantified group',  'rx', $$v ~ '(ab)+'$$,           'must_not','rx_ix');
    PERFORM bt_run('regex','backreference',     'rx', $$v ~ '(a)\1'$$,           'must_not','rx_ix');
    PERFORM bt_run('regex','star on literal',   'rx', $$v ~ 'a*'$$,              'must_not','rx_ix');
    PERFORM bt_run('regex','plus on literal',   'rx', $$v ~ 'a+'$$,              'must_not','rx_ix');
    PERFORM bt_run('regex','quest on literal',  'rx', $$v ~ 'a?'$$,              'must_not','rx_ix');
    PERFORM bt_run('regex','quest on dot',      'rx', $$v ~ '.?'$$,              'must_not','rx_ix');
    PERFORM bt_run('regex','bounded {n,m}',     'rx', $$v ~ 'x{2,4}'$$,          'must_not','rx_ix');
    PERFORM bt_run('regex','shorthand \d',      'rx', $$v ~ '\d'$$,              'must_not','rx_ix');
    PERFORM bt_run('regex','shorthand \w',      'rx', $$v ~ '\w'$$,              'must_not','rx_ix');
    PERFORM bt_run('regex','shorthand \s',      'rx', $$v ~ '\s'$$,              'must_not','rx_ix');
    PERFORM bt_run('regex','word boundary',     'rx', $$v ~ '\yalpha'$$,         'must_not','rx_ix');
    PERFORM bt_run('regex','anchor mid-pattern','rx', $$v ~ '^a$b'$$,            'must_not','rx_ix');
    PERFORM bt_run('regex','caret mid-pattern', 'rx', $$v ~ 'a^b'$$,             'must_not','rx_ix');
    PERFORM bt_run('regex','embedded option',   'rx', $$v ~ '(?i)alpha'$$,       'must_not','rx_ix');
END $BT$;

-- ---- '~*' : decomposed onto ILIKE, which over-matches at worst, so the glob
--      is a usable SUPERSET and the scan sets recheck. ASCII patterns only:
--      a non-ASCII '~*' is refused because ILIKE would DROP qualifying rows
--      ('I' ~* 'ı' is true where the ILIKE form is false), and a subset is
--      not something recheck can repair.
DO $BT$ BEGIN
    PERFORM bt_run('regex','ci anchor start',   'rx', $$v ~* '^ALPHA'$$,         'must','rx_ix');
    PERFORM bt_run('regex','ci anchor end',     'rx', $$v ~* 'BETA$'$$,          'must','rx_ix');
    PERFORM bt_run('regex','ci both anchors',   'rx', $$v ~* '^GAMMA$'$$,        'must','rx_ix');
    PERFORM bt_run('regex','ci mixed literal',  'rx', $$v ~* 'dElTa'$$,          'must','rx_ix');
    PERFORM bt_run('regex','ci dot',            'rx', $$v ~* 'A.C'$$,            'must','rx_ix');
    PERFORM bt_run('regex','ci dot-star',       'rx', $$v ~* '^X.*Y$'$$,         'must','rx_ix');
    PERFORM bt_run('regex','ci dot-plus',       'rx', $$v ~* '^P.+Q$'$$,         'must','rx_ix');
    PERFORM bt_run('regex','ci escaped dot',    'rx', $$v ~* '\.'$$,             'must','rx_ix');
    PERFORM bt_run('regex','ci folded infix',   'rx', $$v ~* 'MIXEDCASE'$$,      'must','rx_ix');
    PERFORM bt_run('regex','ci ASCII vs mb data','rx',$$v ~* 'ber$'$$,           'must','rx_ix');
    PERFORM bt_run('regex','ci non-ASCII',      'rx', $$v ~* '^Ä'$$,             'must_not','rx_ix');
    PERFORM bt_run('regex','ci non-ASCII infix','rx', $$v ~* 'théta'$$,          'must_not','rx_ix');
    PERFORM bt_run('regex','ci bracket',        'rx', $$v ~* '[ABC]'$$,          'must_not','rx_ix');
    PERFORM bt_run('regex','ci star on literal','rx', $$v ~* 'A*'$$,             'must_not','rx_ix');
    PERFORM bt_run('regex','ci alternation',    'rx', $$v ~* '(A|B)'$$,          'must_not','rx_ix');
END $BT$;

-- ---- '!~' : negated, case-sensitive. The glob is exact, so its complement
--      is exact too, and the AM can serve it.
DO $BT$ BEGIN
    PERFORM bt_run('regex','not anchor start',  'rx', $$v !~ '^alpha'$$,         'must','rx_ix');
    PERFORM bt_run('regex','not anchor end',    'rx', $$v !~ 'beta$'$$,          'must','rx_ix');
    PERFORM bt_run('regex','not dot',           'rx', $$v !~ 'a.c'$$,            'must','rx_ix');
    PERFORM bt_run('regex','not dot{n}',        'rx', $$v !~ '^.{5}$'$$,         'must','rx_ix');
    PERFORM bt_run('regex','not escaped dot',   'rx', $$v !~ '\.'$$,             'must','rx_ix');
    PERFORM bt_run('regex','not dot-plus',      'rx', $$v !~ 'zz.+'$$,           'must','rx_ix');
    PERFORM bt_run('regex','not bracket',       'rx', $$v !~ '[abc]'$$,          'must_not','rx_ix');
    PERFORM bt_run('regex','not star',          'rx', $$v !~ 'a*'$$,             'must_not','rx_ix');
    PERFORM bt_run('regex','not alternation',   'rx', $$v !~ '(a|b)'$$,          'must_not','rx_ix');
END $BT$;

-- ---- '!~*' : refused unconditionally, and correctly so. '~*' is served as
--      an ILIKE SUPERSET repaired by recheck; the complement of a superset is
--      a SUBSET, and recheck cannot put back rows the index never returned.
--      Every case here must fall back, including the ones whose glob is
--      plain ASCII and would look perfectly safe.
DO $BT$ BEGIN
    PERFORM bt_run('regex','not-ci anchored',   'rx', $$v !~* '^ALPHA'$$,        'must_not','rx_ix');
    PERFORM bt_run('regex','not-ci anchor end', 'rx', $$v !~* 'BETA$'$$,         'must_not','rx_ix');
    PERFORM bt_run('regex','not-ci dot',        'rx', $$v !~* 'A.C'$$,           'must_not','rx_ix');
    PERFORM bt_run('regex','not-ci escaped',    'rx', $$v !~* '\.'$$,            'must_not','rx_ix');
    PERFORM bt_run('regex','not-ci non-ASCII',  'rx', $$v !~* '^Ä'$$,            'must_not','rx_ix');
    PERFORM bt_run('regex','not-ci bracket',    'rx', $$v !~* '[ABC]'$$,         'must_not','rx_ix');
END $BT$;

-- ---- degenerate forms ------------------------------------------------------
-- These decompose to a bare '%'. Biscuit declines match-everything globs as a
-- general optimisation, the same way it declines LIKE '%', so either outcome
-- is correct and only the answer is asserted.
DO $BT$ BEGIN
    PERFORM bt_run('regex','bare caret',        'rx', $$v ~ '^'$$,               'either','rx_ix');
    PERFORM bt_run('regex','bare dollar',       'rx', $$v ~ '$'$$,               'either','rx_ix');
    PERFORM bt_run('regex','empty pattern',     'rx', $$v ~ ''$$,                'either','rx_ix');
    PERFORM bt_run('regex','dot-star only',     'rx', $$v ~ '^.*$'$$,            'either','rx_ix');
    PERFORM bt_run('regex','dot{0,}',           'rx', $$v ~ '^.{0,}$'$$,         'either','rx_ix');
END $BT$;


-- ---- Identities ------------------------------------------------------------
DO $BT$
DECLARE a bigint; b bigint; n_null int; n_total int;
BEGIN
    SET LOCAL enable_seqscan = off;

    -- 1. An anchored regex and the glob it decomposes to must agree exactly.
    --    This is the rewrite's own correctness claim, checked directly rather
    --    than inferred from both matching a sequential scan.
    SELECT count(*) INTO a FROM rx WHERE v ~    '^alpha';
    SELECT count(*) INTO b FROM rx WHERE v LIKE 'alpha%';
    PERFORM bt_assert(a = b,
        format('~ ''^alpha'' (%s) must equal LIKE ''alpha%%'' (%s)', a, b));

    SELECT count(*) INTO a FROM rx WHERE v ~    '^gamma$';
    SELECT count(*) INTO b FROM rx WHERE v LIKE 'gamma';
    PERFORM bt_assert(a = b,
        format('~ ''^gamma$'' (%s) must equal LIKE ''gamma'' (%s)', a, b));

    SELECT count(*) INTO a FROM rx WHERE v ~    'a.c';
    SELECT count(*) INTO b FROM rx WHERE v LIKE '%a_c%';
    PERFORM bt_assert(a = b,
        format('~ ''a.c'' (%s) must equal LIKE ''%%a_c%%'' (%s)', a, b));

    -- 2. '~*' must agree with the same pattern under ILIKE, over ASCII.
    SELECT count(*) INTO a FROM rx WHERE v ~*    '^ALPHA';
    SELECT count(*) INTO b FROM rx WHERE v ILIKE 'ALPHA%';
    PERFORM bt_assert(a = b,
        format('~* ''^ALPHA'' (%s) must equal ILIKE ''ALPHA%%'' (%s)', a, b));

    -- 3. Negation is exact: ~ and !~ must partition the non-NULL rows.
    SELECT count(*) INTO n_null  FROM rx WHERE v IS NULL;
    SELECT count(*) INTO n_total FROM rx;
    SELECT count(*) INTO a FROM rx WHERE v ~  '^alpha';
    SELECT count(*) INTO b FROM rx WHERE v !~ '^alpha';
    PERFORM bt_assert(a + b = n_total - n_null,
        format('~ (%s) + !~ (%s) must partition the non-NULL rows (%s)',
               a, b, n_total - n_null));

    -- 4. The same must hold for the pair the AM refuses to serve, where both
    --    sides come from a sequential scan -- a cheap check that falling back
    --    does not itself change the answer.
    SELECT count(*) INTO a FROM rx WHERE v ~*  '^ALPHA';
    SELECT count(*) INTO b FROM rx WHERE v !~* '^ALPHA';
    PERFORM bt_assert(a + b = n_total - n_null,
        format('~* (%s) + !~* (%s) must partition the non-NULL rows (%s)',
               a, b, n_total - n_null));
END $BT$;

DO $BT$ BEGIN PERFORM bt_check('regex'); END $BT$;
