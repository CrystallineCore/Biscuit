-- ===========================================================================
--  06_opclass.sql  --  operator class gating
--
--  Biscuit ships three operator classes over text, and the choice is a real
--  storage trade-off rather than a formality: the case-insensitive operators
--  need a second, case-folded structure set, so an index that will never
--  serve them can avoid building it.
--
--    biscuit_ops        (default)  strategies 1-8: LIKE, NOT LIKE, ILIKE,
--                                  NOT ILIKE, ~, !~, ~*, !~*
--    biscuit_like_ops              strategies 1, 2, 5, 6 only -- the
--                                  case-SENSITIVE half
--    biscuit_ilike_ops             strategies 3, 4, 7, 8 only -- the
--                                  case-INSENSITIVE half
--
--  Gating has to hold in both directions to be worth anything. A class that
--  quietly served an operator it does not list would be reading structures it
--  never built; a class that refused one it does list would be a silent
--  performance regression. Each operator is therefore asserted 'must' against
--  the class that claims it and 'must_not' against the class that does not.
--
--  One asymmetry is expected and encoded below: '!~*' is 'must_not' even
--  under biscuit_ilike_ops, which does list strategy 8. The access method
--  refuses it for a correctness reason that has nothing to do with the
--  operator class -- '~*' is served as an ILIKE superset repaired by recheck,
--  and the complement of a superset is a subset that recheck cannot repair.
--  Opclass membership grants permission; it does not override that.
-- ===========================================================================

DELETE FROM bt_result WHERE category = 'opclass';

DROP TABLE IF EXISTS oc_all CASCADE;
DROP TABLE IF EXISTS oc_like CASCADE;
DROP TABLE IF EXISTS oc_ilike CASCADE;

CREATE TABLE oc_all (id int PRIMARY KEY, v text);

INSERT INTO oc_all (id, v)
SELECT g,
       CASE WHEN (g / 12) % 20 = 0 THEN s.stem
            ELSE s.stem || '-' || substr(md5(g::text), 1, 8) END
FROM generate_series(1, 20000) AS g
JOIN (VALUES
    (0,'alpha'), (1,'ALPHA'), (2,'Alpha'), (3,'beta'),
    (4,'BETA'),  (5,'Beta'),  (6,'gamma'), (7,'GAMMA'),
    (8,'a.c'),   (9,'aXc'),   (10,'100%'), (11,'')
) AS s(k, stem) ON s.k = g % 12;

INSERT INTO oc_all (id, v) VALUES (90001, NULL);

CREATE TABLE oc_like  AS SELECT * FROM oc_all;
CREATE TABLE oc_ilike AS SELECT * FROM oc_all;
ALTER TABLE oc_like  ADD PRIMARY KEY (id);
ALTER TABLE oc_ilike ADD PRIMARY KEY (id);

CREATE INDEX oc_all_ix   ON oc_all   USING biscuit (v);
CREATE INDEX oc_like_ix  ON oc_like  USING biscuit (v biscuit_like_ops);
CREATE INDEX oc_ilike_ix ON oc_ilike USING biscuit (v biscuit_ilike_ops);

ANALYZE oc_all;
ANALYZE oc_like;
ANALYZE oc_ilike;


-- ---- biscuit_ops: the default class claims all eight -----------------------
DO $BT$ BEGIN
    PERFORM bt_run('opclass','default: LIKE',      'oc_all', $$v LIKE 'alpha%'$$,     'must','oc_all_ix');
    PERFORM bt_run('opclass','default: NOT LIKE',  'oc_all', $$v NOT LIKE 'alpha%'$$, 'must','oc_all_ix');
    PERFORM bt_run('opclass','default: ILIKE',     'oc_all', $$v ILIKE 'ALPHA%'$$,    'must','oc_all_ix');
    PERFORM bt_run('opclass','default: NOT ILIKE', 'oc_all', $$v NOT ILIKE 'ALPHA%'$$,'must','oc_all_ix');
    PERFORM bt_run('opclass','default: ~',         'oc_all', $$v ~ '^alpha'$$,        'must','oc_all_ix');
    PERFORM bt_run('opclass','default: !~',        'oc_all', $$v !~ '^alpha'$$,       'must','oc_all_ix');
    PERFORM bt_run('opclass','default: ~*',        'oc_all', $$v ~* '^ALPHA'$$,       'must','oc_all_ix');
    -- listed as strategy 8, refused by the AM for the subset reason above
    PERFORM bt_run('opclass','default: !~*',       'oc_all', $$v !~* '^ALPHA'$$,      'must_not','oc_all_ix');
END $BT$;

-- ---- biscuit_like_ops: case-sensitive half only ----------------------------
DO $BT$ BEGIN
    PERFORM bt_run('opclass','like_ops: LIKE',      'oc_like', $$v LIKE 'alpha%'$$,     'must','oc_like_ix');
    PERFORM bt_run('opclass','like_ops: NOT LIKE',  'oc_like', $$v NOT LIKE 'alpha%'$$, 'must','oc_like_ix');
    PERFORM bt_run('opclass','like_ops: ~',         'oc_like', $$v ~ '^alpha'$$,        'must','oc_like_ix');
    PERFORM bt_run('opclass','like_ops: !~',        'oc_like', $$v !~ '^alpha'$$,       'must','oc_like_ix');
    PERFORM bt_run('opclass','like_ops: ~ dotted',  'oc_like', $$v ~ '^a.c$'$$,         'must','oc_like_ix');
    -- not listed: must fall back, and must still be right
    PERFORM bt_run('opclass','like_ops: ILIKE',     'oc_like', $$v ILIKE 'ALPHA%'$$,    'must_not','oc_like_ix');
    PERFORM bt_run('opclass','like_ops: NOT ILIKE', 'oc_like', $$v NOT ILIKE 'ALPHA%'$$,'must_not','oc_like_ix');
    PERFORM bt_run('opclass','like_ops: ~*',        'oc_like', $$v ~* '^ALPHA'$$,       'must_not','oc_like_ix');
    PERFORM bt_run('opclass','like_ops: !~*',       'oc_like', $$v !~* '^ALPHA'$$,      'must_not','oc_like_ix');
END $BT$;

-- ---- biscuit_ilike_ops: case-insensitive half only -------------------------
DO $BT$ BEGIN
    PERFORM bt_run('opclass','ilike_ops: ILIKE',     'oc_ilike', $$v ILIKE 'ALPHA%'$$,    'must','oc_ilike_ix');
    PERFORM bt_run('opclass','ilike_ops: NOT ILIKE', 'oc_ilike', $$v NOT ILIKE 'ALPHA%'$$,'must','oc_ilike_ix');
    PERFORM bt_run('opclass','ilike_ops: ~*',        'oc_ilike', $$v ~* '^ALPHA'$$,       'must','oc_ilike_ix');
    PERFORM bt_run('opclass','ilike_ops: ~* dotted', 'oc_ilike', $$v ~* '^A.C$'$$,        'must','oc_ilike_ix');
    PERFORM bt_run('opclass','ilike_ops: !~*',       'oc_ilike', $$v !~* '^ALPHA'$$,      'must_not','oc_ilike_ix');
    -- not listed: must fall back
    PERFORM bt_run('opclass','ilike_ops: LIKE',      'oc_ilike', $$v LIKE 'alpha%'$$,     'must_not','oc_ilike_ix');
    PERFORM bt_run('opclass','ilike_ops: NOT LIKE',  'oc_ilike', $$v NOT LIKE 'alpha%'$$, 'must_not','oc_ilike_ix');
    PERFORM bt_run('opclass','ilike_ops: ~',         'oc_ilike', $$v ~ '^alpha'$$,        'must_not','oc_ilike_ix');
    PERFORM bt_run('opclass','ilike_ops: !~',        'oc_ilike', $$v !~ '^alpha'$$,       'must_not','oc_ilike_ix');
END $BT$;


-- ---- Catalogue agreement ---------------------------------------------------
-- The gating above is observed from query plans. This checks it against the
-- catalogue that is supposed to be driving it, so a class that behaved
-- correctly by accident rather than by its pg_amop rows would be caught.
DO $BT$
DECLARE n_all int; n_like int; n_ilike int;
BEGIN
    SELECT count(*) INTO n_all FROM pg_amop
     WHERE amopfamily = (SELECT oid FROM pg_opfamily
                          WHERE opfname = 'biscuit_ops'
                            AND opfmethod = (SELECT oid FROM pg_am WHERE amname='biscuit'));
    SELECT count(*) INTO n_like FROM pg_amop
     WHERE amopfamily = (SELECT oid FROM pg_opfamily
                          WHERE opfname = 'biscuit_like_ops'
                            AND opfmethod = (SELECT oid FROM pg_am WHERE amname='biscuit'));
    SELECT count(*) INTO n_ilike FROM pg_amop
     WHERE amopfamily = (SELECT oid FROM pg_opfamily
                          WHERE opfname = 'biscuit_ilike_ops'
                            AND opfmethod = (SELECT oid FROM pg_am WHERE amname='biscuit'));

    PERFORM bt_assert(n_all = 8,
        format('biscuit_ops should list 8 operators, lists %s', n_all));
    PERFORM bt_assert(n_like = 4,
        format('biscuit_like_ops should list 4 operators, lists %s', n_like));
    PERFORM bt_assert(n_ilike = 4,
        format('biscuit_ilike_ops should list 4 operators, lists %s', n_ilike));
    PERFORM bt_assert(n_like + n_ilike = n_all,
        'the two half classes should partition the default class');

    RAISE NOTICE 'opclass : pg_amop lists %/%/% operators for ops/like_ops/ilike_ops',
                 n_all, n_like, n_ilike;
END $BT$;

-- ---- Storage consequence ---------------------------------------------------
-- The half classes exist to avoid building a structure set that will never be
-- read. If that is true, each must be materially smaller than the default
-- class over identical data -- and if it is NOT true, the classes are costing
-- users flexibility for nothing, which is worth knowing either way.
DO $BT$
DECLARE s_all bigint; s_like bigint; s_ilike bigint;
BEGIN
    s_all   := pg_relation_size('oc_all_ix');
    s_like  := pg_relation_size('oc_like_ix');
    s_ilike := pg_relation_size('oc_ilike_ix');

    RAISE NOTICE 'opclass : index sizes -- default % kB, like_ops % kB, ilike_ops % kB',
                 s_all/1024, s_like/1024, s_ilike/1024;

    PERFORM bt_assert(s_like < s_all,
        format('biscuit_like_ops (%s bytes) should be smaller than the default '
               'class (%s bytes), which also builds the case-folded structures',
               s_like, s_all));
    PERFORM bt_assert(s_ilike < s_all,
        format('biscuit_ilike_ops (%s bytes) should be smaller than the default '
               'class (%s bytes)', s_ilike, s_all));
END $BT$;

DO $BT$ BEGIN PERFORM bt_check('opclass'); END $BT$;
