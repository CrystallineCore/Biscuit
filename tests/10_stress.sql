-- ===========================================================================
--  10_stress.sql  --  randomised differential fuzzing
--
--  Every other file in this suite tests patterns a human chose, which means it
--  tests the cases a human thought of. This one generates patterns from a
--  grammar instead, and checks each against the same oracle: PostgreSQL's own
--  matching over a sequential scan.
--
--  Determinism matters more than novelty here. setseed() is fixed, so a
--  failure is reproducible and bisectable rather than a story about a run that
--  happened once on someone's laptop. Changing the seed explores more of the
--  space; changing it CASUALLY turns a regression suite into a flaky one.
--
--  The generator draws its literal fragments from the corpus itself, so most
--  patterns match something. A fuzzer that emitted random bytes would produce
--  a few hundred queries that all return zero rows and agree trivially.
-- ===========================================================================

DELETE FROM bt_result WHERE category = 'stress';

DROP TABLE IF EXISTS st CASCADE;

CREATE TABLE st (id int PRIMARY KEY, v text);

INSERT INTO st (id, v)
SELECT g,
       CASE WHEN (g / 20) % 15 = 0 THEN s.stem
            ELSE s.stem || (ARRAY['-','_','.','','%'])[1 + (g % 5)]
                        || substr(md5(g::text), 1, 1 + (g % 8)) END
FROM generate_series(1, 20000) AS g
JOIN (VALUES
    ( 0,'alpha'),  ( 1,'beta'),   ( 2,'gamma'),  ( 3,'delta'),
    ( 4,'ALPHA'),  ( 5,'Beta'),   ( 6,'a.c'),    ( 7,'a_b'),
    ( 8,'100%'),   ( 9,'back\s'), (10,'Über'),   (11,'théta'),
    (12,'日本'),    (13,'😀x'),    (14,'xyzzy'),  (15,'q'),
    (16,'qq'),     (17,''),       (18,'  sp  '), (19,'0123456789')
) AS s(k, stem) ON s.k = g % 20;

INSERT INTO st (id, v) VALUES (90001, NULL), (90002, '');

CREATE INDEX st_ix ON st USING biscuit (v);
ANALYZE st;


-- ---------------------------------------------------------------------------
--  One fuzz case: evaluate the predicate both ways and record it. No timing,
--  because a few hundred cases at three executions each would dominate the
--  suite's runtime and prove nothing that the per-category files do not
--  already measure.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION st_case(p_op text, p_pattern text)
RETURNS void AS $$
DECLARE
    q  text;
    sn bigint; sh bigint;
    in_ bigint; ih bigint;
    pl text;
BEGIN
    q := format('SELECT count(*), coalesce(sum(hashtext(id::text)::bigint),0) '
                'FROM st WHERE v %s %L', p_op, p_pattern);

    SET LOCAL jit = off;
    SET LOCAL max_parallel_workers_per_gather = 0;

    SET LOCAL enable_indexscan  = off;
    SET LOCAL enable_bitmapscan = off;
    SET LOCAL enable_seqscan    = on;
    EXECUTE q INTO sn, sh;

    SET LOCAL enable_indexscan  = on;
    SET LOCAL enable_bitmapscan = on;
    SET LOCAL enable_seqscan    = off;
    pl := bt_plan(q);
    EXECUTE q INTO in_, ih;

    INSERT INTO bt_result (category, label, rel, predicate, index_name, expect,
                           seq_rows, seq_hash, seq_ms, seq_node,
                           idx_rows, idx_hash, idx_ms, idx_node,
                           idx_used, idx_natural)
    VALUES ('stress', p_op || ' ' || p_pattern, 'st',
            format('v %s %L', p_op, p_pattern), 'st_ix', 'either',
            sn, sh, 0, 'Seq Scan',
            in_, ih, 0, bt_scan_node(pl),
            position('"st_ix"' IN pl) > 0, NULL);
EXCEPTION
    -- A generated pattern can be an invalid regex. That is the generator's
    -- problem, not the index's, so skip it rather than failing the suite --
    -- but only for the specific error class, so a genuine executor error
    -- still propagates.
    WHEN invalid_regular_expression THEN
        RETURN;
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------------------
--  The generator.
-- ---------------------------------------------------------------------------
DO $BT$
DECLARE
    frags  text[] := ARRAY['alpha','beta','gam','elt','a.c','a_b','100','ck\s',
                           'ber','thé','日','😀','xyz','q','012','sp','ALP','Bet'];
    globs  text[] := ARRAY['%','_','%%','_%','%_'];
    rxq    text[] := ARRAY['.','.*','.+','.{2}','.{3,}','.{1}'];
    ops    text[] := ARRAY['LIKE','NOT LIKE','ILIKE','NOT ILIKE'];
    rops   text[] := ARRAY['~','!~','~*','!~*'];
    i int; j int; parts int;
    pat text; op text;
    n_glob int := 0; n_rx int := 0;
BEGIN
    PERFORM setseed(0.42);

    -- ---- glob fuzz --------------------------------------------------------
    FOR i IN 1 .. 200 LOOP
        pat   := '';
        parts := 1 + floor(random() * 3)::int;

        IF random() < 0.5 THEN
            pat := pat || globs[1 + floor(random() * array_length(globs,1))::int];
        END IF;

        FOR j IN 1 .. parts LOOP
            pat := pat || frags[1 + floor(random() * array_length(frags,1))::int];
            IF j < parts OR random() < 0.6 THEN
                pat := pat || globs[1 + floor(random() * array_length(globs,1))::int];
            END IF;
        END LOOP;

        op := ops[1 + floor(random() * array_length(ops,1))::int];
        PERFORM st_case(op, pat);
        n_glob := n_glob + 1;
    END LOOP;

    -- ---- regex fuzz -------------------------------------------------------
    -- Deliberately straddles the decomposable boundary: some of these are
    -- exact globs in disguise and some are not, and the generator does not
    -- know which. That is the point -- the interesting failures live at the
    -- edge, where the decomposer has to decide.
    FOR i IN 1 .. 200 LOOP
        pat   := '';
        parts := 1 + floor(random() * 3)::int;

        IF random() < 0.4 THEN pat := '^' || pat; END IF;

        FOR j IN 1 .. parts LOOP
            pat := pat || frags[1 + floor(random() * array_length(frags,1))::int];
            IF random() < 0.7 THEN
                pat := pat || rxq[1 + floor(random() * array_length(rxq,1))::int];
            END IF;
        END LOOP;

        IF random() < 0.4 THEN pat := pat || '$'; END IF;

        op := rops[1 + floor(random() * array_length(rops,1))::int];
        PERFORM st_case(op, pat);
        n_rx := n_rx + 1;
    END LOOP;

    RAISE NOTICE 'stress : generated % glob and % regex patterns from seed 0.42',
                 n_glob, n_rx;
END $BT$;


-- ---------------------------------------------------------------------------
--  A fuzz run that never exercises the index proves nothing, and a fuzz run
--  where every pattern matches everything proves nearly as little. Both are
--  checked, so the suite fails loudly if the generator degrades rather than
--  quietly reporting several hundred trivial passes.
-- ---------------------------------------------------------------------------
DO $BT$
DECLARE n_total int; n_indexed int; n_nontrivial int; n_zero int;
BEGIN
    SELECT count(*),
           count(*) FILTER (WHERE idx_used),
           count(*) FILTER (WHERE seq_rows > 0 AND seq_rows < 20000),
           count(*) FILTER (WHERE seq_rows = 0)
      INTO n_total, n_indexed, n_nontrivial, n_zero
      FROM bt_result WHERE category = 'stress';

    RAISE NOTICE 'stress : % cases, % served by index, % with a non-trivial '
                 'result set, % matching nothing',
                 n_total, n_indexed, n_nontrivial, n_zero;

    PERFORM bt_assert(n_total >= 300,
        format('generator produced only %s usable cases', n_total));
    PERFORM bt_assert(n_indexed > n_total / 10,
        format('only %s of %s generated patterns reached the index -- the '
               'generator is not exercising the thing under test',
               n_indexed, n_total));
    PERFORM bt_assert(n_nontrivial > n_total / 5,
        format('only %s of %s patterns produced a non-trivial result set',
               n_nontrivial, n_total));
END $BT$;

DO $BT$ BEGIN PERFORM bt_check('stress'); END $BT$;
