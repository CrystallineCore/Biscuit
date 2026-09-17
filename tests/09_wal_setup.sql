-- ===========================================================================
--  09_wal_setup.sql  --  crash-recovery, part 1 of 2
--
--  A custom access method has to WAL-log everything it writes, or a crash
--  leaves an index that disagrees with the heap. The failure is quiet: the
--  index still answers queries, it just answers them from a structure that
--  lost its last few thousand updates, so rows silently disappear from
--  results. Nothing errors and nothing in the logs says so.
--
--  This file sets up state that can only survive a crash if it was properly
--  logged, and records the answers it expects afterwards INTO A TABLE, so the
--  expectations survive the crash alongside the data they describe. The
--  runner then kills the server with `pg_ctl stop -m immediate` -- no clean
--  shutdown, no final checkpoint, recovery from WAL on restart -- and
--  09_wal_verify.sql checks the index against those recorded answers.
--
--  The ordering below is the whole point. Everything after the explicit
--  CHECKPOINT exists only in WAL at the moment of the crash: if any of it is
--  missing from the index after recovery, that work was never logged.
--
--  Structured as:
--    1. base data, index, CHECKPOINT           <- safely on disk
--    2. inserts, updates, deletes              <- WAL only
--    3. a second index built in an explicit transaction  <- WAL only
--    4. an UNLOGGED table with its own index    <- must be RESET, not recovered
--    5. expected answers recorded              <- WAL only
-- ===========================================================================

DROP TABLE IF EXISTS wl CASCADE;
DROP TABLE IF EXISTS wl_expected CASCADE;
DROP TABLE IF EXISTS wl_unlogged CASCADE;

-- ---- 1. base data, indexed, then forced to disk ----------------------------
CREATE TABLE wl (id int PRIMARY KEY, v text, w text);

INSERT INTO wl (id, v, w)
SELECT g,
       CASE WHEN (g / 10) % 20 = 0 THEN s.stem
            ELSE s.stem || '-' || substr(md5(g::text), 1, 8) END,
       'col2-' || (g % 50)
FROM generate_series(1, 20000) AS g
JOIN (VALUES
    (0,'alpha'), (1,'beta'), (2,'gamma'), (3,'delta'), (4,'omega'),
    (5,'ALPHA'), (6,'a.c'),  (7,'100%'),  (8,'a_b'),   (9,'Über')
) AS s(k, stem) ON s.k = g % 10;

CREATE INDEX wl_ix ON wl USING biscuit (v);

CHECKPOINT;

-- ---- 2. everything from here exists only in WAL ----------------------------
INSERT INTO wl (id, v, w)
SELECT 100000 + g, 'post-checkpoint-' || g, 'col2-new'
FROM generate_series(1, 2000) AS g;

INSERT INTO wl (id, v, w) VALUES
    (200001, 'crash-test-alpha', 'col2-x'),
    (200002, 'CRASH-TEST-ALPHA', 'col2-x'),
    (200003, 'crash.test.dots',  'col2-x'),
    (200004, '100%-crash',       'col2-x'),
    (200005, 'Über-crash',       'col2-x'),
    (200006, NULL,               'col2-x'),
    (200007, '',                 'col2-x');

UPDATE wl SET v = 'updated-post-checkpoint-' || id
 WHERE v LIKE 'gamma%' AND id % 3 = 0;

DELETE FROM wl WHERE v LIKE 'omega%' AND id % 2 = 0;

-- A transaction that commits before the crash. Recovery must replay it whole.
BEGIN;
INSERT INTO wl (id, v, w)
SELECT 300000 + g, 'committed-txn-' || g, 'col2-txn'
FROM generate_series(1, 500) AS g;
-- ---- 3. a second index, built inside that same transaction ----------------
-- The build itself has to be logged, not just the rows. An index created
-- after the last checkpoint is the case most likely to come back empty.
CREATE INDEX wl_ix_w ON wl USING biscuit (w);
COMMIT;

-- A transaction that does NOT commit. Recovery must discard it entirely --
-- the opposite failure, where a crash makes uncommitted work visible.
BEGIN;
INSERT INTO wl (id, v, w)
SELECT 400000 + g, 'never-committed-' || g, 'col2-abort'
FROM generate_series(1, 500) AS g;
ROLLBACK;

-- ---- 4. an UNLOGGED table ---------------------------------------------------
-- Unlogged relations are deliberately NOT crash-safe: PostgreSQL truncates
-- them during recovery. The index must be reset with the table rather than
-- surviving as a structure describing rows that are no longer there -- an
-- index left pointing at a truncated heap is the sharpest version of the
-- "index disagrees with heap" failure, because every entry in it is stale.
CREATE UNLOGGED TABLE wl_unlogged (id int PRIMARY KEY, v text);
INSERT INTO wl_unlogged (id, v)
SELECT g, 'unlogged-' || g FROM generate_series(1, 5000) AS g;
CREATE INDEX wl_un_ix ON wl_unlogged USING biscuit (v);

-- ---- 5. record what we expect to see afterwards ----------------------------
-- Computed now, from a SEQUENTIAL SCAN, so the expectations are the heap's
-- answers and not the index's. They are written to a logged table, which puts
-- them through the same recovery as the data they describe.
CREATE TABLE wl_expected (
    case_id    serial PRIMARY KEY,
    rel        text,
    predicate  text,
    exp_rows   bigint,
    exp_hash   bigint
);

DO $BT$
DECLARE
    preds text[] := ARRAY[
        $q$v LIKE 'alpha%'$q$,
        $q$v LIKE 'post-checkpoint-%'$q$,
        $q$v LIKE 'crash-test-%'$q$,
        $q$v LIKE 'committed-txn-%'$q$,
        $q$v LIKE 'never-committed-%'$q$,
        $q$v LIKE 'updated-post-checkpoint-%'$q$,
        $q$v LIKE 'gamma%'$q$,
        $q$v LIKE 'omega%'$q$,
        $q$v LIKE '100\%-crash'$q$,
        $q$v ILIKE 'CRASH-TEST-ALPHA'$q$,
        $q$v ~ '^post-checkpoint-1.{3}$'$q$,
        $q$v ~ 'crash.test.dots'$q$,
        $q$v ~ '^Über'$q$,
        $q$v !~ '^post-checkpoint'$q$,
        $q$v NOT LIKE 'alpha%'$q$,
        $q$v LIKE 'post-checkpoint-%' AND w LIKE 'col2-new'$q$,
        $q$w LIKE 'col2-txn'$q$,
        $q$w LIKE 'col2-4%'$q$,
        $q$w ~ '^col2-1.$'$q$
    ];
    p text; n bigint; h bigint;
BEGIN
    SET LOCAL enable_indexscan  = off;
    SET LOCAL enable_bitmapscan = off;

    FOREACH p IN ARRAY preds LOOP
        EXECUTE format('SELECT count(*), coalesce(sum(hashtext(id::text)::bigint),0) '
                       'FROM wl WHERE %s', p) INTO n, h;
        INSERT INTO wl_expected (rel, predicate, exp_rows, exp_hash)
        VALUES ('wl', p, n, h);
    END LOOP;

    RAISE NOTICE 'wal : recorded % expected answers from a sequential scan',
                 array_length(preds, 1);
END $BT$;

-- Force a WAL segment boundary, so recovery has to cross one rather than
-- replaying a single contiguous run.
SELECT pg_switch_wal();

-- Deliberately NO checkpoint here. Everything after step 1 must come back
-- from the write-ahead log or not at all.
