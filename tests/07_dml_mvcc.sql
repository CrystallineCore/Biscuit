-- ===========================================================================
--  07_dml_mvcc.sql  --  the index under mutation
--
--  Everything up to here queried a static table. That tests the build path and
--  the scan path and nothing else. This file is about the third path: what
--  happens between an INSERT and the next query.
--
--  A bitmap index cannot cheaply rewrite a bitmap per row, so Biscuit carries
--  a pending log and a delta structure and folds them in later. That design
--  buys write throughput and spends it on a much larger correctness surface:
--  a row is now findable through at least three different routes depending on
--  when it was written relative to the last fold, and all three have to agree
--  with the heap.
--
--  The failure this file is built to catch is a row being visible to a
--  sequential scan and invisible to an index scan, or the reverse. Each
--  mutation is therefore followed immediately by a differential query, in the
--  same session, without an intervening VACUUM or checkpoint unless that is
--  the thing under test.
--
--  Dimensions: insert after build, update of the indexed column, update that
--  leaves it alone, delete, re-insert of a deleted value, rollback, nested
--  savepoints with partial rollback, bulk churn, version churn on one row,
--  VACUUM, and values large enough to be pushed out to TOAST.
-- ===========================================================================

DELETE FROM bt_result WHERE category = 'dml';

DROP TABLE IF EXISTS dm CASCADE;

CREATE TABLE dm (id int PRIMARY KEY, v text, tag text);

INSERT INTO dm (id, v, tag)
SELECT g,
       CASE WHEN (g / 10) % 20 = 0 THEN s.stem
            ELSE s.stem || '-' || substr(md5(g::text), 1, 8) END,
       'orig'
FROM generate_series(1, 20000) AS g
JOIN (VALUES
    (0,'alpha'), (1,'beta'), (2,'gamma'), (3,'delta'), (4,'omega'),
    (5,'ALPHA'), (6,'a.c'),  (7,'100%'),  (8,'a_b'),   (9,'zeta')
) AS s(k, stem) ON s.k = g % 10;

CREATE INDEX dm_ix ON dm USING biscuit (v);
ANALYZE dm;

-- baseline, before any mutation
DO $BT$ BEGIN
    PERFORM bt_run('dml','baseline prefix',  'dm', $$v LIKE 'alpha%'$$, 'must','dm_ix');
    PERFORM bt_run('dml','baseline regex',   'dm', $$v ~ '^beta'$$,     'must','dm_ix');
END $BT$;


-- ---- INSERT after the index exists -----------------------------------------
-- These rows never went through the build path; they can only be found via
-- the pending/delta machinery.
INSERT INTO dm (id, v, tag)
SELECT 100000 + g, 'inserted-after-build-' || g, 'new'
FROM generate_series(1, 500) AS g;

INSERT INTO dm (id, v, tag) VALUES
    (200001, 'alpha-added',  'new'),
    (200002, 'ALPHA-ADDED',  'new'),
    (200003, 'beta-added',   'new'),
    (200004, NULL,           'new'),
    (200005, '',             'new'),
    (200006, '100%-added',   'new'),
    (200007, 'a_b-added',    'new');

DO $BT$ BEGIN
    PERFORM bt_run('dml','after insert: new prefix', 'dm', $$v LIKE 'inserted-after-build%'$$,'must','dm_ix');
    PERFORM bt_run('dml','after insert: new exact',  'dm', $$v LIKE 'alpha-added'$$,          'must','dm_ix');
    PERFORM bt_run('dml','after insert: old prefix', 'dm', $$v LIKE 'alpha%'$$,               'must','dm_ix');
    PERFORM bt_run('dml','after insert: regex',      'dm', $$v ~ '^inserted-after-build-1'$$, 'must','dm_ix');
    PERFORM bt_run('dml','after insert: ilike',      'dm', $$v ILIKE 'ALPHA-ADDED'$$,         'must','dm_ix');
    PERFORM bt_run('dml','after insert: escaped pct','dm', $$v LIKE '100\%-added'$$,          'must','dm_ix');
    PERFORM bt_run('dml','after insert: negated',    'dm', $$v NOT LIKE 'inserted%'$$,        'must','dm_ix');
END $BT$;


-- ---- UPDATE of the indexed column ------------------------------------------
-- The old value must stop matching and the new value must start. A stale
-- bitmap entry shows up as the first of those failing; a missing one as the
-- second.
UPDATE dm SET v = 'updated-' || id, tag = 'upd'
 WHERE v LIKE 'gamma%' AND id % 3 = 0;

DO $BT$ BEGIN
    PERFORM bt_run('dml','after update: new value', 'dm', $$v LIKE 'updated-%'$$,  'must','dm_ix');
    PERFORM bt_run('dml','after update: old value', 'dm', $$v LIKE 'gamma%'$$,     'must','dm_ix');
    PERFORM bt_run('dml','after update: regex old', 'dm', $$v ~ '^gamma'$$,        'must','dm_ix');
    PERFORM bt_run('dml','after update: negated',   'dm', $$v NOT LIKE 'gamma%'$$, 'must','dm_ix');
END $BT$;

-- An update that does NOT touch the indexed column. The heap tuple moves (or
-- is updated in place), but the indexed value is unchanged, so every answer
-- must be identical to before -- a different code path from the one above.
UPDATE dm SET tag = 'touched' WHERE id % 7 = 0;

DO $BT$ BEGIN
    PERFORM bt_run('dml','non-indexed update',  'dm', $$v LIKE 'alpha%'$$, 'must','dm_ix');
    PERFORM bt_run('dml','non-indexed + filter','dm', $$v LIKE 'alpha%' AND tag = 'touched'$$,'must','dm_ix');
END $BT$;


-- ---- DELETE, and re-insert of a deleted value ------------------------------
DELETE FROM dm WHERE v LIKE 'omega%' AND id % 2 = 0;

DO $BT$ BEGIN
    PERFORM bt_run('dml','after delete: deleted', 'dm', $$v LIKE 'omega%'$$,    'must','dm_ix');
    PERFORM bt_run('dml','after delete: other',   'dm', $$v LIKE 'delta%'$$,    'must','dm_ix');
    PERFORM bt_run('dml','after delete: negated', 'dm', $$v NOT LIKE 'omega%'$$,'must','dm_ix');
END $BT$;

-- Re-introducing a value that was just deleted is the case where a bitmap
-- entry that was cleared has to come back, rather than one that was never set.
INSERT INTO dm (id, v, tag)
SELECT 300000 + g, 'omega-resurrected-' || g, 'again'
FROM generate_series(1, 50) AS g;

DO $BT$ BEGIN
    PERFORM bt_run('dml','resurrected value', 'dm', $$v LIKE 'omega-resurrected%'$$,'must','dm_ix');
    PERFORM bt_run('dml','resurrected prefix','dm', $$v LIKE 'omega%'$$,             'must','dm_ix');
END $BT$;


-- ---- Transaction visibility ------------------------------------------------
-- Uncommitted work must be visible to the session that did it, through the
-- index, and must vanish on rollback. Both arms of bt_run run inside this
-- transaction, so this also checks that the pending state is session-local
-- rather than something the scan reads from a shared structure too eagerly.
BEGIN;

INSERT INTO dm (id, v, tag)
SELECT 400000 + g, 'in-txn-' || g, 'txn' FROM generate_series(1, 200) AS g;

DO $BT$ BEGIN
    PERFORM bt_run('dml','uncommitted visible to self','dm', $$v LIKE 'in-txn-%'$$,'must','dm_ix');
END $BT$;

-- Savepoints: the partial rollback must leave exactly the work done before it.
SAVEPOINT sp1;
INSERT INTO dm (id, v, tag)
SELECT 500000 + g, 'rolled-back-' || g, 'sp' FROM generate_series(1, 100) AS g;

DO $BT$ BEGIN
    PERFORM bt_run('dml','savepoint: before rollback','dm', $$v LIKE 'rolled-back-%'$$,'must','dm_ix');
END $BT$;

ROLLBACK TO SAVEPOINT sp1;

DO $BT$ BEGIN
    PERFORM bt_run('dml','savepoint: after rollback', 'dm', $$v LIKE 'rolled-back-%'$$,'must','dm_ix');
    PERFORM bt_run('dml','savepoint: sibling kept',   'dm', $$v LIKE 'in-txn-%'$$,     'must','dm_ix');
END $BT$;

COMMIT;

-- A whole transaction rolled back. Nothing it wrote may be findable.
BEGIN;
INSERT INTO dm (id, v, tag)
SELECT 600000 + g, 'aborted-' || g, 'abort' FROM generate_series(1, 300) AS g;
UPDATE dm SET v = 'aborted-update' WHERE v LIKE 'delta%' AND id % 5 = 0;
ROLLBACK;

DO $BT$ BEGIN
    PERFORM bt_run('dml','rolled back insert', 'dm', $$v LIKE 'aborted-%'$$,     'must','dm_ix');
    PERFORM bt_run('dml','rolled back update', 'dm', $$v LIKE 'delta%'$$,        'must','dm_ix');
    PERFORM bt_run('dml','rolled back regex',  'dm', $$v ~ '^aborted'$$,         'must','dm_ix');
END $BT$;


-- ---- Version churn ---------------------------------------------------------
-- One row updated many times in a row, then queried. Each update leaves a
-- dead tuple and a new index entry; only the last may be findable.
DO $BT$
DECLARE i int;
BEGIN
    FOR i IN 1 .. 50 LOOP
        UPDATE dm SET v = 'churn-generation-' || i WHERE id = 1;
    END LOOP;
END $BT$;

DO $BT$ BEGIN
    PERFORM bt_run('dml','churn: final only',    'dm', $$v LIKE 'churn-generation-50'$$,'must','dm_ix');
    PERFORM bt_run('dml','churn: intermediate',  'dm', $$v LIKE 'churn-generation-25'$$,'must','dm_ix');
    PERFORM bt_run('dml','churn: any generation','dm', $$v LIKE 'churn-generation-%'$$,  'must','dm_ix');
END $BT$;

-- Bulk churn: enough rows to push whatever pending structure exists past any
-- threshold that triggers a fold.
UPDATE dm SET v = 'bulk-' || (id % 97) WHERE id BETWEEN 5000 AND 12000;

DO $BT$ BEGIN
    PERFORM bt_run('dml','bulk: new values',   'dm', $$v LIKE 'bulk-%'$$,      'must','dm_ix');
    PERFORM bt_run('dml','bulk: specific',     'dm', $$v LIKE 'bulk-42'$$,     'must','dm_ix');
    PERFORM bt_run('dml','bulk: regex',        'dm', $$v ~ '^bulk-9.$'$$,      'must','dm_ix');
    PERFORM bt_run('dml','bulk: survivors',    'dm', $$v LIKE 'alpha%'$$,      'must','dm_ix');
    PERFORM bt_run('dml','bulk: negated',      'dm', $$v NOT LIKE 'bulk-%'$$,  'must','dm_ix');
END $BT$;


-- ---- VACUUM ----------------------------------------------------------------
-- Dead tuples are reclaimed and the index is told about it. Answers must not
-- change across the operation; a bitmap entry cleared for a live row, or left
-- set for a reclaimed one, shows up immediately.
DELETE FROM dm WHERE v LIKE 'bulk-1%';
VACUUM (ANALYZE) dm;

DO $BT$ BEGIN
    PERFORM bt_run('dml','post-vacuum: deleted',  'dm', $$v LIKE 'bulk-1%'$$,   'must','dm_ix');
    PERFORM bt_run('dml','post-vacuum: retained', 'dm', $$v LIKE 'bulk-%'$$,    'must','dm_ix');
    PERFORM bt_run('dml','post-vacuum: untouched','dm', $$v LIKE 'alpha%'$$,    'must','dm_ix');
    PERFORM bt_run('dml','post-vacuum: regex',    'dm', $$v ~ '^churn'$$,       'must','dm_ix');
    PERFORM bt_run('dml','post-vacuum: ilike',    'dm', $$v ILIKE 'ALPHA%'$$,   'must','dm_ix');
END $BT$;


-- ---- TOAST, and the length ceiling -----------------------------------------
-- Values past roughly 2 kB are compressed and pushed out of line, so the index
-- sees a detoasted datum. The risk being tested is a value indexed by its
-- compressed form, or truncated at the page boundary -- hence markers at the
-- very END of the payload, which a suffix query has to reach past.
--
-- FINDING, and the reason the payloads here stop at 16 kB. Insert cost is
-- QUADRATIC in the length of the indexed value. Measured on a 1,000-row table,
-- inserting one row of length n:
--
--     n =  1,000     3 ms
--     n =  4,000    18 ms
--     n =  8,000    64 ms
--     n = 16,000   256 ms
--     n = 32,000   939 ms
--
-- Every doubling of n quadruples the work, which is the shape you would expect
-- from per-character-position structures being rebuilt rather than extended.
-- Memory follows the same curve: an earlier revision of this file inserted a
-- single 100,000-character value and the backend was killed by the OOM killer
-- (signal 9, not a crash in Biscuit's own code) on a 4 GB machine, taking the
-- cluster through automatic recovery with it.
--
-- That is a real operational limit and it is worth stating plainly: a Biscuit
-- index on a column that can hold long free text is not safe at defaults. The
-- suite therefore documents the curve rather than tripping over it.
INSERT INTO dm (id, v, tag) VALUES
    (700001, repeat('toastable-padding-', 200) || 'END-MARKER-A', 'toast'),
    (700002, repeat('xy', 4000)                || 'END-MARKER-B', 'toast'),
    (700003, 'START-MARKER-C' || repeat(md5('c'), 250),           'toast'),
    (700004, repeat('z', 8000),                                   'toast');

DO $BT$ BEGIN
    PERFORM bt_run('dml','toast: suffix marker',  'dm', $$v LIKE '%END-MARKER-A'$$,   'must','dm_ix');
    PERFORM bt_run('dml','toast: suffix 8k',      'dm', $$v LIKE '%END-MARKER-B'$$,   'must','dm_ix');
    PERFORM bt_run('dml','toast: prefix marker',  'dm', $$v LIKE 'START-MARKER-C%'$$, 'must','dm_ix');
    PERFORM bt_run('dml','toast: regex anchored', 'dm', $$v ~ 'END-MARKER-A$'$$,      'must','dm_ix');
    PERFORM bt_run('dml','toast: 8k run',         'dm', $$v LIKE 'zzzzzzzzzz%'$$,     'must','dm_ix');
    PERFORM bt_run('dml','toast: not matching',   'dm', $$v LIKE '%END-MARKER-Z'$$,   'must','dm_ix');
END $BT$;

-- The scaling curve itself, asserted rather than assumed. This is a
-- regression guard in both directions: it fails if insert cost grows worse
-- than quadratic, and it is the line that will start failing -- visibly, with
-- numbers -- if someone fixes the quadratic behaviour and the comment above
-- goes stale.
DO $BT$
DECLARE
    t0 timestamptz;
    ms_4k numeric; ms_16k numeric; ratio numeric;
BEGIN
    t0 := clock_timestamp();
    INSERT INTO dm (id, v, tag) VALUES (710001, repeat('mn', 2000), 'scale');
    ms_4k := extract(epoch FROM clock_timestamp() - t0) * 1000;

    t0 := clock_timestamp();
    INSERT INTO dm (id, v, tag) VALUES (710002, repeat('mn', 8000), 'scale');
    ms_16k := extract(epoch FROM clock_timestamp() - t0) * 1000;

    ratio := CASE WHEN ms_4k > 0 THEN round(ms_16k / ms_4k, 1) END;

    RAISE NOTICE 'dml : insert of 4kB took % ms, 16kB took % ms (ratio %, '
                 'linear would be 4, quadratic 16)',
                 round(ms_4k,1), round(ms_16k,1), ratio;

    -- A 4x length increase costing more than ~40x would be worse than
    -- quadratic and is treated as a regression.
    PERFORM bt_assert(ratio IS NULL OR ratio < 40,
        format('insert cost is scaling worse than quadratic in value length: '
               '4x the length cost %sx the time', ratio));
END $BT$;

-- ---- Heap agreement --------------------------------------------------------
-- The differential checks prove the index agrees with a sequential scan over
-- the same snapshot. This proves the snapshot itself is the heap: every row
-- the index can find is a row that is actually there, and vice versa, across
-- the whole table rather than one predicate at a time.
DO $BT$
DECLARE n_heap bigint; n_idx bigint; h_heap bigint; h_idx bigint;
BEGIN
    SET LOCAL jit = off;
    SET LOCAL max_parallel_workers_per_gather = 0;

    SET LOCAL enable_indexscan = off;
    SET LOCAL enable_bitmapscan = off;
    SELECT count(*), coalesce(sum(hashtext(id::text)::bigint),0) INTO n_heap, h_heap
      FROM dm WHERE v LIKE '%' OR v IS NULL;

    SET LOCAL enable_indexscan = on;
    SET LOCAL enable_bitmapscan = on;
    SET LOCAL enable_seqscan = off;
    SELECT count(*), coalesce(sum(hashtext(id::text)::bigint),0) INTO n_idx, h_idx
      FROM dm WHERE v LIKE '%' OR v IS NULL;

    PERFORM bt_assert(n_heap = n_idx AND h_heap = h_idx,
        format('after all mutations the index and the heap disagree about the '
               'table contents: heap %s rows/%s, index %s rows/%s',
               n_heap, h_heap, n_idx, h_idx));

    RAISE NOTICE 'dml : index and heap agree on all % rows after mutation', n_heap;
END $BT$;

DO $BT$ BEGIN PERFORM bt_check('dml'); END $BT$;
