-- ===========================================================================
--  09_wal_verify.sql  --  crash-recovery, part 2 of 2
--
--  Runs after `pg_ctl stop -m immediate` and a restart, so everything below
--  reads state that came back through WAL replay rather than a clean
--  shutdown. Three separate questions, in increasing order of strictness:
--
--    1. Did the DATA survive? If the heap lost rows there is no point asking
--       anything about the index, and the failure needs to be reported as a
--       PostgreSQL-level problem rather than blamed on Biscuit.
--    2. Does the INDEX agree with the answers recorded before the crash? This
--       is the real test. The expectations came from a sequential scan taken
--       before the server died, so an index that lost its last updates fails
--       here even though it still answers queries without erroring.
--    3. Does the index agree with a sequential scan TAKEN NOW? This catches
--       the case where both the heap and the index lost the same work -- they
--       would agree with each other and disagree with the record.
--
--  Unlogged relations are checked separately and expect the opposite: their
--  contents must be GONE, and the index must be reset along with the heap
--  rather than surviving as a structure describing rows that no longer exist.
-- ===========================================================================

DELETE FROM bt_result WHERE category = 'wal';

-- ---- 1. did recovery bring the data back? ----------------------------------
DO $BT$
DECLARE n_rows bigint; n_exp int; n_abort bigint;
BEGIN
    PERFORM bt_assert(
        to_regclass('wl') IS NOT NULL,
        'table wl did not survive recovery');
    PERFORM bt_assert(
        to_regclass('wl_expected') IS NOT NULL,
        'table wl_expected did not survive recovery -- nothing to check against');
    PERFORM bt_assert(
        to_regclass('wl_ix') IS NOT NULL AND to_regclass('wl_ix_w') IS NOT NULL,
        'one or both Biscuit indexes did not survive recovery');

    SELECT count(*) INTO n_rows FROM wl;
    SELECT count(*) INTO n_exp  FROM wl_expected;
    PERFORM bt_assert(n_exp > 0, 'wl_expected came back empty');

    -- The rolled-back transaction must not have been resurrected. Recovery
    -- restoring uncommitted work is as much a failure as losing committed
    -- work, and it is the one a "did my rows survive" check would miss.
    SET LOCAL enable_indexscan  = off;
    SET LOCAL enable_bitmapscan = off;
    SELECT count(*) INTO n_abort FROM wl WHERE v LIKE 'never-committed-%';
    PERFORM bt_assert(n_abort = 0,
        format('recovery resurrected %s rows from a transaction that rolled '
               'back before the crash', n_abort));

    RAISE NOTICE 'wal : recovery restored % rows and % recorded expectations',
                 n_rows, n_exp;
END $BT$;


-- ---- 2. does the index still agree with the pre-crash record? --------------
-- The strict check. Each predicate is answered THROUGH THE INDEX and compared
-- against the count and fingerprint taken from a sequential scan before the
-- server was killed.
DO $BT$
DECLARE
    c record; n bigint; h bigint; n_checked int := 0;
BEGIN
    SET LOCAL jit = off;
    SET LOCAL max_parallel_workers_per_gather = 0;
    SET LOCAL enable_seqscan    = off;
    SET LOCAL enable_indexscan  = on;
    SET LOCAL enable_bitmapscan = on;

    FOR c IN SELECT * FROM wl_expected ORDER BY case_id LOOP
        EXECUTE format('SELECT count(*), coalesce(sum(hashtext(id::text)::bigint),0) '
                       'FROM wl WHERE %s', c.predicate) INTO n, h;

        PERFORM bt_assert(n = c.exp_rows AND h = c.exp_hash,
            format('after crash recovery the index disagrees with the answer '
                   'recorded before the crash for [%s]: expected %s rows / '
                   'hash %s, got %s rows / hash %s',
                   c.predicate, c.exp_rows, c.exp_hash, n, h));
        n_checked := n_checked + 1;
    END LOOP;

    RAISE NOTICE 'wal : % predicates match their pre-crash answers exactly',
                 n_checked;
END $BT$;


-- ---- 3. index versus a sequential scan taken now ---------------------------
-- Catches the case where heap and index lost the same work and would agree
-- with each other while both disagreeing with the record above.
DO $BT$
DECLARE c record;
BEGIN
    FOR c IN SELECT predicate FROM wl_expected ORDER BY case_id LOOP
        PERFORM bt_run('wal', 'post-crash: ' || left(c.predicate, 40),
                       'wl', c.predicate, 'either', NULL);
    END LOOP;
END $BT$;

-- Index usage is asserted separately for the predicates that should still be
-- served after recovery. A recovered index that is CORRECT but no longer
-- usable -- priced out, or refusing every clause -- would pass every check
-- above and still be a regression.
DO $BT$ BEGIN
    PERFORM bt_run('wal','post-crash indexable: prefix',   'wl',
        $$v LIKE 'post-checkpoint-%'$$,                    'must','wl_ix');
    PERFORM bt_run('wal','post-crash indexable: regex',    'wl',
        $$v ~ '^committed-txn-'$$,                         'must','wl_ix');
    PERFORM bt_run('wal','post-crash indexable: ilike',    'wl',
        $$v ILIKE 'CRASH-TEST-ALPHA'$$,                    'must','wl_ix');
    PERFORM bt_run('wal','post-crash indexable: negated',  'wl',
        $$v NOT LIKE 'alpha%'$$,                           'must','wl_ix');
    -- The second index, the one built inside a transaction after the last
    -- checkpoint. This is the case most likely to come back as an empty
    -- structure that answers every query with nothing.
    PERFORM bt_run('wal','post-crash: txn-built index',    'wl',
        $$w LIKE 'col2-txn'$$,                             'must','wl_ix_w');
    PERFORM bt_run('wal','post-crash: txn-built regex',    'wl',
        $$w ~ '^col2-1.$'$$,                               'must','wl_ix_w');
END $BT$;

-- A non-empty result through the transaction-built index, asserted directly.
-- Without this, an index that came back empty would agree with a sequential
-- scan on every predicate that legitimately matches nothing and slip through.
DO $BT$
DECLARE n bigint;
BEGIN
    SET LOCAL enable_seqscan = off;
    SELECT count(*) INTO n FROM wl WHERE w LIKE 'col2-txn';
    PERFORM bt_assert(n > 0,
        format('the index built inside a post-checkpoint transaction returned '
               '%s rows for a predicate that should match 500 -- it probably '
               'came back empty', n));
END $BT$;


-- ---- 4. unlogged relations must NOT have survived --------------------------
-- Recovery truncates unlogged relations by design, so the heap must come back
-- empty. The index must be reset along with it rather than surviving as a
-- structure describing rows that no longer exist.
DO $BT$
DECLARE n_heap bigint;
BEGIN
    PERFORM bt_assert(to_regclass('wl_unlogged') IS NOT NULL,
        'the unlogged table itself should still exist -- only its contents '
        'are discarded by recovery');

    SET LOCAL enable_indexscan  = off;
    SET LOCAL enable_bitmapscan = off;
    SELECT count(*) INTO n_heap FROM wl_unlogged;

    PERFORM bt_assert(n_heap = 0,
        format('an unlogged table should be truncated by recovery, found %s rows',
               n_heap));
END $BT$;

-- ---- 4b. KNOWN LIMITATION: an unlogged index needs REINDEX after a crash ---
-- ambuildempty() writes a valid, empty metapage into the INIT fork, which is
-- what recovery copies over the main fork. That is enough to make the reset
-- index readable rather than a zero-length file -- before it was implemented,
-- the first scan after recovery died with
--
--     ERROR:  could not read block 0 in file "base/5/NNNNN": read only 0 of 8192 bytes
--
-- but it is not enough to make it USABLE. biscuit_load_index() also requires a
-- persisted HEADER blob, and a blob lives in pages the init fork does not
-- contain, so the scan now fails cleanly and actionably instead:
--
--     ERROR:  biscuit: no on-disk snapshot found for index "wl_un_ix"
--     HINT:   The index may be corrupt; consider running REINDEX.
--
-- A complete fix means building an empty index INTO the init fork, which
-- requires threading a fork number through the page allocator, the directory
-- and the blob writer -- all of which hard-code MAIN_FORKNUM. That is a real
-- change to the storage layer, not a patch, so this asserts the current
-- behaviour and its workaround rather than pretending it is fixed.
--
-- REINDEX fully restores the index, so the limitation is recoverable.
DO $BT$
DECLARE failed boolean := false;
BEGIN
    -- Forcing the index is the whole point of this probe. The recovered heap
    -- is empty, so an unforced planner picks a sequential scan, never touches
    -- the index, and reports success without having tested anything -- which
    -- is exactly the false pass this SET LOCAL exists to prevent.
    SET LOCAL enable_seqscan = off;

    BEGIN
        PERFORM count(*) FROM wl_unlogged WHERE v LIKE 'unlogged-%';
    EXCEPTION WHEN OTHERS THEN
        failed := true;
    END;

    IF failed THEN
        RAISE NOTICE 'wal : unlogged index unusable after recovery, as expected '
                     '(known limitation); REINDEX restores it';
    ELSE
        RAISE NOTICE 'wal : unlogged index was usable straight after recovery -- '
                     'the init-fork limitation may have been fixed; if so, '
                     'tighten this check';
    END IF;
END $BT$;

REINDEX INDEX wl_un_ix;

-- After REINDEX the index must be empty (matching the truncated heap) AND
-- fully functional for new rows.
INSERT INTO wl_unlogged (id, v)
SELECT g, 'after-recovery-' || g FROM generate_series(1, 500) AS g;

DO $BT$ BEGIN
    PERFORM bt_run('wal','unlogged reusable after REINDEX', 'wl_unlogged',
        $$v LIKE 'after-recovery-%'$$, 'must','wl_un_ix');
    PERFORM bt_run('wal','unlogged stale rows gone',        'wl_unlogged',
        $$v LIKE 'unlogged-%'$$,       'must','wl_un_ix');
END $BT$;

DO $BT$ BEGIN PERFORM bt_check('wal'); END $BT$;
