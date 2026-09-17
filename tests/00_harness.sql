-- ===========================================================================
--  00_harness.sql  --  shared infrastructure for the Biscuit test suite
--
--  Pure SQL. No psql meta-commands anywhere in this suite: every file runs
--  unchanged through psql, pgAdmin, DBeaver, JDBC or a migration runner.
--  Failures are raised as exceptions, so a file's exit status is its result
--  and there is no expected-output file to keep in sync.
--
--  THE CENTRAL IDEA
--  ----------------
--  Every test is differential. The same predicate is evaluated twice against
--  the same rows in the same transaction: once with all index paths disabled
--  (the oracle -- PostgreSQL's own LIKE/regex engine over a sequential scan)
--  and once with sequential scans disabled (forcing Biscuit's hand). The two
--  must agree on the row count AND on a fingerprint of which rows came back.
--
--  Counting alone is not enough: two scans can agree on COUNT(*) and still
--  return different rows. Every fixture table in this suite therefore carries
--  an integer `id`, and the fingerprint is the sum of hashtext over the
--  matched ids.
--
--  MEASUREMENT HYGIENE
--  -------------------
--  bt_run() pins three GUCs for every measurement, in BOTH arms:
--
--    jit = off
--      enable_seqscan = off adds disable_cost (1e10) to the plan, which
--      pushes the total past jit_above_cost, jit_inline_above_cost and
--      jit_optimize_above_cost. Every forced query would then be LLVM
--      compiled with inlining and full optimisation -- roughly 40ms, paid per
--      query, independent of the predicate. That cost has nothing to do with
--      the access method but lands squarely in its column. Measured on this
--      fixture: jit on, identical Seq Scan plan with identical buffer counts,
--      60ms with seqscan on versus 92ms with it off; jit off, 59.9 versus
--      60.1.
--
--    max_parallel_workers_per_gather = 0
--      Disabling sequential scans also disables PARALLEL sequential scans, so
--      a suite that left parallelism on would compare a multi-worker baseline
--      against a single-worker forced scan and charge the difference to
--      Biscuit.
--
--    enable_seqscan / enable_indexscan / enable_bitmapscan
--      Set per arm. The baseline arm disables index paths rather than relying
--      on no index existing, so a fixture can build its index up front and
--      both arms still mean what they say.
-- ===========================================================================

CREATE EXTENSION IF NOT EXISTS biscuit;

-- The planner's handling of the enable_* GUCs changed in PostgreSQL 18, which
-- changes what the forced arm of every test means. Reported up front so a run
-- is self-describing rather than needing the version inferred from its
-- results.
DO $BT$
DECLARE v int := current_setting('server_version_num')::int;
BEGIN
    IF v >= 180000 THEN
        RAISE NOTICE 'harness : server_version_num=% -- PG18+ regime: '
                     'disable_cost is gone, so enable_seqscan=off beats a '
                     'cost-priced-out index path. Refusal is asserted against '
                     'the UNFORCED plan.', v;
    ELSE
        RAISE NOTICE 'harness : server_version_num=% -- pre-PG18 regime: '
                     'disable_cost still applies.', v;
    END IF;
END $BT$;


-- ---------------------------------------------------------------------------
--  Result accumulator. Shared by every category file; the runner reports
--  across all of them at the end.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS bt_result CASCADE;

CREATE TABLE bt_result (
    result_id  bigserial PRIMARY KEY,
    category   text NOT NULL,
    label      text NOT NULL,
    rel        text NOT NULL,
    predicate  text NOT NULL,
    index_name text,

    -- 'must'      the access method must serve this when forced
    -- 'must_not'  it must refuse (outside the supported subset, or gated out
    --             by the operator class)
    -- 'either'    both outcomes are correct -- a match-everything pattern the
    --             AM may decline as an optimisation, or a case where only the
    --             answer is under test
    expect     text NOT NULL CHECK (expect IN ('must','must_not','either')),

    seq_rows   bigint,
    seq_hash   bigint,
    seq_ms     numeric(12,3),
    seq_node   text,

    idx_rows   bigint,
    idx_hash   bigint,
    idx_ms     numeric(12,3),
    idx_node   text,
    idx_used   boolean,
    idx_natural boolean,

    delta_rows bigint,
    delta_hash bigint,
    delta_ms   numeric(12,3),
    speedup    numeric(10,2),
    verdict    text
);


-- ---------------------------------------------------------------------------
--  Deltas are derived in a BEFORE trigger, never written by hand, so no path
--  exists by which a stored delta can disagree with the two measurements it
--  comes from -- including an UPDATE that tries to set one directly.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bt_derive() RETURNS trigger AS $$
BEGIN
    IF NEW.seq_rows IS NULL OR NEW.idx_rows IS NULL THEN
        NEW.delta_rows := NULL;
        NEW.delta_hash := NULL;
        NEW.delta_ms   := NULL;
        NEW.speedup    := NULL;
        NEW.verdict    := 'PENDING';
        RETURN NEW;
    END IF;

    NEW.delta_rows := NEW.idx_rows - NEW.seq_rows;
    NEW.delta_hash := NEW.idx_hash - NEW.seq_hash;
    NEW.delta_ms   := round(NEW.idx_ms - NEW.seq_ms, 3);

    -- Guard the division: a sub-microsecond scan reports 0.000 ms.
    NEW.speedup := CASE
        WHEN NEW.idx_ms IS NULL OR NEW.idx_ms <= 0 THEN NULL
        ELSE round(NEW.seq_ms / NEW.idx_ms, 2)
    END;

    -- Correctness outranks everything. A wrong answer is a wrong answer
    -- whether or not the index was involved in producing it.
    --
    -- The two expectations are checked against DIFFERENT arms, and the reason
    -- is a planner change in PostgreSQL 18.
    --
    --   'must'      checked against idx_used, the FORCED arm. "Can the access
    --               method serve this at all?" Forcing is the right question
    --               on every version.
    --
    --   'must_not'  checked against idx_natural, the UNFORCED arm. Biscuit
    --               declines a clause it cannot decompose by pricing the
    --               index path out -- it marks the key lossy and returns an
    --               astronomical cost. Through PostgreSQL 17 that worked even
    --               under enable_seqscan = off, because disabling a node
    --               added disable_cost (1e10) to it and the lossy path still
    --               cost more. PostgreSQL 18 (commit e2225346) removed
    --               disable_cost: the planner now counts disabled nodes per
    --               path and picks the fewest FIRST, consulting cost only to
    --               break ties. A disabled Seq Scan therefore loses to a
    --               lossy index path at any cost whatsoever, and every
    --               opfamily member appears "used" in the forced arm.
    --
    --               So on PG 18 the forced arm can no longer answer "would
    --               the AM decline this?" -- forcing overrides the very
    --               mechanism the AM uses to decline. The unforced arm still
    --               can, because there cost is once again the deciding
    --               factor. That makes idx_natural the portable signal, and
    --               it is why this one assertion reads the arm the rest of
    --               the suite deliberately does not assert on.
    NEW.verdict := CASE
        WHEN NEW.delta_rows <> 0 THEN 'FAIL: row count differs'
        WHEN NEW.delta_hash <> 0 THEN 'FAIL: same count, different rows'
        WHEN NEW.expect = 'must'     AND NOT NEW.idx_used
                                 THEN 'FAIL: index not used'
        WHEN NEW.expect = 'must_not' AND NEW.idx_natural
                                 THEN 'FAIL: index used where it must not be'
        -- Declined on cost, but a forced path exists. Correct, and worth
        -- seeing: on PG 18 this is what an unsupported clause looks like, and
        -- it means enable_seqscan = off will produce a full lossy index scan
        -- rather than the sequential scan it produces on PG 17 and earlier.
        WHEN NEW.expect = 'must_not' AND NEW.idx_used
                                 THEN 'PASS (declined on cost; forceable)'
        WHEN NOT NEW.idx_used    THEN 'PASS (fallback)'
        WHEN NEW.speedup IS NULL THEN 'PASS (indexed)'
        WHEN NEW.speedup >= 1.0  THEN 'PASS (indexed, faster)'
        ELSE                          'PASS (indexed, slower)'
    END;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bt_derive_trg
    BEFORE INSERT OR UPDATE ON bt_result
    FOR EACH ROW EXECUTE FUNCTION bt_derive();


-- ---------------------------------------------------------------------------
--  Timing: fastest of N, after one untimed warm-up. The minimum is the run
--  least polluted by scheduler noise; this suite compares two algorithms, it
--  does not model a latency distribution.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bt_measure(
        p_sql  text,
        p_reps int,
    OUT o_rows bigint,
    OUT o_hash bigint,
    OUT o_ms   numeric)
AS $$
DECLARE
    t0 timestamptz;
    ms numeric;
    r  int;
BEGIN
    EXECUTE p_sql INTO o_rows, o_hash;          -- warm-up, untimed
    o_ms := NULL;
    FOR r IN 1 .. p_reps LOOP
        t0 := clock_timestamp();
        EXECUTE p_sql INTO o_rows, o_hash;
        ms := extract(epoch FROM clock_timestamp() - t0) * 1000.0;
        IF o_ms IS NULL OR ms < o_ms THEN o_ms := ms; END IF;
    END LOOP;
    o_ms := round(o_ms, 3);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION bt_plan(p_sql text) RETURNS text AS $$
DECLARE j json;
BEGIN
    EXECUTE 'EXPLAIN (FORMAT JSON, COSTS OFF) ' || p_sql INTO j;
    RETURN j::text;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION bt_scan_node(p_plan text) RETURNS text AS $$
BEGIN
    RETURN CASE
        WHEN position('"Bitmap Index Scan"' IN p_plan) > 0 THEN 'Bitmap Index Scan'
        WHEN position('"Index Only Scan"'   IN p_plan) > 0 THEN 'Index Only Scan'
        WHEN position('"Index Scan"'        IN p_plan) > 0 THEN 'Index Scan'
        WHEN position('"Seq Scan"'          IN p_plan) > 0 THEN 'Seq Scan'
        ELSE 'other'
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- ---------------------------------------------------------------------------
--  bt_run -- the whole suite in one function.
--
--  p_rel        fixture relation; must have an integer `id` column
--  p_predicate  the WHERE clause under test, as written by a user
--  p_expect     must / must_not / either
--  p_index      index whose use is being asserted; NULL means "any index"
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bt_run(
        p_category  text,
        p_label     text,
        p_rel       text,
        p_predicate text,
        p_expect    text DEFAULT 'either',
        p_index     text DEFAULT NULL,
        p_reps      int  DEFAULT 2)
RETURNS void AS $$
DECLARE
    q         text;
    m         record;
    pl        text;
    seq_node  text;
    seq_rows  bigint; seq_hash bigint; seq_ms numeric;
    natural_  boolean;
    used_     boolean;
BEGIN
    q := format(
        'SELECT count(*), coalesce(sum(hashtext(id::text)::bigint), 0) '
        'FROM %s WHERE %s', p_rel, p_predicate);

    SET LOCAL jit                             = off;
    SET LOCAL max_parallel_workers_per_gather = 0;

    -- ---- arm 1: the oracle. No index path is available at all. ----
    SET LOCAL enable_seqscan    = on;
    SET LOCAL enable_indexscan  = off;
    SET LOCAL enable_bitmapscan = off;
    m := bt_measure(q, p_reps);
    seq_rows := m.o_rows; seq_hash := m.o_hash; seq_ms := m.o_ms;
    seq_node := bt_scan_node(bt_plan(q));

    -- ---- arm 2a: what the planner does unhindered. Recorded, never
    --      asserted on: a cost model is entitled to decline a usable index
    --      on a cheap query, and that is a tuning question, not a bug. ----
    SET LOCAL enable_seqscan    = on;
    SET LOCAL enable_indexscan  = on;
    SET LOCAL enable_bitmapscan = on;
    pl := bt_plan(q);
    natural_ := CASE
        WHEN p_index IS NULL THEN bt_scan_node(pl) <> 'Seq Scan'
        ELSE position('"' || p_index || '"' IN pl) > 0
    END;

    -- ---- arm 2b: force the issue. With sequential scans disabled, the only
    --      reason to still see a Seq Scan is that the access method refused
    --      the clause. This is what makes "is the index used everywhere it
    --      applies" a test of the AM rather than of the cost model. ----
    SET LOCAL enable_seqscan    = off;
    SET LOCAL enable_indexscan  = on;
    SET LOCAL enable_bitmapscan = on;
    pl := bt_plan(q);
    used_ := CASE
        WHEN p_index IS NULL THEN bt_scan_node(pl) <> 'Seq Scan'
        ELSE position('"' || p_index || '"' IN pl) > 0
    END;
    m := bt_measure(q, p_reps);

    INSERT INTO bt_result (category, label, rel, predicate, index_name, expect,
                           seq_rows, seq_hash, seq_ms, seq_node,
                           idx_rows, idx_hash, idx_ms, idx_node,
                           idx_used, idx_natural)
    VALUES (p_category, p_label, p_rel, p_predicate, p_index, p_expect,
            seq_rows, seq_hash, seq_ms, seq_node,
            m.o_rows, m.o_hash, m.o_ms, bt_scan_node(pl),
            used_, natural_);
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------------------
--  Bespoke assertion, for checks that are not "two scans must agree".
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bt_assert(p_ok boolean, p_message text)
RETURNS void AS $$
BEGIN
    IF p_ok IS NOT TRUE THEN
        RAISE EXCEPTION 'ASSERTION FAILED: %', p_message;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------------------
--  End-of-category gate. Prints every failure, then raises if there are any,
--  so one category file cannot pass silently on the back of another's rows.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bt_check(p_category text) RETURNS void AS $$
DECLARE
    n_total int; n_fail int; r record;
BEGIN
    SELECT count(*), count(*) FILTER (WHERE verdict LIKE 'FAIL%')
      INTO n_total, n_fail
      FROM bt_result WHERE category = p_category;

    IF n_total = 0 THEN
        RAISE EXCEPTION 'category % recorded no cases at all', p_category;
    END IF;

    FOR r IN SELECT label, predicate, verdict, seq_rows, idx_rows
               FROM bt_result
              WHERE category = p_category AND verdict LIKE 'FAIL%'
              ORDER BY result_id LOOP
        RAISE WARNING '[%] % | % | seq=% idx=% | %',
              p_category, r.label, r.predicate, r.seq_rows, r.idx_rows, r.verdict;
    END LOOP;

    IF n_fail > 0 THEN
        RAISE EXCEPTION '% : % of % cases FAILED', p_category, n_fail, n_total;
    END IF;

    RAISE NOTICE '% : % cases passed (% chosen by the planner, % forceable, '
                 '% with no index path at all)',
        p_category, n_total,
        (SELECT count(*) FROM bt_result
          WHERE category = p_category AND idx_natural),
        (SELECT count(*) FROM bt_result
          WHERE category = p_category AND idx_used AND NOT coalesce(idx_natural,false)),
        (SELECT count(*) FROM bt_result
          WHERE category = p_category AND NOT idx_used);
END;
$$ LANGUAGE plpgsql;
