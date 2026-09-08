-- =====================================================================
-- test.sql -- regression test for Biscuit regex support (~ !~ ~* !~*)
-- =====================================================================
--
-- Pure SQL / PL/pgSQL. No psql meta-commands, no \echo, \set, \gset or
-- \quit, so this runs unchanged through psql, pgAdmin, JDBC, DBeaver,
-- `docker exec ... psql -f`, a migration runner, or any other client that
-- can send a script.
--
-- The script ASSERTS rather than printing output for a human to compare
-- against an expected file. Every check records a pass/fail row in
-- bt_result; the final statement raises an exception if anything failed,
-- so a non-zero exit / thrown error is the signal, and the summary tables
-- printed before it tell you what broke.
--
-- Everything runs inside a transaction that is rolled back at the end, so
-- the script leaves no tables, functions or data behind. (It deliberately
-- does not COMMIT: re-running it is always safe.)
--
--
-- WHAT IS BEING TESTED, AND HOW
-- -----------------------------
-- The C-level differential test (test/runtests.sh) already proves that
-- every glob the decomposer emits matches exactly the strings its source
-- regex matches, checked over a few thousand strings. This script tests
-- the parts that only exist once the code is inside a running server:
--
--   1. CORRECTNESS. Every regex predicate is evaluated twice -- once with
--      the index available and once with enable_indexscan/bitmapscan off
--      -- and the two row sets must be identical. This is a self-checking
--      oracle: it does not depend on a hand-maintained expected-output
--      file, and it stays valid as the data changes.
--
--   2. ACCELERATION. Decomposable regexes must actually reach the index,
--      and non-decomposable ones must not (biscuit_costestimate() prices
--      them out). Checked by parsing EXPLAIN output.
--
--   3. THE RECHECK BACKSTOP. See the "lossy recheck" section near the end
--      for why this needs a non-constant pattern to reach at all.
--
--   4. OPCLASS GATING. ~ needs the case-sensitive structures and ~* the
--      case-insensitive ones, so biscuit_like_ops registers only
--      strategies 5/6 and biscuit_ilike_ops only 7/8.

BEGIN;

CREATE EXTENSION IF NOT EXISTS biscuit;

-- =====================================================================
-- Harness
-- =====================================================================

CREATE TABLE bt_result (
    seq      serial PRIMARY KEY,
    section  text NOT NULL,
    name     text NOT NULL,
    passed   boolean NOT NULL,
    detail   text
);

CREATE FUNCTION bt_record(p_section text, p_name text,
                          p_passed boolean, p_detail text DEFAULT NULL)
RETURNS void AS $$
    INSERT INTO bt_result (section, name, passed, detail)
    VALUES (p_section, p_name, p_passed, p_detail);
$$ LANGUAGE sql;

-- Row set for `pred` with the index available.
CREATE FUNCTION bt_rows_indexed(p_pred text) RETURNS int[] AS $$
DECLARE r int[];
BEGIN
    PERFORM set_config('enable_seqscan',    'off', true);
    PERFORM set_config('enable_indexscan',  'on',  true);
    PERFORM set_config('enable_bitmapscan', 'on',  true);
    EXECUTE format(
        'SELECT coalesce(array_agg(id ORDER BY id), ''{}''::int[]) '
        'FROM bt_t WHERE %s', p_pred) INTO r;
    RETURN r;
END $$ LANGUAGE plpgsql;

-- Row set for `pred` with every index path forcibly disabled.
CREATE FUNCTION bt_rows_seq(p_pred text) RETURNS int[] AS $$
DECLARE r int[];
BEGIN
    PERFORM set_config('enable_seqscan',    'on',  true);
    PERFORM set_config('enable_indexscan',  'off', true);
    PERFORM set_config('enable_bitmapscan', 'off', true);
    EXECUTE format(
        'SELECT coalesce(array_agg(id ORDER BY id), ''{}''::int[]) '
        'FROM bt_t WHERE %s', p_pred) INTO r;
    PERFORM set_config('enable_seqscan', 'off', true);
    RETURN r;
END $$ LANGUAGE plpgsql;

-- Core correctness check: indexed and non-indexed evaluation must agree.
CREATE FUNCTION bt_check(p_section text, p_pred text) RETURNS void AS $$
DECLARE
    a int[];
    b int[];
BEGIN
    a := bt_rows_indexed(p_pred);
    b := bt_rows_seq(p_pred);
    IF a IS NOT DISTINCT FROM b THEN
        PERFORM bt_record(p_section, p_pred, true,
                          cardinality(a) || ' rows');
    ELSE
        PERFORM bt_record(p_section, p_pred, false,
            format('indexed=%s rows, seq=%s rows; only-in-indexed=%s, only-in-seq=%s',
                   cardinality(a), cardinality(b),
                   (SELECT coalesce(array_agg(x), '{}') FROM unnest(a) x
                     WHERE x <> ALL (b)),
                   (SELECT coalesce(array_agg(x), '{}') FROM unnest(b) x
                     WHERE x <> ALL (a))));
    END IF;
END $$ LANGUAGE plpgsql;

-- EXPLAIN text for a query, flattened to one string.
CREATE FUNCTION bt_plan(p_query text) RETURNS text AS $$
DECLARE
    rec  record;
    out  text := '';
BEGIN
    FOR rec IN EXECUTE 'EXPLAIN (COSTS OFF) ' || p_query LOOP
        out := out || rec."QUERY PLAN" || ' ';
    END LOOP;
    RETURN out;
END $$ LANGUAGE plpgsql;

-- Assert whether a query's plan touches the named index, with seqscan
-- discouraged so the planner uses the index whenever it considers it
-- viable at all.
CREATE FUNCTION bt_check_plan(p_section text, p_query text,
                              p_index text, p_want_index boolean,
                              p_seqscan text DEFAULT 'off',
                              p_bitmapscan text DEFAULT 'on')
RETURNS void AS $$
DECLARE
    plan text;
    used boolean;
BEGIN
    PERFORM set_config('enable_seqscan',    p_seqscan,    true);
    PERFORM set_config('enable_indexscan',  'on',         true);
    PERFORM set_config('enable_bitmapscan', p_bitmapscan, true);
    plan := bt_plan(p_query);
    used := position(p_index in plan) > 0;
    PERFORM bt_record(p_section,
        format('%s [seqscan=%s bitmap=%s, expect index=%s]',
               p_query, p_seqscan, p_bitmapscan, p_want_index),
        used = p_want_index,
        format('index used=%s; plan=%s', used, plan));
END $$ LANGUAGE plpgsql;

-- =====================================================================
-- Fixture
-- =====================================================================

CREATE TABLE bt_t (id serial PRIMARY KEY, v text);

INSERT INTO bt_t (v) VALUES
    ('abc'), ('abcd'), ('xabc'), ('xabcx'), ('ABC'), ('AbCd'), ('aBc'),
    ('usr_1234_x'), ('usr_9999_y'), ('user1234'), ('usr_1234_'),
    (''), (NULL),
    ('a.b'), ('a%b'), ('a_b'), ('a\b'), ('50%'), ('100%'), ('%'), ('_'),
    ('aaa'), ('aab'), ('ab'), ('a'), ('b'), ('aaaa'),
    ('foo bar'), ('foobar'), ('fooxbar'), ('FOOBAR'), ('FooBar'),
    ('éclair'), ('café'), ('naïve'), ('ééé'), ('aéb'),
    ('  leading'), ('trailing  '), ('multi word value');

-- Bulk rows so the planner has a real reason to prefer the index, and so
-- a wrong candidate set is statistically obvious rather than a 1-row diff.
INSERT INTO bt_t (v)
SELECT 'filler_' || i FROM generate_series(1, 20000) i;
INSERT INTO bt_t (v)
SELECT 'usr_' || i || '_z' FROM generate_series(1, 500) i;

-- Exercise the tombstone / slot-reuse paths, since the all-lossy fallback
-- now sources its candidate set from the live non-null set specifically to
-- avoid handing free slots to TID collection.
DELETE FROM bt_t WHERE v LIKE 'filler_1%';
UPDATE bt_t SET v = v || '_upd' WHERE v LIKE 'filler_2%';
INSERT INTO bt_t (v)
SELECT 'recycled_' || i FROM generate_series(1, 300) i;

CREATE INDEX bt_idx ON bt_t USING biscuit (v);
ANALYZE bt_t;

-- =====================================================================
-- 1. Decomposable subset -- correctness
-- =====================================================================

DO $$
DECLARE p text;
BEGIN
    FOREACH p IN ARRAY ARRAY[
        -- anchors and the unanchored default
        $q$v ~ '^abc$'$q$,          -- exact       -> abc
        $q$v ~ '^abc'$q$,           -- prefix      -> abc%
        $q$v ~ 'abc$'$q$,           -- suffix      -> %abc
        $q$v ~ 'abc'$q$,            -- unanchored  -> %abc%
        $q$v ~ '^$'$q$,             -- empty only  -> ''
        $q$v ~ '^'$q$,              -- everything  -> %
        $q$v ~ '$'$q$,              -- everything  -> %
        $q$v ~ ''$q$,               -- everything  -> %
        -- the any-character atom
        $q$v ~ '^a.c$'$q$,          -- .           -> a_c
        $q$v ~ '^a.*d$'$q$,         -- .*          -> a%d
        $q$v ~ '^a.+d$'$q$,         -- .+          -> a_%d
        $q$v ~ '^.*$'$q$,
        $q$v ~ '^.$'$q$,
        $q$v ~ '^..$'$q$,
        $q$v ~ 'a.*b.*c'$q$,
        -- repetition counts
        $q$v ~ '^.{3}$'$q$,         -- .{n}        -> ___
        $q$v ~ '^.{0}$'$q$,
        $q$v ~ '^a.{2,}$'$q$,       -- .{n,}       -> a__%
        $q$v ~ '^a{3}$'$q$,         -- X{n}        -> aaa
        $q$v ~ '^a{1}b$'$q$,
        -- non-greedy markers, which must not change the match set
        $q$v ~ '^a.*?d$'$q$,
        $q$v ~ '^a.+?d$'$q$,
        -- escaped metacharacters
        $q$v ~ '^a\.b$'$q$,         -- escaped .   -> a.b
        $q$v ~ '\.'$q$,
        $q$v ~ '\\'$q$,             -- literal backslash
        $q$v ~ '^\^'$q$,
        $q$v ~ '\$'$q$,
        -- LIKE wildcards appearing as regex literals: these must be
        -- escaped on the way into the glob or they become wildcards
        $q$v ~ '50%'$q$,
        $q$v ~ '^%$'$q$,
        $q$v ~ '^_$'$q$,
        $q$v ~ 'usr_1234'$q$,       -- '_' is a regex literal here
        $q$v ~ '^usr_1234_$'$q$,
        -- multibyte
        $q$v ~ '^é'$q$,
        $q$v ~ 'é'$q$,
        $q$v ~ '^é{3}$'$q$,
        $q$v ~ '^a.b$'$q$,
        -- whitespace
        $q$v ~ '^  '$q$,
        $q$v ~ ' $'$q$,
        $q$v ~ ' word '$q$
    ] LOOP
        PERFORM bt_check('decomposable', p);
    END LOOP;
END $$;

-- =====================================================================
-- 2. Negated and case-insensitive variants
-- =====================================================================

DO $$
DECLARE p text;
BEGIN
    FOREACH p IN ARRAY ARRAY[
        $q$v !~ '^abc$'$q$,
        $q$v !~ 'abc'$q$,
        $q$v !~ '^a.*d$'$q$,
        $q$v !~ '^usr'$q$,
        $q$v !~ '^.{3}$'$q$,
        $q$v ~* '^abc$'$q$,
        $q$v ~* 'abc'$q$,
        $q$v ~* '^foo.*bar$'$q$,
        $q$v ~* '^ABC$'$q$,
        $q$v ~* 'FOO'$q$,
        $q$v ~* '^a.c$'$q$,
        $q$v ~* '^É'$q$,
        $q$v !~* 'abc'$q$,
        $q$v !~* '^foo'$q$
    ] LOOP
        PERFORM bt_check('negated-and-ilike', p);
    END LOOP;
END $$;

-- =====================================================================
-- 3. Conjunctions, including regex mixed with LIKE
-- =====================================================================
--
-- These exercise the multi-key mask threading in biscuit_rescan(): each
-- key narrows the running candidate set that the next one is evaluated
-- against, and the predicate ordering is computed from the REWRITTEN glob.

DO $$
DECLARE p text;
BEGIN
    FOREACH p IN ARRAY ARRAY[
        $q$v ~ '^a' AND v ~ 'c$'$q$,
        $q$v ~ '^usr' AND v ~ '1234'$q$,
        $q$v ~ '^usr' AND v LIKE '%1234%'$q$,
        $q$v LIKE 'usr%' AND v ~ '_1234_'$q$,
        $q$v ~ 'a' AND v !~ 'b'$q$,
        $q$v ~ '^a' AND v !~ '^ab'$q$,
        $q$v ~* '^foo' AND v ~* 'bar$'$q$,
        $q$v ~ '^filler' AND v ~ '9$'$q$,
        $q$v ~ '^.{3}$' AND v ~ 'a'$q$,
        $q$v ~ '^usr' AND v ~ '^usr_1' AND v ~ '_z$'$q$
    ] LOOP
        PERFORM bt_check('conjunction', p);
    END LOOP;
END $$;

-- =====================================================================
-- 4. Outside the decomposable subset -- must stay CORRECT
-- =====================================================================
--
-- These are not accelerated. The point of the check is that they return
-- exactly the right rows anyway.

DO $$
DECLARE p text;
BEGIN
    FOREACH p IN ARRAY ARRAY[
        $q$v ~ '[abc]'$q$,              -- bracket expression
        $q$v ~ '^[a-z]+$'$q$,
        $q$v ~ '^(foo|bar)'$q$,         -- alternation + group
        $q$v ~ 'foo|bar'$q$,
        $q$v ~ 'a*b'$q$,                -- unbounded literal repetition
        $q$v ~ '^a?b'$q$,               -- optional literal
        $q$v ~ '^a{1,3}$'$q$,           -- bounded range
        $q$v ~ '^.{1,3}$'$q$,
        $q$v ~ '.?'$q$,                 -- optional any-char
        $q$v ~ '\d'$q$,                 -- class shorthand
        $q$v ~ '\w+'$q$,
        $q$v ~ '^\s'$q$,
        $q$v ~* '[0-9]+'$q$,
        $q$v !~ '[abc]'$q$,             -- negated AND lossy
        $q$v !~* '(foo|bar)'$q$,
        $q$v ~ '[abc]' AND v ~ '^a'$q$, -- lossy AND exact together
        $q$v ~ '[abc]' AND v LIKE 'a%'$q$
    ] LOOP
        PERFORM bt_check('non-decomposable', p);
    END LOOP;
END $$;

-- =====================================================================
-- 5. Plan shape
-- =====================================================================
--
-- Decomposable regexes should reach the index; non-decomposable ones
-- should be priced out of it by biscuit_costestimate(). Both are checked
-- with enable_seqscan off, so "did not use the index" means the planner
-- actively rejected it rather than merely preferring a seqscan.

DO $$
DECLARE q text;
BEGIN
    FOREACH q IN ARRAY ARRAY[
        $q$SELECT count(*) FROM bt_t WHERE v ~ '^usr_1234'$q$,
        $q$SELECT count(*) FROM bt_t WHERE v ~ '^abc$'$q$,
        $q$SELECT count(*) FROM bt_t WHERE v ~ 'abcd'$q$,
        $q$SELECT count(*) FROM bt_t WHERE v ~ '^filler_9.*7$'$q$,
        $q$SELECT count(*) FROM bt_t WHERE v ~* '^FOOBAR$'$q$
    ] LOOP
        PERFORM bt_check_plan('plan-accelerated', q, 'bt_idx', true);
    END LOOP;

    FOREACH q IN ARRAY ARRAY[
        $q$SELECT count(*) FROM bt_t WHERE v ~ '^(foo|bar)'$q$,
        $q$SELECT count(*) FROM bt_t WHERE v ~ '[0-9]+'$q$,
        $q$SELECT count(*) FROM bt_t WHERE v ~ '^a{1,3}$'$q$,
        $q$SELECT count(*) FROM bt_t WHERE v ~ '\d'$q$
    ] LOOP
        /*
         * (a) THE PRODUCTION CONTRACT: given a sequential scan as an
         * alternative, the planner must not choose Biscuit for these.
         * That is what BISCUIT_COST_DISABLED buys, and it is a real
         * discrimination rather than a tautology -- the decomposable
         * patterns in the loop above still reach the index with the very
         * same settings.
         */
        PERFORM bt_check_plan('plan-priced-out', q, 'bt_idx', false, 'on', 'on');

        /*
         * (b) THE ACCESS-METHOD CONTRACT: biscuit_costestimate() must
         * disable its OWN index path, so the index loses even when the
         * seqscan has been switched off.
         *
         * enable_bitmapscan is off here, and that is not a way of dodging
         * an inconvenient plan -- it is the boundary of what an access
         * method is able to say. From PG18 (commit e22253467) path choice
         * compares disabled_nodes before cost, and biscuit_costestimate()
         * can raise that counter on the IndexPath it is handed, which is
         * what makes this check meaningful. It cannot raise it on the
         * BitmapHeapPath wrapped around that path: cost_bitmap_heap_scan()
         * derives disabled_nodes from enable_bitmapscan alone and never
         * inherits it from its bitmapqual. So with seqscan off AND bitmap
         * on there is simply no non-disabled alternative left for the
         * planner to pick, and the bitmap path wins on a technicality of
         * core's bookkeeping rather than on anything Biscuit costed. That
         * combination tests core, not us, which is why it is not asserted.
         *
         * Before the PG18 fix this check failed: the index path carried
         * disabled_nodes = 0 against the seqscan's 1 and won outright,
         * 1e18 cost notwithstanding.
         */
        PERFORM bt_check_plan('plan-priced-out', q, 'bt_idx', false, 'off', 'off');
    END LOOP;
END $$;

-- =====================================================================
-- 6. The lossy recheck backstop
-- =====================================================================
--
-- Reaching this path takes some care. When the pattern is a plan-time
-- constant, biscuit_costestimate() sees the non-decomposable regex and
-- disables the path outright, so the executor's lossy branch never runs --
-- which is exactly the intended behaviour, but it means sections 4 and 5
-- above never actually exercise the skip-and-recheck code.
--
-- A pattern the planner cannot fold to a Const takes the other branch in
-- biscuit_pattern_from_clause() ("not a plan-time constant" -> continue,
-- path NOT disabled). The index may then be chosen, the regex is only seen
-- at execution time, and biscuit_build_query_plan() marks the key lossy:
-- the key is skipped, the candidate set stays a superset, xs_recheck is
-- set, and the executor re-evaluates the original regex.
--
-- If this section regresses while section 4 still passes, the bug is in
-- the recheck plumbing (BiscuitScanOpaque.needs_recheck ->
-- scan->xs_recheck / tbm_add_tuples) rather than in the decomposer.

-- LANGUAGE plpgsql, not sql, and deliberately so: the planner INLINES a
-- simple SQL function like `SELECT p`, which would fold this straight back
-- into a Const and hand biscuit_pattern_from_clause() exactly the plan-time
-- constant this section is trying to avoid. A plpgsql function is never
-- inlined. STABLE (not IMMUTABLE) additionally keeps it out of reach of
-- constant folding, while still being usable in an index qual.
CREATE FUNCTION bt_pat(p text) RETURNS text AS $$
BEGIN
    RETURN p;
END $$ LANGUAGE plpgsql STABLE;

DO $$
DECLARE
    a int[];
    b int[];
    plan text;
    p    text;
    pats text[] := ARRAY['[abc]', '^(foo|bar)', '\d', 'a*b', '^a{1,3}$'];
BEGIN
    PERFORM set_config('enable_seqscan', 'off', true);

    FOREACH p IN ARRAY pats LOOP
        -- Indexed evaluation, pattern opaque to the planner.
        PERFORM set_config('enable_indexscan',  'on', true);
        PERFORM set_config('enable_bitmapscan', 'on', true);
        EXECUTE 'SELECT coalesce(array_agg(id ORDER BY id), ''{}''::int[]) '
                'FROM bt_t WHERE v ~ bt_pat($1)'
            INTO a USING p;

        -- Ground truth.
        PERFORM set_config('enable_indexscan',  'off', true);
        PERFORM set_config('enable_bitmapscan', 'off', true);
        PERFORM set_config('enable_seqscan',    'on',  true);
        EXECUTE 'SELECT coalesce(array_agg(id ORDER BY id), ''{}''::int[]) '
                'FROM bt_t WHERE v ~ $1'
            INTO b USING p;
        PERFORM set_config('enable_seqscan', 'off', true);

        PERFORM bt_record('lossy-recheck',
            format('v ~ bt_pat(%L)', p),
            a IS NOT DISTINCT FROM b,
            format('indexed=%s rows, seq=%s rows', cardinality(a), cardinality(b)));
    END LOOP;

    -- Same idea for a decomposable pattern that is nonetheless opaque at
    -- plan time: this one goes down the EXACT branch at execution time, so
    -- it must NOT set recheck, and must still be correct.
    PERFORM set_config('enable_indexscan',  'on', true);
    PERFORM set_config('enable_bitmapscan', 'on', true);
    EXECUTE 'SELECT coalesce(array_agg(id ORDER BY id), ''{}''::int[]) '
            'FROM bt_t WHERE v ~ bt_pat(''^usr_1234'')' INTO a;
    PERFORM set_config('enable_indexscan',  'off', true);
    PERFORM set_config('enable_bitmapscan', 'off', true);
    PERFORM set_config('enable_seqscan',    'on',  true);
    EXECUTE 'SELECT coalesce(array_agg(id ORDER BY id), ''{}''::int[]) '
            'FROM bt_t WHERE v ~ ''^usr_1234''' INTO b;
    PERFORM set_config('enable_seqscan', 'off', true);

    PERFORM bt_record('lossy-recheck', 'opaque but decomposable pattern',
        a IS NOT DISTINCT FROM b,
        format('indexed=%s rows, seq=%s rows', cardinality(a), cardinality(b)));

    -- Force the bitmap-scan path specifically, so the recheck flag is
    -- exercised through tbm_add_tuples() rather than xs_recheck.
    PERFORM set_config('enable_indexscan',  'off', true);
    PERFORM set_config('enable_bitmapscan', 'on',  true);
    plan := bt_plan('SELECT count(*) FROM bt_t WHERE v ~ bt_pat(''[abc]'')');
    EXECUTE 'SELECT coalesce(array_agg(id ORDER BY id), ''{}''::int[]) '
            'FROM bt_t WHERE v ~ bt_pat(''[abc]'')' INTO a;
    PERFORM set_config('enable_bitmapscan', 'off', true);
    PERFORM set_config('enable_seqscan',    'on',  true);
    EXECUTE 'SELECT coalesce(array_agg(id ORDER BY id), ''{}''::int[]) '
            'FROM bt_t WHERE v ~ ''[abc]''' INTO b;

    PERFORM bt_record('lossy-recheck', 'bitmap scan path, lossy pattern',
        a IS NOT DISTINCT FROM b,
        format('indexed=%s rows, seq=%s rows; plan=%s',
               cardinality(a), cardinality(b), plan));
END $$;

-- =====================================================================
-- 7. Opclass gating
-- =====================================================================
--
-- biscuit_like_ops builds only the case-sensitive structures, so it
-- registers strategies 5/6 (~ and !~) and not 7/8; biscuit_ilike_ops is
-- the mirror image. An operator with no strategy in the opfamily is not
-- merely expensive -- the planner never considers the index at all.

CREATE TABLE bt_like (id serial PRIMARY KEY, v text);
INSERT INTO bt_like (v)
SELECT 'row_' || i FROM generate_series(1, 20000) i;
CREATE INDEX bt_like_idx ON bt_like USING biscuit (v biscuit_like_ops);

CREATE TABLE bt_ilike (id serial PRIMARY KEY, v text);
INSERT INTO bt_ilike (v)
SELECT 'row_' || i FROM generate_series(1, 20000) i;
CREATE INDEX bt_ilike_idx ON bt_ilike USING biscuit (v biscuit_ilike_ops);

ANALYZE bt_like;
ANALYZE bt_ilike;

DO $$
DECLARE
    plan text;
BEGIN
    PERFORM set_config('enable_seqscan',    'off', true);
    PERFORM set_config('enable_indexscan',  'on',  true);
    PERFORM set_config('enable_bitmapscan', 'on',  true);

    plan := bt_plan($q$SELECT count(*) FROM bt_like WHERE v ~ '^row_1'$q$);
    PERFORM bt_record('opclass-gating', 'biscuit_like_ops serves ~',
        position('bt_like_idx' in plan) > 0, plan);

    plan := bt_plan($q$SELECT count(*) FROM bt_like WHERE v ~* '^row_1'$q$);
    PERFORM bt_record('opclass-gating', 'biscuit_like_ops rejects ~*',
        position('bt_like_idx' in plan) = 0, plan);

    plan := bt_plan($q$SELECT count(*) FROM bt_ilike WHERE v ~* '^row_1'$q$);
    PERFORM bt_record('opclass-gating', 'biscuit_ilike_ops serves ~*',
        position('bt_ilike_idx' in plan) > 0, plan);

    plan := bt_plan($q$SELECT count(*) FROM bt_ilike WHERE v ~ '^row_1'$q$);
    PERFORM bt_record('opclass-gating', 'biscuit_ilike_ops rejects ~',
        position('bt_ilike_idx' in plan) = 0, plan);
END $$;

-- Catalog-level view of the same thing.
DO $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n
      FROM pg_amop amop
      JOIN pg_opfamily opf ON opf.oid = amop.amopfamily
      JOIN pg_am am        ON am.oid = opf.opfmethod
     WHERE am.amname = 'biscuit'
       AND opf.opfname = 'biscuit_ops'
       AND amop.amopstrategy BETWEEN 5 AND 8;
    PERFORM bt_record('opclass-gating',
        'biscuit_ops registers all four regex strategies', n = 4,
        'found ' || n);

    SELECT count(*) INTO n
      FROM pg_amop amop
      JOIN pg_opfamily opf ON opf.oid = amop.amopfamily
      JOIN pg_am am        ON am.oid = opf.opfmethod
     WHERE am.amname = 'biscuit'
       AND opf.opfname = 'biscuit_like_ops'
       AND amop.amopstrategy IN (7, 8);
    PERFORM bt_record('opclass-gating',
        'biscuit_like_ops omits the case-insensitive regex strategies',
        n = 0, 'found ' || n);

    SELECT count(*) INTO n
      FROM pg_amop amop
      JOIN pg_opfamily opf ON opf.oid = amop.amopfamily
      JOIN pg_am am        ON am.oid = opf.opfmethod
     WHERE am.amname = 'biscuit'
       AND opf.opfname = 'biscuit_ilike_ops'
       AND amop.amopstrategy IN (5, 6);
    PERFORM bt_record('opclass-gating',
        'biscuit_ilike_ops omits the case-sensitive regex strategies',
        n = 0, 'found ' || n);
END $$;

-- =====================================================================
-- 8. Multi-column
-- =====================================================================
--
-- Exercises biscuit_build_candidates_multicolumn(), which has its own
-- copy of the strategy dispatch and its own all-keys-lossy fallback.

CREATE TABLE bt_mc (id serial PRIMARY KEY, a text, b text);
INSERT INTO bt_mc (a, b)
SELECT 'left_' || i, 'right_' || (i % 97) FROM generate_series(1, 20000) i;
INSERT INTO bt_mc (a, b) VALUES ('abc', 'xyz'), ('abcd', 'xy'), (NULL, 'q'), ('r', NULL);
CREATE INDEX bt_mc_idx ON bt_mc USING biscuit (a, b);
ANALYZE bt_mc;

CREATE FUNCTION bt_check_mc(p_pred text) RETURNS void AS $$
DECLARE a int[]; b int[];
BEGIN
    PERFORM set_config('enable_seqscan',    'off', true);
    PERFORM set_config('enable_indexscan',  'on',  true);
    PERFORM set_config('enable_bitmapscan', 'on',  true);
    EXECUTE format('SELECT coalesce(array_agg(id ORDER BY id), ''{}''::int[]) '
                   'FROM bt_mc WHERE %s', p_pred) INTO a;

    PERFORM set_config('enable_seqscan',    'on',  true);
    PERFORM set_config('enable_indexscan',  'off', true);
    PERFORM set_config('enable_bitmapscan', 'off', true);
    EXECUTE format('SELECT coalesce(array_agg(id ORDER BY id), ''{}''::int[]) '
                   'FROM bt_mc WHERE %s', p_pred) INTO b;

    PERFORM bt_record('multicolumn', p_pred, a IS NOT DISTINCT FROM b,
        format('indexed=%s rows, seq=%s rows', cardinality(a), cardinality(b)));
END $$ LANGUAGE plpgsql;

DO $$
DECLARE p text;
BEGIN
    FOREACH p IN ARRAY ARRAY[
        $q$a ~ '^abc'$q$,
        $q$a ~ '^abc$' AND b ~ '^xyz$'$q$,
        $q$a ~ '^left_1' AND b ~ '5$'$q$,
        $q$a ~ '^left_1' AND b LIKE '%5'$q$,
        $q$a ~ '^left' AND b !~ '^right_1$'$q$,
        $q$a ~* '^LEFT_9' AND b ~* '^RIGHT'$q$,
        $q$a ~ '[abc]' AND b ~ '^right_1$'$q$,   -- lossy + exact
        $q$a ~ '[abc]'$q$,                        -- all keys lossy
        $q$a ~ '\d' AND b ~ '\d'$q$               -- all keys lossy, two columns
    ] LOOP
        PERFORM bt_check_mc(p);
    END LOOP;
END $$;

-- =====================================================================
-- 9. Results
-- =====================================================================

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

SELECT section,
       count(*)                            AS checks,
       count(*) FILTER (WHERE passed)      AS passed,
       count(*) FILTER (WHERE NOT passed)  AS failed
  FROM bt_result
 GROUP BY section
 ORDER BY section;

SELECT seq, section, name, detail
  FROM bt_result
 WHERE NOT passed
 ORDER BY seq;

SELECT count(*) AS total_checks,
       count(*) FILTER (WHERE passed)     AS total_passed,
       count(*) FILTER (WHERE NOT passed) AS total_failed
  FROM bt_result;

-- Fail loudly so a script runner, CI job or migration tool notices.
DO $$
DECLARE
    n_fail int;
    n_all  int;
    first  text;
BEGIN
    SELECT count(*) FILTER (WHERE NOT passed), count(*)
      INTO n_fail, n_all FROM bt_result;

    IF n_all = 0 THEN
        RAISE EXCEPTION 'biscuit regex tests: no checks ran at all';
    END IF;

    IF n_fail > 0 THEN
        SELECT section || ' / ' || name || ' -- ' || coalesce(detail, '')
          INTO first FROM bt_result WHERE NOT passed ORDER BY seq LIMIT 1;
        RAISE EXCEPTION 'biscuit regex tests: % of % checks FAILED; first: %',
            n_fail, n_all, first;
    END IF;

    RAISE NOTICE 'biscuit regex tests: all % checks passed', n_all;
END $$;

-- Leave no trace. Every object above was created inside this transaction.
ROLLBACK;
