-- =====================================================================
-- stress.sql -- adversarial testing of Biscuit's regex mode
-- =====================================================================
--
-- test.sql covers the happy paths with hand-picked patterns. This file
-- goes after the things that file cannot reach: randomised patterns, scan
-- reuse, plan caching, and data shapes chosen to break the decomposer's
-- assumptions.
--
-- Same oracle throughout: the index result must equal the sequential-scan
-- result exactly. Pure SQL/PL-pgSQL, self-asserting, rolled back at the end.

BEGIN;
CREATE EXTENSION IF NOT EXISTS biscuit;

CREATE TABLE st_result (
    seq serial PRIMARY KEY, section text, name text,
    passed boolean, detail text);

CREATE FUNCTION st_rec(s text, n text, p boolean, d text DEFAULT NULL)
RETURNS void AS $$
    INSERT INTO st_result(section,name,passed,detail) VALUES (s,n,p,d);
$$ LANGUAGE sql;

-- =====================================================================
-- Fixture: deliberately hostile data
-- =====================================================================
--
-- Short strings over an alphabet that includes every character with
-- special meaning to either the regex parser or the LIKE parser, so that
-- escaping mistakes in the rewrite surface as wrong rows rather than
-- staying invisible behind well-behaved test data.

CREATE TABLE st_t (id serial PRIMARY KEY, v text);

INSERT INTO st_t (v)
SELECT string_agg(c, '')
FROM (
    SELECT i, (ARRAY['a','b','c','%','_','\','.','é',' ','A'])[1 + ((i / p.d) % 10)] AS c
    FROM generate_series(1, 6000) i,
         (VALUES (1),(10),(100),(1000)) AS p(d)
) s GROUP BY i;

-- Plus explicit edge cases the generator above will not produce.
INSERT INTO st_t (v) VALUES
    (NULL), (''), (' '), ('%'), ('_'), ('\'), ('\\'), ('%%'), ('__'),
    ('\%'), ('\_'), ('a%b'), ('a_b'), ('a\b'), ('.'), ('..'), ('...'),
    ('é'), ('ééé'), ('aéb'), ('日本語'), ('a日b'), ('ß'), ('İ'), ('ǅ'),
    (repeat('a', 300)), (repeat('ab', 200)), (repeat('é', 150)),
    ('A'), ('a'), ('AbC'), ('abc'), ('ABC');

-- Churn, so the index carries tombstones and recycled slots rather than a
-- clean append-only layout.
DELETE FROM st_t WHERE id % 17 = 0;
UPDATE st_t SET v = v || 'z' WHERE id % 23 = 0;
INSERT INTO st_t (v) SELECT 'post_' || i FROM generate_series(1,500) i;

CREATE INDEX st_idx ON st_t USING biscuit (v);
ANALYZE st_t;

-- Compare index vs seqscan for one predicate.
-- Row-set fingerprint rather than the full id array.
--
-- count alone would miss a swap; count+sum would miss a compensating pair.
-- Adding sum(id*id) makes an undetected difference require two independent
-- coincidences, which no bitmap bug produces in practice -- and it keeps
-- the fuzz loop from materialising thousands of multi-thousand-element
-- arrays inside one transaction, which is enough to exhaust a small box.
CREATE FUNCTION st_fp(p_pred text) RETURNS text AS $$
DECLARE r text;
BEGIN
    EXECUTE format(
        'SELECT count(*)||''/''||coalesce(sum(id::bigint),0)||''/''
                ||coalesce(sum(id::bigint*id::bigint),0)
           FROM st_t WHERE %s', p_pred) INTO r;
    RETURN r;
END $$ LANGUAGE plpgsql;

CREATE FUNCTION st_cmp(p_section text, p_pred text, p_label text DEFAULT NULL)
RETURNS boolean AS $$
DECLARE a text; b text; ok boolean;
BEGIN
    PERFORM set_config('enable_seqscan','off',true);
    PERFORM set_config('enable_indexscan','on',true);
    PERFORM set_config('enable_bitmapscan','on',true);
    a := st_fp(p_pred);

    PERFORM set_config('enable_seqscan','on',true);
    PERFORM set_config('enable_indexscan','off',true);
    PERFORM set_config('enable_bitmapscan','off',true);
    b := st_fp(p_pred);

    ok := a IS NOT DISTINCT FROM b;
    IF NOT ok THEN
        PERFORM st_rec(p_section, coalesce(p_label,p_pred), false,
            format('indexed=%s  seqscan=%s  (count/sum/sumsq)', a, b));
    END IF;
    RETURN ok;
END $$ LANGUAGE plpgsql;

-- =====================================================================
-- A. Randomised pattern fuzz
-- =====================================================================
--
-- Builds patterns from the full grammar -- decomposable atoms mixed with
-- constructs that must be rejected -- so the decomposer's accept/reject
-- boundary is probed from both sides, in all four case/negation modes.

DO $do$
DECLARE
    atoms  text[] := ARRAY['a','b','c','.','\.','\%','\_','\\','é','A',' ',
                           '[abc]','(a|b)','\d','\w','%','_'];
    quants text[] := ARRAY['','','','','*','+','?','{2}','{0}','{1,3}','{2,}','*?','+?'];
    pre    text[] := ARRAY['','','^'];
    suf    text[] := ARRAY['','','$'];
    ops    text[] := ARRAY['~','!~','~*','!~*'];
    body   text; rx text; op text; pred text;
    i int; j int; nat int;
    nfail int := 0; ntot int := 0;
BEGIN
    PERFORM setseed(0.4242);
    FOR i IN 1..900 LOOP
        body := '';
        nat  := 1 + floor(random()*4)::int;
        FOR j IN 1..nat LOOP
            body := body
                 || atoms[1+floor(random()*array_length(atoms,1))::int]
                 || quants[1+floor(random()*array_length(quants,1))::int];
        END LOOP;
        rx := pre[1+floor(random()*3)::int] || body || suf[1+floor(random()*3)::int];
        op := ops[1+floor(random()*4)::int];

        -- Skip patterns PostgreSQL itself rejects; we are testing the
        -- rewrite, not regex validity.
        BEGIN
            PERFORM ''  ~ rx;
        EXCEPTION WHEN others THEN CONTINUE;
        END;

        pred := format('v %s %L', op, rx);
        ntot := ntot + 1;
        IF NOT st_cmp('fuzz-random', pred) THEN nfail := nfail + 1; END IF;
    END LOOP;
    PERFORM st_rec('fuzz-random', format('%s randomised patterns', ntot),
                   nfail = 0, format('%s mismatches', nfail));
END $do$;

-- =====================================================================
-- B. Scan reuse: nested-loop rescan alternating lossy and exact
-- =====================================================================
--
-- THE key risk in the regex change. BiscuitScanOpaque.needs_recheck is
-- per-scan state, but one scan node is rescanned once per outer row. If
-- the flag latched on, an exact pattern following a lossy one would ask
-- for a needless recheck (slow but correct); if it failed to SET on a
-- later rescan, a lossy pattern following an exact one would return a
-- superset with NO recheck -- extra rows, silently.
--
-- The join below drives exactly that alternation through a single inner
-- scan, then compares against the same join with the index disabled.

CREATE TABLE st_pat (id serial PRIMARY KEY, rx text, lossy boolean);
INSERT INTO st_pat (rx, lossy) VALUES
    ('^a',      false), ('[abc]',   true),
    ('^ab',     false), ('(a|b)',   true),
    ('b$',      false), ('\d',      true),
    ('^a.c$',   false), ('a*b',     true),
    ('^post_1', false), ('^a{1,3}$',true),
    ('^é',      false), ('[0-9]+',  true);

CREATE FUNCTION st_join(use_index boolean) RETURNS text AS $$
DECLARE r text;
BEGIN
    PERFORM set_config('enable_seqscan',    CASE WHEN use_index THEN 'off' ELSE 'on'  END, true);
    PERFORM set_config('enable_indexscan',  CASE WHEN use_index THEN 'on'  ELSE 'off' END, true);
    PERFORM set_config('enable_bitmapscan', CASE WHEN use_index THEN 'on'  ELSE 'off' END, true);
    PERFORM set_config('enable_material',   'off', true);
    PERFORM set_config('enable_hashjoin',   'off', true);
    PERFORM set_config('enable_mergejoin',  'off', true);
    SELECT string_agg(p.id || ':' || c, ',' ORDER BY p.id) INTO r
      FROM st_pat p
      CROSS JOIN LATERAL (SELECT count(*) AS c FROM st_t t WHERE t.v ~ p.rx) x;
    RETURN r;
END $$ LANGUAGE plpgsql;

DO $do$
DECLARE a text; b text;
BEGIN
    a := st_join(true);
    b := st_join(false);
    PERFORM st_rec('rescan-latch', 'nested-loop alternating lossy/exact',
                   a IS NOT DISTINCT FROM b,
                   format('indexed=%s  seq=%s', a, b));
END $do$;

-- Reverse the order so the FIRST rescan is lossy and later ones exact.
DO $do$
DECLARE a text; b text;
BEGIN
    UPDATE st_pat SET id = -id;
    a := st_join(true);
    b := st_join(false);
    PERFORM st_rec('rescan-latch', 'nested-loop, lossy first',
                   a IS NOT DISTINCT FROM b, format('indexed=%s seq=%s', a, b));
    UPDATE st_pat SET id = -id;
END $do$;

-- =====================================================================
-- C. Plan caching: prepared statements and generic plans
-- =====================================================================
--
-- After five executions PostgreSQL may switch to a generic plan, where the
-- pattern is a Param rather than a Const. biscuit_pattern_from_clause()
-- then takes its "not a plan-time constant" branch, so the path is NOT
-- cost-disabled and the executor sees the regex for the first time at scan
-- time -- the same route section 6 of test.sql uses, but reached through
-- the plan cache instead. Alternating lossy and exact parameters across
-- executions of one prepared statement re-tests the latch from C's angle.

DO $do$
DECLARE
    pats text[] := ARRAY['^a','[abc]','^ab','(a|b)','b$','\d','^a.c$','a*b',
                         '^post_1','[0-9]+','^é','\w'];
    p text; a bigint; b bigint; bad int := 0; i int;
BEGIN
    PREPARE st_ps(text) AS SELECT count(*) FROM st_t WHERE v ~ $1;
    FOR i IN 1..3 LOOP
      FOREACH p IN ARRAY pats LOOP
        PERFORM set_config('enable_seqscan','off',true);
        PERFORM set_config('enable_indexscan','on',true);
        PERFORM set_config('enable_bitmapscan','on',true);
        -- format(), not USING: inside PL/pgSQL the $1 of an outer EXECUTE
        -- is consumed by PL/pgSQL itself, so it never reaches the prepared
        -- statement's own parameter slot.
        EXECUTE format('EXECUTE st_ps(%L)', p) INTO a;

        PERFORM set_config('enable_seqscan','on',true);
        PERFORM set_config('enable_indexscan','off',true);
        PERFORM set_config('enable_bitmapscan','off',true);
        EXECUTE 'SELECT count(*) FROM st_t WHERE v ~ $1' INTO b USING p;

        IF a IS DISTINCT FROM b THEN bad := bad + 1; END IF;
      END LOOP;
    END LOOP;
    DEALLOCATE st_ps;
    PERFORM st_rec('plan-cache', 'prepared stmt, 36 executions, mixed lossy/exact',
                   bad = 0, format('%s mismatches', bad));
END $do$;

-- =====================================================================
-- D. Pathological pattern shapes
-- =====================================================================

DO $do$
DECLARE p text; bad int := 0; n int := 0;
BEGIN
    FOREACH p IN ARRAY ARRAY[
        -- repetition bounds at and around the guard
        -- PostgreSQL's own DUPMAX is 255, so anything above that is
        -- rejected by the server before Biscuit sees it (verified: the
        -- error is identical with and without the index). These probe the
        -- boundary from below, where the decomposer is actually reachable.
        $q$v ~ '^.{254}$'$q$, $q$v ~ '^.{255}$'$q$, $q$v ~ '^a{255}$'$q$,
        $q$v ~ '^.{0}$'$q$,   $q$v ~ '^a{0}$'$q$, $q$v ~ '^.{1}$'$q$,
        $q$v ~ '^.{200}.{55}$'$q$,
        -- anchors in odd places
        $q$v ~ '^^a'$q$, $q$v ~ 'a$$'$q$, $q$v ~ '^$'$q$, $q$v ~ '$^'$q$,
        $q$v ~ '\^'$q$,  $q$v ~ '\$'$q$,  $q$v ~ '^\^\$$'$q$,
        -- backslash edge cases
        $q$v ~ '\\'$q$, $q$v ~ '\\\\'$q$, $q$v ~ '\\a'$q$, $q$v ~ 'a\\'$q$,
        $q$v ~ '\%'$q$, $q$v ~ '\_'$q$, $q$v ~ '\.'$q$, $q$v ~ '\\%'$q$,
        -- LIKE metacharacters as regex literals
        $q$v ~ '%'$q$, $q$v ~ '_'$q$, $q$v ~ '%%'$q$, $q$v ~ '__'$q$,
        $q$v ~ '^%$'$q$, $q$v ~ '^_$'$q$, $q$v ~ '%_%'$q$,
        -- wildcards and collapsing
        $q$v ~ '.*.*.*'$q$, $q$v ~ '^.*.*$'$q$, $q$v ~ '.+.+'$q$,
        $q$v ~ '^.*a.*$'$q$, $q$v ~ '.{2,}.{2,}'$q$,
        -- multibyte with quantifiers
        $q$v ~ '^é{3}$'$q$, $q$v ~ '^é.é$'$q$, $q$v ~ '日{1}'$q$,
        $q$v ~ '^.{3}$'$q$,
        -- long literals
        $q$v ~ '^a{255}$'$q$, $q$v ~ 'aaaaaaaaaaaaaaaaaaaa'$q$,
        -- empty and near-empty
        $q$v ~ ''$q$, $q$v !~ ''$q$, $q$v ~* ''$q$,
        -- non-greedy
        $q$v ~ '^a.*?b$'$q$, $q$v ~ '^.+?$'$q$, $q$v ~ '.{2,}?'$q$
    ] LOOP
        n := n + 1;
        IF NOT st_cmp('pathological', p) THEN bad := bad + 1; END IF;
    END LOOP;
    PERFORM st_rec('pathological', format('%s pathological shapes', n),
                   bad = 0, format('%s mismatches', bad));
END $do$;

-- =====================================================================
-- E. Case-insensitive fuzz
-- =====================================================================
--
-- ~* maps to ILIKE, which folds with lower(). Characters whose case
-- folding is not length-preserving or not round-tripping (ß, İ, ǅ) are the
-- ones most likely to expose a mismatch between regex case-insensitivity
-- and the index's lowercased structures.

DO $do$
DECLARE p text; bad int := 0; n int := 0;
BEGIN
    FOREACH p IN ARRAY ARRAY[
        $q$v ~* '^abc$'$q$, $q$v ~* '^ABC$'$q$, $q$v ~* 'AbC'$q$,
        $q$v ~* '^a'$q$,    $q$v ~* '^A'$q$,    $q$v ~* 'É'$q$,
        $q$v ~* '^é$'$q$,   $q$v ~* 'ß'$q$,     $q$v ~* 'İ'$q$,
        $q$v ~* 'ǅ'$q$,     $q$v ~* '^.{3}$'$q$,$q$v ~* '\%'$q$,
        $q$v !~* '^a'$q$,   $q$v !~* 'ABC'$q$
    ] LOOP
        n := n + 1;
        IF NOT st_cmp('case-fold', p) THEN bad := bad + 1; END IF;
    END LOOP;
    PERFORM st_rec('case-fold', format('%s case-folding patterns', n),
                   bad = 0, format('%s mismatches', bad));
END $do$;

-- =====================================================================
-- F. Conjunctions of many keys, mixed lossy/exact
-- =====================================================================

DO $do$
DECLARE p text; bad int := 0; n int := 0;
BEGIN
    FOREACH p IN ARRAY ARRAY[
        $q$v ~ '^a' AND v ~ '[bc]'$q$,
        $q$v ~ '[abc]' AND v ~ '\d'$q$,
        $q$v ~ '^a' AND v ~ 'b' AND v ~ 'c$'$q$,
        $q$v ~ '[abc]' AND v ~ '(a|b)' AND v ~ '\w'$q$,
        $q$v ~ '^a' AND v !~ '[bc]'$q$,
        $q$v !~ '[abc]' AND v !~ '\d'$q$,
        $q$v ~ '^a' AND v LIKE '%b%' AND v ~ '\d'$q$,
        $q$v ~* '^A' AND v ~ '[abc]'$q$,
        $q$v ~ '^.{3}$' AND v ~ '[abc]'$q$
    ] LOOP
        n := n + 1;
        IF NOT st_cmp('conjunction-mixed', p) THEN bad := bad + 1; END IF;
    END LOOP;
    PERFORM st_rec('conjunction-mixed', format('%s mixed conjunctions', n),
                   bad = 0, format('%s mismatches', bad));
END $do$;

-- =====================================================================
-- G. Concurrent-ish churn: query with rows pending in the log
-- =====================================================================
--
-- Biscuit buffers modifications in a pending log that is drained on read.
-- A regex scan must see the same rows as a seqscan even when the log is
-- non-empty at scan time, and the lossy path's synthesised all-rows set
-- must not include undrained or recycled slots.

DO $do$
DECLARE bad int := 0;
BEGIN
    INSERT INTO st_t (v) SELECT 'churn_' || i FROM generate_series(1,400) i;
    IF NOT st_cmp('pending-log','v ~ ''^churn_1''')       THEN bad := bad+1; END IF;
    IF NOT st_cmp('pending-log','v ~ ''[abc]''')          THEN bad := bad+1; END IF;
    UPDATE st_t SET v = v || 'q' WHERE v LIKE 'churn\_2%';
    IF NOT st_cmp('pending-log','v ~ ''q$''')             THEN bad := bad+1; END IF;
    IF NOT st_cmp('pending-log','v ~ ''\d'' AND v ~ ''^churn''') THEN bad := bad+1; END IF;
    DELETE FROM st_t WHERE v LIKE 'churn\_3%';
    IF NOT st_cmp('pending-log','v ~ ''^churn''')         THEN bad := bad+1; END IF;
    IF NOT st_cmp('pending-log','v !~ ''^churn''')        THEN bad := bad+1; END IF;
    PERFORM st_rec('pending-log','regex over pending-log churn', bad = 0,
                   format('%s mismatches', bad));
END $do$;

-- =====================================================================
-- H. All-keys-lossy fallback, single and multi column
-- =====================================================================

CREATE TABLE st_mc (id serial PRIMARY KEY, a text, b text);
INSERT INTO st_mc (a,b)
SELECT 'a'||(i%211), CASE WHEN i%13=0 THEN NULL ELSE 'b'||(i%97) END
FROM generate_series(1,8000) i;
INSERT INTO st_mc (a,b) VALUES (NULL,'x'), ('y',NULL), (NULL,NULL), ('',''); 
DELETE FROM st_mc WHERE id % 29 = 0;
CREATE INDEX st_mc_idx ON st_mc USING biscuit (a,b);
ANALYZE st_mc;

CREATE FUNCTION st_cmp_mc(p_pred text) RETURNS boolean AS $$
DECLARE a text; b text;
BEGIN
    PERFORM set_config('enable_seqscan','off',true);
    PERFORM set_config('enable_indexscan','on',true);
    PERFORM set_config('enable_bitmapscan','on',true);
    EXECUTE format('SELECT count(*)||''/''||coalesce(sum(id::bigint),0) FROM st_mc WHERE %s',p_pred) INTO a;
    PERFORM set_config('enable_seqscan','on',true);
    PERFORM set_config('enable_indexscan','off',true);
    PERFORM set_config('enable_bitmapscan','off',true);
    EXECUTE format('SELECT count(*)||''/''||coalesce(sum(id::bigint),0) FROM st_mc WHERE %s',p_pred) INTO b;
    IF a IS DISTINCT FROM b THEN
        PERFORM st_rec('all-lossy', p_pred, false, format('indexed=%s seqscan=%s', a, b));
        RETURN false;
    END IF;
    RETURN true;
END $$ LANGUAGE plpgsql;

DO $do$
DECLARE p text; bad int := 0; n int := 0;
BEGIN
    FOREACH p IN ARRAY ARRAY[
        $q$a ~ '[abc]'$q$,
        $q$a ~ '\d' AND b ~ '\d'$q$,
        $q$a ~ '(a|b)' AND b ~ '[0-9]'$q$,
        $q$b ~ '\w'$q$,
        $q$a !~ '[abc]'$q$,
        $q$a ~ '\d' AND b ~ '^b1$'$q$,
        $q$a ~ '^a1$' AND b ~ '\d'$q$
    ] LOOP
        n := n+1;
        IF NOT st_cmp_mc(p) THEN bad := bad+1; END IF;
    END LOOP;
    PERFORM st_rec('all-lossy', format('%s all/partial-lossy multicolumn', n),
                   bad = 0, format('%s mismatches', bad));
END $do$;

-- =====================================================================
-- I. Regressions for two bugs found by this file
-- =====================================================================
--
-- Both were invisible to fixtures that happen to contain the "right" data,
-- so each builds its own minimal table rather than reusing st_t.

-- I.1  Anchored LIKE dropped its length filter when the table contained no
--      value of the pattern's exact length.  biscuit_pattern.c applied the
--      exact-length bitmap only when it was non-NULL, and a NULL bitmap
--      (meaning "no rows of that length") fell through BOTH branches, so
--      the constraint was silently skipped and an anchored pattern
--      degraded into a bare prefix match.  Pure LIKE -- no regex involved
--      -- but every '^...$' regex decomposes to a wildcard-free glob and
--      therefore reaches the same code.
--
--      The fixture must NOT contain 'a' or 'ab', which is exactly what
--      hid this from the other suites.

CREATE TABLE st_len (v text);
INSERT INTO st_len VALUES ('abc'), ('abcd'), ('xyz');
INSERT INTO st_len SELECT 'pad' || i FROM generate_series(1,2000) i;
CREATE INDEX st_len_idx ON st_len USING biscuit (v);
ANALYZE st_len;

CREATE FUNCTION st_len_cmp(p_pred text) RETURNS boolean AS $fn$
DECLARE a bigint; b bigint;
BEGIN
    PERFORM set_config('enable_seqscan','off',true);
    PERFORM set_config('enable_indexscan','on',true);
    PERFORM set_config('enable_bitmapscan','on',true);
    EXECUTE format('SELECT count(*) FROM st_len WHERE %s', p_pred) INTO a;
    PERFORM set_config('enable_seqscan','on',true);
    PERFORM set_config('enable_indexscan','off',true);
    PERFORM set_config('enable_bitmapscan','off',true);
    EXECUTE format('SELECT count(*) FROM st_len WHERE %s', p_pred) INTO b;
    IF a IS DISTINCT FROM b THEN
        PERFORM st_rec('anchored-length', p_pred, false,
                       format('indexed=%s seqscan=%s', a, b));
        RETURN false;
    END IF;
    RETURN true;
END $fn$ LANGUAGE plpgsql;

DO $do$
DECLARE p text; bad int := 0; n int := 0;
BEGIN
    FOREACH p IN ARRAY ARRAY[
        -- prefixes of existing values, matching nothing: the exact failure
        $q$v LIKE 'a'$q$,      $q$v LIKE 'ab'$q$,     $q$v LIKE 'abc'$q$,
        $q$v LIKE 'x'$q$,      $q$v LIKE 'xy'$q$,     $q$v LIKE 'p'$q$,
        $q$v LIKE 'pa'$q$,     $q$v LIKE 'pad'$q$,
        -- and the regex forms, which decompose to the same globs
        $q$v ~ '^a$'$q$,       $q$v ~ '^ab$'$q$,      $q$v ~ '^abc$'$q$,
        $q$v ~ '^x$'$q$,       $q$v ~ '^xy$'$q$,      $q$v ~ '^pad$'$q$,
        $q$v ~* '^A$'$q$,      $q$v ~* '^AB$'$q$,     $q$v ~* '^ABC$'$q$,
        -- ILIKE form of the same path
        $q$v ILIKE 'a'$q$,     $q$v ILIKE 'ab'$q$,
        -- lengths beyond anything in the table
        $q$v LIKE 'abcde'$q$,  $q$v ~ '^.{9}$'$q$,    $q$v ~ '^.{40}$'$q$,
        -- negated forms exercise the complement of the same set
        $q$v NOT LIKE 'a'$q$,  $q$v !~ '^ab$'$q$
    ] LOOP
        n := n + 1;
        IF NOT st_len_cmp(p) THEN bad := bad + 1; END IF;
    END LOOP;
    PERFORM st_rec('anchored-length',
        format('%s anchored patterns with no same-length row', n),
        bad = 0, format('%s mismatches', bad));
END $do$;

-- I.2  Case-insensitive regex was rewritten onto ILIKE, but PostgreSQL's
--      regex case folding and ILIKE's lower()-based folding are different
--      relations, and they disagree in BOTH directions:
--
--          'I'  ~* 'ı'   true   /  'I'  ILIKE '%ı%'   false   (missing row)
--          'ǅ' ~* 'ǅ'  false  /  'ǅ' ILIKE '%ǅ%'  true    (extra row)
--
--      Extra rows are recoverable by recheck; missing rows are not. The
--      fix decomposes ~* only for pure-ASCII patterns (confining the
--      disagreement to the recoverable direction) and marks those
--      predicates for recheck, and never decomposes !~* at all, since the
--      complement of a superset is a subset.

CREATE TABLE st_fold (v text);
INSERT INTO st_fold VALUES
    ('I'),('i'),('İ'),('ı'),('ǅ'),('ǆ'),('Ǆ'),('ǈ'),('ǉ'),('Ǉ'),
    ('ǋ'),('ǌ'),('Ǌ'),('Σ'),('σ'),('ς'),('ß'),('ẞ'),('é'),('É'),
    ('a'),('A'),('abc'),('ABC'),('AbC');
INSERT INTO st_fold SELECT 'pad' || i FROM generate_series(1,2000) i;
CREATE INDEX st_fold_idx ON st_fold USING biscuit (v);
ANALYZE st_fold;

DO $do$
DECLARE
    pats text[] := ARRAY['I','i','İ','ı','ǅ','ǆ','Ǆ','ǈ','ǋ','Σ','σ','ς',
                         'ß','ẞ','é','É','a','A','abc','ABC','AbC'];
    ops  text[] := ARRAY['~*','!~*','~','!~'];
    p text; o text; a bigint; b bigint; bad int := 0; n int := 0;
BEGIN
    FOREACH o IN ARRAY ops LOOP
      FOREACH p IN ARRAY pats LOOP
        n := n + 1;
        PERFORM set_config('enable_seqscan','off',true);
        PERFORM set_config('enable_indexscan','on',true);
        PERFORM set_config('enable_bitmapscan','on',true);
        EXECUTE format('SELECT count(*) FROM st_fold WHERE v %s %L', o, '^'||p||'$') INTO a;
        PERFORM set_config('enable_seqscan','on',true);
        PERFORM set_config('enable_indexscan','off',true);
        PERFORM set_config('enable_bitmapscan','off',true);
        EXECUTE format('SELECT count(*) FROM st_fold WHERE v %s %L', o, '^'||p||'$') INTO b;
        IF a IS DISTINCT FROM b THEN
            bad := bad + 1;
            PERFORM st_rec('case-fold-regression', format('v %s ''^%s$''', o, p),
                           false, format('indexed=%s seqscan=%s', a, b));
        END IF;
      END LOOP;
    END LOOP;
    PERFORM st_rec('case-fold-regression',
        format('%s (operator, tricky-character) combinations', n),
        bad = 0, format('%s mismatches', bad));
END $do$;

-- =====================================================================
-- Results
-- =====================================================================
RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan;
RESET enable_material; RESET enable_hashjoin; RESET enable_mergejoin;

SELECT section, count(*) AS checks,
       count(*) FILTER (WHERE passed) AS passed,
       count(*) FILTER (WHERE NOT passed) AS failed
FROM st_result GROUP BY section ORDER BY section;

SELECT seq, section, name, detail FROM st_result WHERE NOT passed ORDER BY seq LIMIT 40;

DO $do$
DECLARE nf int; na int;
BEGIN
    SELECT count(*) FILTER (WHERE NOT passed), count(*) INTO nf, na FROM st_result;
    IF nf > 0 THEN
        RAISE EXCEPTION 'biscuit regex stress: % of % checks FAILED', nf, na;
    END IF;
    RAISE NOTICE 'biscuit regex stress: all % checks passed', na;
END $do$;

ROLLBACK;
