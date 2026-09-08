-- biscuit--3.0.0--3.1.0.sql
-- Adds regular-expression support (~ !~ ~* !~*) to an existing Biscuit 3.0.0
-- installation.
--
-- NO REINDEX REQUIRED, and no on-disk format change. Regex support is
-- purely a plan-time rewrite: a regex qual is decomposed into an equivalent
-- LIKE glob (biscuit_regex_to_glob(), biscuit_regex.c) and then evaluated
-- by the same character-position bitmaps that already serve LIKE/ILIKE.
-- The existing index structures serve the rewritten glob unchanged, so this
-- upgrade only has to teach the catalog which operators the AM now answers
-- to.
--
-- You DO need to have installed the new shared library (the one built with
-- biscuit_regex.o) before running this, since strategies 5-8 will start
-- being dispatched to it as soon as the operators are registered.

\echo Use "ALTER EXTENSION biscuit UPDATE TO '3.1.0'" to load this file. \quit

-- ==================== OPERATOR FAMILY MEMBERSHIP ====================
--
-- ADD OPERATOR on the FAMILY (rather than recreating the opclass) is what
-- makes this a no-REINDEX upgrade: the planner matches an index qual to an
-- index column by opfamily membership, so loose family members are enough
-- to make ~ / !~ / ~* / !~* eligible. Nothing about the stored index
-- changes, and existing indexes pick this up immediately.
--
-- Case-sensitivity gating mirrors LIKE/ILIKE exactly, because it is the
-- same underlying structure set: ~ and !~ need the case-sensitive
-- position/length/char-cache structures, ~* and !~* need the _lower ones.
-- An index built with biscuit_like_ops simply does not have the _lower
-- structures, so registering 7/8 there would let the planner choose an
-- index that cannot answer the query.

-- biscuit_ops builds both structure sets, so it gets all four.
ALTER OPERATOR FAMILY biscuit_ops USING biscuit ADD
    OPERATOR 5 ~ (text, text),
    OPERATOR 6 !~ (text, text),
    OPERATOR 7 ~* (text, text),
    OPERATOR 8 !~* (text, text);

-- biscuit_like_ops: case-sensitive structures only.
ALTER OPERATOR FAMILY biscuit_like_ops USING biscuit ADD
    OPERATOR 5 ~ (text, text),
    OPERATOR 6 !~ (text, text);

-- biscuit_ilike_ops: case-insensitive structures only.
ALTER OPERATOR FAMILY biscuit_ilike_ops USING biscuit ADD
    OPERATOR 7 ~* (text, text),
    OPERATOR 8 !~* (text, text);

-- ==================== VIEW REFRESH ====================
--
-- biscuit_operators decoded strategies 1-4 only and would have reported the
-- four new ones as 'UNKNOWN'.

CREATE OR REPLACE VIEW biscuit_operators AS
SELECT
    opf.opfname AS opfamily,
    amop.amopstrategy AS strategy,
    op.oprname AS operator,
    format_type(op.oprleft, NULL) AS left_type,
    format_type(op.oprright, NULL) AS right_type,
    CASE amop.amopstrategy
        WHEN 1 THEN 'LIKE'
        WHEN 2 THEN 'NOT LIKE'
        WHEN 3 THEN 'ILIKE'
        WHEN 4 THEN 'NOT ILIKE'
        WHEN 5 THEN 'regex match (~)'
        WHEN 6 THEN 'regex non-match (!~)'
        WHEN 7 THEN 'regex match, case-insensitive (~*)'
        WHEN 8 THEN 'regex non-match, case-insensitive (!~*)'
        ELSE 'UNKNOWN'
    END AS description,
    CASE
        WHEN amop.amopstrategy BETWEEN 5 AND 8 THEN
            'glob-decomposable subset only; other regexes fall back to recheck'
        ELSE 'fully supported'
    END AS coverage
FROM pg_amop amop
JOIN pg_operator op ON amop.amopopr = op.oid
JOIN pg_opfamily opf ON amop.amopfamily = opf.oid
JOIN pg_am am ON opf.opfmethod = am.oid
WHERE am.amname = 'biscuit'
ORDER BY opf.opfname, amop.amopstrategy;

COMMENT ON VIEW biscuit_operators IS
'Shows which operators are registered for Biscuit indexes, grouped by
opfamily. Strategies 1-4 are LIKE/ILIKE; strategies 5-8 are the regex
operators, which are served by rewriting the regex into an equivalent glob
at plan time and are therefore accelerated only for the decomposable subset
(see the REGULAR EXPRESSIONS section of biscuit.sql). Text columns can use
biscuit_ops (default), biscuit_like_ops, or biscuit_ilike_ops -- each has
its own opfamily, so this view shows a separate row group per opclass in
use. CHAR(n)/bpchar columns are supported via an expression index on
(col::text), not a native opclass.';

-- ==================== VERSION HISTORY ====================

INSERT INTO biscuit_version_table (version, description) VALUES
('3.1.0', 'Regular expression support (~ !~ ~* !~*, strategies 5-8) for the subset of regexes that decompose exactly to a LIKE glob -- anchors, literals, ".", ".*", ".+" and fixed/open-ended repetition counts. Regexes outside the subset remain correct via executor recheck and are priced out of the planner rather than accelerated. No on-disk format change and no REINDEX required: the rewrite happens entirely at plan time and the existing index structures serve the resulting glob unchanged.')
ON CONFLICT (version) DO NOTHING;
