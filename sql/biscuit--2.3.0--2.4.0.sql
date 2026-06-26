-- biscuit--2.3.0--2.4.0.sql
-- Migration script: Biscuit 2.3.0 → 2.4.0
--
-- Changes in this release:
--   FIX #7: biscuit_operators view now joins through pg_am and filters by
--            am.amname = 'biscuit' instead of a hardcoded opfamily name.
--            Adds opfamily column so future opclasses are included automatically.
--   FIX #9: biscuit_text_ops comment corrected: CHAR(n)/bpchar is NOT covered
--            by an implicit cast to text.  Documented expression index workaround.

-- Complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION biscuit UPDATE TO '2.4.0'" to load this file. \quit

-- ==================== FIX #7: biscuit_operators view ====================
--
-- 2.3.0 filtered by a hardcoded opfamily name (pg_opfamily.opfname =
-- 'biscuit_text_ops'), which silently excludes any opclass added in future
-- releases.  The new definition joins through pg_am so every opfamily
-- registered under the 'biscuit' access method is covered.  It also adds
-- an opfamily column so callers can distinguish families when there is more
-- than one.
--
-- The view's column list changes (opfamily is new), so DROP + CREATE is
-- required.  The GRANT is re-issued because it is lost on DROP.

DROP VIEW biscuit_operators;

CREATE VIEW biscuit_operators AS
SELECT
    opf.opfname                    AS opfamily,
    amop.amopstrategy              AS strategy,
    op.oprname                     AS operator,
    format_type(op.oprleft,  NULL) AS left_type,
    format_type(op.oprright, NULL) AS right_type,
    CASE amop.amopstrategy
        WHEN 1 THEN 'LIKE'
        WHEN 2 THEN 'NOT LIKE'
        WHEN 3 THEN 'ILIKE'
        WHEN 4 THEN 'NOT ILIKE'
        ELSE        'UNKNOWN'
    END                            AS description
FROM pg_amop     amop
JOIN pg_operator op  ON amop.amopopr    = op.oid
JOIN pg_opfamily opf ON amop.amopfamily = opf.oid
JOIN pg_am       am  ON opf.opfmethod   = am.oid
WHERE am.amname = 'biscuit'
ORDER BY opf.opfname, amop.amopstrategy;

COMMENT ON VIEW biscuit_operators IS
'Shows which operators are registered for Biscuit indexes.
Currently text (via biscuit_text_ops); CHAR(n)/bpchar columns are
supported via an expression index on (col::text), not a native opclass.';

GRANT SELECT ON biscuit_operators TO PUBLIC;

-- ==================== FIX #9: biscuit_text_ops comment ====================
--
-- 2.3.0 stated "VARCHAR and CHAR types will implicitly cast to text to use
-- this class."  That is wrong for CHAR(n)/bpchar: PostgreSQL has no
-- (bpchar, bpchar) LIKE/ILIKE operators, so a plain index on a bpchar
-- column cannot be used by this opclass regardless of access method.
-- The correct workaround is an expression index on the ::text cast:
--   CREATE INDEX idx ON tbl USING biscuit ((char_col::text));

COMMENT ON OPERATOR CLASS biscuit_text_ops USING biscuit IS
'Operator class for text types - supports LIKE, NOT LIKE, ILIKE, NOT ILIKE.
VARCHAR types will implicitly cast to text to use this class.';

-- ==================== VERSION TABLE ====================

INSERT INTO biscuit_version_table (version, description)
VALUES ('2.4.0', 'Added expression index support and improved parallel scan and datatype compatibility')
ON CONFLICT (version) DO NOTHING;
