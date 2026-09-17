/*
 * biscuit_regex.h
 * Decomposition of a subset of POSIX regular expressions into equivalent
 * LIKE/ILIKE glob patterns, so that regex predicates can be served by the
 * existing Biscuit character-position bitmap machinery unchanged.
 *
 * ============================================================
 * WHY A SUBSET, AND WHY "CLEANLY"
 * ============================================================
 *
 * Biscuit's whole query engine is built around LIKE semantics: a pattern
 * is a sequence of literal parts separated by '%' wildcards, with '_'
 * standing for exactly one character (biscuit_parse_pattern(),
 * biscuit_pattern.c).  That structure is what makes the per-position
 * bitmaps usable -- each literal part can be evaluated as an AND of
 * "character c at offset k" bitmaps.
 *
 * A general regular expression has no such structure.  Alternation,
 * bracket expressions, backreferences and bounded repetition all describe
 * sets of strings that cannot be written as a single glob.  Rather than
 * bolt a second, weaker matching engine onto the index, this module takes
 * the opposite approach: identify the regexes that ARE exactly a glob in
 * disguise, rewrite them, and hand them to the existing engine.
 *
 * "Cleanly decomposable" here means EXACTLY equivalent -- the emitted glob
 * matches precisely the same set of strings as the regex, no more and no
 * less.  This matters because Biscuit is an exact-match AM: it sets
 * xs_recheck = false and nothing downstream re-tests the predicate (see
 * biscuit_scan.c and the invariant comments in biscuit_index.c,
 * biscuit_pendlog.c).  A merely-approximate rewrite would silently return
 * wrong rows.  Anything this module cannot prove exact is reported as
 * BISCUIT_REGEX_UNSUPPORTED and handled by the lossy path described in
 * biscuit_build_query_plan(), which falls back to recheck rather than
 * trusting an inexact glob.
 *
 * ============================================================
 * THE SUPPORTED SUBSET
 * ============================================================
 *
 *   ANCHORS
 *     ^ at the very start          -- anchors the match to the start
 *     $ at the very end            -- anchors the match to the end
 *
 *     PostgreSQL's ~ operator is UNANCHORED (it matches if the regex
 *     matches anywhere in the value), whereas LIKE is whole-string.  A
 *     missing ^ therefore becomes a leading '%' and a missing $ becomes a
 *     trailing '%'.  ^ or $ anywhere else is rejected: mid-pattern they
 *     are still anchors, not literals, and cannot be expressed as a glob.
 *
 *   ATOMS
 *     literal characters           -- copied through (UTF-8 aware; a
 *                                     multi-byte character is one atom)
 *     .                            -- becomes '_'  (any single character)
 *     \X for non-alphanumeric X    -- literal X  ( \. \* \\ \$ ... )
 *
 *   QUANTIFIERS
 *     .*        -> %               (any run, possibly empty)
 *     .+        -> _%              (any run, at least one character)
 *     .{n}      -> n underscores
 *     .{n,}     -> n underscores followed by %
 *     X{n}      -> n copies of literal X
 *     a trailing '?' on any of the above (the ARE non-greedy marker) is
 *     accepted and ignored: greediness changes which substring a match
 *     prefers, never whether a match exists, and Biscuit only ever asks
 *     the latter question.
 *
 * ============================================================
 * DELIBERATELY REJECTED
 * ============================================================
 *
 *   ( ) groups and alternation |   -- a|b is a set of two globs, not one
 *   [ ] bracket expressions        -- [abc] likewise; '_' is strictly
 *                                     weaker (any char) and 'a' strictly
 *                                     stronger, neither is equivalent
 *   X* X+ X? on a literal          -- unbounded/optional repetition of a
 *                                     specific character has no glob form
 *                                     ('a*' is not 'a%' -- 'a%' requires
 *                                     the 'a')
 *   .? and X{n,m} with n != m      -- a bounded upper limit needs
 *                                     alternation to express
 *   \d \w \s \b \1 ...             -- class shorthands and backreferences
 *                                     (any \<alphanumeric> escape)
 *   ***= ***: (?...) directives    -- rejected by the '(' and leading-'*'
 *                                     rules; embedded options such as
 *                                     (?n) would also change '.' semantics
 *                                     out from under the '.' -> '_' rule
 *
 * Rejection is always safe -- it costs a recheck, never correctness.  The
 * failure mode this module must avoid is the opposite one: emitting a glob
 * for a regex it did not fully understand.  Every branch below therefore
 * fails closed.
 */

#ifndef BISCUIT_REGEX_H
#define BISCUIT_REGEX_H

#include "biscuit_common.h"

/*
 * Upper bound on a single {n} / {n,} repetition count.
 *
 * Set to match PostgreSQL's own DUPMAX (_POSIX2_RE_DUP_MAX, 255 -- see
 * src/include/regex/regguts.h): a larger count is rejected by the server's
 * regex compiler with "invalid repetition count(s)" before this code ever
 * runs, so accepting more here could only ever produce a glob for a regex
 * that is not legal in the first place.
 *
 * Two things this guard is still doing. It bounds the expansion, since
 * '.{255}' becomes 255 underscores and a pattern built from many such
 * quantifiers would otherwise let a short query string turn into a large
 * allocation. And it keeps the decomposer honest if it is ever called from
 * somewhere that has not already been through the server's parser -- over
 * the limit is treated as unsupported, which degrades to recheck (and thus
 * to PostgreSQL raising its own error) rather than to a silently different
 * answer.
 */
#define BISCUIT_REGEX_MAX_REPEAT   255

typedef enum BiscuitRegexResult
{
    /*
     * The regex was fully decomposed.  *out_glob is a LIKE pattern
     * matching exactly the same strings, safe to feed to the normal
     * exact-match path with xs_recheck left false.
     */
    BISCUIT_REGEX_EXACT = 0,

    /*
     * The regex uses a construct outside the supported subset.  *out_glob
     * is left NULL and the caller must NOT use the index result as
     * authoritative -- see biscuit_build_query_plan()'s lossy handling.
     */
    BISCUIT_REGEX_UNSUPPORTED
} BiscuitRegexResult;

/*
 * biscuit_regex_to_glob
 *
 * Attempt an exact rewrite of `regex` (as passed to the ~ / ~* family of
 * operators, i.e. UNANCHORED POSIX ARE) into a LIKE pattern.
 *
 * On BISCUIT_REGEX_EXACT, *out_glob receives a palloc'd string in the
 * current memory context, using the same backslash escape convention
 * biscuit_parse_pattern() expects ('\%' and '\_' for literal wildcards,
 * '\\' for a literal backslash).  On BISCUIT_REGEX_UNSUPPORTED it is set
 * to NULL.
 *
 * out_reason, when non-NULL, receives a short static string naming the
 * construct that caused the rejection.  It is meant for DEBUG logging and
 * for the errdetail of the "cannot use index for this regex" path; it is
 * never NULL-terminated garbage and never needs freeing.
 */
extern BiscuitRegexResult biscuit_regex_to_glob(const char *regex,
                                                char **out_glob,
                                                const char **out_reason);

/* ==================== STRATEGY HELPERS ==================== */

/*
 * Recover a strategy number for an operator by looking it up in the index
 * column's opfamily, for callers that run before ScanKeys exist (the
 * planner cost model). Returns 0 if the operator is not a member of the
 * family -- see the implementation comment for why this goes through the
 * catalog rather than comparing operator OIDs.
 */
extern int biscuit_strategy_for_operator(Oid opno, Oid opfamily);

/*
 * True if the glob contains no non-ASCII bytes. Gates case-insensitive
 * rewrites -- see the implementation comment for why ~* and ILIKE cannot
 * be treated as equivalent over non-ASCII text.
 */
extern bool biscuit_regex_glob_is_ascii(const char *glob);

/*
 * Second half of the case-insensitive decomposition gate: true if `glob`
 * can be trusted for data compared under `collation`. Refuses
 * nondeterministic collations outright, and refuses ICU collations
 * specifically for position-sensitive globs (those with a bare '_'),
 * where a length-changing case fold (verified for U+0130) can make the
 * rewrite silently drop a row that ~* would have matched. See the
 * implementation comment for the full argument and the empirical check
 * behind it.
 */
extern bool biscuit_ci_regex_collation_safe(Oid collation, const char *glob);

/* True for the four regex strategy numbers (5..8). */
extern bool biscuit_strategy_is_regex(int strategy);

/*
 * Map a regex strategy onto the LIKE/ILIKE strategy with the same case
 * sensitivity and the same polarity:
 *
 *   BISCUIT_REGEX_STRATEGY      (~)   -> BISCUIT_LIKE_STRATEGY
 *   BISCUIT_NOT_REGEX_STRATEGY  (!~)  -> BISCUIT_NOT_LIKE_STRATEGY
 *   BISCUIT_IREGEX_STRATEGY     (~*)  -> BISCUIT_ILIKE_STRATEGY
 *   BISCUIT_NOT_IREGEX_STRATEGY (!~*) -> BISCUIT_NOT_ILIKE_STRATEGY
 *
 * Non-regex strategies are returned unchanged, so callers can apply this
 * unconditionally to every key.
 */
extern int biscuit_regex_effective_strategy(int strategy);

/*
 * True if this strategy needs the case-insensitive ("_lower") structure
 * set.  Regex-aware replacement for the open-coded
 * (s == BISCUIT_ILIKE_STRATEGY || s == BISCUIT_NOT_ILIKE_STRATEGY) tests.
 */
extern bool biscuit_strategy_is_case_insensitive(int strategy);

/* True if this strategy is a negated one (NOT LIKE / NOT ILIKE / !~ / !~*). */
extern bool biscuit_strategy_is_negated(int strategy);

#endif /* BISCUIT_REGEX_H */
