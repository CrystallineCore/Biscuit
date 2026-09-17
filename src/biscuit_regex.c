/*
 * biscuit_regex.c
 * Exact decomposition of a regular-expression subset into LIKE globs.
 *
 * See biscuit_regex.h for the supported subset, the rejected constructs,
 * and why exactness rather than approximation is the requirement.
 *
 * The parser is a single left-to-right pass with one atom of lookahead for
 * the quantifier.  There is no backtracking and no intermediate AST: every
 * atom is either emitted or causes an immediate rejection, so the function
 * is O(len) in the regex and O(output) in allocation.  Keeping it a single
 * pass is what makes "fails closed" auditable -- there is exactly one
 * `return unsupported` helper and every construct that reaches it is
 * visible in this file.
 */

#include "biscuit_regex.h"
#include "biscuit_utf8.h"
#include "catalog/pg_collation.h"      /* DEFAULT_COLLATION_OID */
#include "catalog/pg_collation_d.h"    /* COLLPROVIDER_ICU */
#include "utils/lsyscache.h"           /* get_op_opfamily_strategy() */
#include "utils/pg_locale.h"           /* pg_newlocale_from_collation() */

/* ================================================================
 * SECTION 1 – small helpers
 * ================================================================ */

/*
 * ARE treats a backslash before an ALPHANUMERIC as a class shorthand,
 * a backreference, or a constraint escape (\d \w \s \S \b \B \1 \n \t...).
 * Some of those are literals (\n, \t) and could in principle be expanded,
 * but the set is version-dependent and the win is negligible, so the whole
 * \<alnum> space is rejected.  A backslash before anything else is a plain
 * literal escape and is safe to unwrap.
 */
static bool
biscuit_regex_escape_is_literal(unsigned char c)
{
    return !((c >= 'a' && c <= 'z') ||
             (c >= 'A' && c <= 'Z') ||
             (c >= '0' && c <= '9'));
}

/*
 * Append one literal character (1..4 bytes, already validated) to the glob
 * under construction, escaping the three bytes that would otherwise be
 * read back as LIKE syntax by biscuit_parse_pattern().
 *
 * Only single-byte characters can collide: '%', '_' and '\' are all ASCII,
 * and every byte of a multi-byte UTF-8 sequence has the high bit set, so
 * the loop below never accidentally escapes a continuation byte.
 */
static void
biscuit_regex_emit_literal(StringInfo out, const char *atom, int atom_len)
{
    int k;

    for (k = 0; k < atom_len; k++)
    {
        char c = atom[k];

        if (c == '%' || c == '_' || c == '\\')
            appendStringInfoChar(out, '\\');
        appendStringInfoChar(out, c);
    }
}

/*
 * True if the '$' at byte offset `pos` is a real anchor rather than a
 * backslash-escaped literal.  Counts the unbroken run of backslashes
 * immediately before it: an even run means every backslash escaped the one
 * after it and the '$' is bare; an odd run means the last backslash
 * escaped the '$'.
 *
 * `low` bounds the scan so a leading '^' already consumed by the caller is
 * never walked back over.
 */
static bool
biscuit_regex_is_bare(const char *re, int pos, int low)
{
    int bs = 0;
    int j  = pos - 1;

    while (j >= low && re[j] == '\\')
    {
        bs++;
        j--;
    }
    return (bs % 2) == 0;
}

/* ================================================================
 * SECTION 2 – quantifier parsing
 * ================================================================ */

typedef enum
{
    BQ_NONE,        /* no quantifier          */
    BQ_STAR,        /* *      -> {0,}         */
    BQ_PLUS,        /* +      -> {1,}         */
    BQ_QUEST,       /* ?      -> {0,1}        */
    BQ_EXACT,       /* {n}                    */
    BQ_ATLEAST      /* {n,}                   */
} BiscuitQuantKind;

typedef struct
{
    BiscuitQuantKind kind;
    int              count;     /* n, for BQ_EXACT / BQ_ATLEAST */
} BiscuitQuant;

/*
 * Read the quantifier (if any) at *ip and advance *ip past it.
 *
 * Returns false only for a brace form outside the subset -- {n,m} with a
 * finite upper bound, a malformed brace, or a repeat count over
 * BISCUIT_REGEX_MAX_REPEAT.  "No quantifier present" is a successful parse
 * yielding BQ_NONE, since a bare atom is the common case.
 *
 * A '?' immediately following *, + or a brace form is the ARE non-greedy
 * marker.  It is consumed and discarded: non-greedy matching selects a
 * different substring when several matches are possible, but the set of
 * strings containing a match is identical, and that set is the only thing
 * a WHERE clause observes.
 */
static bool
biscuit_regex_read_quant(const char *re, int *ip, int end,
                         BiscuitQuant *q, const char **reason)
{
    int i = *ip;

    q->kind  = BQ_NONE;
    q->count = 0;

    if (i >= end)
        return true;

    if (re[i] == '*')      { q->kind = BQ_STAR;  i++; }
    else if (re[i] == '+') { q->kind = BQ_PLUS;  i++; }
    else if (re[i] == '?') { q->kind = BQ_QUEST; i++; *ip = i; return true; }
    else if (re[i] == '{')
    {
        int n = 0;
        int digits = 0;
        int j = i + 1;

        while (j < end && re[j] >= '0' && re[j] <= '9')
        {
            /*
             * Bound the accumulator itself, not just the final value: a
             * count like {99999999999} would overflow int before the
             * range check below ever ran.
             */
            if (n > BISCUIT_REGEX_MAX_REPEAT)
            {
                *reason = "repetition count too large";
                return false;
            }
            n = n * 10 + (re[j] - '0');
            digits++;
            j++;
        }

        if (digits == 0)
        {
            /*
             * '{' not introducing a valid bound.  ARE would treat this as
             * a literal brace, but that reading is easy to get subtly
             * wrong and the construct is vanishingly rare in practice, so
             * reject rather than guess.
             */
            *reason = "'{' that does not introduce a repetition bound";
            return false;
        }
        if (n > BISCUIT_REGEX_MAX_REPEAT)
        {
            *reason = "repetition count too large";
            return false;
        }

        if (j < end && re[j] == '}')
        {
            q->kind  = BQ_EXACT;
            q->count = n;
            i = j + 1;
        }
        else if (j + 1 < end && re[j] == ',' && re[j + 1] == '}')
        {
            q->kind  = BQ_ATLEAST;
            q->count = n;
            i = j + 2;
        }
        else
        {
            /*
             * {n,m} with a finite m.  Expressing "between n and m
             * characters" as a glob needs alternation ('__%' cannot cap
             * the length), so there is no exact rewrite.
             */
            *reason = "bounded repetition {n,m}";
            return false;
        }
    }
    else
    {
        return true;    /* no quantifier */
    }

    /* Optional ARE non-greedy marker on *, + or a brace form. */
    if (i < end && re[i] == '?')
        i++;

    *ip = i;
    return true;
}

/* ================================================================
 * SECTION 3 – the decomposer
 * ================================================================ */

BiscuitRegexResult
biscuit_regex_to_glob(const char *regex, char **out_glob, const char **out_reason)
{
    StringInfoData out;
    const char    *reason = "unsupported construct";
    int            len;
    int            i;
    int            end;
    bool           anchored_start = false;
    bool           anchored_end   = false;

    if (out_glob)
        *out_glob = NULL;

    if (regex == NULL)
    {
        if (out_reason)
            *out_reason = "null pattern";
        return BISCUIT_REGEX_UNSUPPORTED;
    }

    len = (int) strlen(regex);
    i   = 0;
    end = len;

    /*
     * Anchors first, so the main loop can treat a stray '^' or '$' as an
     * unconditional rejection.  Mid-pattern they remain anchors (this is
     * ARE, not a shell glob) and constrain the match position in a way no
     * LIKE pattern can express -- '^' after the first atom, for instance,
     * makes the whole regex unsatisfiable, which is very much not what
     * treating it as a literal '^' would produce.
     */
    if (i < end && regex[i] == '^')
    {
        anchored_start = true;
        i++;
    }

    /*
     * The `end > i` guard keeps this from re-reading a '^' the branch
     * above already consumed: for the regex "^" we must not then also see
     * a trailing anchor at offset 0.
     */
    if (end > i && regex[end - 1] == '$' &&
        biscuit_regex_is_bare(regex, end - 1, i))
    {
        anchored_end = true;
        end--;
    }

    initStringInfo(&out);

    /*
     * ~ is unanchored: without a leading '^' the regex may match starting
     * anywhere, which is a leading '%' in whole-string glob terms.
     */
    if (!anchored_start)
        appendStringInfoChar(&out, '%');

    while (i < end)
    {
        unsigned char c = (unsigned char) regex[i];
        const char   *atom     = NULL;   /* NULL means "the '.' atom" */
        int           atom_len = 0;
        BiscuitQuant  q;
        int           r;

        CHECK_FOR_INTERRUPTS();

        /* ---- read one atom ---- */
        if (c == '\\')
        {
            if (i + 1 >= end)
            {
                reason = "trailing backslash";
                goto unsupported;
            }
            if (!biscuit_regex_escape_is_literal((unsigned char) regex[i + 1]))
            {
                reason = "character-class shorthand or backreference escape";
                goto unsupported;
            }
            atom     = &regex[i + 1];
            atom_len = 1;
            i += 2;
        }
        else if (c == '.')
        {
            atom     = NULL;            /* the any-character atom */
            atom_len = 0;
            i += 1;
        }
        else if (c == '^' || c == '$')
        {
            reason = "anchor in the middle of the pattern";
            goto unsupported;
        }
        else if (c == '[' || c == ']')
        {
            reason = "bracket expression";
            goto unsupported;
        }
        else if (c == '(' || c == ')')
        {
            reason = "group or embedded-option directive";
            goto unsupported;
        }
        else if (c == '|')
        {
            reason = "alternation";
            goto unsupported;
        }
        else if (c == '*' || c == '+' || c == '?' || c == '{' || c == '}')
        {
            /*
             * A quantifier here has no atom to bind to -- either the regex
             * is malformed, or (for '*' at offset 0) it is one of ARE's
             * "***=" / "***:" director prefixes, which change how the rest
             * of the string is interpreted.  Neither is decomposable.
             */
            reason = "quantifier with no preceding atom, or director prefix";
            goto unsupported;
        }
        else
        {
            /* Ordinary literal.  One UTF-8 character is one atom, so that
             * a following quantifier repeats the whole character rather
             * than its trailing byte. */
            atom_len = biscuit_utf8_char_length(c);
            if (atom_len < 1 || i + atom_len > end)
            {
                reason = "truncated or invalid UTF-8 sequence";
                goto unsupported;
            }
            atom = &regex[i];
            i += atom_len;
        }

        /* ---- read the quantifier bound to it ---- */
        if (!biscuit_regex_read_quant(regex, &i, end, &q, &reason))
            goto unsupported;

        /* ---- emit ---- */
        if (atom == NULL)
        {
            /* '.' -- any single character, which is exactly '_'. */
            switch (q.kind)
            {
                case BQ_NONE:
                    appendStringInfoChar(&out, '_');
                    break;
                case BQ_STAR:
                    /* zero or more of anything == '%' */
                    appendStringInfoChar(&out, '%');
                    break;
                case BQ_PLUS:
                    /* one or more of anything == one char then anything */
                    appendStringInfoString(&out, "_%");
                    break;
                case BQ_EXACT:
                    for (r = 0; r < q.count; r++)
                        appendStringInfoChar(&out, '_');
                    break;
                case BQ_ATLEAST:
                    for (r = 0; r < q.count; r++)
                        appendStringInfoChar(&out, '_');
                    appendStringInfoChar(&out, '%');
                    break;
                case BQ_QUEST:
                    /*
                     * '.?' is "zero or one character".  '_' demands one and
                     * '%' permits many; there is no glob for exactly this,
                     * so it is out of the subset.
                     */
                    reason = "'?' quantifier";
                    goto unsupported;
            }
        }
        else
        {
            switch (q.kind)
            {
                case BQ_NONE:
                    biscuit_regex_emit_literal(&out, atom, atom_len);
                    break;
                case BQ_EXACT:
                    for (r = 0; r < q.count; r++)
                        biscuit_regex_emit_literal(&out, atom, atom_len);
                    break;
                default:
                    /*
                     * 'a*', 'a+', 'a?', 'a{2,}' -- repetition of a SPECIFIC
                     * character.  The tempting rewrites are all wrong:
                     * 'a*' is not 'a%' (which requires an 'a') and not '%'
                     * (which permits a 'b' where the regex demands nothing
                     * or 'a's).  A glob cannot constrain a run to one
                     * repeated character.
                     */
                    reason = "unbounded or optional repetition of a literal";
                    goto unsupported;
            }
        }
    }

    if (!anchored_end)
        appendStringInfoChar(&out, '%');

    /*
     * Collapse runs of '%'.  '%%' and '%' are equivalent to the matcher,
     * but the collapsed form is what the rest of the engine expects to
     * see: biscuit_parse_pattern() splits on '%' and an empty part between
     * two of them would add a no-op segment for the windowed matcher and
     * the cost model to reason about.  Cheap to do once here, versus
     * teaching every consumer to tolerate it.
     *
     * The scan is escape-aware: a '\%' is a literal percent and must not
     * be absorbed into an adjacent wildcard.
     */
    {
        char *src = out.data;
        char *dst = out.data;

        while (*src)
        {
            if (*src == '\\' && src[1])
            {
                *dst++ = *src++;        /* the backslash   */
                *dst++ = *src++;        /* the escaped byte */
                continue;
            }
            if (*src == '%')
            {
                *dst++ = '%';
                while (*src == '%')
                    src++;
                continue;
            }
            *dst++ = *src++;
        }
        *dst = '\0';
        out.len = (int) (dst - out.data);
    }

    if (out_glob)
        *out_glob = out.data;
    else
        pfree(out.data);

    if (out_reason)
        *out_reason = NULL;

    return BISCUIT_REGEX_EXACT;

unsupported:
    pfree(out.data);
    if (out_glob)
        *out_glob = NULL;
    if (out_reason)
        *out_reason = reason;
    return BISCUIT_REGEX_UNSUPPORTED;
}

/* ================================================================
 * SECTION 4 – strategy-number helpers
 * ================================================================ */

/*
 * biscuit_strategy_for_operator
 *
 * Recover the Biscuit strategy number for an operator, by asking the
 * opfamily the index column was built with.
 *
 * ScanKey.sk_strategy is the normal source of this, but the cost model
 * (biscuit_costestimate(), biscuit_index.c) runs before ScanKeys exist and
 * has only the clause tree, so it has to work back from the operator.
 *
 * Going through get_op_opfamily_strategy() rather than comparing against
 * OID_TEXT_REGEXEQ_OP and friends is deliberate. The opfamily is the same
 * pg_amop data that biscuit.sql populated, so this answer is correct by
 * construction and stays correct if the operator set is ever extended --
 * there is no second list of operators here to drift out of sync with the
 * SQL, no hardcoded catalog OIDs, no dependency on which OID_TEXT_* macros
 * a given PostgreSQL release happens to export, and no get_negator() round
 * trip to recover !~ and !~* (they are registered as strategies 6 and 8
 * directly, so they come back for free).
 *
 * Returns 0 when the operator is not a member of the family, which callers
 * treat as "not a regex operator" via biscuit_strategy_is_regex().
 */
int
biscuit_strategy_for_operator(Oid opno, Oid opfamily)
{
    if (!OidIsValid(opno) || !OidIsValid(opfamily))
        return 0;
    return get_op_opfamily_strategy(opno, opfamily);
}

/*
 * biscuit_regex_glob_is_ascii
 *
 * True if every byte of the emitted glob is 7-bit ASCII.
 *
 * Used to gate case-insensitive decomposition. PostgreSQL's regex ~*
 * operator and the LIKE-family ILIKE do NOT agree on case folding once
 * non-ASCII is involved, and they disagree in both directions:
 *
 *     'I'  ~* 'ı'          -> true    'I'  ILIKE '%ı%'   -> false
 *     'ǅ' ~* 'ǅ'         -> false   'ǅ' ILIKE '%ǅ%'  -> true
 *
 * The first direction is the dangerous one. ILIKE missing a row that ~*
 * matches means the index returns too FEW rows, and executor recheck can
 * only ever remove rows, never restore them. Restricting the rewrite to
 * ASCII patterns confines the disagreement to the second direction, where
 * ILIKE over-matches and recheck can clean up.
 */
bool
biscuit_regex_glob_is_ascii(const char *glob)
{
    const unsigned char *p = (const unsigned char *) glob;

    if (p == NULL)
        return false;
    for (; *p; p++)
        if (*p >= 0x80)
            return false;
    return true;
}

/*
 * biscuit_glob_is_position_sensitive
 *
 * True if `glob` contains a bare (unescaped) '_'. biscuit_regex_to_glob()
 * only ever emits a bare '_' for a regex '.' (see the BQ_NONE/BQ_EXACT/
 * BQ_ATLEAST arms above); every literal underscore from the source regex
 * is escaped as '\_' by biscuit_regex_emit_literal(). So a bare '_' here
 * means the glob has to line up one specific DATA character against one
 * specific pattern position -- unlike a plain '%literal%' substring test,
 * which only asks whether the literal occurs somewhere, and so tolerates
 * a neighbouring character changing length under case-folding.
 *
 * The escaping convention mirrored here is exactly
 * biscuit_regex_emit_literal()'s: '\' escapes the single byte after it.
 */
static bool
biscuit_glob_is_position_sensitive(const char *glob)
{
    const char *p = glob;

    if (p == NULL)
        return false;

    for (; *p; p++)
    {
        if (*p == '\\' && p[1] != '\0')
        {
            p++;
            continue;
        }
        if (*p == '_')
            return true;
    }
    return false;
}

/*
 * biscuit_ci_regex_collation_safe
 *
 * Second half of the case-insensitive decomposition gate (the first half
 * is biscuit_regex_glob_is_ascii()). ASCII-ness of the PATTERN rules out
 * one hazard -- ILIKE and ~* disagreeing on how a non-ASCII pattern
 * character folds -- but says nothing about how the DATA's collation
 * folds case, which is a second, independent hazard this function closes.
 *
 * Two failure modes, both checked here:
 *
 *   NONDETERMINISTIC COLLATIONS. Equality is no longer byte-for-byte
 *   (e.g. accent- or width-insensitive collations can equate strings of
 *   different lengths outright), so there is no basis at all for
 *   trusting a byte-oriented glob rewrite. Refused unconditionally.
 *
 *   ICU CASE-FOLDING LENGTH CHANGES. Verified empirically (scanning
 *   character_length(lower(chr(g))) for every BMP codepoint, g = 1..
 *   65533 excluding the UTF-16 surrogate range) that the database's
 *   default/libc lower() never changes a character's length -- 0 of
 *   65486 codepoints tested. The same scan against ICU's "und-x-icu"
 *   collation found exactly one: U+0130 (LATIN CAPITAL LETTER I WITH DOT
 *   ABOVE), which lowers to 'i' + a combining dot above (1 char -> 2
 *   chars) under ICU, and to plain 'i' (1 char -> 1 char) under libc.
 *   This holds even though "und-x-icu" is itself a DETERMINISTIC
 *   collation, so determinism alone does not close this hole.
 *
 *   A length change only matters when the glob is position-sensitive
 *   (biscuit_glob_is_position_sensitive()): a '_' standing in for regex
 *   '.' requires the data to have exactly one character there, and if
 *   that character folds to two, the alignment of every following '_'/
 *   literal shifts and the ILIKE rewrite can miss a row that ~* would
 *   have matched -- an UNDER-match that recheck, a pure filter, cannot
 *   repair. A glob with no '_' (a plain '%literal%'/prefix/suffix test)
 *   has no such alignment to break: substring containment does not care
 *   that some OTHER character elsewhere folded to a different length.
 *
 * Only one ICU codepoint is known to misbehave this way, but nothing
 * guarantees it is the only one across every ICU locale and future
 * Unicode version, so this refuses the whole ICU + position-sensitive
 * combination rather than special-casing U+0130.
 */
bool
biscuit_ci_regex_collation_safe(Oid collation, const char *glob)
{
    pg_locale_t locale;

    if (!OidIsValid(collation))
        collation = DEFAULT_COLLATION_OID;

    locale = pg_newlocale_from_collation(collation);

    /* NULL locale means "C"/"POSIX": deterministic, byte-for-byte, no
     * case-folding at all beyond plain ASCII -- safe unconditionally. */
    if (locale == NULL)
        return true;

    if (!locale->deterministic)
        return false;

    if (locale->provider == COLLPROVIDER_ICU &&
        biscuit_glob_is_position_sensitive(glob))
        return false;

    return true;
}

bool
biscuit_strategy_is_regex(int strategy){
    return strategy == BISCUIT_REGEX_STRATEGY ||
           strategy == BISCUIT_NOT_REGEX_STRATEGY ||
           strategy == BISCUIT_IREGEX_STRATEGY ||
           strategy == BISCUIT_NOT_IREGEX_STRATEGY;
}

int
biscuit_regex_effective_strategy(int strategy)
{
    switch (strategy)
    {
        case BISCUIT_REGEX_STRATEGY:      return BISCUIT_LIKE_STRATEGY;
        case BISCUIT_NOT_REGEX_STRATEGY:  return BISCUIT_NOT_LIKE_STRATEGY;
        case BISCUIT_IREGEX_STRATEGY:     return BISCUIT_ILIKE_STRATEGY;
        case BISCUIT_NOT_IREGEX_STRATEGY: return BISCUIT_NOT_ILIKE_STRATEGY;
        default:                          return strategy;
    }
}

bool
biscuit_strategy_is_case_insensitive(int strategy)
{
    return strategy == BISCUIT_ILIKE_STRATEGY ||
           strategy == BISCUIT_NOT_ILIKE_STRATEGY ||
           strategy == BISCUIT_IREGEX_STRATEGY ||
           strategy == BISCUIT_NOT_IREGEX_STRATEGY;
}

bool
biscuit_strategy_is_negated(int strategy)
{
    return strategy == BISCUIT_NOT_LIKE_STRATEGY ||
           strategy == BISCUIT_NOT_ILIKE_STRATEGY ||
           strategy == BISCUIT_NOT_REGEX_STRATEGY ||
           strategy == BISCUIT_NOT_IREGEX_STRATEGY;
}
