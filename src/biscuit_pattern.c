/*
 * biscuit_pattern.c
 * LIKE / ILIKE pattern parsing, bitmap-level position matching,
 * recursive windowed matching, and the multi-column query optimizer.
 *
 * The public interface is declared in biscuit_pattern.h.
 */

#include "biscuit_common.h"
#include "biscuit_bitmap.h"
#include "biscuit_utf8.h"
#include "biscuit_pattern.h"

/* ================================================================
 * SECTION 1 – CharIndex bitmap accessor helpers
 * ================================================================
 *
 * Sorted PosEntry arrays use binary search for O(log n) lookup and
 * insertion-sort to keep entries ordered.
 */

/* ---------- single-column (legacy) case-sensitive ---------- */

RoaringBitmap *
biscuit_get_pos_bitmap(BiscuitIndex *idx, unsigned char ch, int pos)
{
    CharIndex *cidx  = &idx->pos_idx_legacy[ch];
    int        left  = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < pos) left  = mid + 1;
        else                                    right = mid - 1;
    }
    return NULL;
}

RoaringBitmap *
biscuit_get_neg_bitmap(BiscuitIndex *idx, unsigned char ch, int neg_offset)
{
    CharIndex *cidx  = &idx->neg_idx_legacy[ch];
    int        left  = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < neg_offset) left  = mid + 1;
        else                                           right = mid - 1;
    }
    return NULL;
}

void
biscuit_set_pos_bitmap(BiscuitIndex *idx, unsigned char ch, int pos, RoaringBitmap *bm)
{
    CharIndex *cidx       = &idx->pos_idx_legacy[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < pos) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_neg_bitmap(BiscuitIndex *idx, unsigned char ch, int neg_offset, RoaringBitmap *bm)
{
    CharIndex *cidx       = &idx->neg_idx_legacy[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < neg_offset) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

/* ---------- single-column case-insensitive (lower) ---------- */

RoaringBitmap *
biscuit_get_pos_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int pos)
{
    CharIndex *cidx  = &idx->pos_idx_lower[ch];
    int        left  = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < pos) left  = mid + 1;
        else                                    right = mid - 1;
    }
    return NULL;
}

RoaringBitmap *
biscuit_get_neg_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int neg_offset)
{
    CharIndex *cidx  = &idx->neg_idx_lower[ch];
    int        left  = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < neg_offset) left  = mid + 1;
        else                                           right = mid - 1;
    }
    return NULL;
}

void
biscuit_set_pos_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int pos, RoaringBitmap *bm)
{
    CharIndex *cidx       = &idx->pos_idx_lower[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < pos) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_neg_bitmap_lower(BiscuitIndex *idx, unsigned char ch, int neg_offset, RoaringBitmap *bm)
{
    CharIndex *cidx       = &idx->neg_idx_lower[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < neg_offset) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

/* ---------- multi-column accessors ---------- */

RoaringBitmap *
biscuit_get_col_pos_bitmap(ColumnIndex *col, unsigned char ch, int pos)
{
    CharIndex *cidx  = &col->pos_idx[ch];
    int        left  = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < pos) left  = mid + 1;
        else                                    right = mid - 1;
    }
    return NULL;
}

RoaringBitmap *
biscuit_get_col_neg_bitmap(ColumnIndex *col, unsigned char ch, int neg_offset)
{
    CharIndex *cidx  = &col->neg_idx[ch];
    int        left  = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < neg_offset) left  = mid + 1;
        else                                           right = mid - 1;
    }
    return NULL;
}

RoaringBitmap *
biscuit_get_col_pos_bitmap_lower(ColumnIndex *col, unsigned char ch, int pos)
{
    CharIndex *cidx  = &col->pos_idx_lower[ch];
    int        left  = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < pos) left  = mid + 1;
        else                                    right = mid - 1;
    }
    return NULL;
}

RoaringBitmap *
biscuit_get_col_neg_bitmap_lower(ColumnIndex *col, unsigned char ch, int neg_offset)
{
    CharIndex *cidx  = &col->neg_idx_lower[ch];
    int        left  = 0, right = cidx->count - 1;
    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) return cidx->entries[mid].bitmap;
        else if (cidx->entries[mid].pos < neg_offset) left  = mid + 1;
        else                                           right = mid - 1;
    }
    return NULL;
}

void
biscuit_set_col_pos_bitmap(ColumnIndex *col, unsigned char ch, int pos, RoaringBitmap *bm)
{
    CharIndex *cidx       = &col->pos_idx[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < pos) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_col_neg_bitmap(ColumnIndex *col, unsigned char ch, int neg_offset, RoaringBitmap *bm)
{
    CharIndex *cidx       = &col->neg_idx[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < neg_offset) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_col_pos_bitmap_lower(ColumnIndex *col, unsigned char ch, int pos, RoaringBitmap *bm)
{
    CharIndex *cidx       = &col->pos_idx_lower[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == pos) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < pos) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = pos;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

void
biscuit_set_col_neg_bitmap_lower(ColumnIndex *col, unsigned char ch, int neg_offset, RoaringBitmap *bm)
{
    CharIndex *cidx       = &col->neg_idx_lower[ch];
    int        left       = 0, right = cidx->count - 1;
    int        insert_pos = cidx->count;
    int        i;

    while (left <= right) {
        int mid = (left + right) >> 1;
        if (cidx->entries[mid].pos == neg_offset) { cidx->entries[mid].bitmap = bm; return; }
        else if (cidx->entries[mid].pos < neg_offset) left = mid + 1;
        else { insert_pos = mid; right = mid - 1; }
    }

    if (cidx->count >= cidx->capacity) {
        int       new_cap     = cidx->capacity * 2;
        PosEntry *new_entries = (PosEntry *) palloc(new_cap * sizeof(PosEntry));
        if (cidx->count > 0) memcpy(new_entries, cidx->entries, cidx->count * sizeof(PosEntry));
        pfree(cidx->entries);
        cidx->entries  = new_entries;
        cidx->capacity = new_cap;
    }

    for (i = cidx->count; i > insert_pos; i--)
        cidx->entries[i] = cidx->entries[i - 1];

    cidx->entries[insert_pos].pos    = neg_offset;
    cidx->entries[insert_pos].bitmap = bm;
    cidx->count++;
}

/* ================================================================
 * SECTION 2 – Length bitmap helpers
 * ================================================================ */

static RoaringBitmap *
biscuit_get_length_ge(BiscuitIndex *idx, int min_len)
{
    if (min_len <= 0 && idx->length_ge_bitmaps_legacy && idx->length_ge_bitmaps_legacy[0])
        return biscuit_roaring_copy(idx->length_ge_bitmaps_legacy[0]);
    if (min_len < idx->max_length_legacy && idx->length_ge_bitmaps_legacy &&
        idx->length_ge_bitmaps_legacy[min_len])
        return biscuit_roaring_copy(idx->length_ge_bitmaps_legacy[min_len]);
    return biscuit_roaring_create();
}

static RoaringBitmap *
biscuit_get_length_ge_lower(BiscuitIndex *idx, int min_len)
{
    if (min_len <= 0 && idx->length_ge_bitmaps_lower && idx->length_ge_bitmaps_lower[0])
        return biscuit_roaring_copy(idx->length_ge_bitmaps_lower[0]);
    if (min_len < idx->max_length_lower && idx->length_ge_bitmaps_lower &&
        idx->length_ge_bitmaps_lower[min_len])
        return biscuit_roaring_copy(idx->length_ge_bitmaps_lower[min_len]);
    return biscuit_roaring_create();
}

static RoaringBitmap *
biscuit_get_col_length_ge(ColumnIndex *col, int min_len)
{
    if (min_len <= 0 && col->length_ge_bitmaps && col->length_ge_bitmaps[0])
        return biscuit_roaring_copy(col->length_ge_bitmaps[0]);
    if (min_len < col->max_length && col->length_ge_bitmaps &&
        col->length_ge_bitmaps[min_len])
        return biscuit_roaring_copy(col->length_ge_bitmaps[min_len]);
    return biscuit_roaring_create();
}

static RoaringBitmap *
biscuit_get_col_length_ge_lower(ColumnIndex *col, int min_len)
{
    if (min_len <= 0 && col->length_ge_bitmaps_lower && col->length_ge_bitmaps_lower[0])
        return biscuit_roaring_copy(col->length_ge_bitmaps_lower[0]);
    if (min_len < col->max_length_lower && col->length_ge_bitmaps_lower &&
        col->length_ge_bitmaps_lower[min_len])
        return biscuit_roaring_copy(col->length_ge_bitmaps_lower[min_len]);
    return biscuit_roaring_create();
}

/* ================================================================
 * SECTION 3 – Pattern parsing
 * ================================================================
 *
 * BISCUIT_LITERAL_ESC is a sentinel byte (SOH, 0x01) stored in
 * ParsedPattern part strings to indicate that the following byte is
 * a literal character rather than a LIKE wildcard.  It is used to
 * distinguish a literal '_' (escaped as '\\_' in SQL) from the
 * single-character wildcard '_'.
 *
 * 0x01 (SOH) is chosen because:
 *  - It cannot appear in valid UTF-8 text stored in PostgreSQL.
 *  - It cannot be a LIKE wildcard ('%' or '_').
 *  - It is distinct from any real data byte the matchers would see.
 */
#define BISCUIT_LITERAL_ESC  '\x01'

/*
 * biscuit_part_char_count
 * Count the number of LIKE pattern characters represented by a part
 * string that may contain BISCUIT_LITERAL_ESC + byte sentinel pairs.
 * Each sentinel pair counts as one character.  Regular UTF-8 sequences
 * are counted normally.
 */
static int
biscuit_part_char_count(const char *part, int byte_len)
{
    int count = 0;
    int i     = 0;
    while (i < byte_len)
    {
        unsigned char c = (unsigned char) part[i];
        if (c == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            i += 2; /* sentinel + literal byte = 1 char */
        }
        else
        {
            i += biscuit_utf8_char_length(c);
        }
        count++;
    }
    return count;
}

/*
 * biscuit_part_seed_byte
 * ----------------------
 * Return the first concrete (non-wildcard) byte from a part string for
 * use as a char-cache lookup seed.  Skips:
 *   - bare '_' bytes (single-char wildcard)
 *   - BISCUIT_LITERAL_ESC prefixes, but returns the byte AFTER the prefix
 *     (that byte is the literal character to seed from)
 * Returns 0 (NUL) if the part is all wildcards.
 */
static unsigned char
biscuit_part_seed_byte(const char *part, int byte_len)
{
    int i = 0;
    while (i < byte_len)
    {
        unsigned char c = (unsigned char) part[i];
        if (c == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            if (i + 1 < byte_len)
                return (unsigned char) part[i + 1]; /* literal char */
            i += 2;
        }
        else if (c == '_')
        {
            i++;  /* skip single-char wildcard */
        }
        else
        {
            return c;  /* first concrete character */
        }
    }
    return 0;  /* all wildcards */
}

/*
 * biscuit_part_match_substr
 * -------------------------
 * Check whether the sentinel-encoded part string matches a substring of
 * haystack starting at byte offset hay_off.
 *
 * Rules:
 *   - A bare '_' in the part matches exactly one UTF-8 character in hay.
 *   - A BISCUIT_LITERAL_ESC + byte pair matches that exact literal byte.
 *   - All other bytes match themselves exactly.
 *
 * Returns true on a full match of the part, false otherwise.
 * hay/hay_byte_len: the full string and its byte length.
 * hay_off: byte offset within hay where matching starts.
 */
static bool
biscuit_part_match_substr(const char *hay, int hay_byte_len, int hay_off,
                           const char *part, int part_byte_len)
{
    int pi = 0;  /* part byte position */
    int hi = hay_off;  /* haystack byte position */

    while (pi < part_byte_len)
    {
        unsigned char pc;
        if (hi >= hay_byte_len)
            return false;

        pc = (unsigned char) part[pi];

        if (pc == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal;
            /* Literal match: part[pi+1] must equal hay[hi] exactly */
            if (pi + 1 >= part_byte_len)
                return false;
            literal = (unsigned char) part[pi + 1];
            if ((unsigned char) hay[hi] != literal)
                return false;
            pi += 2;
            hi += 1;
        }
        else if (pc == '_')
        {
            /* Single-char wildcard: consume one UTF-8 char from haystack */
            int cl = biscuit_utf8_char_length((unsigned char) hay[hi]);
            if (hi + cl > hay_byte_len)
                return false;
            pi += 1;
            hi += cl;
        }
        else
        {
            /* Literal byte from pattern: must match exactly */
            if ((unsigned char) hay[hi] != pc)
                return false;
            pi += 1;
            hi += 1;
        }
    }
    return true;  /* all part bytes consumed */
}


/* Splits a LIKE/ILIKE pattern on unescaped '%' wildcards.
 *
 * SQL escape convention (backslash, matching what PostgreSQL passes to
 * index AMs after processing the ESCAPE clause):
 *   '\%'  -> literal '%'  (not a wildcard separator)
 *   '\_'  -> literal '_'  (not a single-char wildcard)
 *   '\\'  -> literal '\'
 *
 * Each resulting part string has escape sequences collapsed to the
 * actual character they represent, so the downstream bitmap matchers
 * see real bytes and never mis-treat them as wildcards.
 *
 * starts_percent / ends_percent reflect whether the *unescaped* pattern
 * starts/ends with a wildcard '%'.
 */
ParsedPattern *
biscuit_parse_pattern(const char *pattern)
{
    ParsedPattern *parsed;
    int            plen      = strlen(pattern);
    int            max_parts = plen + 1;
    int            part_count;
    char          *buf;      /* unescaped part accumulator */
    int            buf_len;
    bool           in_part;
    int            i;

    buf      = (char *) palloc(2 * plen + 1); /* 2× for literal-_ sentinel pairs */
    buf_len  = 0;
    in_part  = false;

    parsed                 = (ParsedPattern *) palloc(sizeof(ParsedPattern));
    parsed->parts          = (char **) palloc(max_parts * sizeof(char *));
    parsed->part_lens      = (int *)   palloc(max_parts * sizeof(int));
    parsed->part_byte_lens = (int *)   palloc(max_parts * sizeof(int));
    parsed->part_count     = 0;
    parsed->starts_percent = false;
    parsed->ends_percent   = false;
    part_count             = 0;

    /*
     * Determine starts_percent: true only when the very first character
     * is an unescaped '%'.
     */
    if (plen > 0)
    {
        if (pattern[0] == '\\' && plen > 1)
            parsed->starts_percent = false;  /* escaped char — not a wildcard */
        else
            parsed->starts_percent = (pattern[0] == '%');
    }

    /*
     * Determine ends_percent: true only when the last character is an
     * unescaped '%'.  Count consecutive backslashes immediately before
     * it; an odd count means the '%' is escaped.
     */
    if (plen > 0 && pattern[plen - 1] == '%')
    {
        int bs = 0, j = plen - 2;
        while (j >= 0 && pattern[j] == '\\') { bs++; j--; }
        parsed->ends_percent = ((bs % 2) == 0);
    }

    /* Main scan: walk pattern, unescape on the fly, split on bare '%'. */
    for (i = 0; i <= plen; )
    {
        if (i == plen)
        {
            /* End of pattern: flush current part if any. */
            if (in_part)
            {
                char *part_str             = pnstrdup(buf, buf_len);
                parsed->parts[part_count]          = part_str;
                parsed->part_byte_lens[part_count]  = buf_len;
                parsed->part_lens[part_count]       =
                    biscuit_part_char_count(part_str, buf_len);
                part_count++;
            }
            break;
        }

        if (pattern[i] == '\\' && i + 1 < plen)
        {
            char escaped = pattern[i + 1];
            /*
             * Backslash escapes any following character:
             *   '\%'  -> literal '%'  (not a wildcard separator)
             *   '\_'  -> literal '_'  (emit sentinel so matchers don't
             *                          treat it as a single-char wildcard)
             *   '\\'  -> literal '\'
             *   '\X'  -> literal 'X'  (for any other X, e.g. '\.')
             *
             * For '_' we store BISCUIT_LITERAL_ESC + '_' in the part
             * buffer so downstream matchers can distinguish literal '_'
             * from wildcard '_'.  All other escaped chars are stored as
             * their literal byte (they have no wildcard meaning anyway).
             */
            if (!in_part)
                in_part = true;
            if (escaped == '_')
            {
                buf[buf_len++] = BISCUIT_LITERAL_ESC;
                buf[buf_len++] = '_';
            }
            else
            {
                /* '%', '\\', '.', '-', etc. — just the literal byte */
                buf[buf_len++] = escaped;
            }
            i += 2;
        }
        else if (pattern[i] == '%')
        {
            /* Unescaped '%': wildcard separator — flush current part. */
            if (in_part)
            {
                char *part_str             = pnstrdup(buf, buf_len);
                parsed->parts[part_count]          = part_str;
                parsed->part_byte_lens[part_count]  = buf_len;
                parsed->part_lens[part_count]       =
                    biscuit_part_char_count(part_str, buf_len);
                part_count++;
                in_part = false;
                buf_len = 0;
            }
            i++;
        }
        else
        {
            /* Regular character (including unescaped '_'): copy verbatim.
             * Multi-byte UTF-8 sequences are copied whole. */
            int char_len = biscuit_utf8_char_length((unsigned char) pattern[i]);
            if (i + char_len > plen)
                char_len = plen - i;
            if (!in_part)
                in_part = true;
            memcpy(buf + buf_len, pattern + i, char_len);
            buf_len += char_len;
            i       += char_len;
        }
    }

    pfree(buf);
    parsed->part_count = part_count;
    return parsed;
}

void
biscuit_free_parsed_pattern(ParsedPattern *parsed)
{
    int i;

    if (!parsed)
        return;

    if (parsed->parts)
    {
        for (i = 0; i < parsed->part_count; i++)
        {
            if (parsed->parts[i])
            {
                pfree(parsed->parts[i]);
                parsed->parts[i] = NULL;
            }
        }
        pfree(parsed->parts);
    }
    if (parsed->part_lens)     pfree(parsed->part_lens);
    if (parsed->part_byte_lens) pfree(parsed->part_byte_lens);
    pfree(parsed);
}

/* ================================================================
 * SECTION 4 – Low-level part matching (single-column)
 * ================================================================ */

/*
 * Match part at a specific character position (from start) – case-sensitive.
 * Handles underscore (_) wildcards and multi-byte UTF-8 sequences.
 */
static RoaringBitmap *
biscuit_match_part_at_pos(BiscuitIndex *idx, const char *part,
                          int part_byte_len, int start_pos)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int            part_byte_pos  = 0;
    int            char_pos       = start_pos;
    int            concrete_chars = 0;
    bool           first_char     = true;

    while (part_byte_pos < part_byte_len)
    {
        unsigned char first_byte = (unsigned char) part[part_byte_pos];

        /* BISCUIT_LITERAL_ESC prefix: next byte is a literal character,
         * not a wildcard.  Consume both bytes and treat the second as a
         * concrete 1-byte character for bitmap lookup. */
        if (first_byte == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal_byte;
            RoaringBitmap *char_bm;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            literal_byte = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            char_bm = biscuit_get_pos_bitmap(idx, literal_byte, char_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2;
            char_pos++;
            continue;
        }

        if (first_byte == '_') { part_byte_pos++; char_pos++; continue; }

        {
        int char_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_len > part_byte_len)
            char_len = part_byte_len - part_byte_pos;

        concrete_chars++;

        if (char_len == 1)
        {
            RoaringBitmap *char_bm = biscuit_get_pos_bitmap(idx, first_byte, char_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
        }
        else
        {
            RoaringBitmap *multibyte = NULL;
            int b;
            for (b = 0; b < char_len; b++)
            {
                unsigned char bv = (unsigned char) part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_pos_bitmap(idx, bv, char_pos);
                if (!byte_bm) {
                    if (multibyte) biscuit_roaring_free(multibyte);
                    if (result)   biscuit_roaring_free(result);
                    return biscuit_roaring_create();
                }
                if (b == 0) multibyte = biscuit_roaring_copy(byte_bm);
                else { biscuit_roaring_and_inplace(multibyte, byte_bm); if (biscuit_roaring_is_empty(multibyte)) { biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } }
            }
            if (first_char) { result = multibyte; first_char = false; }
            else { biscuit_roaring_and_inplace(result, multibyte); biscuit_roaring_free(multibyte); if (biscuit_roaring_is_empty(result)) return result; }
        }

        part_byte_pos += char_len;
        char_pos++;
        } /* end char_len block */
    }

    {
        int pattern_char_count = biscuit_part_char_count(part, part_byte_len);
        if (concrete_chars == 0) {
            if (result) biscuit_roaring_free(result);
            result = biscuit_get_length_ge(idx, start_pos + pattern_char_count);
        } else {
            len_filter = biscuit_get_length_ge(idx, start_pos + pattern_char_count);
            if (len_filter) {
                if (result) biscuit_roaring_and_inplace(result, len_filter);
                else        { result = len_filter; len_filter = NULL; }
                if (len_filter) biscuit_roaring_free(len_filter);
            }
        }
    }

    return result ? result : biscuit_roaring_create();
}

/* Match part anchored at the end of string – case-sensitive. */
static RoaringBitmap *
biscuit_match_part_at_end(BiscuitIndex *idx, const char *part, int part_byte_len)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int            concrete_chars        = 0;
    bool           first_char            = true;
    int            pattern_char_count    = biscuit_part_char_count(part, part_byte_len);
    int            part_byte_pos         = 0;
    int            char_offset_from_end  = 0;

    while (part_byte_pos < part_byte_len)
    {
        unsigned char first_byte = (unsigned char) part[part_byte_pos];

        if (first_byte == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal_byte;
            RoaringBitmap *char_bm;
            int neg_pos;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            literal_byte = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            neg_pos = -(pattern_char_count - char_offset_from_end);
            char_bm = biscuit_get_neg_bitmap(idx, literal_byte, neg_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2;
            char_offset_from_end++;
            continue;
        }

        if (first_byte == '_') { part_byte_pos++; char_offset_from_end++; continue; }

        {
        int char_len = biscuit_utf8_char_length(first_byte);
        int neg_pos;
        if (part_byte_pos + char_len > part_byte_len)
            char_len = part_byte_len - part_byte_pos;

        concrete_chars++;
        neg_pos = -(pattern_char_count - char_offset_from_end);

        if (char_len == 1)
        {
            RoaringBitmap *char_bm = biscuit_get_neg_bitmap(idx, first_byte, neg_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
        }
        else
        {
            RoaringBitmap *multibyte = NULL;
            int b;
            for (b = 0; b < char_len; b++)
            {
                unsigned char bv = (unsigned char) part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_neg_bitmap(idx, bv, neg_pos);
                if (!byte_bm) { if (multibyte) biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
                if (b == 0) multibyte = biscuit_roaring_copy(byte_bm);
                else { biscuit_roaring_and_inplace(multibyte, byte_bm); if (biscuit_roaring_is_empty(multibyte)) { biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } }
            }
            if (first_char) { result = multibyte; first_char = false; }
            else { biscuit_roaring_and_inplace(result, multibyte); biscuit_roaring_free(multibyte); if (biscuit_roaring_is_empty(result)) return result; }
        }

        part_byte_pos += char_len;
        char_offset_from_end++;
        } /* end char_len block */
    }

    if (concrete_chars == 0) {
        if (result) biscuit_roaring_free(result);
        result = biscuit_get_length_ge(idx, pattern_char_count);
    } else {
        len_filter = biscuit_get_length_ge(idx, pattern_char_count);
        if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); }
    }

    return result ? result : biscuit_roaring_create();
}

/* Case-insensitive variants reuse the same logic via _lower accessors */
static RoaringBitmap *
biscuit_match_part_at_pos_ilike(BiscuitIndex *idx, const char *part,
                                int part_byte_len, int start_pos)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int            part_byte_pos  = 0;
    int            char_pos       = start_pos;
    int            concrete_chars = 0;
    bool           first_char     = true;

    while (part_byte_pos < part_byte_len)
    {
        unsigned char first_byte = (unsigned char) part[part_byte_pos];
        if (first_byte == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal_byte;
            RoaringBitmap *char_bm;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            literal_byte = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            char_bm = biscuit_get_pos_bitmap_lower(idx, literal_byte, char_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2;
            char_pos++;
            continue;
        }
        if (first_byte == '_') { part_byte_pos++; char_pos++; continue; }

        {
        int char_len = biscuit_utf8_char_length(first_byte);
        if (part_byte_pos + char_len > part_byte_len)
            char_len = part_byte_len - part_byte_pos;
        concrete_chars++;

        if (char_len == 1)
        {
            RoaringBitmap *char_bm = biscuit_get_pos_bitmap_lower(idx, first_byte, char_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
        }
        else
        {
            RoaringBitmap *multibyte = NULL; int b;
            for (b = 0; b < char_len; b++) {
                unsigned char bv = (unsigned char) part[part_byte_pos + b];
                RoaringBitmap *byte_bm = biscuit_get_pos_bitmap_lower(idx, bv, char_pos);
                if (!byte_bm) { if (multibyte) biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
                if (b == 0) multibyte = biscuit_roaring_copy(byte_bm);
                else { biscuit_roaring_and_inplace(multibyte, byte_bm); if (biscuit_roaring_is_empty(multibyte)) { biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } }
            }
            if (first_char) { result = multibyte; first_char = false; }
            else { biscuit_roaring_and_inplace(result, multibyte); biscuit_roaring_free(multibyte); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += char_len;
        char_pos++;
        } /* end char_len block */
    }

    {
        int pattern_char_count = biscuit_part_char_count(part, part_byte_len);
        if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_length_ge_lower(idx, start_pos + pattern_char_count); }
        else { len_filter = biscuit_get_length_ge_lower(idx, start_pos + pattern_char_count); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } }
    }

    return result ? result : biscuit_roaring_create();
}

static RoaringBitmap *
biscuit_match_part_at_end_ilike(BiscuitIndex *idx, const char *part, int part_byte_len)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int            concrete_chars       = 0;
    bool           first_char           = true;
    int            pattern_char_count   = biscuit_part_char_count(part, part_byte_len);
    int            part_byte_pos        = 0;
    int            char_offset_from_end = 0;

    while (part_byte_pos < part_byte_len)
    {
        unsigned char first_byte = (unsigned char) part[part_byte_pos];
        if (first_byte == (unsigned char) BISCUIT_LITERAL_ESC)
        {
            unsigned char literal_byte;
            RoaringBitmap *char_bm;
            int neg_pos;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            literal_byte = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            neg_pos = -(pattern_char_count - char_offset_from_end);
            char_bm = biscuit_get_neg_bitmap_lower(idx, literal_byte, neg_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2;
            char_offset_from_end++;
            continue;
        }
        if (first_byte == '_') { part_byte_pos++; char_offset_from_end++; continue; }
        {
        int char_len = biscuit_utf8_char_length(first_byte);
        int neg_pos;
        if (part_byte_pos + char_len > part_byte_len) char_len = part_byte_len - part_byte_pos;
        concrete_chars++;
        neg_pos = -(pattern_char_count - char_offset_from_end);
        if (char_len == 1) {
            RoaringBitmap *char_bm = biscuit_get_neg_bitmap_lower(idx, first_byte, neg_pos);
            if (!char_bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(char_bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, char_bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *multibyte = NULL; int b;
            for (b = 0; b < char_len; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *byte_bm = biscuit_get_neg_bitmap_lower(idx, bv, neg_pos); if (!byte_bm) { if (multibyte) biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) multibyte = biscuit_roaring_copy(byte_bm); else { biscuit_roaring_and_inplace(multibyte, byte_bm); if (biscuit_roaring_is_empty(multibyte)) { biscuit_roaring_free(multibyte); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = multibyte; first_char = false; }
            else { biscuit_roaring_and_inplace(result, multibyte); biscuit_roaring_free(multibyte); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += char_len;
        char_offset_from_end++;
        } /* end char_len block */
    }

    if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_length_ge_lower(idx, pattern_char_count); }
    else { len_filter = biscuit_get_length_ge_lower(idx, pattern_char_count); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } }

    return result ? result : biscuit_roaring_create();
}


/* ================================================================
 * SECTION 5 – Iterative windowed matching (single-column)
 *
 * Replaces the former recursive implementations to eliminate both
 * stack-overflow risk and the O(positions^parts) allocation pattern
 * that caused OOM crashes on long-text columns.
 *
 * Algorithm: explicit work-stack of (part_idx, min_pos, candidates)
 * frames.  Children are pushed instead of recursed into.  Each frame
 * owns its candidates bitmap and frees it when the frame is done.
 * This bounds live bitmap count to O(part_count * max_len) instead of
 * the former exponential.
 * ================================================================ */

typedef struct WMFrame {
    int            part_idx;
    int            min_pos;
    RoaringBitmap *candidates;
} WMFrame;

#define WM_MAX_STACK 512

static void
biscuit_recursive_windowed_match(
    RoaringBitmap *result, BiscuitIndex *idx,
    const char **parts, int *part_lens, int part_count,
    bool ends_percent, int part_idx, int min_pos,
    RoaringBitmap *current_candidates, int max_len)
{
    int i;
    WMFrame *stack     = (WMFrame *) palloc(WM_MAX_STACK * sizeof(WMFrame));
    int      stack_top = 0;
    int *pcl = (int *) palloc(part_count * sizeof(int));
    int *suf = (int *) palloc(part_count * sizeof(int));
    for (i = 0; i < part_count; i++)
        pcl[i] = biscuit_part_char_count(parts[i], part_lens[i]);
    suf[part_count - 1] = 0;
    for (i = part_count - 2; i >= 0; i--)
        suf[i] = suf[i + 1] + pcl[i + 1];

    stack[stack_top].part_idx   = part_idx;
    stack[stack_top].min_pos    = min_pos;
    stack[stack_top].candidates = biscuit_roaring_copy(current_candidates);
    stack_top++;

    while (stack_top > 0)
    {
        int            pidx;
        int            mpos;
        RoaringBitmap *cands;
        int            part_cl;
        int            suf_len;
        int            max_pos;
        stack_top--;
        pidx  = stack[stack_top].part_idx;
        mpos  = stack[stack_top].min_pos;
        cands = stack[stack_top].candidates;

        if (pidx >= part_count)
        {
            biscuit_roaring_or_inplace(result, cands);
            biscuit_roaring_free(cands);
            continue;
        }

        part_cl = pcl[pidx];
        suf_len = suf[pidx];
        max_pos = max_len - part_cl - suf_len;

        if (pidx == part_count - 1 && !ends_percent)
        {
            RoaringBitmap *em = biscuit_match_part_at_end(idx, parts[pidx], part_lens[pidx]);
            if (em)
            {
                int mrl;
                RoaringBitmap *lc;
                biscuit_roaring_and_inplace(em, cands);
                mrl = mpos + part_cl;
                lc = biscuit_get_length_ge(idx, mrl);
                if (lc) { biscuit_roaring_and_inplace(em, lc); biscuit_roaring_free(lc); }
                if (!biscuit_roaring_is_empty(em))
                    biscuit_roaring_or_inplace(result, em);
                biscuit_roaring_free(em);
            }
            biscuit_roaring_free(cands);
            continue;
        }

        if (mpos > max_pos) { biscuit_roaring_free(cands); continue; }

        for (i = mpos; i <= max_pos; i++)
        {
            int pos;
            RoaringBitmap *pm;
            RoaringBitmap *nc;
            pos = i;
            pm = biscuit_match_part_at_pos(idx, parts[pidx],
                                           part_lens[pidx], pos);
            if (!pm)
                continue;
            nc = biscuit_roaring_copy(cands);
            biscuit_roaring_and_inplace(nc, pm);
            biscuit_roaring_free(pm);
            if (biscuit_roaring_is_empty(nc))
            {
                biscuit_roaring_free(nc);
                continue;
            }
            if (stack_top >= WM_MAX_STACK)
            {
                biscuit_roaring_or_inplace(result, nc);
                biscuit_roaring_free(nc);
                continue;
            }
            stack[stack_top].part_idx   = pidx + 1;
            stack[stack_top].min_pos    = pos + part_cl;
            stack[stack_top].candidates = nc;
            stack_top++;
        }
        biscuit_roaring_free(cands);
    }

    pfree(stack);
    pfree(pcl);
    pfree(suf);
}

static void
biscuit_recursive_windowed_match_ilike(
    RoaringBitmap *result, BiscuitIndex *idx,
    const char **parts, int *part_lens, int part_count,
    bool ends_percent, int part_idx, int min_pos,
    RoaringBitmap *current_candidates, int max_len)
{
    int i;
    WMFrame *stack     = (WMFrame *) palloc(WM_MAX_STACK * sizeof(WMFrame));
    int      stack_top = 0;
    int *pcl = (int *) palloc(part_count * sizeof(int));
    int *suf = (int *) palloc(part_count * sizeof(int));
    for (i = 0; i < part_count; i++)
        pcl[i] = biscuit_part_char_count(parts[i], part_lens[i]);
    suf[part_count - 1] = 0;
    for (i = part_count - 2; i >= 0; i--)
        suf[i] = suf[i + 1] + pcl[i + 1];

    stack[stack_top].part_idx   = part_idx;
    stack[stack_top].min_pos    = min_pos;
    stack[stack_top].candidates = biscuit_roaring_copy(current_candidates);
    stack_top++;

    while (stack_top > 0)
    {
        int            pidx;
        int            mpos;
        RoaringBitmap *cands;
        int            part_cl;
        int            suf_len;
        int            max_pos;
        stack_top--;
        pidx  = stack[stack_top].part_idx;
        mpos  = stack[stack_top].min_pos;
        cands = stack[stack_top].candidates;

        if (pidx >= part_count)
        {
            biscuit_roaring_or_inplace(result, cands);
            biscuit_roaring_free(cands);
            continue;
        }

        part_cl = pcl[pidx];
        suf_len = suf[pidx];
        max_pos = max_len - part_cl - suf_len;

        if (pidx == part_count - 1 && !ends_percent)
        {
            RoaringBitmap *em = biscuit_match_part_at_end_ilike(idx, parts[pidx], part_lens[pidx]);
            if (em)
            {
                int mrl;
                RoaringBitmap *lc;
                biscuit_roaring_and_inplace(em, cands);
                mrl = mpos + part_cl;
                lc = biscuit_get_length_ge_lower(idx, mrl);
                if (lc) { biscuit_roaring_and_inplace(em, lc); biscuit_roaring_free(lc); }
                if (!biscuit_roaring_is_empty(em))
                    biscuit_roaring_or_inplace(result, em);
                biscuit_roaring_free(em);
            }
            biscuit_roaring_free(cands);
            continue;
        }

        if (mpos > max_pos) { biscuit_roaring_free(cands); continue; }

        for (i = mpos; i <= max_pos; i++)
        {
            int pos;
            RoaringBitmap *pm;
            RoaringBitmap *nc;
            pos = i;
            pm = biscuit_match_part_at_pos_ilike(idx, parts[pidx], part_lens[pidx], pos);
            if (!pm) continue;
            nc = biscuit_roaring_copy(cands);
            biscuit_roaring_and_inplace(nc, pm);
            biscuit_roaring_free(pm);
            if (biscuit_roaring_is_empty(nc)) { biscuit_roaring_free(nc); continue; }
            if (stack_top >= WM_MAX_STACK)
            {
                biscuit_roaring_or_inplace(result, nc);
                biscuit_roaring_free(nc);
                continue;
            }
            stack[stack_top].part_idx   = pidx + 1;
            stack[stack_top].min_pos    = pos + part_cl;
            stack[stack_top].candidates = nc;
            stack_top++;
        }
        biscuit_roaring_free(cands);
    }

    pfree(stack);
    pfree(pcl);
    pfree(suf);
}

/* ================================================================
 * SECTION 6 – Multi-column part matching helpers
 * ================================================================ */

static RoaringBitmap *
biscuit_match_col_part_at_pos(ColumnIndex *col, const char *part,
                              int part_byte_len, int start_pos)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int part_byte_pos = 0, char_pos = start_pos, concrete_chars = 0;
    bool first_char = true;

    while (part_byte_pos < part_byte_len) {
        unsigned char fb = (unsigned char) part[part_byte_pos];
        if (fb == (unsigned char) BISCUIT_LITERAL_ESC) {
            unsigned char lb; RoaringBitmap *bm;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            lb = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            bm = biscuit_get_col_pos_bitmap(col, lb, char_pos);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2; char_pos++; continue;
        }
        if (fb == '_') { part_byte_pos++; char_pos++; continue; }
        {
        int cl = biscuit_utf8_char_length(fb);
        if (part_byte_pos + cl > part_byte_len) cl = part_byte_len - part_byte_pos;
        concrete_chars++;
        if (cl == 1) {
            RoaringBitmap *bm = biscuit_get_col_pos_bitmap(col, fb, char_pos);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *mb = NULL; int b;
            for (b = 0; b < cl; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *bm = biscuit_get_col_pos_bitmap(col, bv, char_pos); if (!bm) { if (mb) biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) mb = biscuit_roaring_copy(bm); else { biscuit_roaring_and_inplace(mb, bm); if (biscuit_roaring_is_empty(mb)) { biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = mb; first_char = false; } else { biscuit_roaring_and_inplace(result, mb); biscuit_roaring_free(mb); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += cl; char_pos++;
        } /* end cl block */
    }

    { int pcc = biscuit_part_char_count(part, part_byte_len);
      if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_col_length_ge(col, start_pos + pcc); }
      else { len_filter = biscuit_get_col_length_ge(col, start_pos + pcc); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } } }
    return result ? result : biscuit_roaring_create();
}

static RoaringBitmap *
biscuit_match_col_part_at_end(ColumnIndex *col, const char *part, int part_byte_len)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int concrete_chars = 0, pcc = biscuit_part_char_count(part, part_byte_len);
    int part_byte_pos = 0, cfe = 0;
    bool first_char = true;

    while (part_byte_pos < part_byte_len) {
        unsigned char fb = (unsigned char) part[part_byte_pos];
        if (fb == (unsigned char) BISCUIT_LITERAL_ESC) {
            unsigned char lb; RoaringBitmap *bm; int neg;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            lb = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            neg = -(pcc - cfe);
            bm = biscuit_get_col_neg_bitmap(col, lb, neg);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2; cfe++; continue;
        }
        if (fb == '_') { part_byte_pos++; cfe++; continue; }
        {
        int cl = biscuit_utf8_char_length(fb);
        int neg;
        if (part_byte_pos + cl > part_byte_len) cl = part_byte_len - part_byte_pos;
        concrete_chars++;
        neg = -(pcc - cfe);
        if (cl == 1) {
            RoaringBitmap *bm = biscuit_get_col_neg_bitmap(col, fb, neg);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *mb = NULL; int b;
            for (b = 0; b < cl; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *bm = biscuit_get_col_neg_bitmap(col, bv, neg); if (!bm) { if (mb) biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) mb = biscuit_roaring_copy(bm); else { biscuit_roaring_and_inplace(mb, bm); if (biscuit_roaring_is_empty(mb)) { biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = mb; first_char = false; } else { biscuit_roaring_and_inplace(result, mb); biscuit_roaring_free(mb); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += cl; cfe++;
        } /* end cl block */
    }

    if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_col_length_ge(col, pcc); }
    else { len_filter = biscuit_get_col_length_ge(col, pcc); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } }
    return result ? result : biscuit_roaring_create();
}

/* Case-insensitive multi-column variants */
static RoaringBitmap *
biscuit_match_col_part_at_pos_ilike(ColumnIndex *col, const char *part,
                                    int part_byte_len, int start_pos)
{
    /* Identical to biscuit_match_col_part_at_pos but uses _lower accessors */
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int part_byte_pos = 0, char_pos = start_pos, concrete_chars = 0;
    bool first_char = true;

    while (part_byte_pos < part_byte_len) {
        unsigned char fb = (unsigned char) part[part_byte_pos];
        if (fb == (unsigned char) BISCUIT_LITERAL_ESC) {
            unsigned char lb; RoaringBitmap *bm;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            lb = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            bm = biscuit_get_col_pos_bitmap_lower(col, lb, char_pos);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2; char_pos++; continue;
        }
        if (fb == '_') { part_byte_pos++; char_pos++; continue; }
        {
        int cl = biscuit_utf8_char_length(fb);
        if (part_byte_pos + cl > part_byte_len) cl = part_byte_len - part_byte_pos;
        concrete_chars++;
        if (cl == 1) {
            RoaringBitmap *bm = biscuit_get_col_pos_bitmap_lower(col, fb, char_pos);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *mb = NULL; int b;
            for (b = 0; b < cl; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *bm = biscuit_get_col_pos_bitmap_lower(col, bv, char_pos); if (!bm) { if (mb) biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) mb = biscuit_roaring_copy(bm); else { biscuit_roaring_and_inplace(mb, bm); if (biscuit_roaring_is_empty(mb)) { biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = mb; first_char = false; } else { biscuit_roaring_and_inplace(result, mb); biscuit_roaring_free(mb); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += cl; char_pos++;
        } /* end cl block */
    }

    { int pcc = biscuit_part_char_count(part, part_byte_len);
      if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_col_length_ge_lower(col, start_pos + pcc); }
      else { len_filter = biscuit_get_col_length_ge_lower(col, start_pos + pcc); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } } }
    return result ? result : biscuit_roaring_create();
}

static RoaringBitmap *
biscuit_match_col_part_at_end_ilike(ColumnIndex *col, const char *part, int part_byte_len)
{
    RoaringBitmap *result = NULL;
    RoaringBitmap *len_filter;
    int concrete_chars = 0, pcc = biscuit_part_char_count(part, part_byte_len);
    int part_byte_pos = 0, cfe = 0;
    bool first_char = true;

    while (part_byte_pos < part_byte_len) {
        unsigned char fb = (unsigned char) part[part_byte_pos];
        if (fb == (unsigned char) BISCUIT_LITERAL_ESC) {
            unsigned char lb; RoaringBitmap *bm; int neg;
            if (part_byte_pos + 1 >= part_byte_len) { part_byte_pos++; continue; }
            lb = (unsigned char) part[part_byte_pos + 1];
            concrete_chars++;
            neg = -(pcc - cfe);
            bm = biscuit_get_col_neg_bitmap_lower(col, lb, neg);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
            part_byte_pos += 2; cfe++; continue;
        }
        if (fb == '_') { part_byte_pos++; cfe++; continue; }
        {
        int cl = biscuit_utf8_char_length(fb);
        int neg;
        if (part_byte_pos + cl > part_byte_len) cl = part_byte_len - part_byte_pos;
        concrete_chars++;
        neg = -(pcc - cfe);
        if (cl == 1) {
            RoaringBitmap *bm = biscuit_get_col_neg_bitmap_lower(col, fb, neg);
            if (!bm) { if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); }
            if (first_char) { result = biscuit_roaring_copy(bm); first_char = false; }
            else { biscuit_roaring_and_inplace(result, bm); if (biscuit_roaring_is_empty(result)) return result; }
        } else {
            RoaringBitmap *mb = NULL; int b;
            for (b = 0; b < cl; b++) { unsigned char bv = (unsigned char) part[part_byte_pos + b]; RoaringBitmap *bm = biscuit_get_col_neg_bitmap_lower(col, bv, neg); if (!bm) { if (mb) biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } if (b == 0) mb = biscuit_roaring_copy(bm); else { biscuit_roaring_and_inplace(mb, bm); if (biscuit_roaring_is_empty(mb)) { biscuit_roaring_free(mb); if (result) biscuit_roaring_free(result); return biscuit_roaring_create(); } } }
            if (first_char) { result = mb; first_char = false; } else { biscuit_roaring_and_inplace(result, mb); biscuit_roaring_free(mb); if (biscuit_roaring_is_empty(result)) return result; }
        }
        part_byte_pos += cl; cfe++;
        } /* end cl block */
    }

    if (concrete_chars == 0) { if (result) biscuit_roaring_free(result); result = biscuit_get_col_length_ge_lower(col, pcc); }
    else { len_filter = biscuit_get_col_length_ge_lower(col, pcc); if (len_filter) { if (result) biscuit_roaring_and_inplace(result, len_filter); else { result = len_filter; len_filter = NULL; } if (len_filter) biscuit_roaring_free(len_filter); } }
    return result ? result : biscuit_roaring_create();
}


/* ================================================================
 * SECTION 7 – Iterative windowed matching (multi-column)
 *
 * Same stack-based approach as Section 5 but operating on a
 * ColumnIndex pointer and using the col-specific bitmap accessors.
 * ================================================================ */

static void
biscuit_recursive_windowed_match_col(
    RoaringBitmap *result, ColumnIndex *col,
    const char **parts, int *part_byte_lens, int part_count,
    bool ends_percent, int part_idx, int min_char_pos,
    RoaringBitmap *current_candidates, int max_char_len)
{
    int i;
    WMFrame *stack     = (WMFrame *) palloc(WM_MAX_STACK * sizeof(WMFrame));
    int      stack_top = 0;
    int *pcl = (int *) palloc(part_count * sizeof(int));
    int *suf = (int *) palloc(part_count * sizeof(int));
    for (i = 0; i < part_count; i++)
        pcl[i] = biscuit_part_char_count(parts[i], part_byte_lens[i]);
    suf[part_count - 1] = 0;
    for (i = part_count - 2; i >= 0; i--)
        suf[i] = suf[i + 1] + pcl[i + 1];

    stack[stack_top].part_idx   = part_idx;
    stack[stack_top].min_pos    = min_char_pos;
    stack[stack_top].candidates = biscuit_roaring_copy(current_candidates);
    stack_top++;

    while (stack_top > 0)
    {
        int            pidx;
        int            mpos;
        RoaringBitmap *cands;
        int            part_cl;
        int            suf_len;
        int            max_pos;
        stack_top--;
        pidx  = stack[stack_top].part_idx;
        mpos  = stack[stack_top].min_pos;
        cands = stack[stack_top].candidates;

        if (pidx >= part_count)
        {
            biscuit_roaring_or_inplace(result, cands);
            biscuit_roaring_free(cands);
            continue;
        }

        part_cl = pcl[pidx];
        suf_len = suf[pidx];
        max_pos = max_char_len - part_cl - suf_len;

        if (pidx == part_count - 1 && !ends_percent)
        {
            RoaringBitmap *em = biscuit_match_col_part_at_end(col, parts[pidx], part_byte_lens[pidx]);
            if (em)
            {
                int mrl;
                RoaringBitmap *lc;
                biscuit_roaring_and_inplace(em, cands);
                mrl = mpos + part_cl;
                lc = biscuit_get_col_length_ge(col, mrl);
                if (lc) { biscuit_roaring_and_inplace(em, lc); biscuit_roaring_free(lc); }
                if (!biscuit_roaring_is_empty(em))
                    biscuit_roaring_or_inplace(result, em);
                biscuit_roaring_free(em);
            }
            biscuit_roaring_free(cands);
            continue;
        }

        if (mpos > max_pos) { biscuit_roaring_free(cands); continue; }

        for (i = mpos; i <= max_pos; i++)
        {
            int pos;
            RoaringBitmap *pm;
            RoaringBitmap *nc;
            pos = i;
            pm = biscuit_match_col_part_at_pos(col, parts[pidx], part_byte_lens[pidx], pos);
            if (!pm) continue;
            nc = biscuit_roaring_copy(cands);
            biscuit_roaring_and_inplace(nc, pm);
            biscuit_roaring_free(pm);
            if (biscuit_roaring_is_empty(nc)) { biscuit_roaring_free(nc); continue; }
            if (stack_top >= WM_MAX_STACK)
            {
                biscuit_roaring_or_inplace(result, nc);
                biscuit_roaring_free(nc);
                continue;
            }
            stack[stack_top].part_idx   = pidx + 1;
            stack[stack_top].min_pos    = pos + part_cl;
            stack[stack_top].candidates = nc;
            stack_top++;
        }
        biscuit_roaring_free(cands);
    }

    pfree(stack);
    pfree(pcl);
    pfree(suf);
}

static void
biscuit_recursive_windowed_match_col_ilike(
    RoaringBitmap *result, ColumnIndex *col,
    const char **parts, int *part_byte_lens, int part_count,
    bool ends_percent, int part_idx, int min_char_pos,
    RoaringBitmap *current_candidates, int max_char_len)
{
    int i;
    WMFrame *stack     = (WMFrame *) palloc(WM_MAX_STACK * sizeof(WMFrame));
    int      stack_top = 0;
    int *pcl = (int *) palloc(part_count * sizeof(int));
    int *suf = (int *) palloc(part_count * sizeof(int));
    for (i = 0; i < part_count; i++)
        pcl[i] = biscuit_part_char_count(parts[i], part_byte_lens[i]);
    suf[part_count - 1] = 0;
    for (i = part_count - 2; i >= 0; i--)
        suf[i] = suf[i + 1] + pcl[i + 1];

    stack[stack_top].part_idx   = part_idx;
    stack[stack_top].min_pos    = min_char_pos;
    stack[stack_top].candidates = biscuit_roaring_copy(current_candidates);
    stack_top++;

    while (stack_top > 0)
    {
        int            pidx;
        int            mpos;
        RoaringBitmap *cands;
        int            part_cl;
        int            suf_len;
        int            max_pos;
        stack_top--;
        pidx  = stack[stack_top].part_idx;
        mpos  = stack[stack_top].min_pos;
        cands = stack[stack_top].candidates;

        if (pidx >= part_count)
        {
            biscuit_roaring_or_inplace(result, cands);
            biscuit_roaring_free(cands);
            continue;
        }

        part_cl = pcl[pidx];
        suf_len = suf[pidx];
        max_pos = max_char_len - part_cl - suf_len;

        if (pidx == part_count - 1 && !ends_percent)
        {
            RoaringBitmap *em = biscuit_match_col_part_at_end_ilike(col, parts[pidx], part_byte_lens[pidx]);
            if (em)
            {
                int mrl;
                RoaringBitmap *lc;
                biscuit_roaring_and_inplace(em, cands);
                mrl = mpos + part_cl;
                lc = biscuit_get_col_length_ge_lower(col, mrl);
                if (lc) { biscuit_roaring_and_inplace(em, lc); biscuit_roaring_free(lc); }
                if (!biscuit_roaring_is_empty(em))
                    biscuit_roaring_or_inplace(result, em);
                biscuit_roaring_free(em);
            }
            biscuit_roaring_free(cands);
            continue;
        }

        if (mpos > max_pos) { biscuit_roaring_free(cands); continue; }

        for (i = mpos; i <= max_pos; i++)
        {
            int pos;
            RoaringBitmap *pm;
            RoaringBitmap *nc;
            pos = i;
            pm = biscuit_match_col_part_at_pos_ilike(col, parts[pidx], part_byte_lens[pidx], pos);
            if (!pm) continue;
            nc = biscuit_roaring_copy(cands);
            biscuit_roaring_and_inplace(nc, pm);
            biscuit_roaring_free(pm);
            if (biscuit_roaring_is_empty(nc)) { biscuit_roaring_free(nc); continue; }
            if (stack_top >= WM_MAX_STACK)
            {
                biscuit_roaring_or_inplace(result, nc);
                biscuit_roaring_free(nc);
                continue;
            }
            stack[stack_top].part_idx   = pidx + 1;
            stack[stack_top].min_pos    = pos + part_cl;
            stack[stack_top].candidates = nc;
            stack_top++;
        }
        biscuit_roaring_free(cands);
    }

    pfree(stack);
    pfree(pcl);
    pfree(suf);
}

/* ================================================================
 * SECTION 8 – Public query entry points (single-column)
 * ================================================================ */

RoaringBitmap *
biscuit_query_pattern(BiscuitIndex *idx, const char *pattern)
{
    int            plen = strlen(pattern);
    ParsedPattern *parsed = NULL;
    int            min_len, i;
    RoaringBitmap *result = NULL;
    int            wildcard_count = 0, percent_count = 0;
    bool           only_wildcards = true;

    if (plen == 0) {
        /* Empty pattern '' matches only records with an empty string value */
        if (idx->length_bitmaps_legacy && idx->length_bitmaps_legacy[0])
            return biscuit_roaring_copy(idx->length_bitmaps_legacy[0]);
        return biscuit_roaring_create();
    }

    if (plen == 1 && pattern[0] == '%') {
        /* '%' matches every non-tombstoned, non-NULL record */
        if (idx->length_ge_bitmaps_legacy && idx->length_ge_bitmaps_legacy[0])
            result = biscuit_roaring_copy(idx->length_ge_bitmaps_legacy[0]);
        else {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                bool ts = false;
                #ifdef HAVE_ROARING
                ts = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
                #else
                { uint32_t bl = i >> 6, bt = i & 63; ts = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
                #endif
                if (!ts && idx->data_cache[i]) biscuit_roaring_add(result, i);
            }
        }
        return result;
    }

    for (i = 0; i < plen; i++) {
        if (pattern[i] == '%') percent_count++;
        else if (pattern[i] == '_') wildcard_count++;
        else { only_wildcards = false; break; }
    }

    if (only_wildcards) {
        if (percent_count > 0) return biscuit_get_length_ge(idx, wildcard_count);
        if (wildcard_count < idx->max_length_legacy && idx->length_bitmaps_legacy[wildcard_count])
            return biscuit_roaring_copy(idx->length_bitmaps_legacy[wildcard_count]);
        return biscuit_roaring_create();
    }

    PG_TRY();
    {
        parsed = biscuit_parse_pattern(pattern);

        if (parsed->part_count == 0) {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                bool ts = false;
                #ifdef HAVE_ROARING
                ts = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
                #else
                { uint32_t bl = i >> 6, bt = i & 63; ts = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
                #endif
                if (!ts) biscuit_roaring_add(result, i);
            }
            biscuit_free_parsed_pattern(parsed);
            parsed = NULL; /* FIX 5: prevent double-free in PG_CATCH */
            return result;
        }

        min_len = 0;
        for (i = 0; i < parsed->part_count; i++)
            min_len += parsed->part_lens[i];

        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                result = biscuit_match_part_at_pos(idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (result && min_len < idx->max_length_legacy && idx->length_bitmaps_legacy[min_len])
                    biscuit_roaring_and_inplace(result, idx->length_bitmaps_legacy[min_len]);
                else if (!result || min_len >= idx->max_length_legacy) { if (result) biscuit_roaring_free(result); result = biscuit_roaring_create(); }
            } else if (!parsed->starts_percent) {
                result = biscuit_match_part_at_pos(idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                result = biscuit_match_part_at_end(idx, parsed->parts[0], parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                /* '%abc%' – substring via char-cache and brute verification */
                result = biscuit_roaring_create();
                if (parsed->part_byte_lens[0] > 0) {
                    unsigned char first_byte = biscuit_part_seed_byte(
                        parsed->parts[0], parsed->part_byte_lens[0]);
                    if (first_byte != 0) {
                        RoaringBitmap *candidates = idx->char_cache_legacy[first_byte]
                            ? biscuit_roaring_copy(idx->char_cache_legacy[first_byte])
                            : biscuit_roaring_create();
                        int part_char_len = parsed->part_lens[0];
                        RoaringBitmap *lf = biscuit_get_length_ge(idx, part_char_len);
                        if (lf) { biscuit_roaring_and_inplace(candidates, lf); biscuit_roaring_free(lf); }

                        #ifdef HAVE_ROARING
                        {
                            roaring_uint32_iterator_t *iter = roaring_iterator_create(candidates);
                            while (iter->has_value) {
                                uint32_t rec = iter->current_value;
                                if (rec < (uint32_t) idx->num_records && idx->data_cache[rec]) {
                                    const char *hay = idx->data_cache[rec];
                                    int hbl = strlen(hay), hcl = biscuit_utf8_char_count(hay, hbl);
                                    bool found = false;
                                    for (int cp = 0; cp <= hcl - part_char_len && !found; cp++) {
                                        int bo = biscuit_utf8_char_to_byte_offset(hay, hbl, cp);
                                        if (bo < 0) continue;
                                        if (biscuit_part_match_substr(hay, hbl, bo,
                                                parsed->parts[0], parsed->part_byte_lens[0]))
                                            found = true;
                                    }
                                    if (found) biscuit_roaring_add(result, rec);
                                }
                                roaring_uint32_iterator_advance(iter);
                            }
                            roaring_uint32_iterator_free(iter);
                        }
                        #else
                        {
                            uint64_t cnt;
                            uint32_t *indices = biscuit_roaring_to_array(candidates, &cnt);
                            if (indices) {
                                for (int j = 0; j < (int) cnt; j++) {
                                    uint32_t rec = indices[j];
                                    if (rec < (uint32_t) idx->num_records && idx->data_cache[rec]) {
                                        const char *hay = idx->data_cache[rec];
                                        int hbl = strlen(hay), hcl = biscuit_utf8_char_count(hay, hbl);
                                        bool found = false;
                                        for (int cp = 0; cp <= hcl - part_char_len && !found; cp++) {
                                            int bo = biscuit_utf8_char_to_byte_offset(hay, hbl, cp);
                                            if (bo < 0) continue;
                                            if (biscuit_part_match_substr(hay, hbl, bo,
                                                    parsed->parts[0], parsed->part_byte_lens[0]))
                                                found = true;
                                        }
                                        if (found) biscuit_roaring_add(result, rec);
                                    }
                                }
                                pfree(indices);
                            }
                        }
                        #endif
                        biscuit_roaring_free(candidates);
                    }
                }
            }
        } else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            RoaringBitmap *prefix = biscuit_match_part_at_pos(idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
            RoaringBitmap *suffix = biscuit_match_part_at_end(idx, parsed->parts[1], parsed->part_byte_lens[1]);
            if (!prefix || !suffix) { if (prefix) biscuit_roaring_free(prefix); if (suffix) biscuit_roaring_free(suffix); result = biscuit_roaring_create(); }
            else {
                RoaringBitmap *lf;
                biscuit_roaring_and_inplace(prefix, suffix); biscuit_roaring_free(suffix);
                lf = biscuit_get_length_ge(idx, min_len);
                if (lf) { biscuit_roaring_and_inplace(prefix, lf); biscuit_roaring_free(lf); }
                result = prefix;
            }
        } else {
            RoaringBitmap *candidates;
            result = biscuit_roaring_create();
            candidates = biscuit_get_length_ge(idx, min_len);
            if (candidates && !biscuit_roaring_is_empty(candidates)) {
                if (!parsed->starts_percent) {
                    RoaringBitmap *first = biscuit_match_part_at_pos(idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                    if (first) { biscuit_roaring_and_inplace(first, candidates); biscuit_roaring_free(candidates); candidates = first; }
                }
                if (!biscuit_roaring_is_empty(candidates)) {
                    biscuit_recursive_windowed_match(result, idx,
                        (const char **) parsed->parts, parsed->part_byte_lens, parsed->part_count,
                        parsed->ends_percent, 0, 0, candidates, idx->max_len);
                }
                biscuit_roaring_free(candidates);
            } else if (candidates) biscuit_roaring_free(candidates);
        }

        biscuit_free_parsed_pattern(parsed);
        parsed = NULL; /* FIX 5: prevent double-free in PG_CATCH */
    }
    PG_CATCH();
    {
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return result ? result : biscuit_roaring_create();
}

/* ILIKE single-column: lower-case the pattern first, then reuse logic */
RoaringBitmap *
biscuit_query_pattern_ilike(BiscuitIndex *idx, const char *pattern)
{
    int            plen = strlen(pattern);
    char          *pl   = biscuit_str_tolower(pattern, plen);
    RoaringBitmap *result;

    /* delegate with lowercased pattern, using _lower accessors implicitly
       via biscuit_match_part_at_pos_ilike / _end_ilike / get_length_ge_lower */

    ParsedPattern *parsed = NULL;
    int            min_len, i;
    int            wildcard_count = 0, percent_count = 0;
    bool           only_wildcards = true;

    plen = strlen(pl);

    if (plen == 0) {
        if (idx->length_bitmaps_legacy && idx->length_bitmaps_legacy[0])
            result = biscuit_roaring_copy(idx->length_bitmaps_legacy[0]);
        else result = biscuit_roaring_create();
        pfree(pl); return result;
    }

    if (plen == 1 && pl[0] == '%') {
        /* '%' matches every non-tombstoned, non-NULL record */
        if (idx->length_ge_bitmaps_lower && idx->length_ge_bitmaps_lower[0])
            result = biscuit_roaring_copy(idx->length_ge_bitmaps_lower[0]);
        else {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                bool ts = false;
#ifdef HAVE_ROARING
                ts = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
#else
                { uint32_t bl = i >> 6, bt = i & 63; ts = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
#endif
                if (!ts && idx->data_cache_lower && idx->data_cache_lower[i]) biscuit_roaring_add(result, i);
            }
        }
        pfree(pl); return result;
    }

    for (i = 0; i < plen; i++) {
        if (pl[i] == '%') percent_count++;
        else if (pl[i] == '_') wildcard_count++;
        else { only_wildcards = false; break; }
    }

    if (only_wildcards) {
        if (percent_count > 0) result = biscuit_get_length_ge_lower(idx, wildcard_count);
        else if (wildcard_count < idx->max_length_lower && idx->length_bitmaps_lower && idx->length_bitmaps_lower[wildcard_count])
            result = biscuit_roaring_copy(idx->length_bitmaps_lower[wildcard_count]);
        else result = biscuit_roaring_create();
        pfree(pl); return result;
    }

    PG_TRY();
    {
        parsed = biscuit_parse_pattern(pl);
        if (parsed->part_count == 0) {
            result = biscuit_roaring_create();
            for (i = 0; i < idx->num_records; i++) {
                bool ts = false;
#ifdef HAVE_ROARING
                ts = roaring_bitmap_contains(idx->tombstones, (uint32_t) i);
#else
                { uint32_t bl = i >> 6, bt = i & 63; ts = ((int) bl < idx->tombstones->num_blocks && (idx->tombstones->blocks[bl] & (1ULL << bt))); }
#endif
                if (!ts) biscuit_roaring_add(result, i);
            }
            biscuit_free_parsed_pattern(parsed); parsed = NULL; /* FIX 5 */ pfree(pl); return result;
        }
        min_len = 0;
        for (i = 0; i < parsed->part_count; i++) min_len += parsed->part_lens[i];

        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                result = biscuit_match_part_at_pos_ilike(idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (result && min_len < idx->max_length_lower && idx->length_bitmaps_lower && idx->length_bitmaps_lower[min_len])
                    biscuit_roaring_and_inplace(result, idx->length_bitmaps_lower[min_len]);
                else if (!result || min_len >= idx->max_length_lower) { if (result) biscuit_roaring_free(result); result = biscuit_roaring_create(); }
            } else if (!parsed->starts_percent) {
                result = biscuit_match_part_at_pos_ilike(idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                result = biscuit_match_part_at_end_ilike(idx, parsed->parts[0], parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                result = biscuit_roaring_create();
                /* substring ILIKE – similar brute verification via lowercase cache */
                if (parsed->part_byte_lens[0] > 0) {
                    unsigned char first_byte = biscuit_part_seed_byte(
                        parsed->parts[0], parsed->part_byte_lens[0]);
                    RoaringBitmap *candidates = (first_byte != 0 && idx->char_cache_lower[first_byte])
                        ? biscuit_roaring_copy(idx->char_cache_lower[first_byte])
                        : biscuit_roaring_create();
                    int pcl = parsed->part_lens[0];
                    RoaringBitmap *lf = biscuit_get_length_ge_lower(idx, pcl);
                    if (lf) { biscuit_roaring_and_inplace(candidates, lf); biscuit_roaring_free(lf); }
                    #ifdef HAVE_ROARING
                    { roaring_uint32_iterator_t *iter = roaring_iterator_create(candidates);
                      while (iter->has_value) { uint32_t rec = iter->current_value;
                        if (rec < (uint32_t) idx->num_records && idx->data_cache_lower && idx->data_cache_lower[rec]) {
                            const char *hay = idx->data_cache_lower[rec];
                            int hbl = strlen(hay), hcl = biscuit_utf8_char_count(hay, hbl);
                            bool found = false;
                            for (int cp = 0; cp <= hcl - pcl && !found; cp++) {
                                int bo = biscuit_utf8_char_to_byte_offset(hay, hbl, cp);
                                if (bo >= 0 && biscuit_part_match_substr(hay, hbl, bo,
                                        parsed->parts[0], parsed->part_byte_lens[0]))
                                    found = true;
                            }
                            if (found) biscuit_roaring_add(result, rec);
                        }
                        roaring_uint32_iterator_advance(iter); }
                      roaring_uint32_iterator_free(iter); }
                    #else
                    { uint64_t cnt; uint32_t *indices = biscuit_roaring_to_array(candidates, &cnt);
                      if (indices) { for (int j = 0; j < (int) cnt; j++) { uint32_t rec = indices[j];
                            if (rec < (uint32_t) idx->num_records && idx->data_cache_lower && idx->data_cache_lower[rec]) {
                                const char *hay = idx->data_cache_lower[rec];
                                int hbl = strlen(hay), hcl = biscuit_utf8_char_count(hay, hbl);
                                bool found = false;
                                for (int cp = 0; cp <= hcl - pcl && !found; cp++) {
                                    int bo = biscuit_utf8_char_to_byte_offset(hay, hbl, cp);
                                    if (bo >= 0 && biscuit_part_match_substr(hay, hbl, bo,
                                            parsed->parts[0], parsed->part_byte_lens[0]))
                                        found = true;
                                }
                                if (found) biscuit_roaring_add(result, rec); } }
                          pfree(indices); } }
                    #endif
                    biscuit_roaring_free(candidates);
                }
            }
        } else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            RoaringBitmap *prefix = biscuit_match_part_at_pos_ilike(idx, parsed->parts[0], parsed->part_byte_lens[0], 0);
            RoaringBitmap *suffix = biscuit_match_part_at_end_ilike(idx, parsed->parts[1], parsed->part_byte_lens[1]);
            if (!prefix || !suffix) { if (prefix) biscuit_roaring_free(prefix); if (suffix) biscuit_roaring_free(suffix); result = biscuit_roaring_create(); }
            else { RoaringBitmap *lf;
                   biscuit_roaring_and_inplace(prefix, suffix); biscuit_roaring_free(suffix);
                   lf = biscuit_get_length_ge_lower(idx, min_len);
                   if (lf) { biscuit_roaring_and_inplace(prefix, lf); biscuit_roaring_free(lf); }
                   result = prefix; }
        } else {
            RoaringBitmap *candidates;
            result = biscuit_roaring_create();
            candidates = biscuit_get_length_ge_lower(idx, min_len);
            if (candidates && !biscuit_roaring_is_empty(candidates)) {
                if (!parsed->starts_percent) { RoaringBitmap *first = biscuit_match_part_at_pos_ilike(idx, parsed->parts[0], parsed->part_byte_lens[0], 0); if (first) { biscuit_roaring_and_inplace(first, candidates); biscuit_roaring_free(candidates); candidates = first; } }
                if (!biscuit_roaring_is_empty(candidates))
                    biscuit_recursive_windowed_match_ilike(result, idx, (const char **) parsed->parts, parsed->part_byte_lens, parsed->part_count, parsed->ends_percent, 0, 0, candidates, idx->max_length_lower);
                biscuit_roaring_free(candidates);
            } else if (candidates) biscuit_roaring_free(candidates);
        }

        biscuit_free_parsed_pattern(parsed);
        parsed = NULL; /* FIX 5 */
    }
    PG_CATCH();
    {
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        pfree(pl);
        PG_RE_THROW();
    }
    PG_END_TRY();

    pfree(pl);
    return result ? result : biscuit_roaring_create();
}

/* ================================================================
 * SECTION 9 – Public multi-column query entry points
 * ================================================================ */

RoaringBitmap *
biscuit_query_column_pattern(BiscuitIndex *idx, int col_idx, const char *pattern)
{
    ColumnIndex   *col;
    int            plen = strlen(pattern);
    ParsedPattern *parsed = NULL;
    int            min_len, i;
    RoaringBitmap *result = NULL;
    int            wildcard_count = 0, percent_count = 0;
    bool           only_wildcards = true;

    if (!idx || col_idx < 0 || col_idx >= idx->num_columns || !idx->column_indices)
        return biscuit_roaring_create();

    col = &idx->column_indices[col_idx];

    if (!col->length_bitmaps || !col->length_ge_bitmaps || col->max_length <= 0)
        return biscuit_roaring_create();

    if (plen == 0) {
        if (col->length_bitmaps[0]) return biscuit_roaring_copy(col->length_bitmaps[0]);
        return biscuit_roaring_create();
    }
    if (plen == 1 && pattern[0] == '%') {
        if (col->length_ge_bitmaps[0]) return biscuit_roaring_copy(col->length_ge_bitmaps[0]);
        return biscuit_roaring_create();
    }

    for (i = 0; i < plen; i++) {
        if (pattern[i] == '%') percent_count++;
        else if (pattern[i] == '_') wildcard_count++;
        else { only_wildcards = false; break; }
    }

    if (only_wildcards) {
        if (percent_count > 0 && wildcard_count <= col->max_length && col->length_ge_bitmaps[wildcard_count])
            return biscuit_roaring_copy(col->length_ge_bitmaps[wildcard_count]);
        if (!percent_count && wildcard_count <= col->max_length && col->length_bitmaps[wildcard_count])
            return biscuit_roaring_copy(col->length_bitmaps[wildcard_count]);
        return biscuit_roaring_create();
    }

    PG_TRY();
    {
        parsed = biscuit_parse_pattern(pattern);
        if (parsed->part_count == 0) {
            result = col->length_ge_bitmaps[0] ? biscuit_roaring_copy(col->length_ge_bitmaps[0]) : biscuit_roaring_create();
            biscuit_free_parsed_pattern(parsed); parsed = NULL; /* FIX 5 */ return result;
        }
        min_len = 0;
        for (i = 0; i < parsed->part_count; i++) min_len += parsed->part_lens[i];

        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                result = biscuit_match_col_part_at_pos(col, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (result && min_len <= col->max_length && col->length_bitmaps[min_len]) biscuit_roaring_and_inplace(result, col->length_bitmaps[min_len]);
                else if (result) { biscuit_roaring_free(result); result = biscuit_roaring_create(); }
                else result = biscuit_roaring_create();
            } else if (!parsed->starts_percent) {
                result = biscuit_match_col_part_at_pos(col, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                result = biscuit_match_col_part_at_end(col, parsed->parts[0], parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                /*
                 * Substring LIKE: %needle%
                 * Seed candidates from char_cache[first_byte] (case-sensitive),
                 * filter by minimum length, then verify with strstr.
                 */
                result = biscuit_roaring_create();
                if (parsed->part_byte_lens[0] > 0)
                {
                    unsigned char  fb    = (unsigned char) parsed->parts[0][0];
                    RoaringBitmap *cands = col->char_cache[fb]
                                          ? biscuit_roaring_copy(col->char_cache[fb])
                                          : biscuit_roaring_create();
                    int            pcl   = parsed->part_lens[0];
                    RoaringBitmap *lf    = biscuit_get_col_length_ge(col, pcl);

                    if (lf) { biscuit_roaring_and_inplace(cands, lf); biscuit_roaring_free(lf); }

#ifdef HAVE_ROARING
                    {
                        roaring_uint32_iterator_t *iter = roaring_iterator_create(cands);
                        while (iter->has_value)
                        {
                            uint32_t    rec = iter->current_value;
                            const char *hay;
                            if (rec < (uint32_t) idx->num_records &&
                                idx->column_data_cache &&
                                idx->column_data_cache[col_idx] &&
                                (hay = idx->column_data_cache[col_idx][rec]) != NULL)
                            {
                                if (strstr(hay, parsed->parts[0]) != NULL)
                                    biscuit_roaring_add(result, rec);
                            }
                            roaring_uint32_iterator_advance(iter);
                        }
                        roaring_uint32_iterator_free(iter);
                    }
#else
                    {
                        uint64_t  cnt;
                        uint32_t *indices = biscuit_roaring_to_array(cands, &cnt);
                        if (indices)
                        {
                            int j;
                            for (j = 0; j < (int) cnt; j++)
                            {
                                uint32_t    rec = indices[j];
                                const char *hay;
                                if (rec < (uint32_t) idx->num_records &&
                                    idx->column_data_cache &&
                                    idx->column_data_cache[col_idx] &&
                                    (hay = idx->column_data_cache[col_idx][rec]) != NULL)
                                {
                                    if (strstr(hay, parsed->parts[0]) != NULL)
                                        biscuit_roaring_add(result, rec);
                                }
                            }
                            pfree(indices);
                        }
                    }
#endif
                    biscuit_roaring_free(cands);
                }
            }
        } else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            RoaringBitmap *prefix = biscuit_match_col_part_at_pos(col, parsed->parts[0], parsed->part_byte_lens[0], 0);
            RoaringBitmap *suffix = biscuit_match_col_part_at_end(col, parsed->parts[1], parsed->part_byte_lens[1]);
            if (!prefix || !suffix) { if (prefix) biscuit_roaring_free(prefix); if (suffix) biscuit_roaring_free(suffix); result = biscuit_roaring_create(); }
            else { RoaringBitmap *lf;
                   biscuit_roaring_and_inplace(prefix, suffix); biscuit_roaring_free(suffix);
                   lf = biscuit_get_col_length_ge(col, min_len);
                   if (lf) { biscuit_roaring_and_inplace(prefix, lf); biscuit_roaring_free(lf); }
                   result = prefix; }
        } else {
            RoaringBitmap *cands;
            result = biscuit_roaring_create();
            cands = biscuit_get_col_length_ge(col, min_len);
            if (cands && !biscuit_roaring_is_empty(cands)) {
                if (!parsed->starts_percent) { RoaringBitmap *first = biscuit_match_col_part_at_pos(col, parsed->parts[0], parsed->part_byte_lens[0], 0); if (first) { biscuit_roaring_and_inplace(first, cands); biscuit_roaring_free(cands); cands = first; } }
                if (!biscuit_roaring_is_empty(cands))
                    biscuit_recursive_windowed_match_col(result, col, (const char **) parsed->parts, parsed->part_byte_lens, parsed->part_count, parsed->ends_percent, 0, 0, cands, col->max_length);
                biscuit_roaring_free(cands);
            } else if (cands) biscuit_roaring_free(cands);
        }
        biscuit_free_parsed_pattern(parsed);
        parsed = NULL; /* FIX 5 */
    }
    PG_CATCH();
    {
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return result ? result : biscuit_roaring_create();
}

/* ILIKE variant for multi-column: lowercase pattern first */
RoaringBitmap *
biscuit_query_column_pattern_ilike(BiscuitIndex *idx, int col_idx, const char *pattern)
{
    char          *pl;
    RoaringBitmap *result;
    ColumnIndex   *col;
    int            plen = strlen(pattern);

    if (!idx || col_idx < 0 || col_idx >= idx->num_columns || !idx->column_indices)
        return biscuit_roaring_create();

    col = &idx->column_indices[col_idx];
    pl  = biscuit_str_tolower(pattern, plen);

    /* delegate via case-insensitive (lower) accessors in a similar flow */
    {
    ParsedPattern *parsed = NULL;
    int            min_len, i;
    int            wc = 0, pc = 0;
    bool           ow = true;

    plen = strlen(pl);
    result = NULL;

    if (plen == 0) { result = (col->length_bitmaps_lower && col->length_bitmaps_lower[0]) ? biscuit_roaring_copy(col->length_bitmaps_lower[0]) : biscuit_roaring_create(); pfree(pl); return result; }
    if (plen == 1 && pl[0] == '%') { result = (col->length_ge_bitmaps_lower && col->length_ge_bitmaps_lower[0]) ? biscuit_roaring_copy(col->length_ge_bitmaps_lower[0]) : biscuit_roaring_create(); pfree(pl); return result; }

    for (i = 0; i < plen; i++) { if (pl[i] == '%') pc++; else if (pl[i] == '_') wc++; else { ow = false; break; } }
    if (ow) {
        if (pc > 0) result = biscuit_get_col_length_ge_lower(col, wc);
        else if (wc <= col->max_length_lower && col->length_bitmaps_lower && col->length_bitmaps_lower[wc]) result = biscuit_roaring_copy(col->length_bitmaps_lower[wc]);
        else result = biscuit_roaring_create();
        pfree(pl); return result;
    }

    PG_TRY();
    {
        parsed = biscuit_parse_pattern(pl);
        if (parsed->part_count == 0) { result = (col->length_ge_bitmaps_lower && col->length_ge_bitmaps_lower[0]) ? biscuit_roaring_copy(col->length_ge_bitmaps_lower[0]) : biscuit_roaring_create(); biscuit_free_parsed_pattern(parsed); parsed = NULL; /* FIX 5 */ pfree(pl); return result; }
        min_len = 0;
        for (i = 0; i < parsed->part_count; i++) min_len += parsed->part_lens[i];

        if (parsed->part_count == 1) {
            if (!parsed->starts_percent && !parsed->ends_percent) {
                result = biscuit_match_col_part_at_pos_ilike(col, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (result && min_len <= col->max_length_lower && col->length_bitmaps_lower && col->length_bitmaps_lower[min_len]) biscuit_roaring_and_inplace(result, col->length_bitmaps_lower[min_len]);
                else if (result) { biscuit_roaring_free(result); result = biscuit_roaring_create(); }
                else result = biscuit_roaring_create();
            } else if (!parsed->starts_percent) {
                result = biscuit_match_col_part_at_pos_ilike(col, parsed->parts[0], parsed->part_byte_lens[0], 0);
                if (!result) result = biscuit_roaring_create();
            } else if (!parsed->ends_percent) {
                result = biscuit_match_col_part_at_end_ilike(col, parsed->parts[0], parsed->part_byte_lens[0]);
                if (!result) result = biscuit_roaring_create();
            } else {
                /*
                 * Substring ILIKE: %needle%
                 * pl (the pattern) has already been fully lowercased above.
                 * parsed->parts[0] is therefore also lowercase.
                 * Seed candidates from char_cache_lower[first_byte_of_lowercase_needle],
                 * filter by minimum length, then verify with strstr against the
                 * lowercased data cache (column_data_cache holds the original strings;
                 * we lowercase on the fly per candidate, which is cheap because
                 * char_cache_lower has already pruned the candidate set to only rows
                 * that contain the first character of the needle).
                 */
                result = biscuit_roaring_create();
                if (parsed->part_byte_lens[0] > 0)
                {
                    unsigned char  fb    = (unsigned char) parsed->parts[0][0]; /* lowercase first byte */
                    RoaringBitmap *cands = col->char_cache_lower[fb]
                                          ? biscuit_roaring_copy(col->char_cache_lower[fb])
                                          : biscuit_roaring_create();
                    int            pcl   = parsed->part_lens[0];
                    RoaringBitmap *lf    = biscuit_get_col_length_ge_lower(col, pcl);

                    if (lf) { biscuit_roaring_and_inplace(cands, lf); biscuit_roaring_free(lf); }

#ifdef HAVE_ROARING
                    {
                        roaring_uint32_iterator_t *iter = roaring_iterator_create(cands);
                        while (iter->has_value)
                        {
                            uint32_t    rec = iter->current_value;
                            const char *hay;
                            /*
                             * Use the pre-lowercased cache populated at build
                             * / insert time.  This avoids a
                             * palloc + tolower + pfree per candidate — the hot
                             * path inside an already bitmap-pruned set.
                             * column_data_cache_lower mirrors column_data_cache
                             * slot-for-slot; a NULL entry means the source value
                             * was NULL, so the same guard applies.
                             */
                            if (rec < (uint32_t) idx->num_records &&
                                idx->column_data_cache_lower &&
                                idx->column_data_cache_lower[col_idx] &&
                                (hay = idx->column_data_cache_lower[col_idx][rec]) != NULL)
                            {
                                if (strstr(hay, parsed->parts[0]) != NULL)
                                    biscuit_roaring_add(result, rec);
                            }
                            roaring_uint32_iterator_advance(iter);
                        }
                        roaring_uint32_iterator_free(iter);
                    }
#else
                    {
                        uint64_t  cnt;
                        uint32_t *indices = biscuit_roaring_to_array(cands, &cnt);
                        if (indices)
                        {
                            int j;
                            for (j = 0; j < (int) cnt; j++)
                            {
                                uint32_t    rec = indices[j];
                                const char *hay;
                                if (rec < (uint32_t) idx->num_records &&
                                    idx->column_data_cache_lower &&
                                    idx->column_data_cache_lower[col_idx] &&
                                    (hay = idx->column_data_cache_lower[col_idx][rec]) != NULL)
                                {
                                    if (strstr(hay, parsed->parts[0]) != NULL)
                                        biscuit_roaring_add(result, rec);
                                }
                            }
                            pfree(indices);
                        }
                    }
#endif
                    biscuit_roaring_free(cands);
                }
            }
        } else if (parsed->part_count == 2 && !parsed->starts_percent && !parsed->ends_percent) {
            RoaringBitmap *prefix = biscuit_match_col_part_at_pos_ilike(col, parsed->parts[0], parsed->part_byte_lens[0], 0);
            RoaringBitmap *suffix = biscuit_match_col_part_at_end_ilike(col, parsed->parts[1], parsed->part_byte_lens[1]);
            if (!prefix || !suffix) { if (prefix) biscuit_roaring_free(prefix); if (suffix) biscuit_roaring_free(suffix); result = biscuit_roaring_create(); }
            else { RoaringBitmap *lf; biscuit_roaring_and_inplace(prefix, suffix); biscuit_roaring_free(suffix); lf = biscuit_get_col_length_ge_lower(col, min_len); if (lf) { biscuit_roaring_and_inplace(prefix, lf); biscuit_roaring_free(lf); } result = prefix; }
        } else {
            RoaringBitmap *cands;
            result = biscuit_roaring_create();
            cands = biscuit_get_col_length_ge_lower(col, min_len);
            if (cands && !biscuit_roaring_is_empty(cands)) {
                if (!parsed->starts_percent) { RoaringBitmap *first = biscuit_match_col_part_at_pos_ilike(col, parsed->parts[0], parsed->part_byte_lens[0], 0); if (first) { biscuit_roaring_and_inplace(first, cands); biscuit_roaring_free(cands); cands = first; } }
                if (!biscuit_roaring_is_empty(cands))
                    biscuit_recursive_windowed_match_col_ilike(result, col, (const char **) parsed->parts, parsed->part_byte_lens, parsed->part_count, parsed->ends_percent, 0, 0, cands, col->max_length_lower);
                biscuit_roaring_free(cands);
            } else if (cands) biscuit_roaring_free(cands);
        }
        biscuit_free_parsed_pattern(parsed);
        parsed = NULL; /* FIX 5 */
    }
    PG_CATCH();
    {
        if (parsed) biscuit_free_parsed_pattern(parsed);
        if (result) biscuit_roaring_free(result);
        pfree(pl);
        PG_RE_THROW();
    }
    PG_END_TRY();

    pfree(pl);
    return result ? result : biscuit_roaring_create();
    } /* end parsed block */
}

/* ================================================================
 * SECTION 10 – Query plan / optimizer
 * ================================================================ */

static int
calculate_anchor_strength(const char *pattern, bool is_prefix, bool is_suffix)
{
    int strength = 0, i, len = strlen(pattern);
    if (!is_prefix && !is_suffix) return 0;
    if (is_prefix) { for (i = 0; i < len && pattern[i] != '%'; i++) strength += (pattern[i] != '_') ? 10 : 3; }
    if (is_suffix) { int ss = len; for (i = len - 1; i >= 0 && pattern[i] != '%'; i--) ss = i; for (i = ss; i < len; i++) strength += (pattern[i] != '_') ? 10 : 3; }
    return Min(strength, 100);
}

static void
analyze_pattern(QueryPredicate *pred)
{
    const char *p = pred->pattern;
    int len = strlen(p), i;
    bool in_pct = false;

    pred->concrete_chars = pred->underscore_count = pred->percent_count = 0;
    pred->partition_count = 0; pred->has_percent = false;

    for (i = 0; i < len; i++) {
        if (p[i] == '%') { pred->has_percent = true; if (!in_pct) { pred->percent_count++; in_pct = true; } }
        else { if (in_pct) pred->partition_count++; in_pct = false; if (p[i] == '_') pred->underscore_count++; else pred->concrete_chars++; }
    }
    if (!in_pct && len > 0) pred->partition_count++;

    pred->is_exact  = !pred->has_percent && pred->underscore_count == 0;
    pred->is_prefix = (len > 0 && p[0] != '%') && pred->has_percent;
    pred->is_suffix = (len > 0 && p[len - 1] != '%') && pred->has_percent;
    pred->is_substring = pred->starts_percent = pred->ends_percent = false;
    if (pred->has_percent) {
        pred->starts_percent  = (p[0] == '%');
        pred->ends_percent    = (p[len - 1] == '%');
        pred->is_substring    = pred->starts_percent && pred->ends_percent && pred->percent_count >= 2;
    }
    pred->anchor_strength = calculate_anchor_strength(p, pred->is_prefix, pred->is_suffix);
    pred->selectivity_score = pred->is_exact ? 0.0
        : pred->is_prefix || pred->is_suffix ? 0.1 + (pred->percent_count * 0.1)
        : pred->is_substring ? 0.5 : 0.8;
    pred->selectivity_score -= (pred->concrete_chars * 0.05);
    if (pred->selectivity_score < 0.0) pred->selectivity_score = 0.01;
    if (pred->selectivity_score > 1.0) pred->selectivity_score = 1.0;
}

static int
compare_predicates(const void *a, const void *b)
{
    const QueryPredicate *pa = (const QueryPredicate *) a;
    const QueryPredicate *pb = (const QueryPredicate *) b;
    if (pa->selectivity_score < pb->selectivity_score) return -1;
    if (pa->selectivity_score > pb->selectivity_score) return  1;
    return 0;
}

QueryPlan *
biscuit_build_query_plan(BiscuitIndex *idx, ScanKey keys, int nkeys)
{
    QueryPlan *plan;
    int        i;

    (void) idx;  /* reserved for future cardinality-aware planning */

    plan            = (QueryPlan *) palloc(sizeof(QueryPlan));
    plan->predicates = (QueryPredicate *) palloc(nkeys * sizeof(QueryPredicate));
    plan->count     = 0;
    plan->capacity  = nkeys;

    for (i = 0; i < nkeys; i++)
    {
        ScanKey        key  = &keys[i];
        QueryPredicate *pred = &plan->predicates[plan->count];

        if (key->sk_flags & SK_ISNULL)
            continue;

        pred->column_index = key->sk_attno - 1;
        pred->scan_key     = key;

        {
            text *pt = DatumGetTextPP(key->sk_argument);
            pred->pattern = pstrdup(text_to_cstring(pt));
        }

        analyze_pattern(pred);
        plan->count++;
    }

    /* Sort by selectivity: most selective first */
    if (plan->count > 1)
        qsort(plan->predicates, plan->count, sizeof(QueryPredicate), compare_predicates);

    return plan;
}

void
biscuit_free_query_plan(QueryPlan *plan)
{
    int i;

    if (!plan)
        return;

    PG_TRY();
    {
        if (plan->predicates) {
            for (i = 0; i < plan->count; i++) {
                if (plan->predicates[i].pattern) {
                    pfree(plan->predicates[i].pattern);
                    plan->predicates[i].pattern = NULL;
                }
            }
            pfree(plan->predicates);
            plan->predicates = NULL;
        }
        pfree(plan);
    }
    PG_CATCH();
    {
        FlushErrorState();
    }
    PG_END_TRY();
}
