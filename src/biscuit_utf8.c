/*
 * biscuit_utf8.c
 * UTF-8 character-level utilities and type-conversion helpers.
 */

#include "biscuit_common.h"
#include "biscuit_utf8.h"

/* ==================== UTF-8 BYTE-LENGTH LOOKUP ==================== */

/*
 * Return the byte width of a UTF-8 sequence from its leading byte.
 *   0x00–0x7F  → 1  (ASCII)
 *   0x80–0xBF  → 1  (invalid continuation byte – treat as single)
 *   0xC0–0xDF  → 2
 *   0xE0–0xEF  → 3
 *   0xF0–0xF7  → 4
 *   0xF8+      → 1  (invalid – treat as single)
 */
int
biscuit_utf8_char_length(unsigned char c)
{
    if (c < 0x80) return 1;
    if (c < 0xC0) return 1;  /* continuation byte */
    if (c < 0xE0) return 2;
    if (c < 0xF0) return 3;
    if (c < 0xF8) return 4;
    return 1;
}

/* ==================== CHARACTER COUNT ==================== */

/*
 * Count UTF-8 characters (not bytes).
 * Invalid sequences are counted as one character each.
 */
int
biscuit_utf8_char_count(const char *str, int byte_len)
{
    int char_count = 0;
    int pos        = 0;

    while (pos < byte_len)
    {
        int char_len = biscuit_utf8_char_length((unsigned char) str[pos]);
        /* Don't read past buffer */
        if (pos + char_len > byte_len)
            char_len = byte_len - pos;
        pos += char_len;
        char_count++;
    }
    return char_count;
}

/* ==================== CONTINUATION BYTE CHECK ==================== */

bool
biscuit_utf8_is_continuation(unsigned char c)
{
    return (c & 0xC0) == 0x80;
}

/* ==================== VALIDATE A SINGLE CHARACTER ==================== */

bool
biscuit_utf8_validate_char(const char *str, int byte_pos, int byte_len)
{
    int i;
    unsigned char c;
    int char_len;

    if (byte_pos >= byte_len)
        return false;

    c        = (unsigned char) str[byte_pos];
    char_len = biscuit_utf8_char_length(c);

    if (byte_pos + char_len > byte_len)
        return false;

    for (i = 1; i < char_len; i++)
    {
        if (!biscuit_utf8_is_continuation((unsigned char) str[byte_pos + i]))
            return false;
    }
    return true;
}

/* ==================== CHARACTER → BYTE OFFSET ==================== */

/*
 * Return the byte offset corresponding to the character at char_pos.
 * Returns -1 when char_pos is out of range or the encoding is invalid.
 */
int
biscuit_utf8_char_to_byte_offset(const char *str, int byte_len, int char_pos)
{
    int current_char = 0;
    int byte_pos     = 0;

    while (byte_pos < byte_len && current_char < char_pos)
    {
        int char_len = biscuit_utf8_char_length((unsigned char) str[byte_pos]);
        if (byte_pos + char_len > byte_len)
            return -1;
        byte_pos += char_len;
        current_char++;
    }
    return (current_char == char_pos) ? byte_pos : -1;
}

/* ==================== LOWERCASE CONVERSION ==================== */

/*
 * Convert str (length len bytes) to lowercase using PostgreSQL's
 * locale-aware lower() built-in.  The result is palloc'd.
 */
char *
biscuit_str_tolower(const char *str, int len)
{
    text *input;
    text *result_text;
    Oid   collation;

    if (len == 0)
        return pstrdup("");

    input     = cstring_to_text_with_len(str, len);
    collation = get_typcollation(TEXTOID);

    result_text = DatumGetTextP(
        DirectFunctionCall2Coll(
            lower,
            collation,
            PointerGetDatum(input),
            collation
        )
    );
    return text_to_cstring(result_text);
}

/* ==================== DATUM → C STRING ==================== */

/*
 * Extract a C string from a text-family Datum.
 * Supports TEXTOID, VARCHAROID, BPCHAROID.
 * Sets *out_len to the byte length of the returned string.
 */
char *
biscuit_datum_to_text(Datum value, Oid typoid, FmgrInfo *outfunc, int *out_len)
{
    char *result;
    text *txt;
    char *str;
    int   len;
    switch (typoid)
    {
        case BPCHAROID:
        case TEXTOID:
        case VARCHAROID:
        {
            txt    = DatumGetTextPP(value);
            str    = VARDATA_ANY(txt);
            len    = VARSIZE_ANY_EXHDR(txt);
            result = pnstrdup(str, len);
            *out_len = len;
            if ((void *) txt != (void *) DatumGetPointer(value))
                pfree(txt);
            break;
        }
        /*
        case BPCHAROID: //This segment is not valid because sequential scans treat the padded spaces as literal characters
        {
            txt    = DatumGetTextPP(value);
            str    = VARDATA_ANY(txt);
            len    = bpchartruelen(str, VARSIZE_ANY_EXHDR(txt));
            result = pnstrdup(str, len);
            *out_len = len;
            if ((void *) txt != (void *) DatumGetPointer(value))
                pfree(txt);
            break;
        }
        */
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("biscuit index does not support type %s",
                            format_type_be(typoid)),
                     errhint("Only TEXT and VARCHAR types are supported.")));
    }
    return result;
}
