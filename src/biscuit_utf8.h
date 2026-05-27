/*
 * biscuit_utf8.h
 * UTF-8 character-level utility functions used throughout Biscuit.
 */

#ifndef BISCUIT_UTF8_H
#define BISCUIT_UTF8_H

#include "biscuit_common.h"

/* Byte length of a UTF-8 character from its leading byte (1–4) */
extern int  biscuit_utf8_char_length(unsigned char c);

/* Number of UTF-8 characters (not bytes) in a string */
extern int  biscuit_utf8_char_count(const char *str, int byte_len);

/* True if byte is a UTF-8 continuation byte (10xxxxxx) */
extern bool biscuit_utf8_is_continuation(unsigned char c);

/* Validate a UTF-8 character starting at byte_pos */
extern bool biscuit_utf8_validate_char(const char *str, int byte_pos, int byte_len);

/* Byte offset of the character at char_pos (returns -1 on out-of-range) */
extern int  biscuit_utf8_char_to_byte_offset(const char *str, int byte_len, int char_pos);

/* Convert str to lowercase using PostgreSQL's locale-aware lower() */
extern char *biscuit_str_tolower(const char *str, int len);

/* Convert a Datum of a text-family type to a C string */
extern char *biscuit_datum_to_text(Datum value, Oid typoid, FmgrInfo *outfunc, int *out_len);

#endif /* BISCUIT_UTF8_H */