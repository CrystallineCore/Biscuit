/*
 * biscuit_fanout.c
 * The single implementation of "what structures does this string belong
 * to". See biscuit_fanout.h for why this exists and who calls it.
 *
 * This file is deliberately dependency-light: UTF-8 helpers and the
 * BISCUIT_DIR_KIND_* constants, nothing else. It touches no bitmaps, no
 * buffers, no directory, and no BiscuitIndex -- it is a pure function from
 * bytes to identities. That is what lets the write path, the drain and the
 * delta builder share it without any of them dragging the others' state in.
 */

#include "biscuit_common.h"
#include "biscuit_utf8.h"
#include "biscuit_fanout.h"

/*
 * One case-mode pass over a string: POS / NEG / CACHE per byte per
 * character, then LEN and the LEN_GE ladder.
 *
 * The per-BYTE inner loop is not a bug and must not be "fixed" into a
 * per-character loop. The matcher (biscuit_pattern.c) probes these
 * structures a byte at a time, so a 3-byte UTF-8 character is indexed as
 * three separate (ch = byte) entries that all share the same char_pos.
 * Changing the granularity here changes what every existing compacted
 * blob means.
 *
 * Likewise `neg_offset` is recomputed per byte from the *character* count
 * remaining, which for a multi-byte character yields the same offset for
 * each of its bytes -- again matching the write path exactly.
 */
static void
fanout_pass(const char *str, int byte_len,
            int32 col, bool is_lower, uint32 slot,
            int32 len_ge_bound, uint8 op,
            BiscuitFanoutEmit emit, void *ctx)
{
    int byte_pos   = 0;
    int char_pos   = 0;
    int char_count;
    int i;

    while (byte_pos < byte_len)
    {
        unsigned char first_byte = (unsigned char) str[byte_pos];
        int           char_len   = biscuit_utf8_char_length(first_byte);
        int           b;

        if (byte_pos + char_len > byte_len)
            char_len = byte_len - byte_pos;

        for (b = 0; b < char_len; b++)
        {
            unsigned char uch             = (unsigned char) str[byte_pos + b];
            int           remaining_chars = biscuit_utf8_char_count(str + byte_pos,
                                                                    byte_len - byte_pos);

            emit(ctx, col, is_lower, BISCUIT_DIR_KIND_POS,
                 (int32) uch, (int32) char_pos, slot, op);

            emit(ctx, col, is_lower, BISCUIT_DIR_KIND_NEG,
                 (int32) uch, (int32) -remaining_chars, slot, op);

            emit(ctx, col, is_lower, BISCUIT_DIR_KIND_CACHE,
                 (int32) uch, -1, slot, op);
        }

        byte_pos += char_len;
        char_pos++;
    }

    /*
     * LEN / LEN_GE.
     *
     * biscuit_insert() emitted these inline, gated on the same case mode
     * as the character pass above (case-sensitive under BISCUIT_MODE_LIKE,
     * lowercase under a non-NULL data_cache_lower[slot], which is only
     * populated under BISCUIT_MODE_ILIKE). Folding them in here is what
     * makes this function the whole answer for one (string, case mode)
     * rather than most of it.
     *
     * The LEN_GE ladder is inclusive of char_count: a string of length L
     * is a member of length_ge[i] for every i <= L. The write path also
     * clamped i to the live allocated capacity of length_ge_bitmaps[],
     * which is what len_ge_bound reproduces; a caller with no such array
     * passes -1.
     */
    char_count = biscuit_utf8_char_count(str, byte_len);

    emit(ctx, col, is_lower, BISCUIT_DIR_KIND_LEN,
         -1, (int32) char_count, slot, op);

    for (i = 0; i <= char_count; i++)
    {
        if (len_ge_bound >= 0 && i >= len_ge_bound)
            break;
        emit(ctx, col, is_lower, BISCUIT_DIR_KIND_LEN_GE,
             -1, (int32) i, slot, op);
    }
}

void
biscuit_fanout_string(const char *str, int byte_len,
                      const char *lower_str, int lower_len,
                      int32 col, uint8 mode, uint32 slot,
                      int32 len_ge_bound,
                      uint8 op,
                      BiscuitFanoutEmit emit, void *ctx)
{
    Assert(op == BISCUIT_PENDING_OP_ADD || op == BISCUIT_PENDING_OP_REMOVE);

    if (emit == NULL)
        return;

    if (str != NULL && (mode & BISCUIT_MODE_LIKE))
        fanout_pass(str, byte_len, col, false, slot, len_ge_bound, op, emit, ctx);

    /*
     * A NULL lower_str suppresses this pass even when the mode bit is set.
     * That is the write path's convention, not a defensive check: a column
     * whose opclass never needs ILIKE leaves data_cache_lower[slot] NULL,
     * and the delta builder sees exactly the same NULL when it reads the
     * (col, is_lower = true) STRCACHE entry back.
     */
    if (lower_str != NULL && (mode & BISCUIT_MODE_ILIKE))
        fanout_pass(lower_str, lower_len, col, true, slot, len_ge_bound, op, emit, ctx);
}

uint64
biscuit_fanout_count(const char *str, int byte_len,
                     const char *lower_str, int lower_len,
                     uint8 mode, int32 len_ge_bound)
{
    uint64 total = 0;

    /*
     * Three identities per byte, plus one LEN, plus the LEN_GE ladder --
     * counted rather than emitted. Kept in lockstep with fanout_pass() by
     * being a five-line function directly beneath it.
     */
    if (str != NULL && (mode & BISCUIT_MODE_LIKE))
    {
        int    cc   = biscuit_utf8_char_count(str, byte_len);
        uint64 rung = (uint64) (cc + 1);

        if (len_ge_bound >= 0 && rung > (uint64) len_ge_bound)
            rung = (uint64) len_ge_bound;

        total += (uint64) byte_len * 3 + 1 + rung;
    }

    if (lower_str != NULL && (mode & BISCUIT_MODE_ILIKE))
    {
        int    cc   = biscuit_utf8_char_count(lower_str, lower_len);
        uint64 rung = (uint64) (cc + 1);

        if (len_ge_bound >= 0 && rung > (uint64) len_ge_bound)
            rung = (uint64) len_ge_bound;

        total += (uint64) lower_len * 3 + 1 + rung;
    }

    return total;
}
