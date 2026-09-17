-- ===========================================================================
--  08_unicode.sql  --  UTF-8 and multibyte text
--
--  Biscuit indexes character POSITIONS. In UTF-8 a character is one to four
--  bytes, so "position" is ambiguous in exactly the way that produces silent
--  wrong answers: an index that counted bytes would agree with a sequential
--  scan on every ASCII fixture in this suite and disagree the moment a single
--  accented character appeared earlier in the string.
--
--  The sharpest test is '_'. LIKE's underscore matches exactly one CHARACTER.
--  A byte-oriented index would need three underscores to match one euro sign
--  and would return the wrong rows for a pattern like '_BER'. The regex '.'
--  and '.{n}' carry the same obligation through the decomposer, since '.'
--  becomes '_' and the rewrite is only exact if both mean "one character".
--
--  Also covered: multibyte characters at the first and last position, where
--  an off-by-one in a prefix or suffix scan lands; the four byte-lengths;
--  combining sequences, which must NOT compare equal to their precomposed
--  forms under a byte-exact operator like LIKE; and escaped wildcards sitting
--  next to multibyte data, where the escape logic walks bytes and must not
--  mistake a continuation byte for a metacharacter.
-- ===========================================================================

DELETE FROM bt_result WHERE category = 'unicode';

DROP TABLE IF EXISTS uc CASCADE;

CREATE TABLE uc (id int PRIMARY KEY, v text);

INSERT INTO uc (id, v)
SELECT g,
       CASE WHEN (g / 28) % 20 = 0 THEN s.stem
            ELSE s.stem || '-' || substr(md5(g::text), 1, 6) END
FROM generate_series(1, 28000) AS g
JOIN (VALUES
    -- 1-byte
    ( 0,'abc'),        ( 1,'aBc'),
    -- 2-byte: Latin-1 supplement, Greek, Cyrillic
    ( 2,'éclair'),     ( 3,'Über'),      ( 4,'naïve'),     ( 5,'σοφός'),
    ( 6,'Привет'),     ( 7,'café'),
    -- 3-byte: currency, CJK, symbols
    ( 8,'€100'),       ( 9,'日本語'),     (10,'中文字'),     (11,'한국어'),
    (12,'→arrow'),     (13,'℃temp'),
    -- 4-byte: emoji, astral plane
    (14,'😀smile'),    (15,'a😀b'),      (16,'𝔘𝔫𝔦'),        (17,'🇮🇳flag'),
    -- mixed widths in one value, so a byte-position index misaligns
    -- differently at each offset
    (18,'aé中😀z'),    (19,'xÜy€z'),     (20,'1é2中3😀4'),
    -- combining vs precomposed: these two are different byte sequences that
    -- render identically, and LIKE must keep them distinct
    (21,'e' || U&'\0301' || 'tude'),     (22,'étude'),
    -- multibyte adjacent to LIKE metacharacters as DATA
    (23,'é%b'),        (24,'é_b'),       (25,'100%€'),     (26,'a\é'),
    (27,'')
) AS s(k, stem) ON s.k = g % 28;

INSERT INTO uc (id, v) VALUES (90001, NULL), (90002, 'é'), (90003, '😀');

CREATE INDEX uc_ix ON uc USING biscuit (v);
ANALYZE uc;


-- ---- '_' counts characters, not bytes --------------------------------------
-- The decisive group. 'Über' begins with a 2-byte character: '_ber' must match
-- it and '__ber' must not. A byte-oriented index inverts both answers.
DO $BT$ BEGIN
    PERFORM bt_run('unicode','_ vs 2-byte lead',   'uc', $$v LIKE '_ber%'$$,      'must','uc_ix');
    PERFORM bt_run('unicode','__ vs 2-byte lead',  'uc', $$v LIKE '__ber%'$$,     'must','uc_ix');
    PERFORM bt_run('unicode','_ vs 3-byte lead',   'uc', $$v LIKE '_100%'$$,      'must','uc_ix');
    PERFORM bt_run('unicode','_ vs 4-byte lead',   'uc', $$v LIKE '_smile%'$$,    'must','uc_ix');
    PERFORM bt_run('unicode','_ around 4-byte',    'uc', $$v LIKE 'a_b'$$,        'must','uc_ix');
    PERFORM bt_run('unicode','_ mid 2-byte',       'uc', $$v LIKE 'caf_'$$,       'must','uc_ix');
    PERFORM bt_run('unicode','_ mid 3-byte CJK',   'uc', $$v LIKE '日_語%'$$,      'must','uc_ix');
    PERFORM bt_run('unicode','_ run over mixed',   'uc', $$v LIKE '_____'$$,      'must','uc_ix');
    PERFORM bt_run('unicode','_ exact one char',   'uc', $$v LIKE '_'$$,          'must','uc_ix');
    PERFORM bt_run('unicode','_ mixed widths',     'uc', $$v LIKE 'a_中_z'$$,      'must','uc_ix');
END $BT$;

-- ---- regex '.' and '.{n}' must agree with '_' ------------------------------
-- '.' is rewritten to '_' by the decomposer, so if either one counts bytes the
-- rewrite stops being exact. These mirror the group above through the regex
-- path, and the identity block at the end asserts the two agree.
DO $BT$ BEGIN
    PERFORM bt_run('unicode','regex . 2-byte',     'uc', $$v ~ '^.ber'$$,         'must','uc_ix');
    PERFORM bt_run('unicode','regex . 3-byte',     'uc', $$v ~ '^.100'$$,         'must','uc_ix');
    PERFORM bt_run('unicode','regex . 4-byte',     'uc', $$v ~ '^.smile'$$,       'must','uc_ix');
    PERFORM bt_run('unicode','regex .{3} CJK',     'uc', $$v ~ '^.{3}$'$$,        'must','uc_ix');
    PERFORM bt_run('unicode','regex .{5} mixed',   'uc', $$v ~ '^.{5}$'$$,        'must','uc_ix');
    PERFORM bt_run('unicode','regex . around mb',  'uc', $$v ~ '^a.b$'$$,         'must','uc_ix');
    PERFORM bt_run('unicode','regex .+ multibyte', 'uc', $$v ~ '^日.+'$$,          'must','uc_ix');
    PERFORM bt_run('unicode','regex literal mb',   'uc', $$v ~ '^中文'$$,          'must','uc_ix');
    PERFORM bt_run('unicode','regex mb anchored',  'uc', $$v ~ '語$'$$,            'either','uc_ix');
END $BT$;

-- ---- multibyte at the string boundaries ------------------------------------
-- Prefix and suffix scans are where an off-by-one in character width lands.
DO $BT$ BEGIN
    PERFORM bt_run('unicode','prefix 2-byte',      'uc', $$v LIKE 'é%'$$,         'must','uc_ix');
    PERFORM bt_run('unicode','prefix 3-byte',      'uc', $$v LIKE '€%'$$,         'must','uc_ix');
    PERFORM bt_run('unicode','prefix 4-byte',      'uc', $$v LIKE '😀%'$$,        'must','uc_ix');
    PERFORM bt_run('unicode','prefix CJK',         'uc', $$v LIKE '日本%'$$,       'must','uc_ix');
    PERFORM bt_run('unicode','suffix 2-byte',      'uc', $$v LIKE '%é'$$,         'must','uc_ix');
    PERFORM bt_run('unicode','exact single 2-byte','uc', $$v LIKE 'é'$$,          'must','uc_ix');
    PERFORM bt_run('unicode','exact single 4-byte','uc', $$v LIKE '😀'$$,         'must','uc_ix');
    PERFORM bt_run('unicode','infix 3-byte',       'uc', $$v LIKE '%中%'$$,        'must','uc_ix');
    PERFORM bt_run('unicode','infix 4-byte',       'uc', $$v LIKE '%😀%'$$,       'must','uc_ix');
    PERFORM bt_run('unicode','astral plane',       'uc', $$v LIKE '𝔘%'$$,         'must','uc_ix');
    PERFORM bt_run('unicode','regional indicator', 'uc', $$v LIKE '🇮🇳%'$$,        'must','uc_ix');
END $BT$;

-- ---- combining sequences ---------------------------------------------------
-- 'e' + U+0301 and 'é' render identically and are different byte sequences.
-- LIKE is byte-exact, so a pattern written one way must not match the other.
-- An index that normalised, folded, or compared by rendered glyph would merge
-- them -- and the fixture contains one of each, so the merge is visible.
DO $BT$ BEGIN
    PERFORM bt_run('unicode','precomposed only',   'uc', $$v LIKE 'étude%'$$,     'must','uc_ix');
    PERFORM bt_run('unicode','combining only',     'uc',
        $$v LIKE 'e' || U&'\0301' || 'tude%'$$,                                   'must','uc_ix');
    PERFORM bt_run('unicode','either spelling',    'uc', $$v LIKE '%tude%'$$,     'must','uc_ix');
    PERFORM bt_run('unicode','combining + _',      'uc', $$v LIKE '_tude%'$$,     'must','uc_ix');
END $BT$;

-- ---- escaped wildcards next to multibyte data ------------------------------
-- The escape scan walks bytes. Every byte of a multibyte sequence has its high
-- bit set and '%', '_' and '\' are all ASCII, so a correct implementation can
-- never mistake a continuation byte for a metacharacter -- these cases are
-- what turns that reasoning into a test.
DO $BT$ BEGIN
    PERFORM bt_run('unicode','escaped pct after mb','uc', $$v LIKE 'é\%b'$$,      'must','uc_ix');
    PERFORM bt_run('unicode','escaped und after mb','uc', $$v LIKE 'é\_b'$$,      'must','uc_ix');
    PERFORM bt_run('unicode','escaped pct before mb','uc',$$v LIKE '100\%€'$$,    'must','uc_ix');
    PERFORM bt_run('unicode','backslash before mb', 'uc', $$v LIKE 'a\\é'$$,      'must','uc_ix');
    PERFORM bt_run('unicode','regex escaped mb',    'uc', $$v ~ '^é\%b$'$$,       'must','uc_ix');
END $BT$;

-- ---- case folding over multibyte -------------------------------------------
-- Expectation 'either': whether the AM serves a non-ASCII ILIKE is a design
-- choice with a correctness argument behind it. The answer is not optional.
DO $BT$ BEGIN
    PERFORM bt_run('unicode','ilike 2-byte',       'uc', $$v ILIKE 'ÜBER%'$$,     'either','uc_ix');
    PERFORM bt_run('unicode','ilike greek',        'uc', $$v ILIKE 'ΣΟΦΌΣ'$$,     'either','uc_ix');
    PERFORM bt_run('unicode','ilike cyrillic',     'uc', $$v ILIKE 'привет%'$$,   'either','uc_ix');
    PERFORM bt_run('unicode','ilike CJK no case',  'uc', $$v ILIKE '日本語%'$$,    'either','uc_ix');
    PERFORM bt_run('unicode','ilike emoji no case','uc', $$v ILIKE '😀SMILE%'$$,  'either','uc_ix');
END $BT$;


-- ---- Identities ------------------------------------------------------------
DO $BT$
DECLARE a bigint; b bigint; n int;
BEGIN
    SET LOCAL enable_seqscan = off;

    -- 1. '_' and regex '.' must be the same thing over multibyte input. If
    --    one counts bytes and the other characters, this is where it shows.
    SELECT count(*) INTO a FROM uc WHERE v LIKE '_ber%';
    SELECT count(*) INTO b FROM uc WHERE v ~ '^.ber';
    PERFORM bt_assert(a = b,
        format('LIKE ''_ber%%'' (%s) and ~ ''^.ber'' (%s) must agree', a, b));

    SELECT count(*) INTO a FROM uc WHERE v LIKE '_____';
    SELECT count(*) INTO b FROM uc WHERE v ~ '^.{5}$';
    PERFORM bt_assert(a = b,
        format('five underscores (%s) and .{5} (%s) must agree', a, b));

    -- 2. A pattern of n underscores must select exactly the rows of character
    --    length n -- the definition of "counts characters", checked against
    --    PostgreSQL's own character_length() rather than against another
    --    Biscuit answer.
    FOR n IN 1 .. 6 LOOP
        EXECUTE format('SELECT count(*) FROM uc WHERE v LIKE %L', repeat('_', n))
          INTO a;
        SELECT count(*) INTO b FROM uc WHERE character_length(v) = n;
        PERFORM bt_assert(a = b,
            format('LIKE with %s underscores matched %s rows, but %s rows have '
                   'character_length = %s', n, a, b, n));
    END LOOP;

    -- 3. The same values have a LARGER octet_length than character_length,
    --    which is what makes the check above meaningful rather than vacuous.
    SELECT count(*) INTO a FROM uc WHERE octet_length(v) > character_length(v);
    PERFORM bt_assert(a > 0,
        'fixture contains no multibyte values, so the character-vs-byte '
        'checks above prove nothing');

    -- 4. Combining and precomposed spellings must stay distinct.
    SELECT count(*) INTO a FROM uc WHERE v LIKE 'étude%';
    SELECT count(*) INTO b FROM uc WHERE v LIKE 'e' || U&'\0301' || 'tude%';
    PERFORM bt_assert(a > 0 AND b > 0,
        format('fixture should contain both spellings, got %s and %s', a, b));
    SELECT count(*) INTO a FROM uc
     WHERE v LIKE 'étude%' AND v LIKE 'e' || U&'\0301' || 'tude%';
    PERFORM bt_assert(a = 0,
        format('%s rows matched BOTH the precomposed and the combining '
               'spelling -- LIKE is byte-exact and must keep them distinct', a));

    RAISE NOTICE 'unicode : character-position semantics verified against '
                 'character_length() for lengths 1 through 6';
END $BT$;

DO $BT$ BEGIN PERFORM bt_check('unicode'); END $BT$;
