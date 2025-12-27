-- Ensure UTF-8 behavior
SET client_encoding = 'UTF8';

DROP TABLE IF EXISTS french_words;

CREATE TABLE french_words (
    id SERIAL PRIMARY KEY,
    word TEXT NOT NULL
);

INSERT INTO french_words (word) VALUES
    ('école'),
    ('élève'),
    ('à côté'),
    ('garçon'),
    ('français'),
    ('où'),
    ('être'),
    ('forêt'),
    ('hôpital'),
    ('île'),
    ('Noël'),
    ('ça'),
    ('façade'),
    ('crème'),
    ('déjà'),
    ('piqûre'),
    ('cœur'),
    ('aïe'),
    ('maïs'),
    ('sœur');

-- Sanity check
SELECT id, word FROM french_words ORDER BY id;
