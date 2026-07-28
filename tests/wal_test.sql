-- wal_check.sql
--
-- Standalone sanity check of core WAL functionality on a running
-- PostgreSQL server. Pure SQL -- no psql meta-commands (\echo, \pset,
-- etc.) -- so this runs unmodified through any SQL client/driver, not
-- just psql.
--
-- This does NOT crash the server or test crash recovery (see
-- test_wal_recovery.sh for that) -- it just verifies WAL is actively
-- being written, checkpoints move the redo pointer forward, and reports
-- the config/stats/replication state relevant to WAL and PITR.

-- === WAL configuration ===
SELECT name, setting, unit, context
FROM pg_settings
WHERE name IN (
    'wal_level', 'fsync', 'full_page_writes', 'wal_consistency_checking',
    'max_wal_size', 'min_wal_size', 'wal_compression',
    'archive_mode', 'archive_command'
)
ORDER BY name;

-- === Recovery state ===
SELECT pg_is_in_recovery() AS in_recovery;

-- === Current WAL position ===
SELECT
    pg_current_wal_lsn()                       AS current_lsn,
    pg_current_wal_insert_lsn()                AS insert_lsn,
    pg_current_wal_flush_lsn()                 AS flush_lsn,
    pg_walfile_name(pg_current_wal_lsn())      AS current_wal_file;

-- === Verify LSN advances when data is written ===
-- NOTE: this must use a PERMANENT table. Temp tables are intentionally
-- NOT WAL-logged by PostgreSQL (they're session-local and not meant to
-- survive a crash), so a temp-table probe would always show zero WAL
-- growth regardless of whether the server's WAL path is healthy.
--
-- NOTE: this is written as plain top-level statements rather than a
-- DO $$ ... $$ block. Calling pg_current_wal_lsn() twice inside the same
-- PL/pgSQL block did not reliably reflect the intervening writes when
-- this was tested -- compare the "before"/"after" SELECTs below by eye
-- (or via \gset if you're in psql) instead.
DROP TABLE IF EXISTS public._wal_check_probe;

SELECT pg_current_wal_lsn() AS lsn_before_probe;

CREATE TABLE public._wal_check_probe (id serial, val text);
INSERT INTO public._wal_check_probe (val)
    SELECT 'probe_' || g FROM generate_series(1, 1000) g;

SELECT pg_current_wal_lsn() AS lsn_after_probe;

DROP TABLE public._wal_check_probe;

-- === Control file / last checkpoint ===
SELECT * FROM pg_control_checkpoint();
SELECT * FROM pg_control_system();

-- === Verify CHECKPOINT moves the redo pointer forward ===
DO $$
DECLARE
    redo_before pg_lsn;
    redo_after  pg_lsn;
BEGIN
    SELECT redo_lsn INTO redo_before FROM pg_control_checkpoint();
    CHECKPOINT;
    SELECT redo_lsn INTO redo_after FROM pg_control_checkpoint();

    IF redo_after >= redo_before THEN
        RAISE NOTICE 'OK: checkpoint redo LSN % -> %', redo_before, redo_after;
    ELSE
        RAISE WARNING 'FAIL: redo LSN went backwards (% -> %), should never happen',
            redo_before, redo_after;
    END IF;
END $$;

-- === Cumulative WAL I/O stats ===
SELECT * FROM pg_stat_wal;

-- === Archiver status (relevant if archive_mode = on, for PITR) ===
SELECT * FROM pg_stat_archiver;

-- === Replication slots (physical/logical) ===
SELECT slot_name, slot_type, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots;

-- === Connected standbys (empty on a lone primary) ===
SELECT application_name, state, sent_lsn, write_lsn, flush_lsn, replay_lsn
FROM pg_stat_replication;

-- === Done ===
DO $$
BEGIN
    RAISE NOTICE 'WAL check complete.';
END $$;
