-- biscuit--3.1.0--3.1.1.sql
-- Upgrades a Biscuit 3.1.0 installation to 3.1.1.
--
-- 3.1.1 adds PostgreSQL 19 build support. There is no SQL-level change
-- and no on-disk format change, so no REINDEX is required. This script
-- only records the new version. Install the 3.1.1 shared library first.

\echo Use "ALTER EXTENSION biscuit UPDATE TO '3.1.1'" to load this file. \quit

-- ==================== VERSION HISTORY ====================

INSERT INTO biscuit_version_table (version, description) VALUES
('3.1.1', 'PostgreSQL 19 build support. No SQL-level or on-disk format change and no REINDEX required.')
ON CONFLICT (version) DO NOTHING;
