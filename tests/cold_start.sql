-- ============================================================================
-- COLD START BENCHMARK - The Honest Truth
-- ============================================================================

-- Step 1: Restart PostgreSQL to clear ALL caches
-- Run in terminal: sudo systemctl restart postgresql

-- Step 2: Clear OS page cache (optional but thorough)
-- Run in terminal: sync; echo 3 | sudo tee /proc/sys/vm/drop_caches

-- Step 3: Test cold-start performance
--\timing on

-- ==================== COLD START: pg_trgm ====================
DROP INDEX IF EXISTS idx_biscuit_name;
CREATE INDEX idx_trgm_name ON benchmark_data USING gin (name gin_trgm_ops);
ANALYZE benchmark_data;

-- First query after restart (COLD)
SELECT COUNT(*) FROM benchmark_data WHERE name LIKE 'a%t%';
-- Record this time

-- Second query (WARM)
SELECT COUNT(*) FROM benchmark_data WHERE name LIKE '%abc%';
-- Record this time

-- Third query (WARM)
SELECT COUNT(*) FROM benchmark_data WHERE name LIKE 'a%z';
-- Record this time

-- ==================== COLD START: Biscuit ====================
DROP INDEX IF EXISTS idx_trgm_name;

-- Biscuit rebuilds index on first access
CREATE INDEX idx_biscuit_name ON benchmark_data USING biscuit (name);
ANALYZE benchmark_data;

-- First query after restart (COLD - triggers rebuild)
SELECT COUNT(*) FROM benchmark_data WHERE name LIKE 'a%t%';
-- Record this time

-- Second query (WARM)
SELECT COUNT(*) FROM benchmark_data WHERE name LIKE 'a%z';
-- Record this time

-- Third query (WARM)
SELECT COUNT(*) FROM benchmark_data WHERE name LIKE 'a%z';
-- Record this time

--\timing off