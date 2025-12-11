-- ============================================================================
-- REAL MEMORY USAGE MEASUREMENT
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pg_buffercache;

-- After building BOTH indexes, measure actual buffer usage
SELECT
  c.relname AS index_name,
  count(*) AS buffer_pages,
  pg_size_pretty(count(*) * 8192) AS memory_used,
  pg_size_pretty(pg_relation_size(c.oid)) AS disk_size,
  -- CORRECTED CALCULATION using pg_size_bytes()
  ROUND(100.0 * count(*) / (
        SELECT pg_size_bytes(current_setting('shared_buffers')) / 8192
    ), 2) AS pct_of_shared_buffers_capacity
FROM pg_buffercache b
INNER JOIN pg_class c ON b.relfilenode = pg_relation_filenode(c.oid)
WHERE c.relname IN ('idx_biscuit_name', 'idx_trgm_name')
GROUP BY c.relname, c.oid
ORDER BY memory_used DESC;

-- Also show total shared_buffers usage
SELECT
  pg_size_pretty(COUNT(*) * 8192) AS total_buffers_used,
    -- CORRECTED: Use pg_size_bytes() to safely convert the string to bigint
  pg_size_pretty(pg_size_bytes(current_setting('shared_buffers'))) AS total_buffers_available,
    -- CORRECTED: Use pg_size_bytes() for the total capacity in the denominator
  ROUND(100.0 * COUNT(*) / (
        SELECT pg_size_bytes(current_setting('shared_buffers')) / 8192
    ), 2) AS pct_used
FROM pg_buffercache
WHERE reldatabase IS NOT NULL;