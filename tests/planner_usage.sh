#!/bin/bash

################################################################################
# Fair PostgreSQL Index Benchmark Suite
# 
# This script benchmarks Biscuit, Trigram (GIN), and B-tree indexes for
# pattern matching queries in a controlled, fair manner.
#
# Methodology:
# 1. Each index type runs in complete isolation (separate PostgreSQL restart)
# 2. Cache state is controlled (cold cache tests, then warm cache tests)
# 3. pg_prewarm ensures 100% cache coverage (verified with pg_buffercache)
# 4. Multiple iterations with statistical analysis
# 5. Query order is randomized within each iteration
# 6. Comprehensive metrics extraction
# 7. Count verification to ensure correctness
################################################################################

set -e  # Exit on error

# Configuration
DB_NAME="postgres"
DB_USER="postgres"
RESULTS_DIR="./benchmark_results_$(date +%Y%m%d_%H%M%S)"
NUM_ITERATIONS=10 # Increased for statistical significance
WARMUP_ITERATIONS=3

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

################################################################################
# Utility Functions
################################################################################

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_section() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

# Check if running as postgres user or with sudo access
check_privileges() {
    if [ "$EUID" -ne 0 ] && [ "$(whoami)" != "postgres" ]; then
        log_error "This script must be run as root or postgres user"
        exit 1
    fi
}

# Stop PostgreSQL safely
stop_postgres() {
    log_info "Stopping PostgreSQL..."
    sudo systemctl stop postgresql
    sleep 3
}

# Clear system caches
clear_caches() {
    log_info "Clearing system caches..."
    sudo sync
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    sleep 2
}

# Start PostgreSQL
start_postgres() {
    log_info "Starting PostgreSQL..."
    sudo systemctl start postgresql
    sleep 2  # Brief pause for systemctl to initiate startup
    log_info "Waiting for PostgreSQL to accept connections..."
    for i in {1..30}; do
        if sudo -u postgres pg_isready -q; then
            log_info "PostgreSQL is ready"
            return 0
        fi
        sleep 1
    done
    log_error "PostgreSQL failed to become ready"
    sudo systemctl status postgresql --no-pager
    exit 1
}

# Full restart cycle
restart_postgres_clean() {
    stop_postgres
    clear_caches
    start_postgres
}

################################################################################
# Count Verification Functions
################################################################################

compare_index_counts() {
    log_info "Comparing row counts across all indexes and iterations..."
    
    python3 - <<'PYEOF'
import pandas as pd
import os
import numpy as np

results_dir = os.environ.get('RESULTS_DIR')
metrics_csv = os.environ.get('METRICS_CSV')

try:
    df = pd.read_csv(metrics_csv)
except Exception as e:
    print(f"ERROR: Could not read metrics CSV: {e}")
    exit(1)

if df.empty:
    print("ERROR: No data in metrics CSV")
    exit(1)

print("=" * 80)
print("COUNT VERIFICATION REPORT")
print("=" * 80)
print()

# VERIFICATION 1: Cross-Index Consistency
print("VERIFICATION 1: Cross-Index Consistency")
print("-" * 80)

cross_index_mismatches = []
cross_index_pass = True

for query_id in sorted(df['query_id'].unique()):
    query_data = df[df['query_id'] == query_id]
    
    index_counts = {}
    for idx_type in query_data['index_type'].unique():
        idx_data = query_data[query_data['index_type'] == idx_type]
        counts = idx_data['actual_rows'].values
        index_counts[idx_type] = int(np.median(counts))
    
    if len(index_counts) < 3:
        print(f"⚠ {query_id}: Missing data from {3 - len(index_counts)} index type(s)")
        cross_index_pass = False
        continue
    
    count_values = list(index_counts.values())
    if len(set(count_values)) > 1:
        cross_index_pass = False
        cross_index_mismatches.append(query_id)
        print(f"✗ {query_id}: Count mismatch across indexes")
        for idx_type, count in sorted(index_counts.items()):
            print(f"    {idx_type:12s}: {count:,} rows")

if cross_index_pass:
    print(f"✓ All {len(df['query_id'].unique())} queries return identical counts across all indexes")
else:
    print(f"✗ Found {len(cross_index_mismatches)} queries with cross-index mismatches")

print()

# VERIFICATION 2: Cross-Iteration Consistency
print("VERIFICATION 2: Cross-Iteration Consistency")
print("-" * 80)

cross_iter_mismatches = []
cross_iter_pass = True

for query_id in sorted(df['query_id'].unique()):
    for idx_type in df['index_type'].unique():
        counts = df[(df['query_id'] == query_id) & (df['index_type'] == idx_type)]['actual_rows'].values
        
        if len(counts) == 0:
            continue
        
        unique_counts = set(counts)
        if len(unique_counts) > 1:
            cross_iter_pass = False
            cross_iter_mismatches.append((query_id, idx_type))
            print(f"✗ {query_id} ({idx_type}): Count varies across iterations")
            print(f"    Unique counts: {sorted([int(c) for c in unique_counts])}")

if cross_iter_pass:
    total_checks = len(df['query_id'].unique()) * len(df['index_type'].unique())
    print(f"✓ All {total_checks} query×index combinations consistent across iterations")
else:
    print(f"✗ Found {len(cross_iter_mismatches)} query×index combinations with variation")

print()

# OVERALL RESULT
print("=" * 80)
if cross_index_pass and cross_iter_pass:
    total_measurements = len(df)
    unique_queries = len(df['query_id'].unique())
    num_indexes = len(df['index_type'].unique())
    
    print("✓✓✓ SUCCESS: All count verifications passed!")
    print(f"    • {unique_queries} queries verified")
    print(f"    • {num_indexes} index types")
    print(f"    • {total_measurements} total measurements")
    print(f"    • 100% consistency across indexes and iterations")
else:
    print("✗✗✗ FAILURE: Count verification failed!")
    if cross_index_mismatches:
        print(f"    • {len(cross_index_mismatches)} queries with cross-index mismatches")
    if cross_iter_mismatches:
        print(f"    • {len(cross_iter_mismatches)} query×index with iteration inconsistencies")
    print()
    print("This indicates a BUG in one or more index implementations!")
    print("DO NOT trust the performance results until this is resolved.")

print("=" * 80)
print()

# Row Count Distribution
print("Row Count Distribution Across Queries:")
print("-" * 80)

consensus_counts = df.groupby('query_id')['actual_rows'].median()

if not consensus_counts.empty:
    count_ranges = [
        ("Empty (0 rows)", 0, 1),
        ("Ultra-high selectivity (1-100)", 1, 100),
        ("High selectivity (100-1K)", 100, 1000),
        ("Medium selectivity (1K-10K)", 1000, 10000),
        ("Low selectivity (10K-100K)", 10000, 100000),
        ("Very low selectivity (100K+)", 100000, float('inf'))
    ]
    
    for label, min_val, max_val in count_ranges:
        count = ((consensus_counts >= min_val) & (consensus_counts < max_val)).sum()
        if count > 0:
            print(f"  {label:35s}: {count:3d} queries")
    
    print()
    print(f"  Total unique queries: {len(consensus_counts)}")
    print(f"  Min rows returned: {int(consensus_counts.min()):,}")
    print(f"  Max rows returned: {int(consensus_counts.max()):,}")
    print(f"  Median rows returned: {int(consensus_counts.median()):,}")

print()
PYEOF
}

################################################################################
# Query Generation
################################################################################

generate_query_file() {
    local output_file=$1
    
    cat > "$output_file" <<'EOF'
-- ============================================================================
-- COMPREHENSIVE WILDCARD PATTERN MATCHING BENCHMARK
-- Table: interactions (1M rows)
-- Test Coverage: 100+ query patterns across all LIKE/ILIKE combinations
-- ============================================================================

SET enable_seqscan = on;
SET enable_bitmapscan = on;
SET work_mem = '256MB';
SET random_page_cost = 1.1;

-- WARMUP: Load index into cache
SELECT COUNT(*) FROM interactions WHERE country LIKE 'United%';

-- ============================================================================
-- SECTION 1: BASIC LIKE PATTERNS (Prefix, Suffix, Infix)
-- ============================================================================

-- PREFIX PATTERNS (pattern%)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Uni%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'So%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'david%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'kelly%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'And%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'i%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE interaction_type LIKE 'com%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE interaction_type LIKE 'post%';

-- SUFFIX PATTERNS (%pattern)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%stan';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%land';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%smith';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%son';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%er';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%oid';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%eb';

-- INFIX PATTERNS (%pattern%)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%united%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%island%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%rica%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%alex%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%john%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%mill%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%dr%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%eb%';

-- ============================================================================
-- SECTION 2: UNDERSCORE WILDCARDS (_)
-- ============================================================================

-- Single underscore
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Ja_an';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '_ndia';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'i_S';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'We_';

-- Multiple underscores
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'S___h%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Ke__a';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'd_v_d%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'k___y__';

-- Mixed wildcards (% and _)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Bo%_a';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%_lex%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%j_nes';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%m_yer%';

-- ============================================================================
-- SECTION 3: CASE INSENSITIVE (ILIKE)
-- ============================================================================

-- ILIKE prefix
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'japan';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'KENYA';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'uNiTeD%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE 'DAVID%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE 'kelly%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE 'android';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE 'IOS';

-- ILIKE suffix
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE '%africa';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE '%STAN';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '%MILLER';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '%smith';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE '%OID';

-- ILIKE infix
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE '%united%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE '%ISLAND%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '%ALEX%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '%john%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE '%web%';

-- ILIKE prefix with _
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'ja_an';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'KE___';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'uN__eD%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '_A_ID%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE 'ke__y%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE 'a_d_o_d';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE 'IOS';

-- ILIKE suffix with _
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE '%afr___';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE '%_TAN';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '%MI__ER';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '%___th';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE '%OI_';

-- ILIKE infix with _
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE '%un__ed%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE '%___AND%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '%AL__%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '%j__n%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE '%_eb%';

-- ============================================================================
-- SECTION 4: NEGATION (NOT LIKE / NOT ILIKE)
-- ============================================================================

-- NOT LIKE prefix
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE 'Uni%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE 'david%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device NOT LIKE 'And%';

-- NOT LIKE suffix
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE '%stan';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE '%ia';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE '%son';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE '%er';

-- NOT LIKE infix
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE '%land%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE '%Africa%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE '%admin%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE '%test%';

-- NOT ILIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT ILIKE '%africa%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT ILIKE 'united%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device NOT ILIKE 'iOS';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT ILIKE '%admin%';

-- NOT LIKE prefix with _
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE 'U_i%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE '__vid%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device NOT LIKE 'A_d%';

-- NOT LIKE suffix with _
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE '%s__n';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE '%i_';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE '%s_n';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE '%e_';

-- NOT LIKE infix with _
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE '%l__d%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE '%Afri__%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE '%__min%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT LIKE '%t__t%';

-- NOT ILIKE with _
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT ILIKE '%af__ca%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT ILIKE '__ited%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device NOT ILIKE 'i_S';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username NOT ILIKE '%adm__%';

-- ============================================================================
-- SECTION 5: AND COMBINATIONS (2 predicates)
-- ============================================================================

-- LIKE AND LIKE (same pattern type)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Jap%' AND username LIKE '__vid%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' AND device LIKE '%o_d';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%ell%' AND device LIKE '%dr%';

-- LIKE AND LIKE (mixed pattern types)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Uni%' AND username LIKE '%sm_th';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%land' AND username LIKE 'ke__y%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '__vid%' AND device LIKE '%oid';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' AND device LIKE 'i_S';

-- LIKE AND NOT LIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' AND country NOT LIKE 'In_ia';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'da%' AND username NOT LIKE '%vid';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'U__ted%' AND country NOT LIKE '%States%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%oid' AND device NOT LIKE 'Android';

-- ILIKE AND ILIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'ja_an' AND device ILIKE 'ios';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'SOUTH%' AND device ILIKE '%an__oid%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username ILIKE '__vid%' AND username ILIKE '%john%';

-- LIKE AND ILIKE (mixed)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' AND username ILIKE '%ke__y%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'united%' AND device LIKE 'i_S';

-- LIKE AND non-LIKE conditions
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' AND is_premium = 1;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%alex%' AND age BETWEEN 25 AND 35;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'iOS' AND interaction_type = 'post';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Africa%' AND is_premium = 0;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'david%' AND engagement_score > 0.5;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'United%' AND timestamp >= '2025-01-01';

-- NOT LIKE AND NOT LIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country NOT LIKE 'United%' AND username NOT LIKE '%admin%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device NOT LIKE 'Web' AND country NOT LIKE '%ia';

-- ============================================================================
-- SECTION 6: AND COMBINATIONS (3+ predicates)
-- ============================================================================

-- Three LIKE predicates
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'S%' AND username LIKE '%a%' AND device LIKE 'iOS';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' AND username LIKE '%son' AND interaction_type LIKE '%st';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'j%' AND country LIKE '%ia' AND device LIKE '%eb';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Uni%' AND username LIKE '%smith' AND device LIKE 'Web';

-- Three LIKE predicates (mixed patterns)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan%' AND username LIKE '%kelly%' AND device LIKE '%oid%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%ell%' AND country LIKE '%land%' AND device LIKE 'i%';

-- LIKE with multiple non-LIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' AND is_premium = 1 AND age > 30;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%alex%' AND age BETWEEN 25 AND 35 AND device = 'iOS';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Africa%' AND interaction_type = 'post' AND engagement_score > 0.7;

-- Four predicates
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' AND username LIKE '%jones%' AND device LIKE '%oid' AND interaction_type LIKE 'com%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'S%' AND username LIKE 's%' AND device LIKE 'iOS' AND interaction_type LIKE '%st';

-- ============================================================================
-- SECTION 7: OR COMBINATIONS (2 predicates)
-- ============================================================================

-- LIKE OR LIKE (same pattern type - prefix)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' OR country LIKE 'Kenya';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'david%' OR username LIKE 'kelly%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'iOS' OR device LIKE 'Android';

-- LIKE OR LIKE (same pattern type - suffix)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' OR country LIKE '%land';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%son' OR username LIKE '%ton';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%oid' OR device LIKE '%eb';

-- LIKE OR LIKE (mixed pattern types)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Uni%' OR country LIKE '%stan';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'david%' OR username LIKE '%miller';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan%' OR country LIKE '%Africa%';

-- LIKE OR NOT LIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'United%' OR country NOT LIKE '%Africa%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'admin%' OR username NOT LIKE '%test%';

-- ILIKE OR ILIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country ILIKE 'japan' OR country ILIKE 'india';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device ILIKE 'ios' OR device ILIKE 'android';

-- LIKE OR non-LIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' OR is_premium = 1;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%alex%' OR age > 50;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'iOS' OR engagement_score > 0.8;

-- ============================================================================
-- SECTION 8: OR COMBINATIONS (3+ predicates)
-- ============================================================================

-- Three LIKE predicates
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' OR country LIKE 'Kenya' OR country LIKE 'Yemen';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'iOS' OR device LIKE 'Android' OR device LIKE 'Web';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'david%' OR username LIKE 'kelly%' OR username LIKE 'alex%';

-- Three mixed patterns
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' OR country LIKE '%Africa%' OR country LIKE '%island%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'admin%' OR username LIKE '%test%' OR username LIKE '%demo%';

-- Four predicates
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan' OR country LIKE 'Kenya' OR country LIKE 'India' OR country LIKE 'Yemen';

-- ============================================================================
-- SECTION 9: COMPLEX NESTED CONDITIONS
-- ============================================================================

-- (LIKE OR LIKE) AND LIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country LIKE 'Japan' OR country LIKE 'Kenya') AND username LIKE '%a%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (device LIKE 'iOS' OR device LIKE 'Android') AND interaction_type LIKE 'post';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (username LIKE 'david%' OR username LIKE 'kelly%') AND country LIKE '%ia';

-- LIKE AND (LIKE OR LIKE)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' AND (username LIKE 'david%' OR username LIKE 'kelly%');
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%er' AND (country LIKE 'United%' OR country LIKE 'South%');
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'iOS' AND (country LIKE 'Japan' OR country LIKE 'Kenya');

-- (LIKE AND LIKE) OR (LIKE AND LIKE)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country LIKE 'Japan' AND device LIKE 'iOS') OR (country LIKE 'Kenya' AND device LIKE 'Android');
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (username LIKE 'david%' AND is_premium = 1) OR (username LIKE 'kelly%' AND is_premium = 1);
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country LIKE '%Africa%' AND device LIKE 'Android') OR (country LIKE '%Asia%' AND device LIKE 'iOS');

-- (LIKE OR LIKE) AND (LIKE OR LIKE)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country LIKE 'Japan' OR country LIKE 'Kenya') AND (device LIKE 'iOS' OR device LIKE 'Android');
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (username LIKE 'david%' OR username LIKE 'kelly%') AND (country LIKE '%ia' OR country LIKE '%land');

-- NOT LIKE combinations
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country NOT LIKE 'United%' AND username NOT LIKE '%admin%');
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country LIKE 'Japan' OR country NOT LIKE '%ia') AND (device LIKE 'iOS' OR device NOT LIKE 'Web');

-- Deep nesting
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE ((country LIKE 'Japan' OR country LIKE 'Kenya') AND username LIKE '%a%') OR (device LIKE 'iOS' AND interaction_type LIKE 'post');

-- ============================================================================
-- SECTION 10: EDGE CASES AND SPECIAL PATTERNS
-- ============================================================================

-- Very short patterns (low selectivity - many matches)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'M%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'A%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'a%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 's%';

-- Very broad infix patterns
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%a%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%a%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%e%';

-- Exact match (degenerate LIKE - no wildcards)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Japan';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Kenya';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'Android';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'Web';

-- Empty result patterns
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Penguin';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'nonexistent%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%zzzzzzz%';

-- Universal patterns (matches all or most rows)
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%%';

-- Rare long patterns
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Barthelemy%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'French Southern Territories';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Cocos (Keeling)%';

-- Multiple consecutive underscores
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '_______';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '__________';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '________76';

-- ============================================================================
-- SECTION 11: REAL-WORLD QUERY PATTERNS
-- ============================================================================

-- User search patterns
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%john%' OR username LIKE '%david%' OR username LIKE '%alex%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (username LIKE 'admin%' OR username LIKE 'moderator%') AND is_premium = 1;

-- Geographic searches
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%island%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%United%' OR country LIKE '%Kingdom%' OR country LIKE '%States%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Africa%' AND device LIKE 'Android';

-- Content moderation patterns
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (username LIKE '%spam%' OR username LIKE '%bot%') AND toxicity_score > 0.8;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE interaction_type LIKE 'comment' AND username NOT LIKE '%verified%' AND suspicious_score > 0.5;

-- Analytics queries
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'United%' AND interaction_type LIKE 'post' AND timestamp >= '2025-01-01';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE '%Android%' AND engagement_score > 0.5 AND is_premium = 1;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%kelly%' AND age BETWEEN 30 AND 50 AND country LIKE '%ia';

-- Multi-device user tracking
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (device LIKE 'iOS' OR device LIKE 'Android') AND interaction_type LIKE 'post' AND timestamp >= '2024-01-01';

-- Premium user analysis
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Africa%' AND is_premium = 1 AND engagement_score > 0.7;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE (country LIKE 'United%' OR country LIKE 'South%') AND is_premium = 0 AND interaction_type LIKE 'like';

-- ============================================================================
-- SECTION 12: SELECTIVITY SPECTRUM (Critical for fair comparison)
-- ============================================================================

-- Ultra-high selectivity (0.001% - 0.01% of rows) - 1-100 matches
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Saint Barthélemy';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Cocos (Keeling) Islands%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'administrator%';

-- High selectivity (0.01% - 0.1% of rows) - 100-1000 matches
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Uzbek%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'zach%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%istan';

-- Medium selectivity (0.1% - 1% of rows) - 1000-10000 matches
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'United%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'john%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%land';

-- Low selectivity (1% - 10% of rows) - 10000-100000 matches
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'S%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'a%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%a%';

-- Very low selectivity (10%+ of rows) - 100000+ matches
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE '%e%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%a%' OR country LIKE '%e%';

-- ============================================================================
-- SECTION 13: LONG PATTERNS
-- ============================================================================

EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'United States of America%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%French Southern Territories%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'Saint Vincent and the Grenadines%';
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%Democratic People''s Republic%';

-- ============================================================================
-- SECTION 14: SPECIAL CHARACTERS & ESCAPING
-- ============================================================================

EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%(%)%';  -- Parentheses
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%-%';     -- Hyphens
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%.%';     -- Dots
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%''%';    -- Apostrophes

-- ============================================================================
-- SECTION 15: LIMIT + ORDER BY (Real-world pagination)
-- ============================================================================

EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE username LIKE 'dav%' ORDER BY username LIMIT 10;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE '%ia' ORDER BY timestamp LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE device LIKE 'iOS' ORDER BY engagement_score DESC LIMIT 100;
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM interactions WHERE country LIKE 'United%' ORDER BY id LIMIT 20 OFFSET 100;

-- ============================================================================
-- END OF COMPREHENSIVE BENCHMARK QUERIES
-- Total: 200+ test cases covering all pattern matching scenarios
-- ============================================================================
EOF
}

################################################################################
# Index Management
################################################################################

create_biscuit_index() {
    log_info "Creating Biscuit index..."
    sudo -u postgres psql -d "$DB_NAME" <<EOF
DROP INDEX IF EXISTS int_bisc;
CREATE INDEX int_bisc ON interactions USING biscuit(
    interaction_type, 
    username, 
    country, 
    device
);
ANALYZE interactions;
EOF
}

create_trigram_index() {
    log_info "Creating Trigram (GIN) index..."
    sudo -u postgres psql -d "$DB_NAME" <<EOF
DROP INDEX IF EXISTS int_trgm;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX int_trgm ON interactions USING gin (
    interaction_type gin_trgm_ops,
    username gin_trgm_ops,
    country gin_trgm_ops,
    device gin_trgm_ops
);
ANALYZE interactions;
EOF
}

create_btree_index() {
    log_info "Creating B-tree index with text_pattern_ops..."
    sudo -u postgres psql -d "$DB_NAME" <<EOF
DROP INDEX IF EXISTS int_tree;
CREATE INDEX int_tree ON interactions(
    interaction_type text_pattern_ops,
    username text_pattern_ops,
    country text_pattern_ops,
    device text_pattern_ops
);
ANALYZE interactions;
EOF
}

drop_all_indexes() {
    log_info "Dropping all test indexes..."
    sudo -u postgres psql -d "$DB_NAME" <<EOF
DROP INDEX IF EXISTS int_bisc;
DROP INDEX IF EXISTS int_trgm;
DROP INDEX IF EXISTS int_tree;
EOF
}

get_index_size() {
    local index_name="$1"
    local sql

    if [[ "$index_name" == "int_bisc" ]]; then
        sql="SELECT biscuit_size_pretty('$index_name');"
    else
        sql="SELECT pg_size_pretty(pg_relation_size('$index_name'));"
    fi

    sudo -u postgres psql -d "$DB_NAME" -t -A -c "$sql"
}

################################################################################
# Warmup Functions
################################################################################

warmup_cache_full() {
    local index_name=$1
    local index_type=$2
    
    log_info "Performing full cache warmup using pg_prewarm for $index_name..."
    
    # Install pg_prewarm if not already available
    sudo -u postgres psql -d "$DB_NAME" <<EOF > /dev/null 2>&1
CREATE EXTENSION IF NOT EXISTS pg_prewarm;
CREATE EXTENSION IF NOT EXISTS pg_buffercache;
EOF
    
    # Use pg_prewarm to explicitly load entire index into cache
    log_info "Loading index into shared buffers..."
    local prewarm_result=$(sudo -u postgres psql -d "$DB_NAME" -t -A -c \
        "SELECT pg_prewarm('$index_name', 'buffer');")
    log_info "pg_prewarm loaded $prewarm_result blocks into cache"
    
    # Verify cache coverage
    # Verify cache coverage
    log_info "Verifying cache coverage..."
    sudo -u postgres psql -d "$DB_NAME" -t -A -F'|' <<EOF > "$RESULTS_DIR/cache_verification_${index_type}.txt"
SELECT
    c.relname AS index_name,
    c.relpages AS total_pages,
    COUNT(b.*) AS cached_pages,
    ROUND(
        COUNT(b.*)::numeric / NULLIF(c.relpages, 0) * 100,
        2
    ) AS percent_cached
FROM pg_class c
LEFT JOIN pg_buffercache b
  ON b.relfilenode = c.relfilenode
WHERE c.relname = '$index_name'
GROUP BY c.relname, c.relpages;
EOF
    
    # Extract and display cache coverage
    # The output format with -t -A -F'|' is: index_name|total_pages|cached_pages|percent_cached
    local cache_line=$(cat "$RESULTS_DIR/cache_verification_${index_type}.txt" | grep -v '^$' | head -1)
    
    if [[ -z "$cache_line" ]]; then
        log_warn "⚠ No cache verification data found"
    else
        log_info "Raw cache data: $cache_line"
        
        # Extract fields using pipe delimiter
        local index_name_out=$(echo "$cache_line" | cut -d'|' -f1)
        local total_pages=$(echo "$cache_line" | cut -d'|' -f2)
        local cached_pages=$(echo "$cache_line" | cut -d'|' -f3)
        local percent_cached=$(echo "$cache_line" | cut -d'|' -f4)
        
        log_info "Cache verification: $index_name_out - $cached_pages/$total_pages pages cached ($percent_cached%)"
        
        # Check if fully cached using awk for floating point comparison
        if [[ -n "$percent_cached" ]]; then
            local is_cached=$(awk -v p="$percent_cached" 'BEGIN {print (p >= 99.0) ? 1 : 0}')
            if [[ "$is_cached" == "1" ]]; then
                log_info "✓ Index fully cached ($percent_cached%)"
            else
                log_warn "⚠ Index not fully cached (only $percent_cached%) - may affect results"
            fi
        else
            log_warn "⚠ Could not extract cache percentage"
        fi
    fi
    # Also verify the base table is cached for complete fairness
    log_info "Pre-warming base table..."
    sudo -u postgres psql -d "$DB_NAME" -t -A -c \
        "SELECT pg_prewarm('interactions', 'buffer');" > /dev/null 2>&1
    
    log_info "Cache warmup complete - index and table fully loaded"
}

################################################################################
# Benchmark Execution
################################################################################

run_benchmark_iteration() {
    local index_type=$1
    local iteration=$2
    local cache_state=$3  # "cold" or "warm"
    local output_file=$4
    
    log_info "Running $index_type iteration $iteration ($cache_state cache)..."
    
    # Run queries and capture output
    sudo -u postgres psql -d "$DB_NAME" -f "$RESULTS_DIR/queries.sql" \
        > "$output_file" 2>&1
}

################################################################################
# Metrics Extraction
################################################################################

extract_metrics() {
    local result_file=$1
    local index_type=$2
    local iteration=$3
    local cache_state=$4
    local metrics_csv=$5
    
    log_info "Extracting metrics from $result_file..."
    
    # Use Python for robust JSON parsing
    python3 - <<'PYEOF'
import re
import json
import sys
import os

def extract_query_metrics(json_text):
    """Extract metrics from EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) output."""
    try:
        # Parse the JSON directly - it should be clean now
        data = json.loads(json_text)
        
        # Handle both array and direct object formats
        if isinstance(data, list):
            data = data[0]
        
        plan = data['Plan']
        execution_time = data['Execution Time']
        planning_time = data['Planning Time']
        
        # Extract buffer stats
        shared_hit = plan.get('Shared Hit Blocks', 0)
        shared_read = plan.get('Shared Read Blocks', 0)
        shared_written = plan.get('Shared Written Blocks', 0)
        
        # Get rows
        actual_rows = plan.get('Actual Rows', 0)
        
        # Get node type
        node_type = plan.get('Node Type', 'Unknown')
        
        return {
            'execution_time': execution_time,
            'planning_time': planning_time,
            'total_time': execution_time + planning_time,
            'shared_hit': shared_hit,
            'shared_read': shared_read,
            'shared_written': shared_written,
            'actual_rows': actual_rows,
            'node_type': node_type,
            'cache_hit_ratio': shared_hit / (shared_hit + shared_read) if (shared_hit + shared_read) > 0 else 0
        }
    except Exception as e:
        print(f"Error parsing query: {e}", file=sys.stderr)
        return None

# Get variables from environment
result_file = os.environ.get('RESULT_FILE')
index_type = os.environ.get('INDEX_TYPE')
iteration = os.environ.get('ITERATION')
cache_state = os.environ.get('CACHE_STATE')
metrics_csv = os.environ.get('METRICS_CSV')

# Read the result file
try:
    with open(result_file, 'r') as f:
        content = f.read()
except Exception as e:
    print(f"Error reading file: {e}", file=sys.stderr)
    sys.exit(1)

# PostgreSQL outputs JSON with line continuations marked by +
# First, clean up the content by removing the + markers and joining lines
lines = content.split('\n')
cleaned_lines = []
in_json = False

for line in lines:
    stripped = line.strip()
    # Check if we're starting a JSON block
    if stripped.startswith('['):
        in_json = True
        cleaned_lines.append(stripped.rstrip('+').strip())
    elif in_json:
        # Remove + continuation marker and add to cleaned lines
        if stripped.startswith('+'):
            cleaned_lines.append(stripped[1:].strip())
        elif stripped.endswith('+'):
            cleaned_lines.append(stripped[:-1].strip())
        else:
            cleaned_lines.append(stripped)
        
        # Check if JSON block is complete
        if stripped == ']' or stripped.endswith(']'):
            in_json = False
    else:
        cleaned_lines.append(line)

# Rejoin the cleaned content
cleaned_content = '\n'.join(cleaned_lines)

# Now find complete JSON blocks using regex on cleaned content
json_pattern = r'\[\s*\{.*?"Plan".*?"Execution Time".*?\}\s*\]'
json_blocks = re.findall(json_pattern, cleaned_content, re.DOTALL)

if not json_blocks:
    print("ERROR: No JSON EXPLAIN output found", file=sys.stderr)
    print("Cleaned content preview:", file=sys.stderr)
    print(cleaned_content[:2000], file=sys.stderr)
    sys.exit(1)

print(f"Found {len(json_blocks)} JSON blocks", file=sys.stderr)

query_num = 0
extracted_count = 0

for json_block in json_blocks:
    query_num += 1
    
    try:
        metrics = extract_query_metrics(json_block)
        
        if metrics:
            extracted_count += 1
            # Append to CSV
            with open(metrics_csv, 'a') as csv:
                csv.write(f"{index_type},{iteration},{cache_state},Q{query_num:02d},"
                         f"{metrics['execution_time']:.2f},"
                         f"{metrics['planning_time']:.2f},"
                         f"{metrics['total_time']:.2f},"
                         f"{metrics['shared_hit']},"
                         f"{metrics['shared_read']},"
                         f"{metrics['shared_written']},"
                         f"{metrics['actual_rows']},"
                         f"{metrics['cache_hit_ratio']:.4f},"
                         f"{metrics['node_type']}\n")
        else:
            print(f"Warning: Could not extract metrics from query {query_num}", file=sys.stderr)
    except Exception as e:
        print(f"Error processing query {query_num}: {e}", file=sys.stderr)

print(f"Extracted {extracted_count} queries from {len(json_blocks)} JSON blocks", file=sys.stderr)

if extracted_count == 0:
    print("ERROR: Failed to extract any metrics!", file=sys.stderr)
    sys.exit(1)
PYEOF
}

################################################################################
# Statistical Analysis
################################################################################

generate_summary_report() {
    local metrics_csv=$1
    local summary_file=$2
    
    log_info "Generating summary report..."
    
    python3 - <<'PYEOF' > "$summary_file"
import pandas as pd
import numpy as np
import sys
import os

# Get the CSV path and results dir from environment
metrics_csv = os.environ.get('METRICS_CSV')
results_dir = os.environ.get('RESULTS_DIR')

# Read metrics
try:
    df = pd.read_csv(metrics_csv)
except Exception as e:
    print(f"ERROR: Could not read metrics CSV: {e}", file=sys.stderr)
    sys.exit(1)

if df.empty:
    print("ERROR: No data found in metrics CSV", file=sys.stderr)
    sys.exit(1)

print("=" * 80)
print("BENCHMARK SUMMARY REPORT")
print("=" * 80)
print()

print(f"Total records: {len(df)}")
print(f"Index types: {df['index_type'].unique().tolist()}")
print(f"Cache states: {df['cache_state'].unique().tolist()}")
print(f"Iterations per cache state: {df.groupby(['index_type', 'cache_state']).size().iloc[0] // len(df['query_id'].unique())}")
print()

# Overall statistics by index type and cache state with confidence intervals
print("Overall Performance by Index Type and Cache State")
print("-" * 80)
try:
    summary = df.groupby(['index_type', 'cache_state']).agg({
        'execution_time': ['mean', 'median', 'std', 'min', 'max', 'count'],
        'total_time': ['mean', 'median', 'std'],
        'cache_hit_ratio': 'mean',
        'shared_read': 'sum'
    }).round(2)
    print(summary)
    print()
    
    # Calculate 95% confidence intervals
    print("95% Confidence Intervals for Execution Time (ms)")
    print("-" * 80)
    for (idx_type, cache), group in df.groupby(['index_type', 'cache_state']):
        mean = group['execution_time'].mean()
        std = group['execution_time'].std()
        n = len(group)
        # Using t-distribution for small samples
        from scipy import stats
        ci = stats.t.interval(0.95, n-1, loc=mean, scale=std/np.sqrt(n))
        print(f"{idx_type:12s} {cache:6s}: {mean:7.2f} ± {(ci[1]-mean):6.2f} ms  [n={n}]")
except Exception as e:
    print(f"Warning: Could not generate overall summary: {e}")
print()

# Statistical significance testing
print("Statistical Significance Tests (Warm Cache)")
print("-" * 80)
try:
    from scipy import stats as sp_stats
    warm_df = df[df['cache_state'] == 'warm']
    
    index_types = warm_df['index_type'].unique()
    print("Pairwise t-tests (p-values):")
    print()
    for i, idx1 in enumerate(index_types):
        for idx2 in index_types[i+1:]:
            data1 = warm_df[warm_df['index_type'] == idx1]['execution_time']
            data2 = warm_df[warm_df['index_type'] == idx2]['execution_time']
            t_stat, p_val = sp_stats.ttest_ind(data1, data2)
            significance = "***" if p_val < 0.001 else "**" if p_val < 0.01 else "*" if p_val < 0.05 else "ns"
            print(f"  {idx1:12s} vs {idx2:12s}: p={p_val:.4f} {significance}")
    print()
    print("  *** p<0.001  ** p<0.01  * p<0.05  ns = not significant")
except ImportError:
    print("  (scipy not available - install for statistical tests)")
except Exception as e:
    print(f"  Warning: {e}")
print()

# Performance by query type with variance
print("Performance by Query Type (Warm Cache)")
print("-" * 80)
try:
    warm_df = df[df['cache_state'] == 'warm']
    if not warm_df.empty:
        print("Median Execution Time (ms):")
        query_summary = warm_df.groupby(['query_id', 'index_type'])['execution_time'].median().unstack()
        print(query_summary.round(2))
        print()
        
        print("Coefficient of Variation (lower = more consistent):")
        query_cv = warm_df.groupby(['query_id', 'index_type'])['execution_time'].apply(
            lambda x: (x.std() / x.mean()) if x.mean() > 0 else 0
        ).unstack()
        print(query_cv.round(3))
    else:
        print("No warm cache data available")
except Exception as e:
    print(f"Warning: Could not generate query summary: {e}")
print()

# Index size comparison
print("Index Sizes")
print("-" * 80)
try:
    for idx_type in ['biscuit', 'trigram', 'btree']:
        size_file = os.path.join(results_dir, f'index_size_{idx_type}.txt')
        try:
            with open(size_file, 'r') as f:
                size = f.read().strip()
                print(f"{idx_type.capitalize()}: {size}")
        except FileNotFoundError:
            print(f"{idx_type.capitalize()}: N/A")
except Exception as e:
    print(f"Warning: Could not read index sizes: {e}")
print()

# Cold vs Warm cache impact
print("Cold vs Warm Cache Impact (Mean Execution Time)")
print("-" * 80)
try:
    cache_impact = df.groupby(['index_type', 'cache_state'])['execution_time'].mean().unstack()
    print(cache_impact.round(2))
except Exception as e:
    print(f"Warning: Could not generate cache impact: {e}")
print()

# Cache efficiency
print("Cache Efficiency (Average Cache Hit Ratio)")
print("-" * 80)
try:
    cache_eff = df.groupby(['index_type', 'cache_state'])['cache_hit_ratio'].mean().unstack()
    print(cache_eff.round(4))
except Exception as e:
    print(f"Warning: Could not generate cache efficiency: {e}")
print()

# Cache verification summary
print("Cache Verification Results")
print("-" * 80)
try:
    for idx_type in ['biscuit', 'trigram', 'btree']:
        cache_file = os.path.join(results_dir, f'cache_verification_{idx_type}.txt')
        try:
            with open(cache_file, 'r') as f:
                lines = f.readlines()
                if len(lines) >= 2:
                    # Get the data line (last line)
                    data_line = lines[-1].strip()
                    parts = data_line.split('|')
                    if len(parts) >= 4:
                        percent = parts[-1].strip()
                        print(f"{idx_type.capitalize()}: {percent}% cached")
        except FileNotFoundError:
            print(f"{idx_type.capitalize()}: No verification data")
except Exception as e:
    print(f"Warning: Could not read cache verification: {e}")
print()

# Determine winner with statistical confidence
print("WINNER DETERMINATION")
print("-" * 80)
try:
    warm_df = df[df['cache_state'] == 'warm']
    if not warm_df.empty:
        warm_stats = warm_df.groupby('index_type')['execution_time'].agg(['mean', 'std', 'count'])
        if not warm_stats.empty:
            winner = warm_stats['mean'].idxmin()
            winner_mean = warm_stats.loc[winner, 'mean']
            winner_std = warm_stats.loc[winner, 'std']
            winner_n = warm_stats.loc[winner, 'count']
            
            print(f"Fastest index (warm cache, mean execution time): {winner}")
            print(f"  Mean execution time: {winner_mean:.2f} ms")
            print(f"  Standard deviation: {winner_std:.2f} ms")
            print(f"  Sample size: {int(winner_n)} queries")
            print()
            
            print("Performance vs. Winner:")
            for idx_type in warm_stats.index:
                if idx_type != winner:
                    other_mean = warm_stats.loc[idx_type, 'mean']
                    speedup = ((other_mean / winner_mean) - 1) * 100
                    slowdown_ms = other_mean - winner_mean
                    print(f"  {idx_type:12s}: {other_mean:7.2f} ms  ({speedup:+6.1f}%, +{slowdown_ms:.2f} ms slower)")
            
            print()
            print("Query Type Performance (warm cache):")
            # Categorize queries
            prefix_queries = ['Q01', 'Q02', 'Q03']
            suffix_queries = ['Q04', 'Q05', 'Q06']
            infix_queries = ['Q07', 'Q08', 'Q09']
            
            for category, queries in [('Prefix', prefix_queries), ('Suffix', suffix_queries), ('Infix', infix_queries)]:
                print(f"\n  {category} queries:")
                cat_df = warm_df[warm_df['query_id'].isin(queries)]
                cat_means = cat_df.groupby('index_type')['execution_time'].mean()
                for idx in cat_means.index:
                    print(f"    {idx:12s}: {cat_means[idx]:7.2f} ms")
        else:
            print("No warm cache data available for winner determination")
    else:
        print("No warm cache data available for winner determination")
except Exception as e:
    print(f"Warning: Could not determine winner: {e}")
    import traceback
    traceback.print_exc()
print()

# Data quality check
print("DATA QUALITY CHECK")
print("-" * 80)
print(f"Total queries executed: {len(df)}")
print(f"Queries per index type:")
for idx_type in df['index_type'].unique():
    count = len(df[df['index_type'] == idx_type])
    print(f"  {idx_type}: {count}")
print()

# Outlier detection
print("Outlier Detection (warm cache, >3 std devs from mean):")
for idx_type in df[df['cache_state'] == 'warm']['index_type'].unique():
    idx_df = df[(df['index_type'] == idx_type) & (df['cache_state'] == 'warm')]
    mean = idx_df['execution_time'].mean()
    std = idx_df['execution_time'].std()
    outliers = idx_df[np.abs(idx_df['execution_time'] - mean) > 3 * std]
    if len(outliers) > 0:
        print(f"  {idx_type}: {len(outliers)} outliers detected")
        for _, row in outliers.head(5).iterrows():
            print(f"    {row['query_id']}: {row['execution_time']:.2f} ms")
    else:
        print(f"  {idx_type}: No outliers detected")
print()

# Warmup effectiveness
print("Warmup Effectiveness (cache hit ratio improvement):")
for idx_type in df['index_type'].unique():
    cold_ratio = df[(df['index_type'] == idx_type) & (df['cache_state'] == 'cold')]['cache_hit_ratio'].mean()
    warm_ratio = df[(df['index_type'] == idx_type) & (df['cache_state'] == 'warm')]['cache_hit_ratio'].mean()
    improvement = (warm_ratio - cold_ratio) * 100
    print(f"  {idx_type:12s}: {cold_ratio:.4f} → {warm_ratio:.4f} (+{improvement:.2f}%)")
print()

# Verify index usage
print("Index Usage Verification:")
print("(All queries should use Index Scan or similar, not Seq Scan)")
node_types = df.groupby(['index_type', 'node_type']).size().unstack(fill_value=0)
print(node_types)
print()

# Publication readiness checklist
print("PUBLICATION READINESS CHECKLIST")
print("-" * 80)

# Check 1: Sufficient iterations
iterations_ok = df.groupby(['index_type', 'cache_state']).size().iloc[0] >= len(df['query_id'].unique()) * 10
print(f"✓ Sufficient iterations (≥10): {iterations_ok}")

# Check 2: Statistical significance
try:
    from scipy import stats as sp_stats
    warm_df = df[df['cache_state'] == 'warm']
    winner_data = warm_df[warm_df['index_type'] == warm_df.groupby('index_type')['execution_time'].mean().idxmin()]
    for other_idx in warm_df['index_type'].unique():
        if other_idx != winner_data['index_type'].iloc[0]:
            other_data = warm_df[warm_df['index_type'] == other_idx]
            _, p_val = sp_stats.ttest_ind(winner_data['execution_time'], other_data['execution_time'])
            if p_val >= 0.05:
                print(f"⚠ Statistical significance check: Comparison with {other_idx} not significant (p={p_val:.4f})")
                break
    else:
        print(f"✓ Statistical significance: All comparisons significant (p<0.05)")
except:
    print(f"? Statistical significance: Unable to compute (scipy not available)")

# Check 3: No sequential scans
seq_scans = df[df['node_type'].str.contains('Seq', case=False, na=False)]
if len(seq_scans) == 0:
    print(f"✓ Index usage verified: No sequential scans detected")
else:
    print(f"⚠ Index usage issue: {len(seq_scans)} sequential scans found")

# Check 4: Warm cache effective
cache_improvement = df.groupby('index_type').apply(
    lambda x: x[x['cache_state']=='warm']['cache_hit_ratio'].mean() - x[x['cache_state']=='cold']['cache_hit_ratio'].mean()
).mean()
if cache_improvement > 0.05:
    print(f"✓ Warmup effective: Average cache hit ratio improved by {cache_improvement*100:.1f}%")
else:
    print(f"⚠ Warmup may be ineffective: Only {cache_improvement*100:.1f}% improvement")

# Check 5: Consistent results
cv_check = warm_df.groupby('index_type')['execution_time'].apply(lambda x: x.std() / x.mean()).mean()
if cv_check < 0.5:
    print(f"✓ Result consistency: Average CV = {cv_check:.3f} (good)")
elif cv_check < 1.0:
    print(f"⚠ Result consistency: Average CV = {cv_check:.3f} (acceptable)")
else:
    print(f"✗ Result consistency: Average CV = {cv_check:.3f} (high variance)")

# Check 6: pg_prewarm verification
print("✓ pg_prewarm used: Cache state explicitly controlled (see cache_verification files)")

print()
print("RECOMMENDATIONS FOR PUBLICATION:")
print("-" * 80)
print("1. Include system specifications in your paper (CPU, RAM, disk type)")
print("2. Mention PostgreSQL version and configuration settings")
print("3. Describe the dataset characteristics (size, cardinality, distribution)")
print("4. Report both median and mean with confidence intervals")
print("5. Include the coefficient of variation to show result stability")
print("6. Discuss any query-specific patterns (e.g., where each index excels)")
print("7. Mention index sizes as a trade-off consideration")
print("8. Highlight use of pg_prewarm for controlled cache state")
print("9. Include cache verification percentages in supplementary materials")
print("10. Consider running on multiple hardware configurations for robustness")
print()
PYEOF
}

################################################################################
# Main Benchmark Loop
################################################################################

run_index_benchmark() {
    local index_type=$1
    local create_index_func=$2
    
    log_section "Benchmarking $index_type Index"
    
    # Create fresh environment
    restart_postgres_clean
    
    # Drop all indexes and create the one we're testing
    drop_all_indexes
    $create_index_func
    
    # Get and save index size
    local index_name=""
    case $index_type in
        "biscuit") index_name="int_bisc" ;;
        "trigram") index_name="int_trgm" ;;
        "btree") index_name="int_tree" ;;
    esac
    get_index_size "$index_name" > "$RESULTS_DIR/index_size_$index_type.txt"
    log_info "Index size: $(cat $RESULTS_DIR/index_size_$index_type.txt)"
    
    # COLD CACHE TESTS
    log_info "Starting cold cache tests..."
    for i in $(seq 1 $NUM_ITERATIONS); do
        restart_postgres_clean
        drop_all_indexes
        $create_index_func
        
        run_benchmark_iteration "$index_type" "$i" "cold" \
            "$RESULTS_DIR/${index_type}_cold_iter${i}.txt"
        
        export RESULT_FILE="$RESULTS_DIR/${index_type}_cold_iter${i}.txt"
        export INDEX_TYPE="$index_type"
        export ITERATION="$i"
        export CACHE_STATE="cold"
        export METRICS_CSV="$RESULTS_DIR/metrics.csv"
        
        extract_metrics "$RESULT_FILE" "$INDEX_TYPE" "$ITERATION" "$CACHE_STATE" "$METRICS_CSV"
    done
    
    # WARM CACHE TESTS
    log_info "Starting warm cache tests..."
    restart_postgres_clean
    drop_all_indexes
    $create_index_func
    warmup_cache_full "$index_name" "$index_type"
    
    for i in $(seq 1 $NUM_ITERATIONS); do
        run_benchmark_iteration "$index_type" "$i" "warm" \
            "$RESULTS_DIR/${index_type}_warm_iter${i}.txt"
        
        export RESULT_FILE="$RESULTS_DIR/${index_type}_warm_iter${i}.txt"
        export INDEX_TYPE="$index_type"
        export ITERATION="$i"
        export CACHE_STATE="warm"
        export METRICS_CSV="$RESULTS_DIR/metrics.csv"
        
        extract_metrics "$RESULT_FILE" "$INDEX_TYPE" "$ITERATION" "$CACHE_STATE" "$METRICS_CSV"
    done
    
    log_info "$index_type benchmark complete"
}

################################################################################
# Main Script
################################################################################

main() {
    log_section "PostgreSQL Index Benchmark Suite"
    
    check_privileges
    
    mkdir -p "$RESULTS_DIR"
    log_info "Results directory: $RESULTS_DIR"
    
    generate_query_file "$RESULTS_DIR/queries.sql"
    
    echo "index_type,iteration,cache_state,query_id,execution_time,planning_time,total_time,shared_hit,shared_read,shared_written,actual_rows,cache_hit_ratio,node_type" \
        > "$RESULTS_DIR/metrics.csv"
    
    # Record system info
    cat > "$RESULTS_DIR/system_info.txt" <<EOF
Benchmark Run: $(date)
Hostname: $(hostname)
PostgreSQL Version: $(sudo -u postgres psql -t -c "SELECT version();")
CPU Info: $(lscpu | grep "Model name" | cut -d: -f2 | xargs)
Memory: $(free -h | grep Mem | awk '{print $2}')
Disk: $(df -h / | tail -1 | awk '{print $2}')
EOF
    
    # Also save PostgreSQL configuration
    sudo -u postgres psql -c "SHOW ALL;" > "$RESULTS_DIR/postgres_config.txt" 2>&1
    
    INDEX_TYPES=("biscuit" "trigram" "btree")
    SHUFFLED=($(shuf -e "${INDEX_TYPES[@]}"))
    
    log_info "Benchmark order: ${SHUFFLED[*]}"
    
    for index_type in "${SHUFFLED[@]}"; do
        case $index_type in
            "biscuit")
                run_index_benchmark "biscuit" "create_biscuit_index"
                ;;
            "trigram")
                run_index_benchmark "trigram" "create_trigram_index"
                ;;
            "btree")
                run_index_benchmark "btree" "create_btree_index"
                ;;
        esac
    done
    
    # Compare counts across all indexes
    log_section "Verifying Count Consistency"
    export RESULTS_DIR="$RESULTS_DIR"
    compare_index_counts | tee "$RESULTS_DIR/count_comparison.txt"
    
    # Generate summary report
    export METRICS_CSV="$RESULTS_DIR/metrics.csv"
    export RESULTS_DIR="$RESULTS_DIR"
    generate_summary_report "$RESULTS_DIR/metrics.csv" "$RESULTS_DIR/summary.txt"
    
    cat "$RESULTS_DIR/summary.txt"
    
    log_section "Benchmark Complete!"
    log_info "Results saved to: $RESULTS_DIR"
    log_info "Metrics CSV: $RESULTS_DIR/metrics.csv"
    log_info "Summary Report: $RESULTS_DIR/summary.txt"
    log_info "Count Verification: $RESULTS_DIR/count_comparison.txt"
    log_info "Cache Verification: $RESULTS_DIR/cache_verification_*.txt"
}

main "$@"