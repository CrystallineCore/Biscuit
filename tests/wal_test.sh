#!/usr/bin/env bash
#
# test_wal_recovery.sh
#
# Focused crash-recovery test suite for the BISCUIT PostgreSQL index
# access method's WAL integration (GenericXLog-based, see biscuit_common.h
# "WAL-LOGGED PAGE STORAGE" section, and biscuit.c's biscuit_object_access_hook()
# / OAT_DROP fix).
#
# What this exercises and why (see also the printed explanation at the
# bottom of the accompanying chat message):
#   - BISCUIT persists all index state (directory + compacted-blob +
#     pending-list chains) inside the index relation's own pages, mutated
#     via GenericXLogStart/GenericXLogFinish (access/generic_xlog.h). There
#     is no custom resource manager -- redo/mask for these records is core
#     PostgreSQL's "Generic" rmgr, so the primary lever for redo-bug
#     detection is wal_consistency_checking, not a bespoke rm_desc.
#   - biscuit_object_access_hook() (OAT_DROP) intentionally does NOT free
#     any durable pages any more -- it only evicts the process-local cache
#     entry -- because OAT_DROP fires inside the still-open DROP
#     transaction, before commit, while GenericXLog page mutations are NOT
#     undone by abort. Freeing pages there meant "BEGIN; DROP INDEX;
#     ROLLBACK;" permanently destroyed the index's on-disk snapshot even
#     though the catalog row came back. This script's DROP-INDEX scenarios
#     (G, H, and bonus I) are built specifically to catch a regression of
#     that fix, including under an abrupt crash rather than just a clean
#     ROLLBACK.
#
# Usage:
#   ./test_wal_recovery.sh
#   BISCUIT_SRC_DIR=/path/to/biscuit/repo ITERATIONS=5 ./test_wal_recovery.sh
#
# See the "Build/run instructions" section of the accompanying explanation
# for prerequisites (PostgreSQL 18 dev headers, the full BISCUIT source
# tree with its Makefile -- this script builds via PGXS).

set -euo pipefail

# This script must NOT be run as root (e.g. via `sudo ./wal_test.sh`).
# initdb/pg_ctl/postgres all refuse to run as root, but `make install`
# below needs to write into PostgreSQL's system lib/share dirs, which are
# usually root-owned. So: run this script as your normal user, and it will
# shell out to `sudo` for just that one install step (you'll get a sudo
# password prompt up front).
if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    echo "ERROR: do not run this script as root/with sudo." >&2
    echo "       initdb/pg_ctl refuse to run as root. Run it as your normal" >&2
    echo "       user; it will invoke 'sudo make install' internally for" >&2
    echo "       the one step that needs elevated privileges." >&2
    exit 1
fi

# Elevate up front (rather than mid-build) so the sudo password prompt
# doesn't appear in the middle of piped/tee'd build output.
SUDO=""
if command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
    echo "This script needs sudo once, to run 'make install' into the" >&2
    echo "PostgreSQL system directories. Everything else (initdb, pg_ctl," >&2
    echo "the actual tests) runs as you, $(whoami)." >&2
    sudo -v
    # Refresh the cached credential every 60s until this script exits, so
    # a slow build doesn't cause `sudo make install` to re-prompt (or fail,
    # if run non-interactively) later on.
    ( while true; do sudo -n -v; sleep 60; done ) 2>/dev/null &
    SUDO_KEEPALIVE_PID=$!
else
    SUDO_KEEPALIVE_PID=""
    echo "WARNING: 'sudo' not found; 'make install' will be attempted" >&2
    echo "         unprivileged and may fail if it needs root." >&2
fi

# ==================== CONFIGURATION (env-overridable) ====================

: "${PG_BIN_DIR:=}"                                   # dir containing pg_ctl/initdb/psql/pg_waldump/pg_config
: "${PGDATA:=${TMPDIR:-/tmp}/biscuit_waltest_pgdata}"  # cluster data dir
: "${PGPORT:=5418}"
: "${PGHOST:=127.0.0.1}"
: "${PGUSER:=$(whoami)}"
: "${PGDATABASE:=biscuit_wal_test}"
: "${BISCUIT_SRC_DIR:=.}"                              # dir with BISCUIT's Makefile/*.c/*.h
: "${ITERATIONS:=3}"
: "${BISCUIT_TEST_ROWS:=400}"                          # baseline row count per scenario
: "${VERBOSE:=0}"

WORKDIR="${PGDATA%/}_work"          # scratch dir: logs, fifos, failure artifacts
PG_LOGFILE="$WORKDIR/postgres.log"
SETUP_LOG="$WORKDIR/setup.log"

# ==================== DERIVED PATHS ====================

if [[ -z "$PG_BIN_DIR" ]]; then
    if command -v pg_config >/dev/null 2>&1; then
        PG_BIN_DIR="$(pg_config --bindir)"
    elif [[ -x /usr/lib/postgresql/18/bin/pg_ctl ]]; then
        PG_BIN_DIR="/usr/lib/postgresql/18/bin"
    else
        echo "ERROR: could not locate PostgreSQL 18 binaries. Set PG_BIN_DIR explicitly." >&2
        exit 1
    fi
fi

PATH="$PG_BIN_DIR:$PATH"
PG_CTL="$PG_BIN_DIR/pg_ctl"
INITDB="$PG_BIN_DIR/initdb"
PSQL_BIN="$PG_BIN_DIR/psql"
PG_WALDUMP="$PG_BIN_DIR/pg_waldump"
PG_CONFIG_BIN="$PG_BIN_DIR/pg_config"
PG_ISREADY="$PG_BIN_DIR/pg_isready"

for bin in "$PG_CTL" "$INITDB" "$PSQL_BIN" "$PG_WALDUMP" "$PG_CONFIG_BIN"; do
    if [[ ! -x "$bin" ]]; then
        echo "ERROR: required binary not found or not executable: $bin" >&2
        exit 1
    fi
done

PG_VERSION_STR="$("$PG_CONFIG_BIN" --version)"
if [[ "$PG_VERSION_STR" != *" 18"* ]]; then
    echo "ERROR: this test requires PostgreSQL 18, found: $PG_VERSION_STR" >&2
    exit 1
fi

# WORKDIR must exist before build_extension()/init_cluster() write to
# $SETUP_LOG. init_cluster() still guards PGDATA itself against clobbering
# a preserved failed run, but the log dir is safe to create eagerly.
if [[ -e "$PGDATA" ]]; then
    echo "ERROR: PGDATA already exists: $PGDATA" >&2
    echo "       This may be a preserved failure from a previous run." >&2
    echo "       Inspect it, then remove it (rm -rf \"$PGDATA\" \"$WORKDIR\") and re-run." >&2
    exit 1
fi
mkdir -p "$WORKDIR"

# ==================== STATE ====================

PASS_COUNT=0
FAIL_COUNT=0
declare -a FAILED_SCENARIOS=()
CURRENT_ITER=0
CLUSTER_RUNNING=0
PRESERVE_ON_EXIT=0

# ==================== LOGGING ====================

log_section() { printf '\n==== %s ====\n' "$1"; }
log_info()    { printf '  %s\n' "$1"; }
log_pass()    { PASS_COUNT=$((PASS_COUNT+1)); printf '  [PASS] %s\n' "$1"; }
log_fail() {
    FAIL_COUNT=$((FAIL_COUNT+1))
    FAILED_SCENARIOS+=("$1 (iteration $CURRENT_ITER)")
    printf '  [FAIL] %s (iteration %s)\n' "$1" "$CURRENT_ITER"
}
vlog() { [[ "$VERBOSE" == "1" ]] && printf '    . %s\n' "$1" || true; }

# ==================== CLEANUP / FAILURE PRESERVATION ====================

cleanup() {
    local exit_code=$?
    if [[ -n "${SUDO_KEEPALIVE_PID:-}" ]]; then
        kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
    fi
    if [[ "$CLUSTER_RUNNING" == "1" ]]; then
        "$PG_CTL" -D "$PGDATA" -m fast -w -t 30 stop >>"$SETUP_LOG" 2>&1 || true
        CLUSTER_RUNNING=0
    fi

    if [[ "$exit_code" != "0" || "$FAIL_COUNT" -gt 0 || "$PRESERVE_ON_EXIT" == "1" ]]; then
        echo
        echo "One or more checks failed (or setup aborted). Preserving for inspection:"
        echo "  PGDATA:  $PGDATA"
        echo "  Logs:    $WORKDIR"
        echo "  Postgres server log: $PG_LOGFILE"
        echo "  Build/setup log:     $SETUP_LOG"
    else
        rm -rf "$PGDATA" "$WORKDIR"
    fi
    exit "$exit_code"
}
trap cleanup EXIT

# ==================== PSQL HELPERS ====================

psql_run() {
    "$PSQL_BIN" -X -q -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" "$@"
}

# Runs a SQL statement, returns first column of first row.
psql_scalar() {
    psql_run -At -c "$1"
}

# Runs a SQL statement, discarding output. Fails the calling context (via -e)
# if the statement errors.
psql_exec() {
    psql_run -c "$1" >/dev/null
}

wait_ready() {
    local tries=0
    while (( tries < 120 )); do
        if "$PG_ISREADY" -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" >/dev/null 2>&1; then
            # pg_isready succeeds even mid-crash-recovery ("starting up") on
            # some versions; confirm we can actually run a query.
            if psql_scalar "SELECT 1;" >/dev/null 2>&1; then
                return 0
            fi
        fi
        sleep 0.5
        tries=$((tries+1))
    done
    echo "ERROR: cluster did not become ready within timeout" >&2
    return 1
}

# ==================== CLUSTER LIFECYCLE ====================

build_extension() {
    log_section "Building BISCUIT extension"
    if [[ ! -f "$BISCUIT_SRC_DIR/Makefile" ]]; then
        echo "ERROR: no Makefile found in BISCUIT_SRC_DIR=$BISCUIT_SRC_DIR" >&2
        echo "       Set BISCUIT_SRC_DIR to the root of the BISCUIT source tree." >&2
        exit 1
    fi
    (
        cd "$BISCUIT_SRC_DIR"
        make PG_CONFIG="$PG_CONFIG_BIN" clean 2>&1 | tee -a "$SETUP_LOG" || true
        make PG_CONFIG="$PG_CONFIG_BIN" 2>&1 | tee -a "$SETUP_LOG"
        $SUDO make PG_CONFIG="$PG_CONFIG_BIN" install 2>&1 | tee -a "$SETUP_LOG"
    )
    log_info "build + install OK (see $SETUP_LOG for details)"
}

init_cluster() {
    log_section "Initializing PostgreSQL 18 cluster on port $PGPORT"
    "$INITDB" -D "$PGDATA" -U "$PGUSER" --data-checksums --encoding=UTF8 \
        --auth=trust >>"$SETUP_LOG" 2>&1

    cat >> "$PGDATA/postgresql.conf" <<EOF

# ---- test_wal_recovery.sh overrides ----
listen_addresses = '$PGHOST'
port = $PGPORT
wal_level = replica
fsync = on
full_page_writes = on
# Single most effective tool for catching redo bugs in a GenericXLog-based
# extension: after every WAL replay, core recomputes the masked page and
# compares it against the full-page image taken on the primary.
wal_consistency_checking = 'all'
# Keep checkpoints out of the way so each scenario's crash genuinely
# exercises REDO of BISCUIT's WAL records rather than replaying against
# already-checkpointed (and thus already-durable) data pages.
checkpoint_timeout = 1h
max_wal_size = 4GB
autovacuum = off
logging_collector = off
log_min_messages = info
log_line_prefix = '%m [%p] '
EOF

    "$PG_CTL" -D "$PGDATA" -l "$PG_LOGFILE" -w -t 60 start
    CLUSTER_RUNNING=1
    wait_ready
    "$PSQL_BIN" -X -q -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres \
        -c "CREATE DATABASE $PGDATABASE;" >>"$SETUP_LOG" 2>&1
    psql_exec "CREATE EXTENSION biscuit;"
    log_info "cluster up, database '$PGDATABASE' created, extension installed"
}

# Abrupt crash: SIGQUIT to all backends, NO shutdown checkpoint. This is the
# same mechanism PostgreSQL's own crash-recovery tests use to simulate a
# hard crash without actually rebooting the host.
crash_cluster() {
    vlog "crashing cluster (pg_ctl -m immediate)"
    "$PG_CTL" -D "$PGDATA" -m immediate -w -t 60 stop >>"$SETUP_LOG" 2>&1 || true
    CLUSTER_RUNNING=0
    sleep 0.3
}

recover_cluster() {
    vlog "restarting cluster (triggers crash recovery)"
    "$PG_CTL" -D "$PGDATA" -l "$PG_LOGFILE" -w -t 120 start
    CLUSTER_RUNNING=1
    wait_ready
    if grep -q "PANIC" "$PG_LOGFILE" 2>/dev/null; then
        echo "    !! PANIC found in server log during recovery !!"
        return 1
    fi
    if grep -qi "inconsistent page" "$PG_LOGFILE" 2>/dev/null; then
        echo "    !! wal_consistency_checking flagged an inconsistent page during redo !!"
        return 1
    fi
    return 0
}

current_lsn() { psql_scalar "SELECT pg_current_wal_lsn();"; }

save_failure_artifacts() {
    local scenario="$1" lsn_before="${2:-}"
    local dir="$WORKDIR/failure_${scenario// /_}_iter${CURRENT_ITER}"
    mkdir -p "$dir"
    cp "$PG_LOGFILE" "$dir/postgres.log" 2>/dev/null || true
    if [[ -n "$lsn_before" && "$CLUSTER_RUNNING" == "1" ]]; then
        "$PG_WALDUMP" -p "$PGDATA/pg_wal" -s "$lsn_before" \
            > "$dir/waldump_full.txt" 2>&1 || true
        "$PG_WALDUMP" --rmgr=Generic -p "$PGDATA/pg_wal" -s "$lsn_before" \
            > "$dir/waldump_generic_only.txt" 2>&1 || true
    fi
    log_info "failure artifacts saved to $dir"
}

# ==================== SCHEMA / BASELINE ====================

# Recreates a known-good baseline: t_heap (with or without a BISCUIT index)
# and t_shadow, a plain-heap control table that mirrors every operation
# t_heap receives within the same transaction. Comparing t_heap's
# index-forced scan results against t_shadow after a crash isolates
# BISCUIT-specific WAL bugs from generic heap/WAL bugs.
setup_baseline() {
    local with_index="${1:-1}"
    psql_exec "DROP TABLE IF EXISTS t_heap CASCADE;"
    psql_exec "DROP TABLE IF EXISTS t_shadow CASCADE;"
    psql_exec "CREATE TABLE t_heap (id integer PRIMARY KEY, val text NOT NULL);"
    psql_exec "CREATE TABLE t_shadow (id integer PRIMARY KEY, val text NOT NULL);"
    psql_exec "INSERT INTO t_heap SELECT i, 'B_' || lpad(i::text, 7, '0') FROM generate_series(1, $BISCUIT_TEST_ROWS) i;"
    psql_exec "INSERT INTO t_shadow SELECT * FROM t_heap;"
    if [[ "$with_index" == "1" ]]; then
        psql_exec "CREATE INDEX t_biscuit_idx ON t_heap USING biscuit (val);"
    fi
    psql_exec "CHECKPOINT;"
}

index_exists() {
    local r
    r="$(psql_scalar "SELECT to_regclass('public.t_biscuit_idx') IS NOT NULL;")"
    [[ "$r" == "t" ]]
}

get_active_records() {
    psql_run -At -c "SELECT biscuit_index_stats('t_biscuit_idx'::regclass::oid);" \
        | grep '^Active records:' | awk '{print $NF}'
}

# ==================== CHECKSUM / ASSERTION HELPERS ====================

# All test data matches the pattern B_NNNNNNN; a literal-underscore LIKE
# pattern deterministically selects every row and exercises BISCUIT's
# prefix+wildcard matching (the actual operator class it supports).
CHECKSUM_QUERY="SELECT coalesce(md5(string_agg(id::text, ',' ORDER BY id)), 'EMPTY') FROM %s WHERE val LIKE 'B\\_%%' ESCAPE '\\'"

checksum_seq() {
    local tbl="$1"
    psql_run -At -c "SET enable_indexscan = off; SET enable_bitmapscan = off; SET enable_indexonlyscan = off;" \
                 -c "$(printf "$CHECKSUM_QUERY" "$tbl")" | tail -n1
}

checksum_idx() {
    local tbl="$1"
    psql_run -At -c "SET enable_seqscan = off; SET enable_bitmapscan = off; SET enable_indexonlyscan = off;" \
                 -c "$(printf "$CHECKSUM_QUERY" "$tbl")" | tail -n1
}

assert_plan_uses_biscuit() {
    local plan
    plan="$(psql_run -At -c "SET enable_seqscan = off; SET enable_bitmapscan = off;" \
                      -c "EXPLAIN (COSTS OFF) $(printf "$CHECKSUM_QUERY" "t_heap")" 2>&1)"
    if ! grep -qi "t_biscuit_idx" <<<"$plan"; then
        log_info "    MISMATCH: expected plan to use t_biscuit_idx, got:"
        log_info "      $(tr '\n' ' ' <<<"$plan")"
        return 1
    fi
    return 0
}

FAILED_ASSERTIONS=0

check_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then
        vlog "OK: $desc"
    else
        log_info "    MISMATCH: $desc -- expected [$expected] got [$actual]"
        FAILED_ASSERTIONS=$((FAILED_ASSERTIONS+1))
    fi
}

check_cond() {
    local desc="$1" ok="$2"   # "0" (bash-truthy success) or nonzero
    if [[ "$ok" == "0" ]]; then
        vlog "OK: $desc"
    else
        log_info "    MISMATCH: $desc"
        FAILED_ASSERTIONS=$((FAILED_ASSERTIONS+1))
    fi
}

# ==================== SCENARIOS ====================
#
# Each scenario: FAILED_ASSERTIONS is reset, the operation + crash +
# recovery run, assertions are collected, and the function returns
# success only if every assertion passed.

scenario_insert_commit() {
    FAILED_ASSERTIONS=0
    setup_baseline 1
    local before_count new_lo new_hi lsn
    before_count="$(psql_scalar "SELECT count(*) FROM t_shadow;")"
    new_lo=$((BISCUIT_TEST_ROWS+1)); new_hi=$((BISCUIT_TEST_ROWS+50))
    lsn="$(current_lsn)"
    psql_exec "BEGIN;
        INSERT INTO t_heap   SELECT i, 'B_' || lpad(i::text,7,'0') FROM generate_series($new_lo,$new_hi) i;
        INSERT INTO t_shadow SELECT i, 'B_' || lpad(i::text,7,'0') FROM generate_series($new_lo,$new_hi) i;
    COMMIT;"
    crash_cluster
    recover_cluster || { FAILED_ASSERTIONS=$((FAILED_ASSERTIONS+1)); }

    local exp_cs seq_cs idx_cs
    exp_cs="$(checksum_seq t_shadow)"
    seq_cs="$(checksum_seq t_heap)"
    idx_cs="$(checksum_idx t_heap)"
    check_cond "planner used biscuit index" "$(assert_plan_uses_biscuit >/dev/null 2>&1; echo $?)"
    check_eq "heap seqscan matches shadow (committed inserts survived)" "$exp_cs" "$seq_cs"
    check_eq "biscuit index scan matches shadow (committed inserts survived)" "$exp_cs" "$idx_cs"
    check_eq "active_records reflects new rows" "$((before_count+50))" "$(get_active_records)"
    if [[ "$FAILED_ASSERTIONS" -gt 0 ]]; then save_failure_artifacts "insert_commit" "$lsn"; fi
    [[ "$FAILED_ASSERTIONS" -eq 0 ]]
}

scenario_insert_rollback() {
    FAILED_ASSERTIONS=0
    setup_baseline 1
    local before_count new_lo new_hi lsn expected_cs
    before_count="$(psql_scalar "SELECT count(*) FROM t_shadow;")"
    expected_cs="$(checksum_seq t_shadow)"
    new_lo=$((BISCUIT_TEST_ROWS+1)); new_hi=$((BISCUIT_TEST_ROWS+50))
    lsn="$(current_lsn)"
    psql_exec "BEGIN;
        INSERT INTO t_heap   SELECT i, 'B_' || lpad(i::text,7,'0') FROM generate_series($new_lo,$new_hi) i;
        INSERT INTO t_shadow SELECT i, 'B_' || lpad(i::text,7,'0') FROM generate_series($new_lo,$new_hi) i;
    ROLLBACK;"
    crash_cluster
    recover_cluster || { FAILED_ASSERTIONS=$((FAILED_ASSERTIONS+1)); }

    local seq_cs idx_cs
    seq_cs="$(checksum_seq t_heap)"
    idx_cs="$(checksum_idx t_heap)"
    check_cond "planner used biscuit index" "$(assert_plan_uses_biscuit >/dev/null 2>&1; echo $?)"
    check_eq "heap seqscan unchanged (rolled-back inserts did not survive)" "$expected_cs" "$seq_cs"
    check_eq "biscuit index scan unchanged (no phantom rows from rolled-back insert)" "$expected_cs" "$idx_cs"
    check_eq "active_records unchanged" "$before_count" "$(get_active_records)"
    if [[ "$FAILED_ASSERTIONS" -gt 0 ]]; then save_failure_artifacts "insert_rollback" "$lsn"; fi
    [[ "$FAILED_ASSERTIONS" -eq 0 ]]
}

scenario_delete_commit() {
    FAILED_ASSERTIONS=0
    setup_baseline 1
    local lsn expected_cs
    lsn="$(current_lsn)"
    psql_exec "BEGIN;
        DELETE FROM t_heap   WHERE id % 7 = 0;
        DELETE FROM t_shadow WHERE id % 7 = 0;
    COMMIT;"
    crash_cluster
    recover_cluster || { FAILED_ASSERTIONS=$((FAILED_ASSERTIONS+1)); }

    expected_cs="$(checksum_seq t_shadow)"
    local seq_cs idx_cs exp_count
    seq_cs="$(checksum_idx t_heap)"
    idx_cs="$(checksum_idx t_heap)"
    exp_count="$(psql_scalar "SELECT count(*) FROM t_shadow;")"
    check_cond "planner used biscuit index" "$(assert_plan_uses_biscuit >/dev/null 2>&1; echo $?)"
    check_eq "heap seqscan matches shadow (committed deletes survived)" "$expected_cs" "$(checksum_seq t_heap)"
    check_eq "biscuit index scan matches shadow (deleted rows absent post-recovery)" "$expected_cs" "$idx_cs"
    check_eq "active_records reflects deletions" "$exp_count" "$(get_active_records)"
    if [[ "$FAILED_ASSERTIONS" -gt 0 ]]; then save_failure_artifacts "delete_commit" "$lsn"; fi
    [[ "$FAILED_ASSERTIONS" -eq 0 ]]
}

scenario_delete_rollback() {
    FAILED_ASSERTIONS=0
    setup_baseline 1
    local lsn expected_cs before_count
    expected_cs="$(checksum_seq t_shadow)"
    before_count="$(psql_scalar "SELECT count(*) FROM t_shadow;")"
    lsn="$(current_lsn)"
    psql_exec "BEGIN;
        DELETE FROM t_heap   WHERE id % 7 = 0;
        DELETE FROM t_shadow WHERE id % 7 = 0;
    ROLLBACK;"
    crash_cluster
    recover_cluster || { FAILED_ASSERTIONS=$((FAILED_ASSERTIONS+1)); }

    check_cond "planner used biscuit index" "$(assert_plan_uses_biscuit >/dev/null 2>&1; echo $?)"
    check_eq "heap seqscan unchanged (rolled-back deletes did not survive)" "$expected_cs" "$(checksum_seq t_heap)"
    check_eq "biscuit index scan unchanged (rolled-back rows still visible)" "$expected_cs" "$(checksum_idx t_heap)"
    check_eq "active_records unchanged" "$before_count" "$(get_active_records)"
    if [[ "$FAILED_ASSERTIONS" -gt 0 ]]; then save_failure_artifacts "delete_rollback" "$lsn"; fi
    [[ "$FAILED_ASSERTIONS" -eq 0 ]]
}

scenario_vacuum_crash() {
    FAILED_ASSERTIONS=0
    setup_baseline 1
    # Create tombstoned/dead index entries first, then VACUUM (which drives
    # biscuit_bulkdelete/biscuit_vacuumcleanup -- a much more WAL-write-heavy
    # path than a plain DELETE, since it retires/frees pages).
    psql_exec "BEGIN;
        DELETE FROM t_heap   WHERE id % 5 = 0;
        DELETE FROM t_shadow WHERE id % 5 = 0;
    COMMIT;"
    local expected_cs exp_count lsn
    expected_cs="$(checksum_seq t_shadow)"
    exp_count="$(psql_scalar "SELECT count(*) FROM t_shadow;")"
    lsn="$(current_lsn)"
    psql_exec "VACUUM t_heap;"
    crash_cluster
    recover_cluster || { FAILED_ASSERTIONS=$((FAILED_ASSERTIONS+1)); }

    check_cond "planner used biscuit index" "$(assert_plan_uses_biscuit >/dev/null 2>&1; echo $?)"
    check_eq "heap seqscan matches shadow after VACUUM+crash" "$expected_cs" "$(checksum_seq t_heap)"
    check_eq "biscuit index scan matches shadow after VACUUM+crash" "$expected_cs" "$(checksum_idx t_heap)"
    check_eq "active_records correct after VACUUM+crash" "$exp_count" "$(get_active_records)"
    # biscuit_index_stats() itself must not error -- a corrupted directory
    # after a crashed VACUUM is exactly the kind of thing that would raise
    # "no on-disk snapshot found" or similar.
    check_cond "biscuit_index_stats() callable post-recovery" "$(psql_run -At -c "SELECT biscuit_index_stats('t_biscuit_idx'::regclass::oid);" >/dev/null 2>&1; echo $?)"
    if [[ "$FAILED_ASSERTIONS" -gt 0 ]]; then save_failure_artifacts "vacuum_crash" "$lsn"; fi
    [[ "$FAILED_ASSERTIONS" -eq 0 ]]
}

scenario_create_index_crash() {
    FAILED_ASSERTIONS=0
    setup_baseline 0    # table populated, NO biscuit index yet
    local expected_cs lsn
    expected_cs="$(checksum_seq t_shadow)"
    lsn="$(current_lsn)"
    psql_exec "CREATE INDEX t_biscuit_idx ON t_heap USING biscuit (val);"
    crash_cluster
    recover_cluster || { FAILED_ASSERTIONS=$((FAILED_ASSERTIONS+1)); }

    check_cond "index exists after crash following CREATE INDEX" "$(index_exists && echo 0 || echo 1)"
    check_cond "planner used biscuit index" "$(assert_plan_uses_biscuit >/dev/null 2>&1; echo $?)"
    check_eq "biscuit index scan matches shadow (build survived crash intact)" "$expected_cs" "$(checksum_idx t_heap)"
    check_eq "active_records equals full baseline row count" "$BISCUIT_TEST_ROWS" "$(get_active_records)"
    if [[ "$FAILED_ASSERTIONS" -gt 0 ]]; then save_failure_artifacts "create_index_crash" "$lsn"; fi
    [[ "$FAILED_ASSERTIONS" -eq 0 ]]
}

# The core OAT_DROP regression test: BEGIN; DROP INDEX; ROLLBACK; then
# crash. Before the fix, biscuit_persist_drop() freed the durable
# directory/blob/pending-list pages as soon as OAT_DROP fired -- a
# non-transactional GenericXLog mutation -- while the catalog drop itself
# was rolled back. The index came back visible in pg_class pointing at
# storage whose backing structures had already been destroyed.
scenario_drop_index_rollback_crash() {
    FAILED_ASSERTIONS=0
    setup_baseline 1
    local expected_cs lsn
    expected_cs="$(checksum_seq t_shadow)"
    lsn="$(current_lsn)"
    psql_exec "BEGIN; DROP INDEX t_biscuit_idx; ROLLBACK;"
    crash_cluster
    recover_cluster || { FAILED_ASSERTIONS=$((FAILED_ASSERTIONS+1)); }

    check_cond "index still exists after ROLLBACKed DROP + crash" "$(index_exists && echo 0 || echo 1)"
    check_cond "biscuit_index_stats() callable (no 'on-disk snapshot' loss)" \
        "$(psql_run -At -c "SELECT biscuit_index_stats('t_biscuit_idx'::regclass::oid);" >/dev/null 2>&1; echo $?)"
    check_cond "planner used biscuit index" "$(assert_plan_uses_biscuit >/dev/null 2>&1; echo $?)"
    check_eq "biscuit index scan matches shadow (index fully usable, no data loss)" "$expected_cs" "$(checksum_idx t_heap)"
    check_eq "active_records unchanged" "$BISCUIT_TEST_ROWS" "$(get_active_records)"
    if [[ "$FAILED_ASSERTIONS" -gt 0 ]]; then save_failure_artifacts "drop_index_rollback_crash" "$lsn"; fi
    [[ "$FAILED_ASSERTIONS" -eq 0 ]]
}

# Complementary case: a *committed* DROP INDEX followed by a crash must not
# resurrect the index (core unlinks the relfilenode at commit; recovery
# must not undo that), and must not disturb the underlying table.
scenario_drop_index_commit_crash() {
    FAILED_ASSERTIONS=0
    setup_baseline 1
    local expected_cs lsn
    expected_cs="$(checksum_seq t_shadow)"
    lsn="$(current_lsn)"
    psql_exec "BEGIN; DROP INDEX t_biscuit_idx; COMMIT;"
    crash_cluster
    recover_cluster || { FAILED_ASSERTIONS=$((FAILED_ASSERTIONS+1)); }

    check_cond "index does NOT exist after committed DROP + crash (not resurrected)" "$(index_exists && echo 1 || echo 0)"
    check_eq "table data untouched by DROP INDEX (heap seqscan matches shadow)" "$expected_cs" "$(checksum_seq t_heap)"
    # Sanity: a fresh CREATE INDEX afterward must work cleanly (no orphaned
    # catalog/file state left behind by the crashed commit).
    check_cond "index rebuildable after committed DROP + crash" \
        "$(psql_exec "CREATE INDEX t_biscuit_idx ON t_heap USING biscuit (val);" >/dev/null 2>&1; echo $?)"
    check_eq "rebuilt index scan matches shadow" "$expected_cs" "$(checksum_idx t_heap)"
    if [[ "$FAILED_ASSERTIONS" -gt 0 ]]; then save_failure_artifacts "drop_index_commit_crash" "$lsn"; fi
    [[ "$FAILED_ASSERTIONS" -eq 0 ]]
}

# Bonus scenario, stronger than G: crash while the DROP INDEX transaction
# is still OPEN (never explicitly rolled back at all -- the crash itself is
# the abort). This is the most literal reproduction of "abrupt crash around
# an uncommitted OAT_DROP firing" the bug description asks for.
scenario_drop_index_uncommitted_crash() {
    FAILED_ASSERTIONS=0
    setup_baseline 1
    local expected_cs lsn fifo appname
    expected_cs="$(checksum_seq t_shadow)"
    lsn="$(current_lsn)"

    fifo="$WORKDIR/dropfifo_$$"
    rm -f "$fifo"; mkfifo "$fifo"
    appname="biscuit_bg_drop_$$"

    PGAPPNAME="$appname" "$PSQL_BIN" -X -q -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
        < "$fifo" > "$WORKDIR/bg_session.log" 2>&1 &
    local bg_pid=$!
    exec 9>"$fifo"
    echo "BEGIN;" >&9
    echo "DROP INDEX t_biscuit_idx;" >&9

    # Wait until the backend has actually executed the DROP and is sitting
    # "idle in transaction" (i.e. waiting for us to send COMMIT/ROLLBACK,
    # which we never will -- we crash instead).
    local tries=0 state=""
    while (( tries < 60 )); do
        state="$(psql_scalar "SELECT state FROM pg_stat_activity WHERE application_name = '$appname' ORDER BY backend_start DESC LIMIT 1;" 2>/dev/null || true)"
        [[ "$state" == "idle in transaction" ]] && break
        sleep 0.2
        tries=$((tries+1))
    done
    check_eq "background DROP INDEX transaction reached open/idle state" "idle in transaction" "$state"

    crash_cluster
    exec 9>&- 2>/dev/null || true
    wait "$bg_pid" 2>/dev/null || true
    rm -f "$fifo"

    recover_cluster || { FAILED_ASSERTIONS=$((FAILED_ASSERTIONS+1)); }

    check_cond "index still exists after crash mid-DROP (implicit abort)" "$(index_exists && echo 0 || echo 1)"
    check_cond "biscuit_index_stats() callable (no 'on-disk snapshot' loss)" \
        "$(psql_run -At -c "SELECT biscuit_index_stats('t_biscuit_idx'::regclass::oid);" >/dev/null 2>&1; echo $?)"
    check_eq "biscuit index scan matches shadow (index fully usable)" "$expected_cs" "$(checksum_idx t_heap)"
    if [[ "$FAILED_ASSERTIONS" -gt 0 ]]; then save_failure_artifacts "drop_index_uncommitted_crash" "$lsn"; fi
    [[ "$FAILED_ASSERTIONS" -eq 0 ]]
}

# ==================== MAIN ====================

SCENARIOS=(
    "INSERT + COMMIT + crash:scenario_insert_commit"
    "INSERT + ROLLBACK + crash:scenario_insert_rollback"
    "DELETE + COMMIT + crash:scenario_delete_commit"
    "DELETE + ROLLBACK + crash:scenario_delete_rollback"
    "VACUUM + crash:scenario_vacuum_crash"
    "CREATE INDEX + crash:scenario_create_index_crash"
    "DROP INDEX + ROLLBACK + crash:scenario_drop_index_rollback_crash"
    "DROP INDEX + COMMIT + crash:scenario_drop_index_commit_crash"
    "DROP INDEX + uncommitted + crash (bonus):scenario_drop_index_uncommitted_crash"
)

build_extension
init_cluster

for (( iter=1; iter<=ITERATIONS; iter++ )); do
    CURRENT_ITER=$iter
    log_section "ITERATION $iter / $ITERATIONS"
    for entry in "${SCENARIOS[@]}"; do
        name="${entry%%:*}"
        fn="${entry##*:}"
        log_info "-- $name --"
        if "$fn"; then
            log_pass "$name"
        else
            log_fail "$name"
        fi
    done
done

log_section "SUMMARY"
echo "  Passed: $PASS_COUNT"
echo "  Failed: $FAIL_COUNT"
if [[ "$FAIL_COUNT" -gt 0 ]]; then
    echo "  Failing scenarios:"
    for f in "${FAILED_SCENARIOS[@]}"; do
        echo "    - $f"
    done
    echo
    echo "RESULT: FAIL"
    exit 1
else
    echo
    echo "RESULT: PASS"
    exit 0
fi
