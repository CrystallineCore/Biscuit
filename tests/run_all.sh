#!/usr/bin/env bash
#
# run_all.sh -- Biscuit test suite runner
#
# Runs every category file against a live PostgreSQL cluster and reports a
# combined result. The SQL files contain no psql meta-commands; this script is
# only a driver, and each file can equally be run by hand through pgAdmin,
# DBeaver, a JDBC client or a migration runner. The one thing the driver adds
# that no SQL client can is the crash in the middle of the WAL test.
#
# Every file asserts internally and RAISEs on failure, so the exit status of
# psql IS the result. There is no expected-output file to regenerate when a
# fixture changes.
#
# Usage:
#   ./run_all.sh                      run everything
#   ./run_all.sh 03_regex 05_multi    run only matching categories
#   PGPORT=5433 PGDATABASE=bt ./run_all.sh
#
# Environment:
#   PGBIN        directory holding psql/pg_ctl  (default: on PATH)
#   PGDATA       data directory; REQUIRED for the crash-recovery test
#   PGHOST PGPORT PGUSER PGDATABASE   standard libpq variables
#   PGCTL_OPTS   extra options for the restart after the crash, e.g.
#                "-p 5433 -k /tmp". Only needed when the running server was
#                started with settings that are NOT in postgresql.conf --
#                pg_ctl cannot rediscover command-line options, so without
#                this the server comes back on the defaults and every
#                subsequent connection fails.
#   PG_START_CMD command to restart the cluster after the crash. Auto-detected
#                for Debian/Ubuntu packaged clusters (pg_ctlcluster); set it
#                by hand for anything else. Plain `pg_ctl -D $PGDATA start`
#                does NOT work on packaged clusters, whose postgresql.conf
#                lives under /etc/postgresql/<ver>/<name>/.
#   PG_STOP_CMD  command to take the cluster down immediately. Optional: the
#                fallback is SIGKILL on the postmaster from postmaster.pid.
#   SKIP_WAL=1   skip the crash test (use when PGDATA is not local, e.g. RDS)

set -uo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PGBIN="${PGBIN:-}"
PSQL="${PGBIN:+$PGBIN/}psql"
PG_CTL="${PGBIN:+$PGBIN/}pg_ctl"

export PGHOST="${PGHOST:-/tmp}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-postgres}"
export PGDATABASE="${PGDATABASE:-postgres}"

# ON_ERROR_STOP is what turns a RAISE EXCEPTION inside a file into a non-zero
# exit status. It is passed on the command line, not written into the SQL.
PSQL_OPTS=(-v ON_ERROR_STOP=1 -q --no-psqlrc)

# Category files, in order. 01 through 08 are independent and could run in any
# order; 09 is split in two around a deliberate crash and 10 is slowest, so
# both go last.
CATEGORIES=(
    01_like
    02_ilike
    03_regex
    04_composition
    05_multicolumn
    06_opclass
    07_dml_mvcc
    08_unicode
    09_wal
    10_stress
)

if [ $# -gt 0 ]; then
    SELECTED=()
    for want in "$@"; do
        for cat in "${CATEGORIES[@]}"; do
            case "$cat" in *"$want"*) SELECTED+=("$cat");; esac
        done
    done
    CATEGORIES=("${SELECTED[@]}")
fi

PASSED=(); FAILED=()

hr()   { printf '%s\n' "------------------------------------------------------------"; }
info() { printf '  %s\n' "$*"; }

run_sql() {
    local file="$1"
    local out rc
    out="$("$PSQL" "${PSQL_OPTS[@]}" -f "$SUITE_DIR/$file" 2>&1)"
    rc=$?

    if [ $rc -ne 0 ]; then
        # On failure show EVERYTHING. Filtering here is how a fatal ends up
        # reported with no reason attached: connection errors, missing
        # extension control files and permission denials all arrive as
        # "psql: error: ..." and match none of the SQL-level keywords below.
        printf '%s\n' "$out" | sed -e 's/^/  /'
        return $rc
    fi

    # On success, NOTICEs carry the per-category summaries and the measured
    # findings; everything else is noise.
    printf '%s\n' "$out" \
        | grep -E 'NOTICE|WARNING|ERROR|FATAL|ASSERTION' \
        | sed -e 's/^psql:[^:]*:[0-9]*: //' -e 's/^/  /'
    return 0
}

# ---------------------------------------------------------------------------
#  Stopping and starting the cluster.
#
#  How a cluster is managed is not something this script can assume. A
#  Debian/Ubuntu packaged cluster keeps its configuration in
#  /etc/postgresql/<ver>/<name>/ and its data in /var/lib/postgresql/<ver>/<name>/,
#  so `pg_ctl -D <datadir> start` fails outright: there is no postgresql.conf
#  where pg_ctl looks for one. Those clusters are driven with pg_ctlcluster,
#  which is detected from the PGDATA path below.
#
#  Overridable, because detection cannot cover everything:
#    PG_START_CMD   command to bring the cluster back up
#    PG_STOP_CMD    command to take it down immediately (optional; the
#                   fallback is SIGKILL on the postmaster, which is a more
#                   faithful crash than any shutdown mode anyway)
# ---------------------------------------------------------------------------
detect_cluster_cmds() {
    if [ -n "${PG_START_CMD:-}" ]; then
        START_CMD_SHOWN="$PG_START_CMD"
        return
    fi

    # /var/lib/postgresql/<ver>/<name>  ->  pg_ctlcluster <ver> <name>
    local ver name
    ver="$(printf '%s' "$PGDATA" | sed -n 's#.*/postgresql/\([0-9][0-9]*\)/\([^/]*\)/*$#\1#p')"
    name="$(printf '%s' "$PGDATA" | sed -n 's#.*/postgresql/\([0-9][0-9]*\)/\([^/]*\)/*$#\2#p')"

    if [ -n "$ver" ] && [ -n "$name" ] && [ -d "/etc/postgresql/$ver/$name" ]; then
        PG_START_CMD="pg_ctlcluster $ver $name start"
        PG_STOP_CMD="${PG_STOP_CMD:-pg_ctlcluster $ver $name stop -m immediate --skip-systemctl-redirect}"
        info "detected a packaged cluster: $ver/$name (using pg_ctlcluster)"
    else
        PG_START_CMD="$PG_CTL -D $PGDATA -w -l ${PGLOG:-$PGDATA/crashtest.log} ${PGCTL_OPTS:+-o \"$PGCTL_OPTS\"} start"
        PG_STOP_CMD="${PG_STOP_CMD:-$PG_CTL -D $PGDATA -m immediate stop}"
    fi
    START_CMD_SHOWN="$PG_START_CMD"
}

crash_server() {
    local log pid rc
    log="$(mktemp)"

    eval "$PG_STOP_CMD" >"$log" 2>&1 </dev/null
    rc=$?
    if [ $rc -eq 0 ]; then
        rm -f "$log"; return 0
    fi

    # Fall back to killing the postmaster outright. This is not a worse crash
    # than -m immediate; it is a slightly more honest one, and it works
    # regardless of how the cluster is managed.
    pid="$(head -1 "$PGDATA/postmaster.pid" 2>/dev/null)"
    if [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null; then
        info "stop command failed; killed postmaster $pid with SIGKILL instead"
        for _ in 1 2 3 4 5 6 7 8 9 10; do
            "$PSQL" --no-psqlrc -tAc 'SELECT 1' >/dev/null 2>&1 || { rm -f "$log"; return 0; }
            sleep 1
        done
        rm -f "$log"; return 0
    fi

    info "could not stop the server:"
    sed 's/^/    /' "$log"
    info "set PG_STOP_CMD, or check that $PGUSER can manage this cluster."
    rm -f "$log"
    return 1
}

start_server() {
    local log rc
    log="$(mktemp)"

    # The daemon INHERITS whatever stdout it is started with, so capturing the
    # start command with $( ) leaves the command substitution's pipe open and
    # the shell blocks on it forever -- the postmaster outlives the subshell
    # and never closes the write end. Redirect to a file and detach stdin
    # instead; never capture a process-spawning command inline.
    eval "$PG_START_CMD" >"$log" 2>&1 </dev/null
    rc=$?

    if [ $rc -ne 0 ]; then
        info "start command exited $rc:"
        sed 's/^/    /' "$log"
    fi

    # pg_ctlcluster returns before recovery finishes, and pg_ctl -w can report
    # ready fractionally early, so poll rather than trusting the exit status.
    for _ in $(seq 1 30); do
        if "$PSQL" --no-psqlrc -tAc 'SELECT 1' >/dev/null 2>&1; then
            rm -f "$log"; return 0
        fi
        sleep 1
    done

    info "server never accepted a connection; last output was:"
    sed 's/^/    /' "$log"
    rm -f "$log"
    return 1
}

# ---------------------------------------------------------------------------
#  Preflight. Each of these is a distinct reason the harness can refuse to
#  load, and each needs a different fix, so they are reported separately
#  rather than as one undifferentiated failure.
# ---------------------------------------------------------------------------
preflight() {
    local out

    if ! out="$("$PSQL" --no-psqlrc -tAc 'SELECT 1' 2>&1)"; then
        echo "FATAL: cannot connect."
        printf '%s\n' "$out" | sed 's/^/  /'
        info "tried: host=$PGHOST port=$PGPORT user=$PGUSER db=$PGDATABASE"
        info "check the server is up and that PGHOST points at its socket"
        info "directory (show unix_socket_directories), not just /tmp."
        return 1
    fi

    info "server: $("$PSQL" --no-psqlrc -tAc 'SHOW server_version' 2>/dev/null)"

    # Is the extension installable at all? A missing control file means the
    # build was never `make install`ed into THIS server's sharedir, which is
    # easy to get wrong when several clusters are on one machine.
    if [ "$("$PSQL" --no-psqlrc -tAc \
            "SELECT count(*) FROM pg_available_extensions WHERE name='biscuit'" \
            2>/dev/null)" != "1" ]; then
        echo "FATAL: the 'biscuit' extension is not available to this server."
        info "pg_config --sharedir for THIS cluster must contain"
        info "extension/biscuit.control -- check with:"
        info "  SHOW config_file;   and   SELECT * FROM pg_available_extensions;"
        info "if several PostgreSQL versions are installed, 'make install' may"
        info "have gone to a different one than the server on port $PGPORT."
        return 1
    fi

    # CREATE EXTENSION needs superuser, and 00_harness.sql starts with it.
    if [ "$("$PSQL" --no-psqlrc -tAc \
            'SELECT usesuper FROM pg_user WHERE usename = current_user' \
            2>/dev/null)" != "t" ] \
       && [ "$("$PSQL" --no-psqlrc -tAc \
            "SELECT count(*) FROM pg_extension WHERE extname='biscuit'" \
            2>/dev/null)" != "1" ]; then
        echo "FATAL: $PGUSER is not a superuser and biscuit is not installed yet."
        info "either run as a superuser, or have one run:"
        info "  CREATE EXTENSION biscuit;"
        info "in database $PGDATABASE first."
        return 1
    fi

    return 0
}

hr
echo "Biscuit test suite"
info "host=$PGHOST port=$PGPORT db=$PGDATABASE"
info "categories: ${CATEGORIES[*]}"
hr

if ! preflight; then
    exit 2
fi

# The harness must load first: it owns bt_result, the delta trigger and
# bt_run(), and it drops and recreates the result table, so running it between
# categories would discard everything recorded so far.
echo "[harness] 00_harness.sql"
if ! run_sql 00_harness.sql; then
    echo "FATAL: harness failed to load; nothing else can run."
    exit 2
fi

for cat in "${CATEGORIES[@]}"; do
    hr
    echo "[$cat]"

    if [ "$cat" = "09_wal" ]; then
        # ---- the one step a SQL client cannot perform on its own ----
        if [ "${SKIP_WAL:-0}" = "1" ]; then
            info "SKIP_WAL=1, skipping crash-recovery test"
            continue
        fi
        if [ -z "${PGDATA:-}" ]; then
            info "PGDATA is not set; skipping crash-recovery test"
            info "(set PGDATA, or SKIP_WAL=1 to silence this)"
            continue
        fi

        detect_cluster_cmds

        info "writing post-checkpoint state ..."
        if ! run_sql 09_wal_setup.sql; then
            FAILED+=("$cat (setup)"); continue
        fi

        # -m immediate: no clean shutdown and no shutdown checkpoint, so the
        # server comes back through WAL replay. This is the whole test -- a
        # graceful restart would flush everything and prove nothing.
        info "killing the server ..."
        if ! crash_server; then
            FAILED+=("$cat (crash)"); continue
        fi

        info "restarting (recovery) ..."
        if ! start_server; then
            info ""
            info "The server is DOWN. Start it by hand before re-running:"
            info "  ${START_CMD_SHOWN}"
            FAILED+=("$cat (restart)"); continue
        fi

        # Recovery can take a moment past the point pg_ctl reports ready.
        for _ in 1 2 3 4 5 6 7 8 9 10; do
            "$PSQL" --no-psqlrc -tAc 'SELECT 1' >/dev/null 2>&1 && break
            sleep 1
        done

        info "verifying the index against its pre-crash answers ..."
        if run_sql 09_wal_verify.sql; then PASSED+=("$cat"); else FAILED+=("$cat"); fi
        continue
    fi

    if run_sql "$cat.sql"; then PASSED+=("$cat"); else FAILED+=("$cat"); fi
done

# ---- combined report -------------------------------------------------------
# Read back out of bt_result, which every category appended to, so the summary
# describes what actually ran rather than what this script thinks ran.
hr
echo "Combined results"
hr
"$PSQL" --no-psqlrc -P pager=off -c "
SELECT category,
       count(*)                                          AS cases,
       count(*) FILTER (WHERE idx_used)                  AS indexed,
       count(*) FILTER (WHERE NOT idx_used)              AS fell_back,
       count(*) FILTER (WHERE delta_rows = 0
                          AND delta_hash = 0)            AS results_exact,
       count(*) FILTER (WHERE verdict LIKE 'FAIL%')      AS failures
FROM bt_result
GROUP BY ROLLUP (category)
ORDER BY category NULLS LAST;"

"$PSQL" --no-psqlrc -P pager=off -c "
SELECT category, label, predicate, seq_rows, idx_rows, verdict
FROM bt_result
WHERE verdict LIKE 'FAIL%'
ORDER BY category, result_id
LIMIT 40;"

hr
if [ ${#PASSED[@]} -gt 0 ]; then
    echo "PASSED (${#PASSED[@]}): ${PASSED[*]}"
fi
if [ ${#FAILED[@]} -gt 0 ]; then
    echo "FAILED (${#FAILED[@]}): ${FAILED[*]}"
    hr
    exit 1
fi
echo "All categories passed."
hr
exit 0
