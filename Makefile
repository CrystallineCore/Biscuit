# Makefile for the Biscuit index access method (PGXS build).
#
# Matches the tree layout: sources under src/*.c, versioned install/upgrade
# scripts under sql/, and the SQL test suite under tests/ (run_all.sh plus
# the numbered category files).

EXTENSION   = biscuit
EXTVERSION  = 3.1.0

MODULE_big  = biscuit

# Built from every .c under src/ so a newly added module is picked up
# automatically. biscuit_regex.c (the regex -> glob decomposer) is the
# addition for this version; if you prefer an explicit OBJS list instead,
# biscuit_regex.o must appear in it.
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c))

# Fresh install plus the no-REINDEX upgrade path from 3.0.0.
DATA = sql/biscuit--$(EXTVERSION).sql \
       sql/biscuit--3.0.0--3.1.0.sql

# -----------------------------------------------------------------------
# Compiler / linker flags
# -----------------------------------------------------------------------
# ORDER MATTERS. pgxs.mk does
#     override CPPFLAGS := $(PG_CPPFLAGS) $(CPPFLAGS)
# with := (immediate expansion) at include time, so anything appended to
# PG_CPPFLAGS AFTER `include $(PGXS)` is silently dropped. SHLIB_LINK is
# expanded lazily and does still work late, which makes the failure mode
# nasty: a late -DHAVE_ROARING is ignored while the matching -lroaring is
# honoured, so the extension links against CRoaring but is compiled with
# the fallback bitmap -- it builds, runs, and quietly gives up the
# performance you thought you had enabled. Keep these above the include.

PG_CPPFLAGS = -Isrc

# CRoaring (optional). Without it the fallback bitmap in biscuit_bitmap.c
# is used -- correct, but slower. Enable with:
#     make WITH_ROARING=1
ifdef WITH_ROARING
PG_CPPFLAGS += -DHAVE_ROARING
SHLIB_LINK  += -lroaring
endif

PG_CONFIG ?= pg_config
PGXS      := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Verify the two CRoaring flags agree, since they are honoured by
# different mechanisms and can silently diverge (see above).
ifdef WITH_ROARING
ifeq (,$(findstring -DHAVE_ROARING,$(CPPFLAGS)))
$(error WITH_ROARING set but -DHAVE_ROARING did not reach CPPFLAGS -- PG_CPPFLAGS must be assigned before `include $$(PGXS)`)
endif
endif

# =======================================================================
# Tests
# =======================================================================
#
# One layer: the canonical suite (tests/), run against a live server.
# Categories, one file each (numeric prefix is the category name used by
# CATEGORIES=... below):
#
#   00_harness      shared setup/assertion helpers, sourced by the rest
#   01_like         LIKE
#   02_ilike        ILIKE
#   03_regex        regex operators (~, ~*, etc.), backed by biscuit_regex.c
#   04_composition  compound/boolean predicates
#   05_multicolumn  multi-column indexes
#   06_opclass      operator class behaviour
#   07_dml_mvcc     DML + MVCC visibility
#   08_unicode      UTF-8 / multibyte handling
#   09_wal_setup,
#   09_wal_verify   crash-recovery pair: setup writes+checkpoints, then the
#                   server is restarted (see check-wal) and verify checks
#                   the data survived. Matched together by the "09_wal"
#                   prefix.
#   10_stress       larger data volumes / performance sanity
#
# Every case is differential: the same predicate is evaluated once with
# index paths disabled -- PostgreSQL's own matching over a sequential
# scan, used as the oracle -- and once with sequential scans disabled,
# and the two must agree on the row count AND on a fingerprint of WHICH
# rows came back. Counting alone is not enough; two scans can agree on
# COUNT(*) and return different rows.
#
# Each case also declares whether the access method must serve it, must
# not, or either, so both failure directions are caught: refusing a
# supported pattern is a silent performance regression, accepting an
# unsupported one is a silent wrong answer.
#
# This is not wired up as a pg_regress REGRESS target. pg_regress
# compares stdout against a checked-in expected/*.out that has to be
# regenerated whenever a fixture changes; these scripts assert internally
# and RAISE on failure, so exit status is the result and there is no
# expected-output file to maintain. It also keeps every .sql file runnable
# through any client -- pgAdmin, DBeaver, JDBC, a migration runner -- since
# none of them contain psql-specific syntax.

SUITE_DIR ?= tests

# Use the psql and pg_ctl belonging to the SAME installation this extension
# was built against. Picking them off PATH is how you end up testing a
# freshly installed .so against a different major version's server.
PG_BINDIR := $(shell $(PG_CONFIG) --bindir)
PSQL      ?= $(PG_BINDIR)/psql

# The canonical suite. Standard libpq environment variables apply
# (PGHOST, PGPORT, PGUSER, PGDATABASE), e.g.
#     make check-suite PGDATABASE=scratch
#     make check-suite CATEGORIES="03_regex 05_multicolumn"
#
# PGDATA is needed ONLY by the crash-recovery category, and is discovered
# from the server itself rather than guessed -- which also means it is
# correct for packaged clusters, whose data directory is nowhere near
# their configuration. If the server cannot be reached the variable comes
# back empty and the runner skips that category with an explanation
# instead of failing.
.PHONY: check-suite
check-suite:
	@PGDATA="$${PGDATA:-$$($(PSQL) -tAX -c 'SHOW data_directory' 2>/dev/null)}" \
	 PGBIN="$(PG_BINDIR)" \
	 $(SUITE_DIR)/run_all.sh $(CATEGORIES)

# Everything except the crash test: no PGDATA needed, nothing is restarted,
# safe against a server you do not own (a managed instance, or a colleague's).
.PHONY: check-suite-nowal
check-suite-nowal:
	@SKIP_WAL=1 PGBIN="$(PG_BINDIR)" $(SUITE_DIR)/run_all.sh

# The crash-recovery category on its own. This one STOPS AND RESTARTS THE
# SERVER -- `pg_ctl -m immediate stop`, i.e. no clean shutdown and no
# shutdown checkpoint, so recovery has to replay from WAL. That is the
# whole point of the test and a graceful restart would prove nothing, but
# it means: do not run this against anything you care about.
.PHONY: check-wal
check-wal:
	@PGDATA="$${PGDATA:-$$($(PSQL) -tAX -c 'SHOW data_directory' 2>/dev/null)}" \
	 PGBIN="$(PG_BINDIR)" \
	 $(SUITE_DIR)/run_all.sh 09_wal

# `make test` is the friendly entry point: the full suite minus the crash
# category. Nothing under it restarts a server.
#
# Deliberately NOT named `check`. pgxs.mk already defines `check` (as a stub
# that prints "make check is not supported", steering you to installcheck),
# and because it is included ABOVE these rules, a second `check:` here does
# not override it -- make merges the prerequisites and runs BOTH recipes, so
# the suite passes and the run still ends with PGXS's refusal message. Same
# applies to `installcheck`. Use names pgxs.mk does not own.
.PHONY: test
test: check-suite

.PHONY: test-all check-all
test-all check-all: check-suite

.PHONY: help
help:
	@echo "Build:"
	@echo "  make [WITH_ROARING=1]      build (WITH_ROARING links CRoaring)"
	@echo "  make install"
	@echo ""
	@echo "Test:"
	@echo "  make test                  full suite, no server restart"
	@echo "  make test-all              full suite INCLUDING the crash test"
	@echo "  make check-suite           canonical suite, all categories"
	@echo "  make check-suite-nowal     canonical suite minus crash recovery"
	@echo "  make check-wal             crash recovery ONLY (restarts the server)"
	@echo ""
	@echo "  CATEGORIES=\"03_regex 05_multicolumn\"   run selected categories"
	@echo "  PGHOST= PGPORT= PGUSER= PGDATABASE=       standard libpq variables"
