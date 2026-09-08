# Makefile for the Biscuit index access method (PGXS build).
#
# NOTE: no Makefile shipped in the source archive this was reconstructed
# from, so this is a fresh one rather than a patch. If you already have a
# working Makefile, the ONLY change regex support strictly requires is
# adding biscuit_regex.o to OBJS, plus shipping the two new SQL scripts.
# Everything else here is reconstruction and can be ignored.

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
# Two independent layers, deliberately kept separate because they fail for
# different reasons and need different things to run:
#
#   check-regex  Pure unit test of the decomposer. Compiles biscuit_regex.c
#                against stubs (test/pgstub.h) and differentially checks
#                every glob it emits against a real regex engine over a few
#                thousand strings. Needs NO server and NO PostgreSQL
#                headers, so it runs in CI before the extension is even
#                buildable. This is the layer that proves the rewrite is
#                semantically exact.
#
#   check-sql    End-to-end test against a live server (sql/test.sql).
#                Proves the parts that only exist inside a running backend:
#                that the index is actually chosen for the decomposable
#                subset, that results match a sequential scan exactly, that
#                the recheck backstop works, and that opclass gating holds.
#
# sql/test.sql is intentionally NOT wired up as a pg_regress REGRESS target.
# pg_regress compares stdout against a checked-in expected/test.out, which
# has to be regenerated whenever the fixture changes; the script instead
# asserts internally and RAISEs an exception on failure, so its exit status
# is the result and no expected-output file has to be maintained. That also
# keeps it runnable through any client (pgAdmin, JDBC, DBeaver, a migration
# runner), not just psql.

.PHONY: check-regex
check-regex:
	cd test && ./runtests.sh

# Override as needed, e.g.  make check-sql PGDATABASE=mydb
# ON_ERROR_STOP is what turns the script's RAISE EXCEPTION into a non-zero
# exit status; the script itself contains no psql-specific syntax.
PSQL ?= psql
.PHONY: check-sql
check-sql:
	$(PSQL) -v ON_ERROR_STOP=1 -f sql/test.sql

# Adversarial layer: randomised patterns, scan reuse, plan caching, hostile
# data, and regressions for the two bugs those found. Slower than
# check-sql, so it is a separate target.
.PHONY: check-stress
check-stress:
	$(PSQL) -v ON_ERROR_STOP=1 -f sql/stress.sql

.PHONY: check-all
check-all: check-regex check-sql check-stress
