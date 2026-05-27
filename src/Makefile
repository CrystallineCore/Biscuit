# Makefile – Biscuit Index Access Method
# Usage: make && make install
#
# Roaring bitmap support: install CRoaring in $(PG_CONFIG --includedir)
# and pass HAVE_ROARING=1 on the command line, e.g.:
#   make HAVE_ROARING=1

MODULE_big = biscuit

OBJS = \
    biscuit_bitmap.o  \
    biscuit_utf8.o    \
    biscuit_cache.o   \
    biscuit_tid.o     \
    biscuit_pattern.o \
    biscuit_index.o   \
    biscuit_scan.o    \
    biscuit.o

EXTENSION  = biscuit
DATA       = biscuit--1.0.sql
PGFILEDESC = "biscuit – pattern-matching index AM"

PG_CONFIG  ?= pg_config
PGXS       := $(shell $(PG_CONFIG) --pgxs)

ifdef HAVE_ROARING
PG_CPPFLAGS += -DHAVE_ROARING
SHLIB_LINK  += -lroaring
endif

include $(PGXS)
