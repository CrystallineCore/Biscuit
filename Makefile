# Makefile for biscuit PostgreSQL extension

EXTENSION = biscuit
EXTVERSION = 2.1.0
MODULE_big = biscuit
OBJS = src/biscuit.o
DATA = sql/biscuit--1.0.sql


PGFILEDESC = "LIKE pattern matching with bitmap indexing"

# PostgreSQL build system
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Compiler flags for stricter checking
override CFLAGS += -Wall -Wmissing-prototypes -Wpointer-arith -Werror=vla -Wendif-labels

# Default target: ensure versioned SQL is generated before build
all: sql/biscuit--1.0.sql

# Build versioned SQL script from base SQL file if needed
sql/biscuit--1.0.sql: sql/biscuit.sql
	cp $< $@

# Clean up build artifacts
.PHONY: clean
clean:
	rm -f src/biscuit.o src/biscuit.bc biscuit.so

# Manual install target (optional; PGXS normally handles this)
install: all
	$(INSTALL) -d $(DESTDIR)$(pkglibdir)
	$(INSTALL) -m 755 biscuit.so $(DESTDIR)$(pkglibdir)/
	$(INSTALL) -d $(DESTDIR)$(datadir)/extension
	$(INSTALL) -m 644 biscuit.control $(DESTDIR)$(datadir)/extension/
	$(INSTALL) -m 644 sql/biscuit--1.0.sql $(DESTDIR)$(datadir)/extension/

dist:
	@echo "Creating distribution archive..."
	rm -rf dist
	mkdir dist
	cp -r $(shell ls | grep -v dist) dist/
	cd dist && zip -r ../$(EXTENSION)-$(EXTVERSION).zip .
	@echo "Created $(EXTENSION)-$(EXTVERSION).zip"