# Makefile for biscuit PostgreSQL extension

EXTENSION = biscuit
EXTVERSION = 2.2.2

MODULE_big = biscuit

# Automatically compile all source files in src/
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c))

DATA = sql/biscuit--1.0.sql

PGFILEDESC = "Wildcard pattern matching through bitmap indexing"

# PostgreSQL build system
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

# Detect OS
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# Try to detect CRoaring library (optional)
ROARING_CFLAGS := $(shell pkg-config --cflags roaring 2>/dev/null)
ROARING_LIBS := $(shell pkg-config --libs roaring 2>/dev/null)

# Fallback to common installation paths if pkg-config fails
ifeq ($(ROARING_CFLAGS),)

ROARING_INCLUDE := $(shell \
	for dir in /usr/include /usr/local/include /opt/homebrew/include /opt/local/include; do \
		if [ -f $$dir/roaring/roaring.h ]; then \
			echo "-I$$dir"; \
			break; \
		fi; \
	done)

ifneq ($(ROARING_INCLUDE),)
ROARING_CFLAGS := $(ROARING_INCLUDE)
endif
endif

ifeq ($(ROARING_LIBS),)

ifeq ($(UNAME_S),Darwin)

# macOS
ROARING_LIBDIR := $(shell \
	for dir in /usr/local/lib /opt/homebrew/lib /opt/local/lib; do \
		if [ -f $$dir/libroaring.dylib ] || [ -f $$dir/libroaring.a ]; then \
			echo "$$dir"; \
			break; \
		fi; \
	done)

else ifeq ($(UNAME_S),Linux)

# Linux
ROARING_LIBDIR := $(shell \
	for dir in \
		/usr/lib/$(shell gcc -print-multiarch 2>/dev/null) \
		/usr/lib64 \
		/usr/lib \
		/usr/local/lib \
		/usr/local/lib64; do \
		if [ -f $$dir/libroaring.so ] || [ -f $$dir/libroaring.a ]; then \
			echo "$$dir"; \
			break; \
		fi; \
	done)

else

# Other Unix-like systems
ROARING_LIBDIR := $(shell \
	for dir in /usr/local/lib /usr/lib /opt/local/lib; do \
		if [ -f $$dir/libroaring.so ] || [ -f $$dir/libroaring.a ]; then \
			echo "$$dir"; \
			break; \
		fi; \
	done)

endif

ifneq ($(ROARING_LIBDIR),)
ROARING_LIBS := -L$(ROARING_LIBDIR) -lroaring
endif
endif

# Apply CRoaring flags only if library was found
ifneq ($(ROARING_CFLAGS),)
ifneq ($(ROARING_LIBS),)

PG_CPPFLAGS += -DHAVE_ROARING $(ROARING_CFLAGS)
SHLIB_LINK += $(ROARING_LIBS)

ROARING_FOUND = yes

# Add rpath for runtime library discovery (Linux/BSD)
ifneq ($(UNAME_S),Darwin)
SHLIB_LINK += -Wl,-rpath,'$$ORIGIN'

ifneq ($(ROARING_LIBDIR),)
SHLIB_LINK += -Wl,-rpath,$(ROARING_LIBDIR)
endif

endif
endif
endif

include $(PGXS)

# Compiler flags
override CFLAGS += \
	-Wall \
	-Wextra \
	-Wmissing-prototypes \
	-Wpointer-arith \
	-Werror=vla \
	-Wendif-labels \
	-fPIC

# Default target
all: sql/biscuit--1.0.sql

# Build versioned SQL script
sql/biscuit--1.0.sql: sql/biscuit.sql
	@mkdir -p sql
	cp $< $@

# Verify dependencies before building
.PHONY: check-deps
check-deps:
	@echo "Checking dependencies..."
	@command -v $(PG_CONFIG) >/dev/null 2>&1 || { \
		echo "PostgreSQL pg_config not found."; \
		echo "Install postgresql-server-dev."; \
		exit 1; \
	}
	@echo "PostgreSQL version: $$($(PG_CONFIG) --version)"

ifeq ($(ROARING_FOUND),yes)
	@echo "CRoaring library: FOUND"
	@echo "  CFLAGS: $(ROARING_CFLAGS)"
	@echo "  LIBS: $(ROARING_LIBS)"
else
	@echo "CRoaring library: NOT FOUND (using fallback bitmap implementation)"
	@echo "For better performance install CRoaring:"
	@echo "  Debian/Ubuntu: apt install libroaring-dev"
	@echo "  macOS: brew install croaring"
	@echo "  Source: https://github.com/RoaringBitmap/CRoaring"
endif

	@echo "All required dependencies found."

# Clean build artifacts
.PHONY: clean
clean:
	rm -f src/*.o
	rm -f src/*.bc
	rm -f biscuit.so
	rm -f sql/biscuit--1.0.sql

# Distribution target
.PHONY: dist
dist:
	@echo "Creating distribution archive for version $(EXTVERSION)..."
	@rm -rf dist
	@mkdir dist
	@cp -r $$(ls | grep -v -E '^(dist|.*\.zip)$$') dist/
	@cd dist && zip -r ../$(EXTENSION)-$(EXTVERSION).zip .
	@rm -rf dist
	@echo "Created $(EXTENSION)-$(EXTVERSION).zip"

# Help target
.PHONY: help
help:
	@echo "Biscuit PostgreSQL Extension v$(EXTVERSION)"
	@echo ""
	@echo "Targets:"
	@echo "  make              - Build the extension"
	@echo "  make check-deps   - Verify dependencies"
	@echo "  make install      - Install the extension"
	@echo "  make clean        - Remove build artifacts"
	@echo "  make dist         - Create distribution archive"
	@echo ""
	@echo "Environment variables:"
	@echo "  PG_CONFIG         - Path to pg_config"
	@echo ""
	@echo "Requirements:"
	@echo "  - PostgreSQL development headers"
	@echo "  - CRoaring (optional)"
	@echo ""
	@echo "Notes:"
	@echo "  - Falls back to internal bitmap implementation if CRoaring is unavailable."
	@echo "  - Run 'make check-deps' to inspect detected libraries."

.PHONY: all install