# Makefile for biscuit PostgreSQL extension

EXTENSION = biscuit
EXTVERSION = 2.1.4
MODULE_big = biscuit
OBJS = src/biscuit.o
DATA = sql/biscuit--1.0.sql

PGFILEDESC = "Wildcard pattern matching through bitmap indexing"

# PostgreSQL build system
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

# Detect OS
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# Try pkg-config first for Roaring
ROARING_CFLAGS := $(shell pkg-config --cflags roaring 2>/dev/null)
ROARING_LIBS := $(shell pkg-config --libs roaring 2>/dev/null)

# Fallback to common installation paths if pkg-config fails
ifeq ($(ROARING_CFLAGS),)
    # Check common include directories
    ROARING_INCLUDE := $(shell \
        for dir in /usr/include /usr/local/include /opt/homebrew/include /opt/local/include; do \
            if [ -f $$dir/roaring/roaring.h ]; then \
                echo "-I$$dir"; \
                break; \
            fi; \
        done)
    
    ifeq ($(ROARING_INCLUDE),)
        $(error CRoaring library headers not found. Install via: apt-get install libroaring-dev, brew install croaring, or from https://github.com/RoaringBitmap/CRoaring)
    endif
    ROARING_CFLAGS := $(ROARING_INCLUDE)
endif

ifeq ($(ROARING_LIBS),)
    # Determine library directory based on OS and architecture
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
        # Linux - check multiarch and standard paths
        ROARING_LIBDIR := $(shell \
            for dir in /usr/lib/$(shell gcc -print-multiarch) /usr/lib64 /usr/lib /usr/local/lib /usr/local/lib64; do \
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
    
    ifeq ($(ROARING_LIBDIR),)
        $(error CRoaring library not found. Install via: apt-get install libroaring-dev, brew install croaring, or from https://github.com/RoaringBitmap/CRoaring)
    endif
    ROARING_LIBS := -L$(ROARING_LIBDIR) -lroaring
endif

# Apply flags
PG_CPPFLAGS += -DHAVE_ROARING $(ROARING_CFLAGS)
SHLIB_LINK += $(ROARING_LIBS)

# Add rpath for runtime library discovery (Linux/BSD)
ifneq ($(UNAME_S),Darwin)
    SHLIB_LINK += -Wl,-rpath,'$$ORIGIN'
    ifneq ($(ROARING_LIBDIR),)
        SHLIB_LINK += -Wl,-rpath,$(ROARING_LIBDIR)
    endif
endif

include $(PGXS)

# Compiler flags for stricter checking
override CFLAGS += -Wall -Wmissing-prototypes -Wpointer-arith -Werror=vla -Wendif-labels

# Add -fPIC for shared libraries on all platforms
override CFLAGS += -fPIC

# Default target: ensure versioned SQL is generated before build
all: sql/biscuit--1.0.sql

# Build versioned SQL script from base SQL file if needed
sql/biscuit--1.0.sql: sql/biscuit.sql
	@mkdir -p sql
	cp $< $@

# Verify dependencies before building
.PHONY: check-deps
check-deps:
	@echo "Checking dependencies..."
	@command -v $(PG_CONFIG) >/dev/null 2>&1 || { echo "PostgreSQL pg_config not found. Install postgresql-server-dev."; exit 1; }
	@echo "PostgreSQL version: $$($(PG_CONFIG) --version)"
	@echo "Roaring CFLAGS: $(ROARING_CFLAGS)"
	@echo "Roaring LIBS: $(ROARING_LIBS)"
	@echo "All dependencies found."

# Clean up build artifacts
.PHONY: clean
clean:
	rm -f src/biscuit.o src/biscuit.bc biscuit.so
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
	@echo "  make check-deps   - Verify all dependencies are installed"
	@echo "  make install      - Install the extension"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make dist         - Create distribution archive"
	@echo ""
	@echo "Environment variables:"
	@echo "  PG_CONFIG         - Path to pg_config (default: pg_config)"
	@echo ""
	@echo "Requirements:"
	@echo "  - PostgreSQL development files"
	@echo "  - CRoaring library (libroaring-dev)"

.PHONY: all install