import os
import sys

# Add project root to Python path if documentation imports project modules.
sys.path.insert(0, os.path.abspath("../../"))

# =============================================================================
# Project Information
# =============================================================================

project = "BISCUIT"
author = "Sivaprasad Murali"
copyright = "2026, Sivaprasad Murali"

# Exact release and documentation series.
release = "3.0.0"
version = "3.0"

# =============================================================================
# General Configuration
# =============================================================================

extensions = [
    "myst_parser",
    "sphinx_sitemap",
    "sphinx.ext.mathjax",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
]

templates_path = ["_templates"]

exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
]

# =============================================================================
# Source Files
# =============================================================================

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

master_doc = "index"

language = "en"

# =============================================================================
# MyST Markdown
# =============================================================================

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "smartquotes",
    "tasklist",
    "attrs_inline",
    "attrs_block",
]

myst_heading_anchors = 3

# =============================================================================
# HTML Output
# =============================================================================

html_theme = "sphinx_rtd_theme"

html_static_path = ["_static"]

html_title = (
    "BISCUIT 3.0.0 Documentation - PostgreSQL Index Access Method"
)

html_short_title = "BISCUIT Docs"

html_favicon = "_static/favicon.ico"
html_logo = "_static/logo.png"

html_theme_options = {
    "collapse_navigation": False,
    "sticky_navigation": True,
    "navigation_depth": 4,
    "style_nav_header_background": "#2C3E50",
}

pygments_style = "sphinx"

# =============================================================================
# Canonical URL
# =============================================================================

html_baseurl = "https://biscuit.readthedocs.io/"

# =============================================================================
# SEO Metadata
# =============================================================================

# =============================================================================
# SEO Metadata
# =============================================================================

html_meta = {
    "description": (
        "BISCUIT 3.0.0 is a PostgreSQL Index Access Method (IAM) designed "
        "for fast and deterministic wildcard pattern matching. It provides "
        "specialized bitmap-based indexing for SQL LIKE and ILIKE queries "
        "and supports multi-column pattern matching workloads in PostgreSQL."
    ),

    "keywords": (
        "BISCUIT, BISCUIT PostgreSQL, BISCUIT IAM, "
        "PostgreSQL Index Access Method, PostgreSQL IAM, "
        "PostgreSQL index, PostgreSQL extension, "
        "PostgreSQL wildcard search, PostgreSQL wildcard index, "
        "PostgreSQL pattern matching, PostgreSQL LIKE index, "
        "PostgreSQL ILIKE index, PostgreSQL LIKE optimization, "
        "PostgreSQL ILIKE optimization, PostgreSQL wildcard matching, "
        "PostgreSQL text indexing, PostgreSQL performance, "
        "PostgreSQL database indexing, PostgreSQL multi-column index, "
        "deterministic index, bitmap index, bitmap indexing, "
        "PGXN, PostgreSQL WAL, PostgreSQL crash recovery, "
        "PostgreSQL point-in-time recovery, PostgreSQL streaming replication"
    ),

    "author": author,

    "robots": "index, follow",

    "viewport": "width=device-width, initial-scale=1.0",

    # =========================================================================
    # OpenGraph / Social Sharing
    # =========================================================================

    "og:title": (
        "BISCUIT 3.0.0 - PostgreSQL Index Access Method "
        "for Fast Wildcard Pattern Matching"
    ),

    "og:description": (
        "BISCUIT is a deterministic PostgreSQL Index Access Method "
        "for high-performance wildcard pattern matching with SQL LIKE "
        "and ILIKE queries. It provides bitmap-based indexing and "
        "multi-column index support. BISCUIT 3.0.0 introduces WAL "
        "integration and durable index storage for crash recovery, "
        "point-in-time recovery, and PostgreSQL replication."
    ),

    "og:type": "website",

    "og:url": html_baseurl,

    "og:site_name": "BISCUIT Documentation",

    "og:image": (
        "https://biscuit.readthedocs.io/en/latest/"
        "_static/logo.png"
    ),

    # =========================================================================
    # Twitter / X
    # =========================================================================

    "twitter:card": "summary_large_image",

    "twitter:title": (
        "BISCUIT 3.0.0 - PostgreSQL Index Access Method"
    ),

    "twitter:description": (
        "A deterministic PostgreSQL Index Access Method for fast "
        "wildcard pattern matching with LIKE and ILIKE, bitmap-based "
        "indexing, and multi-column search support."
    ),

    "twitter:image": (
        "https://biscuit.readthedocs.io/en/latest/"
        "_static/logo.png"
    ),
}
# =============================================================================
# Sitemap
# =============================================================================

sitemap_url_scheme = "{link}"

# =============================================================================
# Documentation Behavior
# =============================================================================

todo_include_todos = False

nitpicky = False

html_show_sourcelink = True
html_show_sphinx = False
html_show_copyright = True
