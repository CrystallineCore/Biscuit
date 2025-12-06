import os
import sys
sys.path.insert(0, os.path.abspath('../../'))

# -- Project Information -----------------------------------------------------

project = 'BISCUIT'
author = 'Sivaprasad Murali'
copyright = '2025'
release = '2.0.0'
version = '2.0.0'

# -- General Configuration ---------------------------------------------------

extensions = [
    "myst_parser",          # Markdown support
    "sphinx_sitemap",       # SEO
    "sphinx.ext.mathjax",   # Optional, remove if not needed
]

templates_path = ['_templates']
exclude_patterns = []

# Support Markdown + RST
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

# Main document (index.md)
master_doc = 'index'

# Language
language = 'en'

# -- HTML Output -------------------------------------------------------------

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

html_title = f"{project} {release} Documentation"
html_short_title = "BISCUIT Docs"
html_favicon = '_static/favicon.ico'
html_logo = '_static/logo.png'

html_theme_options = {
    'collapse_navigation': False,
    'sticky_navigation': True,
    'navigation_depth': 4,
    'style_nav_header_background': '#2C3E50',
}

# -- Sitemap / SEO ----------------------------------------------------------

html_baseurl = "https://biscuit.readthedocs.io/"

html_meta = {
    "description": (
        "BISCUIT – A PostgreSQL Index Access Method (IAM) for ultra-fast, "
        "deterministic substring search across multiple columns. "
        "A modern alternative to pg_trgm for high-performance text matching."
    ),
    "keywords": (
        "postgresql index access method, biscuit iam, pgxn biscuit, "
        "substring index, multi-column index, postgres extension, "
        "postgres performance, text search acceleration"
    ),
    "author": "Sivaprasad Murali",
    "robots": "index, follow",
    "viewport": "width=device-width, initial-scale=1.0",

    # OpenGraph / Social
    "og:title": "BISCUIT – PostgreSQL Index Access Method",
    "og:description": "A modern IAM for deterministic multi-column substring search.",
    "og:type": "website",
    "og:url": "https://biscuit.readthedocs.io/",
    "og:image": "https://biscuit.readthedocs.io/en/latest/_static/logo.png",

    "twitter:card": "summary_large_image",
    "twitter:title": "BISCUIT – PostgreSQL IAM",
    "twitter:description": "Fast, deterministic multi-column substring matching.",
}

# -- MyST Configuration (Markdown) ------------------------------------------

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "smartquotes",
    "tasklist",
]

# -- Sitemap Settings --------------------------------------------------------

sitemap_url_scheme = "{link}"
