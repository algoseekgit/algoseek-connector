# Project metadata
# ----------------
project = "algoseek-connector"
copyright = "2023, AlgoSeek LLC"
author = "AlgoSeek LLC"

# General configuration
# ---------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_toggleprompt",
    "numpydoc",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable/", None),
}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# HTML configuration
# ------------------
html_theme = "alabaster"
html_static_path = ["_static"]
