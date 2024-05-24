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
    "sphinx_design",
    "sphinx_toggleprompt",
    "numpydoc",
    "sphinxcontrib.autodoc_pydantic",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable/", None),
    "requests": ("https://requests.readthedocs.io/en/latest/", None),
}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# HTML configuration
# ------------------
html_theme = "alabaster"
html_static_path = ["_static"]

html_theme_options = {
    "logo": "logo-full-padded.png",
    "description": "The Python entry point for Algoseek datasets",
    "fixed_sidebar": True,
    "sidebar_width": "300px",
    "page_width": "1000px",
}

# autodoc-pydantic
# ----------------
autodoc_pydantic_model_show_json = False
autodoc_pydantic_model_show_field_summary = False
autodoc_pydantic_model_show_config_summary = False
autodoc_pydantic_model_undoc_members = False
autodoc_pydantic_field_list_validators = False
autodoc_pydantic_field_show_constraints = False

autodoc_pydantic_settings_show_json = False
autodoc_pydantic_settings_show_config_summary = False
autodoc_pydantic_settings_show_field_summary = False
autodoc_pydantic_settings_show_validator_members = False
autodoc_pydantic_settings_show_validator_summary = False