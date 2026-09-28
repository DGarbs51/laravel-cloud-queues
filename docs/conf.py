"""Sphinx configuration for the laravel-cloud-queues documentation (Read the Docs).

Build locally with::

    uv sync --group docs --all-extras
    uv run sphinx-build -W --keep-going -b html docs docs/_build/html
"""

from __future__ import annotations

import importlib.metadata

project = "Laravel Cloud Queues"
author = "Devon Garbalosa"
copyright = "Devon Garbalosa and contributors"
release = importlib.metadata.version("laravel-cloud-queues")
version = ".".join(release.split(".")[:2])

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
]

source_suffix = {".md": "markdown", ".rst": "restructuredtext"}
root_doc = "index"

# docs/ also holds the project's internal engineering records (the Laravel contract pack,
# audits and decision log). They are linked from the site on GitHub, not built into it.
exclude_patterns = [
    "_build",
    "architecture.md",
    "decisions.md",
    "deviations.md",
    "references.md",
    "audits/**",
    "contract/**",
]

myst_enable_extensions = ["colon_fence", "deflist", "fieldlist"]
myst_heading_anchors = 3

autodoc_member_order = "bysource"
autodoc_typehints = "signature"
autodoc_preserve_defaults = True
autodoc_default_options = {"members": True, "show-inheritance": True}

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

# Nitpicky cross-references would flag every third-party annotation (boto3, FastAPI,
# ParamSpec internals); broken internal links still fail the build via -W.
nitpicky = False

html_theme = "furo"
html_title = "Laravel Cloud Queues"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_theme_options = {
    "source_repository": "https://github.com/DGarbs51/laravel-cloud-queues/",
    "source_branch": "main",
    "source_directory": "docs/",
    "light_css_variables": {
        "color-brand-primary": "#f53003",
        "color-brand-content": "#e02b00",
    },
    "dark_css_variables": {
        "color-brand-primary": "#ff6a3d",
        "color-brand-content": "#ff7a52",
    },
}

copybutton_prompt_text = r"\$ "
copybutton_prompt_is_regexp = True
