"""
A configuration file for Sphinx
"""


# -- Path setup --------------------------------------------------------------

import sys, os

sys.path.append(".")


# -- Project information -----------------------------------------------------

project = "Kart"
copyright = "2022, Kart Contributors"
author = "Kart Contributors"
release = "0.11.4.dev0"


# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx_rtd_theme",
    "sphinx.ext.extlinks",
    "sphinx.ext.autosectionlabel",
]
templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "include/links.rst",
]


with open("./include/links.rst") as f:
    rst_epilog = f.read(-1)

nitpicky = True

# -- Options for HTML output -------------------------------------------------

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
