# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
sys.path.insert(0, os.path.abspath('../..'))

project = 'neosqlite'
copyright = '2025, cwt'
author = 'cwt'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
]

templates_path = ['_templates']
exclude_patterns: list[str] = []

# Configure autodoc to reduce cross-reference warnings
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'private-members': True,
    'exclude-members': '__weakref__'
}

# Tell Sphinx to prefer the explicit locations over the re-exports
autodoc_typehints_description_target = 'documented'

# Suppress Python cross-reference warnings
# These warnings occur because the main package re-exports classes from their defining modules,
# creating ambiguous references. For example, Collection exists in both neosqlite.collection
# (its defining module) and neosqlite (as a re-export). Since the documentation is working
# correctly despite these warnings, we suppress them to reduce noise.
suppress_warnings = [
    'ref.python'
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
html_static_path = ['_static']
