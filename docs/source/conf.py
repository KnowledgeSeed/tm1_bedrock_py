# Configuration file for the Sphinx documentation builder.
import os
import sys
import importlib.metadata

sys.path.insert(0, os.path.abspath('..'))

# -- Project information -----------------------------------------------------

project = 'tm1_bedrock_py'
copyright = '2025, KnowledgeSeed'
author = 'KnowledgeSeed'
release = '1.0.0'

# -- General configuration ---------------------------------------------------

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# -- Options for HTML output

html_theme = 'sphinx_rtd_theme'

# -- Options for EPUB output
epub_show_urls = 'footnote'

try:
    release = importlib.metadata.version(project)
except importlib.metadata.PackageNotFoundError:
    release = '1.0.0'

version = '.'.join(release.split('.')[:2])
