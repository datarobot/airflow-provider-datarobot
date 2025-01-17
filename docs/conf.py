#
# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# DataRobot, Inc.
#
# This is proprietary source code of DataRobot, Inc. and its
# affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
#
# DataRobot Airflow documentation build configuration file, created by
# sphinx-quickstart on Mon Mar 16 10:50:19 2015.
#
# This file is execfile()d with the current directory set to its containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.
import os
import shutil
import sys

from sphinx.addnodes import versionmodified
from sphinx.application import Sphinx
from sphinx.util.docutils import SphinxDirective
from sphinx_markdown_builder.translator import MarkdownTranslator

sys.path.insert(0, os.path.abspath(".."))
from datarobot import __version__ as version  # noqa

# Make sure magic 'tags' field is defined.
# (Not actually useful but makes Cython and pylint errors go away.)
if "tags" not in locals():
    tags = None

# Copy the changelog to the docs directory so sphinx will include it
root_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
changelog_path = os.path.join(root_dir, "CHANGES.md")
dest_path = os.path.join(root_dir, "docs", "CHANGES.md")
assert os.path.isfile(changelog_path)
shutil.copyfile(changelog_path, dest_path)

# -- General configuration -----------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
needs_sphinx = "8.1.3"

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    # "sphinxcontrib.spelling",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
    "sphinx.ext.doctest",
    "numpydoc",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_markdown_builder",
    "sphinx_autodoc_typehints",
    "myst_parser",
]

myst_enable_extensions = [
    "amsmath",
    "attrs_inline",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
    # "linkify",
    "replacements",
    "smartquotes",
    "strikethrough",
    "substitution",
    "tasklist",
]
# AUTODOC SETUP
# MyST options
myst_heading_anchors = 3
myst_heading_level_offset = 1
autosummary_generate = False
autodoc_class_signature = "separated"

autodoc_default_options = {
    "exclude-members": "__init__",
    "undoc-members": False,
    "private-members": False,
    "special-members": False,
    "show-inheritance": False,
}

autodoc_member_order = "bysource"

# NAPOLEON SETUP
napoleon_numpy_docstring = True
napoleon_use_param = True
napoleon_use_keyword = True
napoleon_use_rtype = True
napoleon_use_ivar = True
napoleon_preprocess_types = True
napoleon_attr_annotations = False
# make warnings go away- as advised by
# https://github.com/phn/pytpm/issues/3#issuecomment-12133978
numpydoc_show_class_members = False

nbsphinx_execute = "never"

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The suffix of source filenames.
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# TODO: Cleanup all errors and warnings ATM supress some of them
# # Suppress specific warnings
suppress_warnings = [
    "ref.python",
    "ref.class",
    "ref.exc",
    "ref.meth",
    "autodoc",
]  # Suppress warnings about missing references

# Showing automatically documented members in same order as they are in source

# The encoding of source files.
# source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = "index"

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# Thes are both the full X.Y.Z version, including alpha/beta/rc tags.
# We think Sphinx probably wants both to be defined.
release = version

project = "DataRobot Airflow"
copyright = "2025, DataRobot, Inc"


# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
# language = None

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
# today = ''
# Else, today_fmt is used as the format for a strftime call.
# today_fmt = '%B %d, %Y'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["_build", "**/.ipynb_checkpoints"]

# The reST default role (used for this markup: `text`) to use for all documents.
# default_role = None

# If true, '()' will be appended to :func: etc. cross-reference text.
# add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
# add_module_names = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
# show_authors = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"

# A list of ignored prefixes for module index sorting.
# modindex_common_prefix = []


# -- Options for HTML output ---------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

html_theme = "sphinx_rtd_theme"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
# html_theme_options = {}

# Add any paths that contain custom themes here, relative to this directory.
# html_theme_path = []

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
# html_title = None

# A shorter title for the navigation bar.  Default is the same as html_title.
# html_short_title = None

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
# html_logo = None

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
# html_favicon = None

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_extra_path = ["_html"]

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
# html_last_updated_fmt = '%b %d, %Y'

# If true, SmartyPants will be used to convert quotes and dashes to
# typographically correct entities.
# html_use_smartypants = True

# Custom sidebar templates, maps document names to template names.
# html_sidebars = {}

# Additional templates that should be rendered to pages, maps page names to
# template names.
# html_additional_pages = {}

# If false, no module index is generated.
# html_domain_indices = True

# If false, no index is generated.
# html_use_index = True

# If true, the index is split into individual pages for each letter.
# html_split_index = False

# If true, links to the reST sources are added to the pages.
# html_show_sourcelink = True

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
# html_show_sphinx = True

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
# html_show_copyright = True

# If true, an OpenSearch description file will be output, and all pages will
# contain a <link> tag referring to it.  The value of this option must be the
# base URL from which the finished HTML is served.
# html_use_opensearch = ''

# This is the file name suffix for HTML files (e.g. ".xhtml").
# html_file_suffix = None

# Output file base name for HTML help builder.
htmlhelp_basename = "DataRobotAirflowDoc"


# -- Options for Markdown output -----------------------------------------------
# Adapted from https://github.com/liran-funaro/sphinx-markdown-builder?tab=readme-ov-file#configurations

# If set to True, then anchors will be added before each section/function/class signature.
# This allows references to a specific anchor in the document.
# markdown_anchor_sections: True
# markdown_anchor_signatures: True

# If set to True, adds metadata to the top of each document containing author, copyright,
# and version.
markdown_docinfo: True

# If set, all references will link to this prefix address.
# markdown_http_base: "https://your-domain.com/docs"

# If set, all references will link to documents with this suffix.
# markdown_uri_doc_suffix: ".html"

# -- Options for LaTeX output --------------------------------------------------

# latex_elements = {
#     The paper size ('letterpaper' or 'a4paper').
#     'papersize': 'letterpaper',

#     The font size ('10pt', '11pt' or '12pt').
#     'pointsize': '10pt',

#     Additional stuff for the LaTeX preamble.
#     'preamble': '',
# }

fh = open("latex_preamble.tex", "r+")
PREAMBLE = fh.read()
fh.close()
latex_elements = {
    "inputenc": "",
    "utf8extra": "",
    "preamble": PREAMBLE,
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author, documentclass [howto/manual]).
latex_documents = [
    (
        "index",
        f"DataRobotAirflow_{version}_Docs.tex",
        "DataRobot Airflow Documentation",
        "DataRobot, Inc.",
        "manual",
    ),
]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
# latex_logo = None

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
# latex_use_parts = False

# If true, show page references after internal links.
# latex_show_pagerefs = False

# If true, show URL addresses after external links.
# latex_show_urls = False

# Documents to append as an appendix to all manuals.
# latex_appendices = []

# If false, no module index is generated.
# latex_domain_indices = True


# -- Options for manual page output --------------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    ("index", "datarobotairflow", "DataRobot Airflow Documentation", ["DataRobot, Inc."], 1)
]

# If true, show URL addresses after external links.
# man_show_urls = False


# -- Options for Texinfo output ------------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (
        "index",
        "DataRobotAirflow",
        "DataRobot Airflow Documentation",
        "DataRobot, Inc.",
        "DataRobotAirflow",
        "One line description of project.",
        "Miscellaneous",
    ),
]

# Documents to append as an appendix to all manuals.
# texinfo_appendices = []

# If false, no module index is generated.
# texinfo_domain_indices = True

# How to display URL addresses: 'footnote', 'no', or 'inline'.
# texinfo_show_urls = 'footnote'

# Nitpick targets to ignore:
# These are third-party packages that don't expose documentation for sphinx to link

nitpick_ignore = [
    ("py:data", "typing.Tuple"),
    ("py:data", "typing.Optional"),
    ("py:data", "typing.Union"),
    ("py:data", "typing.NoReturn"),
    ("py:data", "typing.Any"),
    ("py:class", "typing.Self"),
    ("py:class", "pandas.core.frame.DataFrame"),
    ("py:class", "requests.models.Response"),
    ("py:class", "datetime.datetime"),
    ("py:class", "PIL.Image.Image"),
    ("py:class", "collections.OrderedDict"),
]


def better_versionmodified(self: SphinxDirective, node: versionmodified) -> None:
    if version := node.get("version"):
        self.add(f"<!-- md:version {version} -->", prefix_eol=1)


def setup(app: Sphinx) -> None:
    MarkdownTranslator.visit_versionmodified = better_versionmodified

    app.add_css_file("css/tight_table.css")
