# Makefile for Sphinx documentation
#

# You can set these variables from the command line.
# Hypen `n` below is for "nit-picky mode"
# https://www.sphinx-doc.org/en/master/man/sphinx-build.html#cmdoption-sphinx-build-n
SPHINXOPTS    = -n
SPHINXBUILD   = sphinx-build
PAPER         =
BUILDDIR      = _build

# Internal variables.
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .
# the i18n builder cannot share the environment and doctrees with the others
I18NSPHINXOPTS  = $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .

# DOCS_PACKAGE_FILENAME will be set by Harness - we want to use it if it is, otherwise we will use the version
VERSION = $(shell python version.py)
DOCS_PACKAGE_FILENAME ?= datarobot_examples_$(VERSION).zip

.PHONY: help clean html dirhtml singlehtml md mdzip pickle json htmlhelp epub latex latexpdf xelatexpdf text \
	man changes linkcheck doctest gettext

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  html       to make standalone HTML files"
	@echo "  dirhtml    to make HTML files named index.html in directories"
	@echo "  singlehtml to make a single large HTML file"
	@echo "  pickle     to make pickle files"
	@echo "  json       to make JSON files"
	@echo "  htmlhelp   to make HTML files and a HTML help project"
	@echo "  md         to make Markdown files"
	@echo "  epub       to make an epub"
	@echo "  latex      to make LaTeX files, you can set PAPER=a4 or PAPER=letter"
	@echo "  xelatexpdf to make xeLaTeX files and run them through pdflatex"
	@echo "  text       to make text files"
	@echo "  man        to make manual pages"
	@echo "  texinfo    to make Texinfo files"
	@echo "  info       to make Texinfo files and run them through makeinfo"
	@echo "  gettext    to make PO message catalogs"
	@echo "  changes    to make an overview of all changed/added/deprecated items"
	@echo "  linkcheck  to check all external links for integrity"
	@echo "  doctest    to run all doctests embedded in the documentation (if enabled)"

clean:
	-rm -rf $(BUILDDIR)/*

html:
	$(SPHINXBUILD) -W  -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

htmlzip: html
	cd $(BUILDDIR)/html && zip $(DOCS_PACKAGE_FILENAME) -r ./*
	echo "\nMade $(BUILDDIR)/html/$(DOCS_PACKAGE_FILENAME) for upload to readthedocs"

dirhtml:
	$(SPHINXBUILD) -W -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

singlehtml:
	$(SPHINXBUILD) -W -b singlehtml $(ALLSPHINXOPTS) $(BUILDDIR)/singlehtml
	@echo
	@echo "Build finished. The HTML page is in $(BUILDDIR)/singlehtml."

PNG_FILES = $(shell find . -name '*.png' | grep -v '_build')
md:
	$(SPHINXBUILD) -b markdown $(ALLSPHINXOPTS) $(BUILDDIR)/md
	tar cf - $(PNG_FILES) | tar xf - -C _build/md
	@echo
	@echo "Build finished. The Markdown pages are in $(BUILDDIR)/md."

mdzip: md
	cd $(BUILDDIR) && zip md/$(DOCS_PACKAGE_FILENAME) -r --test --move ./md/*
	echo "\nMade $(BUILDDIR)/md/$(DOCS_PACKAGE_FILENAME) for upload."

pickle:
	$(SPHINXBUILD) -W -b pickle $(ALLSPHINXOPTS) $(BUILDDIR)/pickle
	@echo
	@echo "Build finished; now you can process the pickle files."

json:
	$(SPHINXBUILD) -W -b json $(ALLSPHINXOPTS) $(BUILDDIR)/json
	@echo
	@echo "Build finished; now you can process the JSON files."

htmlhelp:
	$(SPHINXBUILD) -W -b htmlhelp $(ALLSPHINXOPTS) $(BUILDDIR)/htmlhelp
	@echo
	@echo "Build finished; now you can run HTML Help Workshop with the" \
	      ".hhp project file in $(BUILDDIR)/htmlhelp."

epub:
	$(SPHINXBUILD) -W -b epub $(ALLSPHINXOPTS) $(BUILDDIR)/epub
	@echo
	@echo "Build finished. The epub file is in $(BUILDDIR)/epub."

latex:
	$(SPHINXBUILD) -W -b latex $(ALLSPHINXOPTS) $(BUILDDIR)/latex
	@echo
	@echo "Build finished; the LaTeX files are in $(BUILDDIR)/latex."
	@echo "Run \`make' in that directory to run these through (pdf)latex" \
	      "(use \`make xelatexpdf' here to do that automatically)."

xelatexpdf:
	-$(SPHINXBUILD) -W -b latex $(ALLSPHINXOPTS) $(BUILDDIR)/latex
	@echo "Running LaTeX files through pdflatex..."
	-cd $(BUILDDIR)/latex && make PDFLATEX=xelatex all-pdf
	@echo "Making index..."
	-cd $(BUILDDIR)/latex && makeindex -s python.ist *.idx
	@echo "Re-running LaTeX files through xelatex to build index..."
	-cd $(BUILDDIR)/latex && make PDFLATEX=xelatex all-pdf
	@echo "pdflatex finished; the PDF files are in $(BUILDDIR)/latex."

text:
	$(SPHINXBUILD) -W -b text $(ALLSPHINXOPTS) $(BUILDDIR)/text
	@echo
	@echo "Build finished. The text files are in $(BUILDDIR)/text."

man:
	$(SPHINXBUILD) -W -b man $(ALLSPHINXOPTS) $(BUILDDIR)/man
	@echo
	@echo "Build finished. The manual pages are in $(BUILDDIR)/man."

texinfo:
	$(SPHINXBUILD) -W -b texinfo $(ALLSPHINXOPTS) $(BUILDDIR)/texinfo
	@echo
	@echo "Build finished. The Texinfo files are in $(BUILDDIR)/texinfo."
	@echo "Run \`make' in that directory to run these through makeinfo" \
	      "(use \`make info' here to do that automatically)."

info:
	$(SPHINXBUILD) -W -b texinfo $(ALLSPHINXOPTS) $(BUILDDIR)/texinfo
	@echo "Running Texinfo files through makeinfo..."
	make -C $(BUILDDIR)/texinfo info
	@echo "makeinfo finished; the Info files are in $(BUILDDIR)/texinfo."

gettext:
	$(SPHINXBUILD) -W -b gettext $(I18NSPHINXOPTS) $(BUILDDIR)/locale
	@echo
	@echo "Build finished. The message catalogs are in $(BUILDDIR)/locale."

changes:
	$(SPHINXBUILD) -W -b changes $(ALLSPHINXOPTS) $(BUILDDIR)/changes
	@echo
	@echo "The overview file is in $(BUILDDIR)/changes."

linkcheck:
	$(SPHINXBUILD) -W -b linkcheck $(ALLSPHINXOPTS) $(BUILDDIR)/linkcheck
	@echo
	@echo "Link check complete; look for any errors in the above output " \
	      "or in $(BUILDDIR)/linkcheck/output.txt."

spellingtest:
	$(SPHINXBUILD) -W -b spelling . $(BUILDDIR)/spellingtest
	@echo "Testing of spelling in the sources finished, look at the " \
	      "results in $(BUILDDIR)/spellingtest/output.txt."

doctest:
	$(SPHINXBUILD) -W -b doctest $(ALLSPHINXOPTS) $(BUILDDIR)/doctest
	@echo "Testing of doctests in the sources finished, look at the " \
	      "results in $(BUILDDIR)/doctest/output.txt."
