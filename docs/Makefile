SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build
LINKCHECKDIR  = _build/html
SPHINXAUTOBUILD = sphinx-autobuild

help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: livehtml
livehtml:
	$(SPHINXAUTOBUILD) -a "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)


.PHONY: fasthtml
fasthtml:
	$(SPHINXAUTOBUILD) "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: checklinks
	checklinks:
	$(SPHINXBUILD) -b linkcheck $(SPHINXOPTS) $(LINKCHECKDIR) $(O)

.PHONY: latex
latex:
	$(SPHINXBUILD) -b latex $(SPHINXOPTS) "$(SOURCEDIR)" "$(BUILDDIR)/latex"
	@echo
	@echo "Build finished; the LaTeX files are in $(BUILDDIR)/latex."
	@echo "Run \`make' in that directory to run these through (pdf)latex" \
	      "(use \`make latexpdf' here to do that automatically)."

.PHONY: latexpdf
latexpdf:
	$(SPHINXBUILD) -b latex $(SPHINXOPTS) "$(SOURCEDIR)" "$(BUILDDIR)/latex"
	@echo "Running LaTeX files through pdflatex..."
	$(MAKE) -C "$(BUILDDIR)/latex" all-pdf
	@echo "pdflatex finished; the PDF files are in $(BUILDDIR)/latex."
