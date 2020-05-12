export PREFIX := $(abspath env)
export PATH := $(abspath env/bin):$(PATH)

ifeq ($(OS),Windows_NT)
	$(error "On Windows, run `nmake /f makefile.vc` instead.")
else
	PLATFORM := $(shell uname -s)
endif

export CFLAGS += -O2
export CXXFLAGS += -O2
CCACHE_DIR ?= $(abspath .)/.ccache

vendor-Linux-archive = dist/vendor-Linux.tar.gz
vendor-Darwin-archive = dist/vendor-Darwin.tar.gz

ifeq ($(PLATFORM),Darwin)
	LIBSUFFIX = dylib
	WHEELTOOL = delocate
	PY3 ?= $(realpath /Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7)
	WHEELID = macosx_10_9
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	WHEELTOOL = auditwheel
	WHEELID = manylinux2010_x86_64
endif
PY3 := $(or $(PY3),python3.7)

PYDEPS := pip wheel $(WHEELTOOL)

packages = sqlite proj jsonc geos gdal spatialite spatialindex libgit2 pygit2 git
ifeq ($(PLATFORM),Linux)
packages += curl
endif

lib-spatialite := env/lib/mod_spatialite.$(LIBSUFFIX)
lib-gdal := gdal/wheelhouse/GDAL-*$(WHEELID)*.whl
lib-pygit2 := pygit/wheelhouse/pygit2-*$(WHEELID)*.whl
lib-spatialindex := env/lib/libspatialindex_c.$(LIBSUFFIX)
lib-git := env/bin/git
libs := $(lib-gdal) $(lib-spatialite) $(lib-pygit2) $(lib-git) $(lib-spatialindex)


# default target
.PHONY: all
all:
	$(MAKE) $(vendor-$(PLATFORM)-archive)

.PHONY: build
build: $(libs)

# dependencies
proj: sqlite
spatialite: sqlite geos proj
gdal: proj geos sqlite jsonc
pygit2: libgit2

ifeq ($(PLATFORM),Linux)
git: curl
endif

env:
	@echo PY3=$(PY3)
	$(PY3) -m venv --copies env
	env/bin/pip install -U $(PYDEPS)

$(lib-gdal): gdal

$(lib-pygit2): libgit2
	$(MAKE) -C pygit2 wheels

$(lib-spatialite): spatialite

$(lib-spatialindex): spatialindex

$(lib-git): git

.PHONY: $(packages)
$(packages): | env
	@echo PATH=$$PATH PREFIX=$$PREFIX
	$(MAKE) -C $@ install

packages_clean := $(addsuffix _clean,$(packages))

%_clean:
	$(MAKE) -C $* clean

packages_archive := $(addsuffix _archive,$(packages))

%_archive:
	$(MAKE) -C $* archive

.PHONY: sources
sources: $(packages_archive)

.PHONY: clean
clean: $(packages_clean)
	-$(RM) -r env/ wheelhouse/ dist/

packages_cleaner := $(addsuffix _cleaner,$(packages))
%_cleaner:
	$(MAKE) -C $* cleaner

.PHONY: cleaner
cleaner: clean $(packages_cleaner)

packages_cleanest := $(addsuffix _cleanest,$(packages))
%_cleanest:
	$(MAKE) -C $* cleanest

.PHONY: cleanest
cleanest: cleaner $(packages_cleanest)
	-$(RM) -r .ccache/

packages_cleanconfig := $(addsuffix _cleanconfig,$(packages))
%_cleanconfig:
	$(MAKE) -C $* clean-configure

.PHONY: clean-configure
clean-configure: $(packages_cleanconfig)

.PHONY: lib-spatialite
lib-spatialite: $(lib-spatialite)

.PHONY: lib-spatialindex
lib-spatialindex: $(lib-spatialindex)

.PHONY: gdal-wheel
gdal-wheel: $(lib-gdal)

.PHONY: pygit2-wheel
pygit2-wheel: $(lib-pygit2)

.PHONY: git
lib-git: $(lib-git)

.PHONY: build-Darwin
build-Darwin: VENDOR:=build/vendor-Darwin
build-Darwin: VENDOR_ENV:=$(VENDOR)/env
build-Darwin: $(libs)
	-$(RM) -r $(VENDOR) $(vendor-Darwin-archive)
	mkdir -p $(VENDOR)/wheelhouse $(VENDOR_ENV) dist
	for D in bin share lib libexec; do mkdir $(VENDOR_ENV)/$$D; done

	# GDAL/Proj
	cp -vf gdal/wheelhouse/$(notdir $(lib-gdal)) $(VENDOR)/wheelhouse/
	cp -vaf env/share/gdal $(VENDOR_ENV)/share/
	cp -vaf env/share/proj $(VENDOR_ENV)/share/

	# PyGit2
	cp -vf pygit2/wheelhouse/$(notdir $(lib-pygit2)) $(VENDOR)/wheelhouse/

	# Spatialite
	cp -vaf env/lib/$(notdir $(lib-spatialite)) $(VENDOR_ENV)/lib/
	install_name_tool -add_rpath "@loader_path/python3.7/site-packages/osgeo/.dylibs/" $(VENDOR_ENV)/lib/$(notdir $(lib-spatialite))

	cp -vfL env/lib/libspatialindex_c.dylib $(VENDOR_ENV)/lib/
	cp -vfL env/lib/libspatialindex.6.dylib $(VENDOR_ENV)/lib/

	cp -vaf env/share/git-core $(VENDOR_ENV)/share/
	cp -vaf env/libexec/git-core $(VENDOR_ENV)/libexec/
	cp -vaf env/bin/git $(VENDOR_ENV)/bin/

	tar czf $(vendor-Darwin-archive) -C $(VENDOR) env wheelhouse
	-$(RM) -r $(VENDOR)

.PHONY: build-Linux
build-Linux: VENDOR:=build/vendor-Linux
build-Linux:
	-$(RM) -r $(VENDOR) $(vendor-Linux-archive)
	mkdir -p $(VENDOR) dist
	docker run --rm \
		$(if $(MANYLINUX_DEBUG),-it) \
		-v $(abspath .):/src \
		-w /src \
		-v $(CCACHE_DIR):/root/.cache/ccache:cached \
		-e CCACHE_DIR=/root/.cache/ccache \
		-e CCACHE_COMPRESS \
		pyca/cryptography-manylinux2010:x86_64 \
		bash -c '/src/build-manylinux.sh /src/$(VENDOR) $(if $(MANYLINUX_DEBUG),; bash)'

	tar czf $(vendor-Linux-archive) -C $(VENDOR) env wheelhouse
	-$(RM) -r $(VENDOR)

$(vendor-Darwin-archive):
	./github-download.sh Darwin

$(vendor-Linux-archive):
	./github-download.sh Linux

.PHONY: download
download:
	./github-download.sh
