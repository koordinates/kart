export PREFIX := $(abspath env)
export PATH := $(abspath env/bin):$(PATH)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

export CFLAGS += -O2
export CXXFLAGS += -O2

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
	$(MAKE) vendor-$(PLATFORM)-archive

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
	cp -vafr env/share/gdal $(VENDOR_ENV)/share/
	cp -vafr env/share/proj $(VENDOR_ENV)/share/

	# PyGit2
	cp -vf pygit2/wheelhouse/$(notdir $(lib-pygit2)) $(VENDOR)/wheelhouse/

	# Spatialite
	cp -vfa env/lib/$(notdir $(lib-spatialite)) $(VENDOR_ENV)/lib/

	cp -vfL env/lib/libspatialindex_c.dylib $(VENDOR_ENV)/lib/
	cp -vfL env/lib/libspatialindex.6.dylib $(VENDOR_ENV)/lib/

	cp -frav env/share/git-core $(VENDOR_ENV)/share/
	cp -frav env/libexec/git-core $(VENDOR_ENV)/libexec/
	cp -frav env/bin/git $(VENDOR_ENV)/bin/

	tar czf $(vendor-Darwin-archive) -C $(VENDOR) env wheelhouse
	-$(RM) -r $(VENDOR)

.PHONY: build-Linux
build-Linux: VENDOR:=build/vendor-Linux
build-Linux:
	-$(RM) -r $(VENDOR) $(vendor-Linux-archive)
	mkdir -p $(VENDOR) dist
	docker run --rm -it \
		-v $(abspath .):/src \
		-w /src \
		-v $(abspath .)/.ccache:/root/.cache/ccache:cached \
		-e CCACHE_DIR=/root/.cache/ccache \
		pyca/cryptography-manylinux2010:x86_64 \
		bash -c '/src/build-manylinux.sh /src/$(VENDOR) $(if $(MANYLINUX_DEBUG),; bash)'

	tar czf $(vendor-Linux-archive) -C $(VENDOR) env wheelhouse
	-$(RM) -r $(VENDOR)

$(vendor-Linux-archive) $(vendor-Darwin-archive):
	mkdir -p dist
	wget https://s3-us-west-1.amazonaws.com/build-artifacts.sno.earth/vendor/latest/$(notdir $@) -O $@

.PHONY: download-Linux
download-Linux:
	-$(RM) $(vendor-Linux-archive)
	$(MAKE) $(vendor-Linux-archive)

.PHONY: download-Darwin
download-Darwin:
	-$(RM) $(vendor-Darwin-archive)
	$(MAKE) $(vendor-Darwin-archive)

.PHONY: download
download:
	$(MAKE) download-$(PLATFORM)

.PHONY: upload
upload: $(vendor-Linux-archive) $(vendor-Darwin-archive)
	aws s3 cp --acl public-read $(vendor-Linux-archive) s3://build-artifacts.sno.earth/vendor/latest/$(notdir $(vendor-Linux-archive))
	aws s3 cp --acl public-read $(vendor-Darwin-archive) s3://build-artifacts.sno.earth/vendor/latest/$(notdir $(vendor-Darwin-archive))
