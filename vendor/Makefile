# The contents of kart/vendor/ are hashed used as a cache-key for
# built libraries and wheels in vendor-Windows, vendor-Darwin, vendor-Linux.
# Making any change to this file will cause a cache-miss and a fresh build.

# Number of times this line has been changed to force a fresh build: 2

SHELL = /bin/bash

export PREFIX := $(abspath env)
export PATH := $(abspath env/bin):$(PATH)

ifeq ($(OS),Windows_NT)
	$(error "On Windows, run `nmake /f makefile.vc` instead.")
else
	PLATFORM := $(shell uname -s)
endif

export CFLAGS += -O2 -g
export CXXFLAGS += -O2 -g
CCACHE_DIR ?= $(abspath .)/.ccache

vendor-Linux-archive = dist/vendor-Linux.tar.gz
vendor-Darwin-archive = dist/vendor-Darwin.tar.gz

ifeq ($(PLATFORM),Darwin)
	LIBINFIX = cpython-37m-darwin
	LIBSUFFIX = dylib
	WHEELTOOL = delocate==0.8.2
	PY3 ?= $(realpath /Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7)
	WHEELID = macosx_10_9_x86_64
	export LDFLAGS += -Wl,-rpath,'@loader_path'
else ifeq ($(PLATFORM),Linux)
	LIBINFIX = cpython-37m-x86_64-linux-gnu
	LIBSUFFIX = so
	WHEELTOOL = auditwheel
	WHEELID = linux_x86_64
	export LDFLAGS = -Wl,-rpath='$$ORIGIN'
endif
PY3 := $(or $(PY3),python3.7)

PYDEPS := pip wheel $(WHEELTOOL) cmake==3.* certifi
# if we export SSL_CERT_FILE it breaks wget under macos ¯\_(ツ)_/¯
export CERTIFI_SSL_CERT_FILE=$(abspath $(PREFIX)/lib/python3.7/site-packages/certifi/cacert.pem)

packages = proj jsonc geos gdal spatialite libgit2 pygit2 psycopg2 pysqlite3
ifeq ($(PLATFORM),Linux)
packages += sqlite spatialindex git curl libpq pyodbc
endif

brew-deps := Brewfile.lock.json


lib-geos := env/lib/libgeos.$(LIBSUFFIX)
lib-proj := env/lib/libproj.$(LIBSUFFIX)
lib-jsonc := env/lib/libjson-c.$(LIBSUFFIX)
lib-spatialindex := env/lib/libspatialindex_c.$(LIBSUFFIX)
lib-pq := env/lib/libpq.$(LIBSUFFIX)
lib-sqlite := env/lib/libsqlite3.$(LIBSUFFIX)
lib-git := env/bin/git
lib-curl := env/lib/libcurl.$(LIBSUFFIX)

lib-spatialite := env/lib/mod_spatialite.$(LIBSUFFIX)
lib-gdal := env/lib/python3.7/site-packages/osgeo/__init__.py
lib-libgit2 := env/lib/libgit2.$(LIBSUFFIX)
lib-pygit2 := env/lib/python3.7/site-packages/pygit2/__init__.py
lib-pysqlite3 := env/lib/python3.7/site-packages/pysqlite3/__init__.py
lib-psycopg2 := env/lib/python3.7/site-packages/psycopg2/__init__.py
lib-pyodbc := env/lib/python3.7/site-packages/pyodbc.$(LIBINFIX).so
lib-s2_py := env/lib/python3.7/site-packages/s2_py/__init__.py

build-libs := $(lib-gdal) $(lib-spatialite) $(lib-pygit2) $(lib-git) $(lib-libgit2) $(lib-psycopg2) $(lib-pysqlite3) $(lib-s2_py)


# default target
.PHONY: all
all:
	$(MAKE) $(vendor-$(PLATFORM)-archive)

.PHONY: build
build: $(build-libs)

env:
	@echo PY3=$(PY3)
	$(PY3) -m venv --copies env
	env/bin/pip install -U $(PYDEPS)

# dependencies
$(lib-git): $(lib-curl) $(lib-sqlite) $(lib-s2_py) git_build
$(lib-proj): $(lib-sqlite) proj_build
$(lib-libgit2): libgit2_build
$(lib-spatialite): $(lib-sqlite) $(lib-geos) $(lib-proj) spatialite_build

$(lib-gdal): $(lib-sqlite) $(lib-geos) $(lib-proj) $(lib-jsonc) $(lib-pq)
	$(MAKE) -C gdal install wheel
	pip3 install --force-reinstall gdal/wheel/GDAL-*$(WHEELID).whl

$(lib-pygit2): $(lib-libgit2)
	$(MAKE) -C pygit2 wheel
	pip3 install --force-reinstall pygit2/wheel/pygit2-*$(WHEELID).whl

$(lib-psycopg2): $(lib-pq)
	$(MAKE) -C psycopg2 wheel
	pip3 install --force-reinstall psycopg2/wheel/psycopg2-*$(WHEELID).whl

$(lib-pysqlite3): $(lib-sqlite)
	$(MAKE) -C pysqlite3 wheel
	pip3 install --force-reinstall pysqlite3/wheel/pysqlite3-*$(WHEELID).whl

$(lib-pyodbc):
	$(MAKE) -C pyodbc wheel
	pip3 install --force-reinstall pyodbc/wheel/pyodbc-*$(WHEELID).whl

$(lib-s2_py):
	$(MAKE) -C s2_py wheel
	pip3 install --force-reinstall s2_py/wheel/s2_py-*$(WHEELID).whl


ifeq ($(PLATFORM),Darwin)
#
# MacOS
#
brew-libs = $(lib-sqlite) $(lib-curl) $(lib-spatialindex) $(lib-geos) $(lib-jsonc)

$(brew-deps): Brewfile
	brew bundle

.PHONY: brew-deps
brew-deps: $(brew-deps)

.PHONE: brew-libs
brew-libs: $(brew-libs)

env: brew-deps

.PHONY: .delocate
.delocate:
	rm -rf $@
	mkdir $@

delocate-deps: DELOCATE_OUT ?= env/lib
delocate-deps: | env
	./mac-delocate-deps.sh .delocate
	mv -fv .delocate/* $(DELOCATE_OUT)
	rm -rf .delocate/

$(lib-sqlite): $(brew-deps) | env
	$(MAKE) -C sqlite src  # needed for pysqlite3
	cp /usr/local/opt/sqlite3/include/*.h $(PREFIX)/include/
	$(MAKE) .delocate
	cp -av /usr/local/opt/sqlite3/lib/libsqlite3*.dylib .delocate
	$(MAKE) delocate-deps

$(lib-pq): $(brew-deps) | env
	$(MAKE) .delocate
	cp -av /usr/local/opt/libpq/lib/libpq*.dylib .delocate
	$(MAKE) delocate-deps

$(lib-curl): $(brew-deps) | env
	$(MAKE) .delocate
	cp -av /usr/local/opt/curl/lib/libcurl*.dylib .delocate
	$(MAKE) delocate-deps

$(lib-jsonc): $(brew-deps) | env
	$(MAKE) .delocate
	cp -av /usr/local/opt/json-c/lib/libjson-c*.dylib .delocate
	$(MAKE) delocate-deps

$(lib-spatialindex): $(brew-deps) | env
	$(MAKE) .delocate
	cp -av /usr/local/opt/spatialindex/lib/libspatialindex_c*.dylib .delocate
	$(MAKE) delocate-deps

$(lib-geos): $(brew-deps) | env
	$(MAKE) .delocate
	cp -av /usr/local/opt/geos/lib/libgeos*.dylib .delocate
	$(MAKE) delocate-deps

else
#
# Linux
#

build-libs += $(lib-spatialindex) $(lib-curl) $(lib-pyodbc)

$(lib-spatialindex): spatialindex_build
$(lib-sqlite): sqlite_build
$(lib-curl): curl_build
$(lib-geos): geos_build
$(lib-jsonc): jsonc_build
$(lib-pq): libpq_build
$(lib-pyodbc): $(shell ls /usr/{,local/}include/sql.h | tail -n 1)
endif


# .PHONY: $(packages)
packages_build := $(addsuffix _build,$(packages))

%_build: % | env
	@echo PATH=$$PATH PREFIX=$$PREFIX
	$(MAKE) -C $* install

packages_clean := $(addsuffix _clean,$(packages))

%_clean:
	$(MAKE) -C $* clean

packages_archive := $(addsuffix _archive,$(packages))

%_archive:
	$(MAKE) -C $* archive

.PHONY: sources
sources: $(packages_archive)

.PHONY: vendorclean
vendorclean:
	-$(RM) -r env/ wheelhouse/ dist/ .delocate/ Brewfile.lock.json

.PHONY: clean
clean: vendorclean $(packages_clean)

packages_cleaner := $(addsuffix _cleaner,$(packages))
%_cleaner:
	$(MAKE) -C $* cleaner

.PHONY: cleaner
cleaner: vendorclean $(packages_cleaner)

packages_cleanest := $(addsuffix _cleanest,$(packages))
%_cleanest:
	$(MAKE) -C $* cleanest

.PHONY: cleanest
cleanest: vendorclean $(packages_cleanest)
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

.PHONY: lib-gdal
lib-gdal: $(lib-gdal)

.PHONY: lib-pygit2
lib-pygit2: $(lib-pygit2)

.PHONY: lib-libgit2
lib-libgit2: $(lib-libgit2)

.PHONY: lib-git
lib-git: $(lib-git)

.PHONY: lib-pq
lib-pq: $(lib-pq)

.PHONY: lib-sqlite
lib-sqlite: $(lib-sqlite)

.PHONY: lib-pysqlite3
lib-pysqlite3: $(lib-pysqlite3)

.PHONY: lib-curl
lib-curl: $(lib-curl)

.PHONY: lib-jsonc
lib-jsonc: $(lib-jsonc)

.PHONY: lib-geos
lib-geos: $(lib-geos)

.PHONY: lib-proj
lib-proj: $(lib-proj)

.PHONY: lib-psycopg2
lib-psycopg2: $(lib-psycopg2)

.PHONY: lib-pyodbc
lib-pyodbc: $(lib-pyodbc)

.PHONY: lib-s2_py
lib-s2_py: $(lib-s2_py)

.PHONY: build-Darwin
build-Darwin: VENDOR:=build/vendor-Darwin
build-Darwin: VENDOR_ENV:=$(VENDOR)/env
build-Darwin: $(build-libs) $(brew-libs)
	-$(RM) -r $(VENDOR) $(vendor-Darwin-archive)
	mkdir -p $(VENDOR)/wheelhouse $(VENDOR_ENV) dist
	for D in bin share lib libexec; do mkdir $(VENDOR_ENV)/$$D; done

# GDAL/Proj
	cp -vf gdal/wheel/GDAL-*$(WHEELID).whl $(VENDOR)/wheelhouse/
	cp -vaf env/share/gdal $(VENDOR_ENV)/share/
	cp -vaf env/share/proj $(VENDOR_ENV)/share/

	cp -vf pygit2/wheel/pygit2-*$(WHEELID).whl $(VENDOR)/wheelhouse/
	cp -vf psycopg2/wheel/psycopg2-*$(WHEELID).whl $(VENDOR)/wheelhouse/
	cp -vf pysqlite3/wheel/pysqlite3-*$(WHEELID).whl $(VENDOR)/wheelhouse/
	cp -vf s2_py/wheel/s2_py-*$(WHEELID).whl $(VENDOR)/wheelhouse/

	cp -vaf env/lib/*.{dylib,so} $(VENDOR_ENV)/lib/

# Homebrew seems to sometimes have signed code. But signatures may have been
# invalidated by out rpath/etc futzing. Remove them all
	find $(VENDOR_ENV) -type f -exec codesign --remove-signature {} + ;

	mkdir -p $(VENDOR_ENV)/share/git-core/
	cp -vaf env/share/git-core/templates $(VENDOR_ENV)/share/git-core/
	cp -vaf env/libexec/git-core $(VENDOR_ENV)/libexec/
	cp -vaf env/bin/git $(VENDOR_ENV)/bin/

	tar czf $(vendor-Darwin-archive) -C $(VENDOR) env wheelhouse
	-$(RM) -r $(VENDOR)

	ccache --show-stats

# set LINUX_DEBUG=1,2,3 for different debugging options for build-Linux
LINUX_DEBUG_CMD_1 = || bash; exit 1  # drop to shell on errors
LINUX_DEBUG_CMD_2 = ; bash  # drop to shell after
LINUX_DEBUG_CMD_3 = bash  # launch a shell instead of the build

.PHONY: build-Linux
build-Linux: VENDOR:=build/vendor-Linux
build-Linux:
	-$(RM) -r $(VENDOR) $(vendor-Linux-archive)
	mkdir -p $(VENDOR) dist
	time docker run \
		--name kart-vendor-build-linux \
		--rm \
		`[[ -t 0 ]] && echo '-it'` \
		-v $(abspath .):/src \
		-w /src \
		-v sno-vendor-linux-ccache:/root/.cache/ccache \
		-v /build \
		-e CCACHE_DIR=/root/.cache/ccache \
		-e CCACHE_COMPRESS \
		koordinates2/manylinux2014-pyshared:x86_64 \
		bash -c '/src/build-manylinux.sh /src/$(VENDOR) ${LINUX_DEBUG_CMD_$(LINUX_DEBUG)}'

	tar czf $(vendor-Linux-archive) -C $(VENDOR) env wheelhouse
	-$(RM) -r $(VENDOR)

$(vendor-Darwin-archive):
	./github-download.sh Darwin

$(vendor-Linux-archive):
	./github-download.sh Linux

.PHONY: download
download:
	./github-download.sh

.PHONY: print-lib-deps
print-lib-deps: print-lib-deps-$(PLATFORM)

.PHONY: print-lib-deps-Darwin
print-lib-deps-Darwin:
	@echo -e "\nExamining library dependencies..."
	for SO in `find env/lib/ -type f \( -name "*.dylib" -o -name "*.so" \)`; do \
		otool -L "$$SO"; \
		otool -l "$$SO" | grep -A2 RPATH | grep -v cmdsize || true; \
	done

.PHONY: print-lib-deps-Linux
print-lib-deps-Linux:
	@echo -e "\nExamining library dependencies..."
	find env/lib/ -type f -name "*.so" -print -exec ldd {} \;

.PHONY: check-lib-deps
check-lib-deps: check-lib-deps-$(PLATFORM)

.PHONY: check-lib-deps-Darwin
check-lib-deps-Darwin:
	@echo -e "\nLooking for missing library dependencies..."
	for SO in `find env/lib/ -type f \( -name "*.dylib" -o -name "*.so" \)`; do \
		otool -l "$$SO" | grep /usr/local/ && exit 1; \
	done

.PHONY: check-lib-deps-Linux
check-lib-deps-Linux:
	@echo -e "\nLooking for missing library dependencies..."
	find env/lib/ -type f -name "*.so" | xargs -t -I % sh -c '! ( ldd % | grep -P " => /usr/(local|lib)/(?!x86_64-linux-gnu)" )'
