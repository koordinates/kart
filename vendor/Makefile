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
	LIBSUFFIX = dylib
	WHEELTOOL = delocate
	PY3 ?= $(realpath /Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7)
	WHEELID = macosx_10_9_x86_64
	export LDFLAGS += -Wl,-rpath,'@loader_path'
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	WHEELTOOL = auditwheel
	WHEELID = linux_x86_64
	export LDFLAGS += -Wl,-rpath='$$ORIGIN',--enable-new-dtags
endif
PY3 := $(or $(PY3),python3.7)

PYDEPS := pip wheel $(WHEELTOOL) cmake==3.*

packages = proj jsonc geos gdal spatialite libgit2 pygit2
ifeq ($(PLATFORM),Linux)
packages += sqlite spatialindex git curl libpq
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
lib-git2 := env/lib/libgit2.$(LIBSUFFIX)
lib-pygit2 := env/lib/python3.7/site-packages/pygit2/__init__.py
build-libs := $(lib-gdal) $(lib-spatialite) $(lib-pygit2) $(lib-git) $(lib-git2)


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
$(lib-proj): $(lib-sqlite) proj_build
$(lib-git2): libgit2_build
$(lib-spatialite): $(lib-sqlite) $(lib-geos) $(lib-proj) spatialite_build

$(lib-gdal): $(lib-sqlite) $(lib-geos) $(lib-proj) $(lib-jsonc) $(lib-pq)
	$(MAKE) -C gdal install wheel
	pip3 install --force-reinstall gdal/wheel/GDAL-*$(WHEELID).whl

$(lib-pygit2): $(lib-git2)
	$(MAKE) -C pygit2 wheel
	pip3 install --force-reinstall pygit2/wheel/pygit2-*$(WHEELID).whl

ifeq ($(PLATFORM),Darwin)
#
# MacOS
#
brew-libs = $(lib-sqlite) $(lib-curl) $(lib-git) $(lib-spatialindex) $(lib-geos) $(lib-jsonc)

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
	$(MAKE) .delocate
	cp -av /usr/local/opt/sqlite3/lib/libsqlite3*.dylib .delocate
	$(MAKE) delocate-deps

$(lib-pq): $(brew-deps) | env
	$(MAKE) .delocate
	cp -av /usr/local/opt/libpq/lib/libpq*.dylib .delocate
	$(MAKE) delocate-deps

$(lib-git): $(brew-deps) | env
	mkdir -p env/share/git-core
	cp -av /usr/local/opt/git/share/git-core/templates env/share/git-core/

	$(MAKE) .delocate
	mkdir -p env/libexec/git-core/
	for F in `find /usr/local/opt/git/libexec/git-core -mindepth 1 -maxdepth 1`; do \
		if [[ "`file -h $$F`" =~ "Mach-O" ]]; then \
			cp -av "$$F" .delocate/; \
		else \
			cp -av "$$F" env/libexec/git-core/; \
		fi; \
	done
	DELOCATE_OUT=env/libexec/git-core $(MAKE) delocate-deps
	# move the .dylibs to env/lib/
	for F in `find env/libexec/git-core/ -type f -name "*.dylib"`; do \
		N=`basename "$$F"`; \
		mv -fv "$$F" env/lib/; \
		ln -sv "../../lib/$$N" "$$F"; \
	done

	$(MAKE) .delocate
	mkdir -p .delocate/bin .delocate/lib
	cp -av /usr/local/opt/git/bin/* .delocate/bin/
	./mac-delocate-deps.sh .delocate/bin ../lib
	mv -fv .delocate/lib/* env/lib/
	mv -fv .delocate/bin/* env/bin/

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

build-libs += $(lib-spatialindex) $(lib-curl)

$(lib-spatialindex): spatialindex_build
$(lib-sqlite): sqlite_build
$(lib-git): $(lib-curl) git_build
$(lib-curl): curl_build
$(lib-geos): geos_build
$(lib-jsonc): jsonc_build
$(lib-pq): libpq_build
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

.PHONY: lib-git
lib-git: $(lib-git)

.PHONY: lib-pq
lib-pq: $(lib-pq)

.PHONY: lib-sqlite
lib-sqlite: $(lib-sqlite)

.PHONY: lib-curl
lib-curl: $(lib-curl)

.PHONY: lib-jsonc
lib-jsonc: $(lib-jsonc)

.PHONY: lib-geos
lib-geos: $(lib-geos)

.PHONY: lib-proj
lib-proj: $(lib-proj)

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

	# PyGit2
	cp -vf pygit2/wheel/pygit2-*$(WHEELID).whl $(VENDOR)/wheelhouse/

	cp -vaf env/lib/*.{dylib,so} $(VENDOR_ENV)/lib/

	mkdir -p $(VENDOR_ENV)/share/git-core/
	cp -vaf env/share/git-core/templates $(VENDOR_ENV)/share/git-core/
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
	find env/lib/  && find . -name "*.so" -print -exec ldd {} \;
