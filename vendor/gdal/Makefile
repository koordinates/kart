GDAL_VERSION ?= 3.0.3
SHELL = /bin/bash

export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

GDAL_ARCHIVE := gdal-${GDAL_VERSION}.tar.gz

ifeq ($(PLATFORM),Darwin)
	CCACHE_PATH = /usr/local/opt/ccache/bin
	LIBSUFFIX = dylib
	PY3 ?= $(realpath /Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7)
else ifeq ($(PLATFORM),Linux)
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	LIBSUFFIX = so
endif
PY3 := $(or $(PY3),python3.7)

# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

# libtool makes a mess of ccache
# https://github.com/OSGeo/gdal/blob/master/gdal/docker/ubuntu-full/Dockerfile#L195-L201
CCACHE_BIN := $(shell PATH=$(PATH) command -v ccache)

export PKG_CONFIG_PATH := $(realpath $(PREFIX)/lib/pkgconfig)
PROJ_PREFIX = $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config proj --variable=prefix)
SQLITE_PREFIX = $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config sqlite3 --variable=prefix)
JSONC_PREFIX = $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config json-c --variable=prefix)

CFLAGS += -g
CXXFLAGS += -g

configure-gdal := src/config.status
build-gdal := src/.libs/libgdal.$(LIBSUFFIX)

libproj := $(PREFIX)/lib/libproj.$(LIBSUFFIX)
libgeos := $(PREFIX)/lib/libgeos.$(LIBSUFFIX)
libsqlite := $(PREFIX)/lib/libsqlite3.$(LIBSUFFIX)
libjsonc := $(PREFIX)/lib/libjson-c.$(LIBSUFFIX)
libs := $(libproj) $(libgeos) $(libsqlite) $(libjsonc)

# default target
.PHONY: all
all: $(build-gdal)

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) -r wheels wheelhouse
	-$(RM) -r src/.libs/libgdal*
	-$(RM) src/libgdal*
	-$(RM) -r src/swig/python/{build,dist}
	-$(RM) src/gdal.pc

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C src/ clean

.PHONY: cleanest
cleanest: cleaner
	-$(RM) -r src/
	-$(RM) ccache_cc.sh ccache_c++.sh

.PHONY: clean-configure
clean-configure:
	-$(RM) $(configure-gdal)

#
# Download Archives
#

$(GDAL_ARCHIVE):
	wget https://github.com/OSGeo/gdal/releases/download/v$(GDAL_VERSION)/$@ -O $@

.PHONY: archive
archive: $(GDAL_ARCHIVE)

#
# Extract Archives
#

src: $(GDAL_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(GDAL_ARCHIVE) ..."
	tar xzf $(GDAL_ARCHIVE) --strip-components=1 -C $@

.PHONY: source
source: src

ifneq ($(CCACHE_BIN),)
# https://github.com/OSGeo/gdal/blob/master/gdal/docker/ubuntu-full/Dockerfile#L195-L201
# GDAL's "Little trick to avoid issues with Python bindings"

ccache_cc.sh:
	printf "#!/bin/sh\nccache cc \$$*" > $@
	chmod +x $@

ccache_c++.sh:
	printf "#!/bin/sh\nccache c++ \$$*" > $@
	chmod +x $@

export CC := $(abspath ccache_cc.sh)
export CXX := $(abspath ccache_c++.sh)

ccache-deps=ccache_cc.sh ccache_c++.sh
endif

#
# GDAL
#

$(configure-gdal): export PATH:=$(abspath ../geos/env/bin):$(PATH)
$(configure-gdal): | src $(libs) $(ccache-deps)
	@test "$(PROJ_PREFIX)" || ( echo ">> PROJ_PREFIX did not resolve"; exit 1 )
	@test "$(SQLITE_PREFIX)" || ( echo ">> SQLITE_PREFIX did not resolve"; exit 1 )
	@test "$(JSONC_PREFIX)" || ( echo ">> JSONC_PREFIX did not resolve"; exit 1 )
	@test "`command -v geos-config`" && echo ">> geos-config: `command -v geos-config`" || ( echo ">> geos-config not found"; exit 1 )
	@test "`command -v $(PY3)`" && echo ">> PY3: `command -v $(PY3)`" || ( echo ">> PY3 did not resolve"; exit 1 )
	cd src && ./configure \
		--prefix=$(PREFIX) \
		--with-geotiff=internal --with-rename-internal-libgeotiff-symbols \
		--with-libtiff=internal --with-rename-internal-libtiff-symbols \
		--with-proj=$(PROJ_PREFIX) \
		--with-python=$(realpath $(PY3)) \
		--disable-all-optional-drivers \
		--enable-driver-gpkg \
		--enable-driver-shape \
		--without-cfitsio \
		--without-curl \
		--without-freexl \
		--without-gif \
		--without-gnm \
		--without-hdf4 \
		--without-hdf5 \
		--without-jasper \
		--without-jpeg \
		--without-jpeg12 \
		--without-lerc \
		--without-netcdf \
		--without-odbc \
		--without-openjpeg \
		--without-pcidsk \
		--without-pcraster \
		--without-pcre \
		--without-pg \
		--without-png \
		--without-qhull \
		--without-sfcgal \
		--without-spatialite \
		--without-webp \
		--without-xerces \
		--without-xml2 \
		--without-zstd \
		--with-sqlite3=$(SQLITE_PREFIX) \
		--with-libjson-c=$(JSONC_PREFIX)

.PHONY: configure
configure: clean-configure $(configure-gdal)

$(build-gdal): $(configure-gdal) | $(ccache-deps)
	$(MAKE) -C src -j 2
	$(MAKE) print-lib-deps-$(PLATFORM)

.PHONY: wheels
wheels: $(build-gdal)
	-$(RM) -r $@
	cd src/swig/python \
	&& python3 setup.py bdist_wheel -d $(abspath $@)
	$(MAKE) wheelfix-$(PLATFORM)

.PHONY: wheelfix-Darwin
wheelfix-Darwin: export DYLD_LIBRARY_PATH:=$(PREFIX)/lib:$(abspath src/.libs):$(DYLD_LIBRARY_PATH)
wheelfix-Darwin:
	@echo Examining Wheel dependencies...
	delocate-listdeps wheels/*macosx*.whl

	@echo Resolving wheel dependencies...
	for WHL in wheels/*macosx*.whl; do delocate-wheel -v -w wheelhouse/ $$WHL; done

	@echo Re-examining Wheel dependencies...
	delocate-listdeps wheelhouse/*macosx*.whl

.PHONY: wheelfix-Linux
wheelfix-Linux: export LD_LIBRARY_PATH:=$(PREFIX)/lib:$(abspath src/.libs):$(LD_LIBRARY_PATH)
wheelfix-Linux:
	@echo Examining Wheel dependencies...
	auditwheel show wheels/*linux*.whl

	@echo Resolving wheel dependencies...
	ls $(PREFIX)/lib/*.$(LIBSUFFIX)
	for WHL in wheels/*linux*.whl; do auditwheel repair -w wheelhouse/ $$WHL; done
	-$(RM) wheelhouse/*-linux_*.whl

	@echo Re-examining Wheel dependencies...
	auditwheel show wheelhouse/*manylinux*.whl

#
# INSTALLATION
#

.PHONY: install
install: $(build-gdal) | $(ccache-deps)
# SWIG bindings don't seem to pick up the configured PREFIX unless it's in the env again
	$(MAKE) -C src install
	$(MAKE) wheels

.PHONY: print-lib-deps
print-lib-deps: print-lib-deps-$(PLATFORM)

.PHONY: print-lib-deps-Darwin
print-lib-deps-Darwin:
	@echo -e "\nExamining library dependencies..."
	find src \( -name "*.dylib" -o -name "*.so" \) -exec otool -L {} \;

.PHONY: print-lib-deps-Linux
print-lib-deps-Linux:
	@echo -e "\nExamining library dependencies..."
	find src -name "*.so" -print -exec ldd {} \;
