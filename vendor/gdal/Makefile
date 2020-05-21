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
	PY_LDFLAGS := -Wl,-rpath,'@loader_path/../../../'
	WHEELID = macosx_10_9_x86_64
else ifeq ($(PLATFORM),Linux)
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	LIBSUFFIX = so
	PY_LDFLAGS := -Wl,-rpath='$$ORIGIN/lib',--enable-new-dtags
	WHEELID = manylinux2010_x86_64
endif
PY3 := $(or $(PY3),python3.7)
pywheel := wheel/GDAL-$(GDAL_VERSION)-cp37-cp37m-$(WHEELID).whl

# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

# libtool makes a mess of ccache
# https://github.com/OSGeo/gdal/blob/master/gdal/docker/ubuntu-full/Dockerfile#L195-L201
CCACHE_BIN := $(shell PATH=$(PATH) command -v ccache)

PKG_CONFIG_PATH := $(realpath $(PREFIX)/lib/pkgconfig)
PROJ_PREFIX := $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config proj --variable=prefix)

ifeq ($(PLATFORM),Darwin)
SQLITE_PREFIX = /usr/local/opt/sqlite
JSONC_PREFIX = /usr/local/opt/json-c
PG_PREFIX = /usr/local/opt/libpq
PKG_CONFIG_PATH := $(PKG_CONFIG_PATH):$(PG_PREFIX)/lib/pkgconfig
else
SQLITE_PREFIX := $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config sqlite3 --variable=prefix)
JSONC_PREFIX := $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config json-c --variable=prefix)
PG_PREFIX := $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config libpq --variable=prefix)
endif

export PKG_CONFIG_PATH

configure-gdal := src/config.status
build-gdal := src/.libs/libgdal.$(LIBSUFFIX)

libproj := $(PREFIX)/lib/libproj.$(LIBSUFFIX)
libgeos := $(PREFIX)/lib/libgeos.$(LIBSUFFIX)
libsqlite := $(PREFIX)/lib/libsqlite3.$(LIBSUFFIX)
libjsonc := $(PREFIX)/lib/libjson-c.$(LIBSUFFIX)
libs := $(libproj) $(libgeos) $(libsqlite) $(libjsonc)

# default target
.PHONY: all
all: wheel

.PHONY: clean
clean:
	-$(RM) -r env wheel
	-$(RM) -r src/.libs/libgdal*
	-$(RM) src/libgdal*
	-$(RM) -r src/swig/python/{build,dist}
	-$(RM) src/gdal.pc

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C src/ clean

.PHONY: cleanest
cleanest:
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

ifeq ($(PLATFORM),Linux)
$(configure-gdal): export PATH:=$(abspath ../geos/env/bin):$(PATH)
endif

$(configure-gdal): | src $(libs) $(ccache-deps)
	@test "$(PROJ_PREFIX)" || ( echo ">> PROJ_PREFIX did not resolve"; exit 1 )
	@test "$(SQLITE_PREFIX)" || ( echo ">> SQLITE_PREFIX did not resolve"; exit 1 )
	@test "$(JSONC_PREFIX)" || ( echo ">> JSONC_PREFIX did not resolve"; exit 1 )
	@test "$(PG_PREFIX)" || ( echo ">> PG_PREFIX did not resolve"; exit 1 )
	@test "`command -v geos-config`" && echo ">> geos-config: `command -v geos-config`" || ( echo ">> geos-config not found"; exit 1 )
	cd src && ./configure \
		--prefix=$(PREFIX) \
		--with-geotiff=internal --with-rename-internal-libgeotiff-symbols \
		--with-libtiff=internal --with-rename-internal-libtiff-symbols \
		--with-proj=$(PROJ_PREFIX) \
		--without-python \
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
		--without-png \
		--without-qhull \
		--without-sfcgal \
		--without-spatialite \
		--without-webp \
		--without-xerces \
		--without-xml2 \
		--without-zstd \
		--with-pg=yes \
		--enable-driver-pgdump \
		--with-sqlite3=$(SQLITE_PREFIX) \
		--with-libjson-c=$(JSONC_PREFIX)
# PGdump is required c/- https://github.com/OSGeo/gdal/issues/2197

.PHONY: configure
configure: clean-configure $(configure-gdal)

$(build-gdal): $(configure-gdal) | $(ccache-deps)
	$(MAKE) -C src -j 2
	$(MAKE) print-lib-deps-$(PLATFORM)
	$(MAKE) lib-fix-$(PLATFORM)

.PHONY: lib-fix-Darwin
lib-fix-Darwin:
	@echo fixing gdal libraries ...
	install_name_tool \
		-change $(SQLITE_PREFIX)/lib/libsqlite3.0.dylib @rpath/libsqlite3.0.dylib \
		-change /usr/local/opt/geos/lib/libgeos_c.1.dylib @rpath/libgeos_c.1.dylib \
		-change $(JSONC_PREFIX)/lib/libjson-c.4.dylib @rpath/libjson-c.4.dylib \
		-change $(PROJ_PREFIX)/lib/libproj.15.dylib @rpath/libproj.15.dylib \
		-change $(PG_PREFIX)/lib/libpq.5.dylib @rpath/libpq.5.dylib \
		-id libgdal.26.dylib \
		src/.libs/libgdal.26.dylib

	$(MAKE) print-lib-deps-$(PLATFORM)

.PHONY: lib-fix-Linux
lib-fix-Linux:

.PHONY: print-lib-deps
print-lib-deps: print-lib-deps-$(PLATFORM)

$(pywheel): $(build-gdal)
	-$(RM) -r wheel src/swig/python/build

	cd src/swig/python \
	&& LDFLAGS=$(PY_LDFLAGS) python3 setup.py --no-user-cfg build -v

	@echo fixing python bindings ...
	for SO in src/swig/python/build/lib.macosx*/osgeo/*.so; do \
		echo $$SO; \
		install_name_tool \
			-change "libgdal.26.dylib" "@rpath/libgdal.26.dylib" \
			"$$SO"; \
	done

	cd src/swig/python \
	&& python3 setup.py --no-user-cfg bdist_wheel --skip-build -d $(abspath wheel)

	delocate-listdeps $@

.PHONY: wheel
wheel: $(pywheel)

.PHONY: print-lib-deps-Darwin
print-lib-deps-Darwin:
	@echo -e "\nExamining library dependencies..."
	find src \( -name "*.dylib" -o -name "*.so" \) -exec otool -L {} \;

.PHONY: print-lib-deps-Linux
print-lib-deps-Linux:
	@echo -e "\nExamining library dependencies..."
	find src -name "*.so" -print -exec ldd {} \;

#
# INSTALLATION
#

.PHONY: install
install: $(build-gdal) $(pywheel) | $(ccache-deps)
# SWIG bindings don't seem to pick up the configured PREFIX unless it's in the env again
	$(MAKE) -C src install
