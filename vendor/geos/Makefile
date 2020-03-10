GEOS_VERSION = 3.8.0

SHELL = /bin/bash
export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

GEOS_ARCHIVE := geos-$(GEOS_VERSION).tar.bz2

ifeq ($(PLATFORM),Darwin)
	CCACHE_PATH = /usr/local/opt/ccache/bin
	LIBSUFFIX = dylib
else ifeq ($(PLATFORM),Linux)
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	LIBSUFFIX = so
endif

# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

build-geos := src/capi/.libs/libgeos_c.$(LIBSUFFIX)
configure-geos = src/config.status

# default target
.PHONY: all
all: $(build-geos)

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) src/src/.libs/libgeos.* src/src/libgeos.la
	-$(RM) src/capi/.libs/libgeos_c.* src/capi/libgeos_c.la

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C src/ clean

.PHONY: cleanest
cleanest: cleaner
	-$(RM) -r src/

.PHONY: clean-configure
clean-configure:
	-$(RM) $(configure-geos)

#
# Download Archives
#

$(GEOS_ARCHIVE):
	wget https://download.osgeo.org/geos/$@ -O $@

.PHONY: archive
archive: $(GEOS_ARCHIVE)

#
# Extract Archives
#

src: $(GEOS_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(GEOS_ARCHIVE) ..."
	tar xjf $(GEOS_ARCHIVE) --strip-components=1 -C $@


.PHONY: source
source: src

$(configure-geos): export PKG_CONFIG_PATH=/dev/null
$(configure-geos): | src
	cd src/ && ./configure \
		--prefix=$(PREFIX)

.PHONY: configure
configure: clean-configure $(configure-geos)

$(build-geos): $(configure-geos)
	$(MAKE) -C src -j 2
	$(MAKE) print-lib-deps-$(PLATFORM)

#
# INSTALLATION
#

.PHONY: install
install: $(build-geos)
	$(MAKE) -C src install

.PHONY: print-lib-deps
print-lib-deps: print-lib-deps-$(PLATFORM)

.PHONY: print-lib-deps-Darwin
print-lib-deps-Darwin:
	@echo -e "\nExamining library dependencies..."
	cd src/ && find . \( -name "*.dylib" -o -name "*.so" \) -exec otool -L {} \;

.PHONY: print-lib-deps-Linux
print-lib-deps-Linux:
	@echo -e "\nExamining library dependencies..."
	cd src/  && find . -name "*.so" -print -exec ldd {} \;
