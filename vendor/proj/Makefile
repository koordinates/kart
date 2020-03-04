PROJ_VERSION ?= 6.3.0
SHELL = /bin/bash

export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

PROJ_ARCHIVE := proj-$(PROJ_VERSION).tar.gz
PROJ_DATUMGRID_ARCHIVES = proj-datumgrid-1.8.zip #\
# 	proj-datumgrid-europe-1.5.zip \
# 	proj-datumgrid-north-america-1.3.zip \
# 	proj-datumgrid-oceania-1.1.zip \
# 	proj-datumgrid-world-1.0.zip

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

export PKG_CONFIG_PATH := $(realpath $(PREFIX)/lib/pkgconfig)

build-proj := src/src/.libs/libproj.$(LIBSUFFIX)
configure-proj = src/config.status

libsqlite := $(PREFIX)/lib/libsqlite3.$(LIBSUFFIX)
libs := $(libsqlite)

# default target
.PHONY: all
all: $(build-proj)

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) src/src/.libs/libproj.*
	-$(RM) src/src/libproj.la
	-$(RM) src/proj.pc

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C src/ clean

.PHONY: cleanest
cleanest: cleaner
	-$(RM) -r src/

.PHONY: clean-configure
clean-configure:
	-$(RM) $(configure-proj)

#
# Download Archives
#

$(PROJ_ARCHIVE):
	wget https://download.osgeo.org/proj/$@

$(PROJ_DATUMGRID_ARCHIVES):
	wget https://download.osgeo.org/proj/$@

#
# Extract Archives
#

src: $(PROJ_ARCHIVE) $(PROJ_DATUMGRID_ARCHIVES)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(PROJ_ARCHIVE) ..."
	tar xzf $(PROJ_ARCHIVE) --strip-components=1 -C $@
	mkdir -p $@/data
	$(foreach z,$(PROJ_DATUMGRID_ARCHIVES),unzip -q -o -d $@/data $(z);)

.PHONY: source
source: src

#
# PROJ
#

.PHONY: configure
configure: clean-configure $(configure-proj)

$(configure-proj): CFLAGS+=$(SQLITE_CFLAGS)
$(configure-proj): LDFLAGS+=$(SQLITE_LDFLAGS)
$(configure-proj): | src $(libs)
	cd src/ && ./configure --prefix=$(PREFIX)

$(build-proj): $(configure-proj)
	$(MAKE) -C src/ -j 2
	$(MAKE) print-lib-deps-$(PLATFORM)

.PHONY: install
install: $(build-proj)
	$(MAKE) -C src/ install

.PHONY: print-lib-deps
print-lib-deps: print-lib-deps-$(PLATFORM)

.PHONY: print-lib-deps-Darwin
print-lib-deps-Darwin:
	@echo -e "\nExamining library dependencies..."
	find src/ \( -name "*.dylib" -o -name "*.so" \) -exec otool -L {} \;

.PHONY: print-lib-deps-Linux
print-lib-deps-Linux:
	@echo -e "\nExamining library dependencies..."
	find src/ -name "*.so" -print -exec ldd {} \;
