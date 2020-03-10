SPATIALINDEX_VERSION = 1.9.3

CMAKE ?= cmake

SHELL = /bin/bash
export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

SPATIALINDEX_ARCHIVE := spatialindex-src-$(SPATIALINDEX_VERSION).tar.gz

ifeq ($(PLATFORM),Darwin)
	CCACHE_PATH = /usr/local/opt/ccache/bin
	LIBSUFFIX = dylib
	CMAKE_PARAMS = -DCMAKE_SKIP_INSTALL_RPATH=TRUE -DCMAKE_INSTALL_NAME_DIR="@rpath" -DCMAKE_INSTALL_RPATH=@loader_path
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	CMAKE_PARAMS = -DCMAKE_INSTALL_NAME_DIR="$$ORIGIN"
endif

# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

build-spatialindex := build/bin/libspatialindex_c.$(LIBSUFFIX)
configure-spatialindex = build/CMakeCache.txt

# default target
.PHONY: all
all: $(build-spatialindex)

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) build/bin/libspatialindex*

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C build/ clean

.PHONY: cleanest
cleanest: cleaner
	-$(RM) -r build/ src/

.PHONY: clean-configure
clean-configure:
	-$(RM) $(configure-spatialindex)

#
# Download Archives
#

$(SPATIALINDEX_ARCHIVE):
	wget https://github.com/libspatialindex/libspatialindex/releases/download/$(SPATIALINDEX_VERSION)/$@ -O $@

.PHONY: archive
archive: $(SPATIALINDEX_ARCHIVE)

#
# Extract Archives
#

src: $(SPATIALINDEX_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(SPATIALINDEX_ARCHIVE) ..."
	tar xzf $(SPATIALINDEX_ARCHIVE) --strip-components=1 -C $@


.PHONY: source
source: src

$(configure-spatialindex): export PKG_CONFIG_PATH=/dev/null
$(configure-spatialindex): | src
	mkdir -p build
	cd build \
	&& $(CMAKE) -S $(realpath src) \
		$(CMAKE_PARAMS) \
		-DCMAKE_INSTALL_PREFIX=$(PREFIX)

.PHONY: configure
configure: clean-configure $(configure-spatialindex)

$(build-spatialindex): $(configure-spatialindex)
	$(MAKE) -C build -j 2
	$(MAKE) print-lib-deps-$(PLATFORM)

#
# INSTALLATION
#

.PHONY: install
install: $(build-spatialindex)
	$(MAKE) -C build install
ifeq ($(PLATFORM),Darwin)
	install_name_tool \
		 -change libspatialindex.6.dylib @loader_path/libspatialindex.6.dylib \
		 $(PREFIX)/lib/libspatialindex_c.$(LIBSUFFIX)
endif


.PHONY: print-lib-deps
print-lib-deps: print-lib-deps-$(PLATFORM)

.PHONY: print-lib-deps-Darwin
print-lib-deps-Darwin:
	@echo -e "\nExamining library dependencies..."
	cd build/ && find . \( -name "*.dylib" -o -name "*.so" \) -exec otool -L {} \;

.PHONY: print-lib-deps-Linux
print-lib-deps-Linux:
	@echo -e "\nExamining library dependencies..."
	cd build/  && find . -name "*.so" -print -exec ldd {} \;
