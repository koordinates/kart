include version.mk

SHELL = /bin/bash
export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

SQLITE_ARCHIVE := sqlite-autoconf-$(SQLITE_VERSIONID).tar.gz

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

build-sqlite := src/.libs/libsqlite3.0.$(LIBSUFFIX)
configure-sqlite = src/config.status

# default target
.PHONY: all
all: $(build-sqlite)

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) -r src/.libs/libsqlite3* src/libsqlite3.la src/sqlite3
	-$(RM) src/sqlite3.pc

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C src/ clean

.PHONY: cleanest
cleanest:
	-$(RM) -r src/

.PHONY: clean-configure
clean-configure:
	-$(RM) $(configure-sqlite)

#
# Download Archives
#

$(SQLITE_ARCHIVE):
	wget https://www.sqlite.org/$(SQLITE_YEAR)/$@ -O $@

.PHONY: archive
archive: $(SQLITE_ARCHIVE)

#
# Extract Archives
#

src: $(SQLITE_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(SQLITE_ARCHIVE) ..."
	tar xzf $(SQLITE_ARCHIVE) --strip-components=1 -C $@


.PHONY: source
source: src

$(configure-sqlite): export PKG_CONFIG_PATH=/dev/null
$(configure-sqlite): | src
	cd src/ && ./configure \
		--prefix=$(PREFIX)

.PHONY: configure
configure: clean-configure $(configure-sqlite)

$(build-sqlite): $(configure-sqlite)
	$(MAKE) -C src -j 2
	$(MAKE) print-lib-deps-$(PLATFORM)

#
# INSTALLATION
#

.PHONY: install
install: $(build-sqlite)
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
