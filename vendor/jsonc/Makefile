JSONC_VERSION = 0.13.1

SHELL = /bin/bash
export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

JSONC_ARCHIVE := json-c-$(JSONC_VERSION)-nodoc.tar.gz

ifeq ($(PLATFORM),Darwin)
	CCACHE_PATH = /usr/local/opt/ccache/bin
	LIBSUFFIX = dylib
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)

# https://github.com/json-c/json-c/pull/499
# https://stackoverflow.com/a/45137452/2662
	export CFLAGS += -C
endif


# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

build-jsonc := src/.libs/libjson-c.4.$(LIBSUFFIX)
configure-jsonc = src/config.status

# default target
.PHONY: all
all: $(build-jsonc)

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) src/.libs/libjson-c.* src/libjson-c.la
	-$(RM) src/json-c*.pc

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C src/ clean

.PHONY: cleanest
cleanest: cleaner
	-$(RM) -r src/

.PHONY: clean-configure
clean-configure:
	-$(RM) $(configure-jsonc)

#
# Download Archives
#

$(JSONC_ARCHIVE):
	wget https://s3.amazonaws.com/json-c_releases/releases/$@ -O $@

.PHONY: archive
archive: $(JSONC_ARCHIVE)

#
# Extract Archives
#

src: $(JSONC_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(JSONC_ARCHIVE) ..."
	tar xzf $(JSONC_ARCHIVE) --strip-components=1 -C $@


.PHONY: source
source: src

$(configure-jsonc): export PKG_CONFIG_PATH=/dev/null
$(configure-jsonc): | src
	cd src/ && ./configure \
		--prefix=$(PREFIX)

.PHONY: configure
configure: clean-configure $(configure-jsonc)

$(build-jsonc): $(configure-jsonc)
	$(MAKE) -C src -j 2
	$(MAKE) print-lib-deps-$(PLATFORM)

#
# INSTALLATION
#

.PHONY: install
install: $(build-jsonc)
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
