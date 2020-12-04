PG_VERSION = 10.12
MACOS_SDK = MacOSX10.10.sdk

SHELL = /bin/bash
export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

PG_ARCHIVE := postgresql-$(PG_VERSION).tar.gz

ifeq ($(PLATFORM),Darwin)
	CCACHE_PATH = /usr/local/opt/ccache/bin
	LIBSUFFIX = dylib
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
endif

# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

build-pq := src/src/interfaces/libpq/libpq.$(LIBSUFFIX)
configure-pq = src/config.status

# default target
.PHONY: all
all: $(build-pq)

.PHONY: clean
clean:
	-$(RM) -r env
# 	-$(RM) src/.libs/libcurl.* src/libcurl.la
# 	-$(RM) src/curl*.pc

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C src/ distclean

.PHONY: cleanest
cleanest:
	-$(RM) -r src/ $(MACOS_SDK)

.PHONY: clean-configure
clean-configure:
	-$(RM) $(configure-pq)

#
# Download Archives
#

$(MACOS_SDK).tar.xz:
	wget https://github.com/phracker/MacOSX-SDKs/releases/download/10.15/$@ -O $@

$(PG_ARCHIVE):
	wget https://ftp.postgresql.org/pub/source/v$(PG_VERSION)/$@ -O $@

.PHONY: archive
archive: $(PG_ARCHIVE)

#
# Extract Archives
#

src: $(PG_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(PG_ARCHIVE) ..."
	tar xzf $(PG_ARCHIVE) --strip-components=1 -C $@


.PHONY: source
source: src

$(MACOS_SDK): | $(MACOS_SDK).tar.xz
	rm -rf $@
	tar xJf $(MACOS_SDK).tar.xz


# on MacOS we need to download the system headers for OpenSSL
# currently we're building for MacOS10.9+ so we need that SDK
# get from https://github.com/phracker/MacOSX-SDKs
ifeq ($(PLATFORM),Darwin)

export PG_SYSROOT := $(abspath $(MACOS_SDK))
#export CFLAGS += -I$(abspath $(MACOS_SDK))/usr/include
#export LDFLAGS += -L$(abspath $(MACOS_SDK))/usr/lib

source: $(MACOS_SDK)

$(configure-pq): | $(MACOS_SDK)

endif

$(configure-pq): export PKG_CONFIG_PATH=/dev/null
$(configure-pq): | src
	cd src/ && ./configure \
		--prefix=$(PREFIX) \
		--disable-debug \
		--with-openssl \
		--without-readline

.PHONY: configure
configure: clean-configure $(configure-pq)

$(build-pq): $(configure-pq)
	$(MAKE) -C src/src/interfaces/libpq
	$(MAKE) -C src/src/backend generated-headers
	$(MAKE) print-lib-deps-$(PLATFORM)
	$(MAKE) check-lib-deps-$(PLATFORM)

#
# INSTALLATION
#

.PHONY: install
install: $(build-pq)
	$(MAKE) -C src/src/interfaces/libpq install
	$(MAKE) -C src/src/bin/pg_config install
	$(MAKE) -C src/src/include install

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

.PHONY: check-lib-deps
check-lib-deps: check-lib-deps-$(PLATFORM)

.PHONY: check-lib-deps-Darwin
check-lib-deps-Darwin:
	@echo -e "\nLooking for missing library dependencies..."
	find src \( -name "*.dylib" -o -name "*.so" \) | xargs -t -I % sh -c '! (otool -l % | grep /usr/local/ )'

.PHONY: check-lib-deps-Linux
check-lib-deps-Linux:
	@echo -e "\nLooking for missing library dependencies..."
	find src -name "*.so" | xargs -t -I % sh -c '! ( ldd % | grep -P " => /usr/(local|lib)/(?!x86_64-linux-gnu)" )'
