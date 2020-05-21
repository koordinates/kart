CURL_VERSION = 7.68.0

SHELL = /bin/bash
export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

CURL_ARCHIVE := curl-$(CURL_VERSION).tar.gz

ifeq ($(PLATFORM),Darwin)
	CCACHE_PATH = /usr/local/opt/ccache/bin
	LIBSUFFIX = dylib
	CONFIG_FLAGS += --with-darwinssl
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	CONFIG_FLAGS += --with-ssl
endif


# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

build-curl := src/.libs/libcurl.4.$(LIBSUFFIX)
configure-curl = src/config.status

# default target
.PHONY: all
all: $(build-curl)

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) src/.libs/libcurl.* src/libcurl.la
	-$(RM) src/curl*.pc

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C src/ clean

.PHONY: cleanest
cleanest:
	-$(RM) -r src/

.PHONY: clean-configure
clean-configure:
	-$(RM) $(configure-curl)

#
# Download Archives
#

$(CURL_ARCHIVE):
	wget https://curl.haxx.se/download/$@ -O $@

.PHONY: archive
archive: $(CURL_ARCHIVE)

#
# Extract Archives
#

src: $(CURL_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(CURL_ARCHIVE) ..."
	tar xzf $(CURL_ARCHIVE) --strip-components=1 -C $@


.PHONY: source
source: src

$(configure-curl): export PKG_CONFIG_PATH=/dev/null
$(configure-curl): | src
	env | grep PKG
	cd src/ && ./configure \
		--prefix=$(PREFIX) \
		$(CONFIG_FLAGS)

.PHONY: configure
configure: clean-configure $(configure-curl)

$(build-curl): $(configure-curl)
	$(MAKE) -C src -j 2
	$(MAKE) print-lib-deps-$(PLATFORM)

#
# INSTALLATION
#

.PHONY: install
install: $(build-curl)
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
