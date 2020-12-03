JSONC_VERSION = 0.15
JSONC_RELEASE_DATE = 20200726

SHELL = /bin/bash
export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

JSONC_ARCHIVE := json-c-$(JSONC_VERSION).tar.gz

ifeq ($(PLATFORM),Darwin)
	CCACHE_PATH = /usr/local/opt/ccache/bin
	LIBSUFFIX = dylib
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)

	# https://github.com/json-c/json-c/pull/499
	# https://stackoverflow.com/a/45137452/2662
	# still not fixed in 0.15
    export CFLAGS += -C
endif

# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

build-jsonc := src/.libs/libjson-c.5.$(LIBSUFFIX)
configure-jsonc = build/Makefile

# default target
.PHONY: all
all: $(build-jsonc)

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) -r $(configure-jsonc)

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C build/ clean

.PHONY: cleanest
cleanest:
	-$(RM) -r build/ src/

.PHONY: clean-configure
clean-configure:
	-$(RM) $(configure-jsonc)

#
# Download Archives
#

$(JSONC_ARCHIVE):
	wget https://github.com/json-c/json-c/archive/json-c-$(JSONC_VERSION)-$(JSONC_RELEASE_DATE).tar.gz -O $@

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
	mkdir build
	cd build/ && \
	cmake \
		-DBUILD_STATIC_LIBS=OFF \
		-DCMAKE_INSTALL_PREFIX=$(PREFIX) \
		../src

.PHONY: configure
configure: clean-configure $(configure-jsonc)

$(build-jsonc): $(configure-jsonc)
	$(MAKE) -C build -j 2
	$(MAKE) print-lib-deps-$(PLATFORM)
	$(MAKE) check-lib-deps-$(PLATFORM)

#
# INSTALLATION
#

.PHONY: install
install: $(build-jsonc)
	$(MAKE) -C build install

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

.PHONY: check-lib-deps
check-lib-deps: check-lib-deps-$(PLATFORM)

.PHONY: check-lib-deps-Darwin
check-lib-deps-Darwin:
	@echo -e "\nLooking for missing library dependencies..."
	find build \( -name "*.dylib" -o -name "*.so" \) | xargs -t -I % sh -c '! (otool -l % | grep /usr/local/ )'

.PHONY: check-lib-deps-Linux
check-lib-deps-Linux:
	@echo -e "\nLooking for missing library dependencies..."
	find build -name "*.so" | xargs -t -I % sh -c '! ( ldd % | grep -P " => /usr/(local|lib)/(?!x86_64-linux-gnu)" )'
