LIBGIT2_REF ?= v1.1.0
LIBGIT2_REPO ?= libgit2/libgit2
LIBGIT2_ARCHIVE := libgit2-1.1.0.tar.gz

CFLAGS += -g
CXXFLAGS += -g

SHELL = /bin/bash
export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

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

build-libgit2 := build/libgit2.$(LIBSUFFIX)
configure-libgit2 = build/Makefile

# default target
.PHONY: all
all: $(build-libgit2)

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) -r build/libgit2.*

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C build/ clean

.PHONY: cleanest
cleanest:
	-$(RM) -r build/ src/

.PHONY: clean-configure
clean-configure:
	-$(RM) $(configure-libgit2)

#
# Download Archives
#

$(LIBGIT2_ARCHIVE):
	wget https://github.com/$(LIBGIT2_REPO)/archive/$(LIBGIT2_REF).tar.gz -O $@

.PHONY: archive
archive: $(LIBGIT2_ARCHIVE)

#
# Extract Archives
#

src: $(LIBGIT2_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(LIBGIT2_ARCHIVE) ..."
	tar xzf $(LIBGIT2_ARCHIVE) --strip-components=1 -C $@


.PHONY: source
source: src

$(configure-libgit2): export PKG_CONFIG_PATH=/dev/null
$(configure-libgit2): | src
	cmake -S src -B build \
		-DCMAKE_INSTALL_PREFIX=$(PREFIX) \
		-DBUILD_EXAMPLES=NO \
		-DBUILD_CLAR=NO \
		-DUSE_SSH=NO

.PHONY: configure
configure: clean-configure $(configure-libgit2)

$(build-libgit2): $(configure-libgit2)
	cmake \
		--build build \
		--config RelWithDebInfo
	$(MAKE) print-lib-deps-$(PLATFORM)

#
# INSTALLATION
#

.PHONY: install
install: $(build-libgit2)
	cmake \
		--install build \
		--config RelWithDebInfo

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
