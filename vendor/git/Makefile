# v2.33.0 + various git changes + koordinates changes
GIT_REF ?= e1645ec76fb513918e35204a65d94524682b1445
GIT_REPO ?= koordinates/git
GIT_ARCHIVE := git-$(GIT_REF).tar.gz

SHELL = /bin/bash
export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

ifeq ($(PLATFORM),Darwin)
	LIBSUFFIX = dylib
	CURL_PREFIX=/usr/local/opt/curl
	CCACHE_PATH = /usr/local/opt/ccache/bin
	LOADER_PATH = @loader_path
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CURL_PREFIX = $(PREFIX)
	libcurl := $(PREFIX)/lib/libcurl.$(LIBSUFFIX)
	libs += $(libcurl)
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	LOADER_PATH := $$ORIGIN
endif

CONFIG_FLAGS += CURL_CONFIG=$(CURL_PREFIX)/bin/curl-config
CONFIG_FLAGS += NO_GETTEXT=YesPlease
CONFIG_FLAGS += V=1
CONFIG_FLAGS += INSTALL_SYMLINKS=1
CONFIG_FLAGS += FILTER_EXTENSIONS=$(abspath ../spatial-filter/spatial.a)

CFLAGS += -I$(abspath src)

LDFLAGS := -lstdc++
LDFLAGS += -lsqlite3
LDFLAGS += -L$(PREFIX)/lib

SEARCH_PATHS := $(LOADER_PATH)/../ $(LOADER_PATH)/../../
LIB_PATHS := $(patsubst %,%lib,$(SEARCH_PATHS))
RPATH := $(LOADER_PATH) $(SEARCH_PATHS) $(LIB_PATHS)

SPACE := $() $()
RPATH_LINUX := $(subst $(SPACE),:,$(RPATH))
RPATH_DARWIN := $(subst $(SPACE), -add_rpath ,$(RPATH))

build-git := src/git

# default target
.PHONY: all
all: $(build-git)

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) -r src/git

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C src/ clean

.PHONY: cleanest
cleanest:
	-$(RM) -r src/

#
# Download Archives
#
$(GIT_ARCHIVE):
	wget https://github.com/$(GIT_REPO)/archive/$(GIT_REF).tar.gz -O $@

.PHONY: archive
archive: $(GIT_ARCHIVE)

#
# Extract Archives
#

src: $(GIT_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(GIT_ARCHIVE) ..."
	tar xzf $(GIT_ARCHIVE) --strip-components=1 -C $@


.PHONY: source
source: src

$(build-git): export PKG_CONFIG_PATH=/dev/null
$(build-git): | src $(libs)
	$(MAKE) -C src -j 2 $(CONFIG_FLAGS) LINK=g++ CFLAGS='$(CFLAGS)' LDFLAGS='$(LDFLAGS)' all strip

#
# INSTALLATION
#

.PHONY: install
install: export PKG_CONFIG_PATH=/dev/null
install: | src $(libs)
	$(MAKE) -C src prefix=$(PREFIX) $(CONFIG_FLAGS) LINK=g++ CFLAGS='$(CFLAGS)' LDFLAGS='$(LDFLAGS)' install
	$(MAKE) print-lib-deps-$(PLATFORM)
	$(MAKE) lib-fix-$(PLATFORM)
	$(MAKE) check-lib-deps-$(PLATFORM)

.PHONY: print-lib-deps
print-lib-deps: print-lib-deps-$(PLATFORM)

.PHONY: print-lib-deps-Darwin
print-lib-deps-Darwin:
	@echo -e "\nExamining library dependencies..."
	cd $(PREFIX) && find . -perm -o=x -type f -name "git*" -exec otool -L {} \;

.PHONY: print-lib-deps-Linux
print-lib-deps-Linux:
	@echo -e "\nExamining library dependencies..."
	cd $(PREFIX) && find . -perm -o=x -type f -name "git*" -print -exec sh -c "ldd {} && readelf -d {} | grep 'RPATH'" \;

.PHONY: lib-fix-Darwin
lib-fix-Darwin:
	@echo fixing git libraries ...
	cd $(PREFIX) && find . -perm -o=x -type f -name "git*" -print -exec sh -c \
		"install_name_tool -change $(CURL_PREFIX)/lib/libcurl.4.dylib @rpath/libsqlite3.0.dylib {}" \;
	cd $(PREFIX) && find . -perm -o=x -type f -name "git*" -print -exec sh -c \
		"install_name_tool -add_rpath $(RPATH_DARWIN) {}" \;

.PHONY: lib-fix-Linux
lib-fix-Linux:
	@echo fixing git libraries ...
	cd $(PREFIX) && find . -perm -o=x -type f -name "git*" -print -exec \
		patchelf --set-rpath '$(RPATH_LINUX)' {} \;

.PHONY: check-lib-deps
check-lib-deps: check-lib-deps-$(PLATFORM)

.PHONY: check-lib-deps-Darwin
check-lib-deps-Darwin:
	@echo -e "\nLooking for missing library dependencies..."
	cd $(PREFIX) && find . -perm -o=x -type f -name "git*" | xargs -t -I % sh -c '! (otool -l % | grep /usr/local/ )'

.PHONY: check-lib-deps-Linux
check-lib-deps-Linux:
	@echo -e "\nLooking for missing library dependencies..."
	cd $(PREFIX) && find . -perm -o=x -type f -name "git*" | xargs -t -I % sh -c '! ( ldd % | grep -P " => /usr/(local|lib)/(?!x86_64-linux-gnu)" )'
