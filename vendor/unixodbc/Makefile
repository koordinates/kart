UNIXODBC_REF ?= v2.3.9
UNIXODBC_REPO ?= lurcher/unixODBC
UNIXODBC_ARCHIVE := unixODBC-$(UNIXODBC_REF).tar.gz

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
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
endif

# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

automake-unixodbc = src/Makefile.in
automake-libltdl = src/libltdl/Makefile.in
configure-unixodbc = src/config.status

# default target
.PHONY: all
all: $(build-unixodbc)

.PHONY: clean
clean:
	-$(RM) -r env/

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C src/ distclean

.PHONY: cleanest
cleanest:
	-$(RM) -r src/

.PHONY: clean-configure
clean-configure:
	-$(RM) $(configure-unixodbc)


#
# Download Archives
#

$(UNIXODBC_ARCHIVE):
	wget https://github.com/$(UNIXODBC_REPO)/archive/$(UNIXODBC_REF).tar.gz -O $@

.PHONY: archive
archive: $(UNIXODBC_ARCHIVE)

#
# Extract Archives
#

src: $(UNIXODBC_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(UNIXODBC_ARCHIVE) ..."
	tar xzf $(UNIXODBC_ARCHIVE) --strip-components=1 -C $@


.PHONY: source
source: src

#
# unixodbc
#

$(automake-unixodbc): | src
	cd src/ && aclocal && automake

$(automake-libltdl): | src
	cd src/libltdl/ && aclocal && automake

$(configure-unixodbc): automake-unixodbc automake-libltdl
	cd src/ && ./configure \
		--prefix=$(PREFIX)

.PHONY: configure
configure: clean-configure $(configure-unixodbc)

$(build-unixodbc): $(configure-unixodbc)
	$(MAKE) -C src -j 2
	$(MAKE) print-lib-deps-$(PLATFORM)
	$(MAKE) check-lib-deps-$(PLATFORM)

.PHONY: install
install: $(build-uniodbc)
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
