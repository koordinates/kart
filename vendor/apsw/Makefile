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
	PY3 ?= $(realpath /Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7)
	WHEELTOOL = delocate
	export PKG_CONFIG_PATH=/usr/local/opt/sqlite3/lib/pkgconfig/
	CFLAGS+= $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config sqlite3 --cflags)
	LDFLAGS+= $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config sqlite3 --libs)
	export LDFLAGS+=-Wl,-rpath,'@loader_path/../../../'
	SQLITE3_VERSION := $(shell /usr/local/opt/sqlite3/bin/sqlite3 --version 2>/dev/null | awk '{print $$1}')
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	WHEELTOOL = auditwheel
	export CFLAGS += -I$(PREFIX)/include
	export LDFLAGS = -Wl,-rpath='$$ORIGIN/../../../',--enable-new-dtags -L$(PREFIX)/lib -lsqlite3
	SQLITE3_VERSION := $(shell $(PREFIX)/bin/sqlite3 --version 2>/dev/null | awk '{print $$1}')
endif
PY3 := $(or $(PY3),python3.7)

# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

APSW_VERSION := "$(SQLITE3_VERSION)-r1"
APSW_ARCHIVE := "apsw-$(APSW_VERSION).tar.gz"
APSW_BUILD_OPTS := --enable-all-extensions

# default target
.PHONY: all
all: wheel

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) -r wheel wheelhouse
	-$(RM) -r src/build src/dist src/.eggs

.PHONY: cleaner
cleaner: clean
	-(cd src && python3 setup.py clean)

.PHONY: cleanest
cleanest: clean
	-$(RM) -r src

.PHONY: clean-configure
clean-configure:

#
# Download Archives
#

$(APSW_ARCHIVE):
	@echo "APSW_VERSION=$(APSW_VERSION) SQLITE3_VERSION=$(SQLITE3_VERSION) APSW_ARCHIVE=$(APSW_ARCHIVE)"
	wget "https://github.com/rogerbinns/apsw/archive/$(APSW_VERSION).tar.gz" -O $@

.PHONY: archive
archive: $(APSW_ARCHIVE)

#
# Extract Archives
#

src: $(APSW_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(APSW_ARCHIVE) ..."
	tar xzf $(APSW_ARCHIVE) --strip-components=1 -C $@

.PHONY: source
source: src

#
# APSW
#

.PHONY: py-fix-Darwin
py-fix-Darwin:
	for SO in src/build/lib.macosx*/apsw.*.so; do \
		echo $$SO; \
		install_name_tool \
			-change "/usr/local/opt/sqlite/lib/libsqlite3.0.dylib" "@rpath/libsqlite3.0.dylib" \
			"$$SO"; \
	done

.PHONY: py-fix-Linux
py-fix-Linux:

.PHONY: install
install: | src
	@echo 'CFLAGS=$(CFLAGS) LDFLAGS=$(LDFLAGS) PY3=$(PY3)'
	python setup.py install $(APSW_BUILD_OPTS)

.PHONY: wheel
wheel: | src
	@echo 'CFLAGS=$(CFLAGS) LDFLAGS=$(LDFLAGS) PY3=$(PY3)'
	cd src && python3 setup.py --no-user-cfg \
		build $(APSW_BUILD_OPTS)

	$(MAKE) py-fix-$(PLATFORM)

	cd src && python3 setup.py  --no-user-cfg \
		bdist_wheel -d $(abspath $@)

	$(MAKE) py-deps-$(PLATFORM)

.PHONY: py-deps-Darwin
py-deps-Darwin:
	delocate-listdeps wheel/*.whl

.PHONY: py-deps-Linux
py-deps-Linux:
	auditwheel show wheel/*.whl
