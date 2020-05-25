PSYCOPG2_ARCHIVE := psycopg2-2.8.5.tar.gz
PSYCOPG2_URL := https://files.pythonhosted.org/packages/a8/8f/1c5690eebf148d1d1554fc00ccf9101e134636553dbb75bdfef4f85d7647/$(PSYCOPG2_ARCHIVE)

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
	export PKG_CONFIG_PATH="/usr/local/opt/openssl@1.1/lib/pkgconfig"
	LDFLAGS += -Wl,-rpath,'@loader_path/../../..'
	export LDFLAGS += $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config libssl --libs)
	export CFLAGS += $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config libssl --cflags)
	SSL_PREFIX := /usr/local/opt/openssl@1.1
	PG_PREFIX := /usr/local/opt/postgresql
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	WHEELTOOL = auditwheel
	export LDFLAGS = -Wl,-rpath='$$ORIGIN/../../../',--enable-new-dtags
endif
PY3 := $(or $(PY3),python3.7)

# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

# default target
.PHONY: all
all: wheel

.PHONY: clean
clean:
	-$(RM) -r env wheel
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

$(PSYCOPG2_ARCHIVE):
	wget "$(PSYCOPG2_URL)" -O $@

.PHONY: archive
archive: $(PSYCOPG2_ARCHIVE)

#
# Extract Archives
#

src: $(PSYCOPG2_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(PSYCOPG2_ARCHIVE) ..."
	tar xzf $(PSYCOPG2_ARCHIVE) --strip-components=1 -C $@

.PHONY: source
source: src

#
# APSW
#

.PHONY: py-fix-Darwin
py-fix-Darwin:
	install_name_tool \
		-change "$(SSL_PREFIX)/lib/libssl.1.1.dylib" "@rpath/libssl.1.1.dylib" \
		-change "$(SSL_PREFIX)/lib/libcrypto.1.1.dylib" "@rpath/libcrypto.1.1.dylib" \
		-change "$(PG_PREFIX)/lib/libpq.5.dylib" "@rpath/libpq.5.dylib" \
		src/build/lib.macosx*/psycopg2/_psycopg.cpython-37m-darwin.so; \
	otool -L src/build/lib.macosx*/psycopg2/_psycopg.cpython-37m-darwin.so

.PHONY: py-fix-Linux
py-fix-Linux:

.PHONY: install
install: | src
	@echo 'CFLAGS=$(CFLAGS) LDFLAGS=$(LDFLAGS) PY3=$(PY3)'
	python setup.py install

.PHONY: wheel
wheel: | src
	@echo 'CFLAGS=$(CFLAGS) LDFLAGS=$(LDFLAGS) PY3=$(PY3)'
	cd src && python3 setup.py --no-user-cfg build

	$(MAKE) py-fix-$(PLATFORM)

	cd src && python3 setup.py  --no-user-cfg bdist_wheel -d $(abspath $@)

	$(MAKE) py-deps-$(PLATFORM)

.PHONY: py-deps-Darwin
py-deps-Darwin:
	delocate-listdeps wheel/*.whl

.PHONY: py-deps-Linux
py-deps-Linux:
	auditwheel show wheel/*.whl
