PYODBC_REF ?= 4.0.30
PYODBC_REPO ?= mkleehammer/pyodbc
PYODBC_ARCHIVE := pyodbc-$(PYODBC_REF).tar.gz

SHELL = /bin/bash
export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

export CFLAGS += -g

ifeq ($(PLATFORM),Darwin)
	CCACHE_PATH = /usr/local/opt/ccache/bin
	LIBSUFFIX = dylib
	PY3 ?= $(realpath /Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7)
	WHEELTOOL = delocate
	export LDFLAGS+=-Wl,-rpath,'@loader_path/../..'
	UNIXODBC_PREFIX = /usr/local/opt/unixodbc/lib
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	WHEELTOOL = auditwheel
	export LDFLAGS := -Wl,-rpath='$$ORIGIN/../..'
	UNIXODBC_PREFIX = /usr/lib64
	LIBLTDL_PREFIX = /usr/local/lib/libltdl.so.7
endif
PY3 := $(or $(PY3),python3.7)

pyodbc := $(abspath $(PREFIX)/lib/pyodbc.$(LIBSUFFIX))

# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

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

$(PYODBC_ARCHIVE):
	wget https://github.com/$(PYODBC_REPO)/archive/$(PYODBC_REF).tar.gz -O $@

.PHONY: archive
archive: $(PYODBC_ARCHIVE)

#
# Extract Archives
#

src: $(PYODBC_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(PYODBC_ARCHIVE) ..."
	tar xzf $(PYODBC_ARCHIVE) --strip-components=1 -C $@


.PHONY: source
source: src

#
# pyodbc
#

.PHONY: py-fix-Darwin
py-fix-Darwin:
	install_name_tool \
		-change "$(UNIXODBC_PREFIX)/libodbc.2.dylib" "@rpath/libodbc.2.dylib" \
		src/build/lib.macosx*/pyodbc.cpython-37m-darwin.so; \
	otool -L src/build/lib.macosx*/pyodbc.cpython-37m-darwin.so

.PHONY: py-fix-Linux
py-fix-Linux:
	patchelf --remove-rpath \
		src/build/lib.linux-x86_64-*/pyodbc.cpython-37m-x86_64-linux-gnu.so; \
	patchelf --force-rpath --set-rpath '$$ORIGIN/../..' \
		src/build/lib.linux-x86_64-*/pyodbc.cpython-37m-x86_64-linux-gnu.so

.PHONY: install
install: | src
	@echo 'CFLAGS=$(CFLAGS) LDFLAGS=$(LDFLAGS) PY3=$(PY3)'
	cd src && python3 setup.py install

.PHONY: wheel
wheel: | src
	@echo 'CFLAGS=$(CFLAGS) LDFLAGS=$(LDFLAGS) PY3=$(PY3)'
	cd src && python3 setup.py --no-user-cfg build

	$(MAKE) py-fix-$(PLATFORM)

	cd src && python3 setup.py  --no-user-cfg bdist_wheel -d $(abspath $@)

	$(MAKE) py-deps-$(PLATFORM)

.PHONY: py-deps-Darwin
py-deps-Darwin:
	for W in wheel/*.whl; do \
		echo "$W"; \
		! ( delocate-listdeps "$$W" | sed -e '/@rpath/d' | grep '[^[:space:]]' ) || exit 1; \
	done

.PHONY: py-deps-Linux
py-deps-Linux:
	auditwheel show wheel/*.whl
