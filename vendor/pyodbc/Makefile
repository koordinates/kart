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

ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
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

# we don't "fix" pyodbc, because we need to depend on the OS unixodbc package,
# because that's what the _drivers_ (eg. SQL Server) depend on.

.PHONY: install
install: | src
	@echo 'CFLAGS=$(CFLAGS) LDFLAGS=$(LDFLAGS) PY3=$(PY3)'
	cd src && python3 setup.py install

.PHONY: wheel
wheel: | src
	@echo 'CFLAGS=$(CFLAGS) LDFLAGS=$(LDFLAGS) PY3=$(PY3)'
	cd src && python3 setup.py --no-user-cfg build

	cd src && python3 setup.py  --no-user-cfg bdist_wheel -d $(abspath $@)

	$(MAKE) py-deps-$(PLATFORM)

.PHONY: py-deps-Linux
py-deps-Linux:
	auditwheel show wheel/*.whl
