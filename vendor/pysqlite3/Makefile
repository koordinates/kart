PYSQLITE3_REF ?= 0.4.5
PYSQLITE3_REPO ?= coleifer/pysqlite3
PYSQLITE3_ARCHIVE := libsqlite3-$(PYSQLITE3_REF).tar.gz
SQLITE_PATH := ../sqlite/src

SHELL = /bin/bash
export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

export CFLAGS += -g -I$(SQLITE_PATH)

ifeq ($(PLATFORM),Darwin)
	CCACHE_PATH = /usr/local/opt/ccache/bin
	LIBSUFFIX = dylib
	PY3 ?= $(realpath /Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7)
	WHEELTOOL = delocate
	LDFLAGS := -Wl,-rpath,'@loader_path/../../../'
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	WHEELTOOL = auditwheel
	LDFLAGS += -Wl,-rpath='$$ORIGIN/../../..'
endif
PY3 := $(or $(PY3),python3.7)

export LDFLAGS += -L$(PREFIX)/lib -lsqlite3

SQLITE3_ORIGIN := $(addprefix $(SQLITE_PATH)/,sqlite3.h sqlite3.c)
SQLITE3_SOURCE := $(addprefix src/,sqlite3.h sqlite3.c)

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
	-(cd src && $(RM) sqlite3.[ch])

.PHONY: cleanest
cleanest: clean
	-$(RM) -r src

.PHONY: clean-configure
clean-configure:

#
# Download Archives
#

$(PYSQLITE3_ARCHIVE):
	wget https://github.com/$(PYSQLITE3_REPO)/archive/$(PYSQLITE3_REF).tar.gz -O $@

.PHONY: archive
archive: $(PYSQLITE3_ARCHIVE)

#
# Extract Archives
#

src: $(PYSQLITE3_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(PYSQLITE3_ARCHIVE) ..."
	tar xzf $(PYSQLITE3_ARCHIVE) --strip-components=1 -C $@


.PHONY: source
source: src


$(SQLITE3_SOURCE): $(SQLITE3_ORIGIN) | src
	cp $^ $(@D)

#
# PYSQLITE3
#

.PHONY: install
install: $(SQLITE3_SOURCE) | src
	cd src && python3 setup.py --no-user-cfg build_static
	pip3 install src

.PHONY: wheel
wheel: $(SQLITE3_SOURCE) | src
	cd src && python3 setup.py --no-user-cfg build_static
	cd src && python3 setup.py  --no-user-cfg bdist_wheel -d $(abspath $@)
