# v1.7.0 + koordinates changes
PYGIT2_REF ?= kart-0.11.0
PYGIT2_REPO ?= koordinates/pygit2
PYGIT2_ARCHIVE := pygit2-$(PYGIT2_REF).tar.gz

SHELL = /bin/bash
export PREFIX ?= $(abspath env)

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

export LIBGIT2 ?= $(PREFIX)
export CFLAGS += -g

ifeq ($(PLATFORM),Darwin)
	CCACHE_PATH = /usr/local/opt/ccache/bin
	LIBSUFFIX = dylib
	PY3 ?= $(realpath /Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7)
	WHEELTOOL = delocate
	export LDFLAGS+=-Wl,-rpath,'@loader_path/../../../'
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	WHEELTOOL = auditwheel
	export LDFLAGS := -Wl,-rpath='$$ORIGIN/../../..'
endif
PY3 := $(or $(PY3),python3.7)

libgit2 := $(abspath $(PREFIX)/lib/libgit2.$(LIBSUFFIX))

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

$(PYGIT2_ARCHIVE):
	wget https://github.com/$(PYGIT2_REPO)/archive/$(PYGIT2_REF).tar.gz -O $@

.PHONY: archive
archive: $(PYGIT2_ARCHIVE)

#
# Extract Archives
#

src: $(PYGIT2_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(PYGIT2_ARCHIVE) ..."
	tar xzf $(PYGIT2_ARCHIVE) --strip-components=1 -C $@


.PHONY: source
source: src

#
# PYGIT2
#

.PHONY: install
install: | src $(libgit2)
	@echo 'LDFLAGS=$(LDFLAGS) PY3=$(PY3)'
	pip3 install -e src

.PHONY: wheel
wheel: | src $(libgit2)
	@echo 'LDFLAGS=$(LDFLAGS) PY3=$(PY3)'
	cd src && python3 setup.py  --no-user-cfg bdist_wheel -d $(abspath $@)
