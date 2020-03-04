# v1.1.0+
PYGIT2_REF ?= ccf4df153c68d4af7d3d0f4f4f9104afc6f38d43
PYGIT2_REPO ?= libgit2/pygit2
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
	export LDFLAGS+=-Wl,-rpath,'$(abspath $(LIBGIT2))/lib'
else ifeq ($(PLATFORM),Linux)
	LIBSUFFIX = so
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	WHEELTOOL = auditwheel
	export LDFLAGS+=-Wl,-rpath='$(abspath $(LIBGIT2))/lib',--enable-new-dtags
endif
PY3 := $(or $(PY3),python3.7)

libgit2 := $(abspath $(PREFIX)/lib/libgit2.$(LIBSUFFIX))

# use ccache if available
export PATH := $(CCACHE_PATH):$(PREFIX)/bin:$(PATH)

# default target
.PHONY: all
all: wheels

.PHONY: clean
clean:
	-$(RM) -r env
	-$(RM) -r wheels wheelhouse
	-$(RM) -r src/build src/dist src/.eggs

.PHONY: cleaner
cleaner: clean
	-(cd src && python3 setup.py clean)

.PHONY: cleanest
cleanest: cleaner
	-$(RM) -r src

.PHONY: clean-configure
clean-configure:

#
# Download Archives
#

$(PYGIT2_ARCHIVE):
	wget https://github.com/$(PYGIT2_REPO)/archive/$(PYGIT2_REF).tar.gz

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
	@echo LDFLAGS=$(LDFLAGS)
	pip3.7 install -e src
	$(MAKE) wheels

.PHONY: wheels
wheels: | src $(libgit2)
	@echo LDFLAGS=$(LDFLAGS)
	cd src \
	&& python3 setup.py bdist_wheel -d $(abspath $@)
	$(MAKE) wheelfix-$(PLATFORM)

.PHONY: wheelfix-Darwin
wheelfix-Darwin: export DYLD_LIBRARY_PATH:=$(PREFIX)/lib:$(DYLD_LIBRARY_PATH)
wheelfix-Darwin:
	@echo Examining Wheel dependencies...
	delocate-listdeps wheels/*macosx*.whl

	@echo Resolving wheel dependencies...
	for WHL in wheels/*macosx*.whl; do delocate-wheel -v -w wheelhouse/ $$WHL; done

	@echo Re-examining Wheel dependencies...
	delocate-listdeps wheelhouse/*macosx*.whl

.PHONY: wheelfix-Linux
wheelfix-Linux: export LD_LIBRARY_PATH:=$(PREFIX)/lib:$(LD_LIBRARY_PATH)
wheelfix-Linux:
	@echo Examining Wheel dependencies...
	auditwheel show wheels/*linux*.whl

	@echo Resolving wheel dependencies...
	for WHL in wheels/*linux*.whl; do auditwheel repair -w wheelhouse/ $$WHL; done
	-$(RM) wheelhouse/*-linux_*.whl

	@echo Re-examining Wheel dependencies...
	auditwheel show wheelhouse/*manylinux*.whl
