S2_PY_REF ?= 4815e08d0c92c4aedffed05910b47ba862088433
S2_PY_REPO ?= koordinates/s2-py
S2_PY_ARCHIVE := s2-py-$(S2_PY_REF).tar.gz

# s2-py contains s2geometry as a submodule, but submodules aren't included in archives,
# so we have to download this separately:

# v0.9.0+
S2GEOMETRY_REF ?= 20ea540d81f4575a3fc0aea585aac611bcd03ede
S2GEOMETRY_REPO ?= google/s2geometry
S2GEOMETRY_ARCHIVE := s2geometry-$(S2GEOMETRY_REF).tar.gz

GOOGLETEST_REF ?= release-1.11.0
GOOGLETEST_REPO ?= google/googletest
GOOGLETEST_ARCHIVE := googletest-$(GOOGLETEST_REF).tar.gz

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
	PY3 ?= $(realpath /Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7)
	WHEELTOOL = delocate
	export OPENSSL_ROOT_DIR ?= /usr/local/opt/openssl@1.1
	export OPENSSL_INCLUDE_DIR ?= /usr/local/opt/openssl@1.1/include
	export MACOSX_DEPLOYMENT_TARGET = 10.9
	export LDFLAGS+=-Wl,-rpath,'@loader_path'
	export LDFLAGS+=-Wl,-rpath,'@loader_path/../../../'
else ifeq ($(PLATFORM),Linux)
	CCACHE_PATH = /usr/lib/ccache
	CCACHE_PATH := $(or $(CCACHE_PATH),/usr/lib64/ccache)
	LIBSUFFIX = so
	WHEELTOOL = auditwheel
	export LDFLAGS := -Wl,-rpath='$$ORIGIN:$$ORIGIN/../../..'
endif

PY3 := $(or $(PY3),python3.7)

export GTEST_ROOT := $(abspath src/googletest)

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
	-$(RM) -r src/s2geometry src/googletest

.PHONY: clean-configure
clean-configure:

#
# Download Archives
#
$(S2_PY_ARCHIVE):
	wget https://github.com/$(S2_PY_REPO)/archive/$(S2_PY_REF).tar.gz -O $@

$(S2GEOMETRY_ARCHIVE):
	wget https://github.com/$(S2GEOMETRY_REPO)/archive/$(S2GEOMETRY_REF).tar.gz -O $@

$(GOOGLETEST_ARCHIVE):
	wget https://github.com/$(GOOGLETEST_REPO)/archive/$(GOOGLETEST_REF).tar.gz -O $@

.PHONY: archive
archive: $(S2_PY_ARCHIVE) $(S2GEOMETRY_ARCHIVE) $(GOOGLETEST_ARCHIVE)

#
# Extract Archives
#

src: $(S2_PY_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(S2_PY_ARCHIVE) ..."
	tar xzf $(S2_PY_ARCHIVE) --strip-components=1 -C $@
	# Get rid of empty src/s2geometry directory caused by submodule:
	rm -rf src/s2geometry

src/s2geometry: src $(S2GEOMETRY_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(S2GEOMETRY_ARCHIVE) ..."
	tar xzf $(S2GEOMETRY_ARCHIVE) --strip-components=1 -C $@

src/googletest: src $(GOOGLETEST_ARCHIVE)
	rm -rf $@
	mkdir -p $@
	@echo "Extracting $(GOOGLETEST_ARCHIVE) ..."
	tar xzf $(GOOGLETEST_ARCHIVE) --strip-components=1 -C $@

.PHONY: source
source: src src/s2geometry src/googletest


.PHONY: py-fix-Darwin
py-fix-Darwin:
	install_name_tool \
		-change "$(OPENSSL_ROOT_DIR)/lib/libssl.1.1.dylib" "@rpath/libssl.1.1.dylib" \
		-change "$(OPENSSL_ROOT_DIR)/lib/libcrypto.1.1.dylib" "@rpath/libcrypto.1.1.dylib" \
		src/build/lib.macosx*/s2_py/_pywraps2.so
	install_name_tool \
		-change "$(OPENSSL_ROOT_DIR)/lib/libssl.1.1.dylib" "@rpath/libssl.1.1.dylib" \
		-change "$(OPENSSL_ROOT_DIR)/lib/libcrypto.1.1.dylib" "@rpath/libcrypto.1.1.dylib" \
		src/build/lib.macosx*/s2_py/libs2.dylib
	otool -L src/build/lib.macosx*/s2_py/_pywraps2.so
	otool -L src/build/lib.macosx*/s2_py/libs2.dylib


.PHONY: py-fix-Linux
py-fix-Linux:

.PHONY: install
install: | source
	@echo 'CFLAGS=$(CFLAGS) LDFLAGS=$(LDFLAGS) PY3=$(PY3)'
	cd src && python3 setup.py install

.PHONY: wheel
wheel: | source
	@echo 'CFLAGS=$(CFLAGS) LDFLAGS=$(LDFLAGS) PY3=$(PY3)'

	cd src && python3 setup.py build

	$(MAKE) py-fix-$(PLATFORM)

	cd src && python3 setup.py --no-user-cfg bdist_wheel -d $(abspath $@)

	$(MAKE) py-deps-$(PLATFORM)

.PHONY: py-deps-Darwin
py-deps-Darwin:
	for W in wheel/*.whl; do \
		echo "$$W"; \
		! ( delocate-listdeps "$$W" | sed -e '/@rpath/d' -e '/Python/d' | grep '[^[:space:]]' ) || exit 1; \
	done


.PHONY: py-deps-Linux
py-deps-Linux:
	auditwheel show wheel/*.whl
