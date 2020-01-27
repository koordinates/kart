.DEFAULT_GOAL := all

LIBGIT2_REPO ?= https://github.com/koordinates/libgit2.git
LIBGIT2_BRANCH ?= kx-0.28
PYGIT2_REPO ?= https://github.com/koordinates/pygit2.git
PYGIT2_BRANCH ?= kx-0.28

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

DOCKER_TAG ?= sno:latest
DOCKER_BUILD_ARGs ?= --pull

# Python binaries like python, pip in the venv will take precedence.
VIRTUAL_ENV ?= venv
SHELL := /bin/bash  # required for PATH to work
PATH := $(realpath $(VIRTUAL_ENV)/bin):$(PATH)

PREFIX ?= /usr/local

# Python dependencies via pip-compile
BASE_PIP_COMPILE_CMD = CUSTOM_COMPILE_COMMAND="make requirements" pip-compile -v --annotate --no-index --no-emit-trusted-host
PIP_COMPILE_CMD ?= $(BASE_PIP_COMPILE_CMD)
PY_SITEPACKAGES =
PY_REQS = requirements.txt requirements/test.txt requirements/dev.txt
PY_PIP_ARGS ?= -e

# Native library dependencies
ifeq ($(PLATFORM),Darwin)
	PLATFORM_DEPS := Brewfile.lock.json
	libgit2 := $(VIRTUAL_ENV)/lib/libgit2.dylib
else
	PLATFORM_DEPS := $(PLATFORM_DEPS_$(PLATFORM))
	libgit2 := $(VIRTUAL_ENV)/lib/libgit2.so
endif

ifneq ($(MAKECMDGOALS),clean)
-include .venv.mk
endif
.venv.mk: $(VIRTUAL_ENV)
	@echo PY_SITEPACKAGES=$(shell $(VIRTUAL_ENV)/bin/python -c "import os; from distutils.sysconfig import get_python_lib; print(os.path.relpath(get_python_lib(), '$(VIRTUAL_ENV)'))") > $@
	@cat $@

.PHONY: library-deps
library-deps: PLATFORM_CLEAN_$(PLATFORM) $(PLATFORM_DEPS)

# Darwin
Brewfile.lock.json: Brewfile
	@which -s brew || (echo "Homebrew is required for MacOS: https://brew.sh" && exit 1)
	brew bundle install --verbose --no-upgrade

.PHONY: PLATFORM_CLEAN_Darwin
PLATFORM_CLEAN_Darwin:
	rm -rf Brewfile.lock.json

.PHONY: homebrew-upgrade
homebrew-upgrade:
	brew bundle install --verbose

# Create virtualenv
$(VIRTUAL_ENV): $(PLATFORM_DEPS)
	python3 -m venv $(VIRTUAL_ENV)

.PHONY: py-venv-upgrade
py-venv-upgrade: $(VIRTUAL_ENV)
	python3 -m venv $(VIRTUAL_ENV) --upgrade
	rm requirements/.*.installed

.PHONY: py-requirements
py-requirements: requirements/.tools.installed
	touch requirements/*.in
	@$(MAKE) $(PY_REQS)

.PHONY: py-requirements-upgrade
py-requirements-upgrade: requirements/.tools.installed
	touch requirements/*
	export PIP_COMPILE_CMD=$(BASE_PIP_COMPILE_CMD) --upgrade
	@$(MAKE) $(PY_REQS) py-license-check

requirements.txt: requirements/requirements.in requirements/licenses.ini
	$(PIP_COMPILE_CMD) --output-file $@ $<
	@sed -E -i '' -e 's/^(pygit2=)/#\1/' $@  # Comment out pygit2, because we install manually afterwards

requirements/test.txt: requirements/test.in requirements.txt
	$(PIP_COMPILE_CMD) --output-file $@ $<
	@sed -E -i '' -e 's/^(pygit2=)/#\1/' $@  # Comment out pygit2, because we install manually afterwards

requirements/dev.txt: requirements/dev.in requirements.txt requirements/test.txt
	$(PIP_COMPILE_CMD) --output-file $@ $<
	@sed -E -i '' -e 's/^(pygit2=)/#\1/' $@  # Comment out pygit2, because we install manually afterwards

# libgit2
.PHONY: libgit2
libgit2: vendor/libgit2/build
	$(MAKE) -C vendor/libgit2/build install

$(libgit2): vendor/libgit2/build
	$(MAKE) -C vendor/libgit2/build install

vendor/libgit2:
	mkdir -p $@
	git clone --branch=$(LIBGIT2_BRANCH) $(VENDOR_GIT_CLONE_ARGS) $(LIBGIT2_REPO) $@

vendor/libgit2/build: vendor/libgit2
	mkdir -p $@
	cd vendor/libgit2/build; \
	LIBGIT2=$(realpath $(VIRTUAL_ENV)) \
	cmake -S .. \
		-DCMAKE_INSTALL_PREFIX=$(realpath $(VIRTUAL_ENV)) \
		-DBUILD_EXAMPLES=NO \
		-DBUILD_CLAR=NO

# pygit2
pygit2 := $(VIRTUAL_ENV)/$(PY_SITEPACKAGES)/pygit2.egg-link

.PHONY: pygit2
pygit2:
	rm -f $(pygit2)
	@$(MAKE) $(pygit2)

$(pygit2): $(libgit2) vendor/pygit2
	LIBGIT2=$(realpath $(VIRTUAL_ENV)) \
	LDFLAGS="-Wl,-rpath,'$(realpath $(VIRTUAL_ENV)/lib)' $(LDFLAGS)" \
	pip install --no-deps -v $(PY_PIP_ARGS) vendor/pygit2

vendor/pygit2:
	mkdir -p $@
	git clone --branch=$(PYGIT2_BRANCH) $(VENDOR_GIT_CLONE_ARGS) $(PYGIT2_REPO) $@

# Install Python (just release) dependencies
.PHONY: py-deps
py-deps: $(VIRTUAL_ENV) $(pygit2) requirements/.requirements.installed

# Install Python (development & release) py-deps
.PHONY: py-deps-dev
py-deps-dev: py-deps requirements/.dev.installed requirements/.tools.installed

requirements/.requirements.installed: $(VIRTUAL_ENV) requirements.txt
	pip install --no-deps pygdal=="$(shell gdal-config --version).*"
	pip install --no-deps -r requirements.txt
	touch $@

requirements/.dev.installed: $(VIRTUAL_ENV) requirements/dev.txt requirements/test.txt
	pip install --no-deps -r requirements/dev.txt -r requirements/test.txt
	touch $@

requirements/.tools.installed: $(VIRTUAL_ENV)
	pip install -U pip-tools liccheck pipdeptree
	touch $@

# App code
py-app := $(VIRTUAL_ENV)/bin/sno

.PHONY: py-app
py-app: py-deps $(py-app)

$(py-app): setup.py
	pip install --no-deps $(PY_PIP_ARGS) .

# Top-level targets
.PHONY: all
all: py-deps-dev $(py-app)

.PHONY: install
install: $(PREFIX)/bin/sno

$(PREFIX)/bin/sno: $(py-app)
	ln -s $(realpath $(py-app)) $@

# Dependency license checking
.PHONY: py-license-check
py-license-check: py-deps requirements/.tools.installed requirements/licenses.ini
	echo "hi there"
	liccheck -l CAUTIOUS -s requirements/licenses.ini -r requirements.txt


# Docker Image

.PHONY: docker
docker:
	docker build $(DOCKER_BUILD_ARGS) -t $(DOCKER_TAG) .

# CI Tests via Docker

TEST_CLEANUP = find sno tests -name '__pycache__' -print0 | xargs -r0t -- rm -rf

.PHONY: test-cleanup
test-clean:
	rm -rf .pytest_* .coverage coverage
	$(TEST_CLEANUP)

.PHONY: ci-test
ci-test: test-clean
	docker run --rm -it \
		--volume $(PWD):/src:delegated \
		--workdir /src \
		--tmpfs /tmp \
		--user root \
		$(DOCKER_TAG) \
		/src/.buildkite/run-tests.sh \
	&& $(TEST_CLEANUP) \
	|| (R=$$?; $(TEST_CLEANUP) && exit $$R)

.PHONY: test
test: py-deps-dev $(py-app)
	pytest -v --cov-report term --cov-report html:coverage

# Cleanup

.PHONY: clean
clean: PLATFORM_CLEAN_$(PLATFORM) test-clean
	rm -f $(PREFIX)/bin/sno
	-$(MAKE) -C vendor/libgit2/build clean
	-cd vendor/pygit2 && ../../$(VIRTUAL_ENV)/bin/python setup.py clean
	rm -f requirements/.*.installed
	rm -rf $(VIRTUAL_ENV)
	rm -f .venv.mk

.PHONY: distclean
distclean: clean
	rm -rf vendor/libgit2/build
