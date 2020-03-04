.DEFAULT_GOAL := all

PY_VERSION ?= 3.7
PY_ID ?= cp37-cp37m

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

DOCKER_TAG = sno:latest
DOCKER_BUILD_ARGs = --pull

# Python binaries like python, pip in the venv will take precedence.
VIRTUAL_ENV ?= venv
SHELL = /bin/bash  # required for PATH to work

export PREFIX = /usr/local

# Python dependencies via pip-compile
BASE_PIP_COMPILE_CMD = CUSTOM_COMPILE_COMMAND="make requirements" pip-compile -v --annotate --no-index --no-emit-trusted-host
PIP_COMPILE_CMD ?= $(BASE_PIP_COMPILE_CMD)
PY_REQS = requirements.txt requirements/test.txt requirements/dev.txt

# Native library dependencies
ifeq ($(PLATFORM),Darwin)
	LIBSUFFIX = dylib
	PY3 ?= $(realpath /Library/Frameworks/Python.framework/Versions/$(PY_VERSION)/bin/python$(PY_VERSION))
else
	LIBSUFFIX = so
endif
export PY3 := $(or $(PY3),python$(PY_VERSION))
PY_SITEPACKAGES = lib/python$(PY_VERSION)/site-packages

# use ccache if available
export PATH := $(abspath $(VIRTUAL_ENV)/bin):$(PATH)

# Create virtualenv
$(VIRTUAL_ENV):
	$(PY3) -m venv $@

.PHONY: py-venv-upgrade
py-venv-upgrade: | $(VIRTUAL_ENV)
	$(PY3) -m venv $(VIRTUAL_ENV) --upgrade
	rm requirements/.*.installed

.PHONY: py-requirements
py-requirements: | requirements/.tools.installed
	touch requirements/*.in
	@$(MAKE) $(PY_REQS)

.PHONY: py-requirements-upgrade
py-requirements-upgrade: export PIP_COMPILE_CMD=$(BASE_PIP_COMPILE_CMD) --upgrade
py-requirements-upgrade: | requirements/.tools.installed
	touch requirements/*
	@$(MAKE) $(PY_REQS) py-license-check

requirements.txt: requirements/requirements.in requirements/licenses.ini | requirements/.tools.installed
	$(PIP_COMPILE_CMD) --output-file $@ requirements/requirements.in
#   Comment out pygit2, because we install manually afterwards
	sed -E -i.~bak -e 's/^(pygit2=)/\#\1/' $@
	$(RM) $@.~bak

requirements/test.txt: requirements/test.in requirements.txt | requirements/.tools.installed
	$(PIP_COMPILE_CMD) --output-file $@ requirements/test.in
#   Comment out pygit2, because we install manually afterwards
	sed -E -i.~bak -e 's/^(pygit2=)/\#\1/' $@
	$(RM) $@.~bak

requirements/dev.txt: requirements/dev.in requirements.txt requirements/test.txt | requirements/.tools.installed
	$(PIP_COMPILE_CMD) --output-file $@ requirements/dev.in
#   Comment out pygit2, because we install manually afterwards
	sed -E -i.~bak -e 's/^(pygit2=)/\#\1/' $@
	$(RM) $@.~bak

# Vendor Dependencies

vendor-archive = vendor/dist/vendor-$(PLATFORM).tar.gz

$(vendor-archive):
	$(MAKE) -C vendor all

.PHONY: vendor-install
vendor-install: $(vendor-archive) | $(VIRTUAL_ENV)
	-$(RM) -r vendor/dist/wheelhouse
	tar xvzf $(vendor-archive) -C vendor/dist wheelhouse/
	pip install --force-reinstall --no-deps vendor/dist/wheelhouse/GDAL-*.whl
	pip install --force-reinstall --no-deps vendor/dist/wheelhouse/pygit2-*.whl
	tar xzf $(vendor-archive) --overwrite -C $(VIRTUAL_ENV) --strip-components=1 env/

# Install Python (just release) dependencies
.PHONY: py-deps
py-deps: vendor-install requirements/.requirements.installed | $(VIRTUAL_ENV)

# Install Python (development & release) py-deps
.PHONY: py-deps-dev
py-deps-dev: py-deps requirements/.dev.installed requirements/.tools.installed

requirements/.requirements.installed: export SPATIALINDEX_C_LIBRARY:=$(abspath $(VIRTUAL_ENV)/lib/libspatialindex_c.$(LIBSUFFIX))
requirements/.requirements.installed: requirements.txt | $(VIRTUAL_ENV)
	mkdir -p requirements
	pip install --no-deps -r requirements.txt
	touch $@

requirements/.dev.installed: requirements/dev.txt requirements/test.txt | $(VIRTUAL_ENV)
	pip install --no-deps -r requirements/dev.txt -r requirements/test.txt
	touch $@

requirements/.tools.installed: | $(VIRTUAL_ENV)
	mkdir -p requirements
	pip install -U \
		pip-tools \
		liccheck \
		pipdeptree \
		pyinstaller==3.6.* \
		$(WHEELTOOL)
	touch $@

# App code
sno-app-release = $(VIRTUAL_ENV)/$(PY_SITEPACKAGES)/sno
sno-app-dev = $(VIRTUAL_ENV)/$(PY_SITEPACKAGES)/sno.egg-link
ifeq ($(BUILD_TYPE),release)
	sno-app = $(sno-app-release)
else
	sno-app = $(sno-app-dev)
endif
sno-app-any = $(or ($(realpath $(sno-app-dev)),$(realpath $(sno-app-release))))

.PHONY: sno-app
sno-app: BUILD_TYPE=release
sno-app: py-deps $(sno-app)

$(sno-app): setup.py sno | $(VIRTUAL_ENV)
	-$(RM) dist/*
	python3 setup.py sdist
	pip install --force-reinstall --no-deps dist/*.tar.gz

$(sno-app-dev): setup.py | $(VIRTUAL_ENV)
	pip install --force-reinstall --no-deps -e .

# Top-level targets
.PHONY: all
all: BUILD_TYPE=dev
all: py-deps-dev $(sno-app-dev)

.PHONY: install
install: $(sno-app-any)
	ln -s $(realpath $(VIRTUAL_ENV)/bin/sno) $@

# Dependency license checking
.PHONY: py-license-check
py-license-check: py-deps requirements/.tools.installed requirements/licenses.ini
	liccheck -l CAUTIOUS -s requirements/licenses.ini -r requirements.txt


# Docker Image

.PHONY: docker
docker:
	DOCKER_BUILDKIT=1 docker build $(DOCKER_BUILD_ARGS) --progress=plain -t $(DOCKER_TAG) .

# CI Tests via Docker

TEST_CLEANUP = find sno tests -name '__pycache__' -print0 | xargs -r0t -- rm -rf

.PHONY: test-cleanup
test-clean:
	rm -rf .pytest_* .coverage coverage
	$(TEST_CLEANUP)

.PHONY: docker-ci-test
docker-ci-test: test-clean
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
test:
	pytest -v --cov-report term --cov-report html:coverage

# Cleanup

.PHONY: clean
clean: test-clean
	$(RM) $(PREFIX)/bin/sno
	$(RM) requirements/.*.installed
	$(RM) -r $(VIRTUAL_ENV)

.PHONY: cleaner
cleaner: clean
	-$(MAKE) -C vendor clean clean-configure
	-$(MAKE) -C platforms clean

.PHONY: cleanest
cleanest: cleaner
	-$(MAKE) -C vendor cleaner

.PHONY: clean-docker-cache
clean-docker-cache:
	-docker builder prune --filter type=exec.cachemount --force
