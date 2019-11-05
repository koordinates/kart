.PHONY: requirements ci-test docker test

# Python dependencies via pip-compile

REQ_SOURCES=$(wildcard requirements*.in)
REQ_TARGETS=$(REQ_SOURCES:.in=.txt)

requirements: $(REQ_TARGETS)
	# Comment out pygit2, because we install manually afterwards
	sed -i -E 's/^(pygit2=)/#\1/' *.txt

requirements.txt: requirements.in
	pip-compile --rebuild --output-file $@ $<
requirements-test.txt: requirements-test.in requirements.txt
	pip-compile --rebuild --output-file $@ $<
requirements-dev.txt: requirements-dev.in requirements.txt requirements-test.txt
	pip-compile --rebuild --output-file $@ $<

# Docker Image

DOCKER_TAG=sno:latest

docker:
	docker build -t $(DOCKER_TAG) .

# CI Tests via Docker

ci-test:
	docker run --rm -it \
		--volume $(PWD):/src:delegated \
		--workdir /src \
		--tmpfs /tmp \
		--user root \
		$(DOCKER_TAG) \
		/src/.buildkite/run-tests.sh

test:
	pytest -v --cov-report term --cov-report html:coverage
