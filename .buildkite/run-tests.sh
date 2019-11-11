#!/bin/bash
set -e

# install test requirements
echo "--- Installing test requirements..."
pip install \
    -r requirements-test.txt

# run the actual test suite
echo "+++ Running test suite..."
chmod a+wrx ./ ./tests/
cd tests
gosu sno pytest \
    --verbose \
    -p no:sugar \
    --cov-report term \
    --cov-report html:../coverage \
    --junit-xml=../pytest.xml \
    -o cache_dir=/tmp/pytest_cache --cache-clear
