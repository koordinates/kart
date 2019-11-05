#!/bin/bash
set -e

# install test requirements
echo "--- Installing test requirements..."
pip install \
    -r requirements-test.txt

echo "Setting git username/email"
gosu sno git config --global user.name pytest
gosu sno git config --global user.email pytest-sno@koordinates.com

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
