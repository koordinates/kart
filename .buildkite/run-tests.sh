#!/bin/bash
set -e

# install test requirements
echo "--- Installing test requirements..."
pip install \
    -r requirements-test.txt

echo "Setting git username/email"
gosu snowdrop git config --global user.name pytest
gosu snowdrop git config --global user.email pytest-snowdrop@koordinates.com

# run the actual test suite
echo "+++ Running test suite..."
cd tests
gosu snowdrop pytest \
    --verbose \
    -p no:sugar \
    --cov-report term \
    --cov-report html:../coverage \
    --junit-xml=../pytest.xml
