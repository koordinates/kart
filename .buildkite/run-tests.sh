#!/bin/bash
set -e

# install test requirements
echo "--- Installing test requirements..."
/venv/bin/pip install --no-deps --no-cache \
    -r requirements/test.txt

# run the actual test suite
echo "+++ Running test suite..."
chmod a+rwx ./ ./tests/
su sno -c "\
    /venv/bin/pytest \
    --verbose \
    -p no:sugar \
    --cov-report term \
    --cov-report html:coverage \
    --junit-xml=pytest.xml \
    -o cache_dir=/tmp/pytest_cache \
    --cache-clear"
