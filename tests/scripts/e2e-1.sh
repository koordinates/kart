#!/bin/bash
set -eu

if ! command -v realpath >/dev/null; then
    # MacOS doesn't have realpath or readlink -f by default
    function realpath() {
        python -c 'import os, sys; print os.path.realpath(sys.argv[1])' "$1"
    }
fi

function do_error {
    { echo -e "\n‼️ E2E: Error"; } 2>/dev/null
}
trap do_error ERR

HERE=$(dirname "$(realpath "$0")")
TEST_GPKG=${1-${HERE}/../data/e2e.gpkg}
echo "Test data is at: ${TEST_GPKG}"

TMP_PATH=$(mktemp -q -d -t "sno-e2e.XXXXXX")
echo "Using temp folder: ${TMP_PATH}"

function do_cleanup {
    rm -rf "$TMP_PATH"
}
trap do_cleanup EXIT

SNO_PATH=$(dirname "$(realpath "$(command -v sno)")")
echo "Sno is at: ${SNO_PATH}"

mkdir "${TMP_PATH}/test"
cd "${TMP_PATH}/test"
set -x

sno init .
sno config user.name "Sno E2E Test 1"
sno config user.email "sno-e2e-test-1@email.invalid"
sno import "GPKG:${TEST_GPKG}" --table=mylayer

sno log
sno checkout
sno switch -c edit-1
sqlite3 --bail test.gpkg "
  SELECT load_extension('${SNO_PATH}/mod_spatialite');
  SELECT EnableGpkgMode();
  INSERT INTO mylayer (fid, geom) VALUES (999, GeomFromEWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));
"
sno status
sno diff
sno commit -m my-commit
sno switch master
sno status
sno merge edit-1 --no-ff
sno log

{ echo -e "\n✅ E2E: Success"; } 2>/dev/null
