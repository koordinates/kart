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

TMP_PATH=$(mktemp -q -d -t "kart-e2e.XXXXXX")
echo "Using temp folder: ${TMP_PATH}"

function do_cleanup {
    rm -rf "$TMP_PATH"
}
trap do_cleanup EXIT

KART_PATH=$(dirname "$(realpath "$(command -v kart)")")
echo "Kart is at: ${KART_PATH}"

mkdir "${TMP_PATH}/test"
cd "${TMP_PATH}/test"
set -x

kart init --initial-branch=main .
kart config user.name "Kart E2E Test 1"
kart config user.email "kart-e2e-test-1@email.invalid"
kart import "GPKG:${TEST_GPKG}" mylayer

kart log
kart checkout
kart switch -c edit-1
sqlite3 --bail test.gpkg "
  SELECT load_extension('${KART_PATH}/mod_spatialite');
  SELECT EnableGpkgMode();
  INSERT INTO mylayer (fid, geom) VALUES (999, GeomFromEWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));
"
kart status
kart diff --crs=EPSG:3857
kart commit -m my-commit
kart switch main
kart status
kart merge edit-1 --no-ff -m merge-1
kart log

{ echo -e "\n✅ E2E: Success"; } 2>/dev/null
