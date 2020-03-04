#!/bin/bash
set -eu

HERE=$(dirname "$(readlink -f "$0")")
TEST_GPKG=${1-${HERE}/../data/e2e.gpkg}

TMP_PATH=$(mktemp -q -d -t "sno-e2e.XXXXXX")

function cleanup {
    rm -rf "$TMP_PATH"
}
trap cleanup EXIT

SNO_PATH=$(dirname "$(realpath "$(command -v sno)")")

mkdir "${TMP_PATH}/test"
cd "${TMP_PATH}/test"
set -x

sno init .
sno config user.name "Sno E2E Test 1"
sno config user.email "sno-e2e-test-1@email.invalid"
sno import "GPKG:${TEST_GPKG}:mylayer"

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
