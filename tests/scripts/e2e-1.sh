#!/bin/bash
set -eu

if ! command -v realpath >/dev/null; then
    # MacOS doesn't have realpath or readlink -f by default
    function realpath() {
        python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$1"
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
export KART_HELPER_LOG=${TMP_PATH}/kart-helper.log

function do_cleanup {
    if [ -f "$KART_HELPER_LOG" ]; then
        cat "$KART_HELPER_LOG"
    fi
    if [ -z "${NO_CLEANUP-}" ]; then
        rm -rf "$TMP_PATH"
    fi
}
trap do_cleanup EXIT

mkdir -p "${TMP_PATH}/home"
export HOME="${TMP_PATH}/home"
if [ "$(uname -s)" == "Linux" ]; then
    cp -a /etc/skel/. "$HOME/"
fi
export SHELL=/bin/bash
export PAGER=cat

KART_PATH=$(dirname "$(realpath "$(command -v kart)")")
echo "Kart is at: ${KART_PATH}"

SQLITE3_PATH=$(dirname "$(realpath "$(command -v sqlite3)")")
echo "sqlite3 is at: ${SQLITE3_PATH}"

SPATIALITE_PATH=$(echo 'from kart import spatialite_path; print(spatialite_path)' | kart --post-mortem 2>/dev/null | grep spatialite | awk '{print $2}')
echo "Spatialite is at: ${SPATIALITE_PATH}"

mkdir "${TMP_PATH}/test"
cd "${TMP_PATH}/test"
set -x

echo "Using helper mode: ${KART_USE_HELPER:-?}"

kart -vvvv install tab-completion --shell auto
# This checks our tab-completion works with _KART_COMPLETE (and not _KART_CLI_COMPLETE)
COMP_WORDS="kart sta" COMP_CWORD=1 _KART_COMPLETE=bash_complete kart sta

kart init --initial-branch=main .
kart config user.name "Kart E2E Test 1"
kart config user.email "kart-e2e-test-1@email.invalid"
kart import "GPKG:${TEST_GPKG}" mylayer

kart log
kart checkout
kart switch -c edit-1
sqlite3 --bail test.gpkg "
  PRAGMA trusted_schema=1;
  SELECT load_extension('${SPATIALITE_PATH}');
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

# Briefly try a remote to ensure the CA cert bundle is working
kart git ls-remote https://github.com/koordinates/kart.git HEAD

# ext-run error handling
# this should work consistently with/without helper
kart ext-run "$HERE/ext-run-test.py" 0
if kart ext-run "$HERE/ext-run-test.py" 1; then
    echo "ext-run-failure.py expected to exit with 1; got $?"
    exit 1
fi
if kart ext-run "$HERE/ext-run-test.py" throw; then
    echo "ext-run-failure.py expected to exit with 1 via exception; got $?"
    exit 1
fi

{ echo -e "\n✅ E2E: Success"; } 2>/dev/null
