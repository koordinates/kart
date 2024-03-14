#!/bin/bash
set -euo pipefail

# check we're running via `source /path/todevenv.sh`
if ! (return 0 2>/dev/null); then
    echo "This script must be sourced, not executed."
    exit 1
fi

# find the path to the `python3` executable with respect to the location of this
# script. We're in a build tree, so it's effectively ../../build/venv/bin/python3
SCRIPT_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BUILD_PATH="${SCRIPT_PATH}/../../../build"

if [ "$(uname)" == "Darwin" ]; then
    export PYTHONSO="${BUILD_PATH}/venv/bin/python3"
elif [ "$(expr substr "$(uname -s)" 1 5)" == "Linux" ]; then
    PYTHONSO=$("${BUILD_PATH}/venv/bin/python3" -c 'from sysconfig import get_config_var; print("%s/%s" % (get_config_var("LIBDIR"), get_config_var("INSTSONAME")))')
    export PYTHONSO
else
    echo "Unsupported OS"
    exit 1
fi

echo "PYTHONSO library: ${PYTHONSO}"
TRIPLET=$(cmake -B build -L 2>/dev/null | grep VCPKG_TARGET_TRIPLET | awk -F= '{print $2}')
echo "VCPKG triplet: ${TRIPLET}"

export GDAL_PYTHON_DRIVER_PATH="${SCRIPT_PATH}"
export CPL_DEBUG=ON

# Add the Kart-vendored GDAL tools to the path
export PATH="${BUILD_PATH}/vcpkg_installed/${TRIPLET}/tools/gdal/:$PATH"

# Activate the virtualenv too
source "${BUILD_PATH}/venv/bin/activate"
