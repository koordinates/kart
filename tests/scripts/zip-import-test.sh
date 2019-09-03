#!/bin/bash
set -eu

#
# Given a ZIP file of GeoPackages (eg. from Koordinates),
# import them all into a single Snowdrop repository.
#

ZIP=$1
NAME="${ZIP%.*}"
NAME=${NAME%%-GPKG}

REPODIR="${NAME}.snow"
mkdir "$REPODIR"
snow init "$REPODIR"

TMPDIR="$(mktemp -q -d -t "${NAME}.XXXXXX")"

# Bail out if the temp directory wasn't created successfully.
if [ ! -e "$TMPDIR" ]; then
    >&2 echo "Failed to create temp directory"
    exit 1
fi
# Make sure it gets removed even if the script exits abnormally.
trap "exit 1"           HUP INT PIPE QUIT TERM
trap 'rm -rf "$TMPDIR"' EXIT

for GPKG_PATH in $(unzip -qql "$ZIP" "*.gpkg" | awk '{print $4}'); do
    GPKG_NAME=$(basename -- "$GPKG_PATH")
    GPKG_NAME=${GPKG_NAME%.*}
    TABLE=${GPKG_NAME//-/_}

    echo "*** $GPKG_NAME/$TABLE -> $TABLE"
    unzip -d "$TMPDIR" "$ZIP" "$GPKG_PATH"
    (
        cd "$REPODIR" \
        && time snow import --version=0.2.0 --x-method=fast "GPKG:${TMPDIR}/${GPKG_PATH}:${TABLE}" "${TABLE}"
    )
    rm "${TMPDIR}/${GPKG_PATH}"
done
