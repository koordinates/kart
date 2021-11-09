#!/bin/bash
set -eu

# parse `otool -l` output to find the minimum macOS deployment target
# for the specified mach-O binary.

TOKEN=
while read -r K V; do
    # echo "L=[$K][$V]"
    if [ "$K" == "cmd" ] && [ "$V" == "LC_BUILD_VERSION" ]; then
        TOKEN=minos
    elif [ "$K" == "cmd" ] && [ "$V" = "LC_VERSION_MIN_MACOSX" ]; then
        TOKEN=version
    elif [ "$K" = "$TOKEN" ]; then
        echo "$V"
        exit 0
    elif [ "$K" == "cmd" ]; then
        TOKEN=
    fi
done < <(otool -l "$1")

exit 1
