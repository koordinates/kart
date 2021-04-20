#!/bin/bash
set -eu

if ! command -v realpath >/dev/null; then
    # MacOS doesn't have realpath or readlink -f by default
    function realpath() {
        python -c 'import os, sys; print os.path.realpath(sys.argv[1])' "$1"
    }
fi

KART_PATH=$(dirname "$(realpath "$(command -v kart)")")
if [ "$(uname)" = "Darwin" ] && [[ "$KART_PATH" =~ ^/Applications ]]; then
    KART_PATH="$(realpath "$KART_PATH/../..")"
fi
echo "Kart is at: ${KART_PATH}"

SNO_PATH=$(realpath "$(command -v sno)")
echo "Found Sno at: ${SNO_PATH}"

if ! command -v find >/dev/null; then
    echo "⚠️ Skipping symlink checks, find isn't available"
else
    echo "Checking for any broken symlinks..."
    BROKEN_LINKS=($(find "$KART_PATH" -type l ! -exec test -e {} \; -print))
    if (( ${#BROKEN_LINKS[@]} )); then
        ls -l "${BROKEN_LINKS[@]}"
        exit 1
    fi
fi

echo "✅ Success"
