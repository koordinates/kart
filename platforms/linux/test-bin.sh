#!/bin/bash
set -eu

ALL=(
    ubuntu:bionic
    ubuntu:xenial
    ubuntu:rolling
    debian:oldoldstable-slim
    debian:oldstable-slim
    debian:stable-slim
    debian:testing-slim
    fedora:latest
    centos:7
    centos:8
)
HERE=$(dirname "$(readlink -f "$0")")

if [ $# -eq 0 ]; then
    TARGETS=${ALL[*]}
else
    TARGETS=$*
fi

for DIST in ${TARGETS[*]}; do
    echo "$DIST..."
    docker run \
        --rm \
        -v "${HERE}/dist/:/mnt/:ro" \
        "$DIST" \
        /mnt/kart/kart_cli --version \
    2>&1 | (while read -r; do echo "  $REPLY"; done)
done
