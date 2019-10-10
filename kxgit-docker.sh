#!/bin/bash

set -eu

docker run --rm -it \
    -v "${HOME}/.gitconfig:/home/sno/.gitconfig:ro" \
    -v "$(pwd):/data" \
    --tmpfs /tmp \
    sno \
    "$@"
