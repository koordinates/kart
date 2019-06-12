#!/bin/bash

set -eu

docker run --rm -it \
    -v "${HOME}/.gitconfig:/home/snowdrop/.gitconfig:ro" \
    -v "$(pwd):/data" \
    snowdrop \
    "$@"
