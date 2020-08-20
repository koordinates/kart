#!/bin/bash
set -eu

TYPE=$1
VERSION=$2
WORKDIR=/tmp/root

mkdir -p ${WORKDIR}/{opt,usr/bin}
cp -r dist/sno ${WORKDIR}/opt

# reset file permissions, PyInstaller gets over-excited
find ${WORKDIR} -maxdepth 1 -type f -not -name sno_cli -exec chmod -x {} \;

# symlink executable
ln -sf /opt/sno/sno_cli ${WORKDIR}/usr/bin/sno

OPTS=
if [ "$TYPE" = "deb" ]; then
    OPTS+="--depends openssh-client"
elif [ "$TYPE" = "rpm" ]; then
    OPTS+="--depends openssh-clients"
fi

# build package
fpm \
    --verbose \
    --input-type dir \
    --chdir ${WORKDIR} \
    --output-type "${TYPE}" \
    --name sno \
    --version "${VERSION}" \
    --url "https://sno.earth" \
    --license "GPLv2" \
    --architecture amd64 \
    --package /src/dist/ \
    --force \
    $OPTS \
    .
