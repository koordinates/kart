#!/bin/bash
set -eu

TYPE=$1
VERSION=$2
WORKDIR=/tmp/root

mkdir -p ${WORKDIR}/{opt,usr/bin}
cp -r dist/kart ${WORKDIR}/opt

# reset file permissions, PyInstaller gets over-excited
find ${WORKDIR} -maxdepth 1 -type f -not -name kart_cli -exec chmod -x {} \;

# symlink executable
ln -sf /opt/kart/kart_cli ${WORKDIR}/usr/bin/kart
ln -sf kart ${WORKDIR}/usr/bin/sno  # Previous name


OPTS=
if [ "$TYPE" = "deb" ]; then
    OPTS+="--depends openssh-client "
    OPTS+="--depends libstdc++6 "
    OPTS+="--depends libgcc1 "
    OPTS+="--deb-recommends libodbc1 "
    OPTS+="--deb-recommends odbcinst "
elif [ "$TYPE" = "rpm" ]; then
    # Weak dependencies are new in RPM, and not supported by FPM yet.
    OPTS+="--depends openssh-clients "
    OPTS+="--depends libstdc++ "
    OPTS+="--depends libgcc "
fi

# build package
fpm \
    --verbose \
    --input-type dir \
    --chdir ${WORKDIR} \
    --output-type "${TYPE}" \
    --name kart \
    --version "${VERSION}" \
    --url "https://www.kartproject.org" \
    --description "Distributed version-control for geospatial and tabular data" \
    --license "GPLv2" \
    --conflicts "sno" \
    --replaces "sno" \
    --architecture amd64 \
    --package /src/dist/ \
    --force \
    $OPTS \
    .
