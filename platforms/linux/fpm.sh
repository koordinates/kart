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
ln -sf /opt/kart/kart_cli_helper ${WORKDIR}/usr/bin/kart
ln -sf /opt/kart/kart_cli ${WORKDIR}/usr/bin/kart_cli
ln -sf kart ${WORKDIR}/usr/bin/sno  # Previous name


OPTS=
if [ "$TYPE" = "deb" ]; then
    OPTS+="--depends openssh-client "
    OPTS+="--depends libstdc++6 "
    OPTS+="--depends libgcc1 "
    OPTS+="--depends libbz2-1.0 "
    OPTS+="--depends libexpat1 "
    OPTS+="--depends liblzma5 "
    OPTS+="--depends libstdc++6 "
    OPTS+="--depends libtinfo5 "
    OPTS+="--depends zlib1g "
    OPTS+="--deb-recommends libodbc1 "
    OPTS+="--deb-recommends odbcinst "
elif [ "$TYPE" = "rpm" ]; then
    # Weak dependencies are new in RPM, and not supported by FPM yet.
    OPTS+="--depends openssh-clients "
    OPTS+="--depends libstdc++ "
    OPTS+="--depends libgcc "
    OPTS+="--depends bzip2-libs "
    OPTS+="--depends expat "
    OPTS+="--depends xz-libs "
    OPTS+="--depends libstdc++ "
    OPTS+="--depends ncurses-libs "
    OPTS+="--depends zlib "
fi

# build package
fpm \
    --verbose \
    --input-type dir \
    --chdir ${WORKDIR} \
    --output-type "${TYPE}" \
    --name kart \
    --version "${VERSION}" \
    --url "https://kartproject.org" \
    --description "Distributed version-control for geospatial and tabular data" \
    --license "GPLv2" \
    --conflicts "sno" \
    --replaces "sno" \
    --architecture amd64 \
    --package /src/dist/ \
    --force \
    $OPTS \
    .
