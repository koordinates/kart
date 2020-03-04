#!/bin/bash
set -eu

OUTPUT=$1
shift

yum install -y cmake3 ccache openssl-devel gettext
export CMAKE=cmake3

export PATH=/opt/python/cp37-cp37m/bin:${PATH}

# setup ccache
echo ">>> Setting up ccache ..."
if [ -n "$CCACHE_DIR" ]; then
    for F in c++ cc cpp g++ gcc i686-redhat-linux-gcc-8 x86_64-redhat-linux-{c++,g++,gcc,gcc-8}; do
        ln -s /usr/bin/ccache /usr/lib64/ccache/$F
    done
    export PATH=/usr/lib64/ccache:${PATH}
fi

echo ">>> Building patched patchelf"
# https://github.com/pypa/auditwheel/issues/159
mkdir /patchelf
curl -sL https://github.com/nvictus/patchelf/archive/d7483d92cfd614e06839c18d2fa194b88ff777a2.tar.gz | tar xz -C /patchelf --strip-components=1
pushd /patchelf
./bootstrap.sh
./configure
make
make install
popd

echo ">>> Python: $(command -v python3.7)"

echo ">>> Setting up /build ..."
mkdir /build
for M in Makefile */Makefile; do
    D=$(dirname "$M")
    mkdir -p "/build/$D"
    cp -v "$M" "/build/$M"
    find "$D" -maxdepth 1 \( -name "*.tar.*" -o -name "*.zip" \) -print -exec ln -s "$(pwd)"/{} "/build/$D/" \;
done

cd /build
if [ $# -gt 0 ]; then
    exec "$@"
else
    mkdir -p "$OUTPUT/wheelhouse" "$OUTPUT"/env/{bin,share,lib,libexec}

    echo ">>> Building Git ..."
    make git
    cp -farv env/bin/git "$OUTPUT/env/bin/"
    cp -farv env/lib/libcurl.*so* "$OUTPUT/env/lib/"
    cp -farv env/share/git-core "$OUTPUT/env/share/"
    cp -farv env/libexec/git-core "$OUTPUT/env/libexec/"

    echo ">>> Building GDAL ..."
    make gdal-wheel
    cp -fv gdal/wheelhouse/GDAL-*.whl "$OUTPUT/wheelhouse/"
    cp -vafr env/share/gdal "$OUTPUT/env/share/"
    cp -vafr env/share/proj "$OUTPUT/env/share/"

    echo ">>> Building PyGit2 ..."
    make pygit2-wheel
    cp -fv pygit2/wheelhouse/pygit2-*.whl "$OUTPUT/wheelhouse"

    echo ">>> Building spatialite ..."
    make spatialite
    cp -fvL spatialite/src/src/.libs/mod_spatialite.so "$OUTPUT/env/lib/"
    # FIXME: Use the GDAL auditwheel libraries
    patchelf --remove-rpath "$OUTPUT/env/lib/mod_spatialite.so"
    patchelf --force-rpath --set-rpath "\$ORIGIN/." "$OUTPUT/env/lib/mod_spatialite.so"
    cp -vfa env/lib/libsqlite3.so* "$OUTPUT/env/lib/"
    cp -vfa env/lib/libproj.so* "$OUTPUT/env/lib/"
    cp -vfa env/lib/libgeos_c.so* "$OUTPUT/env/lib/"
    cp -vfa env/lib/libgeos-*.so "$OUTPUT/env/lib/"

    echo ">>> Building spatialindex ..."
    make spatialindex
    cp -vfL env/lib/libspatialindex_c.so "$OUTPUT/env/lib/"
    cp -vfL env/lib/libspatialindex.so.6 "$OUTPUT/env/lib/"
fi
