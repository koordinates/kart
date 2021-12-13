#!/bin/bash
set -eu

OUTPUT=$1
shift

yum install -y epel-release

yum install -y ccache openssl-devel gettext wget unixODBC-devel expat-devel

export PATH=/opt/python/cp37-cp37m-shared/bin:${PATH}
export LD_LIBRARY_PATH=/build/env/lib:${LD_LIBRARY_PATH}

# setup ccache
echo ">>> Setting up ccache ..."
if [ -n "$CCACHE_DIR" ]; then
    for F in c++ cc cpp g++ gcc i686-redhat-linux-gcc-8 x86_64-redhat-linux-{c++,g++,gcc,gcc-8}; do
        ln -s /usr/bin/ccache /usr/lib64/ccache/$F
    done
    export PATH=/usr/lib64/ccache:${PATH}
fi

echo ">>> Python: $(command -v python3.7)"

echo ">>> Setting up /build ..."
ln -s "$(pwd)/Makefile" "$(pwd)/linux-delocate-deps.py" /build/
for M in */Makefile; do
    D=$(dirname "$M")
    mkdir -p "/build/$D"
    ln -s "$(pwd)/$M" "/build/$M"
    find "$D" -maxdepth 1 \( -name "*.tar.*" -o -name "*.zip" \) -print -exec ln -s "$(pwd)"/{} "/build/$D/" \;
done

# Vendor builds that have more than a simple Makefile as a basis.
# sqlite
cp -v sqlite/version.mk /build/sqlite/
# spatial-filter
cp -v spatial-filter/*.{h,c,cpp} /build/spatial-filter/

cd /build
if [ $# -gt 0 ]; then
    exec "$@"
else
    mkdir -p "$OUTPUT/wheelhouse" "$OUTPUT"/env/{bin,share/git-core,lib,libexec}

    echo ">>> Building Git ..."
    make lib-git
    cp -fav env/bin/git "$OUTPUT/env/bin/"
    cp -fav env/share/git-core/templates "$OUTPUT/env/share/git-core/"
    cp -fav env/libexec/git-core "$OUTPUT/env/libexec/"

    echo ">>> Building GDAL ..."
    make lib-gdal
    cp -fav gdal/wheel/GDAL-*.whl "$OUTPUT/wheelhouse/"
    cp -fav env/share/gdal "$OUTPUT/env/share/"
    cp -fav env/share/proj "$OUTPUT/env/share/"

    echo ">>> Building PyGit2 ..."
    make lib-pygit2
    cp -fav pygit2/wheel/pygit2-*.whl "$OUTPUT/wheelhouse"

    echo ">>> Building spatialite ..."
    make lib-spatialite

    echo ">>> Building spatialindex ..."
    make lib-spatialindex

    echo ">>> Building pysqlite3 ..."
    make lib-pysqlite3
    cp -fav pysqlite3/wheel/pysqlite3-*.whl "$OUTPUT/wheelhouse"

    echo ">>> Building pyodbc ..."
    make lib-pyodbc
    cp -fav pyodbc/wheel/pyodbc-*.whl "$OUTPUT/wheelhouse"

    echo ">>> Building psycopg2 ..."
    make lib-psycopg2
    cp -fav psycopg2/wheel/psycopg2-*.whl "$OUTPUT/wheelhouse"

    echo ">>> Bundling libraries ..."
    env/bin/python3 ./linux-delocate-deps.py env/lib/
    cp -fav env/lib/*.so* "$OUTPUT/env/lib/"

    echo ">>> CCache Stats:"
    ccache --show-stats

fi
