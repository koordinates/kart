#!/bin/bash
set -e

source /root/.bashrc

set -xu

export PY_VERSION=3.7
export PY_SITEPACKAGES="lib/python$PY_VERSION/site-packages"
export VIRTUAL_ENV=/venv

export PATH=/opt/python/cp37-cp37m-shared/bin:${PATH}

python3 --version
python3 -m venv /venv

export PATH=/venv/bin:${PATH}

{ echo ">> Configuring vendor dependencies..."; } 2> /dev/null
tar xvzf vendor/dist/vendor-Linux.tar.gz -C /tmp wheelhouse
rm -rf vendor/dist/env/
tar xzf vendor/dist/vendor-Linux.tar.gz -C vendor/dist/ env
tar xzf vendor/dist/vendor-Linux.tar.gz -C /venv --strip-components=1 env/lib/

# get the Rtree installer working successfully
export SPATIALINDEX_C_LIBRARY="/venv/lib/libspatialindex_c.so"

pip install --no-deps --ignore-installed -r requirements.txt
pip install --no-deps \
    /tmp/wheelhouse/*.whl

pip install "pyinstaller==5.3.*"
# disable the pyodbc hook. TODO: We can override it in PyInstaller 4.x
echo "disable pyodbc hook"
rm "$VIRTUAL_ENV/$PY_SITEPACKAGES/_pyinstaller_hooks_contrib/hooks/stdhooks/hook-pyodbc.py"

python3 setup.py install

{ echo ">> Pre-bundle Smoke Test ..."; } 2> /dev/null
kart --version

{ echo ">> Running PyInstaller ..."; } 2> /dev/null
pyinstaller \
    --clean -y \
    --workpath platforms/linux/build/ \
    --distpath platforms/linux/dist/ \
    kart.spec

# # fix up .so files which should be symlinks
VENDOR_LIB=/src/vendor/dist/env/lib/
(cd platforms/linux/dist/kart/ \
    && for library in `ls *.so*`; do 
    if [ -e $VENDOR_LIB/$library ] ; then 
        if [ -L $VENDOR_LIB/$library ]; then 
            ln -sf `readlink $VENDOR_LIB/$library` $library
        else 
            strip $library; 
        fi; 
    fi; 
done)

{ echo ">> Post-bundle Smoke Test ..."; } 2> /dev/null
platforms/linux/dist/kart/kart_cli --version
