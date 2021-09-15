#!/bin/bash
set -e

source /root/.bashrc

set -xu

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

{ echo ">> Downgrading PyInstaller (https://github.com/pyinstaller/pyinstaller/issues/4674) ..."; } 2> /dev/null
pip install "pyinstaller==3.5.*"

python3 setup.py install

{ echo ">> Pre-bundle Smoke Test ..."; } 2> /dev/null
kart --version

{ echo ">> Running PyInstaller ..."; } 2> /dev/null
pyinstaller \
    --clean -y \
    --workpath platforms/linux/build/ \
    --distpath platforms/linux/dist/ \
    kart.spec

{ echo ">> Post-bundle Smoke Test ..."; } 2> /dev/null
platforms/linux/dist/kart/kart_cli --version
