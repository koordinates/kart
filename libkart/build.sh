#!/usr/bin/env bash
set -eu

echo "🪜  running cmake for libkart..."
cmake -B build -S .
echo "🪜  building libkart & cppgit2..."
cmake --build build

echo "🪜  building cython libkart module..."
# TODO : fix this
touch --no-create bindings/python/libkart.pyx
python3 -m pip install cython
cd bindings/python
python3 setup.py develop

echo "🙌 done"
