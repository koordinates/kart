#!/bin/bash
set -euo pipefail

#
# invoke via
#   myhost $ docker run -v /tmp -v $(pwd):/src -w /src --rm -it ubuntu:focal
#   mycontainer $ vcpkg-vendor/cmake-vcpkg-build-linux.sh [--verbose]

APT_DEPENDS=(
    autoconf
    build-essential
    ccache
    curl
    git
    golang
    libtool
    patchelf
    python3-pip
    python3-venv
    tar
    unzip
    zip
)
PY_DEPENDS=(
    # cmake
    ninja
)
MIN_GOLANG_VERSION=1.17
CMAKE_VERSION=3.25.0-rc2

ARCH=$(dpkg --print-architecture)

echo "🌀  checking setup..."

if [ "${EUID:-$(id -u)}" -eq 0 ]; then
    SUDO=
else
    SUDO=sudo
fi

if ! dpkg-query -f '${Package}\n' -W "${APT_DEPENDS[@]}" >/dev/null 2>&1; then
    echo "🌀  installing apt dependencies..."
    export DEBIAN_FRONTEND=noninteractive
    $SUDO apt-get update
    $SUDO apt-get install -y "${APT_DEPENDS[@]}"
fi

for P in "${PY_DEPENDS[@]}"; do
    if ! pip show --quiet "$P" >/dev/null 2>&1; then
        echo "🌀  installing python build tools..."
        $SUDO pip install "${PY_DEPENDS[@]}"
        # why are these needed? maybe if tmpfs is noexec?
        $SUDO chmod +x /usr/local/lib/python3.*/dist-packages/cmake/data/bin/cmake || true
        $SUDO chmod +x /usr/local/lib/python3.*/dist-packages/ninja/data/bin/ninja || true
        break
    fi
done

if ! command -v cmake >/dev/null; then
    echo "🌀  installing newer cmake..."
    curl -fL https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-$(arch).sh > /tmp/cmake-install-linux.sh
    $SUDO bash /tmp/cmake-install-linux.sh --skip-license --prefix=/usr/local
fi

GOLANG_VERSION=$(go version | grep -oP "(\d+\.\d+\.\d+)")
if dpkg --compare-versions "$GOLANG_VERSION" lt "$MIN_GOLANG_VERSION"; then
    echo "🌀  installing newer golang..."
    $SUDO curl -qL "https://go.dev/dl/go1.19.2.linux-$ARCH.tar.gz" | tar xz -C /usr/local/
    $SUDO ln -sf /usr/local/go/bin/go /usr/local/go/bin/gofmt /usr/local/bin/
fi

if ! [ -f vcpkg-vendor/vcpkg/vcpkg ] || ! [[ "$(file vcpkg-vendor/vcpkg/vcpkg)" =~ "ELF" ]]; then
    echo "🌀  bootstrapping vcpkg..."
    vcpkg-vendor/vcpkg/bootstrap-vcpkg.sh
fi

echo "🌀  running kart cmake configuration..."

if [ "$ARCH" == "arm64" ]; then
    # not sure why this is needed, but vcpkg is quite insistent
    export VCPKG_FORCE_SYSTEM_BINARIES=1
fi

CC=gcc CXX=g++ \
cmake -B /build -S . -DUSE_VCPKG=ON

echo "🌀  running kart cmake build..."
cmake --build /build "$@"
