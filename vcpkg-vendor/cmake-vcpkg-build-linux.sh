#!/bin/bash
set -euo pipefail

#
# invoke via
#   myhost $ docker run -v /build -v /root -v /tmp -v $(pwd):/src -w /src --rm -it ubuntu:focal
#   mycontainer $ vcpkg-vendor/cmake-vcpkg-build-linux.sh [--verbose]

APT_DEPENDS=(
    autoconf
    build-essential
    cargo
    ccache
    curl
    git
    golang
    libodbc1
    libtool
    patchelf
    python3.10
    python3.10-dev
    libpython3.10
    libpython3.10-dev
    python3.10-venv
    python3-pip
    rustc
    tar
    unzip
    zip
)
PY_DEPENDS=(
    # cmake
    ninja
)
MIN_GOLANG_VERSION=1.17
CMAKE_VERSION=3.25.0

source /etc/os-release
ARCH=$(dpkg --print-architecture)
OSID="${ID}-${VERSION_ID}"

echo "OS: ${OSID}/${ARCH}"

echo "🌀  checking setup..."

if [ "${EUID:-$(id -u)}" -eq 0 ]; then
    SUDO=
else
    SUDO=sudo
fi

export DEBIAN_FRONTEND=noninteractive

if [ "$UBUNTU_CODENAME" != "jammy" ]; then
    $SUDO apt-get update -q -y
    $SUDO apt install -q -y --no-install-recommends software-properties-common
    $SUDO add-apt-repository -y ppa:deadsnakes/ppa
fi

if ! dpkg-query -f '${Package}\n' -W "${APT_DEPENDS[@]}" >/dev/null 2>&1; then
    echo "🌀  installing apt dependencies..."
    $SUDO apt-get update -q -y
    $SUDO apt-get install -q -y "${APT_DEPENDS[@]}"
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

# export CC=gcc
# export CXX=g++

echo "🌀  running kart cmake configuration..."
cmake -B /build -S . -DUSE_VCPKG=ON

echo "🌀  running kart cmake build..."
cmake --build /build --verbose

echo "🌀  running kart-bundle cmake build..."
cmake --build /build --target bundle --verbose

if [ $# -eq 0 ]; then
    echo "🌀  running cpack..."
    cd /build
    cpack -G "TGZ,DEB,RPM" --verbose
    mkdir -p "/src/${OSID}-${ARCH}"
    cp -v ./_CPack_Packages/*.{deb,rpm,tar.gz} "/src/${OSID}-${ARCH}/"
fi
