#!/bin/bash
set -euo pipefail

#
# invoke via
#   myhost $ docker run -v /build -v /root -v /tmp -v $(pwd):/src -w /src --rm -it ubuntu:focal
#   mycontainer $ vcpkg-vendor/cmake-vcpkg-build-linux.sh [--verbose]
#
# manylinux images:
# - quay.io/pypa/manylinux_2_28_aarch64
# - quay.io/pypa/manylinux_2_28_x86_64
# - quay.io/pypa/manylinux2014_aarch64

PYVER=3.10
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
    "python${PYVER}"
    "python${PYVER}-dev"
    "libpython${PYVER}"
    "libpython${PYVER}-dev"
    "python${PYVER}-venv"
    python3-pip
    rpm
    rustc
    tar
    unzip
    zip
)
YUM_DEPENDS=(
    perl-IPC-Cmd
    rpm-build
    unixODBC
    zip
)
PY_DEPENDS=(
    # cmake
    ninja
)
MIN_GOLANG_VERSION=1.17
CMAKE_VERSION=3.25.0
PYTHON=python${PYVER}

source /etc/os-release
OSID="${ID}-${VERSION_ID}"

echo "🌀  checking setup..."

if [ "${EUID:-$(id -u)}" -eq 0 ]; then
    SUDO=
else
    SUDO=sudo
fi

if [ "${ID_LIKE}" == "debian" ]; then
    export DEBIAN_FRONTEND=noninteractive
    ARCH=$(dpkg --print-architecture)

    if [ "$UBUNTU_CODENAME" != "jammy" ]; then
        $SUDO apt-get update -q -y
        $SUDO apt install -q -y --no-install-recommends software-properties-common
        $SUDO add-apt-repository -y ppa:deadsnakes/ppa
    fi

    if [ "$UBUNTU_CODENAME" == "bionic" ]; then
        APT_DEPENDS+=('gcc-8' 'g++-8')
        export CC=gcc-8
        export CXX=g++-8
    fi

    if ! dpkg-query -f '${Package}\n' -W "${APT_DEPENDS[@]}" >/dev/null 2>&1; then
        echo "🌀  installing apt dependencies..."
        $SUDO apt-get update -q -y
        $SUDO apt-get install -q -y "${APT_DEPENDS[@]}"
    fi

    if [ "$UBUNTU_CODENAME" != "jammy" ]; then
        echo "🌀  installing pip..."
        curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
        ${PYTHON} /tmp/get-pip.py
    fi

else
    case "$(arch)" in
        "x86_64")
            ARCH=amd64
            ;;
        "aarch64")
            ARCH=arm64
            ;;
        *)
            echo "Unknown arch: $(arch)"
            exit 1
            ;;
    esac

    if command -v dnf >/dev/null; then
        YUM_DEPENDS+=('rustc' 'cargo' 'golang')
        NEED_RUST=0
    else
        NEED_RUST=1
    fi

    echo "🌀  installing yum dependencies..."
    yum install -y "${YUM_DEPENDS[@]}"

    if [ "$NEED_RUST" == 1 ]; then
        if [ ! -f $HOME/.cargo/bin/rustc ]; then
            echo "🌀  installing rustc..."
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs >/tmp/rustup
            sh /tmp/rustup -y --profile=minimal
        fi
        export PATH="$PATH:${HOME}/.cargo/bin"
    fi
fi

echo "🌀  OS: ${OSID}/${ARCH}"
gcc --version


PYTHON=$(realpath "$(command -v "$PYTHON")")
PYROOT=$(dirname "$(dirname "$PYTHON")")
PIP="${PYTHON} -m pip"
IS_MANYLINUX=$(test -n "${AUDITWHEEL_ARCH-}" && echo true || echo false)
PATH="$(dirname "$PYTHON"):${PATH}"
export PATH
ln -sf python3 "$(dirname "$PYTHON")/python"
echo "🌀  updated PATH=$PATH"

for P in "${PY_DEPENDS[@]}"; do
    if ! $PIP show --quiet "$P" >/dev/null 2>&1; then
        echo "🌀  installing python build tools..."
        $SUDO $PIP install "${PY_DEPENDS[@]}"
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

GOLANG_VERSION=$(go version | grep -oP "(\d+\.\d+\.\d+)" || echo "0")
if [ "$MIN_GOLANG_VERSION" != "$(echo -e "${MIN_GOLANG_VERSION}\\n${GOLANG_VERSION}" | sort -V | head -n1)" ]; then
    echo "🌀  installing newer golang... ${MIN_GOLANG_VERSION} > ${GOLANG_VERSION}"
    $SUDO curl -qL "https://go.dev/dl/go1.19.2.linux-$ARCH.tar.gz" | tar xz -C /usr/local/
    $SUDO ln -sf /usr/local/go/bin/go /usr/local/go/bin/gofmt /usr/local/bin/
fi

if [ ! -f "$(aclocal --print)/pkg.m4" ]; then
    echo "🌀  setting ACLOCAL_PATH..."
    ACLOCAL_PATH="$(aclocal --print):/usr/share/aclocal"
    export ACLOCAL_PATH
fi

if [ "$ARCH" == "arm64" ]; then
    # not sure why this is needed, but vcpkg is quite insistent
    export VCPKG_FORCE_SYSTEM_BINARIES=1
fi

if ! [ -f vcpkg-vendor/vcpkg/vcpkg ] || ! [[ "$(file vcpkg-vendor/vcpkg/vcpkg)" =~ "ELF" ]]; then
    echo "🌀  bootstrapping vcpkg..."
    vcpkg-vendor/vcpkg/bootstrap-vcpkg.sh
fi

echo "🌀  installing pkg-config via vcpkg..."
(cd /tmp && /src/vcpkg-vendor/vcpkg/vcpkg install pkgconf --overlay-triplets=/src/vcpkg-vendor/vcpkg-overlay-triplets --triplet=x64-linux)
export PKG_CONFIG=/src/vcpkg-vendor/vcpkg/installed/${ARCH}-linux/tools/pkgconf/pkgconf

BACKUP_LD_LIBRARY_PATH=$LD_LIBRARY_PATH
if [ -n "$LD_LIBRARY_PATH" ]; then
    echo "🌀  override LD_LIBRARY_PATH for vcpkg run..."
    export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/build/vcpkg_installed/${ARCH}-linux/lib
fi

if ! command -v dpkg >/dev/null 2>&1; then
    # no dpkg, but we need an architecture for our .deb files
    EXTRA_CMAKE_OPTIONS="-DCPACK_DEBIAN_PACKAGE_ARCHITECTURE=${ARCH}"
fi

echo "🌀  running kart cmake configuration..."
cmake -B /build -S . -DUSE_VCPKG=ON \
    -DPython3_EXECUTABLE=/build/vcpkg_installed/${ARCH}-linux/tools/python3/python${PYVER} \
    -DPython3_ROOT=/build/vcpkg_installed/${ARCH}-linux \
    -DPKG_CONFIG_EXECUTABLE=/src/vcpkg-vendor/vcpkg/installed/${ARCH}-linux/tools/pkgconf/pkgconf \
    ${EXTRA_CMAKE_OPTIONS-}

export LD_LIBRARY_PATH=${BACKUP_LD_LIBRARY_PATH}

echo "🌀  running kart cmake build..."
cmake --build /build --verbose

echo "🌀  running kart-bundle cmake build..."
cmake --build /build --target bundle --verbose

if [ $# -eq 0 ]; then
    echo "🌀  running cpack..."
    cd /build
    mkdir -p "/src/${OSID}-${ARCH}"
    cpack -G "TGZ;DEB;RPM"
    cp -v ./*.{deb,rpm,tar.gz} "/src/${OSID}-${ARCH}/"
fi
