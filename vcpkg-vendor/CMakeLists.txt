cmake_minimum_required(VERSION 3.25)

include(${CMAKE_CURRENT_LIST_DIR}/vcpkg-toolchain-setup.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/osx-setup.cmake)

project(kart_vendor LANGUAGES C CXX)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake/)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/)

set(CURRENT_PACKAGES_DIR "${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}")

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  execute_process(
    COMMAND brew --prefix
    RESULT_VARIABLE BREW_PREFIX_RETURN
    OUTPUT_VARIABLE BREW_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(BREW_PREFIX_RETURN EQUAL 0)
    message(STATUS "Found Homebrew at: ${BREW_PREFIX}")
  endif()

  find_package(PkgConfig)
  if(NOT PKG_CONFIG_FOUND)
    set(PKG_CONFIG_EXECUTABLE "${BREW_PREFIX}/pkg-config")
  endif()
endif()
find_package(PkgConfig REQUIRED)

cmake_policy(SET CMP0135 NEW)
set(CMAKE_TLS_VERIFY TRUE)

include(ExternalProject)

include(debug_helpers)

find_package(Git REQUIRED)

set(WHEEL_LIST "")
set(LIB_LIST "")
set(EXE_LIST "")

#
# Python
#
# Need Development here or VcPkg falls back to the system interpreter
find_package(
  Python3 3.9
  COMPONENTS Development Interpreter
  REQUIRED)
cmake_path(GET Python3_STDLIB FILENAME Python3_STDLIB_DIR_NAME)
cmake_path(GET Python3_INCLUDE_DIRS FILENAME Python3_INCLUDE_DIR_NAME)

include(PythonGetABIInfo)
pythongetabiinfo()

include(PyCreateVirtualEnvironment)

# ##################################################################################################
# PYTHON WHEELS
# ##################################################################################################

#
# Wheel build environment
#

createvirtualenvironment(wheelBuildEnv REQUIREMENTS "wheel")

# Note that the vcpkg_installed include and library dirs can also be found by directly querying the
# location of a library eg `get_target_property(GDAL_LIBRARY GDAL::GDAL LOCATION)` but in practice
# they all resolve to the same place. Setting it up just once for all the wheels saves us some
# complexity.

# Get the Python include dir: TODO - find out why setuptools doesn't add the python include dir to
# the include path automatically
execute_process(
  COMMAND ${wheelBuildEnv_PYTHON} -c "import sysconfig; print(sysconfig.get_path('include'))"
  OUTPUT_VARIABLE BUILD_WHEEL_INCLUDE_DIRS
  OUTPUT_STRIP_TRAILING_WHITESPACE)

# Add the vcpkg_installed include dir:
list(APPEND BUILD_WHEEL_INCLUDE_DIRS "${CURRENT_PACKAGES_DIR}/include")

# Get the Python stdlib dir: TODO - find out why setuptools doesn't add the python library dir to
# the library path automatically
execute_process(
  COMMAND ${wheelBuildEnv_PYTHON} -c "import sysconfig; print(sysconfig.get_path('stdlib'))"
  OUTPUT_VARIABLE BUILD_WHEEL_LIBRARY_DIRS
  OUTPUT_STRIP_TRAILING_WHITESPACE)

# Add the vcpkg_installed library dir:
list(APPEND BUILD_WHEEL_LIBRARY_DIRS "${CURRENT_PACKAGES_DIR}/lib")
if(WIN32)
  # On Windows, the DLLs are found in the bin dir:
  list(APPEND BUILD_WHEEL_LIBRARY_DIRS "${CURRENT_PACKAGES_DIR}/bin")
endif()

# Different path-list separators on different platforms: TODO: can we use cmake_path(CONVERT ...
# TO_NATIVE_PATH_LIST ...) here?
if(NOT WIN32)
  string(REPLACE ";" ":" BUILD_WHEEL_INCLUDE_DIRS "${BUILD_WHEEL_INCLUDE_DIRS}")
  string(REPLACE ";" ":" BUILD_WHEEL_LIBRARY_DIRS "${BUILD_WHEEL_LIBRARY_DIRS}")
endif()

#
# cffi wheel
#
find_package(unofficial-libffi CONFIG REQUIRED)

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cffi-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${BUILD_WHEEL_INCLUDE_DIRS}
library_dirs=${BUILD_WHEEL_LIBRARY_DIRS}
$<$<NOT:$<PLATFORM_ID:Windows>>:rpath=${CURRENT_PACKAGES_DIR}/lib>
")

set(CFFI_WHEEL_VER 1.16.0)
ExternalProject_Add(
  cffi
  URL https://files.pythonhosted.org/packages/68/ce/95b0bae7968c65473e1298efb042e10cafc7bafc14d9e4f154008241c91d/cffi-1.16.0.tar.gz
  URL_HASH SHA256=bcb3ef43e58665bbda2fb198698fcae6776483e0c4a631aa5647806c25e02cc0
  DOWNLOAD_NO_PROGRESS ON
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv unofficial::libffi::libffi
  EXCLUDE_FROM_ALL ON
  BUILD_BYPRODUCTS "<SOURCE_DIR>/build"
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/cffi-prefix/setup.cfg
                    <SOURCE_DIR>
  BUILD_COMMAND ${CMAKE_COMMAND} -E rm -rf <INSTALL_DIR>/*.whl
  COMMAND ${wheelBuildEnv_PYTHON} -m build --wheel --outdir <TMP_DIR>/dist
  INSTALL_COMMAND ${CMAKE_COMMAND} -E copy
                  <TMP_DIR>/dist/cffi-${CFFI_WHEEL_VER}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(cffi INSTALL_DIR)
set(CFFI_WHEEL ${INSTALL_DIR}/cffi-${CFFI_WHEEL_VER}-${Python3_WHEEL_ID}.whl)
list(APPEND WHEEL_LIST ${CFFI_WHEEL})

#
# gdal_py wheel We download the bindings associated with the version we picked up via vcpkg/FindGDAL
#
find_package(GDAL REQUIRED)

if(WIN32)
  set(GDAL_CONFIG_EXE "")
else()
  set(GDAL_CONFIG_EXE "${CURRENT_PACKAGES_DIR}/tools/gdal/gdal-config")
endif()

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/gdal_py-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${BUILD_WHEEL_INCLUDE_DIRS}
library_dirs=${BUILD_WHEEL_LIBRARY_DIRS}
libraries=gdal
gdal_config=${CURRENT_PACKAGES_DIR}/tools/gdal/gdal-config
")

message(STATUS "GDAL: Will download & build Python bindings for v${GDAL_VERSION}")

if(MACOS)
  # yucky patch otherwise compiling ends up with an extra empty path on the end of the include &
  # library dir lists, causing the python module to not actually link to libgdal. Upstream
  # https://github.com/OSGeo/gdal/pull/7032
  file(
    GENERATE
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/gdal_py-prefix/patch_setup_py.sh
    CONTENT
      "if ! patch --reverse --silent --force --dry-run -i \"${CMAKE_CURRENT_SOURCE_DIR}/gdal__setup.py.patch\" >/dev/null 2>&1
then
  patch -i \"${CMAKE_CURRENT_SOURCE_DIR}/gdal__setup.py.patch\"
fi
")
  set(GDAL_PATCH_COMMAND bash ${CMAKE_CURRENT_BINARY_DIR}/gdal_py-prefix/patch_setup_py.sh)
endif()

ExternalProject_Add(
  gdal_py
  URL https://pypi.org/packages/source/G/GDAL/GDAL-${GDAL_VERSION}.tar.gz
  DOWNLOAD_NO_PROGRESS ON
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv GDAL::GDAL
  EXCLUDE_FROM_ALL ON
  BUILD_BYPRODUCTS "<SOURCE_DIR>/build"
  PATCH_COMMAND ${GDAL_PATCH_COMMAND}
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/gdal_py-prefix/setup.cfg
                    <SOURCE_DIR>
  BUILD_COMMAND ${CMAKE_COMMAND} -E rm -rf <INSTALL_DIR>/*.whl
  COMMAND ${wheelBuildEnv_PYTHON} -m build --wheel --outdir <TMP_DIR>/dist
  INSTALL_COMMAND ${CMAKE_COMMAND} -E copy
                  <TMP_DIR>/dist/gdal-${GDAL_VERSION}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(gdal_py INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/gdal-${GDAL_VERSION}-${Python3_WHEEL_ID}.whl)

#
# psycopg2 wheel
#
find_package(PostgreSQL REQUIRED)

if(WIN32)
  set(PG_CONFIG_EXE "${CURRENT_PACKAGES_DIR}/tools/libpq/pg_config.exe")
else()
  set(PG_CONFIG_EXE "${CURRENT_PACKAGES_DIR}/tools/libpq/bin/pg_config")
endif()

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/psycopg2-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${BUILD_WHEEL_INCLUDE_DIRS}
library_dirs=${BUILD_WHEEL_LIBRARY_DIRS}
pg_config=${PG_CONFIG_EXE}
define=
")

set(PYSCOPG2_WHEEL_VER 2.9.9)
ExternalProject_Add(
  psycopg2
  URL https://files.pythonhosted.org/packages/c9/5e/dc6acaf46d78979d6b03458b7a1618a68e152a6776fce95daac5e0f0301b/psycopg2-2.9.9.tar.gz
  URL_HASH SHA256=d1454bde93fb1e224166811694d600e746430c006fbb031ea06ecc2ea41bf156
  DOWNLOAD_NO_PROGRESS ON
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv PostgreSQL::PostgreSQL
  EXCLUDE_FROM_ALL ON
  BUILD_BYPRODUCTS "<SOURCE_DIR>/build"
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/psycopg2-prefix/setup.cfg
                    <SOURCE_DIR>
  BUILD_COMMAND ${CMAKE_COMMAND} -E rm -rf <INSTALL_DIR>/*.whl
  COMMAND ${wheelBuildEnv_PYTHON} -m build --wheel --outdir <TMP_DIR>/dist
  INSTALL_COMMAND
    ${CMAKE_COMMAND} -E copy <TMP_DIR>/dist/psycopg2-${PYSCOPG2_WHEEL_VER}-${Python3_WHEEL_ID}.whl
    <INSTALL_DIR>)
ExternalProject_Get_Property(psycopg2 INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/psycopg2-${PYSCOPG2_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# pygit2 wheel
#
find_package(unofficial-git2 CONFIG REQUIRED)
createvirtualenvironment(pygitWheelBuildEnv REQUIREMENTS wheel ${CFFI_WHEEL})
add_dependencies(pygitWheelBuildEnv cffi)

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/pygit2-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${BUILD_WHEEL_INCLUDE_DIRS}
library_dirs=${BUILD_WHEEL_LIBRARY_DIRS}
")

set(PYGIT2_WHEEL_VER 1.12.1)
ExternalProject_Add(
  pygit2
  GIT_REPOSITORY https://github.com/koordinates/pygit2.git
  GIT_TAG kart-v0.14.1
  GIT_SHALLOW ON
  BUILD_IN_SOURCE ON
  DEPENDS pygitWheelBuildEnv unofficial::git2::libgit2package
  EXCLUDE_FROM_ALL ON
  BUILD_BYPRODUCTS "<SOURCE_DIR>/build"
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/pygit2-prefix/setup.cfg
                    <SOURCE_DIR>
  BUILD_COMMAND ${CMAKE_COMMAND} -E rm -rf <INSTALL_DIR>/*.whl <TMP_DIR>/dist <SOURCE_DIR>/.eggs
  COMMAND ${CMAKE_COMMAND} -E env LIBGIT2=${CURRENT_PACKAGES_DIR} -- ${pygitWheelBuildEnv_PYTHON} -m
          build --wheel --no-isolation --outdir <TMP_DIR>/dist
  INSTALL_COMMAND ${CMAKE_COMMAND} -E copy
                  <TMP_DIR>/dist/pygit2-${PYGIT2_WHEEL_VER}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(pygit2 INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/pygit2-${PYGIT2_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# pyodbc wheel
#
if(NOT WIN32)
  find_package(unixodbc CONFIG REQUIRED)
  set(PYODBC_BUILD_DEPENDS UNIX::odbc)
  set(ODBC_CONFIG_DIR ${CURRENT_PACKAGES_DIR}/tools/unixodbc/bin)
endif()

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/pyodbc-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${BUILD_WHEEL_INCLUDE_DIRS}
library_dirs=${BUILD_WHEEL_LIBRARY_DIRS}
")

set(PYODBC_WHEEL_VER 5.1.0)
ExternalProject_Add(
  pyodbc
  # if you build from a git repository, pyodbc adds +commit0c0ffee to the wheel version
  URL https://files.pythonhosted.org/packages/d5/5b/a93f7017d4df84c3971cf60ee935149f12e0d1e111febc67ba2e23015a79/pyodbc-5.1.0.tar.gz
  URL_HASH SHA256=397feee44561a6580be08cedbe986436859563f4bb378f48224655c8e987ea60
  DOWNLOAD_NO_PROGRESS ON
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv ${PYODBC_BUILD_DEPENDS}
  EXCLUDE_FROM_ALL ON
  BUILD_BYPRODUCTS "<SOURCE_DIR>/build"
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/pyodbc-prefix/setup.cfg
                    <SOURCE_DIR>
  BUILD_COMMAND ${CMAKE_COMMAND} -E rm -rf <INSTALL_DIR>/*.whl
  COMMAND ${CMAKE_COMMAND} -E env --modify PATH=path_list_prepend:${ODBC_CONFIG_DIR} --
          ${wheelBuildEnv_PYTHON} -m build --wheel --outdir <TMP_DIR>/dist
  INSTALL_COMMAND ${CMAKE_COMMAND} -E copy
                  <TMP_DIR>/dist/pyodbc-${PYODBC_WHEEL_VER}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(pyodbc INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/pyodbc-${PYODBC_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# pysqlite3 wheel
#
find_package(unofficial-sqlite3 CONFIG REQUIRED)

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/pysqlite3-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${BUILD_WHEEL_INCLUDE_DIRS}
library_dirs=${BUILD_WHEEL_LIBRARY_DIRS}
")

set(PYSQLITE3_WHEEL_VER 0.5.2)
ExternalProject_Add(
  pysqlite3
  GIT_REPOSITORY https://github.com/coleifer/pysqlite3.git
  GIT_TAG ${PYSQLITE3_WHEEL_VER}
  GIT_SHALLOW ON
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv unofficial::sqlite3::sqlite3
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/pysqlite3-prefix/setup.cfg
                    <SOURCE_DIR>
  BUILD_COMMAND ${CMAKE_COMMAND} -E rm -rf <INSTALL_DIR>/*.whl
  COMMAND ${wheelBuildEnv_PYTHON} -m build --wheel --outdir <TMP_DIR>/dist
  INSTALL_COMMAND
    ${CMAKE_COMMAND} -E copy
    <TMP_DIR>/dist/pysqlite3-${PYSQLITE3_WHEEL_VER}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(pysqlite3 INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/pysqlite3-${PYSQLITE3_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# cryptography wheel
#

createvirtualenvironment(cryptographyWheelBuildEnv REQUIREMENTS wheel ${CFFI_WHEEL} setuptools_rust)
add_dependencies(cryptographyWheelBuildEnv cffi)

include(FindRust)
find_package(OpenSSL REQUIRED)
find_package(Rust 1.41 REQUIRED)
cmake_path(GET RUST_EXECUTABLE PARENT_PATH RUST_DIR)

set(CRYPTOGRAPHY_WHEEL_VER 41.0.4)
ExternalProject_Add(
  cryptography
  GIT_REPOSITORY https://github.com/pyca/cryptography.git
  GIT_TAG ${CRYPTOGRAPHY_WHEEL_VER}
  GIT_SHALLOW ON
  BUILD_IN_SOURCE ON
  DEPENDS cryptographyWheelBuildEnv OpenSSL::SSL OpenSSL::Crypto
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${CMAKE_COMMAND} -E rm -rf <INSTALL_DIR>/*.whl
  COMMAND
    ${CMAKE_COMMAND} -E env OPENSSL_LIB_DIR=${CURRENT_PACKAGES_DIR}/lib
    OPENSSL_INCLUDE_DIR=${CURRENT_PACKAGES_DIR}/include --modify PATH=path_list_prepend:${RUST_DIR}
    -- ${cryptographyWheelBuildEnv_PIP} wheel --no-binary :all: --no-build-isolation --no-deps
    --no-index --wheel-dir <TMP_DIR>/dist .
  INSTALL_COMMAND
    ${CMAKE_COMMAND} -E copy
    <TMP_DIR>/dist/cryptography-${CRYPTOGRAPHY_WHEEL_VER}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(cryptography INSTALL_DIR)
list(APPEND WHEEL_LIST
     ${INSTALL_DIR}/cryptography-${CRYPTOGRAPHY_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# reflink wheel
#

createvirtualenvironment(reflinkWheelBuildEnv REQUIREMENTS wheel ${CFFI_WHEEL} pytest-runner)
add_dependencies(reflinkWheelBuildEnv cffi)

set(REFLINK_WHEEL_VER 0.2.2)
ExternalProject_Add(
  reflink
  URL https://files.pythonhosted.org/packages/63/47/b503f286378336aa6d2fd3caad10633bb79f3bc6256f1f2f9102dacb3c98/reflink-0.2.2.tar.gz
  URL_HASH SHA256=882375ee7319275ae5f6a6a1287406365dac1e9643b91ad10e5187d3f84253bd
  DOWNLOAD_NO_PROGRESS ON
  BUILD_IN_SOURCE ON
  DEPENDS reflinkWheelBuildEnv
  EXCLUDE_FROM_ALL ON
  BUILD_BYPRODUCTS "<SOURCE_DIR>/build"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${CMAKE_COMMAND} -E rm -rf <INSTALL_DIR>/*.whl
  COMMAND ${reflinkWheelBuildEnv_PYTHON} -m build --wheel --no-isolation --outdir <TMP_DIR>/dist
  INSTALL_COMMAND ${CMAKE_COMMAND} -E copy
                  <TMP_DIR>/dist/reflink-${REFLINK_WHEEL_VER}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(reflink INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/reflink-${REFLINK_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

# ##################################################################################################
# OTHER LIBS AND BINS
# ##################################################################################################

#
# Proj
#
find_package(PROJ CONFIG REQUIRED)

#
# mod_spatialite
#
pkg_check_modules(SPATIALITE REQUIRED spatialite)
# Helper for finding mod_spatialite. Modifies some global state, so keep it contained in a function
function(find_mod_spatialite)
  if(WIN32)
    # We need the .dll, not the .lib
    find_file(
      SPATIALITE_EXTENSION
      NAMES mod_spatialite.dll REQUIRED
      PATHS "${CURRENT_PACKAGES_DIR}/bin"
      NO_DEFAULT_PATH)
  else()
    set(CMAKE_FIND_LIBRARY_PREFIXES "") # otherwise it looks for `libmod_spatialite`
    find_library(
      SPATIALITE_EXTENSION
      NAMES mod_spatialite REQUIRED
      HINTS ${SPATIALITE_LIBDIR}
      NO_DEFAULT_PATH)
  endif()
  cmake_path(GET SPATIALITE_EXTENSION FILENAME SPATIALITE_EXTENSION_NAME)
  set(SPATIALITE_EXTENSION
      "${SPATIALITE_EXTENSION}"
      PARENT_SCOPE)
  set(SPATIALITE_EXTENSION_NAME
      "${SPATIALITE_EXTENSION_NAME}"
      PARENT_SCOPE)
endfunction()
find_mod_spatialite()
list(APPEND LIB_LIST ${SPATIALITE_EXTENSION})

#
# PDAL
#

find_package(PDAL CONFIG REQUIRED)
pkg_check_modules(PDAL REQUIRED pdal)

get_target_property(PDALCPP_LIBRARY pdalcpp LOCATION)
list(APPEND LIB_LIST ${PDALCPP_LIBRARY})

set(PDAL_EXECUTABLE "${CURRENT_PACKAGES_DIR}/tools/pdal/pdal${CMAKE_EXECUTABLE_SUFFIX}")
list(APPEND EXE_LIST "${PDAL_EXECUTABLE}")

#
# Git-LFS
#

include(FindGo)
find_package(Go 1.17 REQUIRED)

set(GIT_LFS_OUTPUT "bin/git-lfs${CMAKE_EXECUTABLE_SUFFIX}")
if(WIN32)
  set(GIT_LFS_LDFLAGS "-s -w -X 'github.com/git-lfs/git-lfs/v3/config.Vendor=Kart'")
  set(GIT_LFS_BUILD ${GO_EXECUTABLE} build -ldflags=${GIT_LFS_LDFLAGS} -trimpath -o
                    ${GIT_LFS_OUTPUT})
else()
  set(GIT_LFS_BUILD make GO=${GO_EXECUTABLE} VENDOR=Kart ${GIT_LFS_OUTPUT})
endif()
ExternalProject_Add(
  git-lfs
  GIT_REPOSITORY https://github.com/git-lfs/git-lfs
  GIT_TAG 02ac3de0e19fbfeea7f8ddb079cfe074367a48d3 # main @ 2023-01-06 (>v3.3.0)
  GIT_SHALLOW OFF # set to ON when GIT_TAG refers to a tag
  BUILD_IN_SOURCE ON
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${GIT_LFS_BUILD}
  INSTALL_COMMAND "")
set(GIT_LFS_EXECUTABLE "git-lfs-prefix/src/git-lfs/${GIT_LFS_OUTPUT}")
list(APPEND EXE_LIST "${GIT_LFS_EXECUTABLE}")

#
# Git Note: we define the Git dependency last since it affects GIT_VERSION_STRING etc, which can
# break other dependencies.
#

if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  # Download a prebuilt Git with spatial-filter extension on Windows. TODO: Use a more recent tag
  # and build inline.

  # FIXME: Update to a kx-latest build (ie based on 2.47.1) FIXME: Get the build process for this
  # custom git build into CI somewhere? Either here or in koordinates/git
  ExternalProject_Add(
    git
    URL https://github.com/koordinates/git/releases/download/kart-v0.15.2-windows-mingit-build/MinGit-2.45.1.7.g5be46a8262-64-bit.zip
    URL_HASH SHA256=322a7b292c382f39d54ff4bd421f240d5ef16f10bffa485718076907750de351
    DOWNLOAD_NO_PROGRESS ON
    BUILD_IN_SOURCE 1 # avoid creation of a build directory
    INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/git
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ${CMAKE_COMMAND} -E copy_directory <SOURCE_DIR> <INSTALL_DIR>)
  set(GIT_VENV_EXE "git/cmd/git.exe")

else()
  # Build Git with spatial-filter extension on macOS and Linux
  find_package(OpenSSL REQUIRED)
  pkg_check_modules(OPENSSL REQUIRED openssl)
  pkg_check_modules(LIBCURL REQUIRED libcurl)
  find_package(pcre2 CONFIG REQUIRED)

  cmake_path(SET GIT_SPATIAL_FILTER_EXT contrib/filter-extensions/spatial/spatial.a)

  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID MATCHES "AppleClang")
    set(CXX_STD_LIB_LDFLAGS "-lc++")
  elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    set(CXX_STD_LIB_LDFLAGS "-L/lib -lstdc++")
  else()
    message(FATAL_ERROR "Unknown C++ compiler")
  endif()

  set(gitBuildConfig
      "prefix=/"
      "V=1"
      "CC=${CMAKE_C_COMPILER}"
      "NO_GETTEXT=YesPlease"
      "INSTALL_SYMLINKS=1"
      "USE_LIBPCRE=1"
      "NO_EXPAT=1"
      "NO_PERL=1"
      "NO_TCLTK=1"
      "RUNTIME_PREFIX=1"
      "LIBPCREDIR=${CURRENT_PACKAGES_DIR}"
      "OPENSSLDIR=${OPENSSL_PREFIX}"
      "CURLDIR=${CURRENT_PACKAGES_DIR}"
      "CURL_LDFLAGS=-lcurl"
      "NO_PYTHON=1"
      "NO_PERL=1"
      "NO_GITWEB=1"
      "NO_TCLTK=1"
      # stuff for building the spatial partial clone filter extension
      "CXX=${CMAKE_CXX_COMPILER}"
      "LINK=${CMAKE_CXX_COMPILER}"
      "LDFLAGS=-L${CURRENT_PACKAGES_DIR}/lib"
      "FILTER_EXTENSIONS=${GIT_SPATIAL_FILTER_EXT}"
      "EXTENSION_LIBS=-L${CURRENT_PACKAGES_DIR}/lib -lsqlite3 ${CXX_STD_LIB_LDFLAGS}")

  ExternalProject_Add(
    git
    GIT_REPOSITORY https://github.com/koordinates/git
    GIT_TAG kart-v2.47.1
    GIT_SHALLOW ON
    BUILD_IN_SOURCE ON
    DEPENDS CURL::libcurl OpenSSL::SSL OpenSSL::Crypto unofficial::sqlite3::sqlite3 PCRE2::8BIT
    EXCLUDE_FROM_ALL ON
    CONFIGURE_COMMAND ""
    INSTALL_DIR env
    BUILD_COMMAND make ${gitBuildConfig} DESTDIR=<INSTALL_DIR> all strip
    INSTALL_COMMAND make ${gitBuildConfig} DESTDIR=<INSTALL_DIR> install
    COMMAND
      ${CMAKE_COMMAND} -E rm -rf <INSTALL_DIR>/share/git-gui <INSTALL_DIR>/share/gitk
      <INSTALL_DIR>/bin/scalar <INSTALL_DIR>/bin/git-cvsserver <INSTALL_DIR>/bin/git-shell
      <INSTALL_DIR>/libexec/git-core/scalar <INSTALL_DIR>/libexec/git-core/git-cvsserver
      <INSTALL_DIR>/libexec/git-core/git-shell)
  set(GIT_EXECUTABLE "git-prefix/dist/bin/git${CMAKE_EXECUTABLE_SUFFIX}")
  set(GIT_VENV_EXE "bin/git")
endif()

set(LIB_DIR "lib/")
if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  set(EXE_DIR scripts)
  set(LIB_SEARCH_DIRS --search-path "${CURRENT_PACKAGES_DIR}/bin")
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(EXE_DIR bin)
  set(LIB_SEARCH_DIRS --search-path "${CURRENT_PACKAGES_DIR}/lib" --search-path
                      /usr/local/opt/libtool/lib/)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(EXE_DIR bin)
  set(LIB_SEARCH_DIRS --search-path "${CURRENT_PACKAGES_DIR}/lib")
endif()

#
# General target
#
add_custom_target(
  vendor_wheels ALL
  DEPENDS cffi
          gdal_py
          psycopg2
          pygit2
          pyodbc
          pysqlite3
          cryptography
          reflink
  COMMENT "Python vendor wheels")

set(VENDOR_CONFIG _kart_env.py)
if(NOT PROJECT_IS_TOP_LEVEL)
  set(VENDOR_CONFIG
      vendor-vcpkg/_kart_env.py
      PARENT_SCOPE)
endif()
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/cmake/template/_kart_env.py.in ${VENDOR_CONFIG}
               ESCAPE_QUOTES)

#
# Vendor Dist Archive
#
if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  set(VENDOR_ARCHIVE kart-vendor.${Python3_WHEEL_ID}.zip)
else()
  set(VENDOR_ARCHIVE kart-vendor.${Python3_WHEEL_ID}.tar.gz)
endif()

if(NOT PROJECT_IS_TOP_LEVEL)
  set(VENDOR_ARCHIVE
      vcpkg-vendor/${VENDOR_ARCHIVE}
      PARENT_SCOPE)
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  cmake_path(GET CMAKE_C_COMPILER PARENT_PATH TOOLS_PATH)
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(EXTRA_COPY_COMMAND ${CMAKE_COMMAND} -E create_symlink mod_spatialite.so
                         env/lib/mod_spatialite.dylib)
endif()

add_custom_command(
  OUTPUT ${VENDOR_ARCHIVE}
  DEPENDS vendor_wheels git git-lfs ${VENDOR_CONFIG} fix_vendor_libs.py
  COMMAND ${CMAKE_COMMAND} -E rm -rf env/share/git-gui
  COMMAND ${CMAKE_COMMAND} -E make_directory wheelhouse env env/lib env/tools env/share
          env/${EXE_DIR}
  COMMAND ${CMAKE_COMMAND} -E copy ${WHEEL_LIST} wheelhouse
  COMMAND ${CMAKE_COMMAND} -E copy ${LIB_LIST} env/lib
  COMMAND ${CMAKE_COMMAND} -E copy ${EXE_LIST} env/${EXE_DIR}
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CURRENT_PACKAGES_DIR}/tools/gdal/ env/tools/gdal
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CURRENT_PACKAGES_DIR}/share/gdal/ env/share/gdal
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CURRENT_PACKAGES_DIR}/share/proj/ env/share/proj
  COMMAND ${EXTRA_COPY_COMMAND}
  COMMAND
    ${CMAKE_COMMAND} -E env CMAKE_COMMAND=${CMAKE_COMMAND} --modify
    PATH=path_list_prepend:${TOOLS_PATH} -- ${wheelBuildEnv_PYTHON}
    "${CMAKE_CURRENT_SOURCE_DIR}/fix_vendor_libs.py" $<IF:$<BOOL:$ENV{VERBOSE}>,-v2,-v0> .
    ${VENDOR_ARCHIVE} ${LIB_SEARCH_DIRS}
  VERBATIM
  COMMENT "Vendor dependency archive assembly")

add_custom_target(
  vendor_archive
  DEPENDS ${VENDOR_ARCHIVE}
  COMMENT "Vendor dependency archive")

add_custom_target(
  vendor ALL
  DEPENDS vendor_archive
  COMMENT "Vendor dependencies")
