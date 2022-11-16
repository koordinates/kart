cmake_minimum_required(VERSION 3.25)

include(${CMAKE_CURRENT_LIST_DIR}/vcpkg-toolchain-setup.cmake)

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

# Different path-list separators on different platforms:
# TODO: can we use cmake_path(CONVERT ... TO_NATIVE_PATH_LIST ...) here?
if(NOT WIN32)
  string(REPLACE ";" ":" BUILD_WHEEL_INCLUDE_DIRS "${BUILD_WHEEL_INCLUDE_DIRS}")
  string(REPLACE ";" ":" BUILD_WHEEL_LIBRARY_DIRS "${BUILD_WHEEL_LIBRARY_DIRS}")
endif()

#
# cffi wheel
#
find_package(libffi REQUIRED)

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cffi-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${BUILD_WHEEL_INCLUDE_DIRS}
library_dirs=${BUILD_WHEEL_LIBRARY_DIRS}
$<$<NOT:$<PLATFORM_ID:Windows>>:rpath=${CURRENT_PACKAGES_DIR}/lib>
")

set(CFFI_WHEEL_VER 1.15.1)
ExternalProject_Add(
  cffi
  URL https://files.pythonhosted.org/packages/2b/a8/050ab4f0c3d4c1b8aaa805f70e26e84d0e27004907c5b8ecc1d31815f92a/cffi-1.15.1.tar.gz
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv libffi
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_BYPRODUCTS "<SOURCE_DIR>/build"
  BUILD_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/cffi-prefix/setup.cfg
                <SOURCE_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist
          --bdist-dir <TMP_DIR>/bdist
  INSTALL_COMMAND ${CMAKE_COMMAND} -E copy
                  <TMP_DIR>/dist/cffi-${CFFI_WHEEL_VER}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(cffi INSTALL_DIR)
set(CFFI_WHEEL ${INSTALL_DIR}/cffi-${CFFI_WHEEL_VER}-${Python3_WHEEL_ID}.whl)
list(APPEND WHEEL_LIST ${CFFI_WHEEL})

#
# gdal_py wheel We download the bindings associated with the version we picked up via vcpkg/FindGDAL
#
find_package(GDAL REQUIRED)

file(
  GLOB GDAL_DATA_FILES
  RELATIVE ${CURRENT_PACKAGES_DIR}
  "share/gdal/*")
list(APPEND ENV_FILE_LIST ${GDAL_DATA_FILES})

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

ExternalProject_Add(
  gdal_py
  URL https://pypi.org/packages/source/G/GDAL/GDAL-${GDAL_VERSION}.tar.gz
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv GDAL::GDAL
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_BYPRODUCTS "<SOURCE_DIR>/build"
  BUILD_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/gdal_py-prefix/setup.cfg
                <SOURCE_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist
          --bdist-dir <TMP_DIR>/bdist
  INSTALL_COMMAND ${CMAKE_COMMAND} -E copy
                  <TMP_DIR>/dist/GDAL-${GDAL_VERSION}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(gdal_py INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/GDAL-${GDAL_VERSION}-${Python3_WHEEL_ID}.whl)

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

set(PYSCOPG2_WHEEL_VER 2.8.5)
ExternalProject_Add(
  psycopg2
  URL https://files.pythonhosted.org/packages/a8/8f/1c5690eebf148d1d1554fc00ccf9101e134636553dbb75bdfef4f85d7647/psycopg2-2.8.5.tar.gz
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv PostgreSQL::PostgreSQL
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_BYPRODUCTS "<SOURCE_DIR>/build"
  BUILD_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/psycopg2-prefix/setup.cfg
                <SOURCE_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist
          --bdist-dir <TMP_DIR>/bdist
  INSTALL_COMMAND
    ${CMAKE_COMMAND} -E copy <TMP_DIR>/dist/psycopg2-${PYSCOPG2_WHEEL_VER}-${Python3_WHEEL_ID}.whl
    <INSTALL_DIR>)
ExternalProject_Get_Property(psycopg2 INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/psycopg2-${PYSCOPG2_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# pygit2 wheel
#
find_package(unofficial-git2 CONFIG REQUIRED)

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/pygit2-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${BUILD_WHEEL_INCLUDE_DIRS}
library_dirs=${BUILD_WHEEL_LIBRARY_DIRS}
")

set(PYGIT2_WHEEL_VER 1.9.0)
ExternalProject_Add(
  pygit2
  GIT_REPOSITORY https://github.com/koordinates/pygit2.git
  GIT_TAG kart-v0.11.1
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv unofficial::git2::git2
  EXCLUDE_FROM_ALL ON
  BUILD_BYPRODUCTS "<SOURCE_DIR>/build"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/pygit2-prefix/setup.cfg
                <SOURCE_DIR>
  COMMAND ${CMAKE_COMMAND} -E env LIBGIT2=${CURRENT_PACKAGES_DIR} -- ${wheelBuildEnv_PYTHON}
          setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist --bdist-dir <TMP_DIR>/bdist
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
endif()

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/pyodbc-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${BUILD_WHEEL_INCLUDE_DIRS}
library_dirs=${BUILD_WHEEL_LIBRARY_DIRS}
")

set(PYODBC_WHEEL_VER 4.0.32)
ExternalProject_Add(
  pyodbc
  # if you build from a git repository, pyodbc adds +commit0c0ffee to the wheel version
  URL https://files.pythonhosted.org/packages/d1/21/755f5fa373d01d1803d992c7611799dc4e9fcbb5db7d0852990d6ab3c9ad/pyodbc-4.0.32.tar.gz
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv ${PYODBC_BUILD_DEPENDS}
  EXCLUDE_FROM_ALL ON
  BUILD_BYPRODUCTS "<SOURCE_DIR>/build"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/pyodbc-prefix/setup.cfg
                <SOURCE_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist
          --bdist-dir <TMP_DIR>/bdist
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

set(PYSQLITE3_WHEEL_VER 0.4.5)
ExternalProject_Add(
  pysqlite3
  GIT_REPOSITORY https://github.com/coleifer/pysqlite3.git
  GIT_TAG ${PYSQLITE3_WHEEL_VER}
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv unofficial::sqlite3::sqlite3
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/pysqlite3-prefix/setup.cfg
                <SOURCE_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build_ext --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist
          --bdist-dir <TMP_DIR>/bdist
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

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cryptography-prefix/setup.cfg.inc
  CONTENT "
[build_ext]
include_dirs=${BUILD_WHEEL_INCLUDE_DIRS}
library_dirs=${BUILD_WHEEL_LIBRARY_DIRS}
")

set(CRYPTOGRAPHY_WHEEL_VER 38.0.3)
ExternalProject_Add(
  cryptography
  GIT_REPOSITORY https://github.com/pyca/cryptography.git
  GIT_TAG ${CRYPTOGRAPHY_WHEEL_VER}
  BUILD_IN_SOURCE ON
  DEPENDS cryptographyWheelBuildEnv OpenSSL::SSL OpenSSL::Crypto
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ${GIT_EXECUTABLE} restore setup.cfg
  COMMAND ${CMAKE_COMMAND} -E cat <INSTALL_DIR>/setup.cfg.inc >> setup.cfg
  BUILD_COMMAND
    ${CMAKE_COMMAND} -E env --modify PATH=path_list_prepend:${RUST_DIR}
    ${cryptographyWheelBuildEnv_PIP} wheel --no-binary :all: --no-build-isolation --no-deps
    --no-index --wheel-dir <TMP_DIR>/dist .
  INSTALL_COMMAND
    ${CMAKE_COMMAND} -E copy
    <TMP_DIR>/dist/cryptography-${CRYPTOGRAPHY_WHEEL_VER}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(cryptography INSTALL_DIR)
list(APPEND WHEEL_LIST
     ${INSTALL_DIR}/cryptography-${CRYPTOGRAPHY_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

# ##################################################################################################
# OTHER LIBS AND BINS
# ##################################################################################################

#
# Proj
#
find_package(PROJ CONFIG REQUIRED)
file(
  GLOB PROJ_DATA_FILES
  RELATIVE ${CURRENT_PACKAGES_DIR}
  "share/proj/*")
list(APPEND ENV_FILE_LIST ${PROJ_DATA_FILES})

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

get_target_property(PDALUTIL_LIBRARY pdal_util LOCATION)
if(WIN32 OR APPLE)
  get_target_property(PDALCPP_LIBRARY pdalcpp LOCATION)
  list(APPEND LIB_LIST ${PDALCPP_LIBRARY} ${PDALUTIL_LIBRARY})
else()
  get_target_property(PDALBASE_LIBRARY pdal_base LOCATION)
  # set(PDALCPP_LIBRARY ${PDAL_LINK_LIBRARIES}) # why is this different on macOS vs Linux? And why
  # isn't it a target?
  list(APPEND LIB_LIST ${PDALBASE_LIBRARY} ${PDALUTIL_LIBRARY})
endif()

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
  GIT_TAG main
  GIT_SHALLOW ON
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

  ExternalProject_Add(
    git
    URL https://github.com/koordinates/git/releases/download/kart-0.11.0-windows/MinGit-v2.34.0.windows.1.13-64-bit.zip
    URL_HASH MD5=abe020e6ac70c6a6c2566d2c1b2b3d8a
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
  pkg_check_modules(PCRE2 REQUIRED libpcre2-8)
  pkg_check_modules(LIBCURL REQUIRED libcurl)

  cmake_path(SET GIT_SPATIAL_FILTER_EXT contrib/filter-extensions/spatial/spatial.a)

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
      "LIBPCREDIR=${PCRE2_PREFIX}"
      "OPENSSLDIR=${OPENSSL_PREFIX}"
      "CURL_CONFIG=${CURRENT_PACKAGES_DIR}/tools/curl/bin/curl-config"
      # stuff for building the spatial partial clone filter extension
      "CXX=${CMAKE_CXX_COMPILER}"
      "LINK=${CMAKE_CXX_COMPILER}"
      "FILTER_EXTENSIONS=${GIT_SPATIAL_FILTER_EXT}"
      "CFLAGS=-I<SOURCE_DIR>"
      "EXTENSION_LIBS=-L${CURRENT_PACKAGES_DIR}/lib -lsqlite3")

  ExternalProject_Add(
    git
    GIT_REPOSITORY https://github.com/koordinates/git
    GIT_TAG kart-v0.12.0-rc3
    GIT_SHALLOW ON
    BUILD_IN_SOURCE ON
    DEPENDS CURL::libcurl OpenSSL::SSL OpenSSL::Crypto unofficial::sqlite3::sqlite3
    EXCLUDE_FROM_ALL ON
    CONFIGURE_COMMAND ""
    INSTALL_DIR env
    BUILD_COMMAND make ${gitBuildConfig} DESTDIR=<INSTALL_DIR> all strip
    INSTALL_COMMAND make ${gitBuildConfig} DESTDIR=<INSTALL_DIR> install
    COMMAND ${CMAKE_COMMAND} -E rm -rf <INSTALL_DIR>/share/git-gui <INSTALL_DIR>/share/gitk
            <INSTALL_DIR>/bin/scalar <INSTALL_DIR>/bin/git-cvsserver <INSTALL_DIR>/bin/git-shell)
  set(GIT_EXECUTABLE "git-prefix/dist/bin/git${CMAKE_EXECUTABLE_SUFFIX}")
  set(GIT_VENV_EXE "bin/git")
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  set(EXE_DIR scripts)
  set(LIB_DIR "")
  set(LIB_SEARCH_DIR "${CURRENT_PACKAGES_DIR}/bin")
else()
  set(EXE_DIR bin)
  set(LIB_DIR "lib/")
  set(LIB_SEARCH_DIR "${CURRENT_PACKAGES_DIR}/lib")
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

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(EXTRA_COPY_COMMAND ${CMAKE_COMMAND} -E create_symlink mod_spatialite.so
                         env/lib/mod_spatialite.dylib)
endif()

add_custom_command(
  OUTPUT ${VENDOR_ARCHIVE}
  DEPENDS vendor_wheels git git-lfs ${VENDOR_CONFIG} fix_vendor_libs.py
  COMMAND ${CMAKE_COMMAND} -E rm -rf env/share/git-gui
  COMMAND ${CMAKE_COMMAND} -E make_directory wheelhouse env env/lib env/share env/${EXE_DIR}
  COMMAND ${CMAKE_COMMAND} -E copy ${WHEEL_LIST} wheelhouse
  COMMAND ${CMAKE_COMMAND} -E copy ${LIB_LIST} env/lib
  COMMAND ${CMAKE_COMMAND} -E copy ${EXE_LIST} env/${EXE_DIR}
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CURRENT_PACKAGES_DIR}/share/gdal/ env/share/gdal
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CURRENT_PACKAGES_DIR}/share/proj/ env/share/proj
  COMMAND ${EXTRA_COPY_COMMAND}
  COMMAND
    ${CMAKE_COMMAND} -E env CMAKE_COMMAND=${CMAKE_COMMAND} -- ${wheelBuildEnv_PYTHON}
    "${CMAKE_CURRENT_SOURCE_DIR}/fix_vendor_libs.py" $<$<BOOL:$ENV{VERBOSE}>:-vv> .
    ${VENDOR_ARCHIVE} -s "${LIB_SEARCH_DIR}"
  COMMENT "Vendor dependency archive assembly")

add_custom_target(
  vendor_archive
  DEPENDS ${VENDOR_ARCHIVE}
  COMMENT "Vendor dependency archive")

add_custom_target(
  vendor ALL
  DEPENDS vendor_archive
  COMMENT "Vendor dependencies")
