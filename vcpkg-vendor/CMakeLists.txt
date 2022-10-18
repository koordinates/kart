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
include(ExternalProject)

include(debug_helpers)

set(WHEEL_LIST "")
set(LIB_LIST "")
set(BIN_LIST "")

#
# Python
#
# Need Development here or VcPkg falls back to the system interpreter
find_package(
  Python3 3.10
  COMPONENTS Development Interpreter
  REQUIRED)
cmake_path(GET Python3_STDLIB FILENAME Python3_STDLIB_DIR_NAME)
cmake_path(GET Python3_INCLUDE_DIRS FILENAME Python3_INCLUDE_DIR_NAME)

list(APPEND BIN_LIST ${Python3_EXECUTABLE})

include(PythonGetABIInfo)
pythongetabiinfo()

include(PyCreateVirtualEnvironment)
createvirtualenvironment(wheelBuildEnv REQUIREMENTS "wheel")

#
# pysqlite3
#
find_package(unofficial-sqlite3 CONFIG REQUIRED)
get_target_property(SQLite3_INCLUDE_DIRS unofficial::sqlite3::sqlite3 INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(SQLite3_LIBRARY unofficial::sqlite3::sqlite3 LOCATION)
cmake_path(GET SQLite3_LIBRARY PARENT_PATH SQLite3_LIBRARY_DIR)
if(WIN32)
  # SQLite3_LIBRARY points to the .dll - we seem to need the .lib as well
  # TODO - should we need both? Is it linking to the wrong one?
  set(SQLite3_LIBRARY_DIRS "${SQLite3_LIBRARY_DIR}" "${SQLite3_LIBRARY_DIR}/../lib")
else()
  set(SQLite3_LIBRARY_DIRS "${SQLite3_LIBRARY_DIR}")
endif()

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/pysqlite3-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${SQLite3_INCLUDE_DIRS}
library_dirs=${SQLite3_LIBRARY_DIRS}
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
# psycopg2
#
find_package(PostgreSQL REQUIRED)

if(WIN32)
  set(PostgreSQL_BINARY_DIR "${PostgreSQL_LIBRARY_DIR}/../tools/libpq")
else()
  set(PostgreSQL_BINARY_DIR "${PostgreSQL_LIBRARY_DIR}/../tools/libpq/bin")
  string(REPLACE ";" ":" PostgreSQL_INCLUDE_DIRS "${PostgreSQL_INCLUDE_DIRS}")
endif()

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/psycopg2-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${PostgreSQL_INCLUDE_DIRS}
library_dirs=${PostgreSQL_LIBRARY_DIRS}
pg_config=${PostgreSQL_BINARY_DIR}/pg_config${CMAKE_EXECUTABLE_SUFFIX}
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
  BUILD_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/psycopg2-prefix/setup.cfg
                <SOURCE_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build_ext
          --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist
          --bdist-dir <TMP_DIR>/bdist
  INSTALL_COMMAND
    ${CMAKE_COMMAND} -E copy <TMP_DIR>/dist/psycopg2-${PYSCOPG2_WHEEL_VER}-${Python3_WHEEL_ID}.whl
    <INSTALL_DIR>)
ExternalProject_Get_Property(psycopg2 INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/psycopg2-${PYSCOPG2_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# cffi
#
find_package(libffi REQUIRED)
get_target_property(LIBFFI_LIBRARY libffi LOCATION)
cmake_path(GET LIBFFI_LIBRARY PARENT_PATH LIBFFI_LIBRARY_DIR)
set(LIBFFI_INCLUDE_DIRS "${LIBFFI_LIBRARY_DIR}/../include")

if(WIN32)
  # LIBFFI_LIBRARY points to the .dll - we seem to need the .lib as well
  # TODO - should we need both? Is it linking to the wrong one?
  set(LIBFFI_LIBRARY_DIRS "${LIBFFI_LIBRARY_DIR}" "${LIBFFI_LIBRARY_DIR}/../lib")
  # TODO - setuptools can't find the python lib, even though sysconfig can.
  set(CFFI_BUILD_LIBRARY_DIRS ${LIBFFI_LIBRARY_DIRS} "{CURRENT_PACKAGES_DIR}/tools/python3/Lib")
else()
  set(CFFI_BUILD_LIBRARY_DIRS "${LIBFFI_LIBRARY_DIR}")
endif()

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cffi-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${LIBFFI_INCLUDE_DIRS}
library_dirs=${LIBFFI_LIBRARY_DIRS}
")

set(CFFI_WHEEL_VER 1.15.1)
ExternalProject_Add(
  cffi
  URL https://files.pythonhosted.org/packages/2b/a8/050ab4f0c3d4c1b8aaa805f70e26e84d0e27004907c5b8ecc1d31815f92a/cffi-1.15.1.tar.gz
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv libffi
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/cffi-prefix/setup.cfg <SOURCE_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build_ext --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist --bdist-dir <TMP_DIR>/bdist
  INSTALL_COMMAND ${CMAKE_COMMAND} -E copy
                  <TMP_DIR>/dist/cffi-${CFFI_WHEEL_VER}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(cffi INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/cffi-${CFFI_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# pygit2
#
find_package(unofficial-git2 CONFIG REQUIRED)
get_target_property(Git2_LIBRARIES unofficial::git2::git2 LOCATION)
cmake_path(GET Git2_LIBRARIES PARENT_PATH Git2_LIB_DIR)

pkg_check_modules(LIBGIT2 REQUIRED libgit2)

list(JOIN VCPKG_IMPORT_PREFIX " " PYGIT2_BUILD_ENV_LIBGIT2)
list(JOIN LIBGIT2_LIBDIR " " PYGIT2_BUILD_ENV_LIBGIT2_LIB)
list(JOIN LIBGIT2_STATIC_CFLAGS " " PYGIT2_BUILD_ENV_CFLAGS)
list(JOIN LIBGIT2_STATIC_LDFLAGS " " PYGIT2_BUILD_ENV_LDFLAGS)

set(PYGIT2_BUILD_ENV
    "LIBGIT2=${PYGIT2_BUILD_ENV_LIBGIT2}" "LIBGIT2_LIB=${PYGIT2_BUILD_ENV_LIBGIT2_LIB}"
    "CFLAGS=${PYGIT2_BUILD_ENV_CFLAGS}" "LDFLAGS=${PYGIT2_BUILD_ENV_LDFLAGS}")

set(PYGIT2_WHEEL_VER 1.9.0)
ExternalProject_Add(
  pygit2
  GIT_REPOSITORY https://github.com/koordinates/pygit2.git
  GIT_TAG kart-v0.11.1
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv unofficial::git2::git2
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_BYPRODUCTS "<SOURCE_DIR>/.eggs/" "<SOURCE_DIR>/pygit2.egg-info/" "<SOURCE_DIR>/build/"
  BUILD_COMMAND ${CMAKE_COMMAND} -E rm -rf "<SOURCE_DIR>/.eggs/"
  COMMAND ${CMAKE_COMMAND} -E env ${PYGIT2_BUILD_ENV} ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg
          bdist_wheel --dist-dir <TMP_DIR>/dist --bdist-dir <TMP_DIR>/bdist
  INSTALL_COMMAND ${CMAKE_COMMAND} -E copy
                  <TMP_DIR>/dist/pygit2-${PYGIT2_WHEEL_VER}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(pygit2 INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/pygit2-${PYGIT2_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# pyodbc
#
if(WIN32)
  # TODO - setuptools can't find the python lib, even though sysconfig can.
  set(PYODBC_BUILD_LIBRARY_DIRS "{CURRENT_PACKAGES_DIR}/tools/python3/Lib")

else()
  find_package(unixodbc CONFIG REQUIRED)
  get_target_property(ODBC_INCLUDE_DIRS UNIX::odbc INTERFACE_INCLUDE_DIRECTORIES)
  get_target_property(ODBC_LIBRARY UNIX::odbc LOCATION)
  cmake_path(GET ODBC_LIBRARY PARENT_PATH ODBC_LIBRARY_DIR)

  set(PYODBC_BUILD_INCLUDE_DIRS ${ODBC_INCLUDE_DIRS})
  set(PYODBC_BUILD_LIBRARY_DIRS ${ODBC_LIBRARY_DIR})
  set(PYODBC_BUILD_DEPENDS UNIX::odbc)
endif()

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/pyodbc-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${PYODBC_BUILD_INCLUDE_DIRS}
library_dirs=${PYODBC_BUILD_LIBRARY_DIRS}
")

set(PYODBC_WHEEL_VER 4.0.32)
ExternalProject_Add(
  pyodbc
  # if you build from a git repository, pyodbc adds +commit0c0ffee to the wheel version
  URL https://files.pythonhosted.org/packages/d1/21/755f5fa373d01d1803d992c7611799dc4e9fcbb5db7d0852990d6ab3c9ad/pyodbc-4.0.32.tar.gz
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv ${PYODBC_BUILD_DEPENDS}
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/pysqlite3-prefix/setup.cfg <SOURCE_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build_ext --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist --bdist-dir <TMP_DIR>/bdist
  INSTALL_COMMAND ${CMAKE_COMMAND} -E copy
                  <TMP_DIR>/dist/pyodbc-${PYODBC_WHEEL_VER}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(pyodbc INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/pyodbc-${PYODBC_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# GDAL Python module We download the bindings associated with the version we picked up via
# vcpkg/FindGDAL
#
find_package(GDAL REQUIRED)
get_target_property(GDAL_INCLUDE_DIRS unofficial::sqlite3::sqlite3 INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(GDAL_LIBRARY GDAL::GDAL LOCATION)
cmake_path(GET GDAL_LIBRARY PARENT_PATH GDAL_LIBRARY_DIR)

if(WIN32)
  # GDAL_LIBRARY points to the .dll - we seem to need the .lib as well
  # TODO - should we need both? Is it linking to the wrong one?
  set(GDAL_LIBRARY_DIRS "${GDAL_LIBRARY_DIR}" "${GDAL_LIBRARY_DIR}/../lib")
else()
  set(GDAL_LIBRARY_DIRS "${GDAL_LIBRARY_DIR}")
endif()

file(
  GLOB GDAL_DATA_FILES
  RELATIVE ${CURRENT_PACKAGES_DIR}
  "share/gdal/*")
list(APPEND ENV_FILE_LIST ${GDAL_DATA_FILES})

message(STATUS "GDAL: Will download & build Python bindings for v${GDAL_VERSION}")

file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/gdal_py-prefix/setup.cfg
  CONTENT
    "[build_ext]
include_dirs=${GDAL_INCLUDE_DIRS}
library_dirs=${GDAL_LIBRARY_DIRS}
libraries=gdal
gdal_config=${CURRENT_PACKAGES_DIR}/tools/gdal/gdal-config
")

ExternalProject_Add(
  gdal_py
  URL https://pypi.org/packages/source/G/GDAL/GDAL-${GDAL_VERSION}.tar.gz
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv GDAL::GDAL
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/gdal_py-prefix/setup.cfg
                <SOURCE_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build_ext --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist --bdist-dir <TMP_DIR>/bdist
  INSTALL_COMMAND
    ${CMAKE_COMMAND} -E copy
    <TMP_DIR>/dist/GDAL-${GDAL_VERSION}-${Python3_WHEEL_ID}.whl <INSTALL_DIR>)
ExternalProject_Get_Property(gdal_py INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/GDAL-${GDAL_VERSION}-${Python3_WHEEL_ID}.whl)

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
  set(CMAKE_FIND_LIBRARY_PREFIXES "") # otherwise it looks for `libmod_spatialite`
  find_library(
    SPATIALITE_EXTENSION
    NAMES mod_spatialite REQUIRED
    HINTS ${SPATIALITE_LIBDIR}
    NO_DEFAULT_PATH)
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

#
# Git-LFS
#

set(GIT_LFS_OUTPUT "bin/git-lfs${CMAKE_EXECUTABLE_SUFFIX}")
if(WIN32)
  set(GIT_LFS_LDFLAGS "-s -w -X 'github.com/git-lfs/git-lfs/v3/config.Vendor=Kart'")
  set(GIT_LFS_BUILD go build -ldflags=${GIT_LFS_LDFLAGS} -trimpath -o ${GIT_LFS_OUTPUT})
else()
  set(GIT_LFS_BUILD make VENDOR=Kart ${GIT_LFS_OUTPUT})
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
list(APPEND BIN_LIST "${GIT_LFS_EXECUTABLE}")

#
# Git Note: we define the Git dependency last since it affects GIT_VERSION_STRING etc, which can
# break other dependencies.
#
find_package(OpenSSL REQUIRED)
pkg_check_modules(OPENSSL REQUIRED openssl)
pkg_check_modules(PCRE2 REQUIRED libpcre2-8)
pkg_check_modules(LIBCURL REQUIRED libcurl)

cmake_path(SET GIT_SPATIAL_FILTER_EXT "../vendor/spatial-filter/spatial.a")
cmake_path(ABSOLUTE_PATH GIT_SPATIAL_FILTER_EXT)

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
    "LDFLAGS=-L${SQLITE3_LIB_DIR}"
    "EXTENSION_LIBS=-lsqlite3")

ExternalProject_Add(
  git
  GIT_REPOSITORY https://github.com/koordinates/git
  GIT_TAG kx-latest
  GIT_SHALLOW ON
  BUILD_IN_SOURCE ON
  DEPENDS CURL::libcurl OpenSSL::SSL OpenSSL::Crypto unofficial::sqlite3::sqlite3
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  INSTALL_DIR env
  BUILD_COMMAND make ${gitBuildConfig} DESTDIR=<INSTALL_DIR> all strip
  INSTALL_COMMAND make ${gitBuildConfig} DESTDIR=<INSTALL_DIR> install
  COMMAND ${CMAKE_COMMAND} -E rm -rf <INSTALL_DIR>/share/git-gui <INSTALL_DIR>/share/gitk)
set(GIT_EXECUTABLE "git-prefix/dist/bin/git${CMAKE_EXECUTABLE_SUFFIX}")

#
# General target
#
add_custom_target(
  vendor_wheels ALL
  DEPENDS pygit2 psycopg2 pysqlite3 pyodbc gdal_py cffi
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
set(VENDOR_ARCHIVE vendor-${CMAKE_SYSTEM_NAME}.tar.gz)
if(NOT PROJECT_IS_TOP_LEVEL)
  set(VENDOR_ARCHIVE
      vcpkg-vendor/${VENDOR_ARCHIVE}
      PARENT_SCOPE)
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  separate_arguments(
    SYMLINK_MOD_SPATIALITE_COMMAND UNIX_COMMAND
    "${CMAKE_COMMAND} -E create_symlink mod_spatialite.so env/lib/mod_spatialite.dylib")
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(VENDOR_FIX_EXTRA -s "${CURRENT_PACKAGES_DIR}/lib")
endif()

add_custom_command(
  OUTPUT ${VENDOR_ARCHIVE}
  DEPENDS vendor_wheels git git-lfs ${VENDOR_CONFIG} fix_vendor_libs.py
  COMMAND ${CMAKE_COMMAND} -E rm -rf env/share/git-gui
  COMMAND ${CMAKE_COMMAND} -E make_directory wheelhouse env env/lib env/share
  COMMAND ${CMAKE_COMMAND} -E copy ${WHEEL_LIST} wheelhouse
  COMMAND ${CMAKE_COMMAND} -E copy ${LIB_LIST} env/lib
  COMMAND ${SYMLINK_MOD_SPATIALITE_COMMAND}
  COMMAND ${CMAKE_COMMAND} -E copy ${BIN_LIST} env/bin
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CURRENT_PACKAGES_DIR}/share/gdal/ env/share/gdal
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CURRENT_PACKAGES_DIR}/share/proj/ env/share/proj
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${Python3_STDLIB} env/lib/${Python3_STDLIB_DIR_NAME}
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${Python3_INCLUDE_DIRS}
          env/include/${Python3_INCLUDE_DIR_NAME}
  COMMAND ${wheelBuildEnv_PYTHON} "${CMAKE_CURRENT_SOURCE_DIR}/fix_vendor_libs.py"
          $<$<BOOL:$ENV{VERBOSE}>:-vv> . ${VENDOR_ARCHIVE} ${VENDOR_FIX_EXTRA}
  COMMENT "Vendor dependency archive assembly")

add_custom_target(
  vendor_archive
  DEPENDS ${VENDOR_ARCHIVE}
  COMMENT "Vendor dependency archive")

add_custom_target(
  vendor ALL
  DEPENDS vendor_archive
  COMMENT "Vendor dependencies")
