cmake_minimum_required(VERSION 3.22)

set(CMAKE_TOOLCHAIN_FILE "${CMAKE_CURRENT_LIST_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake" CACHE STRING "")
set(VCPKG_OVERLAY_PORTS "${CMAKE_CURRENT_LIST_DIR}/vcpkg-overlay-ports")
set(VCPKG_OVERLAY_TRIPLETS "${CMAKE_CURRENT_LIST_DIR}/vcpkg-overlay-triplets")
set(VCPKG_BUILD_TYPE release)

# equivalent of 11.0 on arm64
set(CMAKE_OSX_DEPLOYMENT_TARGET "10.15")

project(kart_vendor
    LANGUAGES C CXX
)

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

include(ExternalProject)

# function(copy_lib_symlinks tgt output)
#   if(NOT TARGET ${tgt})
#     message("There is no target named '${tgt}'")
#     return()
#   endif()

#   get_target_property(location ${tgt} LOCATION)  # /path/to/libfoo.1.2.3.dylib / .so.1.2.3
#   cmake_path(GET ${location} FILENAME dir)  # /path/to
#   cmake_path(GET ${location} FILENAME fullname)  # libfoo.1.2.3.dylib / libfoo.so.1.2.3
#   cmake_path(GET ${location} FILENAME name)  # libfoo.1.2.3.dylib / libfoo.so.1.2.3
#   cmake_path(GET ${location} EXTENSION ext)  # .1.2.3.dylib / .so.1.2.3
#   cmake_path(GET ${location} STEM stem)      # libfoo

#   add_custom_target(${name}_lib_symlinks)

#   if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
#     # libfoo.1.2.3.dylib
#     # libfoo.1.2.dylib -> libfoo.1.2.3.dylib
#     # libfoo.1.dylib -> libfoo.1.2.3.dylib
#     add_custom_target(${output}/${name}.dylib
#       COMMAND ${CMAKE_COMMAND} -E create_symlink ${name} ${out}
#     )



#   elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
#     # libfoo.so.1.2.3
#     # libfoo.so.1.2 -> libfoo.so.1.2.3
#     # libfoo.so.1 -> libfoo.so.1.2.3
#     while(NOT STREQUAL(ext) ".so")
#       add_custom_target("${dir}/${stem}${ext}"
#         COMMAND ${CMAKE_COMMAND} -E create_symlink "${fullname}" "${dir}/${stem}${ext}"
#       )

#       cmake_path(GET "${stem}${ext}" EXTENSION ext)
#     endwhile()
#   else()
#     return()
#   endif()
# endfunction(copy_lib_symlinks)

# -----------------------------------------------------------------------------
# Debugging helpers
# -----------------------------------------------------------------------------
#
# Print all properties of target
#
## https://stackoverflow.com/questions/32183975/how-to-print-all-the-properties-of-a-target-in-cmake/56738858#56738858
## https://stackoverflow.com/a/56738858/3743145
## Get all properties that cmake supports
execute_process(COMMAND cmake --help-property-list OUTPUT_VARIABLE CMAKE_PROPERTY_LIST)
## Convert command output into a CMake list
STRING(REGEX REPLACE ";" "\\\\;" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")
STRING(REGEX REPLACE "\n" ";" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")
list(REMOVE_DUPLICATES CMAKE_PROPERTY_LIST)
function(print_target_properties tgt)
    if(NOT TARGET ${tgt})
      message("There is no target named '${tgt}'")
      return()
    endif()

    foreach (prop ${CMAKE_PROPERTY_LIST})
        string(REPLACE "<CONFIG>" "${CMAKE_BUILD_TYPE}" prop ${prop})
        get_target_property(propval ${tgt} ${prop})
        if (propval)
            message(STATUS "${tgt} ${prop} = ${propval}")
        endif()
    endforeach(prop)
endfunction(print_target_properties)

#
# Print all variables with PREFIX
function (print_vars_prefix _prefix)
  get_cmake_property(_vars VARIABLES)
  string (REGEX MATCHALL "(^|;)${_prefix}[A-Za-z0-9_]*" _matchedVars "${_vars}")
  foreach (_var IN LISTS _matchedVars)
    message(STATUS "${_var}=${${_var}}")
  endforeach()
endfunction()
#
# -----------------------------------------------------------------------------

set(WHEEL_LIST "")
set(LIB_LIST "")
set(BIN_LIST "")

#
# Python
#
# Need Development here or VcPkg falls back to the system interpreter
find_package(Python3 3.10 COMPONENTS Development Interpreter REQUIRED)
cmake_path(GET Python3_STDLIB FILENAME Python3_STDLIB_DIR_NAME)

list(APPEND BIN_LIST ${Python3_EXECUTABLE})

include(PythonGetABIInfo)
pythongetabiinfo()

include(PyCreateVirtualEnvironment)
createvirtualenvironment(wheelBuildEnv REQUIREMENTS "wheel" "delocate")

#
# pysqlite3
#
find_package(unofficial-sqlite3 CONFIG REQUIRED)
get_target_property(SQLite3_INCLUDE_DIRS unofficial::sqlite3::sqlite3 INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(SQLite3_LIBRARIES unofficial::sqlite3::sqlite3 LOCATION)
cmake_path(GET SQLite3_LIBRARIES PARENT_PATH SQLITE3_LIB_DIR)

set(PYSQLITE3_WHEEL_VER 0.4.5)
ExternalProject_Add(
  pysqlite3
  GIT_REPOSITORY https://github.com/coleifer/pysqlite3.git
  GIT_TAG ${PYSQLITE3_WHEEL_VER}
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv unofficial::sqlite3::sqlite3
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND
    ${CMAKE_COMMAND} -E env ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build_ext --include-dirs
    ${SQLite3_INCLUDE_DIRS} --library-dirs ${SQLITE3_LIB_DIR} --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist
          --bdist-dir <TMP_DIR>/bdist
  COMMAND ${CMAKE_COMMAND} -E env ${wheelBuildEnv_EXEC} ${wheelBuildEnv_BINARY_DIR}/delocate-wheel -v -w <INSTALL_DIR> <TMP_DIR>/dist/pysqlite3-${PYSQLITE3_WHEEL_VER}-${Python3_WHEEL_ID}.whl
  INSTALL_COMMAND "")
ExternalProject_Get_Property(pysqlite3 INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/pysqlite3-${PYSQLITE3_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# psycopg2
#
find_package(PostgreSQL REQUIRED)
set(PYSCOPG2_WHEEL_VER 2.8.5)
ExternalProject_Add(
  psycopg2
  URL https://files.pythonhosted.org/packages/a8/8f/1c5690eebf148d1d1554fc00ccf9101e134636553dbb75bdfef4f85d7647/psycopg2-2.8.5.tar.gz
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv PostgreSQL::PostgreSQL
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND
    ${CMAKE_COMMAND} -E env ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build_ext --include-dirs
    $<JOIN:$<TARGET_PROPERTY:PostgreSQL::PostgreSQL,INTERFACE_INCLUDE_DIRECTORIES>,:>
    --library-dirs ${PostgreSQL_LIBRARY_DIRS} --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist
          --bdist-dir <TMP_DIR>/bdist
  COMMAND ${CMAKE_COMMAND} -E env ${wheelBuildEnv_EXEC} ${wheelBuildEnv_BINARY_DIR}/delocate-wheel -v -w <INSTALL_DIR> <TMP_DIR>/dist/psycopg2-${PYSCOPG2_WHEEL_VER}-${Python3_WHEEL_ID}.whl
  INSTALL_COMMAND "")
ExternalProject_Get_Property(psycopg2 INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/psycopg2-${PYSCOPG2_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

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
  "LIBGIT2=${PYGIT2_BUILD_ENV_LIBGIT2}"
  "LIBGIT2_LIB=${PYGIT2_BUILD_ENV_LIBGIT2_LIB}"
  "CFLAGS=${PYGIT2_BUILD_ENV_CFLAGS}"
  "LDFLAGS=${PYGIT2_BUILD_ENV_LDFLAGS}"
)

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
  COMMAND ${CMAKE_COMMAND} -E env ${PYGIT2_BUILD_ENV}
          ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist
          --bdist-dir <TMP_DIR>/bdist
  COMMAND ${CMAKE_COMMAND} -E env ${wheelBuildEnv_EXEC} ${wheelBuildEnv_BINARY_DIR}/delocate-wheel -v -w <INSTALL_DIR> <TMP_DIR>/dist/pygit2-${PYGIT2_WHEEL_VER}-${Python3_WHEEL_ID}.whl
  INSTALL_COMMAND ""
)
ExternalProject_Get_Property(pygit2 INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/pygit2-${PYGIT2_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# pyodbc
#
if(NOT WIN32)
  find_package(unixodbc CONFIG REQUIRED)
  get_target_property(ODBC_INCLUDE_DIRS UNIX::odbc INTERFACE_INCLUDE_DIRECTORIES)
  get_target_property(ODBC_LIBRARIES UNIX::odbc LOCATION)
  cmake_path(GET ODBC_LIBRARIES PARENT_PATH ODBC_LIB_DIR)

  set(PYODBC_BUILD_ENV
    "CFLAGS=-I${ODBC_INCLUDE_DIRS}"
    "LDFLAGS=-L${ODBC_LIB_DIR} -lodbc"
  )
  set(ODBC_DEPENDS UNIX::odbc)
endif()

set(PYODBC_WHEEL_VER 4.0.32)
ExternalProject_Add(
  pyodbc
  # if you build from a git repository, pyodbc adds +commit0c0ffee to the wheel version
  URL https://files.pythonhosted.org/packages/d1/21/755f5fa373d01d1803d992c7611799dc4e9fcbb5db7d0852990d6ab3c9ad/pyodbc-4.0.32.tar.gz
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv ${ODBC_DEPENDS}
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${CMAKE_COMMAND} -E env ${PYODBC_BUILD_ENV}
                ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <TMP_DIR>/dist
                --bdist-dir <TMP_DIR>/bdist
  COMMAND ${CMAKE_COMMAND} -E env ${wheelBuildEnv_EXEC} ${wheelBuildEnv_BINARY_DIR}/delocate-wheel -v -w <INSTALL_DIR> <TMP_DIR>/dist/pyodbc-${PYODBC_WHEEL_VER}-${Python3_WHEEL_ID}.whl
  INSTALL_COMMAND "")
ExternalProject_Get_Property(pyodbc INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/pyodbc-${PYODBC_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# GDAL Python module We download the bindings associated with the version we
# picked up via vcpkg/FindGDAL
#
find_package(GDAL REQUIRED)
pkg_check_modules(GDAL REQUIRED gdal)
get_target_property(GDAL_LIBRARY_PATH GDAL::GDAL LOCATION)
list(APPEND LIB_LIST ${GDAL_LIBRARY_PATH})

file(GLOB GDAL_DATA_FILES RELATIVE ${CURRENT_PACKAGES_DIR} "share/gdal/*")
list(APPEND ENV_FILE_LIST ${GDAL_DATA_FILES})

message(STATUS "GDAL: Will download & build Python bindings for v${GDAL_VERSION}")
set(PYGDAL_BUILD_GDAL_CONFIG "${VCPKG_IMPORT_PREFIX}/tools/gdal/bin/gdal-config")

ExternalProject_Add(
  gdal_py
  URL https://pypi.org/packages/source/G/GDAL/GDAL-${GDAL_VERSION}.tar.gz
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv GDAL::GDAL
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${CMAKE_COMMAND} -E env ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build_ext
                --gdal-config ${PYGDAL_BUILD_GDAL_CONFIG} --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build --build-temp <TMP_DIR>/build
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <INSTALL_DIR>
          --bdist-dir <TMP_DIR>/bdist
  INSTALL_COMMAND "")
ExternalProject_Get_Property(gdal_py INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/GDAL-${GDAL_VERSION}-${Python3_WHEEL_ID}.whl)

#
# Proj
#
find_package(PROJ CONFIG REQUIRED)
file(GLOB PROJ_DATA_FILES RELATIVE ${CURRENT_PACKAGES_DIR} "share/proj/*")
list(APPEND ENV_FILE_LIST ${PROJ_DATA_FILES})


#
# mod_spatialite
#
pkg_check_modules(SPATIALITE REQUIRED spatialite)
FUNCTION( find_mod_spatialite )
  set(CMAKE_FIND_LIBRARY_PREFIXES "")  # otherwise it looks for `libmod_spatialite`
  find_library(
    SPATIALITE_EXTENSION
    NAMES mod_spatialite
    REQUIRED
    HINTS ${SPATIALITE_LIBDIR}
    NO_DEFAULT_PATH)
  cmake_path(GET SPATIALITE_EXTENSION PARENT_PATH SPATIALITE_EXTENSION_DIR)
  set(SPATIALITE_EXTENSION "${SPATIALITE_EXTENSION}" PARENT_SCOPE)
  set(SPATIALITE_EXTENSION_DIR "${SPATIALITE_EXTENSION_DIR}" PARENT_SCOPE)
ENDFUNCTION ()
find_mod_spatialite()
list(APPEND LIB_LIST ${SPATIALITE_EXTENSION})

#
# libspatialindex
#
find_package(libspatialindex CONFIG REQUIRED)
get_target_property(SIDX_C_LIBRARY spatialindex_c LOCATION)
get_target_property(SIDX_CPP_LIBRARY spatialindex LOCATION)
list(APPEND LIB_LIST ${SIDX_C_LIBRARY} ${SIDX_CPP_LIBRARY})

#
# PDAL
#

find_package(PDAL CONFIG REQUIRED)
pkg_check_modules(PDAL REQUIRED pdal)

get_target_property(PDALUTIL_LIBRARY pdal_util LOCATION)
get_target_property(PDALCPP_LIBRARY pdalcpp LOCATION)
list(APPEND LIB_LIST ${PDALCPP_LIBRARY} ${PDALUTIL_LIBRARY})
list(APPEND BIN_LIST "${VCPKG_IMPORT_PREFIX}/tools/pdal/pdal")

print_target_properties(pdalcpp)
print_target_properties(pdal_util)

#
# Git-LFS
#

ExternalProject_Add(
  git-lfs
  GIT_REPOSITORY https://github.com/git-lfs/git-lfs
  GIT_TAG main
  GIT_SHALLOW ON
  BUILD_IN_SOURCE ON
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND make "VERSION=v3.2.0-46-gc6497f8b"
  INSTALL_COMMAND ""
)
set(GIT_LFS_EXECUTABLE "git-lfs-prefix/src/git-lfs/bin/git-lfs${CMAKE_EXECUTABLE_SUFFIX}")
list(APPEND BIN_LIST "${GIT_LFS_EXECUTABLE}")

#
# Git
# Note: we define the Git dependency last since it affects GIT_VERSION_STRING etc,
# which can break other dependencies.
#
include(FindOpenSSL)
pkg_check_modules(PCRE2 REQUIRED libpcre2-8)
pkg_check_modules(OPENSSL REQUIRED openssl)
pkg_check_modules(LIBCURL REQUIRED libcurl)
list(JOIN LIBCURL_STATIC_CFLAGS " " GIT_BUILD_CURL_CFLAGS)
list(JOIN LIBCURL_STATIC_LDFLAGS " " GIT_BUILD_CURL_LDFLAGS)

cmake_path(SET GIT_SPATIAL_FILTER_EXT "../vendor/spatial-filter/spatial.a")
cmake_path(ABSOLUTE_PATH GIT_SPATIAL_FILTER_EXT)
set(GIT_BUILD_LDFLAGS "-L${SQLITE3_LIB_DIR} -lsqlite3 -lstdc++")

set(gitBuildConfig
  "V=1"
  "NO_GETTEXT=YesPlease"
  "INSTALL_SYMLINKS=1"
  "USE_LIBPCRE=1"
  "NO_EXPAT=1" "NO_PERL=1"
  "RUNTIME_PREFIX=1"
  "LIBPCREDIR=${PCRE2_PREFIX}"
  "OPENSSLDIR=${OPENSSL_PREFIX}"
  # stuff for building the spatial partial clone filter extension
  "LINK=${CMAKE_CXX_COMPILER}"
  "FILTER_EXTENSIONS=${GIT_SPATIAL_FILTER_EXT}"
)

ExternalProject_Add(
  git
  GIT_REPOSITORY https://github.com/koordinates/git
  GIT_TAG kart-v0.11.0
  GIT_SHALLOW ON
  BUILD_IN_SOURCE ON
  DEPENDS CURL::libcurl OpenSSL::SSL OpenSSL::Crypto unofficial::sqlite3::sqlite3
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  INSTALL_DIR env
  BUILD_COMMAND make
    prefix=/ "DESTDIR=<INSTALL_DIR>"
    ${gitBuildConfig}
    "CFLAGS=-I<SOURCE_DIR>" "LDFLAGS=${GIT_BUILD_LDFLAGS}"
    "CURL_CFLAGS=${GIT_BUILD_CURL_CFLAGS}" "CURL_LDFLAGS=${GIT_BUILD_CURL_LDFLAGS}"
    all strip
  INSTALL_COMMAND make
    prefix=/ "DESTDIR=<INSTALL_DIR>"
    ${gitBuildConfig}
    "CFLAGS=-I<SOURCE_DIR>" "LDFLAGS=${GIT_BUILD_LDFLAGS}"
    "CURL_CFLAGS=${GIT_BUILD_CURL_CFLAGS}" "CURL_LDFLAGS=${GIT_BUILD_CURL_LDFLAGS}"
    install
  COMMAND ${CMAKE_COMMAND} -E rm -rf <INSTALL_DIR>/share/git-gui <INSTALL_DIR>/share/gitk
  )
set(GIT_EXECUTABLE "git-prefix/dist/bin/git${CMAKE_EXECUTABLE_SUFFIX}")

#
# General target
#
add_custom_target(
  vendor_wheels ALL
  DEPENDS pygit2 psycopg2 pysqlite3 pyodbc gdal_py
  COMMENT "Python vendor wheels")

set(VENDOR_CONFIG _kart_env.py)
set(VENDOR_CONFIG
    vendor/_kart_env.py
    PARENT_SCOPE)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/cmake/template/_kart_env.py.in ${VENDOR_CONFIG}
               ESCAPE_QUOTES)

#
# Vendor Dist Archive
#
set(VENDOR_ARCHIVE vendor-${CMAKE_SYSTEM_NAME}.tar.gz)
set(VENDOR_ARCHIVE
    vcpkg-vendor/${VENDOR_ARCHIVE}
    PARENT_SCOPE)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(SYMLINK_MOD_SPATIALITE_COMMAND "${CMAKE_COMMAND} -E create_symlink mod_spatialite.so env/lib/mod_spatialite.dylib")
  separate_arguments(SYMLINK_MOD_SPATIALITE_COMMAND)
endif()

add_custom_command(
  OUTPUT ${VENDOR_ARCHIVE}
  DEPENDS vendor_wheels git git-lfs ${VENDOR_CONFIG}
  COMMAND ${CMAKE_COMMAND} -E rm -rf env/share/git-gui
  COMMAND ${CMAKE_COMMAND} -E make_directory wheelhouse env env/lib env/share
  COMMAND ${CMAKE_COMMAND} -E copy ${WHEEL_LIST} wheelhouse
  COMMAND ${CMAKE_COMMAND} -E copy ${LIB_LIST} env/lib
  COMMAND ${SYMLINK_MOD_SPATIALITE_COMMAND}
  COMMAND ${CMAKE_COMMAND} -E copy ${BIN_LIST} env/bin
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CURRENT_PACKAGES_DIR}/share/gdal/ env/share/gdal
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CURRENT_PACKAGES_DIR}/share/proj/ env/share/proj
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${Python3_STDLIB} env/lib/${Python3_STDLIB_DIR_NAME}
  COMMAND ${CMAKE_COMMAND} -E tar czf ${VENDOR_ARCHIVE} -- wheelhouse env ${VENDOR_CONFIG}
  COMMENT "Vendor dependency archive")
add_custom_target(
  vendor_archive
  DEPENDS ${VENDOR_ARCHIVE}
  COMMENT "Vendor dependency archive")

add_custom_target(
  vendor ALL
  DEPENDS vendor_archive
  COMMENT "Vendor dependencies")
