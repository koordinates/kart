cmake_minimum_required(VERSION 3.21)
include(FetchContent)
include(ExternalProject)
include(PyCreateVirtualEnvironment)
include(GetPrerequisites)
include(CheckSymbolExists)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake/)
find_package(Python3 3.7...<3.8 REQUIRED COMPONENTS Interpreter)

if(UNIX)
  find_package(PkgConfig REQUIRED)
endif()

if(IS_HERMETIC)
  message(FATAL_ERROR, "No hermetic builds yet, grasshopper")
endif()

# Pass CCache settings to ExternalProject builds
set(EXT_BUILD_ENV ${CMAKE_COMMAND} -E env "CXX=${CMAKE_CXX_COMPILER}" "CC=${CMAKE_C_COMPILER}")
message(VERBOSE "Using CXX=${CMAKE_CXX_COMPILER} CC=${CMAKE_C_COMPILER} for external projects")

#
# library dependencies
#

# Prefer HomeBrew packages first, since the MacOS builtin ones can be ancient and weird.
if(MACOS)
  set(CMAKE_FIND_FRAMEWORK LAST)

  if(NOT SQLite3_ROOT AND EXISTS /usr/local/opt/sqlite3)
    set(SQLite3_ROOT /usr/local/opt/sqlite3)
    list(APPEND pkgConfigPath /usr/local/opt/sqlite3/lib/pkgconfig)
    message(STATUS "Preferring Homebrew SQLite3 over macOS: ${SQLite3_ROOT}")
  endif()
  if(NOT OpenSSL_ROOT AND EXISTS /usr/local/opt/openssl@1.1)
    set(OpenSSL_ROOT /usr/local/opt/openssl@1.1)
    list(APPEND pkgConfigPath /usr/local/opt/openssl@1.1/lib/pkgconfig)
    message(STATUS "Preferring Homebrew OpenSSL over macOS: ${OpenSSL_ROOT}")
  endif()
  if(NOT CURL_ROOT AND EXISTS /usr/local/opt/curl)
    set(CURL_ROOT /usr/local/opt/curl)
    list(APPEND pkgConfigPath /usr/local/opt/curl/lib/pkgconfig)
    message(STATUS "Preferring Homebrew CURL over macOS: ${CURL_ROOT}")
  endif()
endif()

find_package(Git 2.31 REQUIRED)
find_package(GDAL 3.3.2 REQUIRED)
find_package(Spatialindex 1.9.3 REQUIRED)
find_package(SpatiaLite 5.0.1 REQUIRED)
message(VERBOSE "SpatiaLite extension: ${SpatiaLite_EXTENSION}")
if(NOT SpatiaLite_EXTENSION)
  message(
    FATAL_ERROR
      "Found SpatiaLite, but not the mod_spatialite extension. Set SpatiaLite_ROOT or SpatiaLite_EXTENSION"
  )
endif()

find_package(SQLite3 3.31.1 REQUIRED)
if(NOT SQLite3_HAS_LOAD_EXTENSION)
  if(MACOS)
    message(
      FATAL_ERROR
        "Found SQLite3, but sqlite3_enable_load_extension is disabled.
            On macOS, you should `brew install sqlite3` as the system SQLite3 library disables
            extension loading by default.")
  else()
    message(FATAL_ERROR "Found SQLite3, but sqlite3_enable_load_extension is disabled")
  endif()
endif()
find_package(PostgreSQL REQUIRED)
find_package(OpenSSL 1.1 REQUIRED)
find_package(ODBC 2.3.9 REQUIRED)
find_package(LibGit2 1.1.0 REQUIRED)
find_package(SWIG REQUIRED COMPONENTS python)

if(NOT GDAL_CONFIG)
  message(FATAL_ERROR "Found GDAL, but not gdal-config. Set GDAL_CONFIG")
endif()

# find GDAL's data directory
execute_process(
  COMMAND ${GDAL_CONFIG} --datadir
  OUTPUT_VARIABLE GDAL_DATADIR
  OUTPUT_STRIP_TRAILING_WHITESPACE)
message(VERBOSE "GDAL data directory: ${GDAL_DATADIR}")

#
# Find PROJ's data directory For the PROJ that GDAL links against
#
function(get_proj_library_path)
  # GDAL's libraries
  get_prerequisites(${GDAL_LIBRARY} DEPENDENCIES 1 0 "" "")

  foreach(lib ${DEPENDENCIES})
    cmake_path(GET lib STEM lib_stem)
    string(TOLOWER "${lib_stem}" lib_stem)
    if(lib_stem MATCHES "^(lib)?proj$")
      message(VERBOSE "Found PROJ library at ${lib}")
      set(proj_lib ${lib})
      break()
    endif()
  endforeach()
  if(proj_lib)
    set(PROJ_LIBRARY
        ${proj_lib}
        PARENT_SCOPE)
  endif()
endfunction()

if(PROJ_DATADIR)
  find_path(
    PROJ_DATADIR
    NAMES proj.db
    PATHS ${PROJ_DATADIR}
    NO_DEFAULT_PATH)
else()
  get_proj_library_path()
  message(VERBOSE "Found PROJ: ${PROJ_LIBRARY}")
  if(PROJ_LIBRARY)
    cmake_path(GET PROJ_LIBRARY PARENT_PATH PROJ_LIBRARY_DIR)
    find_path(
      PROJ_DATADIR
      NAMES proj.db
      PATHS "${PROJ_LIBRARY_DIR}/../"
      PATH_SUFFIXES share/proj
      NO_DEFAULT_PATH)
  endif()
endif()
if(NOT PROJ_DATADIR)
  message(FATAL_ERROR "Couldn't find PROJ proj.db. Set PROJ_DATADIR.")
else()
  message("Found PROJ data directory in ${PROJ_DATADIR}")
endif()

#
# Python
#
createvirtualenvironment(wheelBuildEnv REQUIREMENTS "wheel")
set(WHEEL_LIST "")

#
# pysqlite3
#
cmake_path(GET SQLite3_LIBRARIES PARENT_PATH SQLITE3_LIB_DIR)
set(PYSQLITE3_WHEEL_VER 0.4.5)
ExternalProject_Add(
  pysqlite3
  GIT_REPOSITORY https://github.com/coleifer/pysqlite3.git
  GIT_TAG ${PYSQLITE3_WHEEL_VER}
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv SQLite::SQLite3
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND
    ${EXT_BUILD_ENV} ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build_ext --include-dirs
    ${SQLite3_INCLUDE_DIRS} --library-dirs ${SQLITE3_LIB_DIR} --build-temp <TMP_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build --build-temp <TMP_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <INSTALL_DIR>
          --bdist-dir <TMP_DIR>
  INSTALL_COMMAND "")
ExternalProject_Get_Property(pysqlite3 INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/pysqlite3-${PYSQLITE3_WHEEL_VER}-${PY_WHEELID}.whl)

#
# psycopg2
#
cmake_path(GET OPENSSL_SSL_LIBRARY PARENT_PATH OPENSSL_LIB_DIR)
set(PYSCOPG2_WHEEL_VER 2.8.5)
ExternalProject_Add(
  psycopg2
  URL https://files.pythonhosted.org/packages/a8/8f/1c5690eebf148d1d1554fc00ccf9101e134636553dbb75bdfef4f85d7647/psycopg2-2.8.5.tar.gz
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv PostgreSQL::PostgreSQL OpenSSL::SSL
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND
    ${EXT_BUILD_ENV} ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build_ext --include-dirs
    ${PostgreSQL_INCLUDE_DIR}:${OPENSSL_INCLUDE_DIR} --library-dirs
    ${PostgreSQL_LIBRARY_DIR}:${OPENSSL_LIB_DIR} --build-temp <TMP_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build --build-temp <TMP_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <INSTALL_DIR>
          --bdist-dir <TMP_DIR>
  INSTALL_COMMAND "")
ExternalProject_Get_Property(psycopg2 INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/psycopg2-${PYSCOPG2_WHEEL_VER}-${PY_WHEELID}.whl)

#
# pygit2
#
cmake_path(GET LibGit2_LIBRARY PARENT_PATH LIBGIT2_LIB_DIR)
set(PYGIT2_WHEEL_VER 1.7.0)
ExternalProject_Add(
  pygit2
  GIT_REPOSITORY https://github.com/libgit2/pygit2.git
  GIT_TAG v${PYGIT2_WHEEL_VER}
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv LibGit2::LibGit2
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND
    ${EXT_BUILD_ENV} ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build_ext --include-dirs
    ${LibGit2_INCLUDE_DIR} --library-dirs ${LIBGIT2_LIB_DIR} --build-temp <TMP_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build --build-temp <TMP_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <INSTALL_DIR>
          --bdist-dir <TMP_DIR>
  INSTALL_COMMAND "")
ExternalProject_Get_Property(pygit2 INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/pygit2-${PYGIT2_WHEEL_VER}-${PY_WHEELID}.whl)

#
# pyodbc
#
if(WIN32)
  set(pyodbc_ext_args build_ext)
else()
  cmake_path(GET ODBC_LIBRARIES PARENT_PATH ODBC_LIB_DIR)
  set(pyodbc_ext_args build_ext --include-dirs ${ODBC_INCLUDE_DIRS} --library-dirs ${ODBC_LIB_DIR})
endif()
set(PYODBC_WHEEL_VER 4.0.32)
ExternalProject_Add(
  pyodbc
  # if you build from a git repository, pyodbc adds +commit0c0ffee to the wheel version
  URL https://files.pythonhosted.org/packages/d1/21/755f5fa373d01d1803d992c7611799dc4e9fcbb5db7d0852990d6ab3c9ad/pyodbc-4.0.32.tar.gz
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv SQLite::SQLite3
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${EXT_BUILD_ENV} ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg ${pyodbc_ext_args}
                --build-temp <TMP_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build --build-temp <TMP_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <INSTALL_DIR>
          --bdist-dir <TMP_DIR>
  INSTALL_COMMAND "")
ExternalProject_Get_Property(pyodbc INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/pyodbc-${PYODBC_WHEEL_VER}-${Python3_WHEEL_ID}.whl)

#
# s2_py
#
# Using a GoogleTest binary distribution doesn't appear to satisfy S2, it wants access to the source
# CMake config. Nevermind that GoogleTest is documented as being optional...
ExternalProject_Add(
  googletest_src
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.11.0
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  INSTALL_COMMAND ""
  EXCLUDE_FROM_ALL ON)
ExternalProject_Get_Property(googletest_src SOURCE_DIR)
set(GOOGLETEST_SRC_DIR ${SOURCE_DIR})
# Includes the S2 library as a submodule
set(S2PY_BUILD_ENV # "VERBOSE=1"
    "GTEST_ROOT=googletest" "OPENSSL_ROOT_DIR=${OpenSSL_ROOT}" "SWIG_EXECUTABLE=${SWIG_EXECUTABLE}"
    "SWIG_DIR=${SWIG_DIR}")
if(MACOS)
  list(APPEND S2PY_BUILD_ENV "MACOSX_DEPLOYMENT_TARGET=${PY_MACOS_DEPLOYMENT}")
endif()

set(S2PY_WHEEL_VER 0.11.0)
ExternalProject_Add(
  s2_py
  GIT_REPOSITORY https://github.com/koordinates/s2-py.git
  GIT_TAG 4815e08d0c92c4aedffed05910b47ba862088433
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv OpenSSL::SSL googletest_src
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -E create_symlink ${GOOGLETEST_SRC_DIR} googletest
  # something weird happens here - `bdist_wheel` always ends up re-running build/build_ext, so we do
  # it once
  BUILD_COMMAND ${EXT_BUILD_ENV} ${S2PY_BUILD_ENV} ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg
                bdist_wheel --dist-dir <INSTALL_DIR> --bdist-dir <TMP_DIR>
  INSTALL_COMMAND "")
ExternalProject_Get_Property(s2_py INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/s2_py-${S2PY_WHEEL_VER}-${PY_WHEELID}.whl)

#
# GDAL Python module We download the bindings associated with the version we picked up via FindGDAL
#
message("Will download & build GDAL Python bindings for v${GDAL_VERSION}")
ExternalProject_Add(
  gdal_py
  URL https://pypi.io/packages/source/G/GDAL/GDAL-${GDAL_VERSION}.tar.gz
  BUILD_IN_SOURCE ON
  DEPENDS wheelBuildEnv GDAL::GDAL
  EXCLUDE_FROM_ALL ON
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ${EXT_BUILD_ENV} ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build_ext
                --gdal-config ${GDAL_CONFIG} --build-temp <TMP_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg build --build-temp <TMP_DIR>
  COMMAND ${wheelBuildEnv_PYTHON} setup.py --no-user-cfg bdist_wheel --dist-dir <INSTALL_DIR>
          --bdist-dir <TMP_DIR>
  INSTALL_COMMAND "")
ExternalProject_Get_Property(gdal_py INSTALL_DIR)
list(APPEND WHEEL_LIST ${INSTALL_DIR}/GDAL-${GDAL_VERSION}-${PY_WHEELID}.whl)

#
# General target
#
add_custom_target(
  vendor_wheels
  DEPENDS gdal_py s2_py pygit2 psycopg2 pysqlite3 pyodbc
  COMMENT "Python vendor wheels")

#
set(VENDOR_CONFIG _env.py)
set(VENDOR_CONFIG
    vendor/_env.py
    PARENT_SCOPE)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/cmake/template/_env.py.in ${VENDOR_CONFIG} ESCAPE_QUOTES)

#
# Vendor Dist Archive
#
set(VENDOR_ARCHIVE vendor-${CMAKE_SYSTEM_NAME}.tar.gz)
set(VENDOR_ARCHIVE
    vendor/${VENDOR_ARCHIVE}
    PARENT_SCOPE)
add_custom_command(
  OUTPUT ${VENDOR_ARCHIVE}
  DEPENDS vendor_wheels ${VENDOR_CONFIG}
  COMMAND ${CMAKE_COMMAND} -E make_directory wheelhouse env
  COMMAND ${CMAKE_COMMAND} -E copy_if_different ${WHEEL_LIST} wheelhouse
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
