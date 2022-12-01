cmake_minimum_required(VERSION 3.21)

if(USE_VCPKG)
  # this needs to be super-early, before the first project() call
  include("vcpkg-vendor/vcpkg-toolchain-setup.cmake")
endif()

project(
  kart
  VERSION 0.12.0
  DESCRIPTION "Version control for geospatial data"
  HOMEPAGE_URL "https://kartproject.org"
  LANGUAGES C CXX)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

include("vcpkg-vendor/osx-setup.cmake")
include(PythonGetABIInfo)
include(CCache)

# find_package(Git REQUIRED) execute_process( COMMAND ${GIT_EXECUTABLE} rev-parse HEAD
# RESULT_VARIABLE result OUTPUT_VARIABLE KART_GIT_COMMIT OUTPUT_STRIP_TRAILING_WHITESPACE )
# if(result) message(FATAL_ERROR "Failed to get Git commit: ${result}") endif()

#
# options
#

option(USE_VCPKG "Use vcpkg for vendor dependencies")
set(VENDOR_ARCHIVE
    ""
    CACHE
      FILEPATH
      "Use vendor packages from CI: path to vendor archive file from https://github.com/koordinates/kart"
)

#
# setup
#

set(CMAKE_TLS_VERIFY TRUE)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(LINUX ON)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(MACOS ON)
endif()

if(PROJECT_IS_TOP_LEVEL)
  include(CTest)

  if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    if(MACOS)
      set(CMAKE_INSTALL_PREFIX
          "/Applications"
          CACHE PATH "Install path prefix" FORCE)
    elseif(LINUX)
      set(CMAKE_INSTALL_PREFIX
          "/opt"
          CACHE PATH "Install path prefix" FORCE)
    endif()
  endif()
endif()

# OSX: prefer Homebrew over Frameworks
set(Python3_FIND_IMPLEMENTATIONS "CPython")
find_package(Python3 3.9 REQUIRED COMPONENTS Development Interpreter)
pythongetabiinfo()

if(USE_VCPKG)
  message("Using VCPKG for vendor dependencies...")
  add_subdirectory(vcpkg-vendor)
  set(VENDOR_TARGET vendor)
else()
  #
  # dependencies
  #
  if(VENDOR_ARCHIVE)
    message("Using prebuilt vendor dependencies from ${VENDOR_ARCHIVE} ...")
    if(NOT EXISTS ${VENDOR_ARCHIVE})
      message(
        FATAL_ERROR
          "${VENDOR_ARCHIVE} not found. Download from https://github.com/koordinates/kart/actions")
    endif()
    set(VENDOR_TARGET ${VENDOR_ARCHIVE})
  else()
    message("Using local vendor dependencies...")
    add_subdirectory(vendor)
    set(VENDOR_TARGET vendor)
  endif()
endif()

set(DOCS "docs/pages/commands")
file(COPY ${DOCS} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
set(DOCS "scripts")
file(COPY ${DOCS} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

#
# main build targets
#
include(KartPy)
include(KartBundle)

#
# Tests
#
if(BUILD_TESTING)
  #
  # Main unit tests (label=pytest)
  #
  if(DEFINED LINUX AND DEFINED ENV{KART_SQLSERVER_URL})
    # pytest-xdist causes some weird behaviour with pyodbc on Linux. Split MSSQL tests out into a
    # separate pytest run.
    add_test(
      NAME pytest
      COMMAND ${VENV_PYTEST} -v -m "not\ mssql" ${PYTEST_ARGS}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    set_property(TEST pytest PROPERTY LABELS "pytest")

    add_test(
      NAME pytest-mssql
      COMMAND ${VENV_PYTEST} -v -p no:xdist -m mssql ${PYTEST_ARGS}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    set_property(TEST pytest-mssql PROPERTY LABELS "pytest")
  else()
    add_test(
      NAME pytest
      COMMAND ${VENV_PYTEST} -v ${PYTEST_ARGS}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    set_property(TEST pytest PROPERTY LABELS "pytest")
  endif()

  #
  # E2E tests for bundles/packages (label=e2e)
  #
  find_program(
    SQLite3_EXE
    NAMES sqlite3
    PATHS "${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}"
    PATH_SUFFIXES tools share)
  cmake_path(GET SQLite3_EXE PARENT_PATH SQLite3_EXE_DIR)
  if(WIN32)
    add_test(
      NAME e2e-1
      COMMAND Powershell.exe -File "tests\\scripts\\e2e-1.ps1"
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  else()
    add_test(
      NAME e2e-1
      COMMAND tests/scripts/e2e-1.sh
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  endif()
  set_property(TEST e2e-1 PROPERTY LABELS "e2e")
  set_property(
    TEST e2e-1
    PROPERTY ENVIRONMENT_MODIFICATION
             "PATH=path_list_prepend:${SQLite3_EXE_DIR};PATH=path_list_prepend:${BUNDLE_EXE_DIR}")
endif()

#
# Installation
#

install(
  DIRECTORY ${BUNDLE_DIR}
  DESTINATION "."
  USE_SOURCE_PERMISSIONS
  COMPONENT bundle)
if(NOT WIN32)
  set(KART_SYSTEM_SYMLINK_DIR
      /usr/local/bin
      CACHE PATH "Directory to create the system path kart symlink in")
  if(KART_SYSTEM_SYMLINK_DIR)
    install(
      CODE "message(STATUS \"Installing link: ${KART_SYSTEM_SYMLINK_DIR}/kart -> $<PATH:APPEND,\${CMAKE_INSTALL_PREFIX},${BUNDLE_PREFIX_REL_EXE}>\")"
      CODE "file(CREATE_LINK \"$<PATH:APPEND,\${CMAKE_INSTALL_PREFIX},${BUNDLE_PREFIX_REL_EXE}>\" \"${KART_SYSTEM_SYMLINK_DIR}/kart\" SYMBOLIC)"
      COMPONENT addToPath)
  endif()
endif()
