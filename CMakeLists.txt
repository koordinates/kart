cmake_minimum_required(VERSION 3.21)

project(
  KART
  VERSION 0.11.3
  DESCRIPTION "Version control for geospatial data"
  HOMEPAGE_URL "https://kartproject.org"
  LANGUAGES C CXX)

enable_testing()

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

include(PythonGetABIInfo)
include(CCache)

# find_package(Git REQUIRED) execute_process( COMMAND ${GIT_EXECUTABLE} rev-parse HEAD
# RESULT_VARIABLE result OUTPUT_VARIABLE KART_GIT_COMMIT OUTPUT_STRIP_TRAILING_WHITESPACE )
# if(result) message(FATAL_ERROR "Failed to get Git commit: ${result}") endif()

#
# options
#

option(IS_HERMETIC "Isolated build where Kart builds all dependencies")
message("Hermetic build? ${IS_HERMETIC}")

set(VENDOR_ARCHIVE
    ""
    CACHE
      FILEPATH
      "Use vendor packages from CI: path to vendor archive file from https://github.com/koordinates/kart"
)

#
# setup
#

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(LINUX ON)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(MACOS ON)
endif()

# OSX: prefer Homebrew over Frameworks
set(Python3_FIND_IMPLEMENTATIONS "CPython")
find_package(Python3 3.7 REQUIRED COMPONENTS Interpreter)
pythongetabiinfo()

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
  # CI artifacts are zips: extract it to get the inner vendor archive
  cmake_path(GET VENDOR_ARCHIVE EXTENSION LAST_ONLY vendor_ext)
  if(vendor_ext STREQUAL ".zip" AND NOT (WIN32 AND DEFINED ENV{CI}))
    message("Extracting CI artifact ${VENDOR_ARCHIVE}...")
    # cmake-lint: disable=E1126
    file(
      ARCHIVE_EXTRACT
      INPUT
      ${VENDOR_ARCHIVE}
      DESTINATION
      ${CMAKE_CURRENT_BINARY_DIR}
      PATTERNS
      vendor-${CMAKE_SYSTEM_NAME}.*
      VERBOSE)
    if(WIN32)
      set(VENDOR_ARCHIVE vendor-${CMAKE_SYSTEM_NAME}.zip)
    else()
      set(VENDOR_ARCHIVE vendor-${CMAKE_SYSTEM_NAME}.tar.gz)
    endif()
  endif()
  set(VENDOR_TARGET ${VENDOR_ARCHIVE})
else()
  message("Using local vendor dependencies...")
  add_subdirectory(vendor)
  set(VENDOR_TARGET vendor)
endif()

#
# main build targets
#
include(KartPy)

# install

#
# Tests
#
if(WIN32 OR DEFINED ENV{CI})
  # xdist isn't reliable in CI
  set(PYTEST_DEFAULT_ARGS -p no:xdist)
endif()
set(PYTEST_ARGS
    ${PYTEST_DEFAULT_ARGS}
    CACHE STRING "pytest arguments")

if(DEFINED ENV{CI})
  message(STATUS "CTest: configuring for CI")
  if(WIN32 OR DEFINED ENV{PYTEST_SHARDS})
    set(PYTEST_SHARDS
        20
        CACHE STRING "Number of pytest shards")
    message(STATUS "CTest: using ${PYTEST_SHARDS} shards")

    foreach(shard RANGE 1 ${PYTEST_SHARDS} 1)
      math(EXPR shardIdx "${shard} - 1")
      add_test(
        NAME pytest-shard-${shard}
        COMMAND ${VENV_PYTEST} -vv --log-level=DEBUG ${PYTEST_ARGS} -p no:sugar --no-cov
                --num-shards ${PYTEST_SHARDS} --shard-id ${shardIdx} --benchmark-enable
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    endforeach()
  else()
    add_test(
      NAME pytest
      COMMAND
        ${VENV_PYTEST} -vv --log-level=DEBUG -p no:sugar ${PYTEST_ARGS} --cov-report term
        --cov-report "html:${CMAKE_CURRENT_BINARY_DIR}/test-results/coverage/" --junit-xml
        "${CMAKE_CURRENT_BINARY_DIR}/test-results/junit.xml" --benchmark-enable
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  endif()
else()
  add_test(
    NAME pytest
    COMMAND ${VENV_PYTEST} -v ${PYTEST_ARGS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif()

# packaging
