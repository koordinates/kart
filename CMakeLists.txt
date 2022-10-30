cmake_minimum_required(VERSION 3.21)

if(USE_VCPKG)
  # this needs to be super-early, before the first project() call
  include("vcpkg-vendor/vcpkg-toolchain-setup.cmake")
endif()

project(
  KART
  VERSION 0.11.6
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

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(LINUX ON)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(MACOS ON)
endif()

if(USE_VCPKG)
  message("Using VCPKG for vendor dependencies...")
  add_subdirectory(vcpkg-vendor)
  set(VENDOR_TARGET vendor)
else()
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
