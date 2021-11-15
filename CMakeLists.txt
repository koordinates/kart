cmake_minimum_required(VERSION 3.21)

project(
  KART
  VERSION 0.10.6
  LANGUAGES C CXX)

enable_testing()

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
set(KART_CMAKE_TEMPLATE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/cmake/template)

include(PythonGetABIInfo)

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

# CCache
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
  set(ccacheEnv CCACHE_CPP2=true CCACHE_BASEDIR=${CMAKE_BINARY_DIR}
                CCACHE_SLOPPINESS=pch_defines,time_macros)
  foreach(lang IN ITEMS C CXX)
    set(CMAKE_${lang}_COMPILER_LAUNCHER ${CMAKE_COMMAND} -E env ${ccacheEnv} ${CCACHE_PROGRAM})
  endforeach()
endif()

# OSX: prefer Homebrew over Frameworks
set(Python3_FIND_IMPLEMENTATIONS "CPython")
find_package(Python3 3.7 REQUIRED COMPONENTS Interpreter)
pythongetabiinfo()

# version configure_file(${KART_CMAKE_TEMPLATE_DIR}/VERSION.in kart/VERSION @ONLY)

#
# dependencies
#
if(VENDOR_ARCHIVE)
  message("Using prebuilt vendor dependencies...")
  if(NOT EXISTS ${VENDOR_ARCHIVE})
    message(
      FATAL_ERROR
        "${VENDOR_ARCHIVE} not found. Download from https://github.com/koordinates/kart/actions")
  endif()
  # if it's a .zip, extract it to get the inner archive
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
add_test(
  NAME pytest
  COMMAND ${VENV_EXEC_ENV} pytest -v
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

# packaging
