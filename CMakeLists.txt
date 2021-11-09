cmake_minimum_required(VERSION 3.21)

project(
  KART
  VERSION 0.10.6
  LANGUAGES C CXX)
enable_testing()

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
set(KART_CMAKE_TEMPLATE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/cmake/template)

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
find_package(Python3 3.7...<3.8 REQUIRED COMPONENTS Interpreter)
# FIXME: Is this really the best way?
set(PY_VER_CODE
    "cp${Python3_VERSION_MAJOR}${Python3_VERSION_MINOR}-cp${Python3_VERSION_MAJOR}${Python3_VERSION_MINOR}m"
)
if(MACOS)
  if(NOT PY_MACOS_DEPLOYMENT)
    execute_process(
      COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/cmake/macos_get_deployment.sh "${Python3_EXECUTABLE}"
      OUTPUT_VARIABLE PY_MACOS_DEPLOYMENT
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    if((NOT ${RESULT_VARIABLE} EQUAL 0) OR (NOT ${PY_MACOS_DEPLOYMENT}))
      message(
        FATAL_ERROR
          "Can't determine MacOS Deployment target for ${Python3_EXECUTABLE}. Set via PY_MACOS_DEPLOYMENT=X.Y"
      )
    endif()
  endif()
  string(REPLACE "." "_" PY_MACOS_DEPLOYMENT_ID ${PY_MACOS_DEPLOYMENT})
  set(PY_WHEELID "${PY_VER_CODE}-macosx_${PY_MACOS_DEPLOYMENT_ID}_${CMAKE_SYSTEM_PROCESSOR}")
elseif(LINUX)
  set(PY_WHEELID "${PY_VER_CODE}-linux_${CMAKE_SYSTEM_PROCESSOR}")
endif()
message(VERBOSE "Python wheel identifier: ${PY_WHEELID}")

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
  if(vendor_ext STREQUAL ".zip")
    set(VENDOR_TMP ${CMAKE_CURRENT_BINARY_DIR}/vendor-tmp)
    message("Extracting CI artifact ${VENDOR_ARCHIVE} to ${VENDOR_TMP} ...")
    file(REMOVE_RECURSE ${VENDOR_TMP})
    # cmake-lint: disable=E1126
    file(ARCHIVE_EXTRACT INPUT ${VENDOR_ARCHIVE} DESTINATION ${VENDOR_TMP} VERBOSE)
    set(VENDOR_ARCHIVE ${VENDOR_TMP}/vendor-${CMAKE_SYSTEM_NAME}.tar.gz)
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
