# Source:
# https://github.com/brson/heka-rs/blob/26b6010c0f3b15f797206254e1bd62ad3a83b414/cmake/FindRust.cmake#L4

# This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0. If a copy of
# the MPL was not distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

# The module defines the following variables: RUST_FOUND - true if the Rust was found
# RUST_EXECUTABLE - path to the executable RUST_VERSION - Rust version number Example usage:
# find_package(Rust 0.12.0 REQUIRED)

find_program(
  RUST_EXECUTABLE rustc
  PATHS
  PATH_SUFFIXES bin)
if(RUST_EXECUTABLE)
  execute_process(
    COMMAND ${RUST_EXECUTABLE} -V
    OUTPUT_VARIABLE RUST_VERSION_OUTPUT
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(RUST_VERSION_OUTPUT MATCHES "rustc ([0-9]+\\.[0-9]+\\.[0-9]+)")
    set(RUST_VERSION ${CMAKE_MATCH_1})
  endif()
endif()
mark_as_advanced(RUST_EXECUTABLE)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  Rust
  REQUIRED_VARS RUST_EXECUTABLE RUST_VERSION
  VERSION_VAR RUST_VERSION)
