# Build the libkart C-ABI shared library (Rust crate at libkart/) and stage it so the PyInstaller
# bundle (kart.spec) ships it. libkart lets external processes (e.g. cave) read Kart repositories
# in-process instead of shelling out to `kart ext-run`.

find_program(CARGO_EXECUTABLE cargo REQUIRED)

set(LIBKART_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/libkart)
set(LIBKART_CARGO_TARGET_DIR ${CMAKE_CURRENT_BINARY_DIR}/libkart-target)
set(LIBKART_STAGE_DIR ${CMAKE_CURRENT_BINARY_DIR}/libkart)

# cargo's cdylib output name, and the (lib-prefixed, consistent) name we stage it under.
if(WIN32)
  set(_libkart_built_name "kart.dll")
  set(_libkart_staged_name "libkart.dll")
elseif(MACOS)
  set(_libkart_built_name "libkart.dylib")
  set(_libkart_staged_name "libkart.dylib")
else()
  set(_libkart_built_name "libkart.so")
  set(_libkart_staged_name "libkart.so")
endif()

set(LIBKART_LIB ${LIBKART_STAGE_DIR}/${_libkart_staged_name})

# Re-run cargo when any crate source changes.
file(GLOB_RECURSE _libkart_sources CONFIGURE_DEPENDS "${LIBKART_SRC_DIR}/src/*.rs")

add_custom_command(
  OUTPUT ${LIBKART_LIB}
  DEPENDS ${_libkart_sources} ${LIBKART_SRC_DIR}/Cargo.toml ${LIBKART_SRC_DIR}/Cargo.lock
  COMMAND ${CARGO_EXECUTABLE} build --release --locked --manifest-path ${LIBKART_SRC_DIR}/Cargo.toml
          --target-dir ${LIBKART_CARGO_TARGET_DIR}
  COMMAND ${CMAKE_COMMAND} -E make_directory ${LIBKART_STAGE_DIR}
  COMMAND ${CMAKE_COMMAND} -E copy_if_different
          ${LIBKART_CARGO_TARGET_DIR}/release/${_libkart_built_name} ${LIBKART_LIB}
  COMMENT "Building libkart (Rust C-ABI library)"
  VERBATIM)

add_custom_target(
  libkart ALL
  DEPENDS ${LIBKART_LIB}
  COMMENT "libkart C-ABI library")

# Tests (label "pytest" so they run under the existing ci-{linux,macos} ctest preset).
if(BUILD_TESTING)
  # Rust unit + C-ABI tests.
  add_test(
    NAME libkart-cargo-test
    COMMAND ${CARGO_EXECUTABLE} test --locked --manifest-path ${LIBKART_SRC_DIR}/Cargo.toml
            --target-dir ${LIBKART_CARGO_TARGET_DIR}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  set_property(TEST libkart-cargo-test PROPERTY LABELS "pytest")

  # Golden parity check: load the built cdylib and diff its output against Kart's own Python
  # implementation in the same process.
  add_test(
    NAME libkart-golden
    COMMAND ${VENV_PY} ${LIBKART_SRC_DIR}/tests_py/golden_check.py
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  set_property(TEST libkart-golden PROPERTY LABELS "pytest")
  set_property(TEST libkart-golden PROPERTY ENVIRONMENT "LIBKART_PATH=${LIBKART_LIB}")
endif()
