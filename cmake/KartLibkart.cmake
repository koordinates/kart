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
  libkart
  DEPENDS ${LIBKART_LIB}
  COMMENT "libkart C-ABI library")
