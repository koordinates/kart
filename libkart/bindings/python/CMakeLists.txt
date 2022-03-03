set(Python3_FIND_IMPLEMENTATIONS "CPython")
find_package(Python3 3.7 REQUIRED COMPONENTS Interpreter)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../../../cmake)
include(PyCreateVirtualEnvironment)

createvirtualenvironment(cythonEnv REQUIREMENTS "cython==3.0.0a10;wheel")

# Prepare Python's setup.cfg
file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/setup.cfg
  CONTENT
    "[build_ext]
library_dirs=$<TARGET_FILE_DIR:kart>
include_dirs=$<JOIN:$<TARGET_PROPERTY:kart,INCLUDE_DIRECTORIES>,:>
rpath=$<TARGET_FILE_DIR:kart>
")

set(PYBUILD_ENV
    ${CMAKE_COMMAND}
    -E
    env
    "CXX=${CMAKE_CXX_COMPILER}"
    "CC=${CMAKE_C_COMPILER}"
    "CFLAGS=${CMAKE_C_FLAGS}"
    "CXXFLAGS=${CMAKE_CXX_FLAGS}")

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/py-bindings.stamp
  COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/tmp
  COMMAND ${PYBUILD_ENV} ${cythonEnv_PYTHON} setup.py --no-user-cfg build_ext
          --build-temp=${CMAKE_CURRENT_BINARY_DIR}/tmp --build-lib=${CMAKE_CURRENT_BINARY_DIR}
  COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/py-bindings.stamp
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS cythonEnv setup.py setup.cfg libkart_.pxd libkart.pyx kart
  BYPRODUCTS ${CMAKE_CURRENT_SOURCE_DIR}/libkart.cpp
  COMMENT "Building Python bindings to libkart")

add_custom_target(
  kart-python-bindings ALL
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/py-bindings.stamp
  COMMENT "libkart Python bindings")
