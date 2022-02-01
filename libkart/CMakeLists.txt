cmake_minimum_required(VERSION 3.21)

# set the project name
project(
  "libkart"
  VERSION 0.0.0
  LANGUAGES C CXX)

option(BUILD_PYTHON_BINDINGS "Build Python bindings using cython" ON)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../vendor/cmake)

# use a decently modern c/c++
set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_BUILD_TYPE Debug)

include(FindPackageHandleStandardArgs)

find_package(LibGit2 1.3.0 REQUIRED)

# libfmt, since <format> isn't widespread in compilers yet
find_package(fmt)

add_subdirectory(cppgit2 EXCLUDE_FROM_ALL)

# build our own library
add_library(
  kart SHARED
  # need to list *ALL* cpp files here
  src/kart.cpp src/kart/repo.cpp src/kart/structure.cpp src/kart/dataset3.cpp)
set_target_properties(kart PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(kart PUBLIC "include")
target_include_directories(kart PUBLIC cppgit2)
target_link_libraries(kart PRIVATE fmt::fmt cppgit2)

if(BUILD_PYTHON_BINDINGS)
  find_package(PythonInterp 3.7 REQUIRED)
  find_package(PythonLibs 3.7 REQUIRED)

  # Prepare Python's setup.cfg
  file(
    GENERATE
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/bindings/python/setup.cfg
    CONTENT
      "[build_ext]
library_dirs=$<TARGET_FILE_DIR:kart>
include_dirs=$<JOIN:$<TARGET_PROPERTY:kart,INCLUDE_DIRECTORIES>,:>
")

  add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/python.stamp
    COMMAND ${PYTHON_EXECUTABLE} setup.py build_ext
    COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/python.stamp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/bindings/python
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/bindings/python/setup.py
            ${CMAKE_CURRENT_SOURCE_DIR}/bindings/python/setup.cfg
            ${CMAKE_CURRENT_SOURCE_DIR}/bindings/python/libkart_.pxd
            ${CMAKE_CURRENT_SOURCE_DIR}/bindings/python/libkart.pyx
            kart
    COMMENT "Building Python bindings to libkart")
  add_custom_target(
    kart-python-bindings ALL
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/python.stamp
    COMMENT "Build Python bindings to libkart")
endif()
