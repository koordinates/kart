cmake_minimum_required(VERSION 3.21)

# set the project name
project(
  "libkart"
  VERSION 0.0.0
  LANGUAGES C CXX)

option(BUILD_PYTHON_BINDINGS "Build Python bindings using cython" ON)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../vendor/cmake)

# use a decently modern c++
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_BUILD_TYPE Debug)

# make it way debuggable
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_C_FLAGS_DEBUG "-g -O0")

# https://stackoverflow.com/a/54606606/988527
if(APPLE)
  set(CMAKE_THREAD_LIBS_INIT "-lpthread")
  set(CMAKE_HAVE_THREADS_LIBRARY 1)
  set(CMAKE_USE_WIN32_THREADS_INIT 0)
  set(CMAKE_USE_PTHREADS_INIT 1)
  set(THREADS_PREFER_PTHREAD_FLAG ON)
endif()

include(FindPackageHandleStandardArgs)

# libfmt, since <format> isn't widespread in compilers yet
find_package(fmt)

add_subdirectory(cppgit2 ${CMAKE_BINARY_DIR}/cppgit2)

# build our own library
add_library(
  kart SHARED
  # need to list *ALL* cpp files here
  src/kart.cpp src/kart/repo.cpp src/kart/structure.cpp src/kart/dataset3.cpp
  src/kart/tree_walker.cpp)
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
