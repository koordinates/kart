cmake_minimum_required(VERSION 3.21)

# set the project name
project(
  "libkart"
  VERSION 0.0.0
  LANGUAGES C CXX)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../vendor/cmake)

# use a decently modern c++
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_BUILD_TYPE Debug)

# make it way debuggable
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_C_FLAGS_DEBUG "-g -O0")

include(FindPackageHandleStandardArgs)

find_package(LibGit2 1.3.0 REQUIRED)

# libfmt, since <format> isn't widespread in compilers yet
find_package(fmt)

add_subdirectory(cppgit2 EXCLUDE_FROM_ALL)

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
