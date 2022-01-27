cmake_minimum_required(VERSION 3.15)

# set the project name
project(
  "libkart"
  VERSION 0.0.0
  LANGUAGES CXX)

# use a decently modern c++
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_BUILD_TYPE Debug)

include(FindPackageHandleStandardArgs)

# tell it how to find libgit2 FIXME: move to a FindLibgit2 module
find_path(GIT2_INCLUDE_PATH NAMES git2.h)
find_library(GIT2_LIBRARY NAMES git2)
find_package_handle_standard_args(libgit2 REQUIRED_VARS GIT2_LIBRARY GIT2_INCLUDE_PATH)
add_library(git2 UNKNOWN IMPORTED)
set_target_properties(
  git2
  PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${GIT2_INCLUDE_PATH}"
             IMPORTED_LINK_INTERFACE_LANGUAGES "C"
             IMPORTED_LOCATION "${GIT2_LIBRARY}")

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
