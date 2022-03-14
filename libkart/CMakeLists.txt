cmake_minimum_required(VERSION 3.21)

# set the project name
project(
  "libkart"
  VERSION 0.0.0
  LANGUAGES C CXX)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/../cmake
     ${CMAKE_CURRENT_LIST_DIR}/../vendor/cmake)

include(CCache)

# use a decently modern c++
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_BUILD_TYPE Debug)

# libfmt, since <format> isn't widespread in compilers yet
find_package(fmt)

find_package(LibGit2 1.4.0 REQUIRED)

# build libkart
add_library(kart SHARED "")
add_subdirectory(src)
add_subdirectory(include)

set_target_properties(kart PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(kart PUBLIC "include")
target_link_libraries(kart PRIVATE fmt::fmt git2)

option(WITH_LIBKART_PYTHON "Build Python bindings for Libkart" ON)
if(WITH_LIBKART_PYTHON)
  add_subdirectory(bindings/python)
endif()
