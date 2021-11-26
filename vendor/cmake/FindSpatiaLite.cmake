# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying file Copyright.txt or
# https://cmake.org/licensing for details.

# From: GDAL

#[=======================================================================[.rst:
FindSpatiaLite
--------------

CMake module to search for SpatiaLite library

IMPORTED Targets
^^^^^^^^^^^^^^^^

This module defines :prop_tgt:`IMPORTED` target ``SpatiaLite::SpatiaLite``, if
Spatialite has been found.

Result Variables
^^^^^^^^^^^^^^^^

This module defines the following variables:

``SpatiaLite_FOUND``
  True if Spatialite found.

``SpatiaLite_INCLUDE_DIRS``
  where to find Spatialite.h, etc.

``SpatiaLite_LIBRARIES``
  List of libraries when using Spatialite.

``SpatiaLite_VERSION_STRING``
  The version of Spatialite found.
#]=======================================================================]

if(CMAKE_VERSION VERSION_LESS 3.13)
  set(SpatiaLite_ROOT CACHE PATH "")
endif()

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
  pkg_check_modules(PC_SpatiaLite QUIET spatialite)
  set(SpatiaLite_VERSION_STRING ${PC_SpatiaLite_VERSION})
endif()

find_path(
  SpatiaLite_INCLUDE_DIR
  NAMES spatialite.h
  HINTS ${SpatiaLite_ROOT} ${PC_SpatiaLite_INCLUDE_DIR}
  PATH_SUFFIXES include)
find_library(
  SpatiaLite_LIBRARY
  NAMES spatialite
  HINTS ${SpatiaLite_ROOT} ${PC_SpatiaLite_LIBRARY_DIRS}
  PATH_SUFFIXES lib)
mark_as_advanced(SpatiaLite_LIBRARY SpatiaLite_INCLUDE_DIR)

if(SpatiaLite_LIBRARY
   AND SpatiaLite_INCLUDE_DIR
   AND NOT SpatiaLite_VERSION_STRING)
  file(STRINGS "${SpatiaLite_INCLUDE_DIR}/spatialite.h" _SpatiaLite_h_ver
       REGEX "^[ \t]version[ \t]([0-9]+\\.[0-9]+),.*")
  string(REGEX REPLACE "[ \t]version[ \t]([0-9]+\\.[0-9]+),.*" "\\1" _SpatiaLite_h_ver
                       ${_SpatiaLite_h_ver})
  set(SpatiaLite_VERSION_STRING "${_SpatiaLite_h_ver}")
endif()

set(_CMAKE_FIND_LIBRARY_PREFIXES ${CMAKE_FIND_LIBRARY_PREFIXES})
set(CMAKE_FIND_LIBRARY_PREFIXES "")
find_library(
  SpatiaLite_EXTENSION
  NAMES mod_spatialite
  HINTS ${SpatiaLite_ROOT} ${PC_SpatiaLite_LIBRARY_DIRS}
  PATH_SUFFIXES lib
  NO_DEFAULT_PATH)
set(CMAKE_FIND_LIBRARY_PREFIXES ${_CMAKE_FIND_LIBRARY_PREFIXES})

if(SpatiaLite_EXTENSION)
  cmake_path(GET SpatiaLite_EXTENSION PARENT_PATH SpatiaLite_EXTENSION_DIR)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  SpatiaLite
  FOUND_VAR SpatiaLite_FOUND
  REQUIRED_VARS SpatiaLite_LIBRARY SpatiaLite_INCLUDE_DIR
  VERSION_VAR SpatiaLite_VERSION_STRING)
if(SpatiaLite_FOUND)
  set(SpatiaLite_LIBRARIES ${SpatiaLite_LIBRARY})
  set(SpatiaLite_INCLUDE_DIRS ${SpatiaLite_INCLUDE_DIR})
  if(NOT TARGET SPATIALITE::SPATIALITE)
    add_library(SPATIALITE::SPATIALITE UNKNOWN IMPORTED)
    set_target_properties(
      SPATIALITE::SPATIALITE
      PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${SpatiaLite_INCLUDE_DIR}
                 IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                 IMPORTED_LOCATION ${SpatiaLite_LIBRARY})
  endif()
endif()
