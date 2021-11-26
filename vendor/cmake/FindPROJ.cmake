# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying file
# COPYING-CMAKE-SCRIPTS or https://cmake.org/licensing for details.

# From: GDAL

#[=======================================================================[.rst:
FindPROJ
---------

CMake module to search for PROJ(PROJ.4 and PROJ) library

On success, the macro sets the following variables:
``PROJ_FOUND``
  if the library found

``PROJ_LIBRARIES``
  full path to the library

``PROJ_INCLUDE_DIRS``
  where to find the library headers

``PROJ_VERSION_STRING``
  version string of PROJ

Copyright (c) 2009 Mateusz Loskot <mateusz@loskot.net>
Copyright (c) 2015 NextGIS <info@nextgis.com>
Copyright (c) 2018 Hiroshi Miura

#]=======================================================================]

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
  pkg_check_modules(PC_PROJ QUIET proj)
  set(PROJ_VERSION_STRING ${PC_PROJ_VERSION})
endif()

find_path(
  PROJ_INCLUDE_DIR proj.h
  HINTS ${PROJ_ROOT} ${PC_PROJ_INCLUDE_DIR}
  DOC "Path to PROJ library include directory")

if(MSVC)
  set(PROJ_NAMES proj proj_i)
  find_library(
    PROJ_IMP_LIBRARY
    NAMES proj_i
    HINTS ${PROJ_ROOT} ${PC_PROJ_LIBRARY_DIRS}
    DOC "Path to PROJ library file")
  find_library(
    PROJ_LIBRARY
    NAMES proj
    HINTS ${PROJ_ROOT} ${PC_PROJ_LIBRARY_DIRS}
    DOC "Path to PROJ library file")
elseif(MINGW OR CYGWIN)
  find_library(
    PROJ_LIBRARY
    NAMES proj
          libproj-0
          libproj-9
          libproj-10
          libproj-11
          libproj-12
          libproj-13
    HINTS ${PROJ_ROOT} ${PC_PROJ_LIBRARY_DIRS}
    DOC "Path to PROJ library file")
else()
  find_library(
    PROJ_LIBRARY
    NAMES proj
    HINTS ${PROJ_ROOT} ${PC_PROJ_LIBRARY_DIRS}
    DOC "Path to PROJ library file")
endif()

if(PROJ_INCLUDE_DIR AND NOT PROJ_VERSION_STRING)
  file(READ "${PROJ_INCLUDE_DIR}/proj.h" PROJ_H_CONTENTS)
  string(REGEX REPLACE "^.*PROJ_VERSION_MAJOR +([0-9]+).*$" "\\1" PROJ_VERSION_MAJOR
                       "${PROJ_H_CONTENTS}")
  string(REGEX REPLACE "^.*PROJ_VERSION_MINOR +([0-9]+).*$" "\\1" PROJ_VERSION_MINOR
                       "${PROJ_H_CONTENTS}")
  string(REGEX REPLACE "^.*PROJ_VERSION_PATCH +([0-9]+).*$" "\\1" PROJ_VERSION_PATCH
                       "${PROJ_H_CONTENTS}")
  unset(PROJ_H_CONTENTS)
  set(PROJ_VERSION_STRING "${PROJ_VERSION_MAJOR}.${PROJ_VERSION_MINOR}.${PROJ_VERSION_PATCH}")
endif()

if(NOT PROJ_DATADIR)
  find_path(
    PROJ_DATADIR proj.db
    HINTS ${PROJ_ROOT} ${PC_PROJ_DATADIR}
    PATH_SUFFIXES share/proj
    DOC "Path to PROJ data files")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  PROJ
  REQUIRED_VARS PROJ_LIBRARY PROJ_INCLUDE_DIR
  VERSION_VAR PROJ_VERSION_STRING)
mark_as_advanced(PROJ_INCLUDE_DIR PROJ_LIBRARY)

if(PROJ_FOUND)
  set(PROJ_LIBRARIES ${PROJ_LIBRARY})
  set(PROJ_INCLUDE_DIRS ${PROJ_INCLUDE_DIR})
  if(NOT TARGET PROJ::PROJ)
    add_library(PROJ::PROJ UNKNOWN IMPORTED)
    set_target_properties(
      PROJ::PROJ
      PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${PROJ_INCLUDE_DIR}
                 IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                 IMPORTED_LOCATION ${PROJ_LIBRARY})
    if(PROJ_IMP_LIBRARY)
      set_property(
        TARGET PROJ::PROJ
        APPEND
        PROPERTY IMPORTED_IMPLIB ${PROJ_IMP_LIBRARY})
    endif()
  endif()
endif()
