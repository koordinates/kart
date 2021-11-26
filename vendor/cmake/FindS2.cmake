#[=======================================================================[.rst:
FindS2
------

Finds the S2 Geometry library.

Imported Targets
^^^^^^^^^^^^^^^^

This module provides the following imported targets, if found:

``S2::S2``
  The S2 Geometry library

Result Variables
^^^^^^^^^^^^^^^^

This will define the following variables:

``S2_FOUND``
  True if the system has the S2 library.
``S2_VERSION``
  The version of the S2 library which was found.
``S2_INCLUDE_DIRS``
  Include directories needed to use S2.
``S2_LIBRARIES``
  Libraries needed to link to S2.

Cache Variables
^^^^^^^^^^^^^^^

The following cache variables may also be set:

``S2_INCLUDE_DIR``
  The directory containing ``s2point.h``.
``S2_LIBRARY``
  The path to the S2 library.

#]=======================================================================]

find_package(PkgConfig)
pkg_check_modules(PC_S2 QUIET S2)

find_path(
  S2_INCLUDE_DIR
  NAMES s2point.h
  PATHS ${PC_S2_INCLUDE_DIRS}
  PATH_SUFFIXES s2)
find_library(
  S2_LIBRARY
  NAMES s2
  PATHS ${PC_S2_LIBRARY_DIRS})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  S2
  FOUND_VAR S2_FOUND
  REQUIRED_VARS S2_LIBRARY S2_INCLUDE_DIR)

if(S2_FOUND AND NOT TARGET S2::S2)
  add_library(S2::S2 INTERFACE IMPORTED)
  set_target_properties(
    S2::S2
    PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${S2_INCLUDE_DIR}"
               IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
               IMPORTED_LOCATION "${S2_LIBRARY}")
endif()
