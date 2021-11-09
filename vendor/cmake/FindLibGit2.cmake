# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying file Copyright.txt or
# https://cmake.org/licensing for details.

#[=======================================================================[.rst:
FindLibGit2
-----------

Finds the LibGit2 Geometry library.

Imported Targets
^^^^^^^^^^^^^^^^

This module provides the following imported targets, if found:

``LibGit2::LibGit2``
  The LibGit2 Geometry library

Result Variables
^^^^^^^^^^^^^^^^

This will define the following variables:

``LibGit2_FOUND``
  True if the system has the LibGit2 library.
``LibGit2_VERSION``
  The version of the LibGit2 library which was found.
``LibGit2_INCLUDE_DIRS``
  Include directories needed to use LibGit2.
``LibGit2_LIBRARIES``
  Libraries needed to link to LibGit2.

Cache Variables
^^^^^^^^^^^^^^^

The following cache variables may also be set:

``LibGit2_INCLUDE_DIR``
  The directory containing ``git2.h``.
``LibGit2_LIBRARY``
  The path to the LibGit2 library.

#]=======================================================================]

find_package(PkgConfig)
pkg_check_modules(PC_libgit2 QUIET libgit2)

find_path(
  LibGit2_INCLUDE_DIR
  NAMES git2.h
  PATHS ${PC_libgit2_INCLUDE_DIRS})
find_library(
  LibGit2_LIBRARY
  NAMES git2
  PATHS ${PC_libgit2_LIBRARY_DIRS})

set(LibGit2_VERSION ${PC_libgit2_VERSION})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  LibGit2
  FOUND_VAR LibGit2_FOUND
  REQUIRED_VARS LibGit2_LIBRARY LibGit2_INCLUDE_DIR
  VERSION_VAR LibGit2_VERSION)

if(NOT TARGET LibGit2::LibGit2)
  add_library(LibGit2::LibGit2 UNKNOWN IMPORTED)
  set_target_properties(
    LibGit2::LibGit2
    PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${LibGit2_INCLUDE_DIR}"
               IMPORTED_LINK_INTERFACE_LANGUAGES "C"
               IMPORTED_LOCATION "${LibGit2_LIBRARY}")
endif()
