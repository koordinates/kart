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
include(CheckTypeSize)

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

if(LibGit2_FOUND)
  # check whether it has error subcodes ie: whether it's the Koordinates fork or it's been merged
  # into upstream
  set(CMAKE_REQUIRED_INCLUDES ${LibGit2_INCLUDE_DIR})
  set(CMAKE_EXTRA_INCLUDE_FILES "git2/errors.h")
  set(CMAKE_REQUIRED_QUIET ON)
  check_type_size("git_error_subcode" error_subcode)
  unset(CMAKE_REQUIRED_INCLUDES)
  unset(CMAKE_EXTRA_INCLUDE_FILES)
  unset(CMAKE_REQUIRED_QUIET)

  if(HAVE_error_subcode)
    set(LibGit2_HAS_ERROR_SUBCODE ON)
  endif()

  mark_as_advanced(LibGit2_LIBRARY LibGit2_INCLUDE_DIR LibGit2_HAS_ERROR_SUBCODE)

  if(NOT TARGET LibGit2::LibGit2)
    add_library(LibGit2::LibGit2 UNKNOWN IMPORTED)
    set_target_properties(
      LibGit2::LibGit2
      PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${LibGit2_INCLUDE_DIR}"
                 IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                 IMPORTED_LOCATION "${LibGit2_LIBRARY}")

    if(LibGit2_HAS_ERROR_SUBCODE)
      set_property(
        TARGET LibGit2::LibGit2
        APPEND
        PROPERTY INTERFACE_COMPILE_DEFINITIONS "LibGit2_HAS_ERROR_SUBCODE")
    endif()
  endif()
endif()
