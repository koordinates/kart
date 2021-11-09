# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying file
# COPYING-CMAKE-SCRIPTS or https://cmake.org/licensing for details.

# From: GDAL

#[=======================================================================[.rst:
FindSpatialindex
----------------

# - Find Libspatialindex
#
# Once run this will define:
#
# SPATIALINDEX_FOUND       = system has Spatialindex C++ lib
# SPATIALINDEX_LIBRARY     = full path to the Spatialindex C++ library
# SPATIALINDEX_INCLUDE_DIR = where to find C++ library headers
# SPATIALINDEX_C_LIBRARY     = full path to the Spatialindex C library
# SPATIALINDEX_C_INCLUDE_DIR = where to find C library headers
#
#]=======================================================================]

find_path(
  SPATIALINDEX_INCLUDE_DIR
  NAMES SpatialIndex.h
  PATH_SUFFIXES spatialindex)
find_library(SPATIALINDEX_LIBRARY NAMES spatialindex_i spatialindex)

find_path(
  SPATIALINDEX_C_INCLUDE_DIR
  NAMES sidx_api.h
  PATH_SUFFIXES spatialindex/capi)
find_library(SPATIALINDEX_C_LIBRARY NAMES spatialindex_c)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(
  Spatialindex
  FOUND_VAR SPATIALINDEX_FOUND
  REQUIRED_VARS SPATIALINDEX_LIBRARY SPATIALINDEX_INCLUDE_DIR SPATIALINDEX_C_LIBRARY
                SPATIALINDEX_C_INCLUDE_DIR)
