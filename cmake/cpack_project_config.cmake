# Exclude component "addToPath" for TGZ & ZIP.
# Is handled in post-build for RPM & DEB
if (CPACK_GENERATOR MATCHES "^(TGZ|ZIP|DEB|RPM)$")
  list(REMOVE_ITEM CPACK_COMPONENTS_ALL "addToPath")
endif()

# Selectively decide whether to include kart-1.2.3/ dir at the top level
if (CPACK_GENERATOR MATCHES "^(TGZ|ZIP)$")
  set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY ON)
  set(CPACK_COMPONENT_INCLUDE_TOPLEVEL_DIRECTORY ON)
endif()

# Need to set 'CMAKE_<GENERATOR>_COMPONENT_INSTALL' to ON, otherwise CPack ignores CPACK_COMPONENTS_ALL variable
set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_DEB_COMPONENT_INSTALL ON)
