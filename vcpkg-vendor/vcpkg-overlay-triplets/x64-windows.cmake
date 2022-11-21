set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE dynamic)
set(VCPKG_BUILD_TYPE release)

if(PORT MATCHES "^(sqlite3)$")
    # On Windowns Python bundles its own sqlite3.dll, and this appears on the
    # search path before our build. Statically link sqlite3 where we need it.
    set(VCPKG_LIBRARY_LINKAGE static)
endif()
