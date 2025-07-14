set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE dynamic)
set(VCPKG_BUILD_TYPE release)

set(VCPKG_CMAKE_SYSTEM_NAME Darwin)
set(VCPKG_OSX_ARCHITECTURES arm64)
set(VCPKG_OSX_DEPLOYMENT_TARGET
    "13.0"
    CACHE STRING "" FORCE)

set(ENV{LDFLAGS} -Wl,-rpath,${CURRENT_INSTALLED_DIR}/lib/)

# https://github.com/microsoft/vcpkg/issues/10038
set(VCPKG_C_FLAGS "-mmacosx-version-min=13.0")
set(VCPKG_CXX_FLAGS "-mmacosx-version-min=13.0")
set(ENV{MACOSX_DEPLOYMENT_TARGET} "13.0")
