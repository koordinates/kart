# include()d via both /CMakeLists.txt & /vcpkg-vendor/CMakeLists.txt

set(CMAKE_TOOLCHAIN_FILE
    "${CMAKE_CURRENT_LIST_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake"
    CACHE STRING "")
set(VCPKG_MANIFEST_DIR "${CMAKE_CURRENT_LIST_DIR}")
set(VCPKG_OVERLAY_PORTS "${CMAKE_CURRENT_LIST_DIR}/vcpkg-overlay-ports")
set(VCPKG_OVERLAY_TRIPLETS "${CMAKE_CURRENT_LIST_DIR}/vcpkg-overlay-triplets")
set(VCPKG_BUILD_TYPE release)

# equivalent of 11.0 on arm64
set(CMAKE_OSX_DEPLOYMENT_TARGET "10.15")
