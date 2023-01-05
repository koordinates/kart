# include()d via both /CMakeLists.txt & /vcpkg-vendor/CMakeLists.txt

set(CMAKE_TOOLCHAIN_FILE
    "${CMAKE_CURRENT_LIST_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake"
    CACHE STRING "")
set(VCPKG_MANIFEST_DIR "${CMAKE_CURRENT_LIST_DIR}")
set(VCPKG_OVERLAY_PORTS "${CMAKE_CURRENT_LIST_DIR}/vcpkg-overlay-ports")
set(VCPKG_OVERLAY_TRIPLETS "${CMAKE_CURRENT_LIST_DIR}/vcpkg-overlay-triplets")
set(VCPKG_BUILD_TYPE release)
