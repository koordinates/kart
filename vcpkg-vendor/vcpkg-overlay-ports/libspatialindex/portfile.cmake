vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO libspatialindex/libspatialindex
    REF ab1a031ff1be0516dac90c53df164777959010d2
    SHA512 1a21736e3f14b7d4dab93130727c66f8fdc2d851446715269d76fd6426cab33217c853c0cb0d05edcd9624259cb6b0525821b69cd491ee26bcf7b044000300fb
    HEAD_REF master
)

vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    OPTIONS
        -DSIDX_BUILD_TESTS:BOOL=OFF
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(CONFIG_PATH lib/cmake/libspatialindex)

vcpkg_copy_pdbs()

#Debug
file(REMOVE_RECURSE ${CURRENT_PACKAGES_DIR}/debug/include)

# Handle copyright
file(INSTALL ${SOURCE_PATH}/COPYING DESTINATION ${CURRENT_PACKAGES_DIR}/share/${PORT} RENAME copyright)
