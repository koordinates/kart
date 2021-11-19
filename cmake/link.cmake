# create a symlink on Unix or a .cmd file on Windows
# LINK_NAME -> TARGET
#
# invoke via ${CMAKE_COMMAND} -DTARGET:FILEPATH=... -DLINK_NAME:FILEPATH=... -P link.cmake

if(WIN32)
  # Windows does mostly support symlinks, but Python entrypoints
  # don't work when symlinked
  cmake_path(NATIVE_PATH TARGET target_abs)
  file(CONFIGURE
    OUTPUT "${LINK_NAME}.cmd"
    CONTENT "@echo off\n\"${target_abs}\" %*\n"
    NEWLINE_STYLE WIN32)
else()
  file(CREATE_LINK ${TARGET} ${LINK_NAME} SYMBOLIC)
endif()
