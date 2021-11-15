# create either a symlink or a .bat file
# TARGET should include .exe on windows
# LINK_NAME should not.
if(NOT WIN32)
  file(CREATE_LINK "${TARGET}" "${LINK_NAME}" SYMBOLIC)
else()
  file(CREATE_LINK "${TARGET}" "${LINK_NAME}.exe" RESULT err SYMBOLIC)
  # 0 on success
  if(err)
    # symlink failed, create a .bat file instead
    get_filename_component(exe_abs "${TARGET}"
      REALPATH BASE_DIR "${CMAKE_BINARY_DIR}")

    file(CONFIGURE
      OUTPUT "${LINK_NAME}.bat"
      CONTENT "@echo off\n\"${exe_abs}\" %*\n"
      NEWLINE_STYLE WIN32)
    endif()
endif()
