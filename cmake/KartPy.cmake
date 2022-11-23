# Virtualenv helper variables
if(WIN32)
  cmake_path(SET VENV_BIN ${CMAKE_CURRENT_BINARY_DIR}/venv/Scripts)
  cmake_path(SET VENV_PY ${VENV_BIN}/Python.exe)

  cmake_path(NATIVE_PATH VENV_PY VENV_PY)

  cmake_path(SET KART_EXE_VENV ${VENV_BIN}/kart.exe)
  cmake_path(SET KART_EXE_BUILD ${CMAKE_CURRENT_BINARY_DIR}/kart.cmd)
else()
  set(VENV_BIN ${CMAKE_CURRENT_BINARY_DIR}/venv/bin)
  # this is needed sometimes for bad setup.py files that invoke Python again seems ok without it on
  # Windows
  set(VENV_EXEC_ENV ${CMAKE_COMMAND} -E env "PATH=${VENV_BIN}:$ENV{PATH}")
  set(VENV_PY ${VENV_EXEC_ENV} ${VENV_BIN}/python)
  cmake_path(SET KART_EXE_VENV ${VENV_BIN}/kart)
  cmake_path(SET KART_EXE_BUILD ${CMAKE_CURRENT_BINARY_DIR}/kart)
endif()

set(VENV_PIP_INSTALL ${VENV_PY} -m pip install --isolated --disable-pip-version-check)

cmake_path(SET VENV_PYTEST ${VENV_BIN}/pytest)
cmake_path(SET VENV_DOCS ${CMAKE_CURRENT_BINARY_DIR}/venv/help)
cmake_path(NATIVE_PATH VENV_PYTEST VENV_PYTEST)

add_custom_command(
  OUTPUT venv/.venv.stamp
  BYPRODUCTS venv
  COMMAND ${Python3_EXECUTABLE} -m venv --clear venv
  COMMAND ${VENV_PIP_INSTALL} --quiet --upgrade pip setuptools
  COMMAND ${CMAKE_COMMAND} -E touch venv/.venv.stamp
  COMMENT "Creating Kart virtualenv...")

add_custom_command(
  OUTPUT venv/.vendor.stamp
  BYPRODUCTS vendor-tmp
  DEPENDS venv/.venv.stamp ${VENDOR_ARCHIVE} ${VENDOR_TARGET}
          ${CMAKE_CURRENT_SOURCE_DIR}/cmake/extract_vendor_archive.cmake
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMAND ${CMAKE_COMMAND} "-DVENDOR_ARCHIVE=${VENDOR_ARCHIVE}" -P
          ${CMAKE_CURRENT_SOURCE_DIR}/cmake/extract_vendor_archive.cmake
  COMMAND ${CMAKE_COMMAND} -E touch venv/.vendor.stamp
  COMMENT "Installing vendor dependencies...")

add_subdirectory("requirements")

foreach(reqFile reqDep IN ZIP_LISTS REQUIREMENTS_FILES REQUIREMENTS_DEPS)
  set(reqStamp "venv/.py-${reqFile}.stamp")
  if(reqDep)
    set(reqDep "venv/.py-${reqDep}.stamp")
  endif()

  add_custom_command(
    OUTPUT ${reqStamp}
    DEPENDS venv/.vendor.stamp requirements/${reqFile} ${reqDep}
    COMMAND ${VENV_PIP_INSTALL} --no-deps -r "${CMAKE_CURRENT_SOURCE_DIR}/requirements/${reqFile}"
    COMMAND ${CMAKE_COMMAND} -E touch ${reqStamp}
    COMMENT "Installing Python dependencies: ${reqFile}")

  list(APPEND pydeps ${reqStamp})
endforeach()
add_custom_target(
  py-dependencies
  DEPENDS venv/.vendor.stamp ${pydeps}
  COMMENT "Installing Python dependencies...")

add_custom_command(
  OUTPUT ${KART_EXE_VENV} ${KART_EXE_BUILD}
  DEPENDS py-dependencies setup.py
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMAND ${VENV_PIP_INSTALL} --force-reinstall --no-deps --editable "${CMAKE_CURRENT_SOURCE_DIR}"
  COMMAND ${CMAKE_COMMAND} "-DTARGET:FILEPATH=${KART_EXE_VENV}" "-DLINK_NAME:FILEPATH=kart" -P
          ${CMAKE_CURRENT_SOURCE_DIR}/cmake/link.cmake
  COMMENT "Installing Kart...")

add_custom_command(
  OUTPUT ${VENV_DOCS}
  DEPENDS ${KART_EXE_VENV}
  COMMAND ${VENV_PY} ${CMAKE_CURRENT_BINARY_DIR}/scripts/doc_gen.py
          ${CMAKE_CURRENT_BINARY_DIR}/commands ${VENV_DOCS}
  COMMENT "Generating help pages...")

add_custom_target(
  cli ALL
  DEPENDS ${KART_EXE_BUILD} ${VENV_DOCS}
  COMMENT "Kart CLI")
