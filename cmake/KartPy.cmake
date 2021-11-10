# Virtualenv helper variables
set(VENV_BIN ${CMAKE_CURRENT_BINARY_DIR}/venv/bin)
# this is needed sometimes for bad setup.py files that invoke Python again
set(VENV_EXEC_ENV ${CMAKE_COMMAND} -E env "PATH=${VENV_BIN}:$ENV{PATH}")
set(VENV_PY ${VENV_EXEC_ENV} python)
set(VENV_PIP_INSTALL ${VENV_EXEC_ENV} pip install --isolated --disable-pip-version-check)

add_custom_command(
  OUTPUT venv
  COMMAND ${Python3_EXECUTABLE} -m venv --clear venv
  COMMAND ${VENV_PIP_INSTALL} --quiet --upgrade pip
  COMMENT "Creating Kart virtualenv...")

add_custom_command(
  OUTPUT vendor.stamp
  BYPRODUCTS vendor-tmp
  DEPENDS venv vendor
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMAND ${CMAKE_COMMAND} "-DVENDOR_ARCHIVE=${VENDOR_ARCHIVE}" -P
          ${CMAKE_CURRENT_SOURCE_DIR}/cmake/extract_vendor_archive.cmake
  COMMAND ${CMAKE_COMMAND} -E touch vendor.stamp
  COMMENT "Installing vendor dependencies...")

# TODO: selectively install these
add_custom_command(
  OUTPUT venv.stamp
  DEPENDS venv vendor.stamp requirements.txt requirements/dev.txt requirements/test.txt
  COMMAND ${VENV_PIP_INSTALL} --no-deps -r ${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt
  COMMAND ${VENV_PIP_INSTALL} --no-deps -r ${CMAKE_CURRENT_SOURCE_DIR}/requirements/test.txt
  COMMAND ${VENV_PIP_INSTALL} --no-deps -r ${CMAKE_CURRENT_SOURCE_DIR}/requirements/dev.txt
  COMMAND ${CMAKE_COMMAND} -E touch venv.stamp
  COMMENT "Installing Python dependencies...")

add_custom_command(
  OUTPUT kart sno venv/bin/kart venv/bin/sno
  DEPENDS venv.stamp setup.py
  COMMAND ${VENV_PIP_INSTALL} --force-reinstall --no-deps --editable ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${CMAKE_COMMAND} -E create_symlink venv/bin/kart kart
  COMMAND ${CMAKE_COMMAND} -E create_symlink venv/bin/sno sno
  COMMENT "Installing Kart...")

add_custom_target(
  cli ALL
  DEPENDS kart
  COMMENT "Kart CLI")
