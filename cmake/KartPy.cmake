# Virtualenv helper variables
set(VENV_BIN ${CMAKE_CURRENT_BINARY_DIR}/venv/bin)
# this is needed sometimes for bad setup.py files that invoke Python again
set(VENV_EXEC_ENV ${CMAKE_COMMAND} -E env "PATH=${VENV_BIN}:$ENV{PATH}")
set(VENV_PY ${VENV_EXEC_ENV} python)
set(VENV_PIP ${VENV_EXEC_ENV} pip)
cmake_path(GET Python3_SITELIB PARENT_PATH _sitelib)
cmake_path(GET _sitelib FILENAME _sitelib)
set(VENV_SITELIB ${CMAKE_CURRENT_BINARY_DIR}/venv/lib/${_sitelib}/site-packages)

add_custom_command(
  OUTPUT venv
  COMMAND ${Python3_EXECUTABLE} -m venv venv
  COMMAND ${VENV_PIP} install --upgrade pip
  COMMENT "Creating Virtualenv...")

add_custom_command(
  OUTPUT vendor.stamp
  DEPENDS venv vendor
  COMMAND ${CMAKE_COMMAND} -E rm -rf vendor-dist/
  COMMAND ${CMAKE_COMMAND} -E make_directory vendor-dist/
  COMMAND tar xzf ${VENDOR_ARCHIVE} -C vendor-dist -- wheelhouse/
  COMMAND ${VENV_PIP} install --force-reinstall --no-deps vendor-dist/wheelhouse/*.whl
  COMMAND tar xzf ${VENDOR_ARCHIVE} -C venv --strip-components=1 -- env/
  COMMAND tar xzf ${VENDOR_ARCHIVE} -C vendor-dist -- _env.py || (exit 0)
  COMMAND ${CMAKE_COMMAND} -E touch vendor.stamp
  COMMENT "Installing Vendor dependencies...")

# TODO: selectively install these
add_custom_command(
  OUTPUT venv.stamp
  DEPENDS venv vendor.stamp requirements.txt requirements/dev.txt requirements/test.txt
  COMMAND ${VENV_PIP} install --no-deps -r ${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt
  COMMAND ${VENV_PIP} install --no-deps -r ${CMAKE_CURRENT_SOURCE_DIR}/requirements/test.txt
  COMMAND ${VENV_PIP} install --no-deps -r ${CMAKE_CURRENT_SOURCE_DIR}/requirements/dev.txt
  COMMAND ${CMAKE_COMMAND} -E touch venv.stamp
  COMMENT "Installing Python dependencies...")

add_custom_command(
  OUTPUT kart sno venv/bin/kart venv/bin/sno
  DEPENDS venv.stamp setup.py
  COMMAND ${VENV_PIP} install --force-reinstall --no-deps --editable ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_BINARY_DIR}/vendor-dist/_env.py
          ${CMAKE_CURRENT_SOURCE_DIR}/kart/_env.py
  COMMAND ${CMAKE_COMMAND} -E create_symlink venv/bin/kart kart
  COMMAND ${CMAKE_COMMAND} -E create_symlink venv/bin/sno sno
  COMMENT "Building Kart...")

add_custom_target(
  cli ALL
  DEPENDS kart
  COMMENT "Kart CLI")
