# cmake-lint: disable=E1126

#
# Extracts a vendor archive into the Kart virtualenv
#
file(REMOVE_RECURSE rm -rf vendor-tmp/)
file(MAKE_DIRECTORY vendor-tmp/)

if(WIN32)
  set(PY "venv/Scripts/Python.exe")
  set(PIP "venv/Scripts/pip.exe")
else()
  set(PY "venv/bin/python")
  set(PIP "venv/bin/pip")
endif()

# get the path to the site-packages directory
execute_process(
  COMMAND ${PY} -c "import sysconfig; print(sysconfig.get_paths()['purelib'])"
          COMMAND_ERROR_IS_FATAL ANY
  OUTPUT_VARIABLE venv_purelib
  OUTPUT_STRIP_TRAILING_WHITESPACE)

# extract the archive
message(STATUS "Extracting vendor archive...")
file(ARCHIVE_EXTRACT INPUT ${VENDOR_ARCHIVE} DESTINATION vendor-tmp)

# Maybe need to extract again if the archive has been zipped again by GitHub
file(GLOB NESTED_ARCHIVE vendor-tmp/*.zip vendor-tmp/*.tgz vendor-tmp/*.tar.gz)
if(NOT "${NESTED_ARCHIVE}" STREQUAL "")
  message(STATUS "Extracting nested vendor archive...")
  file(REMOVE_RECURSE vendor-tmp-intermediate)
  file(RENAME vendor-tmp vendor-tmp-intermediate)
  file(GLOB NESTED_ARCHIVE vendor-tmp-intermediate/*.zip vendor-tmp-intermediate/*.tgz vendor-tmp-intermediate/*.tar.gz)
  list(GET NESTED_ARCHIVE 0 NESTED_ARCHIVE)
  file(ARCHIVE_EXTRACT INPUT ${NESTED_ARCHIVE} DESTINATION vendor-tmp)
endif()

# install other env files (libraries, binaries, data)
message(STATUS "Installing environment files...")
# FIXME: why is this different between platforms?
if(WIN32)
  file(COPY vendor-tmp/env/lib/ DESTINATION venv)
  file(COPY vendor-tmp/git/ DESTINATION venv/git/)
else()
  file(COPY vendor-tmp/env/ DESTINATION venv)
endif()

# Upgrade the venv using python from the vendor-archive (if included):
message(STATUS "Upgrading venv...")
execute_process(COMMAND ${PY} -m venv --upgrade venv)
execute_process(COMMAND ${PIP} install --isolated --quiet --disable-pip-version-check --upgrade pip setuptools)

# install wheels
file(
  GLOB wheels
  LIST_DIRECTORIES false
  "vendor-tmp/wheelhouse/*.whl")
execute_process(COMMAND ${PIP} install --isolated --disable-pip-version-check
                        --force-reinstall --no-deps ${wheels} COMMAND_ERROR_IS_FATAL ANY)

# install a _kart_env.py configuration file
if(EXISTS vendor-tmp/_kart_env.py)
  file(INSTALL vendor-tmp/_kart_env.py DESTINATION ${venv_purelib})
else()
  message(STATUS "No _kart_env.py configuration module found in vendor archive")
  file(REMOVE ${venv_purelib}/_kart_env.py)
endif()
