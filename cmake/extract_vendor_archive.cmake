# cmake-lint: disable=E1126

#
# Extracts a vendor archive into the Kart virtualenv
#
file(REMOVE_RECURSE rm -rf vendor-tmp/)
file(MAKE_DIRECTORY vendor-tmp/)

# get the path to the site-packages directory
execute_process(
  COMMAND venv/bin/python -c "import sysconfig; print(sysconfig.get_paths()['purelib'])"
          COMMAND_ERROR_IS_FATAL ANY
  OUTPUT_VARIABLE venv_purelib
  OUTPUT_STRIP_TRAILING_WHITESPACE)

# extract the archive
file(ARCHIVE_EXTRACT INPUT ${VENDOR_ARCHIVE} DESTINATION vendor-tmp)

# install wheels
file(
  GLOB wheels
  LIST_DIRECTORIES false
  "vendor-tmp/wheelhouse/*.whl")
execute_process(COMMAND venv/bin/pip install --isolated --disable-pip-version-check
                        --force-reinstall --no-deps ${wheels} COMMAND_ERROR_IS_FATAL ANY)

# install other env files (libraries, binaries, data)
message(STATUS "Installing environment files...")
file(COPY vendor-tmp/env/ DESTINATION venv)

# install a _kart_env.py configuration file
if(EXISTS vendor-tmp/_kart_env.py)
  file(INSTALL vendor-tmp/_kart_env.py DESTINATION ${venv_purelib})
else()
  message(STATUS "No _kart_env.py configuration module found in vendor archive")
  file(REMOVE ${venv_purelib}/_kart_env.py)
endif()
