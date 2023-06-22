include(PyCreateVirtualEnvironment)

createvirtualenvironment(requirementsEnv REQUIREMENTS "pip-tools" "liccheck")

set(REQUIREMENTS_FILES requirements.txt test.txt docs.txt dev.txt)
set(REQUIREMENTS_DEPS "" requirements.txt test.txt docs.txt)
set(REQUIREMENTS_FILES
    "${REQUIREMENTS_FILES}"
    PARENT_SCOPE)
set(REQUIREMENTS_DEPS
    "${REQUIREMENTS_DEPS}"
    PARENT_SCOPE)

# fixme: use a list from somewhere else
set(WHEEL_DEPS
    cryptography
    psycopg2
    pygit2
    gdal
    cffi
    pysqlite3
    reflink
    # pyodbc: this is OS-qualified already in requirements.in
)

set(PIP_COMPILE_COMMAND
    ${CMAKE_COMMAND} -E env CUSTOM_COMPILE_COMMAND="cmake --build build --target py-requirements"
    ${requirementsEnv_EXEC} pip-compile -v --annotate --no-emit-index-url --no-emit-trusted-host
    --allow-unsafe --find-links "${CMAKE_BINARY_DIR}/vendor-tmp/wheelhouse" --no-emit-find-links)

# Strip out wheels we provide:
string(JOIN "|" wheelDepRegex ${WHEEL_DEPS})
set(WHEEL_STRIP_COMMAND ${CMAKE_COMMAND} "-DMATCH=^(${wheelDepRegex})=" "-DREPLACE=#\\1=")

add_custom_command(
  OUTPUT pip-compile.stamp
  DEPENDS requirementsEnv
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${CMAKE_COMMAND} -E true
  VERBATIM
  COMMENT "Compiling Python dependency requirements")

add_custom_command(
  OUTPUT pip-compile-upgrade.stamp
  DEPENDS requirementsEnv
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${CMAKE_COMMAND} -E true
  VERBATIM
  COMMENT "Upgrading Python dependency requirements")

foreach(reqOutput IN LISTS REQUIREMENTS_FILES)
  string(REPLACE ".txt" ".in" reqInput "${reqOutput}")
  add_custom_command(
    OUTPUT pip-compile.stamp
    APPEND
    COMMAND ${PIP_COMPILE_COMMAND} --output-file ${reqOutput} ${reqInput}
    COMMAND ${WHEEL_STRIP_COMMAND} "-DFILE=${reqOutput}" -P
            ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/str_replace.cmake)

  add_custom_command(
    OUTPUT pip-compile-upgrade.stamp
    APPEND
    COMMAND ${PIP_COMPILE_COMMAND} --upgrade --output-file ${reqOutput} ${reqInput}
    COMMAND ${WHEEL_STRIP_COMMAND} "-DFILE=${reqOutput}" -P
            ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/str_replace.cmake)
endforeach()

add_custom_target(
  py-requirements
  DEPENDS pip-compile.stamp
  COMMENT "Compile Python dependency requirements")

add_custom_target(
  py-requirements-upgrade
  DEPENDS pip-compile-upgrade.stamp
  COMMENT "Upgrade Python dependency requirements")

add_custom_target(
  py-license-check
  DEPENDS requirementsEnv py-dependencies
  COMMAND
    ${CMAKE_COMMAND} -E env PYTHONPATH=${requirementsEnv_PURELIB_DIR} -- ${VENV_PY} -m liccheck -l
    CAUTIOUS -s ${CMAKE_CURRENT_SOURCE_DIR}/licenses.ini -r
    ${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt --no-deps -R liccheck-report.txt
  VERBATIM
  BYPRODUCTS liccheck-report.txt
  COMMENT "Check Python dependency licenses")
