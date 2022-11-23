include(PyCreateVirtualEnvironment)

createvirtualenvironment(requirementsEnv REQUIREMENTS "pip-tools" "liccheck")

set(REQUIREMENTS_FILES requirements.txt test.txt docs.txt dev.txt)
set(REQUIREMENTS_DEPS "" requirements.txt test.txt docs.txt)
set(REQUIREMENTS_FILES
    ${REQUIREMENTS_FILES}
    PARENT_SCOPE)

option(PY_REQUIREMENTS_UPGRADE "py-requirements: upgrade dependencies to latest versions" OFF)

# fixme: use a list from somewhere else
set(WHEEL_DEPS
    cryptography
    psycopg2
    pygit2
    GDAL
    cffi
    pysqlite3
    pyodbc)

set(PIP_COMPILE_COMMAND
    ${CMAKE_COMMAND} -E env CUSTOM_COMPILE_COMMAND="cmake --build build --target py-requirements"
    ${requirementsEnv_EXEC} pip-compile -v --annotate --no-emit-index-url --no-emit-trusted-host
    --allow-unsafe)

if(PY_REQUIREMENTS_UPGRADE)
  list(APPEND PIP_COMPILE_COMMAND "--upgrade")
endif()

string(JOIN "|" wheelDepRegex ${WHEEL_DEPS})

foreach(reqOutput reqDep IN ZIP_LISTS REQUIREMENTS_FILES REQUIREMENTS_DEPS)
  string(REPLACE ".txt" ".in" reqInput "${reqOutput}")

  set(reqGen ${CMAKE_CURRENT_SOURCE_DIR}/${reqOutput})

  if(PY_REQUIREMENTS_UPGRADE)
    list(PREPEND reqGen ${reqOutput}.upgrade)
    set_source_files_properties(${reqOutput}.upgrade PROPERTIES SYMBOLIC true)
  endif()

  set(reqDepends ${CMAKE_CURRENT_SOURCE_DIR}/${reqInput} requirementsEnv)
  if(reqDep)
    list(APPEND reqDepends ${CMAKE_CURRENT_SOURCE_DIR}/${reqDep})
  endif()

  add_custom_command(
    OUTPUT ${reqGen}
    DEPENDS ${reqDepends}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND ${PIP_COMPILE_COMMAND} --output-file ${reqOutput} ${reqInput}
    # Strip out wheels we provide:
    COMMAND ${CMAKE_COMMAND} "-DMATCH=^(${wheelDepRegex})=" "-DREPLACE=#\\1=" "-DFILE=${reqOutput}"
            -P ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/str_replace.cmake
    VERBATIM
    COMMENT "Compiling: ${reqInput} -> ${reqOutput}")
endforeach()

add_custom_target(
  py-requirements
  DEPENDS ${REQUIREMENTS_FILES}
  COMMENT "Compiling Python dependency requirements")

add_custom_target(
  py-license-check
  DEPENDS requirementsEnv py-dependencies
  COMMAND
    ${CMAKE_COMMAND} -E env PYTHONPATH=${requirementsEnv_PURELIB_DIR} -- ${VENV_PY} -m liccheck -l
    CAUTIOUS -s ${CMAKE_CURRENT_SOURCE_DIR}/licenses.ini -r
    ${CMAKE_CURRENT_SOURCE_DIR}/requirements.txt -R liccheck-report.txt
  VERBATIM
  BYPRODUCTS liccheck-report.txt
  COMMENT "Checking Python dependency licenses")
