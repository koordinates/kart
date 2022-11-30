# Modified from:
# https://schemingdeveloper.com/2020/07/02/how-to-create-a-new-python-virtual-environment-in-cmake/
# License: WTFPL

#[=============================================================================[
------------------------
CreateVirtualEnvironment
------------------------
Creates a Python virtual environment with specific requirements.

Required Arguments:
    TARGET (string):
        Target name of the virtual environment that can be used by other
        targets as a dependency.

Optional Arguments:
    REQUIREMENTS_TXT (string):
        Path to requirements.txt list to install with pip
    REQUIREMENTS (list of strings):
        Any additional requirements to install with pip that aren't part of
        requirements.txt, e.g. local packages. If you need to use additional
        specifiers, use file(WRITE) to create a REQUIREMENTS_TXT file.
    SOURCES (list of string):
        Any sources that local packages depend on.
    PREFIX (string):
        Path where the environment will be placed at. Can be relative (under
        ${CMAKE_CURRENT_BINARY_DIR}) or absolute.
    ENV_NAME (string)
        The name of the virtual environment. Unless otherwise specified, this
        is the same as TARGET.


Output Variables TARGET:
    ${TARGET}_PYTHON_EXE:
        Stores the path to the python executable of the virtual environment.
    ${TARGET}_BINARY_DIR:
        Stores the directory of any scripts / executables that are created as
        part of the environment, such as python[.exe], pip[.exe], activate, etc.
    ${TARGET}_EXEC:
        Stores the environment-setup command to use to run things in the virtualenv.
    ${TARGET}_PYTHON:
        Stores the command to use to run python in the virtualenv.
    ${TARGET}_PIP:
        Stores the command to use to run pip in the virtualenv.
    ${TARGET}_VENV_DIR:
        Stores the root path of the virtual environment.
    ${TARGET}_PURELIB_DIR:
        Stores the site-packages path of the virtual environment.

#]=============================================================================]

# Create a Python virtual environment with specific requirements.
function(CreateVirtualEnvironment TARGET)
  # cmake-lint: disable=R0915
  set(KEYWORD_ARGS REQUIREMENTS_TXT PREFIX ENV_NAME)
  set(MULTI_ARGS SOURCES REQUIREMENTS)

  cmake_parse_arguments(ARG "${SINGLE_ARGS}" "${KEYWORD_ARGS}" "${MULTI_ARGS}" ${ARGN})

  if(NOT ARG_ENV_NAME)
    set(ARG_ENV_NAME ${TARGET})
  endif()

  find_package(Python3 REQUIRED COMPONENTS Development Interpreter)

  if(ARG_PREFIX)
    if(IS_ABSOLUTE ${ARG_PREFIX})
      set(VENV ${ARG_PREFIX}/${ARG_ENV_NAME})
    else()
      set(VENV ${CMAKE_CURRENT_BINARY_DIR}/${ARG_PREFIX}/${ARG_ENV_NAME})
    endif()
  else()
    set(VENV ${CMAKE_CURRENT_BINARY_DIR}/${ARG_ENV_NAME})
  endif()

  if(WIN32)
    set(BIN_DIR ${VENV}/Scripts)
  else()
    set(BIN_DIR ${VENV}/bin)
  endif()

  set(PURELIB_DIR ${VENV}/${Python3_PURELIB_REL_PATH})

  set(EXEC ${CMAKE_COMMAND} -E env --modify PATH=path_list_prepend:${BIN_DIR})
  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    # Force compilation with the expected deployment target and architecture
    list(APPEND EXEC "MACOSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}")
    list(APPEND EXEC "ARCHFLAGS=-arch ${CMAKE_HOST_SYSTEM_PROCESSOR}")
  endif()

  set(PYTHON_EXE "${BIN_DIR}/python${CMAKE_EXECUTABLE_SUFFIX}")
  set(PYTHON ${EXEC} ${PYTHON_EXE})

  set(PIP ${PYTHON} -m pip -v)
  set(PIP_INSTALL ${PIP} install --isolated --quiet --disable-pip-version-check)

  if(ARG_REQUIREMENTS_TXT)
    set(REQUIREMENTS -r ${ARG_REQUIREMENTS_TXT})
  endif()

  set(REQUIREMENTS ${REQUIREMENTS} "${ARG_REQUIREMENTS}")

  if(REQUIREMENTS)
    set(DEPS_INSTALL ${PIP_INSTALL} ${REQUIREMENTS})
  else()
    set(DEPS_INSTALL "")
  endif()

  set(CFG_FILE ${VENV}/pyvenv.cfg)
  add_custom_command(
    OUTPUT ${CFG_FILE}
    COMMAND ${Python3_EXECUTABLE} -m venv --clear ${VENV}
    DEPENDS Python3::Python
    COMMENT "${ARG_ENV_NAME}: creating virtualenv at ${VENV}...")
  set(OUTPUT_FILE ${VENV}/.requirements)
  add_custom_command(
    OUTPUT ${OUTPUT_FILE}
    DEPENDS ${CFG_FILE} ${ARG_SOURCES} ${ARG_REQUIREMENTS_TXT}
    COMMAND ${PIP_INSTALL} --upgrade pip setuptools
    COMMAND ${DEPS_INSTALL}
    COMMAND ${PIP} freeze > "${OUTPUT_FILE}"
    COMMENT "${ARG_ENV_NAME}: installing requirements...")

  add_custom_target(
    ${TARGET}
    DEPENDS ${OUTPUT_FILE}
    COMMENT "VirtualEnv: ${TARGET}")

  set(${TARGET}_PYTHON_EXE
      ${PYTHON_EXE}
      PARENT_SCOPE)
  set(${TARGET}_BINARY_DIR
      ${BIN_DIR}
      PARENT_SCOPE)
  set(${TARGET}_EXEC
      ${EXEC}
      PARENT_SCOPE)
  set(${TARGET}_PYTHON
      ${PYTHON}
      PARENT_SCOPE)
  set(${TARGET}_PIP
      ${PIP}
      PARENT_SCOPE)
  set(${TARGET}_VENV_DIR
      ${VENV_DIR}
      PARENT_SCOPE)
  set(${TARGET}_PURELIB_DIR
      ${PURELIB_DIR}
      PARENT_SCOPE)
endfunction()
