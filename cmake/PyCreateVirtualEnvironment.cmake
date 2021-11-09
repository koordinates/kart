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
        requirements.txt, e.g. local packages
    SOURCES (list of string):
        Any sources that local packages depend on.
    PREFIX (string):
        Path where the environment will be placed at. Can be relative (under
        ${CMAKE_CURRENT_BINARY_DIR}) or absolute.
    ENV_NAME (string)
        The name of the virtual environment. Unless otherwise specified, this
        is the same as TARGET.
    NO_UPGRADE_PIP (bool)
        Don't upgrade pip inside the virtualenv to the latest.


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

#]=============================================================================]

# Create a Python virtual environment with specific requirements.
function(CreateVirtualEnvironment TARGET)
  # cmake-lint: disable=R0915
  set(SINGLE_ARGS NO_UPGRADE_PIP)
  set(KEYWORD_ARGS REQUIREMENTS_TXT PREFIX ENV_NAME)
  set(MULTI_ARGS SOURCES REQUIREMENTS)

  cmake_parse_arguments(ARG "${SINGLE_ARGS}" "${KEYWORD_ARGS}" "${MULTI_ARGS}" ${ARGN})

  if(NOT ARG_ENV_NAME)
    set(ARG_ENV_NAME ${TARGET})
  endif()

  find_package(Python3 REQUIRED COMPONENTS Interpreter)

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
    set(EXEC ${CMAKE_COMMAND} -E env "PATH=${BIN_DIR}:$ENV{PATH}")
    set(PYTHON_EXE ${BIN_DIR}/python.exe)
    set(PYTHON ${EXEC} ${PYTHON_EXE})
    set(PIP ${EXEC} ${BIN_DIR}/pip)
  else()
    set(BIN_DIR ${VENV}/bin)
    set(EXEC ${CMAKE_COMMAND} -E env "PATH=${BIN_DIR}:$ENV{PATH}")
    set(PYTHON_EXE ${BIN_DIR}/python)
    set(PYTHON ${EXEC} ${PYTHON_EXE})
    set(PIP ${EXEC} ${BIN_DIR}/pip)
  endif()

  set(PIP_INSTALL ${PIP} install --disable-pip-version-check)

  if(ARG_NO_UPGRADE_PIP)
    set(PIP_UPGRADE "")
  else()
    set(PIP_UPGRADE ${PIP_INSTALL} --upgrade pip)
  endif()

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
    COMMAND ${Python3_EXECUTABLE} -m venv ${VENV}
    COMMENT "${ARG_ENV_NAME}: creating virtualenv...")
  set(OUTPUT_FILE ${VENV}/.requirements)
  add_custom_command(
    OUTPUT ${OUTPUT_FILE}
    COMMAND ${PIP_UPGRADE}
    COMMAND ${DEPS_INSTALL}
    COMMAND ${BIN_DIR}/pip freeze > ${OUTPUT_FILE}
    DEPENDS ${CFG_FILE} ${ARG_SOURCES} ${ARG_REQUIREMENTS_TXT}
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
endfunction()
