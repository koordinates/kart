#[=============================================================================[
----------------
PythonGetABIInfo
----------------
Gets ABI Info for the current Python3 interpreter.

Output Variables:
    Python3_ABIFLAGS:
        ABI flag code string (d=debug, m=malloc, etc)
    Python3_WHEEL_ID:
        Wheel ABI identifier. eg: cp37-cp37m-macosx_10.9_x86_64
    Python3_MACOSX_DEPLOYMENT_TARGET: (macOS only)
        Stores the macOS deployment target Python was built with (eg: 10.9)
    Python3_PURELIB_REL_PATH:
        Relative path from a virtualenv root to the site-packages directory
        eg: lib/python3.11/site-packages

#]=============================================================================]

# Get ABI information for the current Python3 interpreter
function(PythonGetABIInfo)
  if(NOT Python3_INTERPRETER_ID STREQUAL "Python")
    message(FATAL_ERROR "Unsupported Python interpreter: ${Python3_INTERPRETER_ID}")
  endif()

  if(NOT DEFINED Python3_ABIFLAGS)
    # ABI flags are 'd'/'m'/''/etc
    execute_process(
      COMMAND
        ${Python3_EXECUTABLE} -c "import sysconfig; print(sysconfig.get_config_var('abiflags'))"
        COMMAND_ERROR_IS_FATAL ANY
      OUTPUT_VARIABLE py_ABIFLAGS
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(Python3_ABIFLAGS
        ${py_ABIFLAGS}
        CACHE INTERNAL "Python3 ABI flags")
    message(STATUS "Python3 ABI flags: [${py_ABIFLAGS}]")
  endif()

  if(NOT (DEFINED Python3_PLATFORM_TAG AND DEFINED Python3_WHEEL_ID))
    # Platform tag (win32_x64, linux_x86_64, MACOSX_10_9_x86_64, macosx_10_9_universal2, etc)
    execute_process(
      COMMAND ${Python3_EXECUTABLE} -c "import sysconfig; print(sysconfig.get_platform())"
              COMMAND_ERROR_IS_FATAL ANY
      OUTPUT_VARIABLE py_PLATFORM_TAG
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    message(STATUS "Python3 interpreter platform tag: ${py_PLATFORM_TAG}")
    set(Python3_INTERPRETER_PLATFORM_TAG
        ${py_PLATFORM_TAG}
        CACHE INTERNAL "Python3 original interpreter platform tag")

    if(MACOS)
      # Get interpreter macOS deployment target
      execute_process(
        COMMAND
          ${Python3_EXECUTABLE} -c
          "import sysconfig; print(sysconfig.get_config_var('MACOSX_DEPLOYMENT_TARGET'))"
          COMMAND_ERROR_IS_FATAL ANY
        OUTPUT_VARIABLE py_INTERPRETER_MACOSX_DEPLOYMENT_TARGET
        OUTPUT_STRIP_TRAILING_WHITESPACE)

      if(NOT (CMAKE_OSX_DEPLOYMENT_TARGET VERSION_EQUAL py_INTERPRETER_MACOSX_DEPLOYMENT_TARGET))
        message(
          WARNING
            "CMAKE_OSX_DEPLOYMENT_TARGET is ${CMAKE_OSX_DEPLOYMENT_TARGET}. "
            "The Python3 interpreter macOS deployment target is ${py_INTERPRETER_MACOSX_DEPLOYMENT_TARGET}. "
            "Greater one will be used.")
      endif()

      if(CMAKE_OSX_DEPLOYMENT_TARGET VERSION_GREATER py_INTERPRETER_MACOSX_DEPLOYMENT_TARGET)
        set(py_MACOSX_DEPLOYMENT_TARGET "${CMAKE_OSX_DEPLOYMENT_TARGET}")
      else()
        set(py_MACOSX_DEPLOYMENT_TARGET "${py_INTERPRETER_MACOSX_DEPLOYMENT_TARGET}")
      endif()

      if(py_MACOSX_DEPLOYMENT_TARGET MATCHES "^[0-9]+$")
        # We need MAJOR.MINOR - if we have MAJOR but not .MINOR, append ".0" to the end.
        set(py_MACOSX_DEPLOYMENT_TARGET "${py_MACOSX_DEPLOYMENT_TARGET}.0")
      endif()

      set(Python3_MACOSX_DEPLOYMENT_TARGET
          ${py_MACOSX_DEPLOYMENT_TARGET}
          CACHE INTERNAL "Python3 interpreter macOS deployment target")
      message(STATUS "Python3 interpreter macOS deployment target: ${py_MACOSX_DEPLOYMENT_TARGET}")

      string(REGEX REPLACE "${py_INTERPRETER_MACOSX_DEPLOYMENT_TARGET}"
                           "${py_MACOSX_DEPLOYMENT_TARGET}" py_PLATFORM_TAG "${py_PLATFORM_TAG}")
    endif()

    string(REGEX REPLACE "[-\.]+" "_" py_PLATFORM_TAG "${py_PLATFORM_TAG}")
    set(Python3_PLATFORM_TAG
        ${py_PLATFORM_TAG}
        CACHE INTERNAL "Python3 platform tag")
    message(STATUS "Final Python3 platform tag: ${py_PLATFORM_TAG}")

    # eg: cp37-cp37m
    set(py_ver_code
        "cp${Python3_VERSION_MAJOR}${Python3_VERSION_MINOR}-cp${Python3_VERSION_MAJOR}${Python3_VERSION_MINOR}${Python3_ABIFLAGS}"
    )

    # Full Wheel ABI ID
    set(py_wheelid "${py_ver_code}-${py_PLATFORM_TAG}")
    set(Python3_WHEEL_ID
        ${py_wheelid}
        CACHE INTERNAL "Python3 wheel identifier")
    message(STATUS "Python3 Wheel identifier: ${py_wheelid}")
  endif()

  if(NOT DEFINED Python3_PURELIB_REL_PATH)
    # Get the relative path between a Python venv root & the associated site-packages directory. We
    # have to do this at configure-time, which means we can't use the *venv* sysconfig module, we
    # need to use the system python. Which of course produces slightly different paths on Debian.
    execute_process(
      COMMAND
        ${Python3_EXECUTABLE} -c
        "import sysconfig,pathlib; print(pathlib.Path(sysconfig.get_path('purelib')).relative_to(pathlib.Path(sysconfig.get_path('scripts')).parent).parent / 'site-packages')"
        COMMAND_ERROR_IS_FATAL ANY
      OUTPUT_VARIABLE purelib_path
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(Python3_PURELIB_REL_PATH
        ${purelib_path}
        CACHE INTERNAL "Python3 site-packages relative path")
  endif()
endfunction()
