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
    Python3_MACOS_DEPLOYMENT_TARGET: (macOS only)
        Stores the macOS deployment target Python was built with (eg: 10.9)

#]=============================================================================]

# Get ABI information for the current Python3 interpreter
function(PythonGetABIInfo)
  if(NOT Python3_INTERPRETER_ID STREQUAL "Python")
    message(FATAL_ERROR "Unsupported Python interpreter: ${Python3_INTERPRETER_ID}")
  endif()

  if(NOT DEFINED Python3_ABIFLAGS)
    # ABI flags are 'd'/'m'/''/etc
    execute_process(
      COMMAND ${Python3_EXECUTABLE} -c "import sys; print(sys.abiflags)" COMMAND_ERROR_IS_FATAL ANY
      OUTPUT_VARIABLE py_ABIFLAGS
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(Python3_ABIFLAGS
        ${py_ABIFLAGS}
        CACHE STRING "Python3 ABI flags")
    message(STATUS "Python3 ABI flags: [${py_ABIFLAGS}]")
  endif()

  if(NOT Python3_WHEEL_ID)
    # eg: cp37-cp37m
    set(py_ver_code
        "cp${Python3_VERSION_MAJOR}${Python3_VERSION_MINOR}-cp${Python3_VERSION_MAJOR}${Python3_VERSION_MINOR}${Python3_ABIFLAGS}"
    )
    if(MACOS)
      # find the macOS deployment target Python was built with. this determines the associated
      # version of extension modules
      if(NOT Python3_MACOS_DEPLOYMENT_TARGET)
        execute_process(
          COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/cmake/macos_get_deployment.sh "${Python3_EXECUTABLE}"
                  COMMAND_ERROR_IS_FATAL ANY
          OUTPUT_VARIABLE Python3_MACOS_DEPLOYMENT_TARGET
          OUTPUT_STRIP_TRAILING_WHITESPACE)
        if((NOT ${RESULT_VARIABLE} EQUAL 0) OR (NOT ${Python3_MACOS_DEPLOYMENT_TARGET}))
          message(
            FATAL_ERROR
              "Can't determine MacOS Deployment target for ${Python3_EXECUTABLE}. Set via Python3_MACOS_DEPLOYMENT_TARGET=X.Y"
          )
        endif()
        set(Python3_MACOS_DEPLOYMENT_TARGET
            ${Python3_MACOS_DEPLOYMENT_TARGET}
            CACHE STRING "Python3 macOS deployment target")
        message(STATUS "Python3 macOS Deployment target: ${Python3_MACOS_DEPLOYMENT_TARGET}")
      endif()
      # turn 10.9 into 10_9
      string(REPLACE "." "_" py_deployment_target_id ${Python3_MACOS_DEPLOYMENT_TARGET})
      set(py_wheelid "${py_ver_code}-macosx_${py_deployment_target_id}_${CMAKE_SYSTEM_PROCESSOR}")
    elseif(LINUX)
      set(py_wheelid "${py_ver_code}-linux_${CMAKE_SYSTEM_PROCESSOR}")
    elseif(WIN32)
      if(${CMAKE_GENERATOR_PLATFORM} STREQUAL Win32)
        set(py_wheelid "${py_ver_code}-win32")
      elseif(${CMAKE_GENERATOR_PLATFORM} STREQUAL x64)
        set(py_wheelid "${py_ver_code}-win_amd64")
      else()
        message(FATAL_ERROR "Couldn't determine Windows arch? (${CMAKE_GENERATOR_PLATFORM})")
      endif()
    else()
      message(FATAL_ERROR "PythonGetABIInfo: Unknown OS")
    endif()
    # Python3_WHEEL_ID
    set(Python3_WHEEL_ID
        ${py_wheelid}
        CACHE STRING "Python3 wheel identifier")
    message(STATUS "Python3 Wheel identifier: ${py_wheelid}")
  endif()
endfunction()
