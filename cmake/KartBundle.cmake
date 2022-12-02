include(PyCreateVirtualEnvironment)

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/bundleEnv.requirements.txt
     "pyinstaller~=5.6.2\n" "macholib>=1.8;sys_platform=='darwin'\n")

createvirtualenvironment(bundleEnv REQUIREMENTS_TXT "bundleEnv.requirements.txt")

set(PYINSTALLER_ENV "BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}" "PYTHONPATH=${bundleEnv_PURELIB_DIR}")

if(MACOS)
  list(APPEND PYINSTALLER_ENV "DYLD_LIBRARY_PATH=${CMAKE_CURRENT_BINARY_DIR}/venv/lib")
  set(BUNDLE_DIR ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/dist/Kart.app)
  set(BUNDLE_EXE_DIR ${BUNDLE_DIR}/Contents/MacOS)
  set(BUNDLE_EXE ${BUNDLE_EXE_DIR}/kart)
  set(BUNDLE_PREFIX_REL_EXE Kart.app/Contents/MacOS/kart)
elseif(LINUX)
  list(APPEND PYINSTALLER_ENV "LD_LIBRARY_PATH=${CMAKE_CURRENT_BINARY_DIR}/venv/lib")
  set(BUNDLE_DIR ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/dist/kart)
  set(BUNDLE_EXE_DIR ${BUNDLE_DIR})
  set(BUNDLE_EXE ${BUNDLE_EXE_DIR}/kart)
  set(BUNDLE_PREFIX_REL_EXE kart/kart)
elseif(WIN32)
  set(BUNDLE_DIR ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/dist/kart)
  set(BUNDLE_EXE_DIR ${BUNDLE_DIR})
  set(BUNDLE_EXE ${BUNDLE_EXE_DIR}/kart.exe)
  set(BUNDLE_PREFIX_REL_EXE kart/kart.exe)
endif()

add_custom_command(
  OUTPUT pyinstaller.stamp ${BUNDLE_EXE}
  DEPENDS bundleEnv kart.spec # ${KART_EXE_VENV}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/
  COMMAND
    ${CMAKE_COMMAND} -E env ${PYINSTALLER_ENV} -- ${VENV_PY} -m PyInstaller --clean -y --workpath
    ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/ --distpath
    ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/dist/ kart.spec
  VERBATIM
  COMMENT "Running PyInstaller")

set(BUNDLE_DEPS pyinstaller.stamp)

if(WIN32 AND NOT "$ENV{SIGN_AZURE_CERTIFICATE}" STREQUAL "")
  # Windows code-signing using AzureSignTool
  message(STATUS "Enabling Windows code-signing")

  find_program(AZURESIGNTOOL azuresigntool REQUIRED PATHS "$ENV{USERPROFILE}/.dotnet/tools")
  message(STATUS "Found AzureSignTool: ${AZURESIGNTOOL}")
  find_program(SIGNTOOL signtool REQUIRED PATHS ENV WindowsSdkVerBinPath PATH_SUFFIXES x64)
  message(STATUS "Found signtool: ${SIGNTOOL}")

  add_custom_command(
    OUTPUT pyinstaller/codesign.stamp
    DEPENDS pyinstaller/dist/kart/kart.exe
    COMMAND
      ${CMAKE_COMMAND} "-DSIGNTOOL=${SIGNTOOL}" "-DAZURESIGNTOOL=${AZURESIGNTOOL}"
      "-DBUNDLE=${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/dist/kart" -P
      "${CMAKE_CURRENT_LIST_DIR}/win_codesign.cmake"
    COMMAND ${CMAKE_COMMAND} -E touch pyinstaller/codesign.stamp
    VERBATIM
    COMMENT "Code-signing Windows bundle")
  list(APPEND BUNDLE_DEPS pyinstaller/codesign.stamp)

elseif(MACOS AND NOT "$ENV{MACOS_CODESIGN_ID}" STREQUAL "")
  # macOS code-signing
  set(MACOS_CODESIGN_ID $ENV{MACOS_CODESIGN_ID})
  message(STATUS "Enabling macOS code-signing using identity: ${MACOS_CODESIGN_ID}")
  add_custom_command(
    OUTPUT pyinstaller/codesign.stamp
    DEPENDS ${BUNDLE_EXE}
    COMMAND
      codesign --sign "${MACOS_CODESIGN_ID}" --verbose=3 --deep --timestamp --force --strict
      --entitlements ${CMAKE_CURRENT_SOURCE_DIR}/platforms/macos/entitlements.plist -o runtime
      pyinstaller/dist/Kart.app
    COMMAND codesign --display --verbose pyinstaller/dist/Kart.app
    COMMAND codesign --verify --verbose --deep --strict=all pyinstaller/dist/Kart.app
    COMMAND ${CMAKE_COMMAND} -E touch pyinstaller/codesign.stamp
    VERBATIM
    COMMENT "Code-signing macOS bundle")
  list(APPEND BUNDLE_DEPS pyinstaller/codesign.stamp)
endif()

add_custom_target(
  bundle
  DEPENDS ${BUNDLE_DEPS}
  COMMENT "Bundle Kart for installation & packaging")
