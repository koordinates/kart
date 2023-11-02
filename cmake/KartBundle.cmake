include(PyCreateVirtualEnvironment)

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/bundleEnv.requirements.txt
     "pyinstaller~=6.1.0\n" "macholib>=1.8;sys_platform=='darwin'\n")

createvirtualenvironment(bundleEnv REQUIREMENTS_TXT "bundleEnv.requirements.txt")

file(CONFIGURE OUTPUT "VERSION" CONTENT "${KART_VERSION}\n")

set(PYINSTALLER_ENV
    "BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}" "PYTHONPATH=${bundleEnv_PURELIB_DIR}"
    "KART_VERSION=${KART_VERSION}" "KART_VERSION_FILE=${CMAKE_CURRENT_BINARY_DIR}/VERSION"
    "USE_CLI_HELPER=${CLI_HELPER}")

set(BUNDLE_DIR_NAME "kart")
if(MACOS)
  set(BUNDLE_DIR_NAME "Kart.app")
  list(APPEND PYINSTALLER_ENV "DYLD_LIBRARY_PATH=${CMAKE_CURRENT_BINARY_DIR}/venv/lib")
  set(BUNDLE_DIR ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/dist/${BUNDLE_DIR_NAME})
  set(BUNDLE_EXE_DIR ${BUNDLE_DIR}/Contents/MacOS)
  set(BUNDLE_EXE ${BUNDLE_EXE_DIR}/kart)
  set(BUNDLE_PREFIX_REL_EXE ${BUNDLE_DIR_NAME}/Contents/MacOS/kart)
elseif(LINUX)
  list(APPEND PYINSTALLER_ENV "LD_LIBRARY_PATH=${CMAKE_CURRENT_BINARY_DIR}/venv/lib")
  set(BUNDLE_DIR ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/dist/${BUNDLE_DIR_NAME})
  set(BUNDLE_EXE_DIR ${BUNDLE_DIR})
  set(BUNDLE_EXE ${BUNDLE_EXE_DIR}/kart)
  set(BUNDLE_PREFIX_REL_EXE ${BUNDLE_DIR_NAME}/kart)
elseif(WIN32)
  set(BUNDLE_DIR ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/dist/${BUNDLE_DIR_NAME})
  set(BUNDLE_EXE_DIR ${BUNDLE_DIR})
  set(BUNDLE_EXE ${BUNDLE_EXE_DIR}/kart.exe)
  set(BUNDLE_PREFIX_REL_EXE ${BUNDLE_DIR_NAME}/kart.exe)
endif()

if(NOT WIN32)
  set(BUNDLE_DEPENDS kart_cli_helper)
endif()

add_custom_command(
  OUTPUT pyinstaller.stamp ${BUNDLE_EXE}
  DEPENDS bundleEnv kart.spec VERSION ${BUNDLE_DEPENDS} # ${KART_EXE_VENV}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/
  COMMAND
    ${CMAKE_COMMAND} -E env ${PYINSTALLER_ENV} -- ${VENV_PY} -m PyInstaller --clean -y --workpath
    ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/ --distpath
    ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/dist/ kart.spec
  VERBATIM
  COMMENT "Running PyInstaller")

set(BUNDLE_DEPS pyinstaller.stamp)

#
# Code signing
#
if(WIN32 AND WIN_SIGN_BUNDLE)
  # Windows code-signing using AzureSignTool
  message(STATUS "Enabling Windows code-signing")
  add_custom_command(
    OUTPUT pyinstaller/codesign.stamp
    DEPENDS pyinstaller/dist/kart/kart.exe
    COMMAND
      ${CMAKE_COMMAND} "-DSIGNTOOL=${WIN_SIGNTOOL}" "-DAZURESIGNTOOL=${WIN_AZURESIGNTOOL}"
      "-DBUNDLE=${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/dist/kart" -P
      "${CMAKE_CURRENT_LIST_DIR}/win_codesign.cmake"
    COMMAND ${CMAKE_COMMAND} -E touch pyinstaller/codesign.stamp
    VERBATIM
    COMMENT "Code-signing Windows application bundle")
  list(APPEND BUNDLE_DEPS pyinstaller/codesign.stamp)

elseif(MACOS AND MACOS_SIGN_BUNDLE)
  # macOS code-signing
  message(STATUS "Enabling macOS code-signing using identity: $ENV{MACOS_CODESIGN_ID}")
  add_custom_command(
    OUTPUT pyinstaller/codesign.stamp
    DEPENDS ${BUNDLE_EXE}
    COMMAND
      ${XCODE_CODESIGN} --sign "$ENV{MACOS_CODESIGN_ID}" --verbose=3 --deep --timestamp --force
      --strict --entitlements ${CMAKE_CURRENT_SOURCE_DIR}/platforms/macos/entitlements.plist -o
      runtime pyinstaller/dist/Kart.app
    COMMAND ${XCODE_CODESIGN} --display --verbose pyinstaller/dist/Kart.app
    COMMAND ${XCODE_CODESIGN} --verify --verbose --deep --strict=all pyinstaller/dist/Kart.app
    COMMAND ${CMAKE_COMMAND} -E touch pyinstaller/codesign.stamp
    VERBATIM
    COMMENT "Code-signing macOS bundle")
  list(APPEND BUNDLE_DEPS pyinstaller/codesign.stamp)

  if(MACOS_NOTARIZE)
    # macos notarization
    add_custom_command(
      OUTPUT pyinstaller/notarize.stamp
      DEPENDS pyinstaller/codesign.stamp
      BYPRODUCTS kart-bundle-notarize.zip
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/dist
      COMMAND ${XCODE_DITTO} -c -k --sequesterRsrc --keepParent "Kart.app"
              "kart-bundle-notarize.zip"
      COMMAND ${XCODE_XCRUN} notarytool submit kart-bundle-notarize.zip --keychain-profile
              "$ENV{MACOS_NOTARIZE_KEYCHAIN_PROFILE}" --wait --timeout ${MACOS_NOTARIZE_TIMEOUT}
      COMMAND ${XCODE_XCRUN} stapler staple Kart.app
      COMMAND ${XCODE_SPCTL} --assess -t execute -vvv Kart.app
      COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/pyinstaller/notarize.stamp
      VERBATIM
      COMMENT "Notarizing macOS bundle")
    list(APPEND BUNDLE_DEPS pyinstaller/notarize.stamp)
  endif()
endif()

add_custom_target(
  bundle
  DEPENDS ${BUNDLE_DEPS}
  COMMENT "Bundle Kart for installation & packaging")
