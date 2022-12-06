cmake_minimum_required(VERSION 3.21)

if(USE_VCPKG)
  # this needs to be super-early, before the first project() call
  include("vcpkg-vendor/vcpkg-toolchain-setup.cmake")
endif()

project(
  kart
  VERSION 0.12.0
  DESCRIPTION "Version control for geospatial data"
  HOMEPAGE_URL "https://kartproject.org"
  LANGUAGES C CXX)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

set(CMAKE_TLS_VERIFY TRUE)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(LINUX ON)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(MACOS ON)
endif()

include("vcpkg-vendor/osx-setup.cmake")
include(PythonGetABIInfo)
include(CCache)

# Set the Kart application version
if(NOT DEFINED KART_VERSION OR KART_VERSION STREQUAL "")
  file(STRINGS kart/VERSION KART_BASE_VERSION LIMIT_COUNT 1)
  find_package(Git REQUIRED)
  execute_process(
    COMMAND ${GIT_EXECUTABLE} describe --match="" --always --abbrev --dirty=X
    RESULT_VARIABLE err
    OUTPUT_VARIABLE KART_COMMIT_ID
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(NOT err AND KART_COMMIT_ID)
    set(KART_VERSION "${KART_BASE_VERSION}+git${KART_COMMIT_ID}")
  else()
    set(KART_VERSION "${KART_BASE_VERSION}")
  endif()
endif()
set(CMAKE_PROJECT_VERSION "${KART_VERSION}")
set(CPACK_PACKAGE_VERSION "${KART_VERSION}")
message(STATUS "Kart build version: ${KART_VERSION}")

#
# options
#

option(USE_VCPKG "Use vcpkg for vendor dependencies")
option(CLI_HELPER "Default to the CLI helper on macOS & Linux")
set(VENDOR_ARCHIVE
    ""
    CACHE
      FILEPATH
      "Use vendor packages from CI: path to vendor archive file from https://github.com/koordinates/kart"
)

if(MACOS)
  option(MACOS_SIGN_BUNDLE "Notarize macOS bundle")
  option(MACOS_SIGN_PKG "Notarize macOS installer")
  option(MACOS_NOTARIZE "Notarize macOS bundle & installer")
  set(MACOS_NOTARIZE_TIMEOUT
      1h
      CACHE STRING "Timeout for Apple Notarization (eg: 60m, 1h)")

  if(MACOS_SIGN_BUNDLE)
    find_program(XCODE_CODESIGN codesign REQUIRED)
  endif()
  if(MACOS_SIGN_PKG)
    find_program(XCODE_PKGUTIL pkgutil REQUIRED)
  endif()
  if(MACOS_NOTARIZE)
    find_program(XCODE_XCRUN xcrun REQUIRED)
    find_program(XCODE_SPCTL spctl REQUIRED)
    find_program(XCODE_DITTO ditto REQUIRED)
  endif()
endif()

#
# setup
#

if(PROJECT_IS_TOP_LEVEL)
  include(CTest)

  if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    if(MACOS)
      set(CMAKE_INSTALL_PREFIX
          "/Applications"
          CACHE PATH "Install path prefix" FORCE)
    elseif(LINUX)
      set(CMAKE_INSTALL_PREFIX
          "/opt"
          CACHE PATH "Install path prefix" FORCE)
    endif()
  endif()
endif()

# OSX: prefer Homebrew over Frameworks
set(Python3_FIND_IMPLEMENTATIONS "CPython")
find_package(Python3 3.9 REQUIRED COMPONENTS Development Interpreter)
pythongetabiinfo()

if(USE_VCPKG)
  message("Using VCPKG for vendor dependencies...")
  add_subdirectory(vcpkg-vendor)
  set(VENDOR_TARGET vendor)
else()
  #
  # dependencies
  #
  if(VENDOR_ARCHIVE)
    message("Using prebuilt vendor dependencies from ${VENDOR_ARCHIVE} ...")
    if(NOT EXISTS ${VENDOR_ARCHIVE})
      message(
        FATAL_ERROR
          "${VENDOR_ARCHIVE} not found. Download from https://github.com/koordinates/kart/actions")
    endif()
    set(VENDOR_TARGET ${VENDOR_ARCHIVE})
  else()
    message("Using local vendor dependencies...")
    add_subdirectory(vendor)
    set(VENDOR_TARGET vendor)
  endif()
endif()

set(DOCS "docs/pages/commands")
file(COPY ${DOCS} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
set(DOCS "scripts")
file(COPY ${DOCS} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

#
# main build targets
#
if(NOT WIN32)
  add_subdirectory(cli_helper)
endif()

include(KartPy)
include(KartBundle)

#
# Tests
#
if(BUILD_TESTING)
  #
  # Main unit tests (label=pytest)
  #
  if(DEFINED LINUX AND DEFINED ENV{KART_SQLSERVER_URL})
    # pytest-xdist causes some weird behaviour with pyodbc on Linux. Split MSSQL tests out into a
    # separate pytest run.
    add_test(
      NAME pytest
      COMMAND ${VENV_PYTEST} -v -m "not\ mssql" ${PYTEST_ARGS}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    set_property(TEST pytest PROPERTY LABELS "pytest")

    add_test(
      NAME pytest-mssql
      COMMAND ${VENV_PYTEST} -v -p no:xdist -m mssql ${PYTEST_ARGS}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    set_property(TEST pytest-mssql PROPERTY LABELS "pytest")
  else()
    add_test(
      NAME pytest
      COMMAND ${VENV_PYTEST} -v ${PYTEST_ARGS}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    set_property(TEST pytest PROPERTY LABELS "pytest")
  endif()

  #
  # E2E tests for bundles/packages (label=e2e)
  #
  find_program(
    SQLite3_EXE
    NAMES sqlite3
    PATHS "${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}"
    PATH_SUFFIXES tools share)
  cmake_path(GET SQLite3_EXE PARENT_PATH SQLite3_EXE_DIR)
  if(WIN32)
    add_test(
      NAME e2e-1
      COMMAND Powershell.exe -File "tests\\scripts\\e2e-1.ps1"
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  else()
    add_test(
      NAME e2e-1
      COMMAND tests/scripts/e2e-1.sh
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

    if(CLI_HELPER)
      # Run the same test under Linux & Mac using the cli helper
      add_test(NAME cli-helper-clean COMMAND pkill -f "/kart_cli helper")
      set_tests_properties(cli-helper-clean PROPERTIES LABELS "e2e" SKIP_RETURN_CODE 1
                                                       FIXTURES_SETUP cli-helper)
      add_test(
        NAME e2e-1-helper
        COMMAND tests/scripts/e2e-1.sh
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
      set_tests_properties(
        e2e-1-helper
        PROPERTIES
          LABELS
          "e2e"
          FIXTURES_REQUIRED
          cli-helper
          ENVIRONMENT_MODIFICATION
          "PATH=path_list_prepend:${SQLite3_EXE_DIR};PATH=path_list_prepend:${BUNDLE_EXE_DIR}"
          ENVIRONMENT
          "KART_USE_HELPER=1;KART_HELPER_LOG=${CMAKE_CURRENT_BINARY_DIR}/e2e-1-helper.log")
    endif()
  endif()
  set_tests_properties(
    e2e-1
    PROPERTIES LABELS "e2e" ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:${SQLite3_EXE_DIR};PATH=path_list_prepend:${BUNDLE_EXE_DIR}")
endif()

#
# Installation
#

install(
  DIRECTORY ${BUNDLE_DIR}
  DESTINATION "."
  USE_SOURCE_PERMISSIONS
  COMPONENT bundle)
if(NOT WIN32)
  set(KART_SYSTEM_SYMLINK_DIR
      /usr/local/bin
      CACHE PATH "Directory to create the system path kart symlink in")
  if(KART_SYSTEM_SYMLINK_DIR)
    install(
      CODE "message(STATUS \"Installing link: ${KART_SYSTEM_SYMLINK_DIR}/kart -> $<PATH:APPEND,\${CMAKE_INSTALL_PREFIX},${BUNDLE_PREFIX_REL_EXE}>\")"
      CODE "file(CREATE_LINK \"$<PATH:APPEND,\${CMAKE_INSTALL_PREFIX},${BUNDLE_PREFIX_REL_EXE}>\" \"${KART_SYSTEM_SYMLINK_DIR}/kart\" SYMBOLIC)"
      COMPONENT addToPath)
  endif()
endif()

#
# Packaging
#
set(CPACK_PACKAGE_NAME "Kart")
set(CPACK_PACKAGE_VENDOR "Kart Project")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Version control for geospatial data")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://kartproject.org")
set(CPACK_VERBATIM_VARIABLES YES)
set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)

if(MACOS)
  configure_file(platforms/macos/intro.md macos-intro.md @ONLY)
  configure_file(platforms/macos/summary.md macos-summary.md @ONLY)

  find_program(PANDOC pandoc REQUIRED)

  execute_process(
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${PANDOC} -s macos-intro.md --from=gfm -o macos-pkg-welcome.rtf COMMAND_ERROR_IS_FATAL
            ANY)
  execute_process(
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${PANDOC} -s macos-summary.md --from=gfm -o macos-pkg-readme.rtf COMMAND_ERROR_IS_FATAL
            ANY)
  execute_process(
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND ${PANDOC} -s ${CMAKE_CURRENT_SOURCE_DIR}/COPYING --from=twiki -o macos-pkg-license.rtf
            COMMAND_ERROR_IS_FATAL ANY)
  file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-resources)
  file(COPY_FILE platforms/macos/background.png
       ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-resources/background.png)

  set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY OFF)
  set(CPACK_SYSTEM_NAME "macOS-${CMAKE_HOST_SYSTEM_PROCESSOR}")
  set(CPACK_PRODUCTBUILD_IDENTIFIER "com.koordinates.Sno.SnoCore")
  set(CPACK_PACKAGE_ICON ${CMAKE_CURRENT_SOURCE_DIR}/platforms/macos/kart.icns)
  set(CPACK_PRODUCTBUILD_RESOURCES_DIR ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-resources)
  set(CPACK_RESOURCE_FILE_LICENSE ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-license.rtf)
  set(CPACK_RESOURCE_FILE_WELCOME ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-welcome.rtf)
  set(CPACK_RESOURCE_FILE_README ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-readme.rtf)
  set(CPACK_PRODUCTBUILD_BACKGROUND background.png)

  if(MACOS_SIGN_BUNDLE AND MACOS_SIGN_PKG)
    set(CPACK_PKGBUILD_IDENTITY_NAME $ENV{MACOS_PKGSIGN_ID})
    set(CPACK_PRODUCTBUILD_IDENTITY_NAME $ENV{MACOS_PKGSIGN_ID})
  endif()

  # https://gitlab.kitware.com/cmake/cmake/-/issues/21520
  file(
    WRITE ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-pre-build.cmake
    [=[
    file(MAKE_DIRECTORY "${CPACK_TEMPORARY_INSTALL_DIRECTORY}/addToPath/usr/local/bin/")
    file(CREATE_LINK /Applications/Kart.app/Contents/MacOS/kart "${CPACK_TEMPORARY_INSTALL_DIRECTORY}/addToPath/usr/local/bin/kart" SYMBOLIC)
  ]=])
  set(CPACK_PRE_BUILD_SCRIPTS ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-pre-build.cmake)

  if(MACOS_SIGN_BUNDLE
     AND MACOS_SIGN_PKG
     AND MACOS_NOTARIZE)
    file(
      WRITE ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-post-build.cmake
      "
      message(STATUS \"Checking PKG signature...\")
      execute_process(
        COMMAND ${XCODE_PKGUTIL} --check-signature \${CPACK_OUTPUT_FILE_PATH}
        COMMAND_ECHO STDERR
        COMMAND_ERROR_IS_FATAL ANY)
      message(STATUS \"Notarizing PKG with notarytool...\")
      execute_process(
        COMMAND ${XCODE_XCRUN} notarytool submit \${CPACK_OUTPUT_FILE_PATH}
          --keychain-profile \"$ENV{MACOS_NOTARIZE_KEYCHAIN_PROFILE}\"
          --wait --timeout ${MACOS_NOTARIZE_TIMEOUT}
        COMMAND_ECHO NONE
        COMMAND_ERROR_IS_FATAL ANY)
      message(STATUS \"Stapling notarization to PKG...\")
      execute_process(
        COMMAND ${XCODE_XCRUN} stapler staple \${CPACK_OUTPUT_FILE_PATH}
        COMMAND_ECHO STDERR
        COMMAND_ERROR_IS_FATAL ANY)
      message(STATUS \"Checking PKG notarization...\")
      execute_process(
        COMMAND ${XCODE_SPCTL} --assess -vvv --type install \${CPACK_OUTPUT_FILE_PATH}
        COMMAND_ECHO STDERR
        COMMAND_ERROR_IS_FATAL ANY)
      ")
    set(CPACK_POST_BUILD_SCRIPTS ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-post-build.cmake)
  elseif(MACOS_SIGN_BUNDLE AND MACOS_SIGN_PKG)
    file(
      WRITE ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-post-build.cmake
      "
      message(STATUS \"Checking PKG signature...\")
      execute_process(
        COMMAND ${XCODE_PKGUTIL} --check-signature \${CPACK_OUTPUT_FILE_PATH}
        COMMAND_ECHO STDERR
        COMMAND_ERROR_IS_FATAL ANY)
      ")
    set(CPACK_POST_BUILD_SCRIPTS ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-post-build.cmake)
  endif()

  set(CPACK_GENERATOR "ZIP;productbuild")

elseif(LINUX)
  set(CPACK_SYSTEM_NAME "linux-${CMAKE_HOST_SYSTEM_PROCESSOR}")
  set(CPACK_GENERATOR "TGZ")

elseif(WIN32)
  set(CPACK_GENERATOR "ZIP")

endif()

include(CPack)

cpack_add_component(
  bundle
  DISPLAY_NAME "Kart"
  DESCRIPTION "Kart command-line tools"
  REQUIRED)

cpack_add_component(
  addToPath
  DISPLAY_NAME "Add to PATH"
  DESCRIPTION "Add kart to the system path")
