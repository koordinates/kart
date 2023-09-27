cmake_minimum_required(VERSION 3.21)

if(USE_VCPKG)
  # this needs to be super-early, before the first project() call
  include("vcpkg-vendor/vcpkg-toolchain-setup.cmake")
endif()

project(
  kart
  VERSION 0.14.3
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
file(STRINGS kart/VERSION KART_BASE_VERSION LIMIT_COUNT 1)
string(REPLACE "." ";" _verParts "${KART_BASE_VERSION}")
list(GET _verParts 0 CMAKE_PROJECT_VERSION_MAJOR)
list(GET _verParts 1 CMAKE_PROJECT_VERSION_MINOR)
list(GET _verParts 2 CMAKE_PROJECT_VERSION_PATCH)
string(REGEX MATCH "^[0-9]+" CMAKE_PROJECT_VERSION_PATCH "${CMAKE_PROJECT_VERSION_PATCH}")

if(NOT DEFINED KART_VERSION OR KART_VERSION STREQUAL "")
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

if(MACOS OR LINUX)
  option(CLI_HELPER "Enable the CLI helper on macOS & Linux" OFF)
endif()

set(VENDOR_ARCHIVE
    ""
    CACHE
      FILEPATH
      "Use vendor packages from CI: path to vendor archive file from https://github.com/koordinates/kart"
)

if(MACOS)
  option(MACOS_SIGN_BUNDLE "Code-sign macOS bundle")
  option(MACOS_SIGN_PKG "Code-sign macOS .pkg installer")
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
elseif(WIN32)
  option(WIN_SIGN_BUNDLE "Code-sign Windows application")
  option(WIN_SIGN_INSTALLER "Code-sign Windows .msi installer")
  if(WIN_SIGN_BUNDLE OR WIN_SIGN_INSTALLER)
    find_program(WIN_AZURESIGNTOOL azuresigntool REQUIRED PATHS "$ENV{USERPROFILE}/.dotnet/tools")
    find_program(
      WIN_SIGNTOOL signtool REQUIRED
      PATHS ENV WindowsSdkVerBinPath
      PATH_SUFFIXES x64)
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
  if(DEFINED LINUX AND (DEFINED ENV{KART_SQLSERVER_URL} OR DEFINED ENV{KART_MYSQL_URL}))
    # pytest-xdist causes some weird behaviour with pyodbc/mysqldb on Linux. Split tests out into a
    # separate pytest run.
    add_test(
      NAME pytest
      COMMAND ${VENV_PYTEST} -v -m "not\ mssql\ and\ not\ mysql" ${PYTEST_ARGS}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    set_property(TEST pytest PROPERTY LABELS "pytest")

    if(DEFINED ENV{KART_SQLSERVER_URL})
      add_test(
        NAME pytest-mssql
        COMMAND ${VENV_PYTEST} -v -p no:xdist -m mssql ${PYTEST_ARGS}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
      set_property(TEST pytest-mssql PROPERTY LABELS "pytest")
    endif()

    if(DEFINED ENV{KART_MYSQL_URL})
      add_test(
        NAME pytest-mysql
        COMMAND ${VENV_PYTEST} -v -p no:xdist -m mysql ${PYTEST_ARGS}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
      set_property(TEST pytest-mysql PROPERTY LABELS "pytest")
    endif()
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
    set_tests_properties(e2e-1 PROPERTIES ENVIRONMENT "KART_USE_HELPER=0")

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

if(WIN32)
  set(_BUNDLE_INSTALL_DEST ".")
else()
  set(_BUNDLE_INSTALL_DEST ${BUNDLE_DIR_NAME})
endif()

install(
  DIRECTORY "${BUNDLE_DIR}/"
  DESTINATION "${_BUNDLE_INSTALL_DEST}"
  USE_SOURCE_PERMISSIONS
  COMPONENT bundle)
if(NOT WIN32)
  set(KART_SYSTEM_SYMLINK_DIR
      /usr/local/bin
      CACHE PATH "Directory to create the system path kart symlink in")

  if(KART_SYSTEM_SYMLINK_DIR)
    file(
      WRITE ${CMAKE_CURRENT_BINARY_DIR}/install-addToPath.cmake
      "
      set(P \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/..${KART_SYSTEM_SYMLINK_DIR}/kart\")
      cmake_path(NORMAL_PATH P)
      message(STATUS \"Installing link: \${P} (ie: ${KART_SYSTEM_SYMLINK_DIR}/kart) -> ${CMAKE_INSTALL_PREFIX}/${BUNDLE_PREFIX_REL_EXE}\")
      file(CREATE_LINK \"${CMAKE_INSTALL_PREFIX}/${BUNDLE_PREFIX_REL_EXE}\" \"\${P}\" SYMBOLIC)
      ")
    install(SCRIPT ${CMAKE_CURRENT_BINARY_DIR}/install-addToPath.cmake COMPONENT addToPath)
  endif()
endif()

#
# Packaging
#
set(CPACK_PACKAGE_NAME "Kart")
set(CPACK_PACKAGE_VENDOR "Kart Project")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Version control for geospatial data")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://kartproject.org")
set(CPACK_PACKAGE_CONTACT "support@koordinates.com")
set(CPACK_VERBATIM_VARIABLES YES)
set(CPACK_PROJECT_CONFIG_FILE "${CMAKE_SOURCE_DIR}/cmake/cpack_project_config.cmake")
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
  file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-resources)
  file(COPY_FILE platforms/macos/background.png
       ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-resources/background.png)

  set(CPACK_SYSTEM_NAME "macOS-${CMAKE_HOST_SYSTEM_PROCESSOR}")
  set(CPACK_PRODUCTBUILD_IDENTIFIER "com.koordinates.Sno.SnoCore")
  set(CPACK_PACKAGE_ICON ${CMAKE_CURRENT_SOURCE_DIR}/platforms/macos/kart.icns)
  set(CPACK_PRODUCTBUILD_RESOURCES_DIR ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-resources)
  set(CPACK_RESOURCE_FILE_LICENSE ${CMAKE_CURRENT_SOURCE_DIR}/platforms/license.rtf)
  set(CPACK_RESOURCE_FILE_WELCOME ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-welcome.rtf)
  set(CPACK_RESOURCE_FILE_README ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-readme.rtf)
  set(CPACK_PRODUCTBUILD_BACKGROUND background.png)

  if(MACOS_SIGN_BUNDLE AND MACOS_SIGN_PKG)
    set(CPACK_PKGBUILD_IDENTITY_NAME $ENV{MACOS_PKGSIGN_ID})
    set(CPACK_PRODUCTBUILD_IDENTITY_NAME $ENV{MACOS_PKGSIGN_ID})
  endif()

  # https://gitlab.kitware.com/cmake/cmake/-/issues/21520
  file(
    WRITE ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-pre-install.cmake
    "
    file(MAKE_DIRECTORY \"\${CPACK_TEMPORARY_INSTALL_DIRECTORY}/addToPath/usr/local/bin/\")
    file(CREATE_LINK \"${CMAKE_INSTALL_PREFIX}/Kart.app/Contents/MacOS/kart\" \"\${CPACK_TEMPORARY_INSTALL_DIRECTORY}/addToPath/usr/local/bin/kart\" SYMBOLIC)
  ")
  set(CPACK_INSTALL_SCRIPTS ${CMAKE_CURRENT_BINARY_DIR}/macos-pkg-pre-install.cmake)

  if(MACOS_SIGN_BUNDLE OR MACOS_SIGN_PKG)
    file(
      WRITE ${CMAKE_CURRENT_BINARY_DIR}/cpack-macos-post-build-20-check-signing.cmake
      "
      if(\"${MACOS_SIGN_PKG}\" AND \${CPACK_GENERATOR} STREQUAL \"productbuild\")
        message(STATUS \"Checking PKG signature...\")
        execute_process(
          COMMAND ${XCODE_PKGUTIL} --check-signature \${CPACK_TEMPORARY_PACKAGE_FILE_NAME}
          COMMAND_ECHO STDERR
          COMMAND_ERROR_IS_FATAL ANY)

      elseif(\"${MACOS_SIGN_BUNDLE}\" AND \${CPACK_GENERATOR} STREQUAL \"ZIP\")
        message(STATUS \"Checking ZIP Bundle signature...\")
        set(CHECK_DIR \"\${CPACK_PACKAGE_DIRECTORY}/_CPack_Packages/\${CPACK_SYSTEM_NAME}/ZIP-codesigning-check\")
        file(REMOVE_RECURSE \${CHECK_DIR})
        file(ARCHIVE_EXTRACT INPUT \"\${CPACK_TEMPORARY_PACKAGE_FILE_NAME}\" DESTINATION \${CHECK_DIR})
        execute_process(
          COMMAND ${XCODE_CODESIGN} --display --verbose \"\${CHECK_DIR}/\${CPACK_PACKAGE_FILE_NAME}/Kart.app\"
          COMMAND_ECHO STDERR
          COMMAND_ERROR_IS_FATAL ANY)
        execute_process(
          COMMAND ${XCODE_CODESIGN} --verify --verbose --deep --strict=all \"\${CHECK_DIR}/\${CPACK_PACKAGE_FILE_NAME}/Kart.app\"
          COMMAND_ECHO STDERR
          COMMAND_ERROR_IS_FATAL ANY)

      endif()
      ")
    list(APPEND CPACK_POST_BUILD_SCRIPTS
         ${CMAKE_CURRENT_BINARY_DIR}/cpack-macos-post-build-20-check-signing.cmake)

    if(MACOS_NOTARIZE)
      file(
        WRITE ${CMAKE_CURRENT_BINARY_DIR}/cpack-macos-post-build-30-notarize.cmake
        "
        if(\"${MACOS_SIGN_PKG}\" AND \${CPACK_GENERATOR} STREQUAL \"productbuild\")
          message(STATUS \"Checking PKG signature...\")
          execute_process(
            COMMAND ${XCODE_PKGUTIL} --check-signature \${CPACK_TEMPORARY_PACKAGE_FILE_NAME}
            COMMAND_ECHO STDERR
            COMMAND_ERROR_IS_FATAL ANY)
          message(STATUS \"Notarizing PKG with notarytool...\")
          execute_process(
            COMMAND ${XCODE_XCRUN} notarytool submit \${CPACK_TEMPORARY_PACKAGE_FILE_NAME}
              --keychain-profile \"$ENV{MACOS_NOTARIZE_KEYCHAIN_PROFILE}\"
              --wait --timeout ${MACOS_NOTARIZE_TIMEOUT}
            COMMAND_ECHO NONE
            COMMAND_ERROR_IS_FATAL ANY)
          message(STATUS \"Stapling notarization to PKG...\")
          execute_process(
            COMMAND ${XCODE_XCRUN} stapler staple \${CPACK_TEMPORARY_PACKAGE_FILE_NAME}
            COMMAND_ECHO STDERR
            COMMAND_ERROR_IS_FATAL ANY)
          message(STATUS \"Checking PKG notarization...\")
          execute_process(
            COMMAND ${XCODE_SPCTL} --assess -vvv --type install \${CPACK_TEMPORARY_PACKAGE_FILE_NAME}
            COMMAND_ECHO STDERR
            COMMAND_ERROR_IS_FATAL ANY)

        endif()
        ")
      list(APPEND CPACK_POST_BUILD_SCRIPTS
           ${CMAKE_CURRENT_BINARY_DIR}/cpack-macos-post-build-30-notarize.cmake)
    endif()
  endif()

  set(CPACK_GENERATOR "ZIP;productbuild")

elseif(LINUX)
  set(CPACK_SYSTEM_NAME "linux-${CMAKE_HOST_SYSTEM_PROCESSOR}")
  set(CPACK_GENERATOR "TGZ;DEB;RPM")
  # overridden to "" for TGZ
  set(CPACK_PACKAGING_INSTALL_PREFIX "/opt")
  set(CPACK_STRIP_FILES OFF)

  set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
  set(CPACK_DEBIAN_PACKAGE_DEPENDS openssh-client)
  set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
  set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS_PRIVATE_DIRS ${BUNDLE_DIR})

  set(CPACK_RPM_FILE_NAME RPM-DEFAULT)
  set(CPACK_RPM_PACKAGE_LICENSE "GPLv2")
  set(CPACK_RPM_PACKAGE_REQUIRES "openssh-clients")
  set(CPACK_RPM_PACKAGE_AUTOREQPROV OFF)
  set(CPACK_RPM_PACKAGE_RELOCATABLE OFF)
  set(CPACK_RPM_SPEC_CHANGELOG ${CMAKE_CURRENT_SOURCE_DIR}/CHANGELOG.md)
  # drop build-id links; python as python3; don't strip
  set(CPACK_RPM_SPEC_MORE_DEFINE
      "%define _build_id_links none\n%define __python ${Python3_EXECUTABLE}\n%define __strip /bin/true"
  )

  # https://gitlab.kitware.com/cmake/cmake/-/issues/21520
  file(
    WRITE ${CMAKE_CURRENT_BINARY_DIR}/linux-debrpm-pre-build.cmake
    "
    file(MAKE_DIRECTORY \"\${CPACK_TEMPORARY_INSTALL_DIRECTORY}/ALL_COMPONENTS_IN_ONE/usr/bin/\")
    file(CREATE_LINK \"${CMAKE_INSTALL_PREFIX}/kart/kart\" \"\${CPACK_TEMPORARY_INSTALL_DIRECTORY}/ALL_COMPONENTS_IN_ONE/usr/bin/kart\" SYMBOLIC)
    ")
  set(CPACK_PRE_BUILD_SCRIPTS ${CMAKE_CURRENT_BINARY_DIR}/linux-debrpm-pre-build.cmake)

elseif(WIN32)
  set(CPACK_GENERATOR "ZIP;WIX")
  set(CPACK_PACKAGE_INSTALL_DIRECTORY "Kart")
  set(CPACK_WIX_UPGRADE_GUID "53b2434b-7cba-4e79-b63d-2adde1478de9")
  set(CPACK_WIX_PRODUCT_GUID "70118EC7-3341-4039-9FA6-33B1F46CD908")
  set(CPACK_WIX_LICENSE_RTF "${CMAKE_CURRENT_SOURCE_DIR}/platforms/license.rtf")
  set(CPACK_WIX_PATCH_FILE "${CMAKE_CURRENT_SOURCE_DIR}/platforms/windows/cpack-wix-patch.xml")
  set(CPACK_WIX_UI_BANNER "${CMAKE_CURRENT_SOURCE_DIR}/platforms/windows/banner.bmp")
  set(CPACK_WIX_PRODUCT_ICON "${CMAKE_CURRENT_SOURCE_DIR}/platforms/windows/kart.ico")
  set(CPACK_WIX_UI_DIALOG "${CMAKE_CURRENT_SOURCE_DIR}/platforms/windows/dialog.bmp")
  set(CPACK_WIX_PROPERTY_ARPURLINFOABOUT "https://kartproject.org")
  set(CPACK_WIX_PROPERTY_ARPHELPLINK "https://github.com/koordinates/kart/discussions")
  set(CPACK_WIX_ARCHITECTURE "x64")

  if(WIN_SIGN_INSTALLER)
    file(
      WRITE ${CMAKE_CURRENT_BINARY_DIR}/win-msi-post-build.cmake
      "
      if(\${CPACK_GENERATOR} STREQUAL \"WIX\")
        message(STATUS \"Code-signing MSI installer...\")
        execute_process(
          COMMAND
            \"${CMAKE_COMMAND}\" \"-DSIGNTOOL=${WIN_SIGNTOOL}\" \"-DAZURESIGNTOOL=${WIN_AZURESIGNTOOL}\"
            \"-DFILES=\${CPACK_TEMPORARY_PACKAGE_FILE_NAME}\" \"-DDESCRIPTION=Kart Installer\" -P
            \"${CMAKE_CURRENT_LIST_DIR}/cmake/win_codesign.cmake\"
          COMMAND_ECHO STDERR
          COMMAND_ERROR_IS_FATAL ANY)
      endif()
      ")
    set(CPACK_POST_BUILD_SCRIPTS ${CMAKE_CURRENT_BINARY_DIR}/win-msi-post-build.cmake)
  endif()

endif()

include(CPack)

cpack_add_component(
  bundle
  DISPLAY_NAME "Kart"
  DESCRIPTION "Kart command-line tools"
  REQUIRED
  PLIST "${CMAKE_CURRENT_SOURCE_DIR}/platforms/macos/bundle-components.plist")

if(KART_SYSTEM_SYMLINK_DIR)
  cpack_add_component(
    addToPath
    DISPLAY_NAME "Add to PATH"
    DESCRIPTION "Add Kart to the system path")
endif()
