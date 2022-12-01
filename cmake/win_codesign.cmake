#
# Windows Code-signing using AzureSignTool
#

if(NOT DEFINED BUNDLE
OR NOT DEFINED SIGNTOOL
OR NOT DEFINED AZURESIGNTOOL
OR "$ENV{SIGN_AZURE_CERTIFICATE}" STREQUAL "")
message(
 FATAL_ERROR
   " Usage:\n"
   "   cmake -DBUNDLE=<path> -DSIGNTOOL=<path> -DAZURESIGNTOOL=<path> -P win_codesign.cmake\n"
   " Expects the following environment variables to be set:\n"
   "   SIGN_AZURE_VAULT\n"
   "   SIGN_AZURE_CERTIFICATE\n"
   "   SIGN_AZURE_CLIENTID\n"
   "   SIGN_AZURE_TENANTID\n"
   "   SIGN_AZURE_CLIENTSECRET")
endif()

# Find the binaries to sign
file(GLOB_RECURSE binaries LIST_DIRECTORIES false RELATIVE ${BUNDLE}
    "${BUNDLE}/*.exe"
    "${BUNDLE}/*.dll"
)
# Exclude binaries that aren't built as part of Kart
# These should be signed by someone else
list(FILTER binaries EXCLUDE REGEX "^git/")
list(FILTER binaries EXCLUDE REGEX "^MSVC.*\\.dll$")
list(FILTER binaries EXCLUDE REGEX "^VCRUNTIME.*\\.dll$")
list(FILTER binaries EXCLUDE REGEX "^python3.*\\.dll$")
list(LENGTH binaries binCount)
message(VERBOSE "Binaries to sign (${binCount}): ${binaries}")

list(TRANSFORM binaries PREPEND "${BUNDLE}/")
cmake_path(CONVERT "${binaries}" TO_NATIVE_PATH_LIST binariesPaths NORMALIZE)

set(TIMESTAMP_SERVERS
    "http://timestamp.digicert.com"
    "http://timestamp.geotrust.com/tsa"
    "http://timestamp.comodoca.com/rfc3161"
    "http://rfc3161timestamp.globalsign.com/advanced")

foreach(ts IN LISTS TIMESTAMP_SERVERS)
    message(STATUS "Signing ${binCount} binaries using timestamp sever ${ts} ...")
    execute_process(
        COMMAND ${AZURESIGNTOOL} sign
        "--azure-key-vault-url=$ENV{SIGN_AZURE_VAULT}"
        "--azure-key-vault-client-id=$ENV{SIGN_AZURE_CLIENTID}"
        "--azure-key-vault-client-secret=$ENV{SIGN_AZURE_CLIENTSECRET}"
        "--azure-key-vault-certificate=$ENV{SIGN_AZURE_CERTIFICATE}"
        "--azure-key-vault-tenant-id=$ENV{SIGN_AZURE_TENANTID}"
        "--description-url=https://kartproject.org"
        "--description=Kart CLI"
        "--timestamp-rfc3161=${ts}"
        ${binariesPaths}
        COMMAND_ECHO NONE
        RESULT_VARIABLE err
    )
    if(err EQUAL 0)
        message(STATUS "Signing successful")
        break()
    endif()
endforeach()

if(NOT err EQUAL 0)
    message(FATAL_ERROR "Signing error (last exit ${err}), tried multiple timestamp servers")
endif()

message(STATUS "Verifying signatures ...")
execute_process(
    COMMAND ${SIGNTOOL} verify /pa ${binariesPaths}
    COMMAND_ERROR_IS_FATAL ANY
)
message(STATUS "Verifying successful")
