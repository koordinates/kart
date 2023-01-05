# -----------------------------------------------------------------------------
# Debugging helpers
# -----------------------------------------------------------------------------

#
# Print all properties of target
# https://stackoverflow.com/questions/32183975/how-to-print-all-the-properties-of-a-target-in-cmake/56738858#56738858
# https://stackoverflow.com/a/56738858/3743145 Get all properties that cmake supports
execute_process(COMMAND cmake --help-property-list OUTPUT_VARIABLE CMAKE_PROPERTY_LIST)
# Convert command output into a CMake list
string(REGEX REPLACE ";" "\\\\;" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")
string(REGEX REPLACE "\n" ";" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")
list(REMOVE_DUPLICATES CMAKE_PROPERTY_LIST)

# Print all properties of target
function(print_target_properties tgt)
  if(NOT TARGET ${tgt})
    message("There is no target named '${tgt}'")
    return()
  endif()

  foreach(prop ${CMAKE_PROPERTY_LIST})
    string(REPLACE "<CONFIG>" "${CMAKE_BUILD_TYPE}" prop ${prop})
    get_target_property(propval ${tgt} ${prop})
    if(propval)
      message(STATUS "${tgt} ${prop} = ${propval}")
    endif()
  endforeach(prop)
endfunction(print_target_properties)

#
# Print all variables with PREFIX
function(print_vars_prefix _prefix)
  get_cmake_property(_vars VARIABLES)
  string(REGEX MATCHALL "(^|;)${_prefix}[A-Za-z0-9_]*" _matchedVars "${_vars}")
  foreach(_var IN LISTS _matchedVars)
    message(STATUS "${_var}=${${_var}}")
  endforeach()
endfunction()
