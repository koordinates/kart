# In-place file regex string replacement

if(NOT DEFINED FILE
   OR NOT DEFINED MATCH
   OR NOT DEFINED REPLACE)
  message(
    FATAL_ERROR
      "usage: cmake -DMATCH=<regex> -DREPLACE=<replace> -DFILE=<file> -P str_replace.cmake")
endif()

file(STRINGS "${FILE}" lines)

foreach(lineIn IN LISTS lines)
  string(REGEX REPLACE "${MATCH}" "${REPLACE}" lineOut "${lineIn}")
  string(APPEND linesOut "${lineOut}\n")
endforeach()

file(WRITE "${FILE}" ${linesOut})
