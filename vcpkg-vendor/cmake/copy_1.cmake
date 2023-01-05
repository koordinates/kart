# Resolve each glob in a list to a single file and copy it to the destination

if(NOT DEFINED SOURCES OR NOT DEFINED DEST)
  message(FATAL_ERROR "usage: cmake -DSOURCES=<list-of-globs> -DDEST=<dest> -P copy_1.cmake")
endif()

foreach(srcGlob IN LISTS SOURCES)
  file(
    GLOB matches
    LIST_DIRECTORIES false
    "${srcGlob}")

  list(LENGTH matches matchCount)
  if(matchCount EQUAL 1)
    message(STATUS "${matches} -> ${DEST}")
  else()
    list(APPEND err " Expected 1 match for '${srcGlob}', found ${matchCount}:" ${matches})
    list(JOIN err "\n " err)
    message(FATAL_ERROR "${err}")
  endif()

  list(APPEND copyPaths "${matches}")
endforeach()

file(COPY ${copyPaths} DESTINATION ${DEST})
