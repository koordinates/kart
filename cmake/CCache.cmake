# CCache
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
  message(STATUS "Using ccache: ${CCACHE_PROGRAM}")
  set(ccacheEnv CCACHE_CPP2=true CCACHE_BASEDIR=${CMAKE_BINARY_DIR}
                CCACHE_SLOPPINESS=pch_defines,time_macros)
  foreach(lang IN ITEMS C CXX)
    set(CMAKE_${lang}_COMPILER_LAUNCHER ${CMAKE_COMMAND} -E env ${ccacheEnv} ${CCACHE_PROGRAM})
  endforeach()
endif()
