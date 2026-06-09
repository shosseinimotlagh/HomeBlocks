cmake_minimum_required(VERSION 3.11)
project (homeblocks LANGUAGES CXX)
enable_testing()

set(CMAKE_CXX_STANDARD 23)

if ((DEFINED BUILD_COVERAGE) AND (${BUILD_COVERAGE}))
  enable_testing()
  include (cmake/coverage.cmake)
  APPEND_COVERAGE_COMPILER_FLAGS()
  SETUP_TARGET_FOR_COVERAGE_GCOVR_XML(NAME coverage EXECUTABLE ctest DEPENDENCIES )
elseif ((DEFINED THREAD_SANITIZER_ON) AND (${THREAD_SANITIZER_ON}))
  set(SANITIZER_TYPE "thread")
  include (cmake/sanitize.cmake)
elseif ((DEFINED ADDRESS_SANITIZER_ON) AND (${ADDRESS_SANITIZER_ON}))
  set(SANITIZER_TYPE "address")
  include (cmake/sanitize.cmake)
endif()
include (cmake/Flags.cmake)

find_package(GTest QUIET REQUIRED)
if (CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif ()

# set btree type flag
if ((DEFINED USE_FIXED_INDEX) AND (${USE_FIXED_INDEX}))
  message(STATUS "Using fixed btree")
  add_definitions(-DUSE_FIXED_INDEX=1)
else()
  message(STATUS "Using prefix btree")
  add_definitions(-DUSE_FIXED_INDEX=0)
endif()


# add component version information
add_flags("-DPACKAGE_NAME=${PROJECT_NAME}")
add_flags("-DPACKAGE_VERSION=${PROJECT_VERSION}")

add_subdirectory(src)
