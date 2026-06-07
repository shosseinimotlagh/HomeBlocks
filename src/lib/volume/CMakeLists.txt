cmake_minimum_required (VERSION 3.11)

add_flags("-Wno-unused-parameter")

add_library("${PROJECT_NAME}")

target_sources("${PROJECT_NAME}" PRIVATE
    volume.cpp
    volume_chunk_selector.cpp
    $<TARGET_OBJECTS:${PROJECT_NAME}_core>
)

target_link_libraries("${PROJECT_NAME}" PUBLIC
    homestore::homestore
    ${COMMON_DEPS}
)

add_subdirectory(tests)
