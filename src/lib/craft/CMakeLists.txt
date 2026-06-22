cmake_minimum_required (VERSION 3.11)

add_library(${PROJECT_NAME}_craft OBJECT)
target_sources(${PROJECT_NAME}_craft PRIVATE
    craft_repl_dev.cpp
)
target_link_libraries(${PROJECT_NAME}_craft
    ${COMMON_DEPS}
)