cmake_minimum_required (VERSION 3.11)

add_library(${PROJECT_NAME}_craft OBJECT)
target_sources(${PROJECT_NAME}_craft PRIVATE
    craft_repl_dev.cpp
    craft_api.cpp
)
target_link_libraries(${PROJECT_NAME}_craft
    ${COMMON_DEPS}
)

# The in-memory CRAFT reference model is NOT part of the shipped library. It (and the reference client
# + ublk driver that drive it) live under src/test/craft, so the whole thing can be dropped when the
# production CRAFT path lands. Only the client-facing API impl above (craft_api.cpp) ships here.
