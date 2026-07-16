cmake_minimum_required(VERSION 3.11)

# Unit test for CraftReplDev::truncate (S4). Compiles craft_repl_dev.cpp directly to avoid
# dragging in craft_api.cpp → volume.hpp (heavy HomeStore volume plumbing not needed here).
add_executable(test_craft_truncate)
target_sources(test_craft_truncate PRIVATE
    test_craft_truncate.cpp
    ../craft_repl_dev.cpp
)
target_compile_definitions(test_craft_truncate PRIVATE _PRERELEASE)
target_link_libraries(test_craft_truncate
    ${COMMON_TEST_DEPS}
    -rdynamic
)

add_test(NAME CraftTruncateTest COMMAND test_craft_truncate)
