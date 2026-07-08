cmake_minimum_required (VERSION 3.11)

# The original ublk driver, built over the DEPRECATED volume block API (async_read/async_write on a
# single volume_handle, no CRAFT session). Kept to exercise the legacy/production HomeStore path; drop
# this whole directory when the deprecated volume API is removed.
#
# Relax pedantic/unused-parameter for the kernel UAPI headers (ublk_cmd.h, ublksrv.h) pulled in via
# ublkpp; keep -Werror/-Wall/-Wextra. Demote (not silence) the deprecation warning: homeblk_disk.cpp
# still calls the legacy async_read/async_write on purpose.
add_flags("-Wno-pedantic -Wno-unused-parameter -Wno-error=deprecated-declarations")

# Adapter: exposes a homeblocks volume as a ublkpp::ublk_disk (legacy byte-block API).
add_library(homeblk_ublk_disk OBJECT)
target_sources(homeblk_ublk_disk PRIVATE
    homeblk_disk.cpp
)
target_link_libraries(homeblk_ublk_disk
    ${COMMON_DEPS}
    UblkPP::UblkPP
)

# CLI: brings up homeblocks and exposes a volume as /dev/ublkbN (ublkpp_disk-style).
add_executable(homeblk_ublk)
target_sources(homeblk_ublk PRIVATE
    homeblk_ublk_cli.cpp
    $<TARGET_OBJECTS:homeblk_ublk_disk>
)
target_link_libraries(homeblk_ublk
    ${PROJECT_NAME}
    ${COMMON_TEST_DEPS}
    UblkPP::UblkPP
    -rdynamic
)
