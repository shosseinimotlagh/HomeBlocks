cmake_minimum_required (VERSION 3.11)

# Test-local, non-shipped code, grouped so each piece can be dropped as a unit:
#   craft/       -- the CRAFT reference model + reference client + CRAFT ublk driver + their tests.
#                   (model = throwaway once production CRAFT lands; client/driver graduate to src/lib.)
#   legacy_ublk/ -- the original ublk driver over the deprecated volume block API (drop with that API).
#
# ublkpp (a test_requires) backs BOTH ublk drivers; detect it once here and let the subdirs gate their
# ublk targets on UblkPP_FOUND (inherited into the child scopes). The CRAFT model + client + their unit
# tests need no ublk device, so craft/ is always added; only the ublk driver targets are gated.
find_package(UblkPP QUIET)
if (NOT UblkPP_FOUND)
    message(STATUS "ublkpp not found; the ublk drivers (craft_ublk / homeblk_ublk) will be skipped")
endif()

add_subdirectory(craft)

if (UblkPP_FOUND)
    add_subdirectory(legacy_ublk)
endif()
