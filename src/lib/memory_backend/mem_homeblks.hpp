#pragma once

#include <atomic>
#include <utility>

#include "lib/homeblks_impl.hpp"

namespace homeblocks {

class MemoryHomeBlocks : public HomeBlocksImpl {
public:
    MemoryHomeBlocks(home_blocks_config&& cfg);
    ~MemoryHomeBlocks() override = default;
};

} // namespace homeblocks
