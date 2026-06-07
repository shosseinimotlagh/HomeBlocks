#include "mem_homeblks.hpp"

namespace homeblocks {

/// NOTE: We give ourselves the option to provide a different HR instance here than libhomeblocks.a
result< shared< home_blocks > > init_homeblocks(home_blocks_config cfg) {
    return shared< home_blocks >(std::make_shared< MemoryHomeBlocks >(std::move(cfg)));
}

MemoryHomeBlocks::MemoryHomeBlocks(home_blocks_config&& cfg) : HomeBlocksImpl::HomeBlocksImpl(std::move(cfg)) {}

} // namespace homeblocks
