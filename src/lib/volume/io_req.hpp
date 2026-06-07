/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
#pragma once

#include <cstdint>

#include <sisl/fds/utils.hpp> // sisl::Clock

#include "hb_internal.hpp"

namespace homeblocks {

using volume_info_ptr = shared< volume_info >;

// Plain by-value IO request. The free async_read/async_write functions build one on their coroutine frame and
// pass it by reference into volume::read/write -- the frame already provides storage that outlives the awaits,
// so (unlike the pre-coroutine design) there is no heap allocation or refcount here. The volume's in-flight
// count is tracked separately by a vol_io_guard (see volume.hpp).
struct io_req {
    uint8_t* buffer{nullptr};
    lba_t lba{0};
    lba_count_t nlbas{0};
    sisl::Clock::time_point io_start_time{};
    sisl::Clock::time_point data_svc_start_time{};
    sisl::Clock::time_point index_start_time{};
    sisl::Clock::time_point journal_start_time{};

    lba_t end_lba() const { return lba + nlbas - 1; }
};

} // namespace homeblocks
