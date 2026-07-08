/*********************************************************************************
 * Modifications Copyright 2026 eBay Inc.
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

// craft_client: a minimal reference CRAFT client over the PUBLIC volume_handle API. It owns the
// client-side CRAFT work -- assign a per-partition dLSN to each write, broadcast the write to every
// replica handle, tally quorum, advance the commit frontier, and route reads at a horizon. It is built
// for N replicas and exercised here at n=1 (a single in-memory replica), where quorum is 1 so every ack
// commits.
//
// It is deliberately transport-agnostic: it calls ONLY the public free functions (login / async_write /
// async_read / keep_alive) against volume_handles, never the model internals. That is the whole point --
// today the CLI hands it in-memory handles (make_memory_replica_set); the plan is to later hand it
// network-shim handles that marshal the same calls to a remote server. Swapping the handle is the only
// change; this client and the ublk driver above it do not move. So this is the real client, not throwaway.

#include <atomic>
#include <cstdint>
#include <mutex>
#include <set>
#include <vector>

#include <homeblks/home_blocks.hpp>

namespace homeblocks::craft {

class craft_client {
public:
    // One volume_handle per replica device (index `leader` is the login target / the only member at n=1).
    explicit craft_client(std::vector< volume_handle > replicas, uint32_t leader = 0);

    // Establish the session against the leader; stores term / lba_size and seeds the dLSN counter.
    // Resolves to an error if login fails or (unexpectedly at n=1) redirects to another leader.
    async_status login(uint64_t client_token);

    // Broadcast a write to all replicas at a fresh dLSN; commit advances once quorum acks. Empty `data`
    // (size 0) is a zero write. Resolves to the byte count written, or the first replica error / NO_QUORUM.
    async_result< size_t > write(uint64_t addr, uint64_t len, sisl::sg_list data);

    // Unicast read at horizon H = highest acked dLSN. Fills `dest` in place (data bytes; holes -> zeros).
    // Resolves to the byte count (the sparse layout is consumed here; the caller's buffer is fully filled).
    async_result< size_t > read(uint64_t addr, uint64_t len, sisl::sg_list dest);

    // The dedicated commit carrier: advance the leader's frontier + reset its watchdog (keep_alive).
    async_status flush();

    uint32_t lba_size() const { return lba_size_; }
    uint64_t term() const { return term_; }
    int64_t  commit_lsn() const { return commit_.load(std::memory_order_acquire); }
    int64_t  read_horizon() const { return highest_acked_.load(std::memory_order_acquire); }

private:
    client_hdr  make_hdr() const;   // {term_, commit_frontier, commit_frontier}
    void        on_acked(int64_t dlsn); // record a quorum-acked dLSN -> advance frontier + read horizon
    std::size_t quorum() const { return replicas_.size() / 2 + 1; }

    std::vector< volume_handle > replicas_;
    uint32_t                     leader_{0};
    uint64_t                     term_{0};
    uint32_t                     lba_size_{0};

    std::atomic< int64_t > next_dlsn_{0};      // next dLSN to assign (seeded from login dLSN + 1)
    std::atomic< int64_t > highest_acked_{-1}; // read horizon: highest quorum-acked dLSN
    std::atomic< int64_t > commit_{-1};        // contiguous committed frontier (lock-free mirror of frontier_)

    mutable std::mutex  mu_;
    std::set< int64_t > acked_pending_; // acked dLSNs not yet folded into the contiguous frontier
    int64_t             frontier_{-1};  // contiguous acked prefix (guarded by mu_)
};

} // namespace homeblocks::craft
