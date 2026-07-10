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

// craft_client: a reference CRAFT client over the PUBLIC volume_handle API -- assign a per-partition dLSN
// to each write, broadcast it, tally quorum, advance the commit frontier, pick a safe horizon per read.
//
// It calls ONLY the public free functions (login / async_write / async_read / keep_alive) against
// volume_handles, never the model internals, so swapping in a network shim is the only change needed.
// All dLSN bookkeeping lives in dlsn_tracker. See README.md.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <system_error>
#include <vector>

#include <homeblks/home_blocks.hpp>

#include "dlsn_tracker.hpp"
#include "read_route_map.hpp"

namespace homeblocks::craft {

class craft_client {
public:
    // One volume_handle per replica device. `leader` is where login is attempted first. `max_inflight` is
    // the caller's IO concurrency bound; it only sizes the tracker's winner-scan tripwire.
    explicit craft_client(std::vector< volume_handle > replicas, uint32_t leader = 0, uint32_t max_inflight = 128);

    // Establish the session, following NOT_LEADER redirects.
    async_status login(uint64_t client_token);

    // Broadcast a write at a fresh dLSN; commit advances once quorum acks. Empty `data` is a zero write.
    // A sub-quorum write leaves its slot unresolved, which pins the commit frontier -- as CRAFT requires.
    async_result< size_t > write(uint64_t addr, uint64_t len, sisl::sg_list data);

    // Fills `dest` in place (data bytes; holes -> zeros) and resolves to the byte count. Splits into
    // parallel sub-reads when blocks in the range need different horizons.
    async_result< size_t > read(uint64_t addr, uint64_t len, sisl::sg_list dest);

    // The dedicated commit carrier: advance the leader's frontier + reset its watchdog (keep_alive).
    async_status flush();

    uint32_t lba_size() const { return lba_size_; }
    uint64_t term() const { return term_; }
    int64_t commit_lsn() const { return tracker_.frontier(); }
    int64_t read_horizon() const { return tracker_.read_horizon(); }
    // Test hook: how many reads paid for the per-block winner pass.
    uint64_t winner_scans() const { return tracker_.winner_scans(); }
    // Test/observability hooks into the read-routing map: the folded frontier, and whether a member is caught
    // up to it (a behind member is routed around for <= folded reads).
    int64_t route_folded() const { return route_->folded(); }
    bool route_caught_up(std::size_t idx) const { return route_->caught_up(idx); }

    // Observability: the client's dLSN watermarks + the unresolved slots pinning the frontier. Safe to call
    // from any thread while IO is in flight. (Named dlsn_stats, not tracker_stats, so the member does not
    // shadow the returned type inside the class scope.)
    tracker_stats dlsn_stats(std::size_t sample_limit = 16) const { return tracker_.stats(sample_limit); }
    std::size_t replica_count() const { return replicas_.size(); }
    uint32_t leader_index() const { return leader_; }

private:
    // The set-wide min commit_lsn floors journal reclaim on every replica. Our own frontier is an UPPER
    // bound on it, never the min, so sending that would let a replica reclaim journal a lagging peer still
    // needs. -1 (unknown) until a broadcast keep_alive can compute the real minimum.
    static constexpr int64_t k_all_committed_unknown = -1;

    client_hdr make_hdr() const;
    std::size_t quorum() const { return replicas_.size() / 2 + 1; }
    // The two guards every IO opens with; nullopt means the IO may proceed.
    std::optional< std::error_condition > precheck(uint64_t addr, uint64_t len) const;

    // Issue a whole read plan to one target, filling `dest` in place. Factored out of read() so the router
    // can retry it against the next eligible member on a transport failure. `dest` is copied per attempt
    // (descriptors only; both point at the caller's buffer), so a failover re-fills the same buffer.
    async_result< size_t > issue_plan(volume_handle const& target, client_hdr hdr, read_plan const& plan, uint64_t addr,
                                      uint64_t len, sisl::sg_list& dest);

    std::vector< volume_handle > replicas_;
    uint32_t leader_{0};
    uint64_t term_{0};
    uint32_t lba_size_{0};

    dlsn_tracker tracker_;
    // shared_ptr, not a plain member: a detached when_quorum straggler's completion hook records into this
    // map and may finish after the client is destroyed. The hook captures a copy, so a late completion
    // writes into a still-alive (orphaned) map rather than a freed one -- the same discipline the transport
    // uses for MemCraftReplica.
    std::shared_ptr< read_route_map > route_{std::make_shared< read_route_map >()};
};

} // namespace homeblocks::craft
