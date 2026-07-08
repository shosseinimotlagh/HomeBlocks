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
#include "craft_client.hpp"

#include <utility>

#include <sisl/async/when_all.hpp>

namespace homeblocks::craft {

craft_client::craft_client(std::vector< volume_handle > replicas, uint32_t leader) :
        replicas_(std::move(replicas)), leader_(leader) {}

client_hdr craft_client::make_hdr() const {
    // Every IO piggybacks the commit frontier the client has established so far (CRAFT has no standalone
    // commit verb). all_committed_lsn == our frontier too: at n=1 we are the whole set.
    int64_t const c = commit_.load(std::memory_order_acquire);
    return client_hdr{term_, c, c};
}

void craft_client::on_acked(int64_t dlsn) {
    // Read horizon: the highest dLSN that has reached quorum (atomic max). A read at this horizon sees
    // every write that has acked, so read-after-write holds (the depended-on write acked before the read).
    int64_t prev = highest_acked_.load(std::memory_order_relaxed);
    while (dlsn > prev && !highest_acked_.compare_exchange_weak(prev, dlsn, std::memory_order_relaxed)) {}

    // Contiguous commit frontier: fold this dLSN in and advance across any now-contiguous acked run.
    // (Concurrent IOs across ublk queues can ack out of order; the pending set bridges the gaps.)
    std::lock_guard< std::mutex > g{mu_};
    acked_pending_.insert(dlsn);
    while (!acked_pending_.empty() && *acked_pending_.begin() == frontier_ + 1) {
        frontier_ = *acked_pending_.begin();
        acked_pending_.erase(acked_pending_.begin());
    }
    commit_.store(frontier_, std::memory_order_release);
}

async_status craft_client::login(uint64_t client_token) {
    auto lr = co_await homeblocks::login(replicas_[leader_], client_token);
    if (!lr.has_value()) co_return std::unexpected(lr.error());
    if (lr->term == 0) {
        // Follower redirect. At n=1 the single replica is always the leader, so this should not happen;
        // treat it as an error rather than silently spinning. (N: retry against lr->leader_hint.)
        co_return std::unexpected(make_error_condition(craft_error::NOT_LEADER));
    }
    term_ = lr->term;
    lba_size_ = lr->lba_size;
    next_dlsn_.store(lr->dLSN + 1, std::memory_order_relaxed);
    co_return ok();
}

async_result< size_t > craft_client::write(uint64_t addr, uint64_t len, sisl::sg_list data) {
    int64_t const dlsn = next_dlsn_.fetch_add(1, std::memory_order_relaxed);
    client_hdr const hdr = make_hdr();

    // Broadcast to every replica in parallel at the client-assigned dLSN. Each task gets its own sg_list
    // descriptor copy (all copies point at the same caller-owned source buffer, which the caller keeps
    // alive across the co_await). The public task is lazy; when_all starts them all together.
    std::vector< async_status > futs;
    futs.reserve(replicas_.size());
    for (auto& h : replicas_) {
        futs.push_back(homeblocks::async_write(h, hdr, dlsn, addr, len, data));
    }
    auto const results = co_await sisl::async::when_all(std::move(futs));

    std::size_t acks = 0;
    std::error_condition last_err{};
    for (auto const& r : results) {
        if (r.has_value()) {
            ++acks;
        } else {
            last_err = r.error();
        }
    }
    if (acks < quorum()) {
        co_return std::unexpected(last_err ? last_err : make_error_condition(craft_error::NO_QUORUM));
    }
    on_acked(dlsn);
    co_return len;
}

async_result< size_t > craft_client::read(uint64_t addr, uint64_t len, sisl::sg_list dest) {
    int64_t const H = highest_acked_.load(std::memory_order_acquire);
    // n=1: the single replica holds everything, so it is always eligible. (N: route to an eligible holder
    // using the per-replica Missing map -- future work.)
    auto layout = co_await homeblocks::async_read(replicas_[leader_], make_hdr(), H, addr, len, std::move(dest));
    if (!layout.has_value()) co_return std::unexpected(layout.error());
    // `dest` has been filled in place (data extents + zero-filled holes); ublk only needs the byte count.
    co_return len;
}

async_status craft_client::flush() {
    auto r = co_await homeblocks::keep_alive(replicas_[leader_], make_hdr());
    if (!r.has_value()) co_return std::unexpected(r.error());
    co_return ok();
}

} // namespace homeblocks::craft
