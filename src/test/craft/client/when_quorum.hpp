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

// when_quorum: fan out over a runtime vector of peers and resume as soon as `quorum` of them SUCCEED,
// leaving the stragglers running detached. The sibling of sisl::async::when_all (same start_detached +
// value_awaitable latch), differing only in when the latch fires.
//
// This is what lets a write acknowledge at quorum instead of at the slowest replica. It is the one clean
// data-path win CRAFT has over sync-to-all replication, and it is why a straggler must not cost every
// write the transport's timeout.
//
// THE PAYLOAD CONTRACT. Children keep running after we resume, so a child must not reference the caller's
// IO buffer past its first suspension point -- by then the caller may have recycled it. A real transport
// satisfies this for free: issuing the RPC serializes the request into the transport's own send buffer.
// Note the asymmetry that makes early return sound at all: the caller's buffer must survive every child's
// *send*, but we only wait for a quorum of *replies*. Every child runs synchronously to its first
// suspension inside the fan-out loop below, so all sends are away before we ever await the latch.
//
// THE RESULTS RACE, AND WHY IT IS NOT ONE. The latch fires on exactly two triggers:
//
//     (a) `quorum` children have succeeded  -- stragglers may still be writing into `results`
//     (b) every child has finished          -- nothing is writing into `results`
//
// So `results` is safe to read IF AND ONLY IF fewer than `quorum` acks came back: (a) cannot have fired,
// therefore (b) did. That is exactly the sub-quorum path, which is the only path that needs the
// per-replica errors (to tell a provably-absent-everywhere slot from an unresolved one). The quorum path
// reads nothing but the ack count. Do not read `results` without checking `acks < quorum` first.

#include <atomic>
#include <cstddef>
#include <memory>
#include <utility>
#include <variant>
#include <vector>

#include <exec/inline_scheduler.hpp>
#include <stdexec/execution.hpp>

#include <sisl/async/task.hpp>
#include <sisl/async/value_awaitable.hpp>

namespace homeblocks::craft {

namespace detail {

// Fires once, on whichever trigger lands first. `fired_` guards value_awaitable's exactly-once contract.
struct quorum_latch {
    std::atomic< std::size_t > remaining;
    std::atomic< std::size_t > acks{0};
    std::atomic< bool > fired{false};
    std::size_t const quorum;
    sisl::async::value_awaitable< std::monostate > done;

    quorum_latch(std::size_t n, std::size_t q) noexcept : remaining{n}, quorum{q} {}

    void arrive(bool acked) noexcept {
        // Order matters: publish the ack BEFORE the completion, so a waiter woken by the last child's
        // count_down observes every ack that preceded it.
        if (acked && (acks.fetch_add(1, std::memory_order_acq_rel) + 1 == quorum)) fire();
        if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) fire();
    }
    void fire() noexcept {
        if (!fired.exchange(true, std::memory_order_acq_rel)) done.complete({});
    }
};

// Awaits one child, records its result, and arrives at the latch. Swallows exceptions (a throwing child
// must not strand the latch), leaving that slot default-constructed and counted as a non-ack.
template < typename T >
sisl::async::task< void > quorum_run_one(sisl::async::task< T > child, std::shared_ptr< std::vector< T > > results,
                                         std::size_t index, std::shared_ptr< quorum_latch > latch) {
    bool acked = false;
    try {
        auto r = co_await std::move(child);
        acked = r.has_value();
        (*results)[index] = std::move(r);
    } catch (...) {
        // leave results[index] default-constructed; counted as a non-ack
    }
    latch->arrive(acked);
}

} // namespace detail

template < typename T >
struct quorum_result {
    std::size_t acks{0};
    // Readable ONLY when acks < quorum (see the header comment): then every child has finished.
    std::shared_ptr< std::vector< T > > results;
};

// Start every task concurrently; resume once `quorum` of them have completed with a value, or once all
// have finished, whichever comes first. Stragglers keep running detached and keep `results` alive.
template < typename T >
sisl::async::task< quorum_result< T > > when_quorum(std::vector< sisl::async::task< T > > tasks, std::size_t quorum) {
    auto const n = tasks.size();
    auto results = std::make_shared< std::vector< T > >(n);
    if (n == 0) co_return quorum_result< T >{0, std::move(results)};

    auto latch = std::make_shared< detail::quorum_latch >(n, quorum);
    for (std::size_t i = 0; i < n; ++i) {
        // Each child runs synchronously to its first suspension right here, which is where it consumes the
        // caller's payload. exec::task has sticky scheduler affinity, so inject inline_scheduler to let the
        // detached child resume on whatever thread completes it (identical to sisl::async::when_all).
        stdexec::start_detached(stdexec::write_env(detail::quorum_run_one< T >(std::move(tasks[i]), results, i, latch),
                                                   stdexec::prop{stdexec::get_scheduler, exec::inline_scheduler{}}));
    }
    co_await latch->done;
    co_return quorum_result< T >{latch->acks.load(std::memory_order_acquire), std::move(results)};
}

} // namespace homeblocks::craft
