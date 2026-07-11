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

// CraftUblkDisk: a ublkpp leaf disk (ublkpp::ublk_disk) that exposes a CRAFT volume as a ublk block
// device (/dev/ublkbN). It drives a craft_client (login / dLSN-stamped broadcast writes / horizon reads) that sits over
// one-or-more replica handles.
//
// THREADING. Everything the client does on the submit path happens on the ublk queue thread that issued the IO:
// async_iov starts the CRAFT op inline, so the dLSN reservation, the broadcast and the transport's serialize all
// run there, exactly as a real stub would. Only the reply crosses a thread, and it comes back through an fd this
// queue already polls (queue_service below) rather than through a foreign reactor. There is no iomgr here: ublkpp
// runs one OS thread per hardware queue over that queue's own io_uring, and that is the only runtime we need.
//
// This first cut targets an in-memory replica: correctness/crash validation of the I/O path. Zero
// writes (DISCARD / WRITE_ZEROES) are deferred; the op is rejected in async_iov

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <ublkpp/lib/cqe_state.hpp> // ublkpp::cqe_state (the per-queue POLL_ADD awaitable)
#include <ublkpp/lib/ublk_disk.hpp>

#include <homeblks/home_blocks.hpp>

struct iovec;
struct ublk_io_data;
struct ublksrv_queue;

namespace homeblocks::craft {
class craft_client;
}

namespace homeblocks::ublk {

// One landing pad per ublk tag. A tag is "unique in queue wide" (ublksrv.h) and has at most one IO -- hence at
// most one completion -- in flight, which is the same invariant that lets ublkpp pre-reserve async_io::_pool
// per tag. So the tag space IS the queue: no ring, no MPSC, no allocation, no lock.
//
// `result` is a plain store published by the release on `state`; the consumer's acquire-exchange pairs with it.
struct completion_slot {
    std::atomic< ublkpp::cqe_state* > state{nullptr}; // non-null => a completion is waiting on this tag
    int result{0};
};

// Per-queue state. Built in prepare() on that queue's own thread and thereafter touched only by it, except for
// the slots, which any thread may fill.
//
// The service loop is a coroutine parked on an io_uring POLL_ADD over the QUEUE'S OWN ring, watching an
// eventfd. When a CRAFT completion lands on some foreign thread it publishes into its tag's slot and kicks the
// eventfd; run_queue_loop reaps the POLL_ADD CQE, resumes the loop on the queue thread, and the loop resumes
// the per-IO coroutine there. That is the only way a per-IO cqe_state may be resumed: craft_client migrates
// coroutines across threads on its own (dlsn_tracker::resolve -> gate_.signal() resumes a suspended reader
// INLINE on the resolving thread), so resuming a cqe_state wherever the client happened to finish would
// corrupt the queue.
//
// The eventfd is this model's stand-in for a socket. A real CRAFT transport polls its connection fd here
// instead and parses the reply on the queue thread; it will still index its landing pad by request id, which
// is what a tag is. The loop does not change.
struct queue_service {
    int evfd{-1};                   // eventfd(0, EFD_NONBLOCK)
    ublkpp::cqe_state poll_state{}; // stand-alone (_owner == nullptr), reused every iteration
    std::coroutine_handle<> service_handle{};

    std::unique_ptr< completion_slot[] > slots; // q_depth of them; never resized
    std::size_t nslots{0};

    queue_service() = default;
    queue_service(queue_service const&) = delete;
    queue_service& operator=(queue_service const&) = delete;
    ~queue_service();

    // Callable from ANY thread. Publishes into the tag's slot, THEN kicks the eventfd -- in that order, so a
    // post racing a drain is never lost (see the drain in the service loop).
    void post(int tag, ublkpp::cqe_state* st, int result);
};

class CraftUblkDisk : public ublkpp::ublk_disk {
public:
    // `client` is the logged-in CRAFT client (already through login, so term/lba_size are known); it owns
    // the replica handle(s). Geometry (capacity/page_size) is supplied explicitly because the public API
    // exposes no way to introspect a volume_handle. `max_tx` is the largest single transfer to allow.
    CraftUblkDisk(std::shared_ptr< craft::craft_client > client, volume_id_t vol_id, uint64_t capacity,
                  uint32_t page_size, uint32_t max_tx);
    ~CraftUblkDisk() override;

    std::string id() const noexcept override { return _id_str; }

    prepare_result prepare(ublksrv_queue const* q, int const iouring_device_start) override;
    ublkpp::disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs,
                                       uint32_t nr_vecs, uint64_t addr) override;

    // ublkpp calls this on the queue thread when the queue is idle. With no IO to piggyback the commit
    // watermark on, we drive a keep_alive at every leg (one outstanding each) so the session does not expire --
    // the client's stand-in for a keep_alive timer. Fire-and-forget, so it never blocks the queue.
    void probe_tick(ublksrv_queue const* q) noexcept override;

private:
    std::shared_ptr< craft::craft_client > _client;
    volume_id_t _vol_id;
    uint64_t _capacity;
    uint32_t _page_size;
    std::string _id_str;

    // Sized to MAX_NR_HW_QUEUES at construction and never resized, so `q->q_id` indexes it lock-free and each
    // slot is owned by exactly one queue thread. Destroyed with the disk, which ublkpp drops only after every
    // queue thread has joined and every queue ring has been deinit'd.
    std::vector< std::unique_ptr< queue_service > > _services;
};

} // namespace homeblocks::ublk
