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
#include "craft_ublk_disk.hpp"

#include <algorithm>
#include <bit>
#include <cerrno>
#include <stdexcept>
#include <utility>

#include <sisl/logging/logging.h>
#include <ublksrv.h>

#include <ublkpp/lib/cqe_state.hpp> // build_cqe_state_data / next_sqe + the managed-user-data helpers

extern "C" {
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>
}

#include "craft_client.hpp"
#include "coro_helpers.hpp" // homeblocks::detail::detach (src/lib, on the include path)

namespace homeblocks::ublk {

// ublk speaks in 512-byte sectors regardless of the device's logical block size; ublkpp keeps this constant in an
// un-installed internal header, so we redefine the one value we need.
static constexpr uint32_t k_sector_shift = 9;

// ── the per-queue service ──

queue_service::~queue_service() {
    // Safe in this order because ublkpp joins every queue thread and runs ublksrv_queue_deinit (tearing down the
    // queue's io_uring, which kernel-cancels our in-flight POLL_ADD) BEFORE it drops the disk that owns us.
    if (service_handle) service_handle.destroy();
    if (evfd >= 0) close(evfd);
}

void queue_service::post(int tag, ublkpp::cqe_state* st, int result) {
    auto& s = slots[static_cast< std::size_t >(tag)];
    s.result = result; // plain; published by the release store below
    s.state.store(st, std::memory_order_release);

    // Kick AFTER publishing. The drain reads the eventfd before it scans, so a post landing after that read has
    // already published (the scan sees it), and one landing after the scan writes the level-triggered eventfd
    // after the read -- either way the next POLL_ADD fires. Kicking first would lose the wakeup.
    uint64_t const one = 1;
    if (auto const r = ::write(evfd, &one, sizeof(one)); r != sizeof(one)) {
        LOGERROR("craft_ublk_disk: eventfd kick failed ({}); IO may stall", r);
    }
}

// Never returns. Parks on POLL_ADD over the QUEUE'S OWN ring, so run_queue_loop reaps the CQE (managed bit set,
// non-null cqe_state*) and resumes us right here on the queue thread. `_owner` stays null: this is the
// stand-alone form from cqe_state.hpp, so a throw out of our resume is logged by the loop rather than completing
// somebody's ublk tag.
static ublkpp::disk_task< int > craft_service_loop(ublksrv_queue const* q, queue_service* qs) {
    for (;;) {
        qs->poll_state._result = 0;
        qs->poll_state._result_ready = false;
        qs->poll_state._waiter = {};

        auto* sqe = ublkpp::next_sqe(q);
        if (!sqe) [[unlikely]] {
            // Transient SQ pressure. The queue loop submits on its next pass; re-arm rather than drop the poll,
            // which would strand every pending completion on this queue.
            LOGWARN("craft_ublk_disk: no SQE for the completion poll; retrying");
            continue;
        }
        io_uring_prep_poll_add(sqe, qs->evfd, POLLIN);
        sqe->user_data = sisl::async::encode_managed_user_data(&qs->poll_state);

        (void)co_await qs->poll_state;

        // Drain the eventfd FIRST, then scan. See queue_service::post for why the order matters.
        uint64_t v{0};
        if (auto const r = ::read(qs->evfd, &v, sizeof(v)); r != sizeof(v)) { LOGTRACE("eventfd read returned {}", r); }

        // A relaxed load filters the (usually empty) slots without an RMW; only a live slot pays the
        // acquire-exchange, which pairs with post()'s release and makes `result` visible. Single consumer, so
        // nothing can steal a slot between the two. One wake drains a whole batch.
        for (std::size_t i = 0; i < qs->nslots; ++i) {
            auto& s = qs->slots[i];
            if (s.state.load(std::memory_order_relaxed) == nullptr) continue;
            auto* st = s.state.exchange(nullptr, std::memory_order_acquire);
            if (!st) continue;
            st->_result = s.result;
            st->_result_ready = true;
            if (auto h = std::exchange(st->_waiter, {})) h.resume(); // per-IO coroutine, on this queue thread
        }
    }
}

// The per-IO worker. Started INLINE on the queue thread, so it runs to its first suspension there -- which is
// exactly where a real stub serializes the request and hands it to the wire. It resumes wherever the CRAFT
// completion lands (a replica's server thread), and from there it must NOT resume the ublk coroutine directly:
// it posts back to the owning queue. Parameters are by value so they live in the coroutine frame.
static async_status run_craft_io(std::shared_ptr< craft::craft_client > client, bool is_read, uint64_t addr,
                                 uint64_t len, sisl::sg_list sgs, queue_service* qs, ublkpp::cqe_state* state,
                                 int tag) {
    auto const res =
        is_read ? co_await client->read(addr, len, std::move(sgs)) : co_await client->write(addr, len, std::move(sgs));
    int result;
    if (res.has_value()) {
        result = static_cast< int >(res.value());
    } else if (res.error() == std::make_error_condition(std::errc::invalid_argument)) {
        result = -EINVAL; // misaligned addr/len
    } else {
        result = -EIO; // term fenced / no quorum / replica down / etc.
    }

    // We are on an arbitrary thread. craft_client migrates coroutines across threads by itself (a read suspended
    // on dlsn_tracker's resolution_gate is resumed inline by whichever thread later resolves a slot), so the
    // queue's coroutine may only be resumed by the queue. Hand it over via this tag's landing pad.
    qs->post(tag, state, result);
    co_return ok();
}

CraftUblkDisk::CraftUblkDisk(std::shared_ptr< craft::craft_client > client, volume_id_t vol_id, uint64_t capacity,
                             uint32_t page_size, uint32_t max_tx) :
        ublkpp::ublk_disk(),
        _client(std::move(client)),
        _vol_id(vol_id),
        _capacity(capacity),
        _page_size(page_size),
        _id_str(boost::uuids::to_string(vol_id)),
        _services(MAX_NR_HW_QUEUES) {
    if (!_client) throw std::runtime_error("CraftUblkDisk: null craft_client");
    if (page_size == 0 || (page_size & (page_size - 1)) != 0)
        throw std::runtime_error("CraftUblkDisk: page_size must be a power of two");
    if (max_tx < (1u << k_sector_shift)) throw std::runtime_error("CraftUblkDisk: max_tx too small");

    // CRAFT reads/writes through its own datapath, never the kernel page cache.
    _direct_io = true;

    auto const bs_shift = static_cast< uint8_t >(std::countr_zero(page_size));
    auto& p = *params();
    p.basic.logical_bs_shift = bs_shift;
    p.basic.physical_bs_shift = bs_shift;
    // Leave max_sectors at the base default (DEF_BUF_SIZE >> 9), which tracks the ublk per-tag IO buffer
    // (ublkpp's --max_io_size, default 512 KiB): a basic.max_sectors larger than that buffer is rejected by the
    // kernel with EINVAL. Only clamp it DOWN if our own max transfer (max_tx) is smaller, so we never hand the
    // client an IO bigger than we allow.
    p.basic.max_sectors = std::min(p.basic.max_sectors, static_cast< uint32_t >(max_tx >> k_sector_shift));
    p.basic.dev_sectors = capacity >> k_sector_shift;
    // The kernel requires the device size to be a whole multiple of max_sectors.
    p.basic.dev_sectors -= (p.basic.dev_sectors % p.basic.max_sectors);

    // DISCARD/WRITE_ZEROES map cleanly onto a CRAFT zero write (empty sg_list), but enabling them means getting
    // the ublk_param_discard geometry exactly right or the device fails to come up; left disabled for this first
    // cut (the op is also rejected defensively in async_iov). TODO: thin zero-write mapping.
    p.types &= ~UBLK_PARAM_TYPE_DISCARD;

    LOGINFO("CraftUblkDisk [vol={}] sectors={} lbs={} max_sectors={} term={}", _id_str, p.basic.dev_sectors, page_size,
            p.basic.max_sectors, _client->term());
}

CraftUblkDisk::~CraftUblkDisk() = default;

CraftUblkDisk::prepare_result CraftUblkDisk::prepare(ublksrv_queue const* q, int const) {
    // One cqe_state per IO (the per-tag one build_cqe_state_data allocates). It MUST be >= 1: init_queue does
    // _pool.reserve(max_sqes_per_io) and next_state() opens with RELEASE_ASSERT_LT(size, capacity), so a 0 here
    // aborts on the first IO even though we submit no SQE of our own per IO.
    constexpr prepare_result k_result{.fds = {}, .max_sqes_per_io = 1};

    // init_tgt calls prepare(nullptr, 0) on the setup thread, before any queue exists, purely to size the ring.
    // Everything below needs a real queue.
    if (!q) return k_result;

    auto qs = std::make_unique< queue_service >();
    qs->evfd = ::eventfd(0, EFD_NONBLOCK);
    if (qs->evfd < 0) throw std::runtime_error("CraftUblkDisk: eventfd() failed");

    // One landing pad per tag: a tag holds at most one in-flight IO, so a completion never collides.
    if (q->q_depth <= 0) throw std::runtime_error("CraftUblkDisk: non-positive q_depth");
    qs->nslots = static_cast< std::size_t >(q->q_depth);
    qs->slots = std::make_unique< completion_slot[] >(qs->nslots);

    // The loop is lazy. Steal its frame out of the disk_task (so the local's destructor does not free it), then
    // resume once: it runs to its first co_await, staging the POLL_ADD into this queue's ring.
    auto task = craft_service_loop(q, qs.get());
    qs->service_handle = std::exchange(task._coro, {});
    qs->service_handle.resume();

    if (q->q_id < 0 || static_cast< std::size_t >(q->q_id) >= _services.size())
        throw std::runtime_error(fmt::format("CraftUblkDisk: q_id {} out of range", q->q_id));
    _services[q->q_id] = std::move(qs);
    return k_result;
}

ublkpp::disk_task< int > CraftUblkDisk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs,
                                                  uint32_t nr_vecs, uint64_t addr) {
    auto const op = ublksrv_get_op(data->iod);
    if (op == UBLK_IO_OP_FLUSH) co_return 0; // CRAFT IO is durable once acked (quorum-appended); nothing to flush
    if (op == UBLK_IO_OP_DISCARD || op == UBLK_IO_OP_WRITE_ZEROES) co_return -ENOTSUP; // TODO: CRAFT zero write

    bool const is_read = (op == UBLK_IO_OP_READ);

    // Copy the iovec descriptors into an sg_list (moved into run_craft_io's frame) before the first co_await.
    // iov_base points into the kernel-mapped ublk IO buffer and stays valid until ublksrv_complete_io, so the
    // client reads/writes it in place -- only the descriptors are copied, not the data.
    sisl::sg_list sgs;
    sgs.size = 0;
    for (uint32_t i = 0; i < nr_vecs; ++i) {
        sgs.iovs.push_back(iovecs[i]);
        sgs.size += iovecs[i].iov_len;
    }
    uint64_t const len = sgs.size;

    // Per-IO cqe_state, in the tag's pre-reserved pool so the pointer is stable. Nothing resumes it via a CQE:
    // run_craft_io publishes into this tag's landing pad, and this queue's service resumes it on this thread.
    auto* const state = ublkpp::build_cqe_state_data(data).first;

    // Populated by prepare() on this same thread; no lock, no cross-thread read.
    auto* const qs = _services[q->q_id].get();
    if (!qs) [[unlikely]]
        co_return -EIO;

    // Read the tag before the first co_await: `data` belongs to the queue and we must not touch it once the
    // client has us suspended on some other thread. A tag is unique queue-wide and holds one IO at a time, so it
    // indexes a landing pad that cannot collide.
    int const tag = data->tag;
    if (tag < 0 || static_cast< std::size_t >(tag) >= qs->nslots) [[unlikely]]
        co_return -EIO;

    // Start the CRAFT op INLINE, right here on the queue thread: detach() injects an inline_scheduler, so the
    // coroutine runs to its first suspension before returning. That is where the client reserves its dLSN,
    // fans out, and the transport serializes the payload -- all the work a real stub does on the submitting
    // thread. Only the reply crosses threads.
    detail::detach(run_craft_io(_client, is_read, addr, len, std::move(sgs), qs, state, tag));

    co_return co_await *state;
}

} // namespace homeblocks::ublk
