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
// device (/dev/ublkbN). It is a fork of HomeBlkDisk onto the CRAFT client surface: instead of calling
// the (deprecated) byte-block async_read/async_write on a single volume_handle, it drives a craft_client
// (login / dLSN-stamped broadcast writes / horizon reads) that sits over one-or-more replica handles.
//
// The thread bridge is IDENTICAL to HomeBlkDisk and reused on purpose -- it is the execution model the
// real (network-shim-backed) driver needs, not a memory-only shortcut:
//   - ublkpp drives each ublk queue on its own pthread with a SINGLE_ISSUER io_uring; a driver's async_iov
//     coroutine runs there and its per-IO ublkpp::cqe_state must be resumed ON THE QUEUE THREAD.
//   - the CRAFT client op is launched ON a worker reactor (run_on_forget) so it runs where async storage /
//     (eventually) network completions live; when it completes the result is handed straight to the ublk
//     queue's io_uring via IORING_OP_MSG_RING (iomanager.post_msg_ring). The queue thread's run_queue_loop
//     reaps that CQE like a native completion and resumes the per-IO coroutine. No eventfd, no lock.
//
// This first cut targets a single in-memory replica: correctness/crash validation of the I/O path. Zero
// writes (DISCARD / WRITE_ZEROES) are deferred; the op is rejected in async_iov (as HomeBlkDisk does).

#include <cstdint>
#include <memory>
#include <string>

#include <ublkpp/lib/ublk_disk.hpp>

#include <homeblks/home_blocks.hpp>

struct iovec;
struct ublk_io_data;
struct ublksrv_queue;

namespace homeblocks::craft {
class craft_client;
}

namespace homeblocks::ublk {

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

private:
    std::shared_ptr< craft::craft_client > _client;
    volume_id_t _vol_id;
    uint64_t _capacity;
    uint32_t _page_size;
    std::string _id_str;
};

} // namespace homeblocks::ublk
