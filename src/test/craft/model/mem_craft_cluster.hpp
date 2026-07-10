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

// MemTransport -- the in-process stand-in for the CRAFT network AND the (RAFT) cold path, for the
// in-memory reference model. It routes replica<->replica cold-path calls, holds the membership and a
// designated leader, and is the fault-injection surface for tests (set_up / force_subquorum). It does
// NOT own the replicas' lifetime: they are co-owned by their volume_handles (production shape) or by
// the MemReplicaGroup below (model unit tests); MemTransport keeps raw pointers registered at creation.
//
// Membership, replica count, leader, and fault injection are control-plane / orchestration concerns
// that sit OUTSIDE the CRAFT protocol -- here they are just wired up directly (a CLI flag / harness).

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include <boost/functional/hash.hpp>
#include <sisl/async/shared_awaitable.hpp>

#include "mem_craft_replica.hpp"

namespace homeblocks::craft {

using uuid_hash = boost::hash< peer_id_t >;

class MemTransport {
public:
    MemTransport(std::vector< replica_endpoint > members, uint32_t lba_size);
    ~MemTransport(); // stops the timer thread; pending late deliveries are dropped

    // Called by create_memory_volume / make_mem_replica_group as each replica is built.
    void register_replica(MemCraftReplica* r);

    // ── the wire: every client-facing op crosses it ──
    //
    // This is where the network's concerns live, and they ALL die with this class when a real transport
    // replaces it:
    //   * takes the payload at issue -- the stand-in for serializing into a send buffer. THE ONE COPY on
    //     the write path. Nothing above it (craft_client) or below it (do_write) copies bytes.
    //   * decides deliverability. craft_error::REPLICA_DOWN means "I never delivered this request", which
    //     is why the client may count it as a deterministic reject. A server cannot answer that about
    //     itself, so the transport answers it.
    //   * imposes latency (set_delay) and the deadline (set_op_timeout).
    //
    // What survives is the replica's do_write / do_read / do_keep_alive: the journal and the LBA index.
    async_status send_write(MemCraftReplica* to, client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                            sisl::sg_list data);
    async_result< std::vector< io_extent > > send_read(MemCraftReplica* to, client_hdr hdr, int64_t read_lsn,
                                                       uint64_t addr, uint64_t len, sisl::sg_list dest);
    async_result< LSNPair > send_keep_alive(MemCraftReplica* to, client_hdr hdr);

    // ── cold path: leader-only orchestration ──
    // login: GetRSCommitLSN -> SyncRSCommitLSN -> InternalLogin. A non-leader returns LoginResult with
    // term==0 and leader_hint set (not an error). NO_QUORUM / REPLICA_DOWN are errors.
    result< LoginResult > run_login(MemCraftReplica* caller, uint64_t client_token);

    // logout: InternalLogout applied to all live replicas. term-fenced. Returns NOT_LEADER if caller
    // is not the leader.
    status run_logout(MemCraftReplica* caller, uint64_t term);

    // ── membership / routing (consulted by the replicas' client-facing ops) ──
    peer_id_t leader() const;
    bool is_up(peer_id_t id) const;
    bool write_allowed(peer_id_t id) const; // false => this write is dropped (sub-quorum)
    std::size_t member_count() const { return members_.size(); }

    // ── fault injection (tests / CLI) ──
    void set_up(peer_id_t id, bool up);                  // bring a replica down / back up
    void force_subquorum(std::vector< peer_id_t > keep); // subsequent writes land ONLY on `keep`
    void clear_faults();                                 // clears down_, write_keep_ AND every delay
    void set_leader(peer_id_t id);

    // ── latency injection: a straggler replica ──
    //
    // `set_delay` adds artificial network latency to every client-facing op addressed to `id`. Combined
    // with `set_op_timeout` it is how a sub-quorum arises the way production will see one: the op takes
    // longer than the transport is willing to wait, the client gives up on that peer and commits at
    // quorum, and the peer STILL receives and applies the write once the delay elapses. A write that
    // lands after later writes already did leaves a Missing slot behind, which drains when it arrives.
    //
    // A CONSTANT delay only produces uniform lag: the straggler's journal fills in order, just late, so
    // missing_count stays 0. Changing the delay while writes are in flight is what reorders arrivals.
    void set_delay(peer_id_t id, std::chrono::milliseconds d); // 0 removes the delay
    std::chrono::milliseconds delay_for(peer_id_t id) const;

    // How long the transport waits before abandoning an op. 0 (the default) means wait forever, which is
    // the model's original behavior: without it a delay merely slows the client down.
    void set_op_timeout(std::chrono::milliseconds t);
    std::chrono::milliseconds op_timeout() const;

    // Where a completion runs. Unset (the default) means "on one of the service threads", which is the
    // adversarial model: an arbitrary thread, unrelated to the issuer. craft_ublk injects an executor that
    // dispatches onto an iomgr worker reactor so its post_msg_ring completion stays on a uring thread.
    // A real transport reaps its own CQEs, so this hook has no production analog; it exists so the model
    // can stay iomgr-free while the driver above it is not.
    using completion_executor = std::function< void(std::function< void() >) >;
    void set_completion_executor(completion_executor ex);

private:
    // What a suspended op resumes on. Single-shot and completed from a service thread; the awaiting
    // coroutine resumes there, which is why nothing here may hold mu_ across a co_await.
    using sleep_event = sisl::async::shared_awaitable< std::monostate >;

    // How this op should be delivered to `id`. `timed_out` means the caller gives up after `wait` while the
    // op still reaches the peer at `deliver` -- an RPC deadline, not a rollback.
    struct delivery {
        std::chrono::milliseconds wait{0};
        std::chrono::milliseconds deliver{0};
        bool timed_out{false};
    };
    delivery plan_delivery(peer_id_t id) const;

    // Suspend the caller for `d` and resume it on a service thread. `d == 0` still hops: a completion is
    // NEVER inline with its submit, because no real transport delivers one that way. Callers must therefore
    // treat every op as suspending.
    std::shared_ptr< sleep_event > after(std::chrono::milliseconds d);
    // Run `fn` on a service thread after `d`; false if the transport is shutting down and `fn` was dropped.
    // Used for the late delivery of a timed-out write. `fn` must NOT capture a shared_ptr to a replica:
    // replicas hold a shared_ptr to this transport, so that would close a reference cycle. Capture
    // weak_from_this() and lock it.
    bool run_after(std::chrono::milliseconds d, std::function< void() > fn);

    std::vector< MemCraftReplica* > live_replicas_locked() const; // requires mu_ held
    static std::size_t quorum(std::size_t n) { return n / 2 + 1; }

    void ensure_service(); // lazily spawn the service threads on first use; requires tmu_ held
    void service_loop();

    std::vector< replica_endpoint > members_;
    std::unordered_map< peer_id_t, MemCraftReplica*, uuid_hash > by_id_;
    peer_id_t leader_{};
    uint64_t term_{0};
    uint32_t lba_size_{0}; // volume block size (bytes)
    std::unordered_set< peer_id_t, uuid_hash > down_;
    std::optional< std::unordered_set< peer_id_t, uuid_hash > > write_keep_;
    std::unordered_map< peer_id_t, std::chrono::milliseconds, uuid_hash > delays_;
    std::chrono::milliseconds op_timeout_{0};
    mutable std::mutex mu_;

    // The service threads: this model's stand-in for a network completion path. Deadline-ordered queue, K
    // threads popping due items. K > 1 on purpose -- it is what makes replies to one broadcast land out of
    // order and on different threads, which no single-threaded shim would ever exercise.
    //
    // Its own lock, never taken while mu_ is held (and vice versa): a dispatched item re-enters the replica,
    // which re-enters mu_ via is_up() / write_allowed().
    static constexpr std::size_t k_service_threads = 2;
    using clock = std::chrono::steady_clock;
    std::multimap< clock::time_point, std::function< void() > > timers_;
    std::vector< std::thread > service_threads_;
    completion_executor exec_; // guarded by tmu_; set once before IO starts
    std::condition_variable tcv_;
    mutable std::mutex tmu_;
    bool service_stop_{false};
};

// An N-replica in-process group with NO volumes attached -- the model layer used directly by unit
// tests (and the core of make_memory_replica_set, which additionally wraps each replica in a
// volume_handle). HomeStore-free.
struct MemReplicaGroup {
    std::shared_ptr< MemTransport > net;
    std::vector< std::shared_ptr< MemCraftReplica > > replicas; // index 0 is the default leader
};
MemReplicaGroup make_mem_replica_group(volume_id_t vol_id, uint32_t n = 3, uint32_t page_size = 4096);

// Deterministic per-replica id derived from the volume id + index (so tests are reproducible).
peer_id_t mem_replica_id(volume_id_t vol_id, uint32_t index);

} // namespace homeblocks::craft
