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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
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

// Every fault and latency knob, in one immutable snapshot. The IO path reads it with a single acquire load of
// a pointer and takes NO lock: is_up / write_allowed / delay_for / op_timeout are consulted several times per
// replica per IO, and routing all of that through one mutex made it the hottest lock in the model (fifteen
// acquisitions of a single global mutex per 4 KiB write to three replicas).
//
// Mutation is copy-on-write, serialized by MemTransport::fault_mu_ and confined to control paths: tests, the
// CLI, and the REST delay route. A superseded snapshot is RETIRED, never freed, so an op holding a pointer to
// it mid-flight can never dangle. Retention is O(number of fault changes), which is tiny by construction.
struct fault_state {
    std::unordered_set< peer_id_t, uuid_hash > down;
    std::optional< std::unordered_set< peer_id_t, uuid_hash > > write_keep;
    std::unordered_map< peer_id_t, std::chrono::milliseconds, uuid_hash > delays;
    std::chrono::milliseconds op_timeout{0};

    bool is_up(peer_id_t id) const { return !down.contains(id); }
    bool write_allowed(peer_id_t id) const {
        if (down.contains(id)) return false;
        if (write_keep && !write_keep->contains(id)) return false;
        return true;
    }
    std::chrono::milliseconds delay_for(peer_id_t id) const {
        auto const it = delays.find(id);
        return (it == delays.end()) ? std::chrono::milliseconds{0} : it->second;
    }
};

class MemTransport {
public:
    // `threads_per_replica` sizes EACH replica's server pool, not a shared one: three replicas at 2 threads
    // is six threads, three independent servers. See replica_service below.
    MemTransport(std::vector< replica_endpoint > members, uint32_t lba_size, std::size_t threads_per_replica = 2);
    ~MemTransport();

    // Join every replica's server pool and drain what is queued. Idempotent.
    //
    // MUST be called by an owner, on a thread that is NOT one of the pools -- MemReplicaGroup and
    // MemReplicaHandles do it in their destructors. It cannot be left to ~MemTransport: an in-flight op holds
    // a shared_ptr to its replica, which holds a shared_ptr to us, so the LAST reference is routinely dropped
    // by a straggler finishing on a server thread. ~MemTransport would then try to join the thread it is
    // running on ("Resource deadlock avoided"). Once shutdown() has run, ~MemTransport is a no-op wherever it
    // lands.
    void shutdown();

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
    //
    // `to` is a shared_ptr, by value, so it lives in the op's coroutine frame for the op's whole life. It has
    // to: the client acks at QUORUM and leaves stragglers in flight, and one of those may still be applying a
    // write long after its replica's last other owner is gone. An in-flight request keeps its server alive.
    // (No cycle: the frame is transient. The transport itself never holds a strong ref to a replica -- see the
    // weak_from_this() in send_write's late-delivery closure.)
    async_status send_write(std::shared_ptr< MemCraftReplica > to, client_hdr hdr, int64_t dlsn, uint64_t addr,
                            uint64_t len, sisl::sg_list data);
    async_result< std::vector< io_extent > > send_read(std::shared_ptr< MemCraftReplica > to, client_hdr hdr,
                                                       int64_t read_lsn, uint64_t addr, uint64_t len,
                                                       sisl::sg_list dest);
    async_result< LSNPair > send_keep_alive(std::shared_ptr< MemCraftReplica > to, client_hdr hdr);

    // ── cold path: leader-only orchestration ──
    // login: GetRSCommitLSN -> SyncRSCommitLSN -> InternalLogin. A non-leader returns LoginResult with
    // term==0 and leader_hint set (not an error). NO_QUORUM / REPLICA_DOWN are errors.
    result< LoginResult > run_login(MemCraftReplica* caller, uint64_t client_token);

    // logout: InternalLogout applied to all live replicas. term-fenced. Returns NOT_LEADER if caller
    // is not the leader.
    status run_logout(MemCraftReplica* caller, uint64_t term);

    // ── membership / routing (consulted by the replicas' client-facing ops) ──
    peer_id_t leader() const;
    std::size_t member_count() const { return members_.size(); }

    // The IO path's view of the fault/latency state: one acquire load, no lock. Hold the returned pointer for
    // the whole of one op so its checks see a consistent snapshot (a superseded one is retired, never freed).
    fault_state const* faults() const { return faults_.load(std::memory_order_acquire); }

    // Convenience wrappers for control paths (the REST endpoint, the cold path, tests). Each loads the
    // snapshot itself; on the IO path prefer faults() once and query it directly.
    bool is_up(peer_id_t id) const { return faults()->is_up(id); }
    bool write_allowed(peer_id_t id) const { return faults()->write_allowed(id); } // false => sub-quorum drop
    std::chrono::milliseconds delay_for(peer_id_t id) const { return faults()->delay_for(id); }
    std::chrono::milliseconds op_timeout() const { return faults()->op_timeout; }

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

    // How long the transport waits before abandoning an op. 0 (the default) means wait forever, which is
    // the model's original behavior: without it a delay merely slows the client down.
    void set_op_timeout(std::chrono::milliseconds t);

private:
    // Copy-on-write: build the successor under fault_mu_, retire it so readers of the old one stay valid,
    // then publish with a release store. Only control paths call this.
    template < class Fn >
    void mutate_faults(Fn&& fn);
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
    static delivery plan_delivery(fault_state const* f, peer_id_t id);

    // Suspend the caller for `d` and resume it on one of `id`'s server threads. `d == 0` still hops: a
    // completion is NEVER inline with its submit, because no real transport delivers one that way. Callers
    // must therefore treat every op as suspending.
    std::shared_ptr< sleep_event > after(std::chrono::milliseconds d, peer_id_t id);
    // Run `fn` on one of `id`'s server threads after `d`; false if we are shutting down and `fn` was dropped.
    // Used for the late delivery of a timed-out write. `fn` must NOT capture a shared_ptr to a replica:
    // replicas hold a shared_ptr to this transport, so that would close a reference cycle. Capture
    // weak_from_this() and lock it.
    bool run_after(std::chrono::milliseconds d, peer_id_t id, std::function< void() > fn);

    std::vector< MemCraftReplica* > live_replicas_locked() const; // requires mu_ held
    static std::size_t quorum(std::size_t n) { return n / 2 + 1; }

    using clock = std::chrono::steady_clock;

    // One replica's server: its own deadline queue and its own thread pool. Each replica is an INDEPENDENT
    // server, so it may complete and reply out of order with itself (threads > 1, which is what a real server
    // processing concurrently does), while replicas stay independent of each other. Reordering ACROSS peers
    // is what quorum depends on; a shim that funnelled every replica through one pool would model neither.
    //
    // Each pool has its own lock, never taken while mu_ is held (and vice versa): a dispatched item re-enters
    // the replica, which re-enters mu_ via is_up() / write_allowed().
    struct replica_service {
        std::multimap< clock::time_point, std::function< void() > > timers;
        std::vector< std::thread > threads;
        std::condition_variable cv;
        std::mutex mu;
        bool stop{false};
    };
    replica_service* service_for(peer_id_t id); // null only if `id` is not a member
    // Spawned eagerly, from the ctor, on the owner's thread: a std::thread must never be created on the IO
    // path, where it would land on the caller's ublk queue thread (and inherit that queue's comm name, making
    // a thread census unreadable). A real transport likewise establishes its connections, and the completion
    // machinery behind them, at setup.
    void start_service(replica_service& s, std::size_t replica_idx);
    void service_loop(replica_service& s);
    std::atomic< bool > stopped_{false};

    std::vector< replica_endpoint > members_; // const after the ctor
    std::unordered_map< peer_id_t, MemCraftReplica*, uuid_hash > by_id_;
    peer_id_t leader_{};
    uint64_t term_{0};
    uint32_t lba_size_{0}; // volume block size (bytes)
    // Guards ONLY by_id_ / leader_ / term_, all of which are cold-path (register_replica, run_login,
    // run_logout, set_leader). The IO path never takes it.
    mutable std::mutex mu_;

    // The IO path's fault/latency view. `faults_` is the published snapshot; `retired_` owns every snapshot
    // ever published, so a reader can hold a raw pointer across a suspension without a refcount.
    std::atomic< fault_state const* > faults_{nullptr};
    std::mutex fault_mu_;                                         // serializes mutators only
    std::vector< std::unique_ptr< fault_state const > > retired_; // guarded by fault_mu_

    // Built once in the ctor from members_ and never rehashed, so a replica_service* stays stable and the
    // per-pool locks need no protection from mu_.
    std::size_t threads_per_replica_;
    std::unordered_map< peer_id_t, std::unique_ptr< replica_service >, uuid_hash > services_;
};

// An N-replica in-process group with NO volumes attached -- the model layer used directly by unit
// tests (and the core of make_memory_replica_set, which additionally wraps each replica in a
// volume_handle). HomeStore-free.
struct MemReplicaGroup {
    std::shared_ptr< MemTransport > net;
    std::vector< std::shared_ptr< MemCraftReplica > > replicas; // index 0 is the default leader

    MemReplicaGroup() = default;
    MemReplicaGroup(MemReplicaGroup&&) = default;
    MemReplicaGroup& operator=(MemReplicaGroup&&) = default;
    MemReplicaGroup(MemReplicaGroup const&) = delete;
    MemReplicaGroup& operator=(MemReplicaGroup const&) = delete;
    // Join the server pools while we still own everything, so no straggler is left running on a replica we
    // are about to destroy -- and so ~MemTransport never lands on one of its own threads. A moved-from group
    // has a null `net` and does nothing.
    ~MemReplicaGroup() {
        if (net) net->shutdown();
    }
};
MemReplicaGroup make_mem_replica_group(volume_id_t vol_id, uint32_t n = 3, uint32_t page_size = 4096,
                                       std::size_t threads_per_replica = 2);

// Deterministic per-replica id derived from the volume id + index (so tests are reproducible).
peer_id_t mem_replica_id(volume_id_t vol_id, uint32_t index);

} // namespace homeblocks::craft
