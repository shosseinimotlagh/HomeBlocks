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

#include "mem_craft_cluster.hpp"

#include <algorithm>
#include <string>

namespace homeblocks::craft {

namespace {
auto fail(craft_error e) { return std::unexpected(make_error_condition(e)); }

// The serialize. A transport copies the caller's scatter/gather payload into its own send buffer when the
// request is issued, which is exactly why the caller may recycle its buffer once every send is away. This is
// the ONE byte copy on the write path; the replica's journal slot then adopts this buffer.
std::shared_ptr< std::vector< uint8_t > > take_payload(sisl::sg_list const& s) {
    auto b = std::make_shared< std::vector< uint8_t > >();
    b->reserve(s.size);
    for (auto const& io : s.iovs) {
        auto const* p = static_cast< uint8_t const* >(io.iov_base);
        b->insert(b->end(), p, p + io.iov_len);
    }
    return b;
}
} // namespace

// ── the wire ──

MemTransport::delivery MemTransport::plan_delivery(peer_id_t id) const {
    delivery p;
    auto const d = delay_for(id);
    if (d.count() <= 0) return p;

    auto const t = op_timeout();
    if (t.count() > 0 && d >= t) {
        // The caller is abandoned after `t`, but the peer still receives the op `d` from NOW (the late
        // delivery is scheduled at issue). A write therefore lands after writes issued later to undelayed
        // peers, which is what leaves a Missing slot behind.
        return delivery{t, d, true};
    }
    return delivery{d, std::chrono::milliseconds{0}, false};
}

async_status MemTransport::send_write(MemCraftReplica* to, client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                      sisl::sg_list data) {
    auto const id = to->id();
    // Undeliverable: nothing goes on the wire, so the peer provably never appended it. We do not even
    // serialize -- there is no one to send to.
    if (!is_up(id) || !write_allowed(id)) co_return fail(craft_error::REPLICA_DOWN);

    // TRANSPORT REQUIREMENT, not a law of nature: we serialize `data` here, at issue, before suspending, so
    // the client is free to recycle its buffer once every replica op has been started. A real transport that
    // submits an async SEND from the caller's pages CANNOT offer this -- it would have to keep the buffer
    // alive until its send completion, and would owe the client a signal saying so. Whichever it is, write it
    // down before choosing a transport. See "What the transport must provide" in client/README.md.
    auto bytes = (data.size == 0) ? nullptr : take_payload(data); // a zero write carries none

    auto const p = plan_delivery(id);
    if (p.timed_out) {
        // The caller gives up at `wait`; the peer still receives this at `deliver`. Weak ref: a strong one
        // would cycle, since the replica holds a shared_ptr to us.
        run_after(p.deliver, [this, w = to->weak_from_this(), hdr, dlsn, addr, len, bytes] {
            auto const self = w.lock();
            if (!self) return; // replica gone; the in-flight write dies with it
            auto const pid = self->id();
            if (!is_up(pid) || !write_allowed(pid)) return; // went unreachable while in flight: dropped
            (void)self->do_write(hdr, dlsn, addr, len, bytes);
        });
    }

    // ALWAYS suspend, even at zero latency: the reply crosses the wire, so it can never be inline with the
    // submit. Hold the awaitable in a local across it -- it is non-movable and a service thread needs its
    // address to stay put.
    auto ev = after(p.wait);
    co_await *ev;

    if (p.timed_out) co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
    if (!is_up(id) || !write_allowed(id)) co_return fail(craft_error::REPLICA_DOWN); // went down in flight
    co_return to->do_write(hdr, dlsn, addr, len, std::move(bytes));
}

async_result< std::vector< io_extent > > MemTransport::send_read(MemCraftReplica* to, client_hdr hdr, int64_t read_lsn,
                                                                 uint64_t addr, uint64_t len, sisl::sg_list dest) {
    // No late delivery for a read: a result nobody is waiting for is worthless. A sub-quorum fault does not
    // gate reads either -- it drops writes.
    auto const id = to->id();
    if (!is_up(id)) co_return fail(craft_error::REPLICA_DOWN);

    auto const p = plan_delivery(id);
    {
        auto ev = after(p.wait); // always suspends: the reply crosses the wire
        co_await *ev;
    }
    if (p.timed_out) co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
    if (!is_up(id)) co_return fail(craft_error::REPLICA_DOWN);
    co_return to->do_read(hdr, read_lsn, addr, len, std::move(dest));
}

async_result< LSNPair > MemTransport::send_keep_alive(MemCraftReplica* to, client_hdr hdr) {
    auto const id = to->id();
    if (!is_up(id)) co_return fail(craft_error::REPLICA_DOWN);

    auto const p = plan_delivery(id);
    {
        auto ev = after(p.wait); // always suspends: the reply crosses the wire
        co_await *ev;
    }
    if (p.timed_out) co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
    if (!is_up(id)) co_return fail(craft_error::REPLICA_DOWN);
    co_return to->do_keep_alive(hdr);
}

MemTransport::MemTransport(std::vector< replica_endpoint > members, uint32_t lba_size) :
        members_{std::move(members)}, lba_size_{lba_size} {
    if (!members_.empty()) leader_ = members_.front().id;
}

MemTransport::~MemTransport() {
    {
        std::lock_guard< std::mutex > g{tmu_};
        service_stop_ = true;
    }
    tcv_.notify_all();
    for (auto& t : service_threads_) {
        if (t.joinable()) t.join();
    }
    // Anything still queued (a late write whose delay had not elapsed) is dropped with the queue: the
    // replicas are on their way out too, and each closure holds only a weak_ptr to one.
}

void MemTransport::set_completion_executor(completion_executor ex) {
    std::lock_guard< std::mutex > g{tmu_};
    exec_ = std::move(ex);
}

void MemTransport::register_replica(MemCraftReplica* r) {
    std::lock_guard< std::mutex > g{mu_};
    by_id_[r->id()] = r;
}

peer_id_t MemTransport::leader() const {
    std::lock_guard< std::mutex > g{mu_};
    return leader_;
}
void MemTransport::set_leader(peer_id_t id) {
    std::lock_guard< std::mutex > g{mu_};
    leader_ = id;
}

bool MemTransport::is_up(peer_id_t id) const {
    std::lock_guard< std::mutex > g{mu_};
    return !down_.contains(id);
}
bool MemTransport::write_allowed(peer_id_t id) const {
    std::lock_guard< std::mutex > g{mu_};
    if (down_.contains(id)) return false;
    if (write_keep_ && !write_keep_->contains(id)) return false;
    return true;
}

void MemTransport::set_up(peer_id_t id, bool up) {
    std::lock_guard< std::mutex > g{mu_};
    if (up) {
        down_.erase(id);
    } else {
        down_.insert(id);
    }
}
void MemTransport::force_subquorum(std::vector< peer_id_t > keep) {
    std::lock_guard< std::mutex > g{mu_};
    write_keep_ = std::unordered_set< peer_id_t, uuid_hash >(keep.begin(), keep.end());
}
void MemTransport::clear_faults() {
    std::lock_guard< std::mutex > g{mu_};
    down_.clear();
    write_keep_.reset();
    delays_.clear();
}

// ── latency injection ──

void MemTransport::set_delay(peer_id_t id, std::chrono::milliseconds d) {
    std::lock_guard< std::mutex > g{mu_};
    if (d.count() <= 0) {
        delays_.erase(id);
    } else {
        delays_[id] = d;
    }
}
std::chrono::milliseconds MemTransport::delay_for(peer_id_t id) const {
    std::lock_guard< std::mutex > g{mu_};
    auto const it = delays_.find(id);
    return (it == delays_.end()) ? std::chrono::milliseconds{0} : it->second;
}
void MemTransport::set_op_timeout(std::chrono::milliseconds t) {
    std::lock_guard< std::mutex > g{mu_};
    op_timeout_ = (t.count() > 0) ? t : std::chrono::milliseconds{0};
}
std::chrono::milliseconds MemTransport::op_timeout() const {
    std::lock_guard< std::mutex > g{mu_};
    return op_timeout_;
}

// `d == 0` still goes through the queue. A completion is NEVER inline with its submit, because no real
// transport delivers one that way, and a shim that did would let the client silently depend on it: the
// fan-out would run sequentially, when_quorum would never suspend, and a straggler would never race anyone.
std::shared_ptr< MemTransport::sleep_event > MemTransport::after(std::chrono::milliseconds d) {
    auto ev = std::make_shared< sleep_event >();
    // A service thread holds the only other reference; complete() resumes the awaiting coroutine there.
    // If we are already shutting down, complete inline rather than strand the coroutine forever.
    if (!run_after(d, [ev] { ev->complete({}); })) ev->complete({});
    return ev;
}

bool MemTransport::run_after(std::chrono::milliseconds d, std::function< void() > fn) {
    std::unique_lock< std::mutex > lk{tmu_};
    if (service_stop_) return false;
    ensure_service();
    timers_.emplace(clock::now() + d, std::move(fn));
    lk.unlock();
    tcv_.notify_one();
    return true;
}

void MemTransport::ensure_service() {
    if (!service_threads_.empty()) return; // already running (tmu_ held)
    service_threads_.reserve(k_service_threads);
    for (std::size_t i = 0; i < k_service_threads; ++i) {
        service_threads_.emplace_back([this] { service_loop(); });
    }
}

void MemTransport::service_loop() {
    std::unique_lock< std::mutex > lk{tmu_};
    for (;;) {
        if (service_stop_) return;
        if (timers_.empty()) {
            tcv_.wait(lk);
            continue;
        }
        auto const due = timers_.begin()->first;
        if (clock::now() < due) {
            tcv_.wait_until(lk, due); // a nearer deadline (or stop) wakes us early
            continue;
        }
        // Take exactly ONE due item, not all of them: with K threads that is what lets two completions of
        // the same broadcast run concurrently and land out of order. Then dispatch with tmu_ RELEASED --
        // the item re-enters the replica, which takes its own mutex and calls back under mu_.
        auto it = timers_.begin();
        auto fn = std::move(it->second);
        timers_.erase(it);
        bool const more_due = !timers_.empty() && (timers_.begin()->first <= clock::now());
        auto ex = exec_;
        lk.unlock();
        if (more_due) tcv_.notify_one(); // wake a sibling for the item we did not take

        if (ex) {
            ex(std::move(fn)); // hand the completion to the caller's executor (an iomgr reactor)
        } else {
            fn();
        }
        lk.lock();
    }
}

std::vector< MemCraftReplica* > MemTransport::live_replicas_locked() const {
    std::vector< MemCraftReplica* > live;
    for (auto const& m : members_) {
        if (down_.contains(m.id)) continue;
        if (auto it = by_id_.find(m.id); it != by_id_.end()) live.push_back(it->second);
    }
    return live;
}

result< LoginResult > MemTransport::run_login(MemCraftReplica* caller, uint64_t client_token) {
    std::vector< MemCraftReplica* > live;
    std::vector< replica_endpoint > members_copy;
    uint64_t nt{0};
    {
        std::lock_guard< std::mutex > g{mu_};
        if (down_.contains(caller->id())) return fail(craft_error::REPLICA_DOWN);
        if (caller->id() != leader_) return LoginResult{{}, -1, 0, 0, leader_};
        live = live_replicas_locked();
        if (live.size() < quorum(members_.size())) return fail(craft_error::NO_QUORUM);
        nt = ++term_;
        members_copy = members_;
    }
    // Drive the cold path WITHOUT holding mu_ (each replica call takes only its own lock).
    // Phase 1: GetRSCommitLSN -> rs_commit_lsn = max(quorum.last_append_lsn).
    int64_t rs = -1;
    for (auto* r : live)
        rs = std::max(rs, r->peek_lsns().last_append_lsn);
    // Phase 2: SyncRSCommitLSN(rs) applied to all live members.
    for (auto* r : live)
        r->cold_apply_sync(rs, client_token);
    // Phase 3: InternalLogin(token, term+1) applied to all live members.
    for (auto* r : live)
        r->cold_apply_login(client_token, nt);
    // Phase 4: truncate any stale tail above rs.
    for (auto* r : live) {
        if (r->peek_lsns().last_append_lsn > rs) r->cold_truncate_above(rs);
    }
    return LoginResult{std::move(members_copy), rs, nt, lba_size_};
}

status MemTransport::run_logout(MemCraftReplica* caller, uint64_t term) {
    std::vector< MemCraftReplica* > live;
    {
        std::lock_guard< std::mutex > g{mu_};
        if (down_.contains(caller->id())) return fail(craft_error::REPLICA_DOWN);
        if (caller->id() != leader_) return fail(craft_error::NOT_LEADER);
        live = live_replicas_locked();
    }
    // InternalLogout: clear the session on all live replicas. Term was already checked by the caller.
    for (auto* r : live)
        r->cold_apply_logout();
    return ok();
}

// ── group factory (model level; no volumes attached) ──

peer_id_t mem_replica_id(volume_id_t vol_id, uint32_t index) {
    peer_id_t id = vol_id; // copy the 16 bytes, then perturb the tail deterministically with `index`
    auto* p = reinterpret_cast< uint8_t* >(&id);
    p[12] ^= static_cast< uint8_t >(index & 0xff);
    p[13] ^= static_cast< uint8_t >((index >> 8) & 0xff);
    p[14] ^= static_cast< uint8_t >((index >> 16) & 0xff);
    p[15] ^= static_cast< uint8_t >((index >> 24) & 0xff);
    return id;
}

MemReplicaGroup make_mem_replica_group(volume_id_t vol_id, uint32_t n, uint32_t page_size) {
    std::vector< replica_endpoint > members;
    members.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        members.push_back(replica_endpoint{mem_replica_id(vol_id, i), "mem://replica-" + std::to_string(i)});
    }
    auto net = std::make_shared< MemTransport >(members, page_size);
    MemReplicaGroup group;
    group.net = net;
    group.replicas.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        auto r = std::make_shared< MemCraftReplica >(members[i], page_size, net);
        net->register_replica(r.get());
        group.replicas.push_back(std::move(r));
    }
    return group;
}

} // namespace homeblocks::craft
