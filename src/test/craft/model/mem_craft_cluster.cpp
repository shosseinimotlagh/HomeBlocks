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

extern "C" {
#include <pthread.h> // pthread_setname_np: name the pools, so a thread census is legible
}

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

MemTransport::delivery MemTransport::plan_delivery(fault_state const* f, peer_id_t id) {
    delivery p;
    auto const d = f->delay_for(id);
    if (d.count() <= 0) return p;

    auto const t = f->op_timeout;
    if (t.count() > 0 && d >= t) {
        // The caller is abandoned after `t`, but the peer still receives the op `d` from NOW (the late
        // delivery is scheduled at issue). A write therefore lands after writes issued later to undelayed
        // peers, which is what leaves a Missing slot behind.
        return delivery{t, d, true};
    }
    return delivery{d, std::chrono::milliseconds{0}, false};
}

async_status MemTransport::send_write(std::shared_ptr< MemCraftReplica > to, client_hdr hdr, int64_t dlsn,
                                      uint64_t addr, uint64_t len, sisl::sg_list data) {
    auto const id = to->id();
    // One acquire load, no lock: every check below reads the same consistent snapshot.
    auto const* f = faults();
    // Undeliverable: nothing goes on the wire, so the peer provably never appended it. We do not even
    // serialize -- there is no one to send to.
    if (!f->is_up(id) || !f->write_allowed(id)) co_return fail(craft_error::REPLICA_DOWN);

    // TRANSPORT REQUIREMENT, not a law of nature: we serialize `data` here, at issue, before suspending, so
    // the client is free to recycle its buffer once every replica op has been started. A real transport that
    // submits an async SEND from the caller's pages CANNOT offer this -- it would have to keep the buffer
    // alive until its send completion, and would owe the client a signal saying so. Whichever it is, write it
    // down before choosing a transport. See "What the transport must provide" in client/README.md.
    auto bytes = (data.size == 0) ? nullptr : take_payload(data); // a zero write carries none

    auto const p = plan_delivery(f, id);
    if (p.timed_out) {
        // The caller gives up at `wait`; the peer still receives this at `deliver`. Weak ref: a strong one
        // would cycle, since the replica holds a shared_ptr to us.
        run_after(p.deliver, id, [this, w = to->weak_from_this(), hdr, dlsn, addr, len, bytes] {
            auto const self = w.lock();
            if (!self) return; // replica gone; the in-flight write dies with it
            auto const pid = self->id();
            auto const* lf = faults();
            if (!lf->is_up(pid) || !lf->write_allowed(pid)) return; // went unreachable in flight: dropped
            (void)self->do_write(hdr, dlsn, addr, len, bytes);
        });
    }

    // ALWAYS suspend, even at zero latency: the reply crosses the wire, so it can never be inline with the
    // submit. It resumes on one of THIS replica's server threads. Hold the awaitable in a local across the
    // suspension -- it is non-movable and that thread needs its address to stay put.
    auto ev = after(p.wait, id);
    co_await *ev;

    if (p.timed_out) co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
    // Re-read: the peer may have gone down while this request was on the wire.
    auto const* f2 = faults();
    if (!f2->is_up(id) || !f2->write_allowed(id)) co_return fail(craft_error::REPLICA_DOWN);
    co_return to->do_write(hdr, dlsn, addr, len, std::move(bytes));
}

async_result< std::vector< io_extent > > MemTransport::send_read(std::shared_ptr< MemCraftReplica > to, client_hdr hdr,
                                                                 int64_t read_lsn, uint64_t addr, uint64_t len,
                                                                 sisl::sg_list dest) {
    // No late delivery for a read: a result nobody is waiting for is worthless. A sub-quorum fault does not
    // gate reads either -- it drops writes.
    auto const id = to->id();
    auto const* f = faults();
    if (!f->is_up(id)) co_return fail(craft_error::REPLICA_DOWN);

    auto const p = plan_delivery(f, id);
    {
        auto ev = after(p.wait, id); // always suspends: the reply crosses the wire
        co_await *ev;
    }
    if (p.timed_out) co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
    if (!faults()->is_up(id)) co_return fail(craft_error::REPLICA_DOWN);
    co_return to->do_read(hdr, read_lsn, addr, len, std::move(dest));
}

async_result< LSNPair > MemTransport::send_keep_alive(std::shared_ptr< MemCraftReplica > to, client_hdr hdr) {
    auto const id = to->id();
    auto const* f = faults();
    if (!f->is_up(id)) co_return fail(craft_error::REPLICA_DOWN);

    auto const p = plan_delivery(f, id);
    {
        auto ev = after(p.wait, id); // always suspends: the reply crosses the wire
        co_await *ev;
    }
    if (p.timed_out) co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
    if (!faults()->is_up(id)) co_return fail(craft_error::REPLICA_DOWN);
    co_return to->do_keep_alive(hdr);
}

MemTransport::MemTransport(std::vector< replica_endpoint > members, uint32_t lba_size,
                           std::size_t threads_per_replica) :
        members_{std::move(members)},
        lba_size_{lba_size},
        threads_per_replica_{std::max< std::size_t >(1, threads_per_replica)} {
    if (!members_.empty()) leader_ = members_.front().id;
    // Publish the first (empty) fault snapshot before any IO can read it.
    auto initial = std::make_unique< fault_state const >();
    faults_.store(initial.get(), std::memory_order_release);
    retired_.push_back(std::move(initial));
    // Built once, never rehashed, so a replica_service* handed out by service_for() stays stable for life.
    services_.reserve(members_.size());
    for (std::size_t i = 0; i < members_.size(); ++i) {
        auto [it, _] = services_.emplace(members_[i].id, std::make_unique< replica_service >());
        start_service(*it->second, i); // last thing we do: the pool touches only its own replica_service
    }
}

void MemTransport::start_service(replica_service& s, std::size_t const replica_idx) {
    s.threads.reserve(threads_per_replica_);
    for (std::size_t i = 0; i < threads_per_replica_; ++i) {
        s.threads.emplace_back([this, &s] { service_loop(s); });
        // Name them, or they inherit the creator's comm and a thread census reads as nonsense.
        // pthread_setname_np truncates past 15 chars; "craft_svc12_3" fits.
        auto const name = fmt::format("craft_svc{}_{}", replica_idx, i);
        pthread_setname_np(s.threads.back().native_handle(), name.c_str());
    }
}

// A no-op once shutdown() has run, which an owner always does first (MemReplicaGroup / MemReplicaHandles).
MemTransport::~MemTransport() { shutdown(); }

void MemTransport::shutdown() {
    if (stopped_.exchange(true, std::memory_order_acq_rel)) return; // idempotent

    for (auto& [id, sp] : services_) {
        {
            std::lock_guard< std::mutex > g{sp->mu};
            sp->stop = true;
        }
        sp->cv.notify_all();
    }
    for (auto& [id, sp] : services_) {
        for (auto& t : sp->threads) {
            if (t.joinable()) t.join(); // safe: an owner calls us, never a pool thread
        }
    }

    // DRAIN what the joined threads left behind, on this thread. Dropping it instead would strand every
    // suspended op: a straggler parked on its reply awaitable would never resume, so its coroutine frame --
    // and the shared_ptr to the replica living in that frame -- would leak. Running the items lets each op
    // finish and release. New work cannot be queued: run_after() refuses once `stop` is set.
    for (auto& [id, sp] : services_) {
        std::multimap< clock::time_point, std::function< void() > > left;
        {
            std::lock_guard< std::mutex > g{sp->mu};
            left.swap(sp->timers);
        }
        for (auto& [when, fn] : left) {
            fn();
        }
    }
}

MemTransport::replica_service* MemTransport::service_for(peer_id_t id) {
    auto const it = services_.find(id);
    return (it == services_.end()) ? nullptr : it->second.get();
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

// Copy-on-write. Readers never block, and a reader holding the old snapshot stays valid because we retire it
// rather than free it. Only control paths get here.
template < class Fn >
void MemTransport::mutate_faults(Fn&& fn) {
    std::lock_guard< std::mutex > g{fault_mu_};
    auto next = std::make_unique< fault_state >(*faults_.load(std::memory_order_relaxed)); // only we mutate
    fn(*next);
    auto const* raw = next.get();
    retired_.push_back(std::move(next));           // keeps every published snapshot alive for life
    faults_.store(raw, std::memory_order_release); // fully built before it is visible
}

void MemTransport::set_up(peer_id_t id, bool up) {
    mutate_faults([&](fault_state& s) {
        if (up) {
            s.down.erase(id);
        } else {
            s.down.insert(id);
        }
    });
}
void MemTransport::force_subquorum(std::vector< peer_id_t > keep) {
    mutate_faults(
        [&](fault_state& s) { s.write_keep = std::unordered_set< peer_id_t, uuid_hash >(keep.begin(), keep.end()); });
}
void MemTransport::clear_faults() {
    mutate_faults([](fault_state& s) {
        s.down.clear();
        s.write_keep.reset();
        s.delays.clear();
    });
}

// ── latency injection ──

void MemTransport::set_delay(peer_id_t id, std::chrono::milliseconds d) {
    mutate_faults([&](fault_state& s) {
        if (d.count() <= 0) {
            s.delays.erase(id);
        } else {
            s.delays[id] = d;
        }
    });
}
void MemTransport::set_op_timeout(std::chrono::milliseconds t) {
    mutate_faults([&](fault_state& s) { s.op_timeout = (t.count() > 0) ? t : std::chrono::milliseconds{0}; });
}

// `d == 0` still goes through `id`'s server pool. A completion is NEVER inline with its submit, because no
// real transport delivers one that way, and a shim that did would let the client silently depend on it: the
// fan-out would run sequentially, when_quorum would never suspend, and a straggler would never race anyone.
std::shared_ptr< MemTransport::sleep_event > MemTransport::after(std::chrono::milliseconds d, peer_id_t id) {
    auto ev = std::make_shared< sleep_event >();
    // One of `id`'s server threads holds the only other reference; complete() resumes the awaiting coroutine
    // there. If we are already shutting down, complete inline rather than strand the coroutine forever.
    if (!run_after(d, id, [ev] { ev->complete({}); })) ev->complete({});
    return ev;
}

bool MemTransport::run_after(std::chrono::milliseconds d, peer_id_t id, std::function< void() > fn) {
    auto* const s = service_for(id);
    if (!s) return false; // not a member; nothing to run it on

    std::unique_lock< std::mutex > lk{s->mu};
    if (s->stop) return false; // shut down; the pool is joined and nothing will ever run this
    s->timers.emplace(clock::now() + d, std::move(fn));
    lk.unlock();
    s->cv.notify_one();
    return true;
}

void MemTransport::service_loop(replica_service& s) {
    std::unique_lock< std::mutex > lk{s.mu};
    for (;;) {
        if (s.stop) return;
        if (s.timers.empty()) {
            s.cv.wait(lk);
            continue;
        }
        auto const due = s.timers.begin()->first;
        if (clock::now() < due) {
            s.cv.wait_until(lk, due); // a nearer deadline (or stop) wakes us early
            continue;
        }
        // Take exactly ONE due item, not all of them: with more than one thread that is what lets this
        // server complete two of its own requests concurrently and reply to them out of order, which is what
        // a real server does. Dispatch with s.mu RELEASED -- the item re-enters the replica, which takes its
        // own mutex and calls back into is_up() / write_allowed() under mu_.
        auto it = s.timers.begin();
        auto fn = std::move(it->second);
        s.timers.erase(it);
        bool const more_due = !s.timers.empty() && (s.timers.begin()->first <= clock::now());
        lk.unlock();
        if (more_due) s.cv.notify_one(); // wake a sibling for the item we did not take

        fn();
        lk.lock();
    }
}

std::vector< MemCraftReplica* > MemTransport::live_replicas_locked() const {
    auto const* f = faults(); // lock-free; mu_ is held for by_id_, not for this
    std::vector< MemCraftReplica* > live;
    for (auto const& m : members_) {
        if (!f->is_up(m.id)) continue;
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
        if (!faults()->is_up(caller->id())) return fail(craft_error::REPLICA_DOWN);
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
        if (!faults()->is_up(caller->id())) return fail(craft_error::REPLICA_DOWN);
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

MemReplicaGroup make_mem_replica_group(volume_id_t vol_id, uint32_t n, uint32_t page_size,
                                       std::size_t threads_per_replica) {
    std::vector< replica_endpoint > members;
    members.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        members.push_back(replica_endpoint{mem_replica_id(vol_id, i), "mem://replica-" + std::to_string(i)});
    }
    auto net = std::make_shared< MemTransport >(members, page_size, threads_per_replica);
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
