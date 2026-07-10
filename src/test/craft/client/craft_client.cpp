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
#include "when_quorum.hpp"

#include <utility>
#include <vector>

#include <sisl/async/when_all.hpp>
#include <sisl/fds/buffer.hpp> // sisl::sg_iterator

namespace homeblocks::craft {

craft_client::craft_client(std::vector< volume_handle > replicas, uint32_t leader, uint32_t max_inflight) :
        replicas_(std::move(replicas)), leader_(leader), tracker_(max_inflight) {}

client_hdr craft_client::make_hdr() const {
    // Every IO piggybacks the commit frontier: CRAFT has no standalone commit verb.
    return client_hdr{term_, tracker_.frontier(), k_all_committed_unknown};
}

// Fail fast rather than burn a dLSN on an IO the replicas will reject anyway. They enforce alignment too.
std::optional< std::error_condition > craft_client::precheck(uint64_t addr, uint64_t len) const {
    if (term_ == 0) return make_error_condition(std::errc::not_connected);
    bool const aligned = (lba_size_ != 0) && (len != 0) && ((addr % lba_size_) == 0) && ((len % lba_size_) == 0);
    if (!aligned) return make_error_condition(std::errc::invalid_argument);
    return std::nullopt;
}

async_status craft_client::login(uint64_t client_token) {
    std::size_t const n = replicas_.size();
    std::error_condition last_err{};

    // A follower does not fail the call: it returns term == 0 plus leader_hint. We have no id -> handle map
    // (LoginResult::members is empty on a redirect), so walk the handles. A hop that errors is skipped, not
    // fatal -- a down replica must not block login.
    for (std::size_t hop = 0; hop < n; ++hop) {
        auto const target = static_cast< uint32_t >((leader_ + hop) % n);
        auto lr = co_await homeblocks::login(replicas_[target], client_token);
        if (!lr.has_value()) {
            last_err = lr.error();
            continue;
        }
        if (lr->term == 0) continue; // NOT_LEADER redirect

        leader_ = target;
        term_ = lr->term;
        lba_size_ = lr->lba_size;
        tracker_.reset_at(lr->dLSN, lr->lba_size);
        // Seed the router for this session: everything <= the login dLSN is universally held (the login
        // SyncRSCommitLSN barrier), and any evidence from a prior session is cleared.
        route_->reset(replicas_.size(), lr->dLSN);
        co_return ok();
    }
    co_return std::unexpected(last_err ? last_err : make_error_condition(craft_error::NOT_LEADER));
}

async_result< size_t > craft_client::write(uint64_t addr, uint64_t len, sisl::sg_list data) {
    if (auto const e = precheck(addr, len)) co_return std::unexpected(*e);

    // Reserve the dLSN and record its range BEFORE the broadcast: any replica that can hold this slot
    // implies a concurrent read's scan can already see it, which is what makes the horizon safe. The router
    // slot is created here too, single-writer, so every completion below only updates it.
    int64_t const dlsn = tracker_.reserve(addr, len);
    route_->create(dlsn, addr, len);
    client_hdr const hdr = make_hdr();

    // Ack at quorum, not at the slowest replica: a straggler must not cost every write the transport's
    // timeout. Stragglers keep running detached, so each replica op must have consumed `data` by its first
    // suspension point (see when_quorum.hpp) -- the caller may recycle the buffer the moment we return.
    std::size_t acks = 0;
    std::shared_ptr< std::vector< status > > results;

    if (replicas_.size() == 1) {
        auto r = co_await homeblocks::async_write(replicas_[0], hdr, dlsn, addr, len, std::move(data));
        acks = r.has_value() ? 1 : 0;
        route_->record_completion(dlsn, 0, /*acked=*/r.has_value());
        results = std::make_shared< std::vector< status > >(1, std::move(r));
    } else {
        // Each task gets its own sg_list descriptor copy; all point at the same caller-owned source buffer.
        std::vector< async_status > futs;
        futs.reserve(replicas_.size());
        for (auto& h : replicas_) {
            futs.push_back(homeblocks::async_write(h, hdr, dlsn, addr, len, data));
        }
        // Feed EVERY replica's completion to the router, not just acks: the fold needs to know when all
        // children have finished before it decides a member missed the write (else a healthy straggler that
        // acks just after quorum is wrongly stuck behind). The quorum members are recorded before we resume
        // (the hook fires before the latch), so their holds are visible before resolve() advances Ha.
        auto q = co_await when_quorum(std::move(futs), quorum(), [route = route_, dlsn](std::size_t i, bool acked) {
            route->record_completion(dlsn, i, acked);
        });
        acks = q.acks;
        results = std::move(q.results);
    }

    if (acks >= quorum()) {
        // Quorum-durable. Do NOT read `results`: children we stopped waiting for may still be writing it.
        tracker_.resolve(dlsn, slot_outcome::acked);
        co_return len;
    }

    // Sub-quorum. The latch cannot have fired on the quorum trigger, so every child has finished and the
    // per-replica errors are safe to read.
    //
    // A DETERMINISTIC rejection means the peer decided the request and provably did not journal it:
    //   - invalid_argument: it evaluated the request and refused it.
    //   - REPLICA_DOWN:     the request was never delivered to it.
    // Unanimity is what makes that useful. If EVERY replica rejected deterministically, nobody holds the
    // slot and it is provably absent set-wide: Empty, a resolved no-op that must not pin the frontier. A
    // subset proves nothing, so the slot stays unresolved and commit correctly stalls there.
    //
    // A timeout must NEVER count. It is not a rollback: the peer may have appended the write while the reply
    // outran the deadline (the model does exactly this). Counting one would resolve an all-timed-out write
    // Empty and advance the frontier past a dLSN a peer later applies. The transport surfaces it as its own
    // error code. STALE_TERM must not count either: we are deposed, and the client that replaced us may have
    // written real data at this dLSN, so "nobody holds it" is false.
    static constexpr auto deterministic_reject = [](std::error_condition const& e) {
        return (e == std::make_error_condition(std::errc::invalid_argument)) ||
            (e == make_error_condition(craft_error::REPLICA_DOWN));
    };

    std::size_t refused = 0;
    std::error_condition last_err{};
    for (auto const& r : *results) {
        if (r.has_value()) continue;
        last_err = r.error();
        if (deterministic_reject(r.error())) ++refused;
    }

    tracker_.resolve(dlsn, (refused == replicas_.size()) ? slot_outcome::empty : slot_outcome::failed);
    co_return std::unexpected(last_err ? last_err : make_error_condition(craft_error::NO_QUORUM));
}

async_result< size_t > craft_client::issue_plan(volume_handle const& target, client_hdr hdr, read_plan const& plan,
                                                uint64_t addr, uint64_t len, sisl::sg_list& dest) {
    // `dest` is filled in place (data extents + zero-filled holes), so the caller needs only the byte count.
    // We copy the descriptors (not the buffer) per attempt, so a failover can re-issue against `dest`.
    if (plan.size() == 1) {
        sisl::sg_list whole = dest;
        auto layout = co_await homeblocks::async_read(target, hdr, plan.front().H, addr, len, std::move(whole));
        if (!layout.has_value()) co_return std::unexpected(layout.error());
        co_return len;
    }

    // Split read: sg_iterator walks `dest` once, in order, carving one descriptor per segment. Each is
    // passed by value into the callee's coroutine frame, so no descriptor of ours outlives this loop.
    sisl::sg_iterator slicer{dest.iovs};
    std::vector< async_result< std::vector< io_extent > > > futs;
    futs.reserve(plan.size());
    for (auto const& seg : plan) {
        sisl::sg_list sub;
        sub.size = seg.len;
        sub.iovs = slicer.next_iovs(static_cast< uint32_t >(seg.len));
        futs.push_back(homeblocks::async_read(target, hdr, seg.H, seg.addr, seg.len, std::move(sub)));
    }
    for (auto const& r : co_await sisl::async::when_all(std::move(futs))) {
        if (!r.has_value()) co_return std::unexpected(r.error());
    }
    co_return len;
}

async_result< size_t > craft_client::read(uint64_t addr, uint64_t len, sisl::sg_list dest) {
    if (auto const e = precheck(addr, len)) co_return std::unexpected(*e);

    auto const plan = co_await tracker_.plan_read(addr, len);
    if (!plan.has_value()) co_return std::unexpected(plan.error());

    client_hdr const hdr = make_hdr();

    // Route to a member that actually holds the winner for this range, not blindly to the leader. Fold the
    // router up to the current frontier, then take the highest horizon across segments: a member eligible at
    // Hmax over the whole range is eligible for every segment at its own (<=) horizon.
    int64_t const F = tracker_.frontier();
    route_->fold_to(F);
    int64_t Hmax = F;
    for (auto const& seg : *plan)
        Hmax = std::max(Hmax, seg.H);

    // Search eligible members from a ROTATING start (not always the leader): a CRAFT read has no leader
    // affinity -- any eligible replica serves it at the horizon -- so starting every read at the leader would
    // pile all read load on it. The rotor advances to the peer after the first eligible one we pick, so
    // successive reads round-robin across eligible members. It is a `thread_local` (like ublkpp's RAID1): each
    // ublk queue thread rotates its own reads independently, so there is no cross-queue coordination and no
    // shared atomic -- the aggregate still spreads evenly. On a transport-level failure (the holder is down or
    // timed out) we fail over to the next eligible one; any other error (e.g. STALE_TERM) is the client's to
    // surface. `dest` is filled in place, so re-issuing simply re-fills the same buffer.
    std::size_t const n = replicas_.size();
    static thread_local std::size_t read_rotor = 0;
    std::size_t const start = read_rotor % n;
    bool any_eligible = false;
    std::error_condition last_err{};
    for (std::size_t hop = 0; hop < n; ++hop) {
        std::size_t const idx = (start + hop) % n;
        if (!route_->eligible(idx, addr, len, Hmax)) continue;
        if (!any_eligible) read_rotor = idx + 1; // next read on this queue thread starts at the following peer
        any_eligible = true;

        auto r = co_await issue_plan(replicas_[idx], hdr, *plan, addr, len, dest);
        if (r.has_value()) co_return *r;

        last_err = r.error();
        bool const transport_failure = (last_err == make_error_condition(craft_error::REPLICA_DOWN)) ||
            (last_err == std::make_error_condition(std::errc::timed_out));
        if (!transport_failure) co_return std::unexpected(last_err);
        // else: this holder is unreachable right now; try the next eligible member.
    }

    // No eligible member served it. NOT_ELIGIBLE if none was eligible at all (the winner's holders are all
    // behind/excluded); NO_QUORUM if holders existed but every one failed the transport.
    co_return std::unexpected(any_eligible ? make_error_condition(craft_error::NO_QUORUM)
                                           : make_error_condition(craft_error::NOT_ELIGIBLE));
}

async_status craft_client::flush() {
    auto r = co_await homeblocks::keep_alive(replicas_[leader_], make_hdr());
    if (!r.has_value()) co_return std::unexpected(r.error());
    co_return ok();
}

} // namespace homeblocks::craft
