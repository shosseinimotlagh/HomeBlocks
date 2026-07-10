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

// read_route_map: the client's per-member view of "who holds what", so a read can be routed to (and failed
// over to) a replica that actually holds the winner for the requested range instead of blindly hitting the
// leader. Without it, a leader that missed an acked write -- e.g. it was delayed past the op timeout, so the
// write quorum was the two followers -- serves a stale version, and the server cannot self-detect it (a
// replica that missed a SUFFIX of writes has last_append_lsn frozen and does not know the write exists).
//
// It is the SAME overlay a replica uses to route a read between its journal-tail and its applied index,
// generalized from 2 tiers on one node to N members, and it is deliberately the SIBLING of dlsn_tracker:
// both are a sisl::StreamTracker keyed by dLSN with a per-member concept layered on top.
//
//   server (one node)                          client (read_route_map, N members)
//   ---------------------------------          -------------------------------------------------
//   journal-tail overlay (commit_lsn, H]  ->   per-dLSN HOLDER BITSET over (folded, Ha]   sparse, recent
//   applied index        (<= commit_lsn)  ->   per-member FLOOR (<= folded)               dense base
//   route: journal vs index by max D<=H   ->   route: WHICH members hold the max acked D<=H
//
// The floor is stored inversely as `first_miss[i]` -- the LOWEST dLSN member i is known to have missed
// (kNever if it has missed nothing). Member i is caught up to the frontier iff first_miss[i] > folded. This
// inversion is what makes the structure lock-free: first_miss is advanced by fetch_min, which is commutative
// and idempotent, so concurrent folds and re-application are trivially correct -- no per-member CAS loop, no
// sequential "contiguous floor" pass. first_miss[i] is exactly the client's model of member i's commit_lsn
// boundary (below it, i has everything), read across the wire.
//
// FOLD ON COMPLETION. A dLSN is collapsed into the floors only once EVERY replica op for it has completed --
// StreamTracker's completed bit, which the processor sets when the per-member `completed` mask fills. Folding
// the instant the frontier passed it would race a healthy straggler: a member that acks a microsecond after
// quorum would look like a miss and be stuck behind. The misses are recorded IN the completion processor,
// when `completed == all_mask`, reading `holders` under seq_cst so every acking member's bit is already
// visible -- so a slow-but-healthy member records no miss, and fold itself never reads holders.
//
// CONCURRENCY. record_completion() runs from arbitrary server threads (detached when_quorum children) while
// the client thread(s) fold and route. The ONLY lock is StreamTracker's own shared_mutex -- shared for
// update/foreach/completed_upto, exclusive only on truncate/resize -- exactly the tracker's profile. The
// per-member floors (first_miss) and the folded frontier are plain atomics. There is no bespoke lock.
//
// BOUNDING. Healthy, every write reaches all members, records no miss, and its slot is batch-truncated once
// folded. A missed write's slot folds once complete (a down/refusing member completes immediately, so it
// folds promptly even under a sustained fault); its miss lives on in first_miss, O(N). What is not reclaimed
// without the deferred commit_lsn recovery is a member that missed a write and later resynced: the client
// never sees an ack, so first_miss stays low and it is over-avoided (safe, never stale) until re-login.

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>

#include <sisl/fds/stream_tracker.hpp>

namespace homeblocks::craft {

// One overlay slot per dLSN. `dlsn` is carried so the completion processor -- which StreamTracker hands only
// the slot, not its index -- can record a miss against first_miss. Trivially copyable: StreamTracker callocs
// the slot array and memmoves it on compaction (under its unique lock, so never against a live reader).
struct route_slot {
    int64_t dlsn{-1};
    uint64_t addr{0};
    uint64_t len{0};
    uint64_t holders{0};   // members that acked (hold the data)
    uint64_t completed{0}; // members whose op for this dLSN has finished (acked or not)
};

class read_route_map {
public:
    // (Re-)seed for a session of `n` members with login dLSN `L`. The login SyncRSCommitLSN barrier synced
    // the live members at L, so everything <= L is universally held: no member has missed anything, and the
    // overlay starts at L+1. n must be in [1, 64] (the holder set is a uint64 bitset); real sets are 3/5/7.
    void reset(std::size_t n, int64_t login_dlsn) {
        n_ = n;
        quorum_ = n / 2 + 1;
        all_mask_ = mask_of(n);
        for (std::size_t i = 0; i < n; ++i)
            first_miss_[i].store(kNever, std::memory_order_relaxed);
        folded_.store(login_dlsn, std::memory_order_relaxed);
        last_trunc_.store(login_dlsn, std::memory_order_relaxed);
        overlay_.emplace("craft_route", login_dlsn); // base becomes login_dlsn + 1
    }

    // Create the slot when its dLSN is reserved -- single writer, before the broadcast, so it precedes every
    // completion (which only updates). Mirrors dlsn_tracker::reserve creating the tracker slot.
    void create(int64_t d, uint64_t addr, uint64_t len) { overlay_->create(d, route_slot{d, addr, len, 0, 0}); }

    // One replica op for dLSN `d` completed on member `idx`: `acked` iff it holds the write (a timeout /
    // REPLICA_DOWN / STALE_TERM completes acked=false and is NOT evidence of a hold). Called from the
    // when_quorum hook for every child, on arbitrary threads. When the last member completes, the misses for
    // this dLSN are finalized here (see the header note on ordering) and the StreamTracker completed bit is
    // set, which is what fold keys on.
    void record_completion(int64_t d, std::size_t idx, bool acked) {
        uint64_t const b = bit(idx);
        overlay_->update(d, [this, b, acked](route_slot& s) -> bool {
            if (acked) std::atomic_ref< uint64_t >(s.holders).fetch_or(b, std::memory_order_seq_cst);
            uint64_t const nc = std::atomic_ref< uint64_t >(s.completed).fetch_or(b, std::memory_order_seq_cst) | b;
            if (nc != all_mask_) return false;
            // Last completer: all acking members' holder bits are already visible (each set before its
            // completed bit, seq_cst). A complete slot with no holders is an Empty no-op -- everyone serves
            // it as a hole -- so it is nobody's miss.
            uint64_t const held = std::atomic_ref< uint64_t >(s.holders).load(std::memory_order_seq_cst);
            uint64_t const missers = (held == 0) ? 0 : (all_mask_ & ~held);
            for (std::size_t i = 0; i < n_; ++i)
                if (missers & bit(i)) fetch_min(first_miss_[i], s.dlsn);
            return true; // set StreamTracker's completed bit for this dLSN
        });
    }

    // Advance the folded frontier toward F, but only over the contiguous ALL-COMPLETE prefix (so a
    // not-yet-complete straggler stalls it -- correct, since its misses are not finalized). Misses are
    // already in first_miss, so fold only moves the frontier and batch-truncates the collapsed slots.
    void fold_to(int64_t F) {
        int64_t const cur = folded_.load(std::memory_order_acquire);
        if (F <= cur) return;
        int64_t const target = std::min(F, overlay_->completed_upto(cur + 1));
        if (target <= cur) return;

        int64_t exp = cur; // monotonic; a losing CAS just means another thread advanced it further
        while (exp < target &&
               !folded_.compare_exchange_weak(exp, target, std::memory_order_acq_rel, std::memory_order_relaxed)) {}

        // Truncate in batches, gated so exactly one thread reclaims per batch -- truncate takes the
        // StreamTracker's UNIQUE lock, so doing it every fold would serialize the shared-lock readers.
        int64_t lt = last_trunc_.load(std::memory_order_relaxed);
        if ((target - lt >= k_trunc_batch) &&
            last_trunc_.compare_exchange_strong(lt, target, std::memory_order_acq_rel, std::memory_order_relaxed)) {
            overlay_->truncate(target);
        }
    }

    // Is member `idx` eligible to serve a read of [addr,len) whose highest segment horizon is `Hmax`?
    //   (1) first_miss[idx] > folded  -- it missed nothing at or below the frontier, so it holds every
    //       winner there; and
    //   (2) it holds every DURABLE overlay slot in (folded, Hmax] overlapping [addr,len].
    // A slot with fewer than `quorum` holders is a sub-quorum / failed write: it is NEVER a block's winner
    // (the per-block horizon always clamps below an unresolved write, and a higher acked write shadows a lower
    // one), so it is not a required hold. Skipping it is what keeps a failed write that only the leader
    // journaled from making a perfectly serveable read NOT_ELIGIBLE. Among durable (>= quorum) slots, (2) is
    // still the "holds every overlap" superset rather than the minimal "holds the per-block winner"; that is
    // safe (any two quorums intersect, so for N=3 a common holder always exists), with per-block gating left
    // as a refinement.
    bool eligible(std::size_t idx, uint64_t addr, uint64_t len, int64_t Hmax) {
        int64_t const fold = folded_.load(std::memory_order_acquire);
        if (first_miss_[idx].load(std::memory_order_acquire) <= fold) return false;
        uint64_t const b = bit(idx);
        bool ok = true;
        overlay_->foreach_all_active(fold + 1, [&](int64_t d, route_slot& s) -> bool {
            if (d > Hmax) return false; // past the horizon; stop
            uint64_t const held = std::atomic_ref< uint64_t >(s.holders).load(std::memory_order_acquire);
            if (static_cast< std::size_t >(std::popcount(held)) < quorum_)
                return true; // sub-quorum / failed: never a winner, not required
            if (overlaps(addr, len, s.addr, s.len) && !(held & b)) {
                ok = false;
                return false;
            }
            return true;
        });
        return ok;
    }

    // Observability / tests.
    int64_t folded() const { return folded_.load(std::memory_order_acquire); }
    bool caught_up(std::size_t idx) const {
        return first_miss_[idx].load(std::memory_order_acquire) > folded_.load(std::memory_order_acquire);
    }

private:
    static constexpr int64_t kNever = std::numeric_limits< int64_t >::max();
    static constexpr int64_t k_trunc_batch = 512;

    static uint64_t bit(std::size_t i) { return uint64_t{1} << i; }
    static uint64_t mask_of(std::size_t n) { return (n >= 64) ? ~uint64_t{0} : ((uint64_t{1} << n) - 1); }
    static bool overlaps(uint64_t a0, uint64_t l0, uint64_t a1, uint64_t l1) {
        return (a0 < a1 + l1) && (a1 < a0 + l0);
    }
    static void fetch_min(std::atomic< int64_t >& a, int64_t v) {
        int64_t cur = a.load(std::memory_order_relaxed);
        while ((v < cur) && !a.compare_exchange_weak(cur, v, std::memory_order_acq_rel, std::memory_order_relaxed)) {}
    }

    std::optional< sisl::StreamTracker< route_slot > > overlay_; // dLSN -> holders/completed, sibling of the tracker
    std::array< std::atomic< int64_t >, 64 > first_miss_{};      // per member: lowest dLSN it is known to have missed
    std::size_t n_{0};
    std::size_t quorum_{1};                 // n/2 + 1; a slot with fewer holders is failed, never a winner
    std::atomic< int64_t > folded_{-1};     // every dLSN <= this is collapsed into first_miss (trails F on a straggler)
    std::atomic< int64_t > last_trunc_{-1}; // gates batched truncation
    uint64_t all_mask_{0};
};

} // namespace homeblocks::craft
