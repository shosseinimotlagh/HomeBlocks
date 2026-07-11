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
// generalized from 2 tiers on one node to N members. TWO TIERS split at the commit frontier `folded`:
//
//   server (one node)                          client (read_route_map, N members)
//   ---------------------------------          -------------------------------------------------------
//   journal-tail overlay (commit_lsn, H]  ->   ABOVE folded: per-dLSN HOLDER BITSET (sisl::StreamTracker)
//   applied index        (<= commit_lsn)  ->   BELOW folded: per-member LBA-range MISSING MAP (boost::icl)
//   route: journal vs index by max D<=H   ->   route: WHICH members hold every winner <= H over the range
//
// ABOVE `folded` (the live (folded, Ha] window): the StreamTracker overlay carries, per dLSN, the holder
// bitset. Its per-write updates are allocation-free bit-ORs, which is why we keep it for the churny recent
// window rather than folding everything into one interval map. It also handles the horizon clamp and
// stragglers precisely (a not-yet-acked member is simply not in the holder set).
//
// BELOW `folded`: a per-member boost::icl::interval_map keyed by BYTE range. This is the CRAFT design's
// "per-replica Missing map" (docs/craft/rpcs.md:82-83: an eligible replica has "no Missing slot <= H
// overlapping the range"). eligible() below the frontier is one LBA-overlap query -- so a member missing
// block X still serves reads of block Y. That is the whole point: the old scalar excluded a member from ALL
// sub-frontier reads once it missed anything, which for N=3 could make a read of a block ALL members hold
// return NOT_ELIGIBLE.
//
// FOLD IS ASCENDING. A missed write is applied to the interval map only on fold, in ascending dLSN order,
// because clear-on-supersede is order-sensitive: "member holds the LATEST folded write on this block ->
// erase; missed it -> set". Folds are contiguous ascending (F advances over the resolved prefix), so per LBA
// the map ends reflecting the highest folded winner. record_completion cannot do this (it fires in
// completion order), so a non-universal completed slot is handed to fold via `pending_`. Universal (all-ack)
// and Empty (no-ack) and sub-quorum (never a winner) slots hand off nothing -> healthy writes touch no
// interval map, and a healthy fold is lock-free (an atomic frontier advance gated by `pending_hint_`).
//
// CONCURRENCY. The interval maps are guarded by a std::shared_mutex `map_mu_`: eligible() takes it shared,
// the fold apply takes it exclusive (fault-only -- healthy folds never touch it). `pending_`/`pending_hint_`
// carry misses from the completion threads to the fold; `fold_mu_` serializes the fault-path fold so the
// apply stays ascending. The overlay keeps its own StreamTracker shared_mutex. So a healthy read is:
// lock-free frontier advance + shared map query (empty map) + shared overlay scan.
//
// BOUNDING. The map is bounded by each member's coalesced missing byte coverage (icl merges adjacent
// ranges), cleared by supersede (a later folded write it holds) and by commit_lsn recovery (advance_synced,
// fed by the broadcast keep_alive), reset on re-login. Until server-side resync fills a member's holes its
// commit_lsn stalls just below the first one, so recovery clears only what resync has filled; a member that
// missed a block only rewritten universally (or never) stays over-avoided (safe, never stale) until then.
//
// RECLAIM FLOOR. `synced_` doubles as the client's model of each member's commit_lsn, so `all_committed_` --
// the min across members, stamped into `client_hdr.all_committed_lsn` -- is the set-wide journal reclaim
// floor. A member we have not heard a commit_lsn from (down, or pre-keep_alive) holds it at the login
// baseline, correctly pinning the floor down until that member reports progress.

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <utility>
#include <vector>

#include <boost/icl/interval.hpp>
#include <boost/icl/interval_map.hpp>

#include <sisl/fds/stream_tracker.hpp>

namespace homeblocks::craft {

// One overlay slot per dLSN, above the frontier. `dlsn`/`addr`/`len` are carried so fold, which drains a
// completed slot's miss, is self-contained. Trivially copyable: StreamTracker callocs the slot array and
// memmoves it on compaction (under its unique lock, so never against a live reader).
struct route_slot {
    int64_t dlsn{-1};
    uint64_t addr{0};
    uint64_t len{0};
    uint64_t holders{0};   // members that acked (hold the data)
    uint64_t completed{0}; // members whose op for this dLSN has finished (acked or not)
};

class read_route_map {
    // A durable-but-non-universal completed slot handed from a completion thread to the fold. Self-contained
    // so fold never re-reads the (possibly truncated) overlay slot.
    struct pending_miss {
        int64_t dlsn;
        uint64_t held;
        uint64_t addr;
        uint64_t len;
    };
    // key = byte offset; value = the dLSN the member is known to miss on that range (only the deferred
    // recovery reads the value). partial_enricher (not the default absorber) so a segment whose value is
    // dLSN 0 -- the very first write after a fresh login -- is retained rather than absorbed as the identity.
    using missing_map = boost::icl::interval_map< uint64_t, int64_t, boost::icl::partial_enricher >;

public:
    // (Re-)seed for a session of `n` members with login dLSN `L`. The login SyncRSCommitLSN barrier synced
    // the live members at L, so everything <= L is universally held: no member is missing anything, the
    // overlay starts at L+1, and each member's Synced baseline is L. n must be in [1, 64] (bitset).
    void reset(std::size_t n, int64_t login_dlsn) {
        assert(n >= 1 && n <= k_max_members); // real CRAFT replica sets are 3/5/7/9
        n_ = n;
        quorum_ = n / 2 + 1;
        all_mask_ = mask_of(n);
        login_dlsn_ = login_dlsn;
        {
            std::unique_lock< std::shared_mutex > g{map_mu_};
            for (std::size_t i = 0; i < n; ++i)
                miss_[i].clear();
        }
        for (std::size_t i = 0; i < n; ++i) {
            synced_[i].store(login_dlsn, std::memory_order_relaxed);
            ka_inflight_[i].store(false, std::memory_order_relaxed);
        }
        all_committed_.store(login_dlsn, std::memory_order_relaxed); // min synced_ == login baseline
        {
            std::lock_guard< std::mutex > g{pending_mu_};
            pending_.clear();
        }
        pending_hint_.store(kNever, std::memory_order_relaxed);
        folded_.store(login_dlsn, std::memory_order_relaxed);
        last_trunc_.store(login_dlsn, std::memory_order_relaxed);
        overlay_.emplace("craft_route", login_dlsn); // base becomes login_dlsn + 1
    }

    // Create the slot when its dLSN is reserved -- single writer, before the broadcast, so it precedes every
    // completion (which only updates). Mirrors dlsn_tracker::reserve creating the tracker slot.
    void create(int64_t d, uint64_t addr, uint64_t len) { overlay_->create(d, route_slot{d, addr, len, 0, 0}); }

    // One replica op for dLSN `d` completed on member `idx`: `acked` iff it holds the write (a timeout /
    // REPLICA_DOWN / STALE_TERM completes acked=false). Called from the when_quorum hook for every child, on
    // arbitrary threads. On the last completer, a DURABLE (quorum-acked) but non-universal slot is handed to
    // the fold via pending_ (fold applies it to the interval map in ascending order); the completed bit it
    // sets is what fold keys on -- and the pending push happens BEFORE that bit, so a fold that later sees
    // this dLSN complete is guaranteed to see the pending entry.
    void record_completion(int64_t d, std::size_t idx, bool acked) {
        uint64_t const b = bit(idx);
        overlay_->update(d, [this, b, acked](route_slot& s) -> bool {
            if (acked) std::atomic_ref< uint64_t >(s.holders).fetch_or(b, std::memory_order_seq_cst);
            uint64_t const nc = std::atomic_ref< uint64_t >(s.completed).fetch_or(b, std::memory_order_seq_cst) | b;
            if (nc != all_mask_) return false;
            // Last completer: all acking members' holder bits are visible (each set before its completed bit,
            // seq_cst). Hand off only a DURABLE (>= quorum) slot that someone missed: a sub-quorum / Empty
            // (< quorum) slot is never a winner and never folds below F, and a universal slot is nobody's miss.
            uint64_t const held = std::atomic_ref< uint64_t >(s.holders).load(std::memory_order_seq_cst);
            if (static_cast< std::size_t >(std::popcount(held)) >= quorum_ && held != all_mask_) {
                std::lock_guard< std::mutex > g{pending_mu_};
                pending_.push_back(pending_miss{s.dlsn, held, s.addr, s.len});
                int64_t h = pending_hint_.load(std::memory_order_relaxed);
                while (s.dlsn < h &&
                       !pending_hint_.compare_exchange_weak(h, s.dlsn, std::memory_order_release,
                                                            std::memory_order_relaxed)) {}
            }
            return true; // set StreamTracker's completed bit for this dLSN
        });
    }

    // Advance the folded frontier toward F over the contiguous ALL-COMPLETE prefix, applying any pending
    // misses <= the new frontier to the interval maps. Healthy fast path (no pending miss <= target) is
    // lock-free: it just advances the atomic frontier and batch-truncates.
    void fold_to(int64_t F) {
        int64_t cur = folded_.load(std::memory_order_acquire);
        if (F <= cur) return;
        int64_t target = std::min(F, overlay_->completed_upto(cur + 1));
        if (target <= cur) return;

        // pending_hint_ is a conservative lower bound on the min pending dLSN (fetch_min on push, ordered
        // before the completed bit). If it is above target, nothing <= target needs the map -> lock-free.
        if (pending_hint_.load(std::memory_order_acquire) > target) {
            advance_folded(target);
            maybe_truncate(target);
            return;
        }

        // Slow (fault) path: serialize so the interval-map apply stays ascending across concurrent folds.
        std::lock_guard< std::mutex > fg{fold_mu_};
        cur = folded_.load(std::memory_order_acquire);
        target = std::min(F, overlay_->completed_upto(cur + 1));
        if (target <= cur) return;

        std::vector< pending_miss > batch;
        {
            std::lock_guard< std::mutex > pg{pending_mu_};
            int64_t new_hint = kNever;
            std::vector< pending_miss > keep;
            keep.reserve(pending_.size());
            for (auto const& e : pending_) {
                if (e.dlsn <= target) {
                    batch.push_back(e);
                } else {
                    keep.push_back(e);
                    new_hint = std::min(new_hint, e.dlsn);
                }
            }
            pending_.swap(keep);
            pending_hint_.store(new_hint, std::memory_order_release);
        }

        if (!batch.empty()) {
            std::sort(batch.begin(), batch.end(),
                      [](pending_miss const& x, pending_miss const& y) { return x.dlsn < y.dlsn; });
            std::unique_lock< std::shared_mutex > mg{map_mu_};
            for (auto const& e : batch) {
                auto const iv = byte_ivl(e.addr, e.len);
                for (std::size_t i = 0; i < n_; ++i) {
                    if (e.held & bit(i))
                        miss_[i].erase(iv); // holds the latest folded write on this range -> not missing
                    else
                        miss_[i].set(std::make_pair(iv, e.dlsn)); // missed it -> missing there
                }
            }
        }
        advance_folded(target); // frontier AFTER the map is applied...
        maybe_truncate(target); // ...and the overlay slot is dropped only after that, so no dLSN is ever
                                // in neither tier (eligible's intersects scans the whole map anyway).
    }

    // Is member `idx` eligible to serve a read of [addr,len) whose highest segment horizon is `Hmax`?
    //   (a) Synced >= L    -- filled to the login baseline (vacuous until recovery advances synced_);
    //   (b) below the frontier: no Missing byte-range overlapping [addr,len) (per-block precise); and
    //   (c) above the frontier: it holds every DURABLE overlay slot in (folded, Hmax] overlapping the range.
    // A slot with fewer than `quorum` holders is a sub-quorum / failed write, never a winner, so skipped in
    // (c). Among durable slots (c) is still the "holds every overlap" superset (safe for N=3 by quorum
    // intersection); per-block gating there is a further refinement.
    bool eligible(std::size_t idx, uint64_t addr, uint64_t len, int64_t Hmax) {
        if (synced_[idx].load(std::memory_order_acquire) < login_dlsn_) return false; // (a)
        {
            std::shared_lock< std::shared_mutex > mg{map_mu_};
            if (boost::icl::intersects(miss_[idx], byte_ivl(addr, len))) return false; // (b)
        }
        int64_t const fold = folded_.load(std::memory_order_acquire); // (c)
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

    // Recovery hook, fed by the broadcast keep_alive with member `idx`'s achieved commit_lsn `C`. Two effects:
    // (1) advance `synced_[idx]` and recompute `all_committed_` (the min-across-members reclaim floor) -- this
    // is live; (2) erase `miss_[idx]` segments <= C, i.e. blocks the member has since applied. Effect (2) is
    // dormant until server-side resync fills the member's holes: a laggard's commit_lsn stalls just below its
    // first Missing hole, so nothing at/above that hole clears until resync fills it (then C passes it and the
    // erase fires). This is the sole place synced_ advances and the sole non-fold map mutation.
    void advance_synced(std::size_t idx, int64_t C) {
        std::lock_guard< std::mutex > fg{fold_mu_};
        if (C <= synced_[idx].load(std::memory_order_relaxed)) return;
        synced_[idx].store(C, std::memory_order_release);
        int64_t floor = synced_[0].load(std::memory_order_relaxed); // min commit_lsn across members
        for (std::size_t i = 1; i < n_; ++i)
            floor = std::min(floor, synced_[i].load(std::memory_order_relaxed));
        all_committed_.store(floor, std::memory_order_release);
        std::unique_lock< std::shared_mutex > mg{map_mu_};
        std::vector< boost::icl::interval< uint64_t >::type > drop;
        for (auto const& seg : miss_[idx])
            if (seg.second <= C) drop.push_back(seg.first);
        for (auto const& iv : drop)
            miss_[idx].erase(iv);
    }

    // At-most-one outstanding keep_alive per member (leg) -- the collapse for the client's timer-less keep_alive
    // drive. try_begin_keepalive returns true iff the caller may fire one (none already in flight for `idx`);
    // the completion MUST call end_keepalive to release. This lives in the route map, not the client, because a
    // detached keep_alive outlives the client (it captures this map's shared_ptr) and clears the flag here.
    bool try_begin_keepalive(std::size_t idx) { return !ka_inflight_[idx].exchange(true, std::memory_order_acq_rel); }
    void end_keepalive(std::size_t idx) { ka_inflight_[idx].store(false, std::memory_order_release); }

    // Observability / tests.
    int64_t folded() const { return folded_.load(std::memory_order_acquire); }
    // The set-wide journal reclaim floor: min commit_lsn across members (client_hdr.all_committed_lsn).
    int64_t all_committed() const { return all_committed_.load(std::memory_order_acquire); }
    bool caught_up(std::size_t idx) const {
        if (synced_[idx].load(std::memory_order_acquire) < login_dlsn_) return false;
        std::shared_lock< std::shared_mutex > mg{map_mu_};
        return miss_[idx].empty();
    }

private:
    static constexpr int64_t kNever = std::numeric_limits< int64_t >::max();
    static constexpr int64_t k_trunc_batch = 512;

    static uint64_t bit(std::size_t i) { return uint64_t{1} << i; }
    static uint64_t mask_of(std::size_t n) { return (n >= 64) ? ~uint64_t{0} : ((uint64_t{1} << n) - 1); }
    static bool overlaps(uint64_t a0, uint64_t l0, uint64_t a1, uint64_t l1) {
        return (a0 < a1 + l1) && (a1 < a0 + l0);
    }
    static boost::icl::interval< uint64_t >::type byte_ivl(uint64_t a, uint64_t l) {
        return boost::icl::interval< uint64_t >::right_open(a, a + l); // [a, a+l)
    }

    void advance_folded(int64_t target) {
        int64_t f = folded_.load(std::memory_order_relaxed);
        while (f < target &&
               !folded_.compare_exchange_weak(f, target, std::memory_order_release, std::memory_order_relaxed)) {}
    }
    void maybe_truncate(int64_t target) {
        int64_t lt = last_trunc_.load(std::memory_order_relaxed);
        if ((target - lt >= k_trunc_batch) &&
            last_trunc_.compare_exchange_strong(lt, target, std::memory_order_acq_rel, std::memory_order_relaxed)) {
            overlay_->truncate(target);
        }
    }

    // Real CRAFT replica sets are 3/5/7/9; size the per-member arrays for that, not the bitset's 64-wide max.
    // The holder bitset stays uint64 (trivially covers <= 9).
    static constexpr std::size_t k_max_members = 16;

    std::optional< sisl::StreamTracker< route_slot > > overlay_; // above the frontier: per-dLSN holder bitsets
    mutable std::shared_mutex map_mu_;              // guards miss_ (shared: eligible; exclusive: fold apply)
    std::array< missing_map, k_max_members > miss_; // below the frontier: per-member Missing byte ranges
    std::array< std::atomic< int64_t >, k_max_members >
        synced_{}; // per member: client's model of its commit_lsn (>= L)
    std::array< std::atomic< bool >, k_max_members >
        ka_inflight_{}; // per member: a keep_alive is outstanding (the one-per-leg collapse)

    std::mutex pending_mu_;                       // guards pending_
    std::vector< pending_miss > pending_;         // durable non-universal completed slots awaiting fold
    std::atomic< int64_t > pending_hint_{kNever}; // conservative min pending dLSN; the fold fast-path gate
    std::mutex fold_mu_;                          // serializes the fault-path fold apply (keeps it ascending)

    std::size_t n_{0};
    std::size_t quorum_{1};
    int64_t login_dlsn_{-1};                   // L: the Synced baseline for this session
    std::atomic< int64_t > all_committed_{-1}; // min synced_ across members; the journal reclaim floor
    std::atomic< int64_t > folded_{-1};        // every dLSN <= this is collapsed into the interval maps
    std::atomic< int64_t > last_trunc_{-1};    // gates batched truncation
    uint64_t all_mask_{0};
};

} // namespace homeblocks::craft
