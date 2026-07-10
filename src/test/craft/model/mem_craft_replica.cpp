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

#include "mem_craft_replica.hpp"
#include "mem_craft_cluster.hpp" // the full MemTransport type

#include <algorithm>
#include <cstring>
#include <iterator>

namespace homeblocks::craft {

namespace {
bool all_zero(uint8_t const* p, std::size_t n) {
    for (std::size_t i = 0; i < n; ++i) {
        if (p[i] != 0) return false;
    }
    return true;
}
auto fail(craft_error e) { return std::unexpected(make_error_condition(e)); }
} // namespace

MemCraftReplica::MemCraftReplica(replica_endpoint ep, uint32_t page_size, std::shared_ptr< MemTransport > net) :
        ep_{std::move(ep)}, page_size_{page_size}, net_{std::move(net)} {}

// ── async wrappers: all real work is synchronous, so these complete inline (no reactor) ──

async_result< LoginResult > MemCraftReplica::login(uint64_t client_token) {
    if (!net_) co_return fail(craft_error::NO_QUORUM);
    if (!net_->is_up(ep_.id)) co_return fail(craft_error::REPLICA_DOWN);
    co_return net_->run_login(this, client_token);
}
async_status MemCraftReplica::logout(client_hdr hdr) {
    if (net_ && !net_->is_up(ep_.id)) co_return fail(craft_error::REPLICA_DOWN);
    {
        std::lock_guard< std::mutex > g{mu_};
        if (hdr.term != state_.term) co_return fail(craft_error::STALE_TERM);
    }
    co_return net_ ? net_->run_logout(this, hdr.term) : ok();
}
// Every client-facing op crosses the wire (MemTransport), which owns the payload, decides deliverability, and
// imposes latency. What is left below is the server: journal + index. Nothing here copies bytes.
async_status MemCraftReplica::write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len, sisl::sg_list data) {
    if (!net_) co_return fail(craft_error::NO_QUORUM); // no wire, nothing to deliver over
    co_return co_await net_->send_write(shared_from_this(), hdr, dlsn, addr, len, std::move(data));
}
async_result< std::vector< io_extent > > MemCraftReplica::read(client_hdr hdr, int64_t read_lsn, uint64_t addr,
                                                               uint64_t len, sisl::sg_list dest) {
    if (!net_) co_return fail(craft_error::NO_QUORUM);
    co_return co_await net_->send_read(shared_from_this(), hdr, read_lsn, addr, len, std::move(dest));
}
async_result< LSNPair > MemCraftReplica::keep_alive(client_hdr hdr) {
    if (!net_) co_return fail(craft_error::NO_QUORUM);
    co_return co_await net_->send_keep_alive(shared_from_this(), hdr);
}
async_result< LSNPair > MemCraftReplica::get_lsns() { co_return do_lsns(); }
async_result< LSNPair > MemCraftReplica::get_rs_commit_lsn() { co_return do_lsns(); }
async_result< std::vector< JournalSlot > > MemCraftReplica::fetch_data(std::vector< int64_t > lsns) {
    co_return do_fetch(lsns);
}
async_status MemCraftReplica::truncate(int64_t lsn) { co_return do_truncate(lsn); }

// ── synchronous cores ──

// Takes the payload ALREADY owned: write() copied it exactly once, at issue. The journal slot adopts that
// buffer, so nothing here copies bytes -- this is the replica persisting what the transport handed it.
// `bytes == nullptr` is a zero write (WRITE_ZEROES), which allocates nothing.
status MemCraftReplica::do_write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                 std::shared_ptr< std::vector< uint8_t > > bytes) {
    // Deliverability is the transport's verdict, not ours: by the time we are called, the request arrived.
    // byte-based API: addr/len must be block-aligned (the model works in page_size blocks internally).
    if (addr % page_size_ != 0 || len % page_size_ != 0 || len == 0) {
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    if (bytes && (bytes->size() != len)) {
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    std::lock_guard< std::mutex > g{mu_};
    if (hdr.term != state_.term) return fail(craft_error::STALE_TERM);

    MemJournalSlot slot;
    slot.term = hdr.term;
    slot.lba = addr / page_size_;                            // byte offset -> block index
    slot.len = static_cast< lba_count_t >(len / page_size_); // byte length -> block count
    slot.all_zeros = !bytes;                                 // no payload => zero write; no all_zeros flag
    slot.bytes = std::move(bytes);                           // adopt the buffer; do not copy it again
    journal_[dlsn] = std::move(slot);
    state_.last_append_lsn = std::max(state_.last_append_lsn, dlsn);
    apply_up_to(hdr.commit_lsn); // piggybacked commit: advance the frontier best-effort, in dLSN order
    return ok();
}

result< std::vector< io_extent > > MemCraftReplica::do_read(client_hdr hdr, int64_t read_lsn, uint64_t addr,
                                                            uint64_t len, sisl::sg_list dest) {
    // byte-based API: addr/len block-aligned; dest is a single contiguous buffer covering [addr,addr+len)
    if (addr % page_size_ != 0 || len % page_size_ != 0 || len == 0) {
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    if (dest.size < len) { return std::unexpected(std::make_error_condition(std::errc::invalid_argument)); }
    std::lock_guard< std::mutex > g{mu_};
    if (hdr.term != state_.term) return fail(craft_error::STALE_TERM);
    apply_up_to(hdr.commit_lsn);                           // piggybacked commit: advance the frontier opportunistically
    reads_served_.fetch_add(1, std::memory_order_relaxed); // test observability: witness read routing
    return read_range(read_lsn, addr, len, dest);
}

result< LSNPair > MemCraftReplica::do_keep_alive(client_hdr hdr) {
    std::lock_guard< std::mutex > g{mu_};
    // Term-fenced: a stale client must NOT reset the liveness watchdog (that would block failover).
    if (hdr.term != state_.term) return fail(craft_error::STALE_TERM);
    apply_up_to(hdr.commit_lsn);
    // watchdog reset + journal reclaim below min(hdr.all_committed_lsn, commit_lsn) are deferred seams.
    return LSNPair{state_.commit_lsn, state_.last_append_lsn};
}

result< LSNPair > MemCraftReplica::do_lsns() {
    if (net_ && !net_->is_up(ep_.id)) return fail(craft_error::REPLICA_DOWN);
    std::lock_guard< std::mutex > g{mu_};
    return LSNPair{state_.commit_lsn, state_.last_append_lsn};
}

status MemCraftReplica::do_truncate(int64_t lsn) {
    std::lock_guard< std::mutex > g{mu_};
    journal_.erase(journal_.upper_bound(lsn), journal_.end());
    state_.last_append_lsn = std::min(state_.last_append_lsn, lsn);
    return ok();
}

result< std::vector< JournalSlot > > MemCraftReplica::do_fetch(std::vector< int64_t > const& lsns) {
    std::lock_guard< std::mutex > g{mu_};
    std::vector< JournalSlot > out;
    for (auto lsn : lsns) {
        auto it = journal_.find(lsn);
        if (it == journal_.end()) continue; // not-present-here => omit
        auto const& s = it->second;
        JournalSlot js;
        js.lsn = lsn;
        js.is_empty = s.is_empty;
        js.all_zeros = s.all_zeros;
        js.lba = s.lba;
        js.len = s.len;
        if (!s.all_zeros && !s.is_empty && s.bytes) {
            // seam: the sg_list points into the slot's owned buffer (valid while the slot lives).
            js.data.size = s.bytes->size();
            js.data.iovs.push_back(iovec{s.bytes->data(), s.bytes->size()});
        }
        out.push_back(std::move(js));
    }
    return out;
}

// ── apply / read helpers (mu_ held) ──

void MemCraftReplica::apply_slot(int64_t dlsn, MemJournalSlot const& s) {
    if (s.all_zeros) {
        for (lba_count_t i = 0; i < s.len; ++i) {
            index_.erase(s.lba + i); // unmap => hole
        }
    } else {
        for (lba_count_t i = 0; i < s.len; ++i) {
            index_[s.lba + i] = IndexCell{dlsn, s.bytes, static_cast< std::size_t >(i) * page_size_};
        }
    }
}

void MemCraftReplica::apply_up_to(int64_t target) {
    int64_t next = state_.commit_lsn + 1;
    while (next <= target) {
        auto it = journal_.find(next);
        if (it == journal_.end()) break; // Missing hole -> stall (best-effort)
        if (!it->second.is_empty) apply_slot(next, it->second);
        state_.commit_lsn = next; // Empty slots are skipped on apply but still advance the frontier
        ++next;
    }
}

// Highest-dLSN journal-tail slot with commit_lsn < dLSN <= H that covers `x` (the journal-tail overlay,
// materialized on demand). Slots above H are never examined -- that is the horizon clamp.
MemCraftReplica::MemJournalSlot const* MemCraftReplica::highest_slot_le(lba_t x, int64_t H) const {
    for (auto it = journal_.upper_bound(H); it != journal_.begin();) {
        --it;
        if (it->first <= state_.commit_lsn) break; // reached the applied prefix (served from index_)
        auto const& s = it->second;
        if (s.is_empty) continue;
        if (s.lba <= x && x < s.lba + s.len) return &s;
    }
    return nullptr;
}

std::vector< io_extent > MemCraftReplica::read_range(int64_t H, uint64_t addr, uint64_t len,
                                                     sisl::sg_list const& dest) {
    lba_t const lba0 = addr / page_size_;
    lba_count_t const nblk = static_cast< lba_count_t >(len / page_size_);
    std::vector< io_extent > layout;

    // Scatter writer: advances through dest's iovecs sequentially, one page_size_ chunk at a time.
    std::size_t iov_idx{0}, iov_off{0};
    auto sg_write = [&](uint8_t const* src, std::size_t n) {
        while (n > 0 && iov_idx < dest.iovs.size()) {
            auto const& iov = dest.iovs[iov_idx];
            std::size_t avail = iov.iov_len - iov_off;
            std::size_t to_write = std::min(n, avail);
            auto* dst = static_cast< uint8_t* >(iov.iov_base) + iov_off;
            if (src) {
                std::memcpy(dst, src, to_write);
                src += to_write;
            } else {
                std::memset(dst, 0, to_write);
            }
            n -= to_write;
            iov_off += to_write;
            if (iov_off == iov.iov_len) {
                ++iov_idx;
                iov_off = 0;
            }
        }
    };

    for (lba_count_t i = 0; i < nblk; ++i) {
        lba_t const x = lba0 + i;
        // Winner = highest-dLSN version <= H covering block x, between the applied index cell and the
        // journal tail. A tail slot's dLSN is always > commit_lsn >= any applied cell's, so the tail wins.
        uint8_t const* page = nullptr;
        bool hole = true;
        if (auto* s = highest_slot_le(x, H)) {
            if (!s->all_zeros) {
                page = s->bytes->data() + static_cast< std::size_t >(x - s->lba) * page_size_;
                hole = false;
            } // else: zero write => hole
        } else if (auto it = index_.find(x); it != index_.end()) {
            page = it->second.buf->data() + it->second.off;
            hole = false;
        }
        // read-time scan: an all-zero data page reads back thin (as a hole).
        if (!hole && all_zero(page, page_size_)) hole = true;

        // fill the caller's scatter-gather buffer: data pages get bytes, holes get zeros.
        sg_write(hole ? nullptr : page, page_size_);

        // coalesce the returned layout, in BYTES, with the previous extent if contiguous.
        uint64_t const x_addr = static_cast< uint64_t >(x) * page_size_;
        if (!layout.empty() && layout.back().hole == hole && layout.back().addr + layout.back().len == x_addr) {
            layout.back().len += page_size_;
        } else {
            layout.push_back(io_extent{x_addr, page_size_, hole});
        }
    }
    return layout;
}

// ── observability ──

replica_stats MemCraftReplica::stats() const {
    std::lock_guard< std::mutex > g{mu_};

    replica_stats s;
    s.id = ep_.id;
    s.addr = ep_.addr;
    s.page_size = page_size_;
    s.commit_lsn = state_.commit_lsn;
    s.last_append_lsn = state_.last_append_lsn;
    s.term = state_.term;
    s.client_token = state_.client_token;
    s.mapped_blocks = index_.size();

    s.journal_slots = journal_.size();
    if (!journal_.empty()) {
        s.journal_first_dlsn = journal_.begin()->first;
        s.journal_last_dlsn = journal_.rbegin()->first;
    }
    for (auto const& [dlsn, slot] : journal_) {
        if (slot.is_empty) {
            ++s.empty_slots;
        } else if (slot.all_zeros) {
            ++s.zero_write_slots;
        } else {
            s.journal_data_bytes += static_cast< uint64_t >(slot.len) * page_size_;
        }
    }

    // Missing = the dLSNs in (commit_lsn, last_append_lsn] with no journal entry. Count them by
    // subtraction rather than by walking the range: the uncommitted tail can be arbitrarily wide, while
    // the slots actually present in it are bounded by the client's in-flight window.
    int64_t const lo = state_.commit_lsn + 1;
    int64_t const hi = state_.last_append_lsn;
    if (hi >= lo) {
        auto const first = journal_.lower_bound(lo);
        auto const last = journal_.upper_bound(hi);
        auto const present = static_cast< std::size_t >(std::distance(first, last));
        s.missing_count = static_cast< std::size_t >(hi - lo + 1) - present;

        // Sample the gaps in one pass over the same range, bounded so a missing tail cannot run long.
        int64_t expect = lo;
        for (auto it = first; it != last && s.missing_sample.size() < k_missing_sample; ++it) {
            for (; expect < it->first && s.missing_sample.size() < k_missing_sample; ++expect) {
                s.missing_sample.push_back(expect);
            }
            expect = it->first + 1;
        }
        for (; expect <= hi && s.missing_sample.size() < k_missing_sample; ++expect) {
            s.missing_sample.push_back(expect);
        }
    }
    return s;
}

// ── cold-path hooks (driven by MemTransport, which does NOT hold its own lock while calling these) ──

LSNPair MemCraftReplica::peek_lsns() {
    std::lock_guard< std::mutex > g{mu_};
    return LSNPair{state_.commit_lsn, state_.last_append_lsn};
}
void MemCraftReplica::cold_apply_sync(int64_t rs_commit_lsn, uint64_t /*client_token*/) {
    std::lock_guard< std::mutex > g{mu_};
    // SyncRSCommitLSN: (seam) fetch any missing non-Empty slots <= rs from peers, then advance commit.
    apply_up_to(rs_commit_lsn);
}
void MemCraftReplica::cold_apply_login(uint64_t client_token, uint64_t term) {
    std::lock_guard< std::mutex > g{mu_};
    state_.client_token = client_token;
    state_.term = term;
}
void MemCraftReplica::cold_apply_logout() {
    std::lock_guard< std::mutex > g{mu_};
    state_.client_token = 0;
    state_.term = 0; // no active session; subsequent IOs with old term fail STALE_TERM
}
void MemCraftReplica::cold_truncate_above(int64_t rs_commit_lsn) {
    std::lock_guard< std::mutex > g{mu_};
    journal_.erase(journal_.upper_bound(rs_commit_lsn), journal_.end());
    state_.last_append_lsn = std::min(state_.last_append_lsn, rs_commit_lsn);
}

} // namespace homeblocks::craft
