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

// Unit tests for the in-memory CRAFT reference model, driving MemCraftReplica / MemTransport
// directly (no volume, no HomeStore, no iomgr reactor -- ops complete inline via detail::sync_get).
//
// The IO surface is BYTE-based: addr/len are byte offsets/lengths, block-aligned to the volume's
// lba_size (returned by login). sisl::sg_list is the one buffer type both ways (caller-owned): a WRITE
// takes a source buffer -- EMPTY (size 0) means a zero write (WRITE_ZEROES), non-empty is a data write.
// A READ fills a caller `dest` buffer (data -> bytes, holes -> zeros) and returns a std::vector<io_extent>
// byte layout marking which sub-ranges were data vs holes. Every IO carries a client_hdr {term,
// commit_lsn, all_committed_lsn}; commit_lsn (-1 = don't advance) piggybacks the frontier -- there is no
// standalone commit verb. keep_alive is the dedicated commit carrier and is term-fenced.

#include <chrono>
#include <cstdint>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <boost/uuid/uuid_generators.hpp>
#include <gtest/gtest.h>

#include "craft_test_util.hpp"
#include "model/mem_craft_cluster.hpp"

using namespace homeblocks;
using namespace homeblocks::craft;

using namespace homeblocks::craft::test;

namespace {
constexpr uint64_t TOKEN = 0xC0FFEEULL;
constexpr int64_t NO_COMMIT = -1; // commit_lsn sentinel: don't advance the frontier

volume_id_t new_vol() { return boost::uuids::random_generator()(); }

// Read `nblk` blocks starting at block `lba`, at horizon H (optionally piggybacking commit), into a
// fresh dest buffer pre-filled with 0xEE (so a hole read proves the model actually zeroed it).
struct read_out {
    bool ok{false};
    std::error_condition err{};
    std::vector< io_extent > layout; // which byte sub-ranges were data vs holes
    std::vector< uint8_t > data;     // the filled dest buffer
};
read_out rd(MemCraftReplica& r, uint64_t term, int64_t H, uint64_t lba, uint64_t nblk, int64_t commit = NO_COMMIT) {
    std::vector< uint8_t > dest(nblk * PAGE, 0xEE);
    auto rr = rg(r.read(chdr(term, commit), H, blk(lba), blk(nblk), one_iov(dest)));
    if (!rr.has_value()) return {false, rr.error(), {}, {}};
    return {true, {}, std::move(*rr), std::move(dest)};
}

// Log the leader in; assert success and return {term, dLSN}.
std::pair< uint64_t, int64_t > login_ok(MemReplicaGroup& g) {
    auto lr = rg(g.replicas[0]->login(TOKEN));
    EXPECT_TRUE(lr.has_value());
    return {lr->term, lr->dLSN};
}
} // namespace

// 1. login establishes a session across the set and reports the block size.
TEST(CraftMemModel, LoginEstablishesSession) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto lr = rg(g.replicas[0]->login(TOKEN));
    ASSERT_TRUE(lr.has_value());
    EXPECT_EQ(lr->members.size(), 3u);
    EXPECT_EQ(lr->term, 1u);
    EXPECT_EQ(lr->dLSN, -1);
    EXPECT_EQ(lr->lba_size, PAGE); // the client aligns addr/len to this
}

// login sent to a follower returns a redirect (term==0, leader_hint set) -- not an error.
TEST(CraftMemModel, LoginOnFollowerReturnsLeaderHint) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto lr = rg(g.replicas[1]->login(TOKEN));
    ASSERT_TRUE(lr.has_value());
    EXPECT_EQ(lr->term, 0u);                         // term==0 signals redirect
    EXPECT_EQ(lr->leader_hint, g.replicas[0]->id()); // replica 0 is the default leader
}

// 2. broadcast a write, advance the frontier via keep_alive, read it back.
TEST(CraftMemModel, WriteKeepAliveRead) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    ASSERT_EQ(dl, -1);
    auto buf = page_of(0xAB);
    for (auto& r : g.replicas) {
        ASSERT_TRUE(rg(r->write(chdr(term), 0, blk(5), blk(1), one_iov(buf))).has_value());
    }
    for (auto& r : g.replicas) { // keep_alive is the dedicated commit carrier; returns the achieved pair
        auto c = rg(r->keep_alive(chdr(term, /*commit_lsn*/ 0)));
        ASSERT_TRUE(c.has_value());
        EXPECT_EQ(c->commit_lsn, 0);
        EXPECT_EQ(c->last_append_lsn, 0);
    }
    auto out = rd(*g.replicas[1], term, /*H*/ 0, 5, 1);
    ASSERT_TRUE(out.ok);
    ASSERT_EQ(out.layout.size(), 1u);
    EXPECT_FALSE(out.layout[0].hole);
    EXPECT_EQ(out.layout[0].addr, blk(5));
    EXPECT_EQ(out.layout[0].len, blk(1));
    EXPECT_EQ(out.data, buf);
}

// 3. an appended-but-uncommitted entry is served from the journal-tail overlay, above a committed hole.
TEST(CraftMemModel, OverlayReadAboveCommittedHole) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    auto b0 = page_of(1), b1 = page_of(2);
    ASSERT_TRUE(rg(r.write(chdr(term), 0, blk(5), blk(1), one_iov(b0))).has_value());
    ASSERT_TRUE(rg(r.write(chdr(term), 1, blk(6), blk(1), one_iov(b1))).has_value());
    ASSERT_TRUE(rg(r.keep_alive(chdr(term, 0))).has_value()); // apply dLSN 0 only; dLSN 1 stays in overlay

    auto out = rd(r, term, /*H*/ 1, 5, 2); // block 5 applied, block 6 from overlay
    ASSERT_TRUE(out.ok);
    std::vector< uint8_t > want;
    want.insert(want.end(), b0.begin(), b0.end());
    want.insert(want.end(), b1.begin(), b1.end());
    ASSERT_EQ(out.layout.size(), 1u); // both data + contiguous -> one coalesced extent
    EXPECT_FALSE(out.layout[0].hole);
    EXPECT_EQ(out.layout[0].len, blk(2));
    EXPECT_EQ(out.data, want);

    auto below = rd(r, term, /*H*/ 0, 6, 1); // dLSN1 > H -> hole
    ASSERT_TRUE(below.ok);
    ASSERT_EQ(below.layout.size(), 1u);
    EXPECT_TRUE(below.layout[0].hole);
    EXPECT_EQ(below.data, page_of(0)); // dest was zeroed
}

// 4. the horizon clamp: a locally-held write above H is never returned.
TEST(CraftMemModel, HorizonClampHidesHeldWrite) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    auto b5 = page_of(9);
    ASSERT_TRUE(rg(r.write(chdr(term), 5, blk(10), blk(1), one_iov(b5))).has_value()); // sub-quorum tail

    auto clamped = rd(r, term, /*H*/ 4, 10, 1);
    ASSERT_TRUE(clamped.ok);
    ASSERT_EQ(clamped.layout.size(), 1u);
    EXPECT_TRUE(clamped.layout[0].hole); // dLSN 5 > H=4
    EXPECT_EQ(clamped.data, page_of(0));

    auto seen = rd(r, term, /*H*/ 5, 10, 1);
    ASSERT_TRUE(seen.ok);
    ASSERT_EQ(seen.layout.size(), 1u);
    EXPECT_FALSE(seen.layout[0].hole);
    EXPECT_EQ(seen.data, b5);
}

// 5. a zero (empty-buffer) write reads back as a hole after its frontier passes (range unmap).
TEST(CraftMemModel, ZeroWriteReadsAsHole) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    auto data = page_of(7);
    ASSERT_TRUE(rg(r.write(chdr(term), 0, blk(7), blk(1), one_iov(data))).has_value());
    ASSERT_TRUE(rg(r.keep_alive(chdr(term, 0))).has_value());
    EXPECT_FALSE(rd(r, term, 0, 7, 1).layout[0].hole); // data present

    // empty sg_list => zero write. `len` (blk(1)) is the authoritative range to unmap.
    ASSERT_TRUE(rg(r.write(chdr(term), 1, blk(7), blk(1), sisl::sg_list{})).has_value());
    ASSERT_TRUE(rg(r.keep_alive(chdr(term, 1))).has_value());
    auto out = rd(r, term, 1, 7, 1);
    ASSERT_TRUE(out.ok);
    ASSERT_EQ(out.layout.size(), 1u);
    EXPECT_TRUE(out.layout[0].hole); // unmapped -> hole
    EXPECT_EQ(out.data, page_of(0));
}

// 6. a DATA write of all-zero bytes reads back as a hole via the read-time scan.
TEST(CraftMemModel, AllZeroDataCollapsesToHole) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    auto zeros = page_of(0); // all-zero bytes, written as a normal (non-empty) data write
    ASSERT_TRUE(rg(r.write(chdr(term), 0, blk(8), blk(1), one_iov(zeros))).has_value());
    ASSERT_TRUE(rg(r.keep_alive(chdr(term, 0))).has_value());
    auto out = rd(r, term, 0, 8, 1);
    ASSERT_TRUE(out.ok);
    ASSERT_EQ(out.layout.size(), 1u);
    EXPECT_TRUE(out.layout[0].hole);
}

// 7. commit is best-effort: keep_alive stalls just below the first missing slot, while the overlay
//    still serves the higher write.
TEST(CraftMemModel, CommitStallsAtGap) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    auto buf = page_of(3);
    ASSERT_TRUE(rg(r.write(chdr(term), 0, blk(5), blk(1), one_iov(buf))).has_value());
    ASSERT_TRUE(rg(r.write(chdr(term), 2, blk(6), blk(1), one_iov(buf))).has_value()); // gap at dLSN 1

    auto c = rg(r.keep_alive(chdr(term, 2)));
    ASSERT_TRUE(c.has_value());
    EXPECT_EQ(c->commit_lsn, 0); // stalled below the missing dLSN 1
    EXPECT_EQ(c->last_append_lsn, 2);

    auto out = rd(r, term, /*H*/ 2, 6, 1); // dLSN 2 still readable via the overlay
    ASSERT_TRUE(out.ok);
    EXPECT_FALSE(out.layout[0].hole);
    EXPECT_EQ(out.data, buf);
}

// 8. commit piggybacks on a WRITE: a later write carrying commit_lsn applies the earlier entry.
TEST(CraftMemModel, CommitPiggybacksOnWrite) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    auto b0 = page_of(0x11), b1 = page_of(0x22);
    ASSERT_TRUE(rg(r.write(chdr(term), 0, blk(5), blk(1), one_iov(b0))).has_value()); // no commit yet
    ASSERT_TRUE(
        rg(r.write(chdr(term, /*commit_lsn*/ 0), 1, blk(6), blk(1), one_iov(b1))).has_value()); // rides commit 0

    auto ls = rg(r.get_lsns());
    ASSERT_TRUE(ls.has_value());
    EXPECT_EQ(ls->commit_lsn, 0); // dLSN 0 applied via the piggyback on the dLSN-1 write
    EXPECT_EQ(ls->last_append_lsn, 1);
    auto out = rd(r, term, 0, 5, 1); // served from the index (applied), not the overlay
    ASSERT_TRUE(out.ok);
    EXPECT_FALSE(out.layout[0].hole);
    EXPECT_EQ(out.data, b0);
}

// 9. commit piggybacks on a READ: reading with commit_lsn advances the frontier as a side-effect.
TEST(CraftMemModel, CommitPiggybacksOnRead) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    auto buf = page_of(0x33);
    ASSERT_TRUE(rg(r.write(chdr(term), 0, blk(5), blk(1), one_iov(buf))).has_value());
    ASSERT_EQ(rg(r.get_lsns())->commit_lsn, -1); // not committed yet

    auto out = rd(r, term, /*H*/ 0, 5, 1, /*commit_lsn*/ 0); // read advances the frontier to 0
    ASSERT_TRUE(out.ok);
    EXPECT_FALSE(out.layout[0].hole);
    EXPECT_EQ(out.data, buf);
    EXPECT_EQ(rg(r.get_lsns())->commit_lsn, 0);
}

// 10. term fencing on write: an IO whose term != the session term is rejected (the protocol's ETERM).
TEST(CraftMemModel, WriteTermFencing) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    auto buf = page_of(1);
    auto bad = rg(r.write(chdr(term + 1), 0, blk(5), blk(1), one_iov(buf)));
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error(), make_error_condition(craft_error::STALE_TERM));
    EXPECT_TRUE(rg(r.write(chdr(term), 0, blk(5), blk(1), one_iov(buf))).has_value());
}

// 11. keep_alive is ALSO term-fenced -- a stale client must not reset the liveness watchdog.
TEST(CraftMemModel, KeepAliveIsTermFenced) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    auto stale = rg(r.keep_alive(chdr(term + 1, 0)));
    ASSERT_FALSE(stale.has_value());
    EXPECT_EQ(stale.error(), make_error_condition(craft_error::STALE_TERM));
    ASSERT_TRUE(rg(r.keep_alive(chdr(term, -1))).has_value()); // current term is accepted
}

// 12. misaligned addr/len are rejected server-side with invalid_argument.
TEST(CraftMemModel, MisalignedIoRejected) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    auto buf = page_of(1);
    auto const einval = std::make_error_condition(std::errc::invalid_argument);

    auto bad_addr = rg(r.write(chdr(term), 0, /*addr*/ 1, blk(1), one_iov(buf)));
    ASSERT_FALSE(bad_addr.has_value());
    EXPECT_EQ(bad_addr.error(), einval);

    auto bad_len = rg(r.write(chdr(term), 0, blk(5), /*len*/ 100, one_iov(buf)));
    ASSERT_FALSE(bad_len.has_value());
    EXPECT_EQ(bad_len.error(), einval);

    std::vector< uint8_t > dest(PAGE);
    auto bad_read = rg(r.read(chdr(term), 0, /*addr*/ 3, blk(1), one_iov(dest)));
    ASSERT_FALSE(bad_read.has_value());
    EXPECT_EQ(bad_read.error(), einval);
}

// 13a. fault hook: a downed replica fails addressed I/O; the rest keep a quorum.
TEST(CraftMemModel, ReplicaDownFault) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto buf = page_of(1);
    g.net->set_up(g.replicas[1]->id(), false);
    auto down = rg(g.replicas[1]->write(chdr(term), 0, blk(5), blk(1), one_iov(buf)));
    ASSERT_FALSE(down.has_value());
    EXPECT_EQ(down.error(), make_error_condition(craft_error::REPLICA_DOWN));
    EXPECT_TRUE(rg(g.replicas[0]->write(chdr(term), 0, blk(5), blk(1), one_iov(buf))).has_value());
    EXPECT_TRUE(rg(g.replicas[2]->write(chdr(term), 0, blk(5), blk(1), one_iov(buf))).has_value());
    g.net->set_up(g.replicas[1]->id(), true);
}

// 13b. fault hook: force_subquorum makes a broadcast land on only a minority (< quorum acks).
TEST(CraftMemModel, SubQuorumFault) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto buf = page_of(1);
    g.net->force_subquorum({g.replicas[0]->id()}); // only replica 0 accepts the next writes
    int acks = 0;
    for (auto& r : g.replicas) {
        if (rg(r->write(chdr(term), 0, blk(5), blk(1), one_iov(buf))).has_value()) ++acks;
    }
    EXPECT_EQ(acks, 1); // minority: sub-quorum, not durable
    g.net->clear_faults();
    acks = 0;
    for (auto& r : g.replicas) {
        if (rg(r->write(chdr(term), 1, blk(5), blk(1), one_iov(buf))).has_value()) ++acks;
    }
    EXPECT_EQ(acks, 3); // all members accept again
}

// 14. explicit logout clears the session on all replicas; subsequent IOs fail with STALE_TERM.
TEST(CraftMemModel, LogoutClearsSession) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    ASSERT_TRUE(rg(g.replicas[0]->logout(chdr(term))).has_value());

    // All replicas should now reject the old term.
    auto buf = page_of(1);
    for (auto& r : g.replicas) {
        auto bad = rg(r->write(chdr(term), 0, blk(5), blk(1), one_iov(buf)));
        ASSERT_FALSE(bad.has_value());
        EXPECT_EQ(bad.error(), make_error_condition(craft_error::STALE_TERM));
    }
}

// 15. logout from a follower is rejected with NOT_LEADER.
TEST(CraftMemModel, LogoutOnFollowerReturnsNotLeader) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto bad = rg(g.replicas[1]->logout(chdr(term)));
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error(), make_error_condition(craft_error::NOT_LEADER));
}

// 16. logout is term-fenced: a stale client cannot tear down an active session.
TEST(CraftMemModel, LogoutIsTermFenced) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto stale = rg(g.replicas[0]->logout(chdr(term + 1)));
    ASSERT_FALSE(stale.has_value());
    EXPECT_EQ(stale.error(), make_error_condition(craft_error::STALE_TERM));
    // Session is still alive; a write with the real term still works.
    auto buf = page_of(1);
    EXPECT_TRUE(rg(g.replicas[0]->write(chdr(term), 0, blk(5), blk(1), one_iov(buf))).has_value());
}

// --- latency injection: the straggler replica ---

// Poll `fn` until it holds or `budget` elapses. Delays are wall-clock, so assert on the reached state
// rather than on a sleep being long enough.
template < class Fn >
bool eventually(Fn fn, std::chrono::milliseconds budget = std::chrono::seconds{5}) {
    auto const deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        if (fn()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds{2});
    }
    return fn();
}

// 20. A delay below the timeout just makes the op slow: it still succeeds, and it still lands.
TEST(CraftMemModel, DelayBelowTimeoutMerelySlowsTheOp) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    g.net->set_op_timeout(std::chrono::milliseconds{500});
    g.net->set_delay(r.id(), std::chrono::milliseconds{20});

    auto buf = page_of(9);
    auto const t0 = std::chrono::steady_clock::now();
    ASSERT_TRUE(rg(r.write(chdr(term), 0, blk(1), blk(1), one_iov(buf))).has_value());
    EXPECT_GE(std::chrono::steady_clock::now() - t0, std::chrono::milliseconds{20});
    EXPECT_EQ(r.stats().last_append_lsn, 0);
}

// 21. A delay PAST the transport's timeout: the caller is told timed_out, but the peer still receives and
//     applies the write. This is what an RPC deadline does, and it is what lets a later write land first.
TEST(CraftMemModel, DelayPastTimeoutTimesOutButStillLandsLater) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    g.net->set_op_timeout(std::chrono::milliseconds{10});
    g.net->set_delay(r.id(), std::chrono::milliseconds{150});

    auto buf = page_of(9);
    auto const w = rg(r.write(chdr(term), 0, blk(1), blk(1), one_iov(buf)));
    ASSERT_FALSE(w.has_value());
    EXPECT_EQ(w.error(), std::make_error_condition(std::errc::timed_out));
    EXPECT_EQ(r.stats().journal_slots, 0u) << "not delivered yet";

    // The peer processed it anyway, once the full delay elapsed.
    EXPECT_TRUE(eventually([&] { return r.stats().journal_slots == 1u; }));
    EXPECT_EQ(r.stats().last_append_lsn, 0);
}

// 22. The payoff: a write timed out while delayed lands AFTER a later write issued once the delay is
//     cleared, so the straggler's journal fills out of order and a Missing slot appears -- then drains
//     when the late write finally arrives. force_subquorum can never show the drain: it never delivers.
TEST(CraftMemModel, ClearingADelayLeavesAMissingSlotThatDrains) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    g.net->set_op_timeout(std::chrono::milliseconds{10});

    auto buf = page_of(9);
    ASSERT_TRUE(rg(r.write(chdr(term), 0, blk(1), blk(1), one_iov(buf))).has_value()); // dLSN 0 lands now

    g.net->set_delay(r.id(), std::chrono::milliseconds{300});
    auto const slow = rg(r.write(chdr(term), 1, blk(2), blk(1), one_iov(buf))); // dLSN 1 times out
    ASSERT_FALSE(slow.has_value());

    g.net->set_delay(r.id(), std::chrono::milliseconds{0});                            // straggler recovers
    ASSERT_TRUE(rg(r.write(chdr(term), 2, blk(3), blk(1), one_iov(buf))).has_value()); // dLSN 2 lands now
    ASSERT_TRUE(rg(r.keep_alive(chdr(term, 2))).has_value());

    { // dLSN 1 is still in the transport: the journal has a hole and the frontier is pinned under it.
        auto const s = r.stats();
        EXPECT_EQ(s.commit_lsn, 0);
        EXPECT_EQ(s.last_append_lsn, 2);
        EXPECT_EQ(s.missing_count, 1u);
        EXPECT_EQ(s.missing_sample, std::vector< int64_t >{1});
    }

    // It arrives; the gap closes. A commit carrier is what actually advances the frontier over it.
    ASSERT_TRUE(eventually([&] { return r.stats().missing_count == 0u; }));
    ASSERT_TRUE(rg(r.keep_alive(chdr(term, 2))).has_value());
    auto const s = r.stats();
    EXPECT_EQ(s.journal_slots, 3u);
    EXPECT_EQ(s.commit_lsn, 2) << "frontier catches up once the hole is filled";
    EXPECT_EQ(s.mapped_blocks, 3u);
}

// --- stats(): the observability snapshot behind craft_ublk's REST endpoint ---

// 17. Missing slots are the dLSNs in (commit_lsn, last_append_lsn] with no journal entry -- exactly what
//     apply_up_to stalls on. Here dLSN 1 never arrives, so the frontier pins at 0 while the tail runs to 3.
TEST(CraftMemModel, StatsReportsMissingSlots) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    auto buf = page_of(3);
    ASSERT_TRUE(rg(r.write(chdr(term), 0, blk(5), blk(1), one_iov(buf))).has_value());
    ASSERT_TRUE(rg(r.write(chdr(term), 2, blk(6), blk(1), one_iov(buf))).has_value()); // gap at dLSN 1
    ASSERT_TRUE(rg(r.write(chdr(term), 3, blk(7), blk(1), one_iov(buf))).has_value());
    ASSERT_TRUE(rg(r.keep_alive(chdr(term, 3))).has_value());

    auto const s = r.stats();
    EXPECT_EQ(s.id, r.id());
    EXPECT_EQ(s.page_size, PAGE);
    EXPECT_EQ(s.term, term);
    EXPECT_EQ(s.client_token, TOKEN);

    EXPECT_EQ(s.commit_lsn, 0); // stalled below the missing dLSN 1
    EXPECT_EQ(s.last_append_lsn, 3);
    EXPECT_EQ(s.missing_count, 1u);
    EXPECT_EQ(s.missing_sample, std::vector< int64_t >{1});

    EXPECT_EQ(s.journal_slots, 3u); // 0, 2, 3 -- nothing reclaims below the frontier here
    EXPECT_EQ(s.journal_first_dlsn, 0);
    EXPECT_EQ(s.journal_last_dlsn, 3);
    EXPECT_EQ(s.journal_data_bytes, 3ull * PAGE);
    EXPECT_EQ(s.mapped_blocks, 1u); // only dLSN 0 (block 5) was applied
}

// 18. The count stays exact past the sample cap: a wide gap must not make stats() walk the whole range.
TEST(CraftMemModel, StatsMissingSampleIsCappedButCountIsExact) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];
    auto buf = page_of(3);
    ASSERT_TRUE(rg(r.write(chdr(term), 0, blk(5), blk(1), one_iov(buf))).has_value());
    ASSERT_TRUE(rg(r.write(chdr(term), 100, blk(6), blk(1), one_iov(buf))).has_value());
    ASSERT_TRUE(rg(r.keep_alive(chdr(term, 100))).has_value());

    auto const s = r.stats();
    EXPECT_EQ(s.commit_lsn, 0);
    EXPECT_EQ(s.last_append_lsn, 100);
    EXPECT_EQ(s.missing_count, 99u); // dLSN 1..99
    ASSERT_EQ(s.missing_sample.size(), MemCraftReplica::k_missing_sample);
    EXPECT_EQ(s.missing_sample.front(), 1);
    EXPECT_EQ(s.missing_sample.back(), static_cast< int64_t >(MemCraftReplica::k_missing_sample));
}

// 19. A zero write is a journal slot that carries no bytes and unmaps its range on apply, so it shows up in
//     zero_write_slots, contributes nothing to journal_data_bytes, and shrinks mapped_blocks.
TEST(CraftMemModel, StatsCountsZeroWritesAndMappedBlocks) {
    auto g = make_mem_replica_group(new_vol(), 3, PAGE);
    auto [term, dl] = login_ok(g);
    auto& r = *g.replicas[0];

    auto data = page_of(7, 2); // blocks 4 and 5
    ASSERT_TRUE(rg(r.write(chdr(term), 0, blk(4), blk(2), one_iov(data))).has_value());
    ASSERT_TRUE(rg(r.keep_alive(chdr(term, 0))).has_value());
    {
        auto const s = r.stats();
        EXPECT_EQ(s.commit_lsn, 0);
        EXPECT_EQ(s.missing_count, 0u);
        EXPECT_EQ(s.zero_write_slots, 0u);
        EXPECT_EQ(s.mapped_blocks, 2u);
        EXPECT_EQ(s.journal_data_bytes, 2ull * PAGE);
    }

    ASSERT_TRUE(rg(r.write(chdr(term), 1, blk(4), blk(1), sisl::sg_list{})).has_value()); // zero write
    ASSERT_TRUE(rg(r.keep_alive(chdr(term, 1))).has_value());

    auto const s = r.stats();
    EXPECT_EQ(s.commit_lsn, 1);
    EXPECT_EQ(s.journal_slots, 2u);
    EXPECT_EQ(s.zero_write_slots, 1u);
    EXPECT_EQ(s.empty_slots, 0u);
    EXPECT_EQ(s.journal_data_bytes, 2ull * PAGE); // the zero write adds none
    EXPECT_EQ(s.mapped_blocks, 1u);               // block 4 unmapped; block 5 remains
}
