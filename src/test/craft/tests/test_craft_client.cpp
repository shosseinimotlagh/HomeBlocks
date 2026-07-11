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

// Unit test for craft_client -- the client-side CRAFT sequencing (dLSN assignment, broadcast + quorum,
// commit frontier, read horizon) the ublk driver drives. It runs against the in-memory reference model
// via make_memory_replica_set, with NO ublk device and NO iomgr, so it is a fast CI check of the
// sequencing logic before any /dev/ublkbN validation. n=1 is the current stepping stone; n=3 (with
// force_subquorum) exercises the quorum path the next step builds on.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include <boost/uuid/uuid_generators.hpp>
#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homeblks/home_blocks.hpp>

#include "craft_test_util.hpp"
#include "model/mem_craft_volume.hpp" // make_memory_replica_set, mem_replica_id (via mem_craft_cluster.hpp)
#include "client/craft_client.hpp"
#include "client/read_route_map.hpp"

SISL_LOGGING_DEF(homeblocks)
SISL_LOGGING_INIT(homeblocks)
SISL_OPTIONS_ENABLE(logging)

using namespace homeblocks;

using namespace homeblocks::craft::test;

namespace {
constexpr uint64_t TOKEN = 0xABCDEFULL;

struct Cluster {
    craft::MemReplicaHandles set;
    volume_id_t vid;
    std::shared_ptr< craft::craft_client > client;
};

// Stand up an n-replica in-memory set + a client over it (the client copies the handle vector, so `set`
// retains net for fault injection), and log in. Asserts the login succeeds.
Cluster make_cluster(uint32_t n, uint32_t leader = 0) {
    volume_info info;
    info.id = boost::uuids::random_generator()();
    info.size_bytes = 1ULL << 20;
    info.page_size = PAGE;
    info.name = "craft-client-test";
    info.repl_mode = replication_mode::CRAFT;
    volume_id_t const vid = info.id;

    auto set = craft::make_memory_replica_set(std::move(info), n);
    auto client = std::make_shared< craft::craft_client >(set.handles, leader); // copy handles; set keeps net
    EXPECT_TRUE(rg(client->login(TOKEN)).has_value());
    EXPECT_EQ(client->lba_size(), PAGE);
    return Cluster{std::move(set), vid, std::move(client)};
}

// convenience wrappers
bool wr(craft::craft_client& c, uint64_t off_blk, std::vector< uint8_t >& buf) {
    return rg(c.write(blk(off_blk), buf.size(), one_iov(buf))).has_value();
}
std::vector< uint8_t > rd(craft::craft_client& c, uint64_t off_blk, uint64_t nblk = 1) {
    std::vector< uint8_t > dest(nblk * PAGE, 0xEE);
    auto r = rg(c.read(blk(off_blk), nblk * PAGE, one_iov(dest)));
    EXPECT_TRUE(r.has_value());
    return dest;
}

// Only the leader accepts the write, so it journals the slot while quorum(3)=2 goes unmet: the slot stays
// unresolved and the leader physically holds it.
void write_subquorum(Cluster& cl, uint64_t off_blk, std::vector< uint8_t >& buf) {
    cl.set.net->force_subquorum({craft::mem_replica_id(cl.vid, 0)});
    EXPECT_FALSE(wr(*cl.client, off_blk, buf));
    cl.set.net->clear_faults();
}
} // namespace

// --- n=1: the stepping stone ---

TEST(CraftClient, N1_WriteReadRoundtrip) {
    auto cl = make_cluster(1);
    auto buf = page_of(0x5A);
    ASSERT_TRUE(wr(*cl.client, 9, buf));
    EXPECT_EQ(rd(*cl.client, 9), buf);
}

TEST(CraftClient, N1_UnwrittenReadsZeros) {
    auto cl = make_cluster(1);
    EXPECT_EQ(rd(*cl.client, 3), page_of(0)); // never written -> hole -> zeros
}

TEST(CraftClient, N1_OverwriteNewestWins) {
    auto cl = make_cluster(1);
    auto older = page_of(0xAA);
    auto newer = page_of(0xBB);
    ASSERT_TRUE(wr(*cl.client, 5, older));
    ASSERT_TRUE(wr(*cl.client, 5, newer)); // higher dLSN to the same LBA
    EXPECT_EQ(rd(*cl.client, 5), newer);
}

TEST(CraftClient, N1_CommitAdvancesOnAck) {
    auto cl = make_cluster(1);
    EXPECT_EQ(cl.client->commit_lsn(), -1); // nothing acked yet
    auto b0 = page_of(0x11);
    auto b1 = page_of(0x22);
    ASSERT_TRUE(wr(*cl.client, 0, b0));
    EXPECT_EQ(cl.client->commit_lsn(), 0); // n=1: quorum instantly met -> commit on ack
    ASSERT_TRUE(wr(*cl.client, 1, b1));
    EXPECT_EQ(cl.client->commit_lsn(), 1);
    EXPECT_EQ(cl.client->read_horizon(), 1);
    EXPECT_TRUE(rg(cl.client->flush()).has_value());
}

// --- n=3: quorum path the next step builds on ---

TEST(CraftClient, N3_QuorumWriteRead) {
    auto cl = make_cluster(3);
    auto buf = page_of(0x77);
    ASSERT_TRUE(wr(*cl.client, 12, buf)); // broadcast to 3, all ack
    EXPECT_EQ(cl.client->commit_lsn(), 0);
    EXPECT_EQ(rd(*cl.client, 12), buf);
}

TEST(CraftClient, N3_SubquorumStillCommits) {
    auto cl = make_cluster(3);
    // Drop one replica: 2 of 3 still ack, which meets quorum(3)=2.
    cl.set.net->force_subquorum({craft::mem_replica_id(cl.vid, 0), craft::mem_replica_id(cl.vid, 1)});
    auto buf = page_of(0x33);
    ASSERT_TRUE(wr(*cl.client, 4, buf));
    EXPECT_EQ(rd(*cl.client, 4), buf); // leader (index 0) is kept, so the read is served
}

TEST(CraftClient, N3_BelowQuorumFails) {
    auto cl = make_cluster(3);
    // Keep only the leader: 1 of 3 acks, below quorum(3)=2 -> the write must fail.
    cl.set.net->force_subquorum({craft::mem_replica_id(cl.vid, 0)});
    auto buf = page_of(0x44);
    EXPECT_FALSE(wr(*cl.client, 6, buf));
}

// --- the read horizon: a sub-quorum write must never leak into a read ---

TEST(CraftClient, N3_FailedWriteDoesNotLeakIntoReads) {
    auto cl = make_cluster(3);
    auto v0 = page_of(0x10);
    auto v1 = page_of(0x11);
    auto v2 = page_of(0x12);

    ASSERT_TRUE(wr(*cl.client, 4, v0)); // dLSN 0: durable at block 4
    EXPECT_EQ(cl.client->commit_lsn(), 0);
    write_subquorum(cl, 4, v1);         // dLSN 1: unresolved, and the leader HOLDS it
    ASSERT_TRUE(wr(*cl.client, 9, v2)); // dLSN 2: acks, pushing the horizon above the hole

    EXPECT_EQ(cl.client->commit_lsn(), 0) << "commit must stay pinned beneath the unresolved dLSN 1";
    EXPECT_EQ(cl.client->read_horizon(), 2);

    // Reading block 4 at the raw horizon would let the leader serve its journaled dLSN 1, which may yet
    // be Empty'd -- a later read would then regress. The client clamps below it and serves dLSN 0.
    EXPECT_EQ(rd(*cl.client, 4), v0);
    EXPECT_EQ(cl.client->winner_scans(), 1u) << "the per-block winner pass is what made this safe";

    EXPECT_EQ(rd(*cl.client, 9), v2); // an unrelated block still reads at the full horizon
}

TEST(CraftClient, N3_AckedOverwriteShadowsAFailedWrite) {
    auto cl = make_cluster(3);
    auto v0 = page_of(0x20);
    auto v1 = page_of(0x21);
    auto v2 = page_of(0x22);

    ASSERT_TRUE(wr(*cl.client, 6, v0)); // dLSN 0
    write_subquorum(cl, 6, v1);         // dLSN 1: failed, leader holds it
    ASSERT_TRUE(wr(*cl.client, 6, v2)); // dLSN 2: same block, acks

    // Highest dLSN wins per LBA, so dLSN 2 fully shadows the unresolved dLSN 1: it can never be the version
    // a read sees, and the read proceeds at the full horizon.
    EXPECT_EQ(rd(*cl.client, 6), v2);
    EXPECT_EQ(cl.client->commit_lsn(), 0);
}

TEST(CraftClient, N3_SplitReadServesEachBlockAtItsOwnHorizon) {
    auto cl = make_cluster(3);
    auto v0 = page_of(0x30, 2); // blocks 4-5
    auto v1 = page_of(0x31, 2); // blocks 4-5, will fail
    auto v2 = page_of(0x32);    // block 4 only

    ASSERT_TRUE(wr(*cl.client, 4, v0)); // dLSN 0: both blocks durable
    write_subquorum(cl, 4, v1);         // dLSN 1: failed, leader journals it over BOTH blocks
    ASSERT_TRUE(wr(*cl.client, 4, v2)); // dLSN 2: acks, but covers only block 4

    // Block 4's newest version is the acked dLSN 2, which supersedes the failed dLSN 1 there. Block 5's is
    // the failed dLSN 1 itself, which the leader physically holds. One horizon cannot serve both.
    std::vector< uint8_t > want;
    want.insert(want.end(), v2.begin(), v2.end());          // block 4 -> v2 (read at the horizon)
    want.insert(want.end(), v0.begin(), v0.begin() + PAGE); // block 5 -> v0 (clamped below dLSN 1)
    EXPECT_EQ(rd(*cl.client, 4, 2), want);
    EXPECT_EQ(cl.client->commit_lsn(), 0) << "the failed write still pins commit";
}

// --- session establishment ---

TEST(CraftClient, LoginFollowsLeaderRedirect) {
    // Aim login at replica 2, a follower: it succeeds with term == 0 + leader_hint rather than erroring.
    auto cl = make_cluster(3, /*leader=*/2);
    EXPECT_GT(cl.client->term(), 0ULL);

    auto buf = page_of(0x55);
    ASSERT_TRUE(wr(*cl.client, 3, buf));
    EXPECT_EQ(rd(*cl.client, 3), buf);
}

TEST(CraftClient, RecoveryLoginReadsDurableDataNotZeros) {
    auto cl = make_cluster(3);
    auto buf = page_of(0x77);
    ASSERT_TRUE(wr(*cl.client, 2, buf));
    ASSERT_TRUE(wr(*cl.client, 7, buf));
    EXPECT_EQ(cl.client->commit_lsn(), 1);

    // A fresh client logs into the same replicas. login() hands back rs_commit_lsn = 1, which must seed
    // the read horizon as well as the frontier -- seeding only next_dlsn leaves H at -1 and reads zeros.
    auto fresh = std::make_shared< craft::craft_client >(cl.set.handles);
    ASSERT_TRUE(rg(fresh->login(TOKEN + 1)).has_value());
    EXPECT_EQ(fresh->commit_lsn(), 1);
    EXPECT_EQ(fresh->read_horizon(), 1);
    EXPECT_EQ(rd(*fresh, 2), buf);
}

// --- quorum ack: the write must not wait for the slowest replica ---

// A straggler delayed far past the transport deadline costs the write NOTHING: the other two ack, quorum is
// met, and write() returns while that peer is still in flight. Without when_quorum this would block for the
// full op_timeout on every write.
TEST(CraftClient, N3_WriteAcksAtQuorumWithoutWaitingForAStraggler) {
    auto cl = make_cluster(3);
    cl.set.net->set_op_timeout(std::chrono::milliseconds{2000});
    cl.set.net->set_delay(craft::mem_replica_id(cl.vid, 2), std::chrono::milliseconds{2000});

    auto buf = page_of(0x7C);
    auto const t0 = std::chrono::steady_clock::now();
    ASSERT_TRUE(wr(*cl.client, 4, buf)) << "quorum(3)=2 met by the two healthy replicas";
    auto const elapsed = std::chrono::steady_clock::now() - t0;

    EXPECT_LT(elapsed, std::chrono::milliseconds{1000}) << "returned at quorum, not at the deadline";
    EXPECT_EQ(cl.client->commit_lsn(), 0) << "slot resolved acked; the frontier advanced";
}

// The straggler's write must land with the bytes as of ISSUE time, not as of delivery time. This pins the
// payload-ownership contract in craft_replica.hpp: the client acks at quorum and the caller is then free to
// recycle its buffer, so a replica that kept the caller's sg_list across its sleep would deliver whatever the
// caller wrote there next. Overwriting the live buffer (rather than freeing it) makes that deterministic --
// a use-after-free would merely read stale-but-correct bytes and pass.
TEST(CraftClient, N3_StragglerWriteLandsIntactAfterTheClientReturned) {
    auto cl = make_cluster(3);
    cl.set.net->set_op_timeout(std::chrono::milliseconds{20});
    cl.set.net->set_delay(craft::mem_replica_id(cl.vid, 2), std::chrono::milliseconds{200});

    auto buf = page_of(0x5E);
    ASSERT_TRUE(wr(*cl.client, 6, buf)) << "acks at quorum while replica 2 is still in flight";
    ASSERT_EQ(cl.set.replicas[2]->stats().journal_slots, 0u) << "not delivered yet";

    // The caller recycles its buffer the instant the write acks. The in-flight straggler must not see this.
    std::fill(buf.begin(), buf.end(), 0xFF);

    // Lift the delay so the verifying read below is not itself timed out. The late write was already
    // scheduled with its own deadline, so it still arrives late.
    cl.set.net->set_delay(craft::mem_replica_id(cl.vid, 2), std::chrono::milliseconds{0});

    auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while ((cl.set.replicas[2]->stats().journal_slots == 0u) && (std::chrono::steady_clock::now() < deadline)) {
        std::this_thread::sleep_for(std::chrono::milliseconds{2});
    }
    ASSERT_EQ(cl.set.replicas[2]->stats().journal_slots, 1u) << "the peer applied it after the deadline";

    // Read it back straight off the straggler at the write's horizon; the bytes must be intact.
    auto& r = *cl.set.replicas[2];
    std::vector< uint8_t > dest(PAGE, 0xEE);
    auto const out = rg(r.read(chdr(cl.client->term(), /*commit*/ 0), /*H*/ 0, blk(6), blk(1), one_iov(dest)));
    ASSERT_TRUE(out.has_value());
    EXPECT_FALSE((*out)[0].hole);
    EXPECT_EQ(dest, page_of(0x5E)) << "payload survived; the replica copied it at issue, not after its sleep";
}

// --- Empty: provably absent set-wide ---

// Every replica rejects deterministically (never delivered => never appended), so nobody holds the slot. It
// is a permanent no-op: Empty, not failed. An unresolved slot here would pin the frontier forever, because
// nothing will ever fill or Empty a dLSN that does not exist anywhere.
TEST(CraftClient, N3_AllReplicasRefuseResolvesEmptyAndReleasesTheFrontier) {
    auto cl = make_cluster(3);
    auto buf = page_of(1);
    ASSERT_TRUE(wr(*cl.client, 1, buf)); // dLSN 0 commits normally
    EXPECT_EQ(cl.client->commit_lsn(), 0);

    cl.set.net->force_subquorum({}); // keep nobody: all three reject with REPLICA_DOWN
    EXPECT_FALSE(wr(*cl.client, 2, buf));
    cl.set.net->clear_faults();

    EXPECT_EQ(cl.client->commit_lsn(), 1) << "dLSN 1 is Empty: a resolved no-op the frontier passes over";
}

// Contrast: the leader physically journals the slot, so it is NOT provably absent. The slot stays unresolved
// and correctly pins the frontier until a leader resolution round fills or Empties it.
TEST(CraftClient, N3_PartialRefusalPinsTheFrontier) {
    auto cl = make_cluster(3);
    auto buf = page_of(1);
    ASSERT_TRUE(wr(*cl.client, 1, buf));
    EXPECT_EQ(cl.client->commit_lsn(), 0);

    cl.set.net->force_subquorum({craft::mem_replica_id(cl.vid, 0)}); // the leader still holds it
    EXPECT_FALSE(wr(*cl.client, 2, buf));
    cl.set.net->clear_faults();

    EXPECT_EQ(cl.client->commit_lsn(), 0) << "some replica may hold dLSN 1; commit must stall under it";
}

// --- the multi-queue model: K threads share one craft_client ---
//
// ublk picks a queue by CPU, not by LBA, so any LBA can arrive on any queue thread, and all of them drive
// ONE craft_client against ONE partition's dLSN space. That is the concurrency the tracker's atomics exist
// for, and nothing exercised it through craft_client until MemTransport stopped completing ops inline: the
// fan-out used to run sequentially on the issuing thread and when_quorum never even suspended.
//
// Run this under TSAN. It is the test that says the client survives a real transport's execution model.
TEST(CraftClient, N3_ConcurrentWritersFromManyThreads) {
    constexpr uint32_t kThreads = 4;
    constexpr uint32_t kWritesPerThread = 25;
    constexpr uint64_t kBlocks = uint64_t{kThreads} * kWritesPerThread;

    auto cl = make_cluster(3);
    std::vector< std::thread > writers;
    std::atomic< uint32_t > failures{0};

    for (uint32_t t = 0; t < kThreads; ++t) {
        writers.emplace_back([&, t] {
            for (uint32_t i = 0; i < kWritesPerThread; ++i) {
                auto const blk_idx = (t * kWritesPerThread) + i;
                auto buf = page_of(static_cast< uint8_t >(blk_idx & 0xFF));
                if (!wr(*cl.client, blk_idx, buf)) ++failures;
            }
        });
    }
    for (auto& w : writers) {
        w.join();
    }
    EXPECT_EQ(failures.load(), 0u) << "every write reached quorum";

    // Each write took a distinct dLSN, and with all of them acked the frontier must have folded over the
    // whole contiguous range. Nothing may remain unresolved once every writer has joined.
    auto const s = cl.client->dlsn_stats();
    EXPECT_EQ(s.issued, static_cast< int64_t >(kBlocks) - 1);
    EXPECT_EQ(s.frontier, s.issued) << "the contiguous prefix folded over every acked slot";
    EXPECT_EQ(s.unresolved_count, 0u);

    // And every block reads back what its owning thread wrote: no lost or crossed dLSN.
    for (uint64_t b = 0; b < kBlocks; ++b) {
        EXPECT_EQ(rd(*cl.client, b), page_of(static_cast< uint8_t >(b & 0xFF))) << "block " << b;
    }
}

// --- read routing + failover: the per-member eligibility map (read_route_map) ---

// The map in isolation: create at reserve, complete, fold on completion, gate eligibility.
TEST(ReadRouteMap, RecordFoldEligible) {
    craft::read_route_map m;
    m.reset(3, /*L=*/-1);
    EXPECT_EQ(m.folded(), -1);
    for (std::size_t i = 0; i < 3; ++i)
        EXPECT_TRUE(m.caught_up(i));

    // dLSN 0 over block 0. Members 1 and 2 ack; member 0 has not completed yet.
    m.create(0, blk(0), PAGE);
    m.record_completion(0, 1, /*acked=*/true);
    m.record_completion(0, 2, /*acked=*/true);

    // Before it is complete/folded, eligibility reads straight off the holder set: member 0 is not a holder.
    EXPECT_FALSE(m.eligible(0, blk(0), PAGE, /*Hmax=*/0));
    EXPECT_TRUE(m.eligible(1, blk(0), PAGE, 0));
    EXPECT_TRUE(m.eligible(2, blk(0), PAGE, 0)); // a non-overlapping read is eligible for everyone
    EXPECT_TRUE(m.eligible(0, blk(5), PAGE, 0));

    // Incomplete (member 0's op has not finished), so fold stalls -- nobody is stuck yet.
    m.fold_to(0);
    EXPECT_EQ(m.folded(), -1) << "incomplete: not folded";
    EXPECT_TRUE(m.caught_up(0));

    // Member 0's op completes as a terminal miss -> the slot is complete, the miss is recorded, fold advances.
    m.record_completion(0, 0, /*acked=*/false);
    m.fold_to(0);
    EXPECT_EQ(m.folded(), 0);
    EXPECT_TRUE(m.caught_up(1));
    EXPECT_TRUE(m.caught_up(2));
    EXPECT_FALSE(m.caught_up(0)) << "member 0 missed dLSN 0, now at/below the frontier";

    // Post-fold, member 0 is missing ONLY block 0 -- per-block, not sidelined from everything. It is still
    // eligible for a block it did not miss. This is the whole point of the LBA-range map: the old scalar
    // excluded member 0 from block 5 too.
    EXPECT_FALSE(m.eligible(0, blk(0), PAGE, 0)) << "missing block 0";
    EXPECT_TRUE(m.eligible(0, blk(5), PAGE, 0)) << "holds block 5 -> still serves it";
    EXPECT_TRUE(m.eligible(1, blk(5), PAGE, 0));
    // A late ack for the already-folded dLSN does not clear the miss (deferred recovery).
    m.record_completion(0, 0, /*acked=*/true);
    EXPECT_FALSE(m.caught_up(0));

    // A universally held write records no miss (and, being skipped to keep healthy writes lock-free, does not
    // clear member 0's earlier block-0 miss either -- that waits for a non-universal supersede or recovery).
    m.create(1, blk(0), PAGE);
    m.record_completion(1, 0, true);
    m.record_completion(1, 1, true);
    m.record_completion(1, 2, true);
    m.fold_to(1);
    EXPECT_EQ(m.folded(), 1);
    EXPECT_TRUE(m.caught_up(1));
    EXPECT_TRUE(m.caught_up(2));
    EXPECT_FALSE(m.caught_up(0)) << "still missing block 0";

    // Supersede: member 0 now ACKS a later (non-universal) write to block 0 -- dLSN 2, which member 2 misses.
    // Once it folds, member 0 holds the latest folded winner on block 0, so its miss is erased; member 2 now
    // misses block 0. This is the erase-on-hold path (clear-on-supersede in ascending order).
    m.create(2, blk(0), PAGE);
    m.record_completion(2, 0, true);
    m.record_completion(2, 1, true);
    m.record_completion(2, 2, false);
    m.fold_to(2);
    EXPECT_EQ(m.folded(), 2);
    EXPECT_TRUE(m.eligible(0, blk(0), PAGE, 2)) << "a later held write superseded the block-0 miss";
    EXPECT_TRUE(m.caught_up(0));
    EXPECT_FALSE(m.eligible(2, blk(0), PAGE, 2)) << "member 2 missed the latest write on block 0";
    EXPECT_FALSE(m.caught_up(2));
}

// The recovery seam in isolation: advance_synced (fed by the broadcast keep_alive) advances the per-member
// commit_lsn -> the set-wide reclaim floor, and erases Missing entries the member has applied. End to end the
// erase is dormant (a laggard's commit_lsn stalls below its hole until resync fills it), so drive it directly:
// hand the map each member's commit_lsn as if recovery had caught member 2 up.
TEST(ReadRouteMap, AdvanceSyncedLiftsFloorAndClearsMiss) {
    craft::read_route_map m;
    m.reset(3, /*L=*/-1);
    EXPECT_EQ(m.all_committed(), -1) << "floor starts at the login baseline";

    // Member 2 misses dLSN 0 on block 3; fold it into the map.
    m.create(0, blk(3), PAGE);
    m.record_completion(0, 0, true);
    m.record_completion(0, 1, true);
    m.record_completion(0, 2, false);
    m.fold_to(0);
    EXPECT_FALSE(m.caught_up(2));
    EXPECT_FALSE(m.eligible(2, blk(3), PAGE, 0));
    EXPECT_EQ(m.all_committed(), -1) << "no commit_lsn learned yet";

    // The floor is the MIN across members, so the laggard pins it below the caught-up ones: members 0 and 1
    // race ahead to dLSN 5, but the floor stays at the baseline until member 2 reports, then tracks member 2.
    m.advance_synced(0, 5);
    m.advance_synced(1, 5);
    EXPECT_EQ(m.all_committed(), -1) << "member 2 still at the baseline pins the floor, not the leaders' 5";
    m.advance_synced(2, 0);
    EXPECT_EQ(m.all_committed(), 0) << "floor tracks the laggard (0), not the caught-up members (5)";

    // Member 2's commit_lsn passing dLSN 0 means it applied block 3 (resync filled the hole) -> miss cleared.
    EXPECT_TRUE(m.caught_up(2));
    EXPECT_TRUE(m.eligible(2, blk(3), PAGE, 0));
}

// The per-block win: a member missing block X still serves reads of a block Y it holds. Under the old scalar
// a single miss sidelined a member from EVERY sub-frontier read; the LBA-range Missing map only routes it
// around the blocks it actually missed.
TEST(CraftClient, PerBlockMissStillServesOtherBlocks) {
    auto cl = make_cluster(3);
    // Member 2 misses block 4 (sub-quorum keeps only 0 and 1).
    cl.set.net->force_subquorum({craft::mem_replica_id(cl.vid, 0), craft::mem_replica_id(cl.vid, 1)});
    auto v4 = page_of(0xA4);
    ASSERT_TRUE(wr(*cl.client, 4, v4)); // dLSN 0: members 0,1 hold block 4; member 2 misses it
    // Block 9 on members 0 and 2 -- both are REQUIRED to meet quorum, so member 2 deterministically holds it
    // by the time the write returns (no straggler race); member 1 misses block 9.
    cl.set.net->force_subquorum({craft::mem_replica_id(cl.vid, 0), craft::mem_replica_id(cl.vid, 2)});
    auto v9 = page_of(0xB9);
    ASSERT_TRUE(wr(*cl.client, 9, v9)); // dLSN 1: members 0,2 hold block 9; member 1 misses it
    cl.set.net->clear_faults();

    // Take member 0 down. Member 1 missed block 9 (ineligible for it), member 0 is unreachable -- only member 2
    // can serve, and it holds block 9. Under the scalar it was "behind" for missing block 4 and excluded from
    // block 9 too, so this read would be NO_QUORUM. Per-block, it serves.
    cl.set.net->set_up(craft::mem_replica_id(cl.vid, 0), false);
    EXPECT_EQ(rd(*cl.client, 9), v9) << "member 2 missed block 4 but still serves block 9 it holds";
}

// The availability cliff the scalar had: each of the three members misses a DIFFERENT block, so all three are
// "behind". Under the scalar a read of a block ALL THREE hold returns NOT_ELIGIBLE (everyone is sidelined).
// Per-block, each is only missing its own block, so the shared block reads fine.
TEST(CraftClient, ScatteredMissesDoNotSidelineEveryMember) {
    auto cl = make_cluster(3);
    for (uint32_t i = 0; i < 3; ++i) {
        std::vector< peer_id_t > keep;
        for (uint32_t j = 0; j < 3; ++j)
            if (j != i) keep.push_back(craft::mem_replica_id(cl.vid, j));
        cl.set.net->force_subquorum(keep); // member i excluded
        auto v = page_of(static_cast< uint8_t >(0x10 + i));
        ASSERT_TRUE(wr(*cl.client, i, v)); // block i: member i misses it, the other two hold it
        cl.set.net->clear_faults();
    }
    auto vy = page_of(0xCC);
    ASSERT_TRUE(wr(*cl.client, 8, vy)); // block 8: all three ack

    EXPECT_EQ(rd(*cl.client, 8), vy)
        << "block 8 is held by all three; scattered misses on other blocks must not sideline everyone";
}

// (a) The exact read-after-write violation that motivated all of this. The leader misses an acked write
// because it was delayed past the op timeout; the read must route AROUND it to a follower that holds the new
// value. With the leader reachable again (but still lacking the write), routing to it would return the stale
// 0xAA -- which is the bug this fixes.
TEST(CraftClient, LeaderDelayReadReturnsNewValue) {
    auto cl = make_cluster(3); // leader == 0
    auto v_old = page_of(0xAA);
    ASSERT_TRUE(wr(*cl.client, 8, v_old)); // dLSN 0: all three ack

    cl.set.net->set_op_timeout(std::chrono::milliseconds{20});
    cl.set.net->set_delay(craft::mem_replica_id(cl.vid, 0), std::chrono::milliseconds{500});
    auto v_new = page_of(0xBB);
    ASSERT_TRUE(wr(*cl.client, 8, v_new)) << "quorum(3)=2 met by the followers; the leader times out";

    // Reachable again, but its late delivery is ~500ms out, so it still holds only 0xAA right now.
    cl.set.net->set_delay(craft::mem_replica_id(cl.vid, 0), std::chrono::milliseconds{0});

    EXPECT_EQ(rd(*cl.client, 8), v_new) << "routed around the stale leader to a follower holding 0xBB";
}

// (b) The same failover reached the deterministic way: a sub-quorum that excludes the leader. The leader
// provably never received the write (REPLICA_DOWN), so it is excluded from the read.
TEST(CraftClient, SubquorumExcludingLeaderRoutesAround) {
    auto cl = make_cluster(3);
    auto v_old = page_of(0xAA);
    ASSERT_TRUE(wr(*cl.client, 3, v_old)); // dLSN 0: all ack

    cl.set.net->force_subquorum({craft::mem_replica_id(cl.vid, 1), craft::mem_replica_id(cl.vid, 2)});
    auto v_new = page_of(0xBB);
    ASSERT_TRUE(wr(*cl.client, 3, v_new)) << "quorum(3)=2 met by the two followers";
    cl.set.net->clear_faults();

    EXPECT_EQ(rd(*cl.client, 3), v_new) << "leader is provably missing dLSN 1; routed to a follower";
}

// (c) No regression on the healthy path: every write reaches all three, so no member ever records a miss and
// all stay caught up and eligible. Robust against straggler timing: a healthy write records a miss for no one
// (held == all_mask), so caught_up is never transiently false.
TEST(CraftClient, HealthyClusterNoRegression) {
    auto cl = make_cluster(3);
    auto b0 = page_of(0x11);
    auto b1 = page_of(0x22);
    ASSERT_TRUE(wr(*cl.client, 2, b0));
    ASSERT_TRUE(wr(*cl.client, 7, b1));
    EXPECT_EQ(rd(*cl.client, 2), b0);
    EXPECT_EQ(rd(*cl.client, 7), b1);
    for (std::size_t i = 0; i < 3; ++i)
        EXPECT_TRUE(cl.client->route_caught_up(i)) << "member " << i << " missed nothing; caught up";
}

// (c2) Reads round-robin across eligible members rather than piling on the leader. A CRAFT read has no leader
// affinity, so the router rotates the start each read. All three hold the block and are eligible, so a run of
// reads (a multiple of 3) lands an equal share on each -- deterministic on the single test thread regardless
// of the rotor's starting value (any 3k consecutive reads split evenly across 3 members).
TEST(CraftClient, HealthyReadsRoundRobinAcrossMembers) {
    auto cl = make_cluster(3);
    auto buf = page_of(0x5A);
    ASSERT_TRUE(wr(*cl.client, 4, buf)); // all three hold it -> all eligible

    std::array< std::size_t, 3 > before{};
    for (std::size_t i = 0; i < 3; ++i)
        before[i] = cl.set.replicas[i]->reads_served();

    constexpr std::size_t kReads = 30;
    for (std::size_t i = 0; i < kReads; ++i)
        EXPECT_EQ(rd(*cl.client, 4), buf);

    for (std::size_t i = 0; i < 3; ++i)
        EXPECT_EQ(cl.set.replicas[i]->reads_served() - before[i], kReads / 3)
            << "member " << i << " served an uneven share -- reads did not round-robin";
}

// (d) When every holder of the winner is unreachable, the read must FAIL (NO_QUORUM), never fall back to a
// member that is missing the write and serve stale data.
TEST(CraftClient, AllHoldersDownReturnsNotEligible) {
    auto cl = make_cluster(3);
    auto v_old = page_of(0xAA);
    ASSERT_TRUE(wr(*cl.client, 5, v_old)); // dLSN 0: all ack

    // Followers hold dLSN 1; the leader is provably missing it.
    cl.set.net->force_subquorum({craft::mem_replica_id(cl.vid, 1), craft::mem_replica_id(cl.vid, 2)});
    auto v_new = page_of(0xBB);
    ASSERT_TRUE(wr(*cl.client, 5, v_new));
    cl.set.net->clear_faults();

    // Now take both holders down. The only member "eligible" by holding is unreachable; the leader is
    // ineligible (behind). The read must not serve the leader's stale 0xAA.
    cl.set.net->set_up(craft::mem_replica_id(cl.vid, 1), false);
    cl.set.net->set_up(craft::mem_replica_id(cl.vid, 2), false);

    std::vector< uint8_t > dest(PAGE, 0xEE);
    auto r = rg(cl.client->read(blk(5), PAGE, one_iov(dest)));
    ASSERT_FALSE(r.has_value()) << "no reachable holder; must not serve stale data";
    EXPECT_TRUE(r.error() == make_error_condition(craft_error::NO_QUORUM) ||
                r.error() == make_error_condition(craft_error::NOT_ELIGIBLE))
        << "got: " << r.error().message();
    EXPECT_NE(dest, v_old) << "never filled from the stale leader";
}

// (e) A member that missed an acked write is marked behind and routed around. The sub-quorum drop is the
// FIRST write, so its completion is deterministic: the dropped replica returns REPLICA_DOWN inline and the
// two quorum members must both ack before the write returns, so all three completions are recorded before
// the read folds -- no healthy straggler to stall the fold (which is correct, but non-deterministic to
// assert against).
TEST(CraftClient, BehindMemberIsRoutedAround) {
    auto cl = make_cluster(3);

    cl.set.net->force_subquorum({craft::mem_replica_id(cl.vid, 0), craft::mem_replica_id(cl.vid, 1)});
    auto v = page_of(0xBB);
    ASSERT_TRUE(wr(*cl.client, 6, v)); // dLSN 0: leader + follower 1 ack, follower 2 provably misses it
    cl.set.net->clear_faults();

    EXPECT_EQ(rd(*cl.client, 6), v) << "served by a holder (the leader)";
    EXPECT_FALSE(cl.client->route_caught_up(2)) << "follower 2 missed the acked write; behind the frontier";
    EXPECT_TRUE(cl.client->route_caught_up(0)) << "leader held it; still caught up";
    EXPECT_TRUE(cl.client->route_caught_up(1)) << "follower 1 held it; still caught up";
}

// Broadcast keep_alive learns every member's achieved commit_lsn and advances the set-wide reclaim floor
// (min across members). Healthy: all members apply up to the frontier, so the floor equals it.
TEST(CraftClient, KeepAliveAdvancesReclaimFloor) {
    auto cl = make_cluster(3);
    int64_t const base = cl.client->all_committed_lsn(); // login baseline, before any keep_alive
    for (uint64_t b = 0; b < 4; ++b) {
        auto v = page_of(static_cast< uint8_t >(0x40 + b));
        ASSERT_TRUE(wr(*cl.client, b, v));
    }
    // Writes advance the client frontier, but a member only applies up to the commit_lsn piggybacked on the
    // IO it saw; the broadcast keep_alive carries the latest frontier to everyone and reads back their commit.
    ASSERT_TRUE(rg(cl.client->flush()).has_value());

    int64_t const F = cl.client->commit_lsn();
    EXPECT_GT(cl.client->all_committed_lsn(), base);
    EXPECT_EQ(cl.client->all_committed_lsn(), F) << "healthy: every member applied up to the frontier";
    for (uint32_t i = 0; i < 3; ++i)
        EXPECT_EQ(cl.set.replicas[i]->stats().commit_lsn, F);
}

// The reclaim floor is pinned by the member that is BEHIND, not by the frontier: it is the min commit_lsn
// across members, so a member that has not caught up holds it down -- exactly what keeps a peer's still-needed
// journal from being reclaimed. (Sending our own frontier as the floor, as the placeholder -1 avoided, would
// break this.) Member 2 is excluded from every write, so it deterministically holds nothing and never leaves
// the login baseline while the other two advance -- the precise per-dLSN min is unit-tested separately.
TEST(CraftClient, ReclaimFloorPinnedByBehindMember) {
    auto cl = make_cluster(3);
    // Members 0 and 1 are the required quorum for every write (both must ack, so neither straggles); member 2
    // is excluded throughout and holds nothing.
    cl.set.net->force_subquorum({craft::mem_replica_id(cl.vid, 0), craft::mem_replica_id(cl.vid, 1)});
    for (uint64_t b = 0; b < 3; ++b) {
        auto v = page_of(static_cast< uint8_t >(0xA0 + b));
        ASSERT_TRUE(wr(*cl.client, b, v)); // dLSN b: 0,1 hold; member 2 misses it
    }
    cl.set.net->clear_faults();
    ASSERT_TRUE(rg(cl.client->flush()).has_value());

    int64_t const F = cl.client->commit_lsn();
    EXPECT_EQ(cl.set.replicas[0]->stats().commit_lsn, F);
    EXPECT_EQ(cl.set.replicas[1]->stats().commit_lsn, F);
    EXPECT_EQ(cl.set.replicas[2]->stats().commit_lsn, -1) << "member 2 holds nothing; stalls at the baseline";
    EXPECT_EQ(cl.client->all_committed_lsn(), -1) << "floor pinned by the behind member, not the frontier";
    EXPECT_LT(cl.client->all_committed_lsn(), F);
}

// logout tears the session down on every replica (server-side), then a fresh login re-establishes it. The
// session term is observable in the replica stats, so we can watch it clear and come back.
TEST(CraftClient, LogoutClearsSessionThenReloginWorks) {
    auto cl = make_cluster(3);
    auto v = page_of(0x5A);
    ASSERT_TRUE(wr(*cl.client, 7, v)) << "session active: write succeeds";
    uint64_t const t0 = cl.client->term();
    ASSERT_NE(t0, 0u);
    for (uint32_t i = 0; i < 3; ++i)
        EXPECT_EQ(cl.set.replicas[i]->stats().term, t0) << "session term held on every replica";

    ASSERT_TRUE(rg(cl.client->logout()).has_value());

    // Server side: the InternalLogout cleared the session term on every replica.
    for (uint32_t i = 0; i < 3; ++i)
        EXPECT_EQ(cl.set.replicas[i]->stats().term, 0u) << "logout tore the session down on every replica";
    // Client side: it now considers itself disconnected, so IO fails fast (no term to fence with).
    auto v2 = page_of(0x5B);
    EXPECT_FALSE(rg(cl.client->write(blk(7), v2.size(), one_iov(v2))).has_value());

    // Re-login re-establishes a live session and IO works again.
    ASSERT_TRUE(rg(cl.client->login(TOKEN)).has_value());
    EXPECT_NE(cl.client->term(), 0u) << "fresh session";
    for (uint32_t i = 0; i < 3; ++i)
        EXPECT_EQ(cl.set.replicas[i]->stats().term, cl.client->term());
    auto v3 = page_of(0x5C);
    EXPECT_TRUE(wr(*cl.client, 7, v3));
    EXPECT_EQ(rd(*cl.client, 7), v3);
}

// The collapse in isolation: at most one outstanding keep_alive per leg. A busy read stream tops a leg up
// again only after its previous keep_alive has completed (released the flag), never one-per-read.
TEST(ReadRouteMap, OneOutstandingKeepAlivePerLeg) {
    craft::read_route_map m;
    m.reset(3, /*L=*/-1);
    EXPECT_TRUE(m.try_begin_keepalive(1)) << "first keep_alive to a leg may fire";
    EXPECT_FALSE(m.try_begin_keepalive(1)) << "one already outstanding on that leg -> collapsed";
    EXPECT_TRUE(m.try_begin_keepalive(2)) << "a different leg is independent";
    m.end_keepalive(1);
    EXPECT_TRUE(m.try_begin_keepalive(1)) << "released -> may fire again";
    EXPECT_FALSE(m.try_begin_keepalive(2)) << "leg 2 is still outstanding";
}

// Timer-less liveness drive: a read touches one leg and keeps the others alive; a write touches everyone and
// keeps no one alive. The keep_alives are fire-and-forget, so they don't delay the read (asserted by the fact
// that rd() returns before they land -- we poll for them).
TEST(CraftClient, ReadDrivesKeepAliveToSkippedLegs) {
    auto cl = make_cluster(3);
    auto v = page_of(0x77);
    ASSERT_TRUE(wr(*cl.client, 2, v));
    for (uint32_t i = 0; i < 3; ++i)
        EXPECT_EQ(cl.set.replicas[i]->keepalives_served(), 0u) << "a write broadcasts to all; it drives no keep_alive";

    ASSERT_EQ(rd(*cl.client, 2), v);
    auto total_ka = [&] {
        std::size_t n = 0;
        for (uint32_t i = 0; i < 3; ++i)
            n += cl.set.replicas[i]->keepalives_served();
        return n;
    };
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (total_ka() < 2 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    EXPECT_GE(total_ka(), 2u) << "the two legs the read skipped each got a (fire-and-forget) keep_alive";
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    sisl::logging::SetLogPattern("[%D %T%z] [%^%L%$] [%t] %v");
    return RUN_ALL_TESTS();
}
