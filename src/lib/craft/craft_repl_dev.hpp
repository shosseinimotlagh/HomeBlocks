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

#include "../hb_internal.hpp"
#include <homestore/replication/repl_dev.hpp>

#include <atomic>
#include <mutex>
#include <set>
#include <vector>

namespace homeblocks {

// ─── wire-protocol types ──────────────────────────────────────────────────────

// Network address of a replica returned by login().
struct replica_endpoint {
    peer_id_t   id;
    std::string addr; // "host:port"
};

// Per-partition in-memory state. Authoritative in memory; recovered from
// journal + superblock on restart.
struct CraftPartitionState {
    int64_t  commit_lsn      {-1}; // highest committed dLSN (applied to LBA index)
    int64_t  last_append_lsn {-1}; // highest appended dLSN (may be uncommitted)
    uint64_t client_token    {0};  // token from the last successful InternalLogin
    uint64_t term            {0};  // current RAFT term
};

// Returned by get_lsns() / get_rs_commit_lsn().
struct LSNPair {
    int64_t commit_lsn;
    int64_t last_append_lsn;
};

// Returned by login().
struct LoginResult {
    std::vector< replica_endpoint > members;
    int64_t  dLSN; // starting LSN for new I/O
    uint64_t term;
};

// One journal slot, returned by fetch_data(). is_empty == true means the LSN
// never reached any replica and the data fields are invalid.
struct JournalSlot {
    int64_t     lsn;
    bool        is_empty{false};
    lba_t       lba{0};
    lba_count_t len{0};
    sisl::sg_list data;
};

// ─── journal backend abstraction ─────────────────────────────────────────────
//
// Injected into CraftReplDev so unit tests can supply a mock without touching
// HomeStore. Production code passes HomeStoreCraftJournalBackend (defined in
// craft_repl_dev.cpp).

class CraftJournalBackend {
public:
    virtual async_status             write_slot(int64_t lsn, lba_t lba, lba_count_t len,
                                                sisl::sg_list data) = 0;
    virtual async_result<JournalSlot> read_slot(int64_t lsn) = 0;
    virtual async_status             truncate_to(int64_t lsn) = 0;
    virtual ~CraftJournalBackend() = default;
};

// ─── CraftReplDev ─────────────────────────────────────────────────────────────
//
// Parallel to HomeStore's ReplDisk. Each CRAFT-mode volume owns one instance
// instead of the solo repl_dev. Non-CRAFT volumes are unaffected.

class CraftReplDev {
public:
    explicit CraftReplDev(volume_id_t vol_id, unique< CraftJournalBackend > journal);
    ~CraftReplDev() = default;

    // ── client-facing ──────────────────────────────────────────────────────

    // Full login sequence (leader-only). Serialized: at most one in-flight
    // login per partition at a time.
    async_result< LoginResult > login(uint64_t client_token, volume_id_t vol_id);

    // Append data to the journal at the client-assigned LSN slot. Zero-copy;
    // does NOT apply data to the LBA index.
    async_status write(uint64_t term, int64_t lsn,
                       lba_t lba, lba_count_t len, sisl::sg_list data);

    // Inline-commit up to min_commit_lsn if needed, then serve from the LBA index.
    async_result< sisl::sg_list > read(uint64_t term, int64_t min_commit_lsn,
                                       lba_t lba, lba_count_t len);

    // Apply journal entries (current_commit, lsn] to the LBA index.
    async_status commit(uint64_t term, int64_t lsn);

    // Same as commit + reset the client-timeout watchdog.
    async_status keep_alive(int64_t commit_lsn);

    // ── internal / peer API ────────────────────────────────────────────────

    // Return {commit_lsn, last_append_lsn} for the local partition.
    async_result< LSNPair > get_lsns(volume_id_t vol_id);

    // Alias of get_lsns exposed to peer servers during GetRSCommitLSN broadcast.
    async_result< LSNPair > get_rs_commit_lsn();

    // Drop all journal entries with dLSN > lsn; clear missing-set entries above lsn.
    async_status truncate(int64_t lsn);

    // Propose a SyncRSCommitLSN RAFT entry (called by watchdog or leader during login).
    async_status append(int64_t sync_to, uint64_t client_token);

    // Return raw journal data for the requested LSNs. Empty slots return
    // JournalSlot{.is_empty=true} rather than an error.
    async_result< std::vector< JournalSlot > > fetch_data(std::vector< int64_t > lsns);

private:
    // ── RAFT listener ──────────────────────────────────────────────────────
    //
    // Handles the two CRAFT RAFT entry types (SyncRSCommitLSN, InternalLogin).
    // All other HomeStore callbacks are no-ops for this backend.

    class CraftRaftListener : public homestore::repl_dev_listener {
    public:
        explicit CraftRaftListener(CraftReplDev* owner) : owner_{owner} {}

        // Dispatches on entry type; the real work is in the two apply_* helpers below.
        void on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                       std::vector< homestore::multi_blk_id > const& blkids,
                       cintrusive< homestore::repl_req_ctx >& ctx) override;

        // ── no-ops ────────────────────────────────────────────────────────
        bool on_pre_commit(int64_t, const sisl::blob&, const sisl::blob&,
                           cintrusive< homestore::repl_req_ctx >&) override { return true; }
        void on_error(homestore::ReplServiceError, const sisl::blob&, const sisl::blob&,
                      cintrusive< homestore::repl_req_ctx >&) override {}
        homestore::result< homestore::blk_alloc_hints >
        get_blk_alloc_hints(sisl::blob const&, uint32_t,
                            cintrusive< homestore::repl_req_ctx >&) override {
            return homestore::blk_alloc_hints{};
        }
        void on_destroy(const homestore::group_id_t&) override {}
        void on_start_replace_member(const std::string&, const homestore::replica_member_info&,
                                     const homestore::replica_member_info&,
                                     homestore::trace_id_t) override {}
        void on_complete_replace_member(const std::string&, const homestore::replica_member_info&,
                                        const homestore::replica_member_info&,
                                        homestore::trace_id_t) override {}
        void on_clean_replace_member_task(const std::string&, const homestore::replica_member_info&,
                                          const homestore::replica_member_info&,
                                          homestore::trace_id_t) override {}
        void on_remove_member(const homestore::replica_id_t&, homestore::trace_id_t) override {}
        void on_rollback(int64_t, const sisl::blob&, const sisl::blob&,
                         cintrusive< homestore::repl_req_ctx >&) override {}
        void on_restart() override {}
        homestore::async_status
        create_snapshot(std::shared_ptr< homestore::snapshot_context >) override { co_return homestore::ok(); }
        bool apply_snapshot(std::shared_ptr< homestore::snapshot_context >) override { return true; }
        std::shared_ptr< homestore::snapshot_context > last_snapshot() override { return nullptr; }
        int  read_snapshot_obj(std::shared_ptr< homestore::snapshot_context >,
                               std::shared_ptr< homestore::snapshot_obj >) override { return 0; }
        void write_snapshot_obj(std::shared_ptr< homestore::snapshot_context >,
                                std::shared_ptr< homestore::snapshot_obj >) override {}
        void free_user_snp_ctx(void*&) override {}
        void on_no_space_left(homestore::repl_lsn_t, sisl::blob const&) override {}
        void notify_committed_lsn(int64_t) override {}
        void on_config_rollback(int64_t) override {}

    private:
        CraftReplDev* owner_;
    };

    // Called from CraftRaftListener::on_commit after deserialising the entry type.
    void apply_sync_rs_commit_lsn(int64_t rs_commit_lsn, uint64_t client_token);
    void apply_internal_login(uint64_t client_token, uint64_t term);

    volume_id_t                  vol_id_;
    unique< CraftJournalBackend > journal_;
    CraftPartitionState          state_;
    std::set< int64_t >          missing_lsns_; // gaps between commit_lsn and last_append_lsn
    std::mutex                   missing_mu_;
    bool                         login_in_progress_{false};
    std::mutex                   login_mu_;
    CraftRaftListener            raft_listener_;
};

} // namespace homeblocks