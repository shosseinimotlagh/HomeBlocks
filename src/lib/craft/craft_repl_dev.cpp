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

#include "craft_repl_dev.hpp"

#include <homestore/logstore/log_store.hpp> // home_log_store, logstore_seq_num_t

namespace homeblocks {

// ─── HomeStore journal backend ────────────────────────────────────────────────
//
// Production backend: wraps a HomeStore home_log_store. write_slot / read_slot
// are stubs implemented in S2 (PR #165). truncate_to() is implemented here (S4).

class HomeStoreCraftJournalBackend : public CraftJournalBackend {
public:
    explicit HomeStoreCraftJournalBackend(shared< homestore::home_log_store > logstore) :
            logstore_{std::move(logstore)} {}

    async_status write_slot(int64_t lsn, lba_t /* lba */, lba_count_t /* len */, sisl::sg_list /* data */) override {
        LOGW("HomeStoreCraftJournalBackend::write_slot lsn={} not yet implemented", lsn);
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override {
        LOGW("HomeStoreCraftJournalBackend::read_slot lsn={} not yet implemented", lsn);
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    // Drop all journal entries with seq_num > lsn; lsn becomes the new tail.
    // home_log_store::rollback(to_lsn) removes everything ABOVE to_lsn, which is exactly what we need.
    async_status truncate_to(int64_t lsn) override {
        if (!logstore_->rollback(static_cast< homestore::logstore_seq_num_t >(lsn)))
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return ok();
    }

private:
    shared< homestore::home_log_store > logstore_;
};

unique< CraftJournalBackend > make_homestore_journal_backend(shared< homestore::home_log_store > logstore) {
    return std::make_unique< HomeStoreCraftJournalBackend >(std::move(logstore));
}

// ─── constructor ──────────────────────────────────────────────────────────────

CraftReplDev::CraftReplDev(volume_id_t vol_id, unique< CraftJournalBackend > journal) :
        vol_id_{vol_id}, journal_{std::move(journal)}, raft_listener_{this} {}

// ─── get_lsns / get_rs_commit_lsn ────────────────────────────────────────────
// Snapshot the in-memory partition state. No lock needed: callers are serialised
// by login or hold the RAFT commit thread.

async_result< craft::lsn_pair > CraftReplDev::get_lsns(volume_id_t /* vol_id */) {
    co_return craft::lsn_pair{state_.commit_lsn, state_.last_append_lsn};
}

async_result< craft::lsn_pair > CraftReplDev::get_rs_commit_lsn() {
    co_return craft::lsn_pair{state_.commit_lsn, state_.last_append_lsn};
}

// ─── truncate (S4) ────────────────────────────────────────────────────────────
//
// Called only during the login sequence, while no writes are in-flight (the
// CRAFT write path is quiesced by login serialisation). Three atomic steps:
//   1. Journal rollback: drop all entries with dLSN > lsn (tail truncation).
//   2. Clamp last_append_lsn to lsn if it is higher.
//   3. Erase all missing-set entries above lsn.
// commit_lsn is not touched: the new rs_commit_lsn passed by the caller is the
// dLSN up to which RAFT consensus has RESOLVED entries. Entries above that are
// the ones being dropped. The missing set tracks gaps in [commit_lsn+1,
// last_append_lsn]; after truncation every entry > lsn is gone from both the
// journal and the missing set.

async_status CraftReplDev::truncate(int64_t lsn) {
    // Step 1: journal rollback — synchronous; fails fast on I/O error.
    if (auto r = co_await journal_->truncate_to(lsn); !r) co_return r;

    // Steps 2 + 3 under the same mutex write() uses.
    {
        std::lock_guard lk{missing_mu_};
        if (state_.last_append_lsn > lsn) state_.last_append_lsn = lsn;
        missing_lsns_.erase(missing_lsns_.upper_bound(lsn), missing_lsns_.end());
    }
    co_return ok();
}

#ifdef _PRERELEASE
void CraftReplDev::seed_lsns(int64_t last_append, std::initializer_list< int64_t > missing) {
    std::lock_guard lk{missing_mu_};
    state_.last_append_lsn = last_append;
    missing_lsns_.clear();
    missing_lsns_.insert(missing);
}
#endif

// ─── stubs (S1/S2/S3/S5/S6/S7 implement these) ───────────────────────────────

async_result< craft::LoginResult > CraftReplDev::login(uint64_t /* client_token */) {
    LOGW("CraftReplDev::login not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_status CraftReplDev::logout(craft::client_hdr /* hdr */) {
    LOGW("CraftReplDev::logout not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< craft::lsn_pair > CraftReplDev::write(craft::client_hdr /* hdr */, int64_t /* dlsn */,
                                                    uint64_t /* addr */, uint64_t /* len */, sisl::sg_list /* data */) {
    LOGW("CraftReplDev::write not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< craft::read_result > CraftReplDev::read(craft::client_hdr /* hdr */, int64_t /* read_lsn */,
                                                      uint64_t /* addr */, uint64_t /* len */,
                                                      sisl::sg_list /* dest */) {
    LOGW("CraftReplDev::read not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< craft::lsn_pair > CraftReplDev::keep_alive(craft::client_hdr /* hdr */) {
    LOGW("CraftReplDev::keep_alive not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< craft::resolution_result > CraftReplDev::request_resolution(craft::client_hdr /* hdr */,
                                                                          int64_t /* upto */) {
    LOGW("CraftReplDev::request_resolution not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_status CraftReplDev::append(int64_t /* sync_to */, uint64_t /* client_token */) {
    LOGW("CraftReplDev::append not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< std::vector< JournalSlot > > CraftReplDev::fetch_data(std::vector< int64_t > /* lsns */) {
    LOGW("CraftReplDev::fetch_data not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

// ─── RAFT listener ────────────────────────────────────────────────────────────

void CraftReplDev::CraftRaftListener::on_commit(int64_t lsn, sisl::blob const& /* header */,
                                                sisl::blob const& /* key */,
                                                std::vector< homestore::multi_blk_id > const& /* blkids */,
                                                cintrusive< homestore::repl_req_ctx >& /* ctx */) {
    // S5 will parse the entry type from `header` and dispatch to
    // owner_->apply_sync_rs_commit_lsn() or owner_->apply_internal_login().
    LOGD("CraftRaftListener::on_commit lsn={} (entry dispatch not yet implemented)", lsn);
}

// ─── RAFT apply helpers (S5 implements) ──────────────────────────────────────

void CraftReplDev::apply_sync_rs_commit_lsn(int64_t rs_commit_lsn, uint64_t /* client_token */) {
    LOGD("apply_sync_rs_commit_lsn rs_commit_lsn={} (not yet implemented)", rs_commit_lsn);
}

void CraftReplDev::apply_internal_login(uint64_t client_token, uint64_t term) {
    LOGD("apply_internal_login client_token={} term={} (not yet implemented)", client_token, term);
}

} // namespace homeblocks
