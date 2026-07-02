# HomeBlocks CRAFT C++ API

`CraftReplDev` is a new class (parallel to HomeStore's `ReplDisk`) that each CRAFT-mode
volume owns instead of a solo `repl_dev`. It exposes the following methods, which
`CraftConnector` calls 1-to-1 when translating incoming NubloxProto RPCs.

All methods are async/coroutine-style (`async_result<T>` or `async_status`) matching the
existing HomeBlocks convention.

> **Canonical design:** the [**CRAFT Design**](https://github.com/eBay/HomeBlocks/wiki/CRAFT-Design)
> wiki page is the source of truth; this file is the C++ API reference.

---

## Per-partition in-memory state

```cpp
struct CraftPartitionState {
    int64_t  commit_lsn       {-1};  // contiguous committed prefix (== Synced)
    int64_t  last_append_lsn  {-1};  // highest appended dLSN (may be uncommitted)
    uint64_t client_token     {0};   // token from last successful InternalLogin
    uint64_t term             {0};   // current RAFT term
};
```

This state is authoritative in memory and recovered from the journal + superblock on restart.

---

## Client-facing API

### `login`

```cpp
struct LoginResult {
    std::vector<replica_endpoint> members;
    int64_t  dLSN;   // starting (per-partition) LSN for new IO
    uint64_t term;
};

async_result<LoginResult>
login(uint64_t client_token, volume_id_t vol_id);
```

Leader-only. Orchestrates the full login sequence:
1. `GetRSCommitLSN` broadcast to all peers (non-RAFT)
2. `FetchData` from an ahead peer if the leader is behind (non-RAFT)
3. Propose `SyncRSCommitLSN(rs_commit_lsn)` via RAFT
4. Propose `InternalLogin(client_token, new_term)` via RAFT
5. Return `LoginResult` after both RAFT entries commit

**Preconditions:** caller is the RAFT leader.
**Postconditions:** all quorum members have `commit_lsn == rs_commit_lsn`; all reject IOs
from any token other than `client_token`.

---

### `write`

```cpp
async_status
write(uint64_t term, int64_t lsn,
      lba_t lba, lba_count_t len, sisl::sg_list data);
```

Appends `data` to the data journal at slot `lsn`. Zero-copy required on the hot path.

Steps:
1. Reject if `term != state.term` → `ETERM`.
2. Write `data` to the journal at position `lsn` (may be out of order).
3. `state.last_append_lsn = max(state.last_append_lsn, lsn)`.
4. ACK.

Does **not** apply data to the LBA index; that happens on `commit`.

---

### `read`

```cpp
async_result<sisl::sg_list>
read(uint64_t term, int64_t readLSN, lba_t lba, lba_count_t len);
```

`readLSN` is the read horizon `H`. Returns the latest version ≤ `H` for the range: from the
LBA index if applied, or straight from the **journal-tail overlay** if only Appended (an
**overlay read**; no index write happens on the read path). The read never fetches from a
peer; the client routes only to a replica that holds every write ≤ `H` overlapping the range
(LBA-overlap eligibility, plus `Synced ≥ L`, the login dLSN).

Rejects if `term != state.term`.

---

### `commit`

```cpp
async_result<LSNPair>
commit(uint64_t term, int64_t lsn);
```

Advance the contiguous commit watermark `commit_lsn` (≡ Synced) toward `lsn`, applying
present journal entries **strictly in dLSN order** (skipping Empty slots) and reclaiming
superseded blocks. Returns the **achieved** `{commit_lsn, last_append_lsn}`: **best-effort**,
`commit_lsn` stalls just below the first Missing hole (resync fills it), which is not an
error and pauses only apply + reclaim, never reads. Readability is per-write and does not
wait for this watermark: appended entries above it are served from the journal-tail overlay,
so a higher LSN can be readable while a lower one is still a hole.

---

### `keep_alive`

```cpp
async_result<LSNPair>
keep_alive(int64_t commit_lsn, int64_t all_committed_lsn);
```

Same as `commit` plus resets the client-timeout watchdog. Returns the replica's
`{commit_lsn, last_append_lsn}` so the client can track the replica-set watermark (this feeds
the reconfig promotion gate, `commit_lsn ≥ startLSN`). `all_committed_lsn` is the set-wide
min the client computed from those responses; the replica may reclaim journal below
`min(all_committed_lsn, its checkpointed apply frontier)`. Sent periodically by the client
even during idle periods to prevent the server from triggering `SyncRSCommitLSN`.

---

### `get_lsns`

```cpp
struct LSNPair { int64_t commit_lsn; int64_t last_append_lsn; };

async_result<LSNPair>
get_lsns(volume_id_t vol_id);
```

Returns `{commit_lsn, last_append_lsn}` for the local partition. Used by peers via
`GetRSCommitLSN` during login and by the leader during `SyncRSCommitLSN`.

---

### `truncate`

```cpp
async_status
truncate(int64_t lsn);
```

Drop all journal entries with dLSN > `lsn`. Called when a replica discovers it has
entries from a previous term that did not reach quorum (new `InternalLogin` forces
a truncate of stale appended entries). Also called during login to clean up followers
whose `last_append > agreed_dLSN`.

---

## Internal / RAFT-entry API

### `append` (propose SyncRSCommitLSN)

```cpp
async_status
append(int64_t sync_to, uint64_t client_token);
```

Proposes a `SyncRSCommitLSN` RAFT entry with value `sync_to`. Callable by the leader's
watchdog, the periodic checkpoint, or a client request (e.g. after a failed sub-quorum
write). The leader first resolves every unresolved slot ≤ `sync_to` (fetch from a holder,
or an `Empty` verdict on quorum-lacks evidence) and attaches the `empty_slots[]` verdict
list; it never proposes past an unresolved slot. `client_token` is embedded so followers
can verify the entry belongs to the current session.

---

### `fetch_data` (for peer resync)

```cpp
async_result<std::vector<JournalSlot>>
fetch_data(std::vector<int64_t> lsns);
```

Returns raw journal data for the requested LSNs. Called server-to-server (not from the
client) during `SyncRSCommitLSN` apply when a replica discovers it is behind.

---

### `get_rs_commit_lsn` (for peer query)

```cpp
async_result<LSNPair>
get_rs_commit_lsn();
```

Alias of `get_lsns` exposed to peer servers during the `GetRSCommitLSN` broadcast.

---

## RAFT state machine entries

These are internal RAFT log entry types, not part of the public API.

### `SyncRSCommitLSN`

```
payload: { rs_commit_lsn: int64, client_token: uint64, empty_slots: [int64] }
```

On RAFT apply (each replica):
1. Verify `client_token` against the current session (stale-session entries are no-ops).
2. Mark every slot in `empty_slots` as `Empty`, discarding any local data held there
   (reconciliation).
3. If `last_append_lsn < rs_commit_lsn`: `fetch_data(missing non-Empty slots)` from peers
   (the leader holds every non-Empty slot ≤ the watermark).
4. `commit_lsn = rs_commit_lsn`.

### `InternalLogin`

```
payload: { client_token: uint64, term: uint64 }
```

On RAFT apply (each replica):
1. `state.client_token = client_token`
2. `state.term = term`
3. From this point, reject writes/reads whose `term` field != `state.term`.

---

## Replacing the existing API

`CraftReplDev` replaces the existing solo `ReplDev` for all volumes. The old
`async_read` / `async_write` surface in `home_blocks.hpp` (consumed by `ScstConnector`)
is superseded. `CraftConnector` is the new frontend; `ScstConnector` is removed.

| Old API (removed) | CRAFT replacement |
|---|---|
| `async_write(vol, addr, sgs)` | `write(term, lsn, lba, len, data)` |
| `async_read(vol, addr, sgs)` | `read(term, readLSN, lba, len)` |
| `async_unmap` (stub) | No equivalent in CRAFT v1 |
| — | `login`, `commit`, `keep_alive`, `truncate`, `fetch_data`, `get_lsns`, `append` |
