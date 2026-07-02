# CRAFT RPCs

8 RPCs total. 4 client↔server, 4 server↔server (2 via RAFT, 2 non-RAFT).
RAFT internal RPCs (heartbeat, vote, membership) are not listed here.

> **Canonical design:** the [**CRAFT Design**](https://github.com/eBay/HomeBlocks/wiki/CRAFT-Design)
> wiki page is the source of truth; this file is the RPC wire-format reference.

---

## Client → Server

### 1. Login (Unicast to leader)

```
Request:  { client_token: uint64 }
Response: { members: [endpoint], dLSN: int64, term: uint64 }
```

Client sends to the RAFT leader. Leader runs the full login orchestration sequence
(GetRSCommitLSN → optional FetchData → SyncRSCommitLSN RAFT → InternalLogin RAFT)
and responds once both RAFT entries commit.

`client_token` is an opaque 64-bit identity token. `dLSN` is the per-partition LSN, the only
LSN CRAFT carries.

HomeBlocks handler: `CraftReplDev::login()`

---

### 2. Write (Broadcast to all replicas)

```
Request:  { term: uint64, lsn: int64, lba: uint64, len: uint32, data: bytes }
Response: { status: Status }
```

Client sends to every replica in the set in parallel. Each replica appends `data` to its
data journal at slot `lsn` and ACKs immediately. Write is durable once quorum ACKs.
Data is **not** readable until committed.

Zero-copy is required: `data` must not be copied during journal append.

HomeBlocks handler: `CraftReplDev::write()`

---

### 3. Read (Unicast to chosen replica)

```
Request:  { term: uint64, readLSN: int64, lba: uint64, len: uint32 }
Response: { status: Status, data: bytes }
```

`readLSN` is the read horizon `H` (the client's contiguous quorum-acked prefix); the read
reflects writes ≤ `H`. The client picks an *eligible* replica: one filled to the login dLSN
`L` (`Synced ≥ L`) whose Missing set has no slot ≤ `H` overlapping `[lba, lba+len)`. The
replica returns the latest version ≤ `H` for the range, from the LBA index if applied or
straight from the journal-tail overlay if only appended (an **overlay read**; no index write
on the read path). A replica **ignores any write above `H` even if it holds it** (the
sub-quorum tail), which is the server-side half of read safety. Because the client only routes
to a servable
replica, **the read path never fetches from a peer**; large multi-LBA reads may be split
across replicas.

HomeBlocks handler: `CraftReplDev::read()`

---

### 4. Commit (Broadcast to all replicas)

```
Request:  { term: uint64, lsn: int64 }
Response: { status: Status, commit_lsn: int64, last_append_lsn: int64 }
```

Tells replicas to advance `commit_lsn` to `lsn`. May be piggybacked on the next
Write or KeepAlive instead of sent as a standalone RPC. The response carries the **achieved**
`{commit_lsn, last_append_lsn}` (best-effort: `commit_lsn` stalls below the first Missing hole).

HomeBlocks handler: `CraftReplDev::commit()`

---

### 5. KeepAlive (Broadcast to all replicas)

```
Request:  { commit_lsn: int64, all_committed_lsn: int64 }
Response: { status: Status, commit_lsn: int64, last_append_lsn: int64 }
```

Advances `commit_lsn` (in-order apply) and resets the per-replica client-timeout watchdog.
The response carries `{commit_lsn, last_append_lsn}` (feeds the client's Missing map and the
reconfig promotion gate); the request carries `all_committed_lsn`, the set-wide min the
client computed from those responses, letting each replica reclaim journal opportunistically
below it. Sent periodically during idle periods and after every quorum-acknowledged write.

HomeBlocks handler: `CraftReplDev::keep_alive()`

---

## Server → Server (non-RAFT)

### 6. GetRSCommitLSN (Broadcast, initiated by leader)

```
Request:  { term: uint64, my_commit_lsn: int64, my_last_append_lsn: int64, is_login: bool }
Response: { term: uint64, commit_lsn: int64, last_append_lsn: int64 }
```

Leader sends to all peers to collect their current LSN state before a `SyncRSCommitLSN`
RAFT proposal. Used during login and on timeout. `is_login=true` (the login poll) makes the
peer **quiesce prior-session writes** before reporting `last_append` (the fencing barrier);
watchdog / periodic polls carry `is_login=false` and never quiesce (they ride the live tail).
Only `last_append_lsn` feeds the recovery watermark; `commit_lsn` is a contiguity
certificate that bounds the leader's hole-resolution window, seeds `all_committed_lsn`
(journal reclaim) when no client is attached, and carries the reconfig promotion gate.

HomeBlocks handler: `CraftReplDev::get_rs_commit_lsn()` / `get_lsns()`
Dispatched by: `CraftConnector` (inter-node channel, non-RAFT)

---

### 7. FetchData (Unicast, from behind replica to an ahead peer)

```
Request:  { lsns: [int64] }
Response: { slots: [{ lsn: int64, is_empty: bool, lba: uint64, len: uint32, data: bytes }] }
```

Called when a replica discovers it is missing data for certain LSNs after receiving a
`SyncRSCommitLSN` RAFT entry. **Three-way per slot:** an entry with `is_empty=false` carries
`data`; `is_empty=true` means the peer has **positively** marked that slot `Empty` in a prior
resync; a requested `lsn` **omitted** from `slots` means *not-present-here*. A peer never
returns `is_empty=true` for a slot it merely lacks. The `Empty` **verdict itself is
leader-only**: the leader runs the broadcast-and-accumulate quorum procedure (itself
included; non-responders never count) before proposing `SyncRSCommitLSN`, and distributes
verdicts in that entry (`empty_slots[]`). Lagging replicas fetch present slots and obey the
verdict list; they never declare `Empty` unilaterally (see the wiki's Resync section for the
intersection argument and the Empty-beats-held-data reconciliation rule).

HomeBlocks handler: `CraftReplDev::fetch_data()`
Dispatched by: `CraftConnector` (inter-node channel, non-RAFT)

---

## Server → Server (RAFT)

### 8. SyncRSCommitLSN (RAFT proposal, from leader)

```
RAFT entry payload: { rs_commit_lsn: int64, client_token: uint64, empty_slots: [int64] }
```

Proposed by the leader via `CraftReplDev::append()`. **Before proposing**, the leader
resolves every unresolved slot ≤ `rs_commit_lsn`: fetch it from any holder, or record an
`Empty` verdict on quorum-lacks evidence; it never proposes past an unresolved slot. On
RAFT commit each replica applies the entry: verify the token against the current session,
mark `empty_slots` as permanent no-op holes (discarding any local data in them), fetch the
remaining missing slots from peers (the leader is guaranteed to hold every non-Empty slot ≤
the watermark), then advance `commit_lsn`. Replicas never declare `Empty` unilaterally.
This is the primary recovery mechanism — it carries no write data, only the watermark and
verdicts.

---

### 9. InternalLogin (RAFT proposal, from leader during login)

```
RAFT entry payload: { client_token: uint64, term: uint64 }
```

Proposed by the leader after `SyncRSCommitLSN` commits. On apply each replica stores
`client_token` and `term`, rejecting any subsequent IO from a different token. Proposed
immediately after the `SyncRSCommitLSN` entry during the login sequence.

---

## RPC Transport

The transport layer for NubloxProto RPCs is decided by the **CRAFT-1 spike** (SDSTOR-22297
dependency). `CraftConnector` is transport-agnostic: it will dispatch via whatever channel
CRAFT-1 selects (likely gRPC or a custom framing over TCP). Server-to-server RPCs (6 and 7)
use the same transport.

During development, before CRAFT-1 lands, `CraftConnector` can use direct C++ function
calls or a stub transport for unit/integration testing.
