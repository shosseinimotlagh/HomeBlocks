# CRAFT — Client Assisted RAFT

**CRAFT** (Client Assisted RAFT) is the replication protocol for HomeBlocks block volumes. It
separates the data path from the consensus path: clients broadcast writes directly to all
replicas at client-assigned LSNs, while RAFT is used only for leader election, login
synchronization, and recovery bookkeeping. Write data never flows through the RAFT log.

> **Canonical design:** the full, up-to-date CRAFT design is the
> [**CRAFT Design**](https://github.com/eBay/HomeBlocks/wiki/CRAFT-Design) wiki page, which is
> the source of truth. This folder holds reference detail the wiki summarizes: RPC wire
> formats ([rpcs.md](rpcs.md)), the C++ API ([api.md](api.md)), and the implementation work
> breakdown ([subtasks.md](subtasks.md)).

## Documents

| File | Contents |
|---|---|
| [protocol.md](protocol.md) | Pointer to the canonical [CRAFT Design](https://github.com/eBay/HomeBlocks/wiki/CRAFT-Design) wiki page |
| [api.md](api.md) | HomeBlocks C++ CRAFT API (`CraftReplDev` methods) |
| [rpcs.md](rpcs.md) | All 8 RPCs (client↔server and server↔server) |
| [states.md](states.md) | Pointer to the canonical wiki page (slot states, read eligibility) |
| [subtasks.md](subtasks.md) | Implementation sub-task breakdown (SDSTOR-22382 children) |

## Glossary

| Term | Definition |
|---|---|
| **CRAFT** | Client Assisted RAFT — the HomeBlocks replication protocol |
| **dLSN** | Data LSN — a monotonically increasing sequence number in the **data journal** of a single partition/replica-set. Per-volume in HomeBlocks (a volume maps to one partition). |
| **gLSN** | Global LSN — monotonically increasing across all partitions of a volume. |
| **rLSN** | RAFT LSN — the index within the RAFT log. Distinct from dLSN. |
| **term** | RAFT term number, incremented on every new client login. Used by replicas to reject stale IOs. |
| **commit_lsn (≡ Synced)** | The contiguous committed prefix: every dLSN ≤ it is applied to the state machine and readable (Empty slots skipped). Readability is per-write, so higher writes can be materialized on demand (CommitAndRead). |
| **last_append_lsn** | Highest dLSN whose data has been written to the data journal (possibly not yet committed). |
| **Replica Set (RS)** | The set of HomeBlocks nodes that hold copies of one partition. Typically 3 nodes. |
| **Partition** | A contiguous region of a Volume, replicated across one Replica Set. In HomeBlocks, partition ≈ volume. |
| **CraftReplDev** | New HomeBlocks replication device class (parallel to `ReplDisk`) that implements CRAFT. |
| **CraftConnector** | New HomeBlocks RPC frontend (parallel to `ScstConnector`) that translates NubloxProto RPCs to `CraftReplDev` API calls. |
| **SyncRSCommitLSN** | A RAFT log entry type. On apply, each replica fetches any missing data up to the encoded dLSN and advances `commit_lsn`. |
| **InternalLogin** | A RAFT log entry type. On apply, stores the new `client_token` and enforces single-writer exclusivity. |
| **Missing** | A dLSN slot that a replica knows about (from a peer or from the RAFT log) but has not yet received data for. |
| **Empty** | A dLSN that was never received by any replica and is not discoverable during resync. Treated as a no-op hole. |

## Key design properties

- **Single writer**: only one client at a time owns a partition (enforced by `InternalLogin` RAFT entry).
- **Leaderless data path**: after login, the RAFT leader has no special role for writes or reads.
- **Client drives commit**: replicas do not commit until told (via `commit` / `keep_alive`). Readability is per-write: a read materializes the entries it touches (CommitAndRead).
- **Client-routed reads**: reads are unicast, chosen by LBA-overlap against the client's per-replica Missing map (plus `Synced ≥ L`, the login dLSN). The read path never fetches from a peer; fetch is resync-only.
- **Merge key, not serialization**: overlapping writes need no ordering; highest dLSN per LBA wins on every replica.
- **Reconfiguration leans on HomeStore**: `replace_member` / learner / snapshot / CP (see the wiki and subtasks S10).
- **Server-side resync**: `SyncRSCommitLSN` lets replicas catch up from each other without client involvement.
- **No HomeStore changes needed**: `CraftReplDev` is built entirely on top of existing HomeStore journal/index/block primitives.
- **Full replacement**: `CraftReplDev` replaces the existing solo `ReplDev` for all volumes. There are no non-CRAFT (ReplDisk/solo) volumes in the final design.
