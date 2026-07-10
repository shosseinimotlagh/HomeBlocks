# craft_client: dLSN tracking, the read horizon, and the split read

This directory holds the reference CRAFT client and the ublk driver above it. The model under
`../model/` is a temporary backing store; `craft_client`, `dlsn_tracker` and `when_quorum` are the pieces
meant to graduate to production once a network shim replaces the in-memory `volume_handle`.

Everything hard about the client lives in one place: **which version of a block may a read see, and how
does that follow from writes that are still in flight?** `dlsn_tracker` answers it. This doc is the map.
The second thread running through it is what the client is allowed to *conclude* from a peer's answer —
an ack, a deterministic reject, and a timeout are three different epistemic states, and conflating the last
two is divergence.

Design source: `HomeBlocks.wiki/CRAFT-Design.md`, sections *Write, Commit, Read, and readLSN protection*,
*Two-sided read safety*, and *Split reads*.

---

## The pieces

| | |
|---|---|
| `dlsn_tracker` | The whole state machine. Assigns dLSNs, tracks each slot's fate, derives the commit frontier and the read horizon. |
| `sisl::StreamTracker<dlsn_slot>` | The spine, keyed by dLSN. Two `AtomicBitset`s (created / completed) over a flat slot array. Same primitive `home_log_store::m_records` uses. |
| `resolution_gate` | A re-armable event. Only the pathological read paths ever wait on it. |
| `when_quorum` | `when_quorum.hpp`. Fan-out that resumes at the quorum'th ack and leaves the stragglers running detached. Sibling of `sisl::async::when_all`; same `start_detached` + `value_awaitable` latch, different firing rule. |
| `craft_client` | Broadcast, quorum tally, login/redirect. Owns a `dlsn_tracker`. |
| `MemTransport` (`../model/`) | The wire, and throwaway. Owns the payload, decides deliverability, injects latency and the deadline. Not a client concern, but the write path crosses it. |

### Three numbers

```
   F   = frontier_       contiguous RESOLVED prefix.  == client_hdr.commit_lsn, == journal reclaim floor
   Ha  = highest_acked_  highest quorum-acked dLSN
   H   = read horizon    per-read (and, when they disagree, per-block)
```

The load-bearing trick is what the tracker's *completed* bit means. It means **resolved**, not "acked":

```
   slot_outcome::acked  ─┐
                         ├──> resolved  ──> completed bit set  ──> F may advance past it
   slot_outcome::empty  ─┘

   slot_outcome::in_flight ─┐
                            ├──> unresolved ──> bit stays clear ──> F PINS here
   slot_outcome::failed    ─┘
```

Which buys the invariant **every slot ≤ F is resolved**. So "is anything unresolved at or below `H`?"
collapses to `F < H`, and the common read is two atomic loads. It also means a failed write pins `F`
automatically — which is exactly the `commit_lsn` the client is supposed to piggyback, since commit must
not pass a slot the leader has not yet filled or Emptied.

---

## A write

```
 craft_client::write(addr, len, data)
        │
        │  ┌──────────────────────────────────────────────────────────────────┐
        ├─►│ tracker.reserve(addr, len)                                       │
        │  │   d = next_dlsn_.fetch_add(1)          ← dense, monotonic        │
        │  │   StreamTracker.create(d, {addr,len,in_flight})                  │
        │  │       payload ─► set active bit  (release; see note below)       │
        │  └──────────────────────────────────────────────────────────────────┘
        │        MUST happen-before the broadcast: any replica that can hold
        │        this slot implies a concurrent read's scan can already see it.
        │
        ├─► async_write(replica[0], hdr{term, F, -1}, d, addr, len, data) ─┐
        ├─► async_write(replica[1], ...)                                   ├─ when_quorum
        ├─► async_write(replica[2], ...) ─────────────────────────────────┘
        │
        │   resume at the QUORUM'th ack, or when every child has finished
        │     acks ≥ quorum ......................... outcome = acked   (stragglers run on, detached)
        │     acks < quorum, every replica rejected
        │                     deterministically ..... outcome = empty   (provably absent set-wide)
        │     acks < quorum, otherwise .............. outcome = failed  (some replica MAY hold it)
        │
        │  ┌──────────────────────────────────────────────────────────────────┐
        └─►│ tracker.resolve(d, outcome)                                      │
           │   StreamTracker.update(d, ...)                                   │
           │       outcome ─► set completed bit if resolved  (release)        │
           │   if acked:  CAS-max highest_acked_         (release; RMW chain) │
           │   if resolved: advance_frontier()                                │
           │       F' = min(completed_upto(F+1), next_dlsn-1);  CAS-max F     │
           │       every kTruncBatch: StreamTracker.truncate(F)               │
           │   gate.signal()                                                  │
           └──────────────────────────────────────────────────────────────────┘
```

Note the resolve ordering: the outcome and the completed bit are published **before** `highest_acked_`.
Because `highest_acked_` is only ever raised by a CAS (never a plain store), every publication of `Ha`
joins one release sequence, so a reader that acquire-loads *any* later `Ha` also sees the outcome of
every slot ≤ `Ha`. That is what lets the read path trust `acked` without further synchronization.

The write path never takes a mutex around an STL container. `StreamTracker`'s `shared_mutex` is taken
*shared* by `create`/`update`/`completed_upto`; only `truncate` (throttled to one per 512 resolves) and
an array resize take it exclusively.

### A non-ack is not one thing

```
  invalid_argument  the peer evaluated the request and refused it   ─┐ DETERMINISTIC:
  REPLICA_DOWN      the transport never delivered it                ─┘ it provably did not append

  timed_out         the reply outran the deadline. The peer MAY have appended it -- and in the
                    model it later does. Never deterministic; an RPC deadline is not a rollback.
  STALE_TERM        we are deposed. The client that replaced us may hold real data at this dLSN,
                    so "nobody holds it" is false.
```

Only a **unanimous** deterministic reject yields `Empty` (`refused == replicas_.size()`): then no replica
holds the slot, it is a permanent no-op, and the frontier may pass over it. A subset proves nothing and
degrades to `failed`. Counting a timeout here would advance `F` past a dLSN a peer later applies, which is
divergence. In every other respect all non-acks are equal.

### Where a write actually goes

```
 craft_client::write                      KEEPER. Copies ZERO bytes: sg_list passes descriptors.
   └─ when_quorum(N tasks, quorum())      KEEPER. Resumes at the quorum'th ack.
        │
  ══════╪═══ SHIM BOUNDARY ═══ homeblocks::async_write(volume_handle) ─► craft_replica* ═══
        │
        └─ MemCraftReplica::write         1-line delegation. Copies nothing.
             └─ MemTransport::send_write  ─────────────────────── THE WIRE (throwaway)
                  1. is_up && write_allowed?  no ─► REPLICA_DOWN ("I never delivered this")
                  2. take_payload(sg_list) ─────► ★ THE ONE BYTE COPY ★  before any suspension
                  3. plan_delivery: delay vs op_timeout
                       delay = 0        ─► co_await after(0)   ← STILL hops. Never inline.
                       delay < timeout  ─► co_await after(delay)
                       delay ≥ timeout  ─► run_after(delay, late do_write) on a service thread
                                           co_await after(timeout); return timed_out
                  └─ MemCraftReplica::do_write ───────────────── THE SERVER
                       journal_[d] ADOPTS the buffer (no second copy)
                       apply_up_to(hdr.commit_lsn) ─► index_, strictly in dLSN order
```

**The payload contract** (`craft_replica::write`, `lib/craft/craft_replica.hpp`): a replica op MUST consume
`data` before its first suspension point. `when_quorum` resumes at quorum and leaves stragglers running, so
the caller may recycle its IO buffer once every op has been *started*. The asymmetry is what makes early
return sound: the buffer must outlive every **send**, but only a quorum of **replies** is awaited. A real
transport satisfies this by construction — issuing the RPC serializes into its own send buffer.
`MemTransport::take_payload` is that serialize, and it is the only place bytes are copied on the write path.
When the network shim replaces `MemTransport`, the copy goes with it. **But this is a requirement we impose on
the transport, not a property of the universe** -- see *What the transport must provide*, requirement 6.

**The reply is the coroutine's return value, not a message.** `send_write` `co_return`s the status, so the
transport frame is on the return path, but there is no response hop and therefore no place to inject
reply-side latency, loss, or reordering. `delay` models the request leg only. The one exception is the late
delivery above, which calls `do_write` from a service thread and discards its status — correct, since the
caller already gave up.

---

## A read

```
 craft_client::read(addr, len, dest)
        │
        │  ┌──────────────────────────────────────────────────────────────────┐
        ├─►│ tracker.plan_read(addr, len)                                     │
        │  ├──────────────────────────────────────────────────────────────────┤
        │  │ (1) F = frontier_ ; Ha = highest_acked_        two atomic loads  │
        │  │     F ≥ Ha ?  ──yes──► ONE segment @ max(F,Ha).  done.           │
        │  │                        nothing unresolved ≤ H exists.            │
        │  │        │no                                                       │
        │  │        ▼                                                         │
        │  │ (2) enumerate the UNRESOLVED slots in (F, Ha]                    │
        │  │       chain completed_upto(h) ─► first non-completed idx ≥ h     │
        │  │       cost is O(#unresolved), NOT O(Ha-F)  ← straggler immunity  │
        │  │     none overlaps [addr,len) ? ──yes──► ONE segment @ Ha. done.  │
        │  │        │no                                                       │
        │  │        ▼                                                         │
        │  │ (3) per-block winner pass, ASCENDING dLSN over [umin, Ha]        │
        │  │       acked slot      ─► clears every block it covers            │
        │  │                          (highest dLSN wins per LBA: it          │
        │  │                           supersedes everything beneath it)      │
        │  │       unresolved slot ─► records itself on blocks still clear    │
        │  │                                                                  │
        │  │     ustar[b] = lowest unresolved slot covering b that sits       │
        │  │                above every acked slot covering b   (or none)     │
        │  │                                                                  │
        │  │       H_b = ustar[b] < 0  ?  Ha  :  ustar[b] - 1                 │
        │  │                                                                  │
        │  │     coalesce equal H_b into contiguous segments                  │
        │  └──────────────────────────────────────────────────────────────────┘
        │
        │   one segment                      several segments (the SPLIT READ)
        │        │                                    │
        │        │                       sisl::sg_iterator carves `dest`
        │        │                                    │
        ├────────┴────────────────────────────────────┼──────────────┐
        ▼                                             ▼              ▼
  async_read(leader, hdr, H,  addr, len, dest)   seg0 @ H_0     seg1 @ H_1   ...  (when_all)
                                                  ▼              ▼
                                             dest[0..n)     dest[n..m)
        │
        │   each replica fills its own slice in place: data extents, holes zero-filled.
        │   nothing to stitch -- only the byte count to return.
        ▼
      len
```

### Why per-block, and why that means no stalling

A replica serves, per LBA, the highest-dLSN version ≤ `H`. So an unresolved slot is only dangerous on
blocks where **no later acked write shadows it**. Reading block `b` at `H_b = ustar[b] - 1`:

* exposes no unresolved write — nothing unresolved remains at or below `ustar[b] - 1` for that block; and
* hides no acked write — the highest acked slot `A_b` covering `b` satisfies `A_b < ustar[b]`, so `A_b`
  is still visible.

Both halves matter, and they are the two violations the design names. Serving an unresolved write breaks
**read stability** (it may later be `Empty`'d, and the next read would regress). Hiding an acked write
breaks **read-after-write** (the app already waited on that ack). A single `H` sometimes cannot do both:

```
     read [block 0, block 1)          dLSN 0: covers blocks 0-1, UNRESOLVED
                                      dLSN 1: covers block 0,    ACKED

     block 0 ── winner at Ha=1 is dLSN 1 (acked)      ─► safe at H = 1
     block 1 ── winner at Ha=1 is dLSN 0 (unresolved) ─► must clamp to H = -1

     H=1  would surface dLSN 0 on block 1   ✗ read stability
     H=-1 would hide    dLSN 1 on block 0   ✗ read-after-write
     ─────────────────────────────────────────────────────────────
     split: [block 0] @ H=1  +  [block 1] @ H=-1     ✓ both hold
```

Because a safe `H_b` always exists (`ustar[b] > F`, so `H_b ≥ F`), **`in_flight` and `failed` fence
identically and neither ever blocks a read.** That is why there is no "wait for the write to resolve"
path in the common case, and why a failed write needs no leader resolution round before reads may
proceed over the range it touched. It only pins `commit_lsn`.

### The only paths that wait

`plan_read` suspends on `resolution_gate` in three pathological cases, and returns `NO_QUORUM` instead if
a `failed` slot is in the blocking set (nothing would ever wake it):

1. an index in `(F, Ha]` whose `create()` is not yet visible — its range is unknown, so it fences
   conservatively; resolves in nanoseconds;
2. `Ha - umin` past the scan cap — a write has hung;
3. the plan would fan out past `k_max_segments` (8 sub-reads).

The gate's `signal()` skips its allocation when nobody is waiting. That fast path is only sound because
`enter()` and `signal()` bracket their flag write and their state read with `seq_cst` fences — a Dekker
interlock. Without it a resolver could read `waiters_ == 0` while a reader reads pre-resolve state, and
the reader would sleep forever.

---

## What is NOT here yet

**Read routing.** Every segment above goes to `replicas_[leader_]`, and that peer's error is surfaced
directly. There is no failover, no eligibility check, and no per-replica **Missing map**.

This is a correctness gap, not just a resilience one. The tracker decides *which version* a read may see;
the Missing map decides *which replica can actually serve it*. The design calls the second half
**route-around** (`CRAFT-Design.md`, "Case (b)"):

```
   what we have                       what is missing
   ────────────                       ───────────────
   H_b  = safe horizon per block      target = a replica that HOLDS the winner at H_b

   if the leader was not in the       ┌─ leader lacks the acked N ─────────────┐
   write quorum for the acked N       │  serves M (unresolved) or older        │
   that wins block b, then at         │  ✗ exposes an unresolved write         │
   H_b = Ha it serves the wrong       │  ✗ hides a durable one                 │
   version and BOTH guarantees        └────────────────────────────────────────┘
   break.  REACHABLE TODAY: delay
   the LEADER past op_timeout_ms.     route-around: pick any member of N's write
                                      quorum -- at least one never received M.
```

The client already knows, per write, which replicas acked, so the raw material for a Missing map exists;
it is just not kept. Until it is, `read()` is correct **only when the leader is in the write quorum of
every winner in the range** — which the model's default configuration always satisfies, and which
`force_subquorum`, or delaying the leader past the deadline, violates.

This is measured, not theorised. Delay the leader, write `0xbb`, get the ack (quorum on the two followers),
lift the delay, read back — `0xaa`. The write was reported durable to the application and the very next read
returned the old value. Delaying a *follower* is safe, because the leader is in the ack set of every write
the client committed.

Also outstanding, in rough order:

- broadcast `keep_alive` to all replicas → the true set-wide `all_committed_lsn` (today we send `-1`,
  deliberately: our own frontier is an upper bound on the minimum, never the minimum, and sending it
  would let a replica reclaim journal a lagging peer still needs);
- a `keep_alive` timer, so the server watchdog sees the client;
- re-login on `STALE_TERM`, and `resolve_upto()` to retire a `failed` hole after a leader resolution
  round (there is no public verb for one today);
- ublk `DISCARD` / `WRITE_ZEROES` → a CRAFT zero write (empty `sg_list`);
- reply-side fault injection. The wire models the request leg only: `plan_delivery` conflates the round trip
  into one `delay`, and the reply is the coroutine's return value rather than a message. A peer that applies
  a write and then loses or delays its *reply* needs `request_latency` and `reply_latency` as separate legs.

Writes now ack at quorum (`when_quorum`); reads do not, because they are unicast. A read that the leader
cannot serve has no failover — that is the Missing map above, not a fan-out problem.

---

## What the transport must provide

There is no transport yet, and picking one now would be fitting the peg before we know the hole. Instead the
shim imposes the **weakest execution model any plausible transport could offer**, and whatever `craft_client`
needs in order to survive it becomes a requirement we can shop for. This section is that list. It is the
output of the exercise, not an input to it.

`MemTransport` therefore refuses to be convenient. It completes **no** op inline with its submit, dispatches
completions from a pool of service threads so replies to one broadcast land **out of order and on different
threads**, and lets K client threads drive one `craft_client` at once. None of that was true before: the
fan-out ran sequentially on the issuing thread, `when_quorum` never suspended, and a straggler never raced
anybody. A shim that comfortable teaches the client nothing.

| # | requirement | why the client needs it | who checks |
|---|---|---|---|
| 1 | A reply may arrive on **any** thread, never the issuer's | `sisl::async::task` resumes wherever the RPC completed. Nothing in the client may capture a shard key across a `co_await`. | the service pool |
| 2 | Replies to one broadcast arrive in **any order** | `when_quorum` resolves on the quorum'th ack, whichever peer that is; the rest keep running detached. | K > 1 service threads |
| 3 | **One client, many caller threads** | ublk picks a queue by CPU, not by LBA, so every queue thread shares one partition's dLSN space. | `N3_ConcurrentWritersFromManyThreads` |
| 4 | A peer that **times out may still have applied** the write | Counting it as a deterministic reject resolves the slot `Empty` and advances `F` past a dLSN a replica later applies. Divergence. | `ClearingADelayLeavesAMissingSlotThatDrains` |
| 5 | `REPLICA_DOWN` means **"I never delivered it"** | Only the transport can say that; a down server cannot answer for itself. It is what lets the client count it as a deterministic reject. | `N3_AllReplicasRefuseResolvesEmptyAndReleasesTheFrontier` |
| 6 | **Either** serialize `data` before suspending, **or** signal when the send completes | The client acks at quorum with stragglers in flight, so the caller's buffer must outlive every *send* while only a quorum of *replies* is awaited. | `N3_StragglerWriteLandsIntactAfterTheClientReturned` |
| 7 | The reply must be **reapable on the issuing queue's ring** | The ublk per-IO coroutine may only be resumed by its own queue thread (see #1: the client migrates coroutines by itself). If the transport cannot deliver its reply as a CQE on that queue's io_uring, the driver must bridge with an fd the queue already polls. | `CraftUblkDisk::queue_service` |

Requirement 7 is what the driver is built around. `queue_service` parks a coroutine on `POLL_ADD` over the
ublk queue's own ring, watching an eventfd; a CRAFT completion on any thread publishes its result into the
IO's tag slot and kicks it. **The eventfd is the model's stand-in for a socket.** A real transport polls its
connection fd in exactly that loop and parses the reply on the queue thread; it will still index its landing
pad by request id, which is what a tag already is, so `craft_service_loop` does not change. This is also why
`craft_ublk` needs no iomgr: ublkpp runs one OS thread per hardware queue over that queue's io_uring, and
that is the whole runtime.

The landing pad deserves a word, because it is the reason the driver takes no lock. A ublk tag is *"unique in
queue wide"* and carries at most one IO, hence at most one completion, at a time — the same invariant that
lets ublkpp pre-reserve `async_io::_pool` per tag. So the tag space **is** the queue: `queue_service` holds a
flat `completion_slot[q_depth]`, the producer release-stores its `cqe_state*` into `slots[tag]`, and the
single consumer acquire-exchanges it back out. No ring, no MPSC queue, no per-completion `std::function`
allocation, no mutex. One wake drains a whole batch; the cost is an O(`q_depth`) scan of relaxed loads, which
at ublkpp's default `--qdepth` of 128 is 128 loads amortized over however many completions that wake carried.

Requirement 6 is the one to watch, because the shim currently **grants it for free** and that is a choice, not
a fact. `MemTransport::take_payload` copies at issue, before any suspension, so today's client may recycle its
buffer the moment every replica op has been started. A transport that submits an async `SEND` from the
caller's pages (io_uring, `MSG_ZEROCOPY`, RDMA) **cannot** offer that: the wire is still reading those pages
until its send completion, and it would owe the client a signal saying so. libiscsi sidesteps the problem by
pushing the PDU out inline (`iscsi_service(ctx, POLLOUT)`) before it suspends, and by never acking early.

So when a transport is chosen, requirement 6 is a checklist item with exactly two acceptable answers:

```
   (a) the stub serializes into its own send buffer before its first suspension point
           -> today's contract holds unchanged, zero client work

   (b) the stub submits asynchronously from the caller's pages
           -> it owes a per-op send-complete signal, distinct from the reply, and
              craft_client must gate its return on  (all sends done) && (quorum acked)
```

Note the signal belongs on the **client stub**, not on `craft_replica` / `homeblocks::async_write`. That is the
*server* surface: by the time `CraftConnector` rematerializes an RPC onto it, the payload already sits in the
server's receive buffer and there is nothing to signal. The reason the distinction is easy to miss is that the
model **fuses** both roles into `MemCraftReplica` -- it is simultaneously the client's link and the server's
replica -- which is precisely the seam a real transport will occupy.

## Concurrency notes

* **Keyed on dLSN, never on thread.** `sisl::async::task` resumes on whatever thread completed the last
  replica's RPC, so a shard key captured before a `co_await` is a data race after it.
* **Stale reads only ever fence more.** dLSNs are dense, so every index in `(F, Ha]` was issued before
  `Ha` was published. A scanner may therefore walk that range and treat anything it cannot positively see
  as resolved *as* unresolved. A side index of in-flight ranges would not have this property — it can be
  read as empty while an entry is concurrently being claimed, and nothing orders those two events.
* **The slot bit publishes the payload.** Two `shared_lock` holders establish no ordering between them,
  so a scanner reaches a slot only via its bit. `sisl::AtomicBitset` therefore sets bits with **release**
  and reads them with **acquire** (`safe_bits` in `bitword.hpp`); with relaxed ops the scanner has no
  acquire edge to the `addr`/`len` it just observed, which is a real data race — the stress test reports
  ~12 of them under TSAN if you weaken the ordering. On x86-64 the ordering is free: `fetch_or` is already
  a locked RMW, and the acquire load and release store are plain `mov`s.
  Standalone `atomic_thread_fence` would be equally correct and **equally invisible to TSAN**, which does
  not model fences (GCC warns `-Wtsan`). Fence-based publication reports a false race on every scan, which
  gets suppressed, which then hides the real ones. Keep the ordering on the ops.
  > **Running TSAN today?** The `safe_bits` release/acquire change is still an unmerged sisl PR, so a local
  > checkout has the relaxed ops and `DlsnTracker.ConcurrentPlanAndResolveIsRaceFree` reports those 12 races
  > in `read_slot`. They are expected until that PR lands, and are unrelated to anything in this directory.
* **`outcome` is the only mutable field** in `dlsn_slot`; every access goes through `std::atomic_ref`.
  `addr`/`len` are written once, before the bit is set, so plain reads are safe once the bit is observed.
* **`when_quorum`'s results vector is readable IFF `acks < quorum`.** The latch fires on exactly two
  triggers: the quorum'th ack (stragglers may still be writing into `results`) or the last child finishing
  (nothing is). Fewer than `quorum` acks means the first cannot have fired, so the second did. The quorum
  path reads only the ack count. This is why the `Empty`-vs-`failed` decision, which needs every replica's
  error, lives on the sub-quorum path.
* **A completion NEVER resumes on the issuing thread.** Every op suspends on a `sleep_event` completed by one
  of the addressed replica's own server threads, even at zero injected latency, because no real transport
  delivers a reply inline with its submit. Each replica has its own pool (`--num_threads`, per replica), so it
  may reply to two of its own requests out of order, and different replicas are independent. Reordering across
  peers is what quorum depends on.
* **The driver hands the result back to its own queue, never resumes it in place.** `run_craft_io` finishes on
  whatever thread the CRAFT completion landed on, release-stores the result into that IO's
  `queue_service::slots[tag]`, and kicks an eventfd. The queue's service-loop coroutine is parked on `POLL_ADD`
  over the queue's own ring, so `run_queue_loop` resumes it *there* and the per-IO `cqe_state` is resumed on the
  queue thread. It has to be: `resolve()` ends in `gate_.signal()`, which resumes a suspended reader inline on
  the resolving thread, so a read issued on queue A can finish on queue B. Drain order is load-bearing —
  `read(evfd)` **before** the slot scan, because a post that lands after the scan writes the level-triggered
  eventfd after our read, and so re-arms the next `POLL_ADD`. Reading second would lose that wakeup.
* **The driver's completion path takes no lock and allocates nothing.** `slots[tag]` is a single-producer /
  single-consumer cell, and the tag guarantees no two producers ever pick the same one. The producer's
  release-store of `state` publishes the plain `result` beside it; the consumer's acquire-exchange pairs with
  it and claims the slot. The relaxed load that filters an empty slot is not a synchronization edge and does
  not need to be, since it only ever decides whether to *skip*.

### Where the locks are

Nothing under `client/` declares a mutex. Every lock a 4 KiB write to three replicas touches lives in the
**model**, and each one is standing in for something a real deployment does not do in-process:

| lock | acquisitions per write | what it really is |
|---|---|---|
| `dlsn_tracker`, `craft_client`, `CraftUblkDisk` | **0** | — |
| `StreamTracker`'s `shared_mutex` | 2, both *shared* | inside sisl; exclusive only on `truncate` (1-in-512) and on resize |
| `MemTransport::mu_` | **0** | guards `by_id_`/`leader_`/`term_` only. Faults are read off a copy-on-write snapshot (`faults()`), so the IO path is two acquire loads |
| `replica_service::mu` | 6 (post + pop, per replica) | the model's stand-in for a NIC. A real transport submits an SQE here |
| `MemCraftReplica::mu_` | 3 (one journal insert each) | the *server*, on the far side of the wire. Not the client's cost |

Both remaining entries are the shim's own bookkeeping, charged to the shim's threads, not the client's.

### Do not benchmark the shim

`craft_ublk`'s IOPS is a property of `MemTransport`, not of CRAFT. Quoting it as a CRAFT number is a category
error, and "optimizing" it is worse: the shim's job is to *impose* a network hop and a serialization layer, so
a shim that got fast would be a shim that stopped modelling the thing it exists to model.

A zero-delay send costs a `make_shared<sleep_event>`, a `std::function` heap allocation, a `std::multimap`
insert keyed on a `time_point` that is always *now*, a mutex, and a `notify_one` futex -- once per replica, on
the issuing queue thread. A real transport's submit is a memcpy into the send buffer plus an SQE, with no
syscall until the batch. **The shim overstates submit cost by roughly an order of magnitude.** That is the
price of moving a request into another execution context in-process, where the "other machine" has to be a
thread. The *shape* is right (a submit costs something, and it costs it on the issuing thread); the magnitude
is not, and no design decision should be made from it.

If you must measure, measure **CPU per IO** -- it does not care how many threads a design throws at the
problem, which aggregate IOPS very much does. At 3 replicas, 4 KiB, QD32 this path costs roughly 33 us/IO and
3.0 context switches per write, and 9.5 us/IO and 0.7 switches per read. The write figure of 3.01, which holds
regardless of queue count, is the model: one wake per replica, three replicas, each request crossing into
exactly one other execution context. Nothing else moves.

Two traps. A single `fio` job submits from one CPU, so ublk steers all of its IO to **one** hw queue and
`--nr_hw_queues` changes nothing; at QD32 you are reading `iops = 32 / latency` off a single queue thread. And
the completion side already coalesces: under load the queue thread never sleeps, so N completions arriving
between two `POLL_ADD`s cost one wake rather than N. Coalescing the eventfd kick would buy nothing.

## Observability: the craft_ublk REST endpoint

`craft_ublk --http_port <n>` (0, the default, disables it) stands up a read-only `sisl::HttpServer` over the
running replica set, so the commit frontier can be watched advancing while fio drives `/dev/ublkbN`. The
implementation is `../admin/craft_admin_http.cpp`, the one TU in this tree that pulls in `sisl::http` and
`nlohmann` -- the model and the tracker stay free of both, which is what keeps `test_craft_memory` and
`test_craft_dlsn_tracker` linkable without HomeStore.

```
sudo ./craft_ublk --vol_size_mb 1024 --replicas 3 --http_port 8080

watch -n1 'curl -s localhost:8080/api/v1/status | jq "{c: .client, r: [.replicas[] | \
  {index, commit_lsn, last_append_lsn, missing_count, commit_lag}]}"'
```

| route | what it reports |
|---|---|
| `GET /api/v1/status` | everything below in one document; the one to `watch` |
| `GET /api/v1/client` | the client's `F` / `Ha` / issued dLSN + the unresolved slots pinning `F` |
| `GET /api/v1/cluster` | leader, member count, `op_timeout_ms`, and the injected fault sets |
| `GET /api/v1/replicas` | one object per replica, in handle order |
| `GET /api/v1/replica?id=` | one replica, by index (`?id=2`) or uuid |
| `PUT /api/v1/replica/delay?id=&ms=` | inject straggler latency (localhost only) |

Handlers run on httplib's own thread, never on an iomgr reactor, so they only call the plain snapshot
accessors (`MemCraftReplica::stats`, `craft_client::dlsn_stats`). They never `co_await` and never `sync_get`.
The routes are literal paths on purpose: a parameterized resource would register its *pattern* in the
server's safelist while `do_auth()` tests the concrete request path against it, so `?id=` rather than `/:id`.

### Reading a lagging replica

Three numbers describe how far a replica trails, and you need all three, because each is blind somewhere:

```
   uncommitted_tail = last_append_lsn - commit_lsn      its own journal tail
   commit_lag       = client.commit_lsn - commit_lsn    how far its frontier trails the client
   append_lag       = client.issued_dlsn - last_append_lsn
   missing_count    = dLSNs in (commit_lsn, last_append_lsn] with no journal entry
```

`missing_count` counts **Missing** slots, the ones `apply_up_to` stalls on. It is **not** a count of holes:
a hole is a resolved reads-as-zero range, and this endpoint says nothing about them.

The trap: a replica that missed a *suffix* of writes reports `missing_count == 0`. `MemTransport::send_write`
returns `REPLICA_DOWN` before anything reaches the journal, so `last_append_lsn` simply never advanced and
there is no gap below it. Missing slots only materialize once a **later** write lands on top of the skipped
ones. So:

```
   force_subquorum({r0, r1})   writes skip r2 entirely            (C++ only, as test_craft_client does)
        r2:  commit_lsn 2   last_append 2   missing 0   commit_lag 3   ← lag shows only here

   clear_faults(); one more write
        r2:  commit_lsn 2   last_append 5   missing 2   [3, 4]         ← now the gap is visible
```

Neither number alone tells the story; together they cover both shapes. `set_up` / `force_subquorum` /
`clear_faults` are reachable only from C++; the delay below is the one fault the endpoint can inject.

### Injecting a straggler

`MemTransport` can add per-replica latency, and it enforces a transport-wide deadline (`--op_timeout_ms`,
0 = wait forever). Together they reproduce a sub-quorum the way production will meet one, rather than by
dropping writes on the floor: an op that outruns the deadline returns `std::errc::timed_out` to the client,
**and the peer still receives and applies it once the delay elapses**. That is what an RPC deadline does, and
it is the only way a replica's journal fills out of order.

A `timed_out` peer is simply not an ack (see *A non-ack is not one thing*): quorum is met by the other two,
the slot resolves `acked`, and `when_quorum` returns **without waiting for the straggler at all** — the
delayed replica costs the write nothing, not even `op_timeout_ms`. The deadline lives entirely below the
client, in the transport, which is where the real one will live too.

```
sudo ./craft_ublk --replicas 3 --http_port 8080 --op_timeout_ms 500

curl -XPUT 'localhost:8080/api/v1/replica/delay?id=2&ms=20000'   # straggler appears
curl -XPUT 'localhost:8080/api/v1/replica/delay?id=2&ms=0'       # straggler recovers, mid-IO
```

Raising the delay only produces uniform lag (`commit_lag` climbs, `missing_count` stays 0). Clearing it is
what reorders arrivals, because writes issued during the delay land *after* writes issued once it is gone:

```
    delay=20s     r2: commit_lag  36  missing  0            in-order, just late
    delay=0       r2: commit_lag  37  missing 35  [2187...]  <- late writes still in the transport
                  r2: commit_lag 143  missing 24  [2198...]     frontier pinned under the first hole
                  r2: commit_lag 357  missing  2  [2220...]
                  r2: commit_lag   1  missing  0             <- last gap fills, frontier leaps
```

**Delaying the LEADER is a read-after-write violation, on purpose.** Reads are unicast to the leader and
there is no per-replica Missing map (see *What is NOT here yet* above), so a leader that missed an acked
write serves the old version of the LBAs that write covered. The window opens when its delay is lifted and
the late writes are still queued: reads stop timing out but the data has not arrived. Measured, not theorised:
write `0xbb`, get an ack, read back `0xaa`. The endpoint logs a warning when you do it. It is the sharpest
demonstration of why the Missing map is a correctness gap and not a resilience nicety. Delaying a *follower*
is safe, because the leader is in the ack set of every write the client committed.

Two properties survive it: no divergence (the late write carries a lower dLSN than the slots stacked above
it, and `apply_up_to` applies in dLSN order once the gap fills, so every replica converges), and no
read-stability violation (the value goes old to new, never backwards).

## Tests

| target | what it pins down |
|---|---|
| `test_craft_dlsn_tracker` | The tracker in isolation: frontier fold, failed-slot pin, fast path, clamp, shadow, split, fan-out stall, straggler, `stats()`'s unresolved-slot walk, plus a concurrent reader/writer stress. Links **without** `homestore::homestore`. |
| `test_craft_client` | The same rules end to end against the model, including the case where the leader physically holds a sub-quorum write. Plus quorum ack: a straggler delayed past the deadline costs the write nothing; its write still lands, with the bytes as of *issue* time (the payload contract — the test overwrites the live buffer rather than freeing it, so a replica that kept the caller's `sg_list` delivers `0xFF` and fails, no sanitizer needed); a unanimous deterministic reject resolves `Empty` and releases `F`, while a partial one pins it. |
| `test_craft_memory` | The model, including `stats()` (Missing-slot counting across a journal gap, the sample cap, zero-write / `mapped_blocks` accounting) and latency injection (a delay under the deadline merely slows an op; a delay past it times out yet still lands, leaving a Missing slot that drains). |

```
ctest --test-dir build/Release -R Craft

# The stress test is the one that needs a sanitizer to say anything:
./build/Sanitized-thread/src/test/craft/test_craft_dlsn_tracker
```
