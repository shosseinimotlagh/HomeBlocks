# CRAFT transport (TCP binding behavior)

Status: design. Companion to `wire.md`. That document is the on-wire encoding; this one is the connection and
session BEHAVIOR of the first real binding, TCP over io_uring: lifecycle, deadlines, liveness, failure
detection, reconnect, and retry. `wire.md` says what the bytes are; this says how a client and a replica keep
a session alive and recover it.

## Model: NVMe/TCP, adapted

The reference is NVMe over Fabrics (NVMe/TCP), not iSCSI. It is the modern block-over-fabric transport, and
`wire.md` already borrows its framing (self-delimiting PDU-style messages, `HDGST`/`DDGST`, an
`ICReq`/`ICResp` connection init). The behavior below borrows the rest of its shape and drops what CRAFT does
not need.

| NVMe/TCP | CRAFT | note |
|---|---|---|
| association (host <-> controller) | session (client <-> replica set) | CRAFT's session is SET-WIDE: one `term`, established by the leader's `InternalLogin`, spanning N replicas. NVMe's is per-controller |
| admin-queue Connect | `LOGIN` | establishes the session and returns geometry |
| IO-queue Connect | `HELLO` | binds an additional connection to the session |
| Command Identifier (CID) | `request_id` | per-connection; not reused until the request completes |
| Keep Alive Timeout (KATO) | session watchdog + the keep_alive drive | a replica tears the session down if it hears nothing within the timeout |
| reconnect the association | reconnect + re-`HELLO` | on a transport error; simpler than iSCSI's ERL 0/1/2 |
| no `CmdSN` | no sequence numbers | NVMe commands are independent; CRAFT ops are idempotent, so neither needs iSCSI-style command sequencing |

What NVMe/TCP has no concept of, and CRAFT adds, is **quorum fan-out**. The client holds one connection per
`(queue, replica)` and drives a write across all replicas' connections, acking at quorum. So one CRAFT IO
spans N connections, and a per-connection failure is a per-**replica** failure the quorum tolerates -- not,
by itself, an IO failure. That is the CRAFT layer on top of the NVMe/TCP transport model.

## Terminology

- **Session**: the set-wide `(client_token, term)` established by `LOGIN`; every IO is fenced by `term`.
- **Connection**: one TCP connection, bound to the session, serving one `(queue, replica)`.
- **`request_id`**: the per-connection correlator in every message (`wire.md`), derived from the ublk tag but
  with its own lifetime (below).

## Connection topology

The client holds a grid of `nr_hw_queues x N` sockets: each ublk queue opens one connection to every replica
(writes fan out to the whole set for quorum), so each queue holds N sockets and each replica has
`nr_hw_queues` of them landing on it. The two axes are different things:

- **Queue axis (`nr_hw_queues`)** -- parallelism, the NVMe/TCP multiple-IO-queue model: one TCP connection
  each, for core-affinity and throughput, all to the same replica set. Not multipath; a queue's socket is not
  an alternate path to another queue's.
- **Replica axis (N)** -- replication. Writes fan out to all N for quorum; reads pick one eligible replica and
  fail over (the only multipath-flavored part). The replicas are consensus copies, not paths to one store.

Exactly one connection does `LOGIN` -- to the leader, establishing the set-wide session `term` -- and every
other connection in the grid does `HELLO` to bind to it: `1 LOGIN + (nr_hw_queues x N - 1) HELLO`, one `term`
shared across the whole grid.

## Connection lifecycle

```
  CLOSED
    | tcp connect (to the endpoint in the attach descriptor)
  CONNECTING
    | tcp established; the replica drops it if the source IP is not on the volume's allow-list
  INIT        connection-init (ICReq/ICResp): negotiate version + digests
    | negotiated
  AUTH        CHAP challenge-response (mutual), using the attach credentials
    | authenticated
  BINDING     LOGIN (first connection: establish the session) or HELLO (bind to an existing one)
    | OK
  BOUND       IO flows
    | logout / drain / error
  CLOSED  <-- (error or timeout schedules RECONNECTING, below)
```

`LOGIN` runs only on the first connection of a session (leader-only orchestration). Every other connection
reaches BOUND through `HELLO`. A transport error, a bind rejection, or a liveness timeout drops the
connection to CLOSED and schedules a reconnect.

## Admission and authentication

Access control lives entirely in the transport binding, not in CRAFT. The message layer (`wire.md`) knows
nothing about credentials; it fences by `term` -- "is this the current session owner?" -- which is a
different question from "is this connection allowed at all." Two independent gates, both provisioned by the
control plane, guard a connection before any CRAFT message flows:

1. **Source-IP allow-list.** When the control plane exposes a volume to a client, it tells the replica which
   client IP(s) may connect. The listener drops any connection whose source address is not on that volume's
   allow-list, at accept, before spending a handshake. Coarse and spoofable on its own, so it narrows
   exposure rather than authenticates.
2. **CHAP.** The control plane generates a per-attach credential (a CHAP identity + secret, ephemeral to the
   attachment) and provisions it to both the replica and the client. In the AUTH phase the replica
   challenges, the client answers with an HMAC over the challenge keyed by the secret (the secret is never
   sent), and the replica verifies; **mutual**, so the client also challenges the replica and a rogue
   endpoint at the advertised address cannot impersonate the volume. Use the NVMe-oF variant --
   **DH-HMAC-CHAP** (Diffie-Hellman augmented HMAC-SHA-256/384/512) -- over iSCSI's legacy MD5 CHAP, for
   offline-attack resistance and forward secrecy, keeping us aligned with the NVMe/TCP model this binding
   already follows.

Auth failure drops the connection (no CRAFT status; auth is pre-CRAFT). Only an authenticated connection
reaches BINDING; the CHAP exchange rides transport PDUs, not CRAFT messages, so `wire.md`'s op catalog is
unchanged.

### Session bring-up

The control plane drives attachment out of band and hands the client an **attach descriptor**:
`{ volume UUID, endpoint host:port, CHAP identity + secret }`. The client then:

1. connects to the endpoint and passes the source-IP gate;
2. runs INIT and AUTH with the descriptor's credentials;
3. sends `LOGIN` -- which may redirect (a follower returns `term == 0` + `leader_hint`, so the client
   reconnects to the leader and repeats 1-3); on success it holds the session `term` and the geometry;
4. opens the remaining `(queue, replica)` connections and brings each to BOUND with `HELLO` (each repeats
   1-2, then binds with the descriptor's volume UUID + the session `term`).

The descriptor's fields split cleanly: the endpoint drives the connect, the credentials drive AUTH, the
volume UUID is what `HELLO` carries. None of it enters a CRAFT message body.

## `request_id` lifetime -- the late-reply rule

A `request_id` is allocated when a request is sent and freed only when its reply arrives OR the connection
resets -- **never when the ublk IO completes**. It is deliberately decoupled from the ublk tag's lifetime:

A read that times out on replica A fails over, and its ublk IO completes via replica B, freeing the tag. If
`request_id` tracked the tag, ublk could reissue that id on connection A while A's abandoned reply is still
in flight, and the late reply would miscorrelate with the new request -- silent corruption. So the transport
keeps a per-connection outstanding table keyed by `request_id` and never reuses an id until its slot frees.
An id whose request timed out stays occupied (the reply may still come, per the timed-out-may-apply
invariant) until it lands or the connection resets, which frees the whole table at once.

The in-flight window per connection is the `request_id` space (a u16: one queue's depth times its split
factor, so it is generous headroom, not a size to fill). If timed-out-but-unresolved ids exhaust it, the
transport resets the connection -- freeing the table and forcing a reconnect -- rather than block.

## Deadlines

A per-request deadline is a **client knob** (the shim's `op_timeout_ms`), not a wire field. On expiry the
client stops waiting on that connection; it does not cancel the request on the wire (there is no cancel op,
and per `wire.md` a timed-out op may still apply). What happens to the IO is per-op:

- **Write child**: the quorum proceeds if a quorum has already acked; the timed-out replica is left as a
  Missing (resync later). Only a genuinely sub-quorum write leaves its slot unresolved and stalls the
  frontier.
- **Read**: fail over to the next eligible replica (already in the router). The abandoned request stays
  outstanding on its connection per the rule above.
- **keep_alive**: dropped; the next drive re-issues it (one outstanding per leg).

The protocol owes only one thing here, and already provides it: a timeout is never counted as a rejection.

## Liveness -- the Keep Alive Timeout

The keep_alive drive (see the client README) already keeps every replica contacted: a read keeps its skipped
legs alive, a write touches all of them, the idle probe covers the rest, one keep_alive outstanding per leg.
The transport's obligation is to keep that cadence inside the replica's Keep Alive Timeout, so a live client
is never torn down. TCP keepalive is the lower-level backstop for a silently dead connection. (The
replica-side watchdog that enforces the timeout is a deferred seam today; the client-side drive is built.)

## Failure detection and reconnect

| signal | meaning | action |
|---|---|---|
| TCP error (RST / EPIPE / ECONNRESET) | connection dead | reset the connection, reconnect |
| request deadline | replica slow or unreachable | per-op (write: quorum proceeds; read: failover); reset the connection if the id window exhausts |
| no reply past a liveness bound | connection suspect | probe; reconnect if unconfirmed |
| `STALE_TERM` on a bound connection | the session term moved past us (a takeover) | the session is gone; re-`LOGIN` (deferred) |

**Reconnect** re-runs CONNECTING -> INIT -> AUTH -> BINDING(`HELLO`) on a fresh socket, with a bounded backoff
(a reconnect delay and a ceiling, the analogue of NVMe/TCP's `ctrl_loss_tmo`, after which the connection is
abandoned). INIT and AUTH run again because the socket is new -- AUTH reuses the same attach credentials, so
there is no control-plane round-trip -- but the **session is not re-established**: `HELLO` re-binds the new
socket to the still-live session (`term` persists). Because ops are idempotent, in-flight requests on the
dead connection are retried on the reconnected one or routed to another replica; there is no per-command
replay log.

**If `HELLO` fails on reconnect** -- `STALE_TERM` (a takeover moved the term while we were gone) or a lost
session (the replica restarted and no longer knows the token) -- the client escalates from re-bind to re-
establish: re-`LOGIN` on the leader for a fresh `term`, then re-`HELLO` every connection to it. So a socket
reconnect is "just `HELLO`" only while the session outlived the disconnect; otherwise it climbs back to
`LOGIN`.

## Retry and idempotency

Retry is always safe, which is what lets reconnect stay simple:

- **Write**: retry with the SAME `dLSN`. A replica that already applied it merges the identical
  highest-`dLSN`-per-LBA write as a no-op; one that missed it now has it. No server-side dedup state.
- **Read / keep_alive**: side-effect-free; retry or fail over freely.

This is the simplification CRAFT earns over iSCSI: `dLSN` idempotency plus side-effect-free reads mean no
`CmdSN`, no command-replay window, no exactly-once machinery on the wire.

Ordering, likewise, is the client's `dLSN`, not wire order: the client assigns each write its slot and
replicas merge by highest-`dLSN`-per-LBA, so the transport needs no ordering guarantee across requests (the
other reason there is no `CmdSN`). Reads are independent.

## Interaction with quorum and the frontier

The transport fails per-connection; CRAFT tolerates it per-replica:

- Losing a connection to one replica is not an IO failure while a quorum of the others is reachable -- writes
  ack at quorum, reads route around.
- A reconnected replica that missed writes is behind; its Missing-map entries and a pinned reclaim floor (its
  `synced` watermark frozen) reflect that until it catches up. Recovery (`advance_synced`, fed by keep_alive)
  clears it once resync fills its holes.
- A sub-quorum write (quorum unreachable) leaves its slot unresolved and stalls the frontier, exactly as the
  protocol requires. The transport must not paper over that with a retry that counts a timeout as an ack.

## Deferred

- The replica-side Keep Alive Timeout watchdog, and `STALE_TERM` auto-re-`LOGIN`.
- Server-side resync (`SyncRSCommitLSN`), which makes the recovery clear-path above effective.
- Encryption on the wire: CHAP (above) authenticates but does not encrypt. TLS 1.3 under the framing, where
  NVMe/TCP puts it, is the confidentiality upgrade -- deferred for an intra-DC v1.
- The exact CHAP PDU encoding (challenge/response transport PDUs): follow the NVMe-oF authentication PDUs, to
  be pinned when the binding is implemented.
- Multi-partition: each partition is its own session and connection set.
