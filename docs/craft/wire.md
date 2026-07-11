# CRAFT wire protocol

Status: design. The reference client (`src/test/craft/`) still runs over the in-memory `MemTransport` shim;
this document specifies the on-wire encoding a real network layer is built against. It is the concrete
encoding of the client-facing verbs in `api.md` and satisfies the transport requirements enumerated in
`src/test/craft/client/README.md` ("What the transport must provide").

## Transport decision

The first real binding is **TCP over io_uring**: kernel TCP for reliability and congestion control, io_uring
for submission (socket ops are SQEs on the ublk queue's own ring, so a reply is a CQE the queue already
reaps -- requirement 7). **Homa** is the back-pocket transport if the quorum-ack tail under incast is ever
measured as the bottleneck and an out-of-tree kernel module is acceptable; **RDMA** (RoCEv2 / iWARP) is the
option for a site willing to spend on RDMA NICs and a lossless fabric. The message encoding below is
transport-agnostic so those bindings do not force a re-encode.

## Two layers

- **The CRAFT message** (this document, most of it) is transport-agnostic: a fixed message header, a
  per-operation header, and an opaque bulk payload. Every transport carries these bytes unchanged.
- **The transport framing** (the "TCP binding" section) owns what is transport-specific: stream delimiting,
  the connection-init handshake, and integrity digests. Digests live here, not in the message: over TCP they
  are CRC32C (TCP's own checksum is too weak for storage); over RDMA the NIC's ICRC already covers the wire,
  so no application digest is added.

## Conventions

- All multi-byte integers are **little-endian** and **naturally aligned** within their header. Headers are
  packed (no implicit padding); every reserved byte MUST be written 0 and ignored on read.
- A field marked `i64` is signed (an `_lsn` or dLSN, where -1 means "none/unknown"); `u64`/`u32`/`u16`/`u8`
  are unsigned. `uuid[16]` is a 16-byte RFC-4122 identifier in network byte order (as `boost::uuids::uuid`
  serializes it).
- Offsets in the per-operation tables are relative to the start of that operation header (which itself begins
  immediately after the 8-byte message header).
- **Little-endian is deliberate**: every field is fixed-offset, so a receiver overlays the header struct on
  the buffer and reads in place -- no parse, no copy. The bulk payload is never interpreted by the wire
  layer.

## The message header

Every request and every response begins with this 8-byte header.

```
      7      6      5      4      3      2      1      0
    +------+------+------+------+------+------+------+------+
  0 |                       op  (u8)                      |   see the op table; request and response
    +------+------+------+------+------+------+------+------+   are distinct op codes
  1 |                     status  (u8)                    |   response ops only; 0 = OK (see below)
    +------+------+------+------+------+------+------+------+
2-3 |                  request_id  (u16)                  |   per-connection correlator; response echoes it
    +------+------+------+------+------+------+------+------+
4-7 |                    body_len  (u32)                  |   bytes of payload after the operation header
    +------+------+------+------+------+------+------+------+
```

There is **no `op_hdr_len` and no per-message `version`**. The operation-header size is fixed per
`(op, version)`, and both are known -- `op` is in the header and the wire `version` is a connection-lifetime
constant negotiated once at connection-init -- so `op` alone implies the operation-header layout. Only the
variable **payload** length (`body_len`) has to be carried. The 8-byte header keeps the operation header
8-aligned, so it can be overlaid in place (its first fields are `_lsn` / token u64s). A receiver frames a
message as `8 + opHdrSize(op) + body_len (+ digests)`; a message whose `op` it does not recognize (a version
bug, or corruption the digest already flags) is unframeable and resets the connection, which is the correct
response anyway.

`op` carries the operation **and** the direction: request and response are distinct codes (there is no
response flag bit), so a server only ever receives request ops and a client only ever receives `_RSP` ops --
the value alone is a direction check. The op set is far under 64, so a single byte is ample.

| op | value | | op | value |
|---|---|---|---|---|
| `LOGIN` | 1 | | `LOGIN_RSP` | 2 |
| `HELLO` | 3 | | `HELLO_RSP` | 4 |
| `WRITE` | 5 | | `WRITE_RSP` | 6 |
| `READ` | 7 | | `READ_RSP` | 8 |
| `KEEPALIVE` | 9 | | `KEEPALIVE_RSP` | 10 |
| `LOGOUT` | 11 | | `LOGOUT_RSP` | 12 |

`request_id` correlates a response to its request. It is a per-connection correlator allocated per request --
not the ublk tag (a split read issues several requests against one tag) -- and is not reused on a connection
until its reply arrives or the connection resets, so a reply the client abandoned on a timeout can never be
miscorrelated with a later request (NVMe/TCP's "CID not reused until the command completes"; see
`transport.md`). A u16 is ample: the in-flight window on one connection is one queue's depth times its split
factor, and connections do not share an id space, so neither `nr_hw_queues` nor the replica count multiplies
it. A request op sets `status` to 0; a response op (the `_RSP` codes) carries the result in `status`.

## Session and connection model

The session is **connection-bound**. A client holds one connection per `(queue, replica)`; each is bound once,
at open, and every IO on it reuses that binding. There is no per-IO or per-op handshake -- that would be a
latency killer -- so the only constants carried per IO are the ones that actually change (the commit
watermarks).

- **`LOGIN`** establishes a new session on the leader (the full `GetRSCommitLSN` -> `SyncRSCommitLSN` ->
  `InternalLogin` orchestration) and binds the connection it arrives on. It returns the session `term`, the
  starting `dLSN`, and the volume geometry.
- **`HELLO`** binds an *additional* connection (another queue, or a follower) to the already-established
  session. The replica knows the session `term` from the replicated `InternalLogin`, so this is an attach,
  not a re-login.
- **Fencing** is by the connection's bound term, not a per-IO wire field: the replica compares the socket's
  bound `term` to its current session `term` on each IO. A takeover bumps the session term; the old
  connection's bound term then trails it and its IOs fail `STALE_TERM`. This is why the per-IO header does
  not carry `term`.

## The common request header

`WRITE`, `READ`, `KEEPALIVE`, and `LOGOUT` requests begin their operation header with this 16-byte block --
CRAFT's commit, piggybacked on the IO the client is already sending (there is no standalone commit verb).
`LOGIN` and `HELLO` omit it (pre-session).

| offset | size | field | notes |
|---|---|---|---|
| 0 | 8 | `commit_lsn` (i64) | advance the contiguous frontier toward this, in dLSN order; -1 = do not advance |
| 8 | 8 | `all_committed_lsn` (i64) | client-computed set-wide min commit_lsn; floors journal reclaim; -1 = unknown |

## Operations

### LOGIN (request op = 1, response op = 2)

Establish a session; leader-only orchestration. A follower does not fail -- it returns `term == 0` with
`leader_hint` set, and the client retries at that replica.

Request operation header (8 bytes):

| offset | size | field | notes |
|---|---|---|---|
| 0 | 8 | `client_token` (u64) | ownership token from the caller |

No request body.

Response operation header (56 bytes):

| offset | size | field | notes |
|---|---|---|---|
| 0 | 8 | `term` (u64) | 0 = NOT_LEADER redirect; > 0 = session term |
| 8 | 8 | `dLSN` (i64) | starting per-partition LSN for new IO |
| 16 | 8 | `capacity` (u64) | volume size in bytes -- the device size presented to ublk |
| 24 | 4 | `lba_size` (u32) | block size in bytes; alignment unit for addr/len |
| 28 | 4 | `max_tx` (u32) | largest single transfer in bytes; caps ublk max_sectors and any `body_len` |
| 32 | 4 | `member_count` (u32) | number of member descriptors in the body |
| 36 | 4 | reserved (0) | |
| 40 | 16 | `leader_hint` (uuid) | non-nil iff redirect (term == 0); nil on success |

Response body: `member_count` member descriptors, packed:

| offset | size | field | notes |
|---|---|---|---|
| 0 | 16 | `id` (uuid) | replica id |
| 16 | 2 | `addr_len` (u16) | length of the address string |
| 18 | `addr_len` | `addr` (bytes) | "host:port", not NUL-terminated |

This member list is the only variable-length, non-overlayable structure in the protocol; it is parsed once,
at login, off the hot path.

### HELLO (request op = 3, response op = 4)

Bind this connection to an existing session. Sent on every connection after the first `LOGIN`. Any replica
answers (not leader-only). `OK` binds; otherwise `WRONG_TOKEN` / `STALE_TERM` / `NOT_LEADER`.

Request operation header (32 bytes):

| offset | size | field | notes |
|---|---|---|---|
| 0 | 16 | `volume_id` (uuid) | which volume this connection serves |
| 16 | 8 | `client_token` (u64) | session owner |
| 24 | 8 | `term` (u64) | the session term the client believes is current |

No request body. Response carries `status` only (no operation header, no body).

### WRITE (request op = 5, response op = 6)

Append the client-assigned write at `dlsn`. An **empty body (`body_len == 0`) is a zero write** (WRITE_ZEROES;
`len` is the range to unmap; reads back as a hole); a non-empty body is a data write of exactly `len` bytes.

Request operation header (40 bytes):

| offset | size | field | notes |
|---|---|---|---|
| 0 | 16 | common request header | `commit_lsn`, `all_committed_lsn` |
| 16 | 8 | `dlsn` (i64) | client-assigned slot |
| 24 | 8 | `addr` (u64) | byte offset, block-aligned |
| 32 | 8 | `len` (u64) | byte length, block-aligned; the write size and (for a zero write) the unmap range |

Request body: `len` bytes of block data, or none for a zero write.

Response operation header (16 bytes) -- the achieved LSNs, piggybacked so the client refreshes the reclaim
floor and recovery on every write without a separate keep_alive:

| offset | size | field | notes |
|---|---|---|---|
| 0 | 8 | `commit_lsn` (i64) | replica's achieved commit_lsn |
| 8 | 8 | `last_append_lsn` (i64) | replica's highest appended dLSN |

No response body. ACK fires on journal-append completion.

### READ (request op = 7, response op = 8)

Return the latest version <= the read horizon `H` for `[addr, addr+len)`, filling the caller's buffer in
place and describing the sparse layout. Entries above `H` are never served even if the replica holds them.

Request operation header (40 bytes):

| offset | size | field | notes |
|---|---|---|---|
| 0 | 16 | common request header | `commit_lsn`, `all_committed_lsn` |
| 16 | 8 | `read_lsn` (i64) | the read horizon `H` |
| 24 | 8 | `addr` (u64) | byte offset, block-aligned |
| 32 | 8 | `len` (u64) | byte length, block-aligned |

No request body.

Response operation header (24 bytes):

| offset | size | field | notes |
|---|---|---|---|
| 0 | 8 | `commit_lsn` (i64) | replica's achieved commit_lsn (piggybacked, as on `WRITE_RSP`) |
| 8 | 8 | `last_append_lsn` (i64) | replica's highest appended dLSN |
| 16 | 4 | `extent_count` (u32) | number of extent descriptors in the body |
| 20 | 4 | reserved (0) | |

Every IO response the client gets back -- `WRITE_RSP`, `READ_RSP`, `KEEPALIVE_RSP` -- leads with the replica's
achieved `{commit_lsn, last_append_lsn}`, so any round-trip to a replica refreshes its watermark in the
client's per-member model (its `synced`, the reclaim floor), not just writes and keep_alives. This is the
response-side mirror of the client watermarks (`commit_lsn`, `all_committed_lsn`) every IO request carries.

Response body: `extent_count` extent descriptors (ascending by `addr`), **then** the concatenated data bytes
for the non-hole extents in the same order. The descriptors come first on purpose: the receiver reads the
layout, computes the destination iovecs (data extents map to their `dest` sub-ranges, holes are zero-filled),
and receives the data directly into `dest` -- a zero-copy scatter that reconstructs the caller's buffer.

Extent descriptor (24 bytes):

| offset | size | field | notes |
|---|---|---|---|
| 0 | 8 | `addr` (u64) | byte offset into the volume |
| 8 | 8 | `len` (u64) | byte length |
| 16 | 1 | `hole` (u8) | 1 = hole (reads as zero, no bytes in the body); 0 = data |
| 17 | 7 | reserved (0) | |

A hole is a resolved "reads as zero" and is NOT the same as Missing.

### KEEPALIVE (request op = 9, response op = 10)

The dedicated commit carrier: advance the frontier toward `commit_lsn`, reset the session watchdog, and
return the member's achieved LSNs. It is also how the client drives liveness without a timer (see the client
README).

Request operation header (16 bytes): the common request header. No body.

Response operation header (16 bytes):

| offset | size | field | notes |
|---|---|---|---|
| 0 | 8 | `commit_lsn` (i64) | |
| 8 | 8 | `last_append_lsn` (i64) | |

### LOGOUT (request op = 11, response op = 12)

Explicit, term-fenced session teardown. Leader-only. The leader commits an `InternalLogout` that clears the
session on every replica; later IO from this client is fenced `STALE_TERM`.

Request operation header (16 bytes): the common request header. No body. Response carries `status` only.

## Status codes

Carried in the message header `status` byte on responses. Values 1-6 are exactly `craft_error`
(`craft_types.hpp`), so client-side mapping is a cast.

| value | name | meaning |
|---|---|---|
| 0 | OK | success |
| 1 | STALE_TERM | IO term != session term (the protocol's ETERM) |
| 2 | NOT_LEADER | leader-only op sent to a follower (LOGIN returns a redirect instead) |
| 3 | NO_QUORUM | could not reach a quorum of live replicas |
| 4 | WRONG_TOKEN | client_token is not the current owner |
| 5 | NOT_ELIGIBLE | replica cannot serve this read (Missing overlap / below login dLSN) |
| 6 | REPLICA_DOWN | addressed replica is down / unreachable |
| 7 | INVALID_ARGUMENT | misaligned addr/len, or `body_len` over `max_tx` |
| 255 | INTERNAL | unexpected server-side failure |

## The TCP binding

### Connection init

Before any CRAFT message, the two ends exchange a transport init (the analogue of NVMe/TCP `ICReq`/`ICResp`):
the client offers its maximum wire `version` and its digest capabilities; the replica replies with the
`version` to use and which digests are enabled. Because the client is always **>= the replica** in version,
the chosen version is the replica's, and the client speaks its backward-compatible codec for that version.
Digest negotiation is per-connection and fixed for its lifetime.

### Framing

A CRAFT message frames from its 8-byte header: `op` implies the operation-header size for the connected
version and `body_len` gives the payload, so the total is `8 + opHdrSize(op) + body_len (+ digests)` -- no
separate length prefix is required. The binding MAY prepend a 4-byte sync word for stream resynchronization;
it is a framing aid, not part of the message. On a message transport (Homa) a message is one datagram and
framing is free.

### Digests

CRC32C (Castagnoli -- the SSE4.2 `crc32` instruction), matching iSCSI and NVMe/TCP practice:

- **Header digest (HDGST)**, default on: a `u32` over the message header plus the operation header, placed
  immediately **after** the operation header and **before** the body -- so a corrupt header is rejected
  before the receiver commits to reading (or allocating for) the payload.
- **Data digest (DDGST)**, negotiated: a `u32` over the body, placed after it. Enable it for wire-to-disk
  end-to-end integrity; it may be left off where the storage layer already carries a client-computed at-rest
  block checksum that covers the same path.

On the wire a framed message is therefore:
`[message header][op header][HDGST?][body][DDGST?]`, with the two digests present per the connection's
negotiation.

### Zero-copy

- **WRITE request**: build `[message header][op header]` in a small scratch, and gather-send it with the data
  iovec pointing straight at the ublk buffer (`SEND_ZC`); the payload is consumed at submit, so a straggler
  send outliving the quorum ack is safe (requirement 6).
- **READ response**: read the header and the extent descriptors, compute the `dest` iovecs, then receive the
  data directly into `dest`. The payload is never copied for the sake of the format.

## Sizing and limits

One ublk IO is one wire message, and ublk already bounds a single transfer at `max_sectors` (derived from
`max_tx`). So `body_len` is always `<= max_tx`; a receiver MUST reject a larger `body_len` with
`INVALID_ARGUMENT` to bound its buffer. There is no application-level fragmentation or reassembly.

## Not covered yet

- **Peer / internal ops** (`SyncRSCommitLSN`, `fetch_data`, the RAFT-entry API): server-to-server, not part
  of the client wire. They get their own op range when the resync path is built.
- **Authentication / encryption**: out of scope for an intra-DC v1. If needed it is a transport-binding
  concern (TLS under the TCP framing), not a change to the CRAFT message.
- **Multi-partition**: each partition is its own client with its own session and connections; this document
  describes a single partition's wire.
