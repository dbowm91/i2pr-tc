# Architecture Overview

Status: implementation snapshot through M003/C002; the C003 SAM 3.3 transport corrective is conditionally closed and is the implementation authority for the I2P transport.

## Current crate graph

```text
i2pr-tc-core
  bencode / metainfo / magnets / peer-wire / piece scheduling / TorrentService vocabulary
          ^
          |
i2pr-tc-storage
  rooted file operations / verified writes / resume persistence / durable torrent catalog
  TorrentRuntime: peer sessions -> bounded block ownership/assembly -> verified storage
```

`i2pr-tc-core` has no network, HTTP, SAM, router, Transmission, or process-execution dependency. `i2pr-tc-storage` depends on core types and performs filesystem I/O beneath its opened root. `TorrentRuntime` is a non-network transfer-state owner: callers provide validated peer-wire messages and an I2P-sized peer identity. It does not dial, listen, or own transport streams.

## Ownership boundaries

Core contains deterministic BitTorrent v1 parsing, domain identifiers, bounded peer-wire frame encoding/decoding and per-peer message state, I2P-hash PEX records, and scheduling primitives. The raw encoded `info` dictionary span is SHA-1 hashed directly; it is not reserialized. Peer-wire state validates infohash handshakes, bitfields, request bounds, and requested piece responses before passing events to the storage runtime.

Storage maps validated torrent-relative components below one authorized root. Resume state is versioned, stored beneath that root, byte-bounded at read and write time, and advisory; its bitmap uses a compact representation with legacy-array decoding, and piece hashes remain authoritative. Disk operations for one payload root serialize through a shared per-root storage handle, so unrelated torrents never serialize behind each other's disk work. Filesystem access rejects symlinked components and includes root-escape fixtures; operations currently use checked standard paths and do not claim race-free protection from a concurrent local filesystem mutator.

The native `TorrentService` trait is independent of Transmission naming and exposes a bounded cursor-based event ring. `MemoryTorrentService` is a deterministic test adapter. `PersistentTorrentService` durably records metainfo/magnet intent, limits, file priorities, and desired run state with atomic per-record replacement; it resets verified-piece claims on restart and restores desired-running records as `Starting`. Its piece-ingest operation validates and stores complete pieces before updating service progress; recovery rechecks the payload and reconstructs progress from stored bytes. `TorrentRuntime` composes that service with `PieceMap`, peer-wire sessions, bounded outstanding peer blocks and bounded assembly buffers; opening the runtime performs recovery before exposing schedulable state. It handles availability replacement, choke/disconnect/cancellation cleanup, verified downloads, and uploads sourced only from verified storage.

State ownership is per torrent. The runtime catalog lock only locates an owned per-torrent state owner, and piece assembly moves a completed piece out of that owner as a reservation carrying a generation. Filesystem work happens with no torrent lock and no catalog lock held; a stop, remove, or recheck mints a new generation, so a late completion reports stale rather than resurrecting state or counting the same bytes twice. Blocking persistence and recheck run through an injected executor with a bounded queue, so they neither block an async peer task nor queue without limit. Network dialing and stream ownership remain the caller's seam. Payload deletion is supported for metainfo-backed torrents under a per-torrent root; magnets without resolved metainfo return typed unsupported.

## I2P transport

`i2pr-tc-i2p` speaks SAM over caller-supplied raw protocol byte streams through
`sam::SamConnectionFactory`; no router, gateway, or daemon type is named by the
crate.

C003 replaced the STREAM-only SAM session model with SAM 3.3
PRIMARY/subsessions:

```text
one primary/control session
  -> one canonical I2P Destination
       +-> STREAM child for peers and HTTP trackers
       +-> DATAGRAM child for future protocol-17 I2P DHT traffic
       +-> RAW child for future protocol-18 I2P DHT traffic
```

The primary control connection remains alive for the session lifetime and is
owned by a supervisor task that holds no unrelated state. STREAM, DATAGRAM, and
RAW attachment connections never create sessions: they issue `HELLO` and their
own command, and attach with `SESSION ADD` on the primary's own control
connection.

The router-confirmed local Destination and its SHA-256 Destination hash are
canonical. The session-ID-derived placeholder hash has been deleted, not
deprecated: `I2pSession::local_peer_hash()` returns a fallible
`Result<[u8; 32], TransportError>` and reports `IdentityNotReady` until the
router has confirmed a Destination, so a consumer that filters itself out of
PEX or refuses a self-connection cannot do so with an invented identity.

The shared-Destination style has two spellings. The specification and Java I2P
use `STYLE=PRIMARY`; i2pd requires the older `STYLE=MASTER` and rejects the
current spelling. The client offers the normative spelling first on its own
fresh connection and records which spelling the service accepted. It never
probes optimistically with two create commands on one connection.

Tracker announces remain I2P-only and bounded. They carry no `User-Agent`,
and the peer extension handshake advertises no client version. Magnet metadata
acquisition over `ut_metadata` verifies and promotes the `info` dictionary;
`i2p_pex` discovery is hash-only and will use the real local Destination hash
for self-filtering after C003. `STREAM FORWARD` is not part of the managed
profile.

M006-A closed with the runtime-neutral DHT state core in
`i2pr-tc-core::dht`: strict bounded KRPC, I2P compact node/peer forms, secure
node IDs, routing and transaction state, rotating announce tokens, local peer
tracking, and a versioned bootstrap snapshot persisted atomically by
`i2pr-tc-storage`. It performs no network I/O. M006-B owns binding those state
transitions to the SAM DATAGRAM/RAW children and qualifying live traffic. The
bounded transport responder and deterministic two-node path are implemented;
live qualification remains operationally blocked.
`DhtResponder::run` refuses to start unless the core's local hash and query
port match the router-confirmed SAM identity; the caller supplies
unpredictable transaction IDs and the token secret. The responder owns one
signed receive worker, one raw receive/dispatch worker, and one bounded
maintenance worker. Its API merges get_peers results into shared
tracker/PEX/DHT provenance and leaves refresh scheduling to the torrent owner.
Verified compact-node Destinations are cached only as positive results, capped
at 256 entries, expired after ten minutes, and invalidated on SAM generation
change. The current repository has no networked per-torrent actor; callers own
bounded traversal/announce timing and withdraw DHT provenance on stop, while
remote announce records expire under the deployed profile.

## Not yet implemented

C003 corrected the SAM lifecycle and identity model and qualified one shared
Destination across STREAM, DATAGRAM, and RAW against an in-memory SAM 3.3
service, with the STREAM child additionally qualified against live routers.
Its production qualification over the managed-app seam still needs M004-B.
The i2pr workspace has a SAM/368 implementation and formal closeout, but those
changes are not yet integrated to upstream main.

M004 remains blocked on the external app SDK/package builder, private app data,
Linux Secured containment, and host-owned local ingress. Package/process
ownership foundations exist upstream.

M005 requires router-owned ReleaseTarget, private invocation, and
staging/export contracts, which do not exist upstream.

## Resource model

Current bounds cover encoded metainfo, bencode nesting/items/strings, file and announce counts/lengths, path component lengths/collisions, piece length, resume file size at load, peer-wire frame size at decode, PEX record counts, magnet URI/name/tracker bounds, metadata extension framing, event retention/batch size, catalog records, scheduler in-flight requests, and M006-A's closed DHT core KRPC/routing/transaction/token/tracker/persistence state. M006-B additionally caps network datagrams at 32 KiB, pending raw replies at 256, a traversal at 32 nodes, per-request timeouts at 120 seconds, token-cache entries at 256, and shared peer-source/backoff entries at 2,000. Live network behavior belongs to M006-B.

## Verification seams

Core has deterministic unit fixtures for bencode, raw-info hashing, magnet parsing, incremental peer-wire framing, PEX records, scheduling, extension block sizes, and service events. Storage has temporary-directory fixtures for cross-file pieces, verified writes, recheck, durable catalog restart, removal, cancellation, path rejection, symlink parents, rooted/stale resume state, progress rebuilt only after stored-piece verification, and runtime block assembly/recovery, including choke and disconnect ownership release. The I2P crate has exact-octet SAM transcript fixtures, full magnet acquisition-to-completion and inbound transfer cases, and a declared test-only harness that qualifies the client against a live SAM bridge; that harness reports a skip with a reason when no bridge is present, so a skip is never mistaken for a pass. It was run against an i2pd 2.61.0 bridge, which exposed four wire defects the transcript fixtures could not, because every fixture had been written to match the client's own assumptions: `SESSION CREATE` omitting its mandatory `DESTINATION=` parameter, base64 in the standard alphabet rather than I2P's, `NAMING LOOKUP` reading `DESTINATION=` where a service answers `VALUE=`, and `STREAM ACCEPT` reading a `DESTINATION=` line where a service writes a bare one. Those fixes are retained, but the run did **not** qualify the final transport architecture: subsequent review established that the torrent identity is per long-lived SAM session/primary, not per operation/connection, and that future DHT requires STREAM/DATAGRAM/RAW to share that same Destination. C003 owns that correction. `scripts/check-foundation-boundaries.py --self-test` guards and mutation-tests the crate dependency/source boundary, rejects reintroduced version fingerprints in library source and integration tests, and rejects an undeclared host socket in a test target; it runs in CI. Six cargo-fuzz targets have retained seed corpora, including one for the SAM parser and its base64 decoder; longer fuzz qualification remains closure work.
