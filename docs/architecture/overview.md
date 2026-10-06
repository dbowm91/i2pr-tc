# Architecture Overview

Status: M001 implementation snapshot; M002/M003 remain gated on M001 closure.

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

`i2pr-tc-i2p` speaks SAM v3 itself over a caller-supplied raw byte stream, through `sam::SamConnectionFactory`. No router, gateway, or daemon type is named anywhere in the repository, because the managed-runtime contract the app depends on is only "one stream carrying the exact ordered protocol octets this app requested". `SamClient` owns the application Destination, its single bounded session id, per-operation deadlines, and cancellation; a session failure marks the transport unavailable rather than retrying, so an unavailable service cannot produce a reconnect storm. `STREAM FORWARD` is not implemented.

Tracker announces are I2P-only and bounded. They carry no `User-Agent`, and the peer extension handshake advertises no client version. Magnet metadata acquisition over `ut_metadata` verifies and promotes the `info` dictionary; `i2p_pex` peer discovery is hash-only and deduplicated.

## Not yet implemented

M004 requires a composed `SamConnectionFactory` backed by the managed i2pr application runtime, plus qualified live-router interoperability, host-owned local ingress, and private persistent-data semantics. None of that is implemented here, and the transport has not yet been qualified against a live router.

M005 requires router-owned ReleaseTarget, private invocation, and staging/export contracts, which do not exist upstream.

## Resource model

Current bounds cover encoded metainfo, bencode nesting/items/strings, file and announce counts/lengths, path component lengths/collisions, piece length, resume file size at load, peer-wire frame size at decode, PEX record counts, magnet URI/name/tracker bounds, metadata extension framing, event retention/batch size, catalog records, and scheduler in-flight requests. Storage operations serialize and support cooperative cancellation between files/pieces. Network and RPC bounds belong to their later milestones.

## Verification seams

Core has deterministic unit fixtures for bencode, raw-info hashing, magnet parsing, incremental peer-wire framing, PEX records, scheduling, extension block sizes, and service events. Storage has temporary-directory fixtures for cross-file pieces, verified writes, recheck, durable catalog restart, removal, cancellation, path rejection, symlink parents, rooted/stale resume state, progress rebuilt only after stored-piece verification, and runtime block assembly/recovery, including choke and disconnect ownership release. The I2P crate has exact-octet SAM transcript fixtures, full magnet acquisition-to-completion and inbound transfer cases, and a declared test-only harness that qualifies the client against a live SAM bridge; that harness reports a skip with a reason when no bridge is present, so a skip is never mistaken for a pass. It was run against an i2pd 2.61.0 bridge, which exposed four wire defects the transcript fixtures could not, because every fixture had been written to match the client's own assumptions: `SESSION CREATE` omitting its mandatory `DESTINATION=` parameter, base64 in the standard alphabet rather than I2P's, `NAMING LOOKUP` reading `DESTINATION=` where a service answers `VALUE=`, and `STREAM ACCEPT` reading a `DESTINATION=` line where a service writes a bare one. `SamSessionDestination` makes the session identity explicit, and a transient identity is per-connection, so a client that must be dialled injects key material instead. `scripts/check-foundation-boundaries.py --self-test` guards and mutation-tests the crate dependency/source boundary, rejects reintroduced version fingerprints in library source and integration tests, and rejects an undeclared host socket in a test target; it runs in CI. Six cargo-fuzz targets have retained seed corpora, including one for the SAM parser and its base64 decoder; longer fuzz qualification remains closure work.
