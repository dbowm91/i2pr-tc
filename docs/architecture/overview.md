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

Storage maps validated torrent-relative components below one authorized root. Resume state is versioned, stored beneath that root, byte-bounded at read and write time, and advisory; its bitmap uses a compact representation with legacy-array decoding, and piece hashes remain authoritative. Disk operations serialize through one storage mutex. Filesystem access rejects symlinked components and includes root-escape fixtures; operations currently use checked standard paths and do not claim race-free protection from a concurrent local filesystem mutator.

The native `TorrentService` trait is independent of Transmission naming and exposes a bounded cursor-based event ring. `MemoryTorrentService` is a deterministic test adapter. `PersistentTorrentService` durably records metainfo/magnet intent, limits, file priorities, and desired run state with atomic per-record replacement; it resets verified-piece claims on restart and restores desired-running records as `Starting`. Its piece-ingest operation validates and stores complete pieces before updating service progress; recovery rechecks the payload and reconstructs progress from stored bytes. `TorrentRuntime` composes that service with `PieceMap`, peer-wire sessions, bounded outstanding peer blocks and bounded assembly buffers; opening the runtime performs recovery before exposing schedulable state. It handles availability replacement, choke/disconnect/cancellation cleanup, verified downloads, and uploads sourced only from verified storage. Network dialing and stream ownership remain later work. Payload deletion is supported for metainfo-backed torrents under a per-torrent root; magnets without resolved metainfo return typed unsupported.

## Planned layers

M002 will add injected I2P/SAM streaming, HTTP trackers, magnet metadata exchange, and `i2p_pex` integration. M003 will add Transmission compatibility over TorrentService. M004 requires qualified i2pr managed-app SAM, ingress, and persistent-data contracts. M005 requires router-owned ReleaseTarget and staging/export contracts. None of those layers is implemented here.

## Resource model

Current bounds cover encoded metainfo, bencode nesting/items/strings, file and announce counts/lengths, path component lengths/collisions, piece length, resume file size at load, peer-wire frame size at decode, PEX record counts, magnet URI/name/tracker bounds, metadata extension framing, event retention/batch size, catalog records, and scheduler in-flight requests. Storage operations serialize and support cooperative cancellation between files/pieces. Network and RPC bounds belong to their later milestones.

## Verification seams

Core has deterministic unit fixtures for bencode, raw-info hashing, magnet parsing, incremental peer-wire framing, PEX records, scheduling, extension block sizes, and service events. Storage has temporary-directory fixtures for cross-file pieces, verified writes, recheck, durable catalog restart, removal, cancellation, path rejection, symlink parents, rooted/stale resume state, progress rebuilt only after stored-piece verification, and runtime block assembly/recovery, including choke and disconnect ownership release. `scripts/check-foundation-boundaries.py --self-test` guards and mutation-tests the crate dependency/source boundary and runs in CI. Five cargo-fuzz targets have retained seed corpora and smoke evidence; longer fuzz qualification remains closure work.
