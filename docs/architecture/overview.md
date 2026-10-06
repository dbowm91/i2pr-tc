# Architecture Overview

Status: M001 implementation snapshot; later layers remain planned.

## Current crate graph

```text
i2pr-tc-core
  bencode / metainfo / magnets / peer-wire / piece scheduling / TorrentService vocabulary
          ^
          |
i2pr-tc-storage
  rooted file operations / verified writes / resume persistence / durable torrent catalog
```

`i2pr-tc-core` has no network, HTTP, SAM, router, Transmission, or process-execution dependency. `i2pr-tc-storage` depends on core types and performs filesystem I/O beneath its opened root. There is no binary or production peer-transfer runtime yet.

## Ownership boundaries

Core contains deterministic BitTorrent v1 parsing, domain identifiers, a bounded peer-wire frame decoder, I2P-hash PEX records, and scheduling primitives. The raw encoded `info` dictionary span is SHA-1 hashed directly; it is not reserialized.

Storage maps validated torrent-relative components below one authorized root. Resume state is versioned, stored beneath that root, byte-bounded at read time, and advisory; piece hashes remain authoritative. Disk operations serialize through one storage mutex. Filesystem access checks symlink components but does not yet provide race-free descriptor-relative operations on every supported platform.

The native `TorrentService` trait is independent of Transmission naming and exposes a bounded cursor-based event ring. `MemoryTorrentService` is a deterministic test adapter. `PersistentTorrentService` durably records metainfo/magnet intent, limits, file priorities, and desired run state with atomic per-record replacement; it resets verified-piece claims on restart and restores desired-running records as `Starting`. Its piece-ingest operation validates and stores complete pieces before updating service progress; recovery rechecks the payload and reconstructs progress from stored bytes. It remains a catalog/state owner, not a single composed PieceMap/storage runtime or peer-transfer engine. Payload deletion is supported for metainfo-backed torrents under a per-torrent root; magnets without resolved metainfo return typed unsupported.

## Planned layers

M002 will add injected I2P/SAM streaming, HTTP trackers, magnet metadata exchange, and `i2p_pex` integration. M003 will add Transmission compatibility over TorrentService. M004 requires qualified i2pr managed-app SAM, ingress, and persistent-data contracts. M005 requires router-owned ReleaseTarget and staging/export contracts. None of those layers is implemented here.

## Resource model

Current bounds cover encoded metainfo, bencode nesting/items/strings, file and announce counts/lengths, path component lengths/collisions, piece length, resume file size at load, peer-wire frame size at decode, PEX record counts, magnet URI/name/tracker bounds, metadata extension framing, event retention/batch size, catalog records, and scheduler in-flight requests. Storage operations serialize and support cooperative cancellation between files/pieces. Network and RPC bounds belong to their later milestones.

## Verification seams

Core has deterministic unit fixtures for bencode, raw-info hashing, magnet parsing, incremental peer-wire framing, PEX records, scheduling, extension block sizes, and service events. Storage has temporary-directory fixtures for cross-file pieces, verified writes, recheck, durable catalog restart, removal, cancellation, path rejection, symlink parents, rooted/stale resume state, and progress rebuilt only after stored-piece verification. `scripts/check-foundation-boundaries.py --self-test` guards and mutation-tests the crate dependency/source boundary. Five cargo-fuzz targets have retained seed corpora and 100-run smoke evidence; sustained fuzz qualification remains closure work.
