# Architecture Overview

Status: M001 implementation snapshot; later layers remain planned.

## Current crate graph

```text
i2pr-tc-core
  bencode / metainfo / magnets / peer-wire / piece scheduling / TorrentService vocabulary
          ^
          |
i2pr-tc-storage
  authorized-root file operations / piece verification / resume persistence
```

`i2pr-tc-core` has no network, HTTP, SAM, router, Transmission, or process-execution dependency. `i2pr-tc-storage` depends on core types and performs filesystem I/O only beneath its opened root. There is no binary or production service runtime yet.

## Ownership boundaries

Core contains deterministic BitTorrent v1 parsing, domain identifiers, a bounded peer-wire frame decoder, I2P-hash PEX records, and scheduling primitives. The raw encoded `info` dictionary span is SHA-1 hashed directly; it is not reserialized.

Storage maps validated torrent-relative components below one authorized root. Resume state is versioned and advisory; piece hashes remain authoritative. The initial implementation uses ordinary filesystem operations and checks symlink components; it does not yet provide race-free descriptor-relative operations on every supported platform.

The native `TorrentService` trait is independent of Transmission naming. `MemoryTorrentService` is a deterministic catalog adapter, not a durable production torrent engine. It intentionally rejects operations it cannot truthfully provide.

## Planned layers

M002 will add injected I2P/SAM streaming, HTTP trackers, magnet metadata exchange, and `i2p_pex` integration. M003 will add Transmission compatibility over TorrentService. M004 requires qualified i2pr managed-app SAM, ingress, and persistent-data contracts. M005 requires router-owned ReleaseTarget and staging/export contracts. None of those layers is implemented here.

## Resource model

Current bounds cover encoded metainfo, bencode nesting/items/strings, file and piece counts, path component lengths, piece length, resume file size at load, peer-wire frame size at decode, PEX record counts, magnet URI/name/tracker bounds, and scheduler in-flight requests. Network and RPC bounds belong to their later milestones.

## Verification seams

Core has deterministic unit fixtures for bencode, raw-info hashing, magnet parsing, peer-wire framing, PEX records, and scheduling. Storage has temporary-directory fixtures for cross-file pieces, recheck, removal, path rejection, and stale resume state. The cargo-fuzz package defines parser and resume targets; corpus seeds and sustained fuzz qualification remain closure work.
