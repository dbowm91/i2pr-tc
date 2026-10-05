# Architecture Overview

Status: target architecture for the foundational roadmap

## Dependency direction

```text
i2pr-tc core
  - bencode framing + exact info-span capture
  - metainfo/magnet types
  - peer wire + extension codecs
  - torrent state and piece scheduling
          |
          v
i2pr-tc storage
  - path-safe file layout
  - random-access piece writes/reads
  - resume/recheck persistence
          |
          v
i2pr-tc I2P
  - I2P address types
  - injected SAM transport/session
  - HTTP I2P trackers
  - peer connect/accept
  - ut_metadata + i2p_pex
          |
          v
TorrentService
          |
          +---- TransmissionAdapter
          +---- managed-app adapter
          +---- bounded update artifact service
```

Exact crate boundaries may be adjusted during M001 if a smaller split is cleaner, but dependency direction must remain equivalent.

## Ownership

Core owns deterministic torrent protocol/state, not network sockets.

Storage owns filesystem I/O beneath an authorized root and never accepts peer paths directly.

I2P transport owns router protocol adaptation and converts tracker/PEX identity into canonical I2P peer values.

TorrentService is the sole mutation/query facade for RPC, managed runtime, and future UI adapters.

Transmission compatibility owns no persisted torrent truth. Managed-app adapter owns no torrent algorithms. Update artifact service owns no release trust/installation.

## Concurrency model

Prefer per-torrent actors/tasks plus bounded channels or equivalent ownership making one component authoritative for each torrent's mutable state. Peer tasks report observations/results; they should not mutate shared state through an unbounded graph of locks.

Cancellation propagates service -> torrent -> tracker/peer operations. Restart restores persisted intent/resume state and revalidates files as needed; it does not reconstruct live peer connections.

## Resource model

Every untrusted count/length is capped before allocation: metainfo bytes, path components/files, piece count/length, peer-wire messages, extension metadata, tracker response/peer count, PEX additions/drops, peer count/in-flight connects, RPC size, and resume-state size.

Operational ceilings may be configurable downward but have validated hard maxima.

## Testing seams

Core codecs/state: deterministic unit/property/fuzz tests.

Storage: temp-directory corruption/recheck/restart fixtures.

I2P: scripted SAM/tracker/peer fixtures first; external Java I2P/i2pd interoperability as closure evidence when feasible.

Transmission: golden protocol corpus plus transmission-remote harness.

Managed runtime: capability-channel integration and negative evidence proving no direct-network requirement.

Updates: fake authenticated ReleaseTarget plus malicious/mismatched artifact cases proving transport cannot install.
