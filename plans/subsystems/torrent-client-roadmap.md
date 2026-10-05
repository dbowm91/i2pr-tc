# Torrent Client Subsystem Roadmap

Status: active planning foundation; M001 ready, M002-M005 dependency-blocked

Canonical authority:

- `plans/000-long-term-specification.md`
- `plans/001-terminology-and-domain-model.md`
- `plans/002-long-term-roadmap.md`
- `plans/003-planning-process.md`
- ADR 0001, ADR 0002, ADR 0003

Research baseline:
`plans/research/001-foundation-and-ecosystem-assessment.md`

Initial repository baseline:
`f9f88bf1ad906f3ce3c0e444d76407eed1a31b66`

## Purpose and ownership

Build the smallest reliable I2P-native torrent backend that can be independently tested, then integrate it with i2pr without moving router authority into the app.

i2pr-tc owns torrent parsing/state/storage, I2P torrent discovery/peer protocol, native torrent service semantics, Transmission translation, and bounded artifact transport.

It does not own i2pr sandboxing, general network policy, router identity, Proposal 170 administrator authority, update trust/installation, or frontend state.

## Core invariants

1. Managed production networking is I2P-only.
2. Canonical peer state is not IP SocketAddr state.
3. Exact raw `info` bytes determine v1 infohash.
4. Peer/tracker/metainfo/RPC/resume input is bounded before allocation.
5. Torrent mutable state has one clear owner per torrent.
6. Resume state is advisory and cannot override hash/file truth.
7. Cancellation and restart are explicit at every I/O boundary.
8. Transmission names are not persisted domain authority.
9. Managed app integration never requires direct host networking.
10. Torrent update transport cannot install or authenticate releases.

## Dependency graph

```text
M001 core protocol/storage
   |\
   | +------> M003 Transmission RPC
   |
   +--------> M002 I2P streaming/trackers/PEX
                 |
                 +------> M004 managed-app integration
                 |           |
                 |           +------> M005 router update transport
                 |
                 +------> future M006 datagram trackers/DHT
```

M003 can begin after M001 freezes TorrentService and may proceed in parallel with M002.

## M001 — Core protocol and storage foundation

Status: ready.

Plan:
`plans/implementation/torrent-client/001-core-protocol-storage-foundation.md`

Create the Rust workspace and deterministic BitTorrent v1 core with no network dependency. Freeze native types/services needed by later transport and RPC work.

Exit: fixtures prove metainfo/infohash/path correctness, peer-wire/extension codecs are bounded, storage can write/recheck/resume, and state survives restart without trusting stale resume data.

## M002 — I2P streaming trackers and PEX

Status: blocked on M001 closure.

Plan:
`plans/implementation/torrent-client/002-i2p-streaming-trackers-and-pex.md`

Add injected SAM transport, long-lived I2P session semantics, HTTP I2P trackers, inbound/outbound peers, magnet metadata, multi-tracker fallback, and i2p_pex. No datagram/DHT dependency.

Exit: qualified swarms transfer data using only I2P address semantics and recover across tracker/peer/router disconnects.

## M003 — Transmission RPC compatibility

Status: blocked on M001 TorrentService contract.

Plan:
`plans/implementation/torrent-client/003-transmission-rpc-compatibility.md`

Build current + bounded legacy Transmission request/response translation over the native service. It is initially in-process/transport-independent.

Exit: golden corpus and transmission-remote interoperability cover the declared method subset with truthful unsupported behavior.

## M004 — i2pr managed-app integration

Status: blocked.

Plan:
`plans/implementation/torrent-client/004-i2pr-managed-app-integration.md`

Hard/interface blockers:

- M002 closure;
- current i2pr managed-app corrective/successors;
- production app-scoped SAM gateway;
- host-owned local-service ingress;
- private persistent app-data semantics.

M003 is a soft dependency for exposing Transmission ingress but not for proving SAM launch.

## M005 — Router update artifact transport

Status: blocked.

Plan:
`plans/implementation/torrent-client/005-router-update-artifact-transport.md`

Hard/interface blockers:

- M002 and M004 closure;
- router-owned normalized ReleaseTarget/update authority;
- private host-to-app invocation;
- capability-mediated artifact staging/export.

## Future M006 — datagram trackers and DHT

Status: deferred; no handoff plan.

Wait for a stable production SAM datagram/PRIMARY/subsession contract, M002 interoperability evidence, and current I2P UDP announce/DHT spec re-review. Do not introduce conventional IP DHT/UDP as an interim substitute.

## Verification strategy

Every implemented milestone adds unit/property tests, fuzz targets for hostile codecs, restart/crash fixtures, cancellation/backpressure tests, static dependency/ownership guards, and an interoperability corpus where an external protocol is claimed.

M002/M004/M005 require negative evidence that clearnet/direct-host networking is not required.

## Completion definition

The foundational workstream closes when M001-M005 are closed and the backend can operate as a secured i2pr managed app, expose its declared Transmission-compatible surface, and serve as a bounded router-update artifact transport without acquiring update authority.

Frontend work and future M006 are not required for foundational closure.
