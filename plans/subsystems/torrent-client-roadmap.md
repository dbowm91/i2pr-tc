# Torrent Client Subsystem Roadmap

Status: active; M001/M003 closed, historical M002 conditionally closed, M002 C001 closed, C002 closed and integrated to `main`; M004 eligible, M005 blocked

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

i2pr-tc owns torrent parsing/state/storage, application-side SAM client behavior, I2P torrent discovery/peer protocol, native torrent service semantics, Transmission translation, and bounded artifact transport.

It does not own i2pr sandboxing, general network policy, router identity, Proposal 170 administrator authority, update trust/installation, or frontend state.

## Core invariants

1. Managed production networking is I2P-only.
2. Canonical peer state is not IP SocketAddr state.
3. Exact raw `info` bytes determine v1 infohash.
4. Peer/tracker/metainfo/RPC/resume/SAM input is bounded before allocation.
5. Torrent mutable state has one clear owner per torrent.
6. Blocking filesystem work does not execute under global torrent-state ownership or directly on async peer workers without bounded offload.
7. Resume state is advisory and cannot override hash/file truth.
8. Cancellation and restart are explicit at every I/O boundary.
9. Transmission names are not persisted domain authority.
10. Managed app integration never requires direct host networking.
11. Raw managed-app SAM streams remain generic router protocol streams; torrent-specific lookup/connect/accept semantics stay application-side.
12. Torrent update transport cannot install or authenticate releases.

## Dependency graph

```text
M001 core protocol/storage (closed)
   |\
   | +------> M003 Transmission RPC (closed)
   |
   +--------> M002 I2P streaming/trackers/PEX (historically conditionally closed)
                 |
                 +------> C001 SAM/runtime/live-transport corrective (ready)
                              |\
                              | +----> C002 foundation branch reconciliation
                              |
                              +------> M004 managed-app integration
                                          |
                                          +------> M005 router update transport

future M006 datagram trackers/DHT remains deferred
```

Historical milestone closure is not rewritten when a corrective is found. C001
adds forward evidence and determines whether M002's remaining operational
qualification is satisfied.

## M001 — Core protocol and storage foundation

Status: closed.

Plan:
`plans/implementation/torrent-client/001-core-protocol-storage-foundation.md`

Closure:
`plans/closure/torrent-client/001-status.md`

M001 provides strict BitTorrent v1 parsing/infohashing, peer-wire/extension
codecs, verified storage/recovery, durable TorrentService state, cancellation,
fuzzing, and dependency-boundary enforcement.

## M002 — I2P streaming trackers and PEX

Status: historically conditionally closed.

Plan:
`plans/implementation/torrent-client/002-i2p-streaming-trackers-and-pex.md`

Historical closure:
`plans/closure/torrent-client/002-status.md`

M002 added the injected `I2pSession`, I2P identities, tracker client,
inbound/outbound peer handling, magnet metadata, PEX, retry/backoff, and
scripted transfer evidence. The closure explicitly left live-router, full
magnet, inbound transfer, and reconnect qualification outstanding.

Post-closure review additionally found that the current `I2pSession` stops
above the raw SAM protocol boundary now frozen by i2pr Plans 354/355, tracker
HTTP exposes a versioned `i2pr-tc/0.1` User-Agent, and runtime piece
persistence can hold global state ownership across synchronous storage work.

Those findings are owned by C001; do not rewrite the historical M002 record.

## M002 C001 — SAM client boundary, runtime I/O, and live-transport hardening

Status: closed.

Plan:
`plans/implementation/torrent-client/006-m002-c001-sam-runtime-hardening.md`

Closure:
`plans/closure/torrent-client/006-m002-c001-status.md`

C001 must:

- implement the application-side SAM-v3 client over a raw SAM connection
  factory compatible with i2pr's managed-app service-stream contract;
- preserve the current high-level `I2pSession` as a useful transport-facing
  API/test seam rather than making i2pr implement torrent-specific operations;
- remove version-specific tracker HTTP fingerprinting;
- move blocking filesystem/hash/write work outside global/per-torrent critical
  sections and off async worker paths with bounded backpressure;
- prove race-safe persistence completion;
- add full magnet/inbound deterministic cases and live Java I2P/available
  alternate-router interoperability evidence where feasible.

Exit: the I2P transport is composition-ready for M004 without a direct host SAM
fallback or a torrent-specific router gateway.

Reached. The application-side SAM-v3 client, the fingerprint removals, the
per-torrent state ownership with generation-checked persistence reservations,
and the bounded storage offload are implemented and verified. Magnet and
inbound transfer qualification passes deterministically. Live-router
qualification is environment-dependent and was not executed; M004 owns running
it against a real bridge as its first act, and that is the medium-severity
residual finding in the C001 closure.

## M003 — Transmission RPC compatibility

Status: closed.

Plan:
`plans/implementation/torrent-client/003-transmission-rpc-compatibility.md`

Closure:
`plans/closure/torrent-client/003-status.md`

The current/legacy adapter remains transport-independent and owns no production
listener. M004 will supply host-owned ingress.

## C002 — Planning, documentation, MSRV, and branch integration reconciliation

Status: closed.

Closure:
`plans/closure/torrent-client/007-c002-status.md`

Integrated into `main` at `624f11d3d400cc71a0243b821260433ac9bb8859` by
fast-forward, with no divergence to reconcile.

Plan:
`plans/implementation/torrent-client/007-c002-foundation-branch-reconciliation.md`

C002 updates README/planning to the implemented state, records an explicit Rust
MSRV/edition policy, rechecks current upstream managed-app ownership, verifies
the exact integration head, and integrates the foundational work line into
`main` without rewriting historical closure evidence.

## M004 — i2pr managed-app integration

Status: blocked.

Plan:
`plans/implementation/torrent-client/004-i2pr-managed-app-integration.md`

Current blocker interpretation after the 2026-10-06 upstream recheck and the
C001/C002 closures:

- i2pr Plans 354 and 355 are closed and provide the private raw SAM/I2CP
  connection seams plus router app-principal capability gateway;
- C001 is closed: the application-side raw-SAM client, the fingerprint
  removals, and the transport/storage hardening are implemented. Live-router
  interoperability was not executed and is M004's first work package, not a
  reason to hold this milestone;
- AppManager/package/process lifecycle and process authentication remain
  unregistered/unimplemented upstream;
- OS sandbox/resource containment remains future work;
- host-owned local Transmission ingress remains unavailable;
- private persistent app-data semantics remain unavailable.

M003's RPC adapter is already closed; its production publication still waits on
host-owned ingress.

M004 is therefore the eligible next milestone, and its first work package is
live qualification of the SAM client against a real bridge, followed by
composing `SamConnectionFactory` from the managed runtime.

## M005 — Router update artifact transport

Status: blocked.

Plan:
`plans/implementation/torrent-client/005-router-update-artifact-transport.md`

Hard/interface blockers:

- M004 closure;
- router-owned normalized ReleaseTarget/update authority;
- private host-to-app invocation;
- capability-mediated artifact staging/export.

The torrent app remains transport only and may not absorb these router trust
owners.

## Future M006 — datagram trackers and DHT

Status: deferred; no handoff plan.

Wait for the streaming/SAM corrective to close, a stable production SAM
datagram/PRIMARY/subsession contract, and a fresh I2P UDP announce/DHT spec
review. Do not introduce conventional IP DHT/UDP as an interim substitute.

## Verification strategy

Every implemented milestone/corrective adds unit/property tests, fuzz targets
for hostile codecs, restart/crash fixtures, cancellation/backpressure tests,
static dependency/ownership guards, and an interoperability corpus where an
external protocol is claimed.

C001/M004/M005 require negative evidence that clearnet/direct-host networking
is not required. C001 additionally requires concurrency evidence proving disk
work does not globally serialize unrelated torrent state.

## Completion definition

The foundational workstream closes when C001/C002 and M004/M005 are closed,
the implementation is integrated to the default branch, and the backend can
operate as a secured i2pr managed app, expose its declared
Transmission-compatible surface, and serve as a bounded router-update artifact
transport without acquiring update authority.

Frontend work and future M006 are not required for foundational closure.
