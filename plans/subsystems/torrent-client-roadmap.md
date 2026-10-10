# Torrent Client Subsystem Roadmap

Status: active; M001/M003 closed, historical M002 conditionally closed, C001/C002 closed; C003 conditionally closed; M006-A closed; M006-B ready; M004 decomposed into blocked A–D slices; M005 blocked

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
11. Raw managed-app SAM streams remain generic router protocol streams; torrent-specific operations stay application-side.
12. One torrent transport generation owns one canonical I2P Destination; peer Streaming and future DHT datagrams must share it.
13. SAM 3.1 STREAM is a compatibility baseline, not the final torrent transport architecture; the target is SAM 3.3 PRIMARY/subsessions.
14. Torrent update transport cannot install or authenticate releases.

## Dependency graph

```text
M001 core/storage (closed)
  +-> M003 Transmission RPC (closed)
  +-> M002 -> C001 -> C002 -> C003 (conditionally closed)
                              |
                              +-> M006-A KRPC core (closed)
                              |      -> M006-B DHT transport/integration (ready)
                              |
                              +-> M004 umbrella
                                   +-> M004-A (009) blocked on upstream 388
                              +-> M004-B (010) blocked on A + 388 (upstream SAM/368 closeout awaits integration to main)
                                   +-> M004-C (011) blocked on A/B + 385 + 386 + 388
                                   +-> M004-D (012) blocked on A + 387 + 388
                                          |
                                          +-> M004 final closure
                                                -> M005 update transport
```

The C003 application implementation edge is discharged. M004 is now split so each remaining upstream interface has a concrete owner. Separately, C003 criterion 8 is completed by M004-B after SAM/368.

Historical milestone closure is not rewritten when a corrective is found. C001
and C002 remain valid evidence for the work they actually closed. C003 corrects
forward the later-discovered SAM session/identity architecture defect and the
long-term transport decision needed for I2P DHT.

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

Historical exit claim: C001 considered the I2P transport composition-ready for M004 without a direct host SAM fallback or torrent-specific router gateway. C003 corrects that conclusion forward: the storage/fingerprint work remains valid, but production transport readiness now requires the SAM 3.3 shared-Destination corrective.

Reached. The application-side SAM-v3 client, the fingerprint removals, the
per-torrent state ownership with generation-checked persistence reservations,
and the bounded storage offload are implemented and verified. Magnet and
inbound transfer qualification passes deterministically. Live-router qualification against i2pd 2.61.0 exposed four wire defects that were fixed, but later SAM lifecycle/DHT research found a deeper architectural defect not covered by that closure: one torrent identity must remain one long-lived Destination across STREAM and future DATAGRAM/RAW use. C003 owns that correction. The previously unexecuted name/second-peer/tracker rows move behind C003 rather than serving as M004's first work package.

## C003 — SAM 3.3 PRIMARY/subsession shared-Destination transport corrective

Status: conditionally closed.

Plan:
`plans/implementation/torrent-client/008-c003-sam33-primary-dht-transport-corrective.md`

Closure: `plans/closure/torrent-client/008-c003-status.md`

Paired upstream:
`dbowm91/i2pr` Plan 368.

The post-C001 SAM/I2CP review established that the torrent transport must own
one long-lived I2P Destination and share it across peer/tracker Streaming and
future DHT datagrams. The SAM client as it stood created session state in the
wrong place, exposed a placeholder session-ID-derived local hash, and could not
honestly serve as the substrate for I2P DHT.

C003 replaced that shape with SAM 3.3 PRIMARY/subsessions:

- one long-lived primary/control session and real local Destination/hash;
- STREAM child transport for peers and HTTP trackers;
- protocol-17/repliable DATAGRAM and protocol-18 RAW children reserved and
  qualified for future M006 DHT;
- explicit Java I2P/i2pd compatibility, including PRIMARY/MASTER differences;
- no torrent-specific I2CP+Streaming implementation.

Delivered and qualified over the real bridge against two routers: i2pd 2.61.0
and, in a corrective pass, Java I2P 2.13.0. Java accepts the normative
`STYLE=PRIMARY` on the first connection and attaches STREAM, DATAGRAM and RAW
children, which is what closed criterion 7 and exposed the datagram `PORT=`
requirement Java enforces (see the closure record §5a). The conditional part is
upstream and unchanged: acceptance criterion 8 requires i2pr Plan 368 to close so
the same matrix can run over the private managed-app SAM seam. Live
peer-to-peer and live datagram rows remain unexecuted with reasons recorded — no
router available here had working I2P transit, so no peer was ever reachable.
They are skips, never passes. C003 does not implement KRPC/DHT.

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

Status: blocked umbrella; decomposed.

Umbrella plan:
`plans/implementation/torrent-client/004-i2pr-managed-app-integration.md`

Upstream has advanced materially: managed runtime 369–371 and package/policy
Plans 382–383 are closed. The remaining integration is split so completed
foundations do not wait behind unrelated sandbox/ingress work.

### M004-A — managed package + lifecycle bootstrap

Plan: `plans/implementation/torrent-client/009-m004a-managed-package-lifecycle-bootstrap.md`

Status: blocked on upstream Plan 388.

Adds the real app process/package and consumes appd/apphost + persistent policy
through the external SDK/package builder. It makes no Secured or final-SAM
claim.

### M004-B — managed SAM 3.3 capability composition

Plan: `plans/implementation/torrent-client/010-m004b-managed-sam33-capability-composition.md`

Status: blocked on M004-A and Plan 388; upstream SAM/368 closeout is prepared but awaits integration to main.

Maps C003's raw connection factory onto app logical SAM streams and discharges
C003 criterion 8 through i2pr's private app gateway.

### M004-C — private data + persistent Destination + Secured profile

Plan: `plans/implementation/torrent-client/011-m004c-private-data-and-secured-profile.md`

Status: blocked on M004-A/B and upstream Plans 385/386/388.

Places all mutable torrent state and Destination keys below the authorized app
root and qualifies the real Linux Secured profile with direct host networking
denied.

### M004-D — Transmission local ingress

Plan: `plans/implementation/torrent-client/012-m004d-transmission-local-ingress.md`

Status: blocked on M004-A and upstream Plans 387/388.

Publishes M003 through host-owned loopback ingress; the app never binds a
listener.

M004 final closure requires A+B+C and D if Transmission publication is claimed
as part of the product profile.

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

## M006 — I2P DHT

### M006-A — KRPC core

Status: closed. Closure evidence: `plans/closure/torrent-client/013-m006a-status.md`.

Plan: `plans/implementation/torrent-client/013-m006-i2p-dht-krpc-core.md`

Implements the runtime-neutral I2P BEP-5 profile: 32-byte Destination-hash
peers, 54-byte nodes, secure node IDs, strict KRPC codec, bounded routing,
tokens, local tracker state, and restart bootstrap persistence. It has no live
router dependency and may proceed immediately.

### M006-B — transport + torrent integration

Status: ready. Hard dependencies M006-A and retained C003 transport are met.
Working transit and a reachable DHT peer remain operational dependencies for
live interoperability evidence, not deterministic implementation.

Plan: `plans/implementation/torrent-client/014-m006-i2p-dht-transport-integration.md`

Binds the core to C003 protocol-17/18 children under the same Destination,
resolves compact hashes through verified b32 lookup, merges DHT provenance into
tracker/PEX peer sources, and owns live qualification. Java I2P 2.13.0 is a
compatible SAM transport; i2pd 2.61.0 is recorded as incompatible with
DATAGRAM/RAW subsessions. i2pr-managed qualification follows SAM/368 + M004-B.

Datagram tracker support remains a later phase; do not conflate it with the
first KRPC implementation.

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
