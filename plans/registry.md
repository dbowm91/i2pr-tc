# i2pr-tc Active Planning Registry

This is the compact control surface for active planning. Canonical direction is defined by `plans/000-003`; detailed requirements live in subsystem and implementation plans.

## Status vocabulary

- proposed
- ready
- active
- blocked
- closing
- closed
- conditionally closed
- superseded
- archived

## Active subsystem roadmaps

| Subsystem | Status | Roadmap | Current milestone | Dependencies/blockers |
|---|---|---|---|---|
| I2P torrent client foundation | active | `plans/subsystems/torrent-client-roadmap.md` | C003 conditionally closed; M004 blocked on upstream | M001/M003 closed. C001/C002 remain historical closure evidence. The post-C001 SAM/I2CP review found the STREAM-only SAM lifecycle was not a correct long-term transport; C003 replaced it with one long-lived shared-Destination primary session plus STREAM/DATAGRAM/RAW children and is conditionally closed (`plans/closure/torrent-client/008-c003-status.md`). The transport is built and qualified against i2pd 2.61.0. The remaining M004 gate is upstream: i2pr Plan 368 is registered `ready` and not implemented, and Plan 368 closure is what its private SAM seam depends on. |

## Implementation handoffs

| Workstream | Milestone | Status | Plan | Dependency note |
|---|---|---|---|---|
| Torrent client | M001 core protocol + storage foundation | **closed** | `plans/implementation/torrent-client/001-core-protocol-storage-foundation.md` | Closure: `plans/closure/torrent-client/001-status.md`; implementation `61ace427d7157e8f168d14a3bc8cc3501fd6492f`. |
| Torrent client | M002 I2P streaming + trackers + magnet metadata + PEX | **conditionally closed** | `plans/implementation/torrent-client/002-i2p-streaming-trackers-and-pex.md` | Historical closure: `plans/closure/torrent-client/002-status.md`; implementation `cbb65d03635652896fa41132c06bf60324c14147`. C001 below owns the post-closure SAM/runtime/fingerprint/interoperability findings; do not rewrite the historical closure. |
| Torrent client | M002 C001 SAM client boundary + runtime I/O + live-transport hardening | **closed (historical; transport readiness corrected forward by C003)** | `plans/implementation/torrent-client/006-m002-c001-sam-runtime-hardening.md` | Closure: `plans/closure/torrent-client/006-m002-c001-status.md`; implementation `852a8ef`. Its storage/fingerprint hardening remains retained. Subsequent protocol review found the SAM session lifetime/placeholder-local-hash model is not sufficient for real STREAM operation or future same-Destination DHT; C003 owns that correction without rewriting C001 history. |
| Torrent client | C003 SAM 3.3 PRIMARY/subsession shared-Destination transport corrective | **conditionally closed** | `plans/implementation/torrent-client/008-c003-sam33-primary-dht-transport-corrective.md` | Closure: `plans/closure/torrent-client/008-c003-status.md`. Replaced the STREAM-only per-operation session model with one long-lived primary identity plus STREAM/DATAGRAM/RAW children; placeholder local hashes are gone and `I2pSession::local_peer_hash` is now fallible. Acceptance criterion 8 is unmet because upstream i2pr Plan 368 is registered `ready` and not implemented; criteria 1-7 and 9 are met, and every unmet row is recorded as an unexercised or blocked row with its reason. Establishes the transport substrate for future M006 I2P DHT without implementing DHT yet. |
| Torrent client | M003 Transmission RPC compatibility | **closed** | `plans/implementation/torrent-client/003-transmission-rpc-compatibility.md` | Closure: `plans/closure/torrent-client/003-status.md`; implementation `8c111cf59eaa54366caed8f72fd84347fd6bddba` + `85d95002ff01a82333f93ffe7178b007dd8c2689`. |
| Torrent client | C002 planning/docs/MSRV/default-branch reconciliation | **closed** | `plans/implementation/torrent-client/007-c002-foundation-branch-reconciliation.md` | Closure: `plans/closure/torrent-client/007-c002-status.md`; integrated to `main` at `624f11d3d400cc71a0243b821260433ac9bb8859` by fast-forward. Rust floor made deliberate at 1.89/edition 2024, pinned and asserted in CI; README and docs reconciled to the implemented state. |
| Torrent client | M004 i2pr managed-app integration | **blocked** | `plans/implementation/torrent-client/004-i2pr-managed-app-integration.md` | The C003 dependency is discharged: there is now a corrected SAM 3.3 PRIMARY/subsession transport to compose. Still blocked on upstream i2pr Plan 368 (registered `ready`, not implemented) plus AppManager/package/process ownership, OS containment, host-owned local ingress, and private persistent-data/key semantics. M004 also owns where injected destination key material is persisted; C003 deliberately chose no host path. Plans 354/355 remain closed prerequisites. |
| Torrent client | M005 router update artifact transport | **blocked** | `plans/implementation/torrent-client/005-router-update-artifact-transport.md` | Requires M004 plus router-owned ReleaseTarget, private invocation, and capability-mediated artifact staging/export contracts. |

## Deferred work

Future M006 I2P DHT/datagram-tracker work has no handoff plan. C003 has now supplied the qualified same-Destination STREAM + DATAGRAM + RAW contract, so authoring an M006 plan against the shipped transport API is unblocked. M006's *live* qualification still has exactly one gate: a router that serves protocol 17/18 children, which today means upstream i2pr Plan 368 (i2pd 2.61.0 rejects `SESSION ADD` for datagram styles). C003 deliberately built the transport substrate without implementing KRPC/DHT.

Frontend work is deferred until backend/RPC/managed-runtime boundaries are stable.

## External interface watch

Primary upstream: `dbowm91/i2pr:main`.

Latest inspected upstream main on 2026-10-06: `579725eec85f8a38233a06f86c23cf6efab3f21b` (Plan 368 registered in registry/roadmap/support inventory; SAM 3.3 support remains explicitly unclaimed).

The authoritative i2pr registry records managed-app Plans 345, 349, 352–355 as closed. Plans 354/355 continue to provide listener-independent private raw SAM/I2CP connections and the app-principal/capability gateway. The router's SAM product is still a SAM 3.1 STREAM baseline; Plan 368 is now registered to add the SAM 3.3 PRIMARY/subsession shared-Destination profile over the existing Streaming plus protocol-17/protocol-18 datagram substrates.

Remaining M004 upstream owners are explicit: Plan 368 SAM 3.3 PRIMARY/subsessions must close first; AppManager/package/process lifecycle and process authentication are not registered/implemented; OS sandbox/resource containment remains future work; host-owned local RPC ingress and private persistent-data/key semantics are not yet available to this consumer.

The upstream tree also has no torrent-facing router `ReleaseTarget`, private update invocation, or artifact staging/export contract, so M005 remains blocked independently.

Recheck i2pr main and the managed-app reference contract before promoting M004/M005 conclusions.
