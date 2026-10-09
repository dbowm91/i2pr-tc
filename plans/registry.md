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
| I2P torrent client foundation | active | `plans/subsystems/torrent-client-roadmap.md` | M006-A ready; M004 split into concrete blocked slices | C003 is conditionally closed with Java I2P+i2pd qualification. Upstream package/process/policy has advanced through Plans 382–383. M006-A KRPC core is executable now. M004-A waits on upstream 388 SDK; M004-B on SAM/368 + 388; M004-C on 385/386/388; M004-D on 387/388. |

## Implementation handoffs

| Workstream | Milestone | Status | Plan | Dependency note |
|---|---|---|---|---|
| Torrent client | M001 core protocol + storage foundation | **closed** | `plans/implementation/torrent-client/001-core-protocol-storage-foundation.md` | Closure: `plans/closure/torrent-client/001-status.md`; implementation `61ace427d7157e8f168d14a3bc8cc3501fd6492f`. |
| Torrent client | M002 I2P streaming + trackers + magnet metadata + PEX | **conditionally closed** | `plans/implementation/torrent-client/002-i2p-streaming-trackers-and-pex.md` | Historical closure: `plans/closure/torrent-client/002-status.md`; implementation `cbb65d03635652896fa41132c06bf60324c14147`. C001 below owns the post-closure SAM/runtime/fingerprint/interoperability findings; do not rewrite the historical closure. |
| Torrent client | M002 C001 SAM client boundary + runtime I/O + live-transport hardening | **closed (historical; transport readiness corrected forward by C003)** | `plans/implementation/torrent-client/006-m002-c001-sam-runtime-hardening.md` | Closure: `plans/closure/torrent-client/006-m002-c001-status.md`; implementation `852a8ef`. Its storage/fingerprint hardening remains retained. Subsequent protocol review found the SAM session lifetime/placeholder-local-hash model is not sufficient for real STREAM operation or future same-Destination DHT; C003 owns that correction without rewriting C001 history. |
| Torrent client | C003 SAM 3.3 PRIMARY/subsession shared-Destination transport corrective | **conditionally closed** | `plans/implementation/torrent-client/008-c003-sam33-primary-dht-transport-corrective.md` | Closure: `plans/closure/torrent-client/008-c003-status.md`. Replaced the STREAM-only per-operation session model with one long-lived primary identity plus STREAM/DATAGRAM/RAW children; placeholder local hashes are gone and `I2pSession::local_peer_hash` is now fallible. Acceptance criterion 8 is unmet because upstream i2pr Plan 368 is registered `ready` and not implemented; criteria 1-7 and 9 are met. Criterion 7 was discharged in a corrective pass that installed Java I2P 2.13.0 and qualified the matrix live against it — the normative `PRIMARY` spelling, `NAME=ME` identity, and all three child styles — and fixed three defects it found. Every row that could not be executed (live peer-to-peer, live datagram traffic) is recorded as a skip with its reason, never as a pass. Establishes the transport substrate for future M006 I2P DHT without implementing DHT yet. |
| Torrent client | M003 Transmission RPC compatibility | **closed** | `plans/implementation/torrent-client/003-transmission-rpc-compatibility.md` | Closure: `plans/closure/torrent-client/003-status.md`; implementation `8c111cf59eaa54366caed8f72fd84347fd6bddba` + `85d95002ff01a82333f93ffe7178b007dd8c2689`. |
| Torrent client | C002 planning/docs/MSRV/default-branch reconciliation | **closed** | `plans/implementation/torrent-client/007-c002-foundation-branch-reconciliation.md` | Closure: `plans/closure/torrent-client/007-c002-status.md`; integrated to `main` at `624f11d3d400cc71a0243b821260433ac9bb8859` by fast-forward. Rust floor made deliberate at 1.89/edition 2024, pinned and asserted in CI; README and docs reconciled to the implemented state. |
| Torrent client | M004 i2pr managed-app integration umbrella | **blocked / decomposed** | `plans/implementation/torrent-client/004-i2pr-managed-app-integration.md` | Execution is split into M004-A through M004-D below. The old claims that AppManager/package/process ownership are absent are superseded: upstream 369–371 and 382–383 are closed. Final M004 closure still requires package/lifecycle, private SAM 3.3, private data+Secured, and host-owned RPC ingress evidence. |
| Torrent client | M004-A managed package + SDK + lifecycle bootstrap | **blocked** | `plans/implementation/torrent-client/009-m004a-managed-package-lifecycle-bootstrap.md` | Blocked only on upstream Plan 388 external app SDK/package builder. Upstream appd/apphost/package/policy foundations are already closed. |
| Torrent client | M004-B managed SAM 3.3 capability composition | **blocked** | `plans/implementation/torrent-client/010-m004b-managed-sam33-capability-composition.md` | Blocked on M004-A, upstream SAM/368, and Plan 388. Discharges C003 criterion 8 through i2pr's private managed SAM path. |
| Torrent client | M004-C private data + persistent Destination + Secured profile | **blocked** | `plans/implementation/torrent-client/011-m004c-private-data-and-secured-profile.md` | Blocked on M004-A/B plus upstream 385 private data, 386 Linux Secured sandbox, and 388 SDK. |
| Torrent client | M004-D host-owned Transmission local ingress | **blocked** | `plans/implementation/torrent-client/012-m004d-transmission-local-ingress.md` | Blocked on M004-A plus upstream 387 host-owned local-service ingress and 388 SDK. |
| Torrent client | M006-A I2P DHT KRPC core | **ready** | `plans/implementation/torrent-client/013-m006-i2p-dht-krpc-core.md` | Deterministic/runtime-neutral I2P BEP-5 core: 32-byte peers, 54-byte nodes, secure node IDs, routing/tokens/tracker/persistence. No live router dependency. |
| Torrent client | M006-B I2P DHT transport + torrent integration | **blocked** | `plans/implementation/torrent-client/014-m006-i2p-dht-transport-integration.md` | Blocked on M006-A. Java I2P can support the SAM DATAGRAM/RAW transport; i2pr-managed qualification additionally waits on SAM/368 + M004-B and live transit. |
| Torrent client | M005 router update artifact transport | **blocked** | `plans/implementation/torrent-client/005-router-update-artifact-transport.md` | Requires M004 plus router-owned ReleaseTarget, private invocation, and capability-mediated artifact staging/export contracts. |

## Deferred work

M006 is now planned as M006-A/M006-B. M006-A is ready and owns deterministic KRPC/routing/token/tracker/persistence work with no live-router dependency. M006-B is blocked on M006-A and owns C003 DATAGRAM/RAW binding, peer-source integration, and live qualification. Java I2P 2.13.0 already accepts protocol-17/18 children; i2pd 2.61.0 does not.

Frontend work is deferred until backend/RPC/managed-runtime boundaries are stable.

## External interface watch

Primary upstream: `dbowm91/i2pr:main`.

Latest inspected upstream main on 2026-10-09: `be2eba4a23987c4fb44c812204638f38a08a77df`.

The authoritative i2pr runtime now has Managed native app runtime/368–371 plus Plans 382–383 closed: trusted manager bridge, real appd/apphost lifecycle, signed packages, persistent trust/grants/selection/autostart, and restart-safe production catalog exist. New Plans 385/387/388 are ready for private app data, host-owned local-service ingress, and external Rust SDK/package building; Plan 386 is blocked on 385 and owns Linux Secured containment. Separately, the colliding SAM/368 remains ready and unimplemented.

Remaining M004 upstream owners are now concrete: SAM/368 for private SAM 3.3; Plan 388 for an external app SDK/package builder; Plan 385 for private app data; Plan 386 for Linux Secured containment; Plan 387 for host-owned local RPC ingress. Package/process/persistent-policy ownership is no longer a blocker.

The upstream tree also has no torrent-facing router `ReleaseTarget`, private update invocation, or artifact staging/export contract, so M005 remains blocked independently.

Recheck i2pr main and the managed-app reference contract before promoting M004/M005 conclusions.
