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
| I2P torrent client foundation | active | `plans/subsystems/torrent-client-roadmap.md` | M004 eligible; C002 closed and integrated to `main` | M001/M003 closed. Historical M002 is conditionally closed, its C001 corrective is closed, and C002 reconciled the branch into `main`. M004 is the eligible next milestone and starts with live-router qualification; M005 retains upstream release/invocation blockers. |

## Implementation handoffs

| Workstream | Milestone | Status | Plan | Dependency note |
|---|---|---|---|---|
| Torrent client | M001 core protocol + storage foundation | **closed** | `plans/implementation/torrent-client/001-core-protocol-storage-foundation.md` | Closure: `plans/closure/torrent-client/001-status.md`; implementation `61ace427d7157e8f168d14a3bc8cc3501fd6492f`. |
| Torrent client | M002 I2P streaming + trackers + magnet metadata + PEX | **conditionally closed** | `plans/implementation/torrent-client/002-i2p-streaming-trackers-and-pex.md` | Historical closure: `plans/closure/torrent-client/002-status.md`; implementation `cbb65d03635652896fa41132c06bf60324c14147`. C001 below owns the post-closure SAM/runtime/fingerprint/interoperability findings; do not rewrite the historical closure. |
| Torrent client | M002 C001 SAM client boundary + runtime I/O + live-transport hardening | **closed** | `plans/implementation/torrent-client/006-m002-c001-sam-runtime-hardening.md` | Closure: `plans/closure/torrent-client/006-m002-c001-status.md`; implementation `852a8ef`. Application-side SAM-v3 client, tracker and extension-handshake fingerprint removal, per-torrent state ownership with generation-checked persistence reservations, and bounded storage offload. Magnet and inbound qualification pass; live-router qualification is environment-dependent and remains an M004 prerequisite. |
| Torrent client | M003 Transmission RPC compatibility | **closed** | `plans/implementation/torrent-client/003-transmission-rpc-compatibility.md` | Closure: `plans/closure/torrent-client/003-status.md`; implementation `8c111cf59eaa54366caed8f72fd84347fd6bddba` + `85d95002ff01a82333f93ffe7178b007dd8c2689`. |
| Torrent client | C002 planning/docs/MSRV/default-branch reconciliation | **closed** | `plans/implementation/torrent-client/007-c002-foundation-branch-reconciliation.md` | Closure: `plans/closure/torrent-client/007-c002-status.md`; integrated to `main` at `624f11d3d400cc71a0243b821260433ac9bb8859` by fast-forward. Rust floor made deliberate at 1.89/edition 2024, pinned and asserted in CI; README and docs reconciled to the implemented state. |
| Torrent client | M004 i2pr managed-app integration | **blocked** | `plans/implementation/torrent-client/004-i2pr-managed-app-integration.md` | C001 is closed, so the application side of the contract is ready; M004 now owns the `SamConnectionFactory` composition and must first run the live interoperability matrix in the C001 closure. Still requires upstream AppManager/package/process ownership, OS containment, host-owned local ingress, and private persistent-data semantics. i2pr Plans 354/355 are closed and no longer blockers. |
| Torrent client | M005 router update artifact transport | **blocked** | `plans/implementation/torrent-client/005-router-update-artifact-transport.md` | Requires M004 plus router-owned ReleaseTarget, private invocation, and capability-mediated artifact staging/export contracts. |

## Deferred work

Future M006 datagram trackers/DHT has no handoff plan. It remains deferred until the streaming/SAM corrective is closed and a stable production SAM datagram/PRIMARY/subsession contract exists.

Frontend work is deferred until backend/RPC/managed-runtime boundaries are stable.

## External interface watch

Primary upstream: `dbowm91/i2pr:main`.

Latest inspected upstream main on 2026-10-06: `144c54da2eaaa46497955e0e371f06ab6efcd1b1`.

The authoritative i2pr registry now records managed-app Plans 345, 349, 352, 353, 354, and 355 as closed. Plans 354/355 provide listener-independent private SAM/I2CP protocol connections and the router app-principal/capability gateway. Their contract is raw ordered SAM/I2CP protocol octets per authorized logical service stream; they deliberately do not provide torrent-specific lookup/connect/accept operations.

Remaining M004 upstream owners are narrower but still real: AppManager/package/process lifecycle and process authentication are not registered/implemented; OS sandbox/resource containment remains future work; host-owned local RPC ingress and private persistent-data semantics are not yet available to this consumer.

The upstream tree also has no torrent-facing router `ReleaseTarget`, private update invocation, or artifact staging/export contract, so M005 remains blocked independently.

Recheck i2pr main and the managed-app reference contract before promoting C001 downstream conclusions or M004/M005.
