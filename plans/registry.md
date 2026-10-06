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
| I2P torrent client foundation | active | `plans/subsystems/torrent-client-roadmap.md` | M004 blocked | M001 closed; M002 conditionally closed pending live-router qualification; M003 closed on M001. M004/M005 retain current upstream interface blockers. |

## Implementation handoffs

| Workstream | Milestone | Status | Plan | Dependency note |
|---|---|---|---|---|
| Torrent client | M001 core protocol + storage foundation | **closed** | `plans/implementation/torrent-client/001-core-protocol-storage-foundation.md` | Closure record: `plans/closure/torrent-client/001-status.md`; implementation commit `61ace427d7157e8f168d14a3bc8cc3501fd6492f`. |
| Torrent client | M002 I2P streaming + trackers + magnet metadata + PEX | **conditionally closed** | `plans/implementation/torrent-client/002-i2p-streaming-trackers-and-pex.md` | Closure record: `plans/closure/torrent-client/002-status.md`; implementation commit `cbb65d03635652896fa41132c06bf60324c14147`. Live-router, end-to-end magnet, inbound transfer, and reconnect qualification remain operationally outstanding. |
| Torrent client | M003 Transmission RPC compatibility | **closed** | `plans/implementation/torrent-client/003-transmission-rpc-compatibility.md` | Closure record: `plans/closure/torrent-client/003-status.md`; implementation commits `8c111cf59eaa54366caed8f72fd84347fd6bddba`, `85d95002ff01a82333f93ffe7178b007dd8c2689`. |
| Torrent client | M004 i2pr managed-app integration | **blocked** | `plans/implementation/torrent-client/004-i2pr-managed-app-integration.md` | M002 operational qualification plus upstream Plans 354/355 and unregistered AppManager/package/process, host ingress, and persistent-data owners. |
| Torrent client | M005 router update artifact transport | **blocked** | `plans/implementation/torrent-client/005-router-update-artifact-transport.md` | M002/M004 plus no router-owned ReleaseTarget, private invocation, or artifact staging/export contract. |

## Deferred work

Future M006 datagram trackers/DHT has no handoff plan. It remains deferred until M002 closes and a stable production SAM datagram/PRIMARY/subsession contract exists.

Frontend work is deferred until backend/RPC/managed-runtime boundaries are stable.

## External interface watch

Primary upstream:
`dbowm91/i2pr:codex/plan-345-native-app-runtime`

Latest inspected upstream refs (2026-10-06): `main` is `144c54da2eaaa46497955e0e371f06ab6efcd1b1`; `codex/plan-345-native-app-runtime` is `ea7b5ccef9bacbddf826f074cc59d891849a1424`. The authoritative `main` registry has Plan 354 ready and Plan 355 blocked on 354; those gateway plans are not closed. The managed-app roadmap and Plan 355 explicitly leave process authentication/runtime, package lifecycle/AppManager, and OS sandbox implementation to a future unregistered owner. No host-owned ingress/private-persistent-data contract is available to this consumer, and the upstream tree has no torrent-facing `ReleaseTarget`, private invocation, or artifact staging/export contract. M004 remains blocked on M002 and those upstream interfaces; M005 remains blocked on M002/M004 and router-owned update interfaces. Recheck both refs and contract files before any later promotion.
