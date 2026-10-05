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
| I2P torrent client foundation | active | `plans/subsystems/torrent-client-roadmap.md` | M001 ready | M001 has no hard dependency. M002/M003 depend on M001; M004 additionally depends on live i2pr managed-app interfaces; M005 depends on router update invocation/artifact contracts. |

## Implementation handoffs

| Workstream | Milestone | Status | Plan | Dependency note |
|---|---|---|---|---|
| Torrent client | M001 core protocol + storage foundation | **ready** | `plans/implementation/torrent-client/001-core-protocol-storage-foundation.md` | No hard dependency; initial coding handoff. |
| Torrent client | M002 I2P streaming + trackers + magnet metadata + PEX | **blocked** | `plans/implementation/torrent-client/002-i2p-streaming-trackers-and-pex.md` | Hard-blocked on M001 closure; recheck current I2P/SAM specs at handoff. |
| Torrent client | M003 Transmission RPC compatibility | **blocked** | `plans/implementation/torrent-client/003-transmission-rpc-compatibility.md` | Blocked until M001 freezes TorrentService; may then run parallel to M002. |
| Torrent client | M004 i2pr managed-app integration | **blocked** | `plans/implementation/torrent-client/004-i2pr-managed-app-integration.md` | M002 plus corrected/live i2pr app runtime, SAM gateway, local ingress, and persistent-data contract. |
| Torrent client | M005 router update artifact transport | **blocked** | `plans/implementation/torrent-client/005-router-update-artifact-transport.md` | M002/M004 plus router-owned ReleaseTarget, private app invocation, and artifact staging/export. |

## Deferred work

Future M006 datagram trackers/DHT has no handoff plan. It remains deferred until M002 closes and a stable production SAM datagram/PRIMARY/subsession contract exists.

Frontend work is deferred until backend/RPC/managed-runtime boundaries are stable.

## External interface watch

Primary upstream:
`dbowm91/i2pr:codex/plan-345-native-app-runtime`

Current reviewed upstream planning includes ADR 0032, the managed-native-app v1 reference, and Plan 349's contract corrective. Treat those as moving interfaces. Before promoting M004 or M005 to ready, re-review current i2pr head and update the roadmap/plan rather than coding around stale assumptions.
