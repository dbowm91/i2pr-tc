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
| I2P torrent client foundation | closing | `plans/subsystems/torrent-client-roadmap.md` | M001 closing | M001 implementation and local verification are complete; M002/M003 remain blocked until the closure commit lands. M004/M005 retain upstream interface blockers. |

## Implementation handoffs

| Workstream | Milestone | Status | Plan | Dependency note |
|---|---|---|---|---|
| Torrent client | M001 core protocol + storage foundation | **closing** | `plans/implementation/torrent-client/001-core-protocol-storage-foundation.md` | Verification passed; closure record and final implementation commit are being recorded. |
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

Latest inspected upstream head: `144c54da2eaaa46497955e0e371f06ab6efcd1b1` (`main`; rechecked 2026-10-06), with managed-runtime plan branch `ea7b5ccef9bacbddf826f074cc59d891849a1424`. Plans 349 and 352–355 close the corrected v1 policy and router-side private SAM/I2CP gateway. The current registry still says AppManager/package/process work is eligible but not registered; process authentication/runtime, package lifecycle, and OS sandbox contracts are therefore still absent. The upstream tree still has no torrent-facing ReleaseTarget or artifact staging/export contract. M004 remains blocked on M002 and the unregistered managed-app contract; M005 remains blocked on M002/M004 and those router-owned update interfaces. Re-review the exact upstream head before any later promotion.
