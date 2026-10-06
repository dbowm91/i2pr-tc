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
| I2P torrent client foundation | active | `plans/subsystems/torrent-client-roadmap.md` | M001 active | M001 is in progress. M002/M003 remain blocked on M001 closure; M004 additionally depends on live i2pr managed-app interfaces; M005 depends on router update invocation/artifact contracts. |

## Implementation handoffs

| Workstream | Milestone | Status | Plan | Dependency note |
|---|---|---|---|---|
| Torrent client | M001 core protocol + storage foundation | **active** | `plans/implementation/torrent-client/001-core-protocol-storage-foundation.md` | Implementation started; closure evidence and remaining acceptance work are outstanding. |
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

Latest inspected upstream head: `2f82c7998fc9f43c6f94843b58faa0b0fdc9c2e4` (`main`; 2026-10-06), with managed-runtime plan branch `ea7b5ccef9bacbddf826f074cc59d891849a1424`. Plans 349 and 352–355 now close the corrected v1 policy and router-side private SAM/I2CP gateway. This clears the previously recorded corrected-contract/gateway blocker, but does not supply a managed app manager, process authentication/runtime, package lifecycle, or OS sandbox; upstream states no AppManager/package/process plan is registered yet. M004 therefore remains blocked on M002 and the missing app-runtime contract. The upstream tree still contains no torrent-facing ReleaseTarget or artifact staging/export contract, so M005 remains blocked on M002/M004 and those router-owned update interfaces. Re-review the exact upstream head before any later promotion.
