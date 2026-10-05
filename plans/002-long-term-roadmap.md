# i2pr-tc Long-Term Implementation Roadmap

Status: execution roadmap for `plans/000-long-term-specification.md`

The roadmap is dependency-ordered, not calendar-ordered.

## Phase 0 — Torrent core foundation

Own BitTorrent v1 metainfo/magnet parsing, infohash correctness, peer-wire codec/state, piece scheduling primitives, storage, resume/recheck, cancellation, and hostile-input bounds without router/network dependencies.

Plan: `plans/implementation/torrent-client/001-core-protocol-storage-foundation.md`.

## Phase 1 — I2P streaming, trackers, metadata, and PEX

Add I2P transport/address/discovery using an injected SAM transport. Reuse one long-lived I2P session and support HTTP I2P trackers, inbound/outbound streaming peers, magnet metadata, multi-tracker fallback, and `i2p_pex`. No DHT/datagram prerequisite.

Plan: `plans/implementation/torrent-client/002-i2p-streaming-trackers-and-pex.md`.

## Phase 2 — Transmission RPC compatibility

Add a transport-independent Transmission adapter with current RPC semantics plus bounded legacy compatibility. Implement the common control/query surface and truthful unsupported behavior.

Plan: `plans/implementation/torrent-client/003-transmission-rpc-compatibility.md`.

Phase 1 and Phase 2 may overlap after Phase 0 freezes TorrentService.

## Phase 3 — i2pr managed-app integration

Package the backend as a secured i2pr native app. I2P access comes through app-scoped SAM capability; Transmission ingress comes through router/AppManager-published local service; persistent data uses an authorized app-private root.

Blocked until corresponding i2pr runtime capabilities are implemented and qualified. Direct host networking is not a substitute.

Plan: `plans/implementation/torrent-client/004-i2pr-managed-app-integration.md`.

## Phase 4 — Router update artifact transport

Add private bounded router-to-app artifact fetching. i2pr supplies an authenticated immutable ReleaseTarget; i2pr-tc fetches/stages it; router code independently authenticates and installs it. Add bounded post-verification seeding without making torrent the sole recovery path.

Plan: `plans/implementation/torrent-client/005-router-update-artifact-transport.md`.

## Phase 5 — Advanced I2P discovery

Future work only: I2P datagram/UDP trackers, I2P DHT through required SAM PRIMARY/subsession/datagram semantics, and possible BitTorrent v2/hybrid evaluation.

Do not write an M006 handoff until the SAM/datagram interface is stable and Phase 1 interoperability evidence justifies it.

## Phase 6 — Frontends

Frontend work follows stable backend/RPC/managed-runtime contracts and consumes those service boundaries rather than becoming a second torrent/network authority.

## Cross-phase rules

Every phase preserves I2P-only production networking, strict input bounds, restart/cancellation semantics, one authority for persisted torrent state, and update trust separation. Every implemented milestone requires a closure record and unblock audit.
