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

Execution is decomposed into M004-A through M004-D so closed upstream work can be consumed independently while remaining security interfaces stay fail-closed. Direct host networking is not a substitute.

Umbrella: `plans/implementation/torrent-client/004-i2pr-managed-app-integration.md`.

Subplans:
- `009-m004a-managed-package-lifecycle-bootstrap.md` — external SDK/package + appd/apphost lifecycle;
- `010-m004b-managed-sam33-capability-composition.md` — private SAM 3.3 composition;
- `011-m004c-private-data-and-secured-profile.md` — app-private state, persistent Destination, Linux Secured;
- `012-m004d-transmission-local-ingress.md` — host-owned local Transmission publication.

## Phase 4 — Router update artifact transport

Add private bounded router-to-app artifact fetching. i2pr supplies an authenticated immutable ReleaseTarget; i2pr-tc fetches/stages it; router code independently authenticates and installs it. Add bounded post-verification seeding without making torrent the sole recovery path.

Plan: `plans/implementation/torrent-client/005-router-update-artifact-transport.md`.

## Phase 5 — Advanced I2P discovery

The SAM/datagram prerequisite is now stable on the application side. M006 is split into:

- M006-A `013-m006-i2p-dht-krpc-core.md` — closed; runtime-neutral I2P KRPC, secure node IDs, routing, tokens, tracker state, persistence;
- M006-B `014-m006-i2p-dht-transport-integration.md` — ready; binds protocol 17/18 children to the same Destination and integrates DHT peer discovery.

Datagram/UDP tracker support and BitTorrent v2/hybrid remain later evaluations rather than being folded into the first DHT implementation.

## Phase 6 — Frontends

Frontend work follows stable backend/RPC/managed-runtime contracts and consumes those service boundaries rather than becoming a second torrent/network authority.

## Cross-phase rules

Every phase preserves I2P-only production networking, strict input bounds, restart/cancellation semantics, one authority for persisted torrent state, and update trust separation. Every implemented milestone requires a closure record and unblock audit.
