# i2pr-tc Terminology and Domain Model

Status: canonical terminology

## Torrent domain

**TorrentMeta** — validated BitTorrent v1 metainfo. The infohash is computed from the exact encoded `info` dictionary bytes, not a reserialized semantic object.

**InfoHashV1** — 20-byte SHA-1 BitTorrent v1 infohash. Torrent identity, not software-release authorization.

**TorrentId** — stable local identifier for a persisted torrent. It is distinct from InfoHashV1.

**PieceMap** — bounded verified/missing/in-flight piece state.

**TransferSession** — live peer/tracker activity for one persisted torrent.

**ResumeState** — durable versioned local state used to accelerate restart. It never overrides piece/file verification when they disagree.

## I2P domain

**Destination** — full I2P Destination identity used for a peer or service.

**DestinationHash** — 32-byte SHA-256 hash of an I2P Destination; used by compact I2P tracker/PEX forms where specified.

**I2pPeerAddress** — canonical peer address represented by Destination or DestinationHash semantics. It is not `SocketAddr`.

**I2pSession** — long-lived application I2P session reused by tracker and peer connections. It is app-scoped and distinct from router identity.

**SamTransport** — injected stream/control transport over which the client speaks the supported SAM contract. Production managed-app code receives this through i2pr capability mediation.

**I2pTracker** — tracker reachable as an I2P service. It discovers peers but is not a trust authority for content or updates.

**I2pPex** — I2P peer exchange carrying I2P peer identity rather than IP socket tuples.

## Application/API domain

**TorrentService** — native application-level API over validated torrent operations. It owns no HTTP or Transmission naming.

**TransmissionAdapter** — translation layer from Transmission RPC into TorrentService operations and snapshots.

**PublishedLocalService** — router/AppManager-owned local ingress whose streams are forwarded to the app; the app does not own the listening host socket.

**ManagedAppSession** — one supervised app instance and its capability channel. It is not an I2P Destination.

## Update domain

**ReleaseTarget** — immutable target description supplied by router-owned update logic, binding target identity/version, expected length, cryptographic digest, and torrent source identity.

**ArtifactFetchRequest** — bounded request asking i2pr-tc to obtain a ReleaseTarget. It is not permission to install.

**StagedArtifact** — completed bytes held in app-private or capability-mediated staging pending router verification.

**UpdateTransport** — i2pr-tc's role: download and optionally seed.

**UpdateAuthority** — router-owned metadata authentication, version policy, final verification, installation, rollback, and restart. i2pr-tc is never UpdateAuthority.

## Required distinctions

- TorrentId != InfoHashV1.
- InfoHashV1 != release signature/digest.
- ManagedAppSession != I2pSession.
- I2pSession/Destination != router identity.
- I2pTracker != update metadata authority.
- TransmissionAdapter != TorrentService.
- StagedArtifact != installed update.
- app-private persistent storage != arbitrary host filesystem access.
