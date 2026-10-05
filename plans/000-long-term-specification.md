# i2pr-tc Long-Term Specification

Status: canonical product and architecture specification

## 1. Product identity

i2pr-tc is a Rust BitTorrent client specialized for operation inside I2P. Its primary deployment target is an i2pr managed native application, while the torrent engine remains testable without i2pr and may expose bounded standalone development adapters.

The backend is the product foundation. A graphical or web frontend is not required for the foundational milestones.

## 2. Primary capabilities

The long-term system must provide:

1. correct BitTorrent v1 metainfo, magnet, peer-wire, piece verification, storage, resume, and recheck behavior;
2. I2P HTTP tracker interoperability, I2P peer streams, magnet metadata exchange, multi-tracker fallback, and `i2p_pex`;
3. resilience to high latency, peer churn, router reconnects, and long periods without usable peers;
4. Transmission RPC compatibility sufficient for existing tools such as transmission-remote and third-party clients;
5. a managed-app deployment that reaches I2P only through app-scoped router capabilities and does not require unrestricted host networking;
6. a bounded artifact-fetch/seeding service that can be used by the router as one transport for authenticated software updates;
7. later I2P datagram tracker and DHT support when required SAM/datagram contracts and interoperability evidence are available.

## 3. Architectural invariants

### Network boundary

Production torrent traffic is I2P-only. Clearnet trackers, clearnet peers, public DNS, LPD, UPnP/NAT-PMP, and transparent fallback to IP transports are not supported by the managed production profile. Transport policy must be structural, not a preference bit inside a process that can still create arbitrary sockets.

### Address model

Canonical peers are I2P Destinations or Destination hashes. IP socket addresses may exist only inside explicit test/development adapters and must never become persisted or protocol-authoritative peer identity. Tracker and PEX compact records are interpreted according to I2P BitTorrent semantics.

### Session ownership

A torrent runtime reuses a bounded long-lived I2P client session for tracker and peer activity. The torrent application's Destination is distinct from router identity, publisher identity, managed-app identity, and torrent infohash.

### Protocol layering

The torrent core must not own SAM sockets, router control credentials, Transmission listeners, UI state, or update trust policy.

Expected dependency direction:

```text
strict torrent protocol/state
    <- storage/persistence
    <- I2P discovery/transport
    <- native TorrentService
    <- Transmission / managed-app / update adapters
```

### Update trust separation

A BitTorrent infohash proves transport/content identity within BitTorrent; it does not authorize software installation.

i2pr remains authoritative for trusted release metadata/signing keys, version/platform selection, rollback/freeze/expiry policy, final target verification, installation, rollback, restart, and update policy.

i2pr-tc may fetch an immutable target described by the router, enforce normal torrent and caller-supplied target bounds, stage/export completed bytes through an authorized capability, and optionally seed an already accepted release under bounded policy. It must never convert torrent completion directly into installation.

### Transmission boundary

Transmission RPC is a compatibility surface, not the native domain model. Unsupported methods/settings must fail truthfully rather than return successful no-ops.

The managed app itself must not bind loopback merely to satisfy Transmission clients. Local endpoint publication is host/AppManager authority.

### Filesystem boundary

The first managed deployment may use an app-private persistent data root. Arbitrary user-selected host directories require a separately authorized scoped-filesystem capability. Router-update artifacts require capability-mediated staging/export, not arbitrary shared paths.

## 4. Security requirements

All hostile network and metainfo inputs are bounded before allocation or state expansion. Path traversal, absolute paths, ambiguous components, length overflow, piece-count overflow, frame overflow, and similar amplification must fail closed.

Peer, tracker, PEX, magnet metadata, RPC, resume-state, and managed-app boundaries require fuzz/property coverage where practical.

No peer/tracker input may influence host network policy, update trust/installation, managed-app grants, arbitrary filesystem paths, or process execution.

## 5. Initial compatibility target

The first implementation deliberately excludes BitTorrent v2/hybrid torrents, clearnet DHT, UDP trackers over IP, uTP, LPD, NAT traversal, UPnP/NAT-PMP, clearnet web seeds, and a frontend.

## 6. End-state completion

The backend is release-capable when a managed i2pr deployment can add an I2P torrent or magnet; discover peers through in-network trackers/PEX; download, verify, resume, recheck, and seed across restarts; tolerate disconnects and long I2P latency; expose the documented Transmission-compatible API without giving the app host networking; fetch a router-requested immutable update artifact without gaining update trust authority; and prove those properties with interoperability, fault, fuzz, restart, and security-boundary evidence.
