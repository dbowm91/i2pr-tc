# Torrent Client M002 — I2P Streaming, Trackers, Magnet Metadata, and PEX

Status: blocked on M001 closure

Source roadmap:
`plans/subsystems/torrent-client-roadmap.md#M002--i2p-streaming-trackers-and-pex`

Primary class: capability + network-protocol infrastructure

Hard dependency: M001 closed.
Interface dependency: current I2P SAM/BitTorrent specifications rechecked at implementation start.

## Objective

Make the M001 torrent service functional on I2P without clearnet fallback or datagram/DHT prerequisites.

Provide injected SAM transport, one long-lived application I2P session, I2P naming/Destination resolution, HTTP I2P tracker announces, inbound/outbound streaming peers, magnet metadata, multi-tracker retry/fallback, `i2p_pex`, and robust reconnect/backoff.

## Transport boundary

Define SAM/session code over injected async byte streams/control channels.

Production managed-app code must not dial a host SAM TCP port. M004 supplies the stream through i2pr.

A standalone development/interoperability adapter may connect to local SAM behind an explicit non-default feature/binary. It remains I2P-only and cannot become the managed production path.

## Address/session model

Canonical I2P peers are full Destination or 32-byte DestinationHash with bounded resolution state/cache semantics. Never translate peers into IP SocketAddr.

Reuse one long-lived I2P session for tracker/peer activity. Session identity is app-scoped and never router identity. Support provided identity and ephemeral development identity; production persistence policy is deferred to M004.

## HTTP I2P trackers

Reject clearnet/IP-literal tracker targets before connection.

Support GET announce initially, binary info_hash/peer_id encoding, started/stopped/completed, uploaded/downloaded/left, bounded numwant, interval/min interval, failure/warning, compact I2P peer records, bounded announce tiers/retry/backoff, and explicit tracker failure state.

Bound HTTP response/headers/body/peer count. Redirects must not escape I2P policy.

## Peer streams

Provide outbound connect and inbound accept through the same I2P session.

Require infohash handshake match, bounded pending inbound handshakes, no peer-ID trust assumption, M001 choke/request/piece flow, I2P-appropriate handshake/idle/request timeouts, backpressure, peer ceilings, disconnect cleanup, and retry suppression.

## Magnet metadata

Use extension protocol/`ut_metadata` with metadata size/piece/in-flight ceilings. Resulting exact metadata infohash must match the magnet. Invalid/mismatched metadata cannot poison torrent state.

## i2p_pex

Use I2P hashes, not IPv4/IPv6 compact tuples. Bound additions/drops. PEX peers enter the same dedup/backoff pipeline as tracker peers and cannot change policy.

## Reliability under bad network conditions

Specify/test exponential backoff with ceilings/jitter; tracker failures independent of surviving peers; SAM disconnect invalidates live streams but not persisted torrent intent; reconnect gradually restores activity; no queued-connect pileup after outage; bounded per-peer request windows; cancellation wins over timers; shutdown stopped-announces are best effort and bounded.

## Ordered work packages

WP1 injected SAM adapter + scripted fake.
WP2 I2P address/resolution and session lifecycle.
WP3 bounded HTTP I2P tracker client/compact peer parsing.
WP4 peer connect/accept integration.
WP5 magnet ut_metadata.
WP6 i2p_pex + peer-source dedup/backoff.
WP7 fault/restart/interoperability qualification.

## Required tests

Reject conventional compact IPv4/IPv6 peers and clearnet/IP trackers before dialing; compact 32-byte tracker records; duplicate peers; wrong infohash handshake; slow/idle peers; mid-piece/mid-metadata disconnect; metadata mismatch/oversize; PEX/tracker response bounds; tracker tier failover; SAM disconnect with many torrents; cancellation during backoff/connect/announce; bounded reconnect; session reuse; no router identity reuse.

Use deterministic fixtures. Closure should also include real-router interoperability against current Java I2P and at least one alternate router/SAM implementation when feasible. If external evidence cannot be gathered, record it as operationally outstanding rather than fabricate a pass.

## Acceptance criteria

M002 closes when an I2P-only swarm can add torrent/magnet, discover through I2P trackers/PEX, transfer/verify, accept peers, resume after peer/router disconnects, and seed without a direct clearnet path in production architecture.

## Stop conditions

Stop if SAM requires canonical SocketAddr peer state, required tracker behavior needs direct DNS/clearnet fallback, DHT/datagrams become necessary for basic functionality, production needs a direct host SAM socket, or interoperability reveals ambiguity needing an ADR/spec decision.

## Closure evidence

Create `plans/closure/torrent-client/002-status.md` with current spec/router versions, SAM/session matrix, tracker/PEX fixtures, fault/backoff evidence, real-router interoperability status, network-boundary guard evidence, verification commands, findings, and unblock audit.
