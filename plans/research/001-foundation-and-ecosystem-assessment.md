# Research 001 — I2P Torrent Client Foundation and Ecosystem Assessment

Status: planning foundation, reviewed 2026-10-05

## Question

What is the smallest correct Rust backend that can replace the essential I2PSnark role for i2pr, interoperate with the current I2P BitTorrent ecosystem, present a Transmission-compatible control surface, and later act as a safe transport for router updates?

## I2P BitTorrent semantics

The I2P BitTorrent specification is not merely conventional BitTorrent behind a proxy. Important wire/discovery differences must be first-class types:

- peers are identified by Destination/Destination-hash semantics rather than IP socket addresses;
- compact I2P tracker responses use 32-byte Destination hashes where specified;
- `i2p_pex` exchanges I2P peer identity;
- non-I2P announce/peer addresses are not valid fallback discovery for an I2P-only client;
- one long-lived I2P session should be reused for tracker and peer operation.

Primary reference:
https://www.i2p.net/en/docs/applications/bittorrent/

Consequence: general Rust torrent engines whose peer/tracker/DHT state is fundamentally `SocketAddr`-centric are poor production substrates even when low-level protocol crates are useful.

## Discovery sequencing

The first functional client does not need I2P DHT. HTTP I2P trackers plus `i2p_pex` provide a useful baseline and avoid making first release depend on SAM datagram/PRIMARY/subsession semantics.

I2P UDP/datagram tracker support is legitimate later work:
https://i2p.net/en/docs/specs/udp-announces/

DHT should follow only after the production SAM/datagram interface is stable and tested against more than one router implementation.

## Rust ecosystem survey

rqbit/librqbit is the strongest current Rust reference and is Apache-2.0. It has mature separation of bencode, peer protocol, trackers, DHT, storage, and application layers. It is useful for behavior, algorithms, tests, and possibly independently reusable permissive low-level crates.

However current rqbit tracker, peer, session, DHT, and listener layers make extensive use of IP `SocketAddr`, TCP listeners, UDP sockets, UPnP, and IP-oriented PEX. Refactoring the whole engine into I2P Destination semantics would be a broad invasive fork, not a transport substitution.

Reference:
https://github.com/ikatson/rqbit

TorrentNG is useful as a Transmission compatibility oracle but its AGPL-3.0 licensing and general-purpose network scope make it a reference rather than desired foundation.

Reference:
https://github.com/snapetech/TorrentNG

Decision: build a small I2P-native engine and selectively reuse or learn from permissively licensed low-level components after dependency review.

## Metainfo/bencode requirement

BitTorrent v1 infohash correctness requires hashing the exact bencoded bytes of the `info` dictionary. A parser that normalizes/re-encodes that dictionary before hashing is insufficient.

A mature strict bencode crate may provide semantic decoding, but the implementation must retain/recover the exact bounded `info` byte slice. If no reviewed dependency provides the needed span behavior, implement a small bounded top-level scanner rather than a second generic serialization framework.

## Transmission compatibility

Transmission compatibility provides existing CLI, automation, and third-party controller interoperability without coupling clients to i2pr frontend work.

The current Transmission RPC specification uses modern JSON-RPC/snake_case semantics while legacy clients use the historical envelope and older naming. Maintain one native TorrentService and adapt both shapes into it.

Reference:
https://github.com/transmission/transmission/blob/main/docs/rpc-spec.md

Initial useful methods: session_get, session_stats, torrent_get, torrent_add, torrent_set for supported properties, torrent_start/start_now/stop, torrent_verify, torrent_reannounce, torrent_remove, and free_space when truthful.

Unsupported settings must return explicit unsupported/invalid outcomes rather than silent success.

## Managed-app pressure

The active i2pr line `codex/plan-345-native-app-runtime` defines a secured profile that denies direct host networking and loopback and exposes I2P access through app-scoped router capabilities. This is the desired production security model.

Reviewed i2pr documents:

- `docs/adr/0032-managed-native-app-process-and-capability-boundary.md`
- `specs/references/managed-native-app-runtime-v1.md`
- `plans/implementation/managed-native-app-runtime/349-managed-app-v1-direction-broker-network-policy-corrective.md`

The torrent app exposes successor interface needs that must not be worked around locally:

1. app-scoped SAM service stream without dialing a host TCP port;
2. router/AppManager-owned local ingress for Transmission RPC;
3. persistent app-data and capability-mediated update artifact handoff;
4. for updates, a private bounded host-to-app service invocation distinct from public Transmission RPC.

These are recorded in `docs/integration/i2pr-managed-app-requirements.md`.

## Router update precedent

Java I2P already uses I2PSnark as a torrent update transport. I2PSnark registers an updater for router signed/SU3 updates, release metadata can advertise torrent update sources, and the torrent update runner returns the completed artifact to update management rather than treating BitTorrent completion as install authorization.

References:
https://github.com/i2p/i2p.i2p/blob/master/apps/i2psnark/java/src/org/klomp/snark/UpdateHandler.java
https://github.com/i2p/i2p.i2p/blob/master/apps/i2psnark/java/src/org/klomp/snark/UpdateRunner.java
https://github.com/i2p/i2p.i2p/blob/master/apps/routerconsole/java/src/net/i2p/router/update/NewsFetcher.java

This is the correct conceptual precedent, but i2pr-tc should use a narrow typed artifact request/result boundary.

## Update metadata options

The torrent client should not choose the router's release metadata format.

Router-side options remain: small i2pr-specific signed manifest; SU3-compatible metadata/artifacts when ecosystem compatibility warrants it; or TUF-style metadata for explicit rollback/freeze/key-rotation/threshold trust semantics.

The torrent client only needs a normalized `ReleaseTarget` with immutable target properties and torrent source information.

A recovery transport must remain available when the torrent app is absent, broken, or incompatible. Torrent must not become the router's only update/recovery path.

## Foundational decisions

Accepted:

- purpose-built I2P-native engine;
- BitTorrent v1 first;
- HTTP I2P trackers + streaming peers + magnet metadata + i2p_pex first;
- datagram tracker/DHT later;
- Transmission compatibility as adapter;
- managed production process has no direct host networking;
- torrent app is update transport, never update authority;
- no frontend in foundational milestones.

## Research checkpoints

Before M002: re-check current I2P BitTorrent/SAM specifications and Java I2PSnark/i2pd streaming interoperability.

Before M003: re-check current Transmission RPC spec/version and transmission-remote behavior.

Before M004: re-check exact i2pr managed-app head, live SAM gateway, local-service ingress, and persistent-data ownership.

Before M005: re-check i2pr update metadata/trust design, artifact staging/export, private host-to-app invocation, and desired release seeding policy.
