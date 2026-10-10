# Torrent Client M006-B — I2P DHT Transport and Torrent Integration

Status: active

Date: 2026-10-10

Implementation started: 2026-10-10. The first implementation tranche adds
same-Destination cached DATAGRAM/RAW channel access, bounded query/reply
correlation, token-scoped announce, DHT peer-source composition, bootstrap
restore/save, and a deterministic two-node fixture. Live Java I2P traffic and
the remaining acceptance audit are still open.

Parent roadmap phase: M006 advanced I2P discovery.

Primary class: discovery capability + async networking/lifecycle invariant.

Hard dependencies:

- M006-A closed (closure: `plans/closure/torrent-client/013-m006a-status.md`).
- C003 transport implementation retained.
- for i2pr-managed qualification: upstream SAM/368 + M004-B.
  These are not required to develop the transport adapter against Java I2P.

Operational dependency for final live interoperability evidence:
at least one router position with working I2P transit and a reachable DHT peer.

## 1. Objective

Bind the M006-A DHT core to C003's same-Destination DATAGRAM/RAW children and
feed discovered peer hashes into the existing torrent peer-source model.

No host UDP socket is introduced.

## 2. Transport mapping

Use the same C003 primary Destination as peer Streaming.

Establish:

- query port Q;
- response port R = Q + 1;
- protocol-17 repliable child on Q;
- protocol-18 raw child on R.

The exact KRPC role mapping must match the current deployed I2P DHT profile
frozen by M006-A: signed queries, raw replies/errors/announce-query path as
applicable.

A DHT child loss must not destroy healthy peer STREAM state unless the primary
session itself is lost.

## 3. Destination-hash resolution

Compact nodes carry Destination hashes, not full Destinations.

Use the existing canonical hash resolution path:

```text
32-byte DestinationHash
  -> canonical 52-char b32.i2p name
  -> I2pSession::lookup
  -> Destination
  -> verify SHA-256 equals requested hash
```

Cache only bounded positive results with explicit expiry/generation ownership.
A mismatched lookup is a hard address error and never updates routing state.

The DHT core itself remains unaware of SAM naming strings.

## 4. Async engine

Own bounded asynchronous tasks for:

- receive loops for protocol 17/18;
- pending reply correlation and timeout cleanup;
- routing maintenance and periodic persistence.

This repository's `TorrentRuntime` deliberately does not own network sessions
or a per-torrent async actor. M006-B therefore exposes bounded caller-driven
`get_peers`, iterative traversal, and token-authorized announce operations;
the torrent owner decides when to start, refresh, and stop discovery. The
responder must not invent a second scheduler or hold a torrent lifecycle lock.

All queues and JoinSets have explicit ceilings. Cancellation stops admission,
cancels outstanding work, joins/aborts children, and flushes only bounded
committed state.

No busy retry loop when a peer/router is unavailable.

## 5. Bootstrap

Supported bootstrap sources, in priority order after WP1 confirmation:

- persisted known-node state;
- trackerless torrent `nodes` Destination hashes where supplied;
- explicitly configured I2P DHT bootstrap hashes if a canonical ecosystem list
  exists and provenance is documented;
- nodes learned from valid replies.

Do not use clearnet bootstrap hosts, DNS, public UDP, or conventional BitTorrent
DHT routers.

An empty DHT remains a valid state: tracker/PEX discovery continues.

## 6. Torrent peer-source integration

Extend `PeerSources` with DHT provenance without making DHT authoritative over
verified torrent state.

Rules:

- DHT peer result is one 32-byte Destination hash;
- filter local hash;
- deduplicate with tracker/PEX;
- failed-peer backoff remains shared or consistently reconciled;
- dropping a DHT source does not drop a peer still known from tracker/PEX;
- discovered peers go through the existing Destination resolution +
  peer-handshake/infohash validation path.

DHT failure never blocks tracker/PEX operation.

## 7. Torrent lifecycle

For a running torrent:

- the torrent owner can perform bounded `get_peers` and iterative traversal;
- the torrent owner can announce only to nodes with a cached, unexpired token
  for the same infohash;
- the API permits refresh before peer starvation and does not run continuous
  background polling;
- distinguish seed/leech flags if the deployed profile supports them;
- on stop, the owner withdraws DHT peer-source provenance; remote announce
  records expire under the deployed profile because no unannounce operation is
  defined;
- magnets may use DHT for peer discovery, but metadata still requires
  `ut_metadata` and exact infohash verification.

Global DHT identity/session is shared across torrents; per-torrent lookups are
bounded consumers, not separate Destinations.

## 8. Privacy and fingerprinting

- no client/version string in KRPC;
- no host IP/port fields;
- do not encode router implementation identity;
- one app Destination is intentionally shared across this torrent client's
  peer/DHT activity; document this linkability domain explicitly;
- do not create a DHT Destination per torrent;
- logs use truncated/opaque identifiers according to existing privacy policy,
  never full private Destination material.

## 9. Live qualification

### Java I2P

With working transit, prove:

- PRIMARY + protocol-17/18 children;
- query reaches an independent I2P DHT peer;
- valid response is received/parsed;
- at least ping plus one iterative find/get-peers trajectory;
- a discovered peer hash resolves and, where available, reaches peer
  Streaming.

### i2pr

After SAM/368 + M004-B, repeat through the private managed-app gateway.

### i2pd

Record current 2.61 behavior as incompatible with datagram subsessions rather
than inventing a fallback. Requalify only if a later i2pd release changes it.

A missing live network is an explicit operational blocker, never a reason to
weaken deterministic acceptance.

## 10. Work packages

WP1 — freeze transport/bootstrap/profile mapping from M006-A dossier.

WP2 — add bounded DATAGRAM/RAW engine and hash-resolution cache.

WP3 — integrate DHT provenance into peer-source state.

WP4 — bounded caller-driven get-peers/announce lifecycle operations and stop
semantics consistent with the networked torrent owner boundary.

WP5 — persistence/restart/cancellation/backpressure.

WP6 — deterministic two-node/in-memory transport qualification.

WP7 — live Java I2P and, when available, i2pr managed qualification.

WP8 — docs/guards/closure.

## 11. Acceptance criteria

M006-B closes when:

1. DHT uses protocol 17/18 children under the same Destination as peer STREAM;
2. no host UDP/network connector exists;
3. compact node hashes resolve through verified b32 lookup;
4. peer results deduplicate with tracker/PEX and preserve source semantics;
5. tracker/PEX continue working when DHT is empty/unavailable;
6. async tasks, transactions, queues, lookup cache, and retry behavior are
   bounded/cancellable;
7. deterministic multi-node tests exercise ping/find/get/announce;
8. live evidence is recorded against at least one compatible deployed router
   when operational infrastructure exists;
9. i2pr private-gateway qualification is added after SAM/368/M004-B before a
   managed-product DHT claim;
10. routine verification passes.

## 12. Stop conditions

Stop if compatible routers require host UDP, if compact-hash lookup cannot
verify the resolved Destination, if DHT requires a second I2P Destination, or
if live qualification reveals a wire profile incompatible with M006-A's frozen
core assumptions.

## 13. Closure evidence

Create `plans/closure/torrent-client/014-m006b-status.md` with transport map,
same-Destination proof, peer-source behavior, deterministic network fixture,
persistence/restart/cancellation results, Java/i2pr/i2pd matrix, live-network
evidence or explicit operational block, and advanced-discovery completion
disposition.
