# Torrent Client C003 — SAM 3.3 Primary/Subsession Transport Corrective

Status: ready

Date: 2026-10-06

Historical source work:

- `plans/implementation/torrent-client/002-i2p-streaming-trackers-and-pex.md`
- `plans/implementation/torrent-client/006-m002-c001-sam-runtime-hardening.md`
- `plans/closure/torrent-client/002-status.md`
- `plans/closure/torrent-client/006-m002-c001-status.md`

Paired upstream dependency:

- `dbowm91/i2pr` Plan 368 — SAM 3.3 PRIMARY/subsession shared-destination profile

Primary class: corrective transport architecture + future capability foundation

## 0. Why this corrective exists

C001 correctly moved SAM protocol ownership into the application and removed
several real wire/runtime defects, but subsequent live-router work and a
SAM-versus-I2CP architecture review exposed a deeper mistake: the client still
models SAM as a STREAM-only collection of independent connections instead of
one long-lived SAM session/destination with attachment connections and, for the
future DHT case, multiple protocol subsessions sharing that same Destination.

The current implementation performs `SESSION CREATE` while opening fresh raw
connections and treats a transient identity as effectively per connection.
That is not the session lifetime model the torrent client needs.

The architecture review also changed the long-term decision. I2PSnark's I2P DHT
uses the same Destination as peer Streaming and multiplexes protocol 17
repliable datagrams plus protocol 18 raw datagrams. Raw I2CP exposes that model
directly, but would also require a non-Java application to own substantially
more client machinery, including destination/LeaseSet lifecycle and an I2P
Streaming implementation. SAM 3.3 PRIMARY plus STREAM/DATAGRAM/RAW subsessions
was designed to expose the same shared-Destination shape without moving
Streaming into this application.

Accordingly:

- do **not** switch i2pr-tc to a private/raw I2CP client in this corrective;
- do **not** continue treating SAM 3.1 STREAM as the final torrent transport;
- target SAM 3.3 PRIMARY/subsession semantics now so future I2P DHT does not
  require another identity/transport redesign.

## 1. Research conclusions made authoritative by this plan

1. I2PSnark is the primary behavioral reference for I2P BitTorrent DHT.
   Its KRPC implementation uses one I2P session/Destination, signed/repliable
   datagrams for DHT queries, raw datagrams for replies/errors/announce
   queries, and I2P-specific compact node/peer encodings.
2. Established SAM-based torrent stacks generally do not currently provide I2P
   DHT. This is an implementation/deployment limitation, not proof that SAM
   cannot support it.
3. SAM 3.3 PRIMARY/subsessions provides the required shared-Destination model:
   one primary identity/tunnel set with STREAM, DATAGRAM, and RAW child
   sessions.
4. Current i2pd implements the 3.3-style shared-session machinery but uses
   `MASTER` terminology in its implementation where current SAM documentation
   uses `PRIMARY`. Interoperability must therefore be qualified explicitly
   instead of assuming one spelling works everywhere.
5. Raw I2CP remains a valid future alternative only if a reusable Rust
   application-side I2CP + Streaming SDK is deliberately created outside this
   torrent crate. i2pr-tc must not grow a second Streaming stack ad hoc.

## 2. Objective

Replace the current SAM session model with a bounded SAM 3.3-capable transport
owner that:

- owns exactly one long-lived primary torrent Destination/session;
- exposes the real local Destination and SHA-256 Destination hash;
- attaches peer/tracker STREAM operation to that primary identity;
- defines reserved DATAGRAM and RAW child transports on the same identity for
  future I2P DHT;
- remains independent of router implementation types and host sockets;
- composes through the managed-app raw SAM service stream when upstream Plan
  368 is available.

C003 does **not** implement the DHT algorithm itself. It establishes and
qualifies the transport/identity substrate M006 will require.

## 3. Non-goals

C003 does not:

- implement BEP 5/I2P KRPC routing tables, token logic, bootstrap, or announce;
- implement UDP/I2P tracker support;
- implement Datagram2/Datagram3 torrent tracker support beyond reserving the
  transport extension point;
- switch production operation to I2CP;
- implement an application-side I2P Streaming protocol;
- implement i2pr AppManager, process sandboxing, local RPC publication, or
  private-data ownership;
- restore direct loopback SAM access to the managed production profile;
- hide the C001 historical defects by rewriting its closure record.

## 4. Corrected identity/session model

The target ownership model is:

```text
TorrentI2pTransport
  |
  +-- one persistent primary control stream
  |     HELLO 3.3
  |     SESSION CREATE STYLE=PRIMARY (or explicitly qualified compatibility form)
  |     DESTINATION=<persistent keys or selected transient form>
  |     remains alive for the primary session lifetime
  |
  +-- STREAM child/subsession
  |     peer CONNECT / ACCEPT
  |     HTTP tracker streams
  |
  +-- DATAGRAM child/subsession
  |     protocol-17/repliable transport reserved for M006 DHT queries
  |
  +-- RAW child/subsession
        protocol-18 transport reserved for M006 DHT replies/errors/announce
```

All children share one I2P Destination and tunnel set.

A child stream/data channel failing must not silently mint a replacement
Destination. Loss of the primary session is a transport-wide identity/session
event and is surfaced as such.

## 5. Real local identity

Delete the semantic fiction that a hash derived from the SAM session ID is a
local peer/Destination hash.

After the primary session is established, obtain the router-confirmed local
Destination (for example through the protocol's `NAME=ME` path where
applicable), validate it, and cache:

- full local Destination bytes;
- SHA-256 Destination hash.

`I2pSession::local_peer_hash()` or its successor must return only the real
Destination hash. If the identity is not yet established, the API must return a
typed not-ready/error state rather than a placeholder value.

PEX self-filtering, local DHT node identity construction, tracker identity, and
future compact peer/node logic must consume the same canonical hash.

## 6. Primary control lifetime

The primary/control connection is first-class owned state, not a temporary
`_stream`.

Required behavior:

- primary creation succeeds exactly once per active generation;
- the control stream remains owned until explicit close, router loss, capability
  revocation, or transport replacement;
- EOF/error on the primary stream invalidates every child transport generation;
- no `STREAM CONNECT`/`STREAM ACCEPT` connection may issue an unrelated
  `SESSION CREATE`;
- restoration is an explicit bounded transition that creates one new primary
  generation and re-establishes required children;
- no reconnect storm;
- child operations from an older generation fail stale rather than attaching
  to a replacement identity accidentally.

## 7. SAM 3.3 compatibility policy

At implementation start, pin the then-current official SAM 3.3 specification
and current Java I2P plus i2pd reference revisions.

The client must have an explicit compatibility matrix for at least:

- canonical SAM 3.3 PRIMARY form;
- current Java I2P behavior;
- current i2pd behavior, including the observed PRIMARY/MASTER terminology
  difference if it still exists.

Do not send two create commands on one connection as optimistic probing.

If compatibility requires a fallback spelling/profile:

1. the first attempt must fail without creating usable session state;
2. fallback occurs on a fresh control connection;
3. the selected profile is recorded as typed negotiated state;
4. all later child operations use only that selected profile;
5. tests prove no duplicate Destination/session ownership remains after the
   failed attempt.

i2pr Plan 368 should prefer the current normative spelling while optionally
accepting a narrow compatibility alias if doing so is protocol-safe.

## 8. Child/subsession abstraction

Replace STREAM-only assumptions with a transport-neutral child vocabulary,
conceptually:

```rust
enum I2pChannelKind {
    Stream,
    RepliableDatagram,
    RawDatagram,
    // Datagram2/3 reserved for later explicit support.
}
```

Exact public types may differ.

The production torrent peer/tracker path continues to use STREAM only during
C003. DATAGRAM/RAW creation, framing, receive dispatch, bounds, and identity
sharing must be implemented sufficiently to prove that future M006 can consume
them without changing the primary identity/session owner.

No DHT logic belongs in the SAM codec.

## 9. DHT transport contract reserved by C003

M006 must be able to build the I2PSnark-compatible KRPC layer above C003 with:

- same local Destination used by peer Streaming;
- protocol 17 signed/repliable datagram send/receive;
- protocol 18 raw datagram send/receive;
- source and destination I2P ports;
- bounded payloads;
- authenticated sender Destination/hash where the selected datagram form
  supplies it;
- explicit query and response port ownership;
- cancellation and receive backpressure;
- no host UDP socket.

C003 acceptance must include a synthetic cross-transport fixture proving one
primary identity is observed identically from STREAM, DATAGRAM, and RAW child
paths.

## 10. Persistent identity policy

A useful torrent node that accepts inbound peer connections and participates in
DHT cannot rely on a new transient Destination for every restored transport.

C003 therefore separates:

- transport support for transient identities, useful for isolated tests; from
- production managed profile, which requires injected/persisted Destination key
  material owned by the M004 private-data/runtime composition.

C003 must define the injection/export-neutral API, but must not choose an
arbitrary host filesystem path.

M004 remains responsible for where the managed app persists that secret.

## 11. Current SAM client migration

Refactor rather than layer another owner beside `SamClient`.

Required migration:

- remove per-operation `SESSION CREATE`;
- split HELLO-only attachment negotiation from primary session creation;
- retain the long-lived primary control stream;
- create/attach STREAM child state through SAM 3.3 semantics;
- obtain/cache real local Destination/hash;
- add DATAGRAM/RAW child primitives;
- remove the placeholder session-ID hash;
- remove production debug prints such as the current remote-handshake
  `eprintln!`;
- keep strict parser/base64/input bounds and the C001 fingerprint removals.

Existing deterministic peer/tracker/magnet tests should continue to target a
higher-level injected session seam, but fixtures must no longer encode the
incorrect lifecycle.

## 12. Failure, restart, and cancellation semantics

Test and define:

- primary control EOF with active peer streams;
- router/capability revocation;
- STREAM child loss while primary remains healthy;
- DATAGRAM/RAW child loss while STREAM remains healthy;
- cancellation during primary creation;
- cancellation during child add/remove;
- primary replacement after failure;
- stale child completion after a new primary generation;
- persistent-key restoration preserving the same Destination hash;
- transient restoration producing a new hash and never being mistaken for the
  prior node;
- bounded shutdown that closes/removes child state before releasing the
  primary;
- malformed or unexpected child traffic without poisoning unrelated channels.

## 13. Upstream dependency

C003's production managed-app qualification is hard-blocked on i2pr Plan 368.

Plan 368 must provide a SAM 3.3 server profile over the already-existing
destination, Streaming, protocol-17 datagram, and protocol-18 raw substrates,
including private managed-app streams. C003 must not emulate missing router
subsession semantics in an application-specific gateway.

A test-only direct connector may qualify Java I2P/i2pd before Plan 368 closes.

## 14. Interoperability matrix

Before closure, record exact versions and results for:

### Java I2P

- HELLO negotiation at 3.3;
- primary creation and lifetime;
- STREAM child connect + accept;
- DATAGRAM child send/receive on same Destination;
- RAW child send/receive on same Destination;
- `NAME=ME`/equivalent real local Destination confirmation;
- control loss tears down child ownership.

### i2pd

Run the same matrix and explicitly record PRIMARY versus MASTER behavior,
SESSION ADD/REMOVE behavior, datagram framing, and any unsupported feature.

### i2pr

After Plan 368, run the same client suite through the managed-app raw SAM seam,
not only the loopback listener.

A skip is evidence of missing infrastructure, never a pass.

## 15. Ordered work packages

### WP1 — freeze research/spec matrix

Pin official SAM 3.3, current Java I2P, current i2pd, I2P BitTorrent/DHT
guidance, and the I2PSnark KRPC reference behavior. Record only independently
implemented behavior; do not copy GPL implementation code.

### WP2 — primary/session state owner

Introduce explicit primary generation/lifetime state and remove per-operation
session creation.

### WP3 — real local Destination identity

Resolve/cache the actual local Destination and hash; eliminate placeholder
hashes and update PEX/self-filtering consumers.

### WP4 — STREAM child conversion

Move tracker/peer CONNECT/ACCEPT to the shared primary/subsession model and run
two-peer live qualification.

### WP5 — DATAGRAM + RAW child substrate

Implement bounded same-Destination datagram/raw send/receive primitives with no
DHT algorithm yet.

### WP6 — reference-router qualification

Run Java I2P and i2pd matrices, including terminology/profile compatibility.

### WP7 — i2pr Plan-368 composition

Run the same matrix over i2pr private managed-app SAM streams after upstream
closure.

### WP8 — planning/docs correction

Correct forward the stale statements that transient identity is
"per-connection", C001 makes M004 ready, or the current STREAM-only client is
the final transport shape. Do not rewrite historical closure evidence.

## 16. Verification

At minimum:

```text
cargo fmt --all -- --check
cargo test --workspace --all-targets --locked
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
cargo deny check
python3 scripts/check-foundation-boundaries.py --self-test
(cd fuzz && cargo check --bins --locked)
git diff --check
```

Extend the SAM fuzz target for 3.3 primary/subsession commands and datagram
framing.

## 17. Acceptance criteria

C003 closes when:

1. one long-lived primary session owns the torrent Destination;
2. peer/tracker STREAM operations share that primary identity without issuing
   their own `SESSION CREATE`;
3. the real local Destination/hash is exposed and placeholder hashes are gone;
4. PEX self-filtering uses the real local hash;
5. bounded DATAGRAM and RAW child transports share exactly the same local
   Destination as STREAM;
6. no DHT algorithm has leaked into the SAM codec;
7. Java I2P and i2pd 3.3 behavior are explicitly qualified or residual
   incompatibilities are recorded;
8. i2pr Plan 368 is closed and the same matrix passes through the managed-app
   private SAM seam before M004 is promoted;
9. primary/child failure, cancellation, restart, and stale-generation tests
   pass;
10. production code contains no direct host SAM/UDP/TCP connector;
11. full repository verification passes.

## 18. Stop conditions

Stop and reconcile rather than improvise if:

- current Java I2P does not provide a usable PRIMARY/subsession profile for
  shared STREAM + DATAGRAM + RAW;
- i2pd compatibility requires semantics that cannot be represented without
  corrupting the normative state model;
- i2pr cannot expose protocol 17/18 children without bypassing its canonical
  destination runtime;
- DHT would require a second Destination separate from peer Streaming;
- making SAM work would require host UDP/TCP access in the managed profile;
- implementation evidence shows a reusable I2CP+Streaming SDK already exists
  and is materially simpler/safer than the SAM 3.3 path.

That last case requires a new ADR/plan. It does not authorize an in-place
torrent-specific I2CP implementation.

## 19. Closure evidence

Create `plans/closure/torrent-client/008-c003-status.md` with:

- implementation SHAs;
- pinned SAM/I2P reference revisions;
- primary/child ownership diagram;
- real local Destination/hash evidence;
- Java I2P / i2pd / i2pr interoperability matrix;
- PRIMARY/MASTER compatibility disposition;
- STREAM/DATAGRAM/RAW same-Destination evidence;
- lifecycle/restart/cancellation matrix;
- PEX self-filter regression evidence;
- fuzz/boundary/full verification results;
- residual findings with severity;
- explicit M004 and M006 readiness decisions.
