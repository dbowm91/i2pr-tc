# Torrent Client M002 C001 — SAM Client Boundary, Runtime I/O, and Live-Transport Hardening

Status: ready for handoff

Historical source milestone:

- `plans/implementation/torrent-client/002-i2p-streaming-trackers-and-pex.md`
- `plans/closure/torrent-client/002-status.md` (conditionally closed)

Canonical authority:

- `plans/000-long-term-specification.md`
- `plans/001-terminology-and-domain-model.md`
- ADR 0001
- `docs/integration/i2pr-managed-app-requirements.md`

Primary class: corrective invariant + transport capability hardening

## 0. Why this is a corrective

M002 correctly kept host networking out of the production I2P crate and proved
tracker/peer behavior through an injected `I2pSession`. Post-closure review
found that the abstraction stopped one layer too high for the now-concrete i2pr
managed-app contract.

Current i2pr main has closed managed-app Plans 354 and 355. The router gateway
maps each authorized `sam` logical stream to one private SAM protocol
connection and passes exact ordered SAM octets. It deliberately does not
translate SAM into torrent-specific `lookup/connect/accept` operations.

The current production-facing gap is therefore:

```text
managed app channel
  -> open(service=sam)
  -> raw SAM protocol stream(s)
  -> [missing application-side SAM v3 client]
  -> I2pSession lookup/connect/accept
  -> tracker/peer code
```

The same review found two additional issues that should be corrected before
M004:

1. tracker announces disclose `User-Agent: i2pr-tc/0.1`, adding avoidable
   implementation/version fingerprint entropy;
2. `TorrentRuntime::receive_block_inner` can hold the global torrent-state
   mutex across synchronous filesystem verification/write work, which can
   serialize unrelated torrents and block an async peer task's Tokio worker.

M002 also still lacks live-router, complete magnet, inbound-transfer, and
router-disconnect/reconnect evidence. This corrective closes those gaps without
rewriting the historical M002 closure.

## 1. Objective

Provide and qualify the real application-side SAM-v3 ownership boundary needed
by i2pr's raw managed-app SAM streams, remove avoidable tracker fingerprinting,
and separate torrent-state synchronization from blocking storage work.

After C001, the I2P transport should be suitable for M004 composition without
requiring i2pr to grow torrent-specific networking APIs.

## 2. Non-goals

C001 does not:

- implement AppManager/process launch, package lifecycle, or OS sandboxing;
- bind a production host SAM listener/socket;
- add clearnet fallback;
- add DHT, Datagram2/Datagram3, UDP trackers, or SAM PRIMARY/subsessions;
- implement router update transport;
- broaden Transmission RPC;
- make live external-router evidence a substitute for managed-app sandbox
  qualification owned by M004.

## 3. Upstream contract pinned for this corrective

Re-check i2pr main at implementation start. The reviewed contract currently
requires:

- one successful managed-app `open(service=sam)` -> one raw SAM protocol
  connection;
- exact SAM bytes, in order, with no gateway protocol rewriting;
- isolated SAM service state per app-principal gateway session;
- no host socket/loopback dependency;
- managed SAM `STREAM FORWARD` unavailable;
- future trusted runtime owns app-channel logical-stream multiplexing.

If upstream changes these semantics, stop and reconcile this plan before
implementation.

## 4. Raw-stream factory and SAM ownership

Do not move `lookup/connect/accept` semantics into i2pr.

Add an application-side abstraction whose authority is only to request a fresh
raw SAM protocol connection, conceptually:

```rust
trait SamConnectionFactory {
    async fn open_sam_connection(&self) -> Result<RawSamStream, ...>;
}
```

Exact names may differ.

Production M004 will implement this factory over managed-app
`open(service=sam)` logical streams. A test/interoperability adapter may
connect to a configured local SAM endpoint, but it must be test/dev-only and
must not become the secured managed profile.

Build a SAM-v3 client/session owner over this factory and implement the existing
high-level `I2pSession` trait from it. This preserves current tracker/peer
tests while putting the actual protocol owner on the application side.

## 5. SAM-v3 minimum surface

Implement only the SAM surface M002 requires:

- `HELLO VERSION` negotiation with an explicit supported version range;
- STREAM session creation/reuse under one bounded application session ID;
- naming lookup/resolution needed by I2P tracker/peer destinations;
- `STREAM CONNECT` on a fresh raw SAM connection;
- `STREAM ACCEPT` on a fresh raw SAM connection;
- exact protocol reply parsing with bounded line/token/value lengths;
- typed result/error mapping;
- EOF, cancellation, timeout, and reconnect-safe cleanup.

The implementation must account for SAM's multi-connection model: one logical
torrent I2P identity/session may require multiple raw SAM protocol connections
sharing the same SAM session identifier inside the app's isolated router
context. Do not assume one raw stream carries all torrent peer streams.

Do not implement `STREAM FORWARD`.

Destination/private-key material handling must preserve the product's identity
boundary. No router identity or router administrator credential may enter this
crate.

## 6. Session lifetime and reconnect semantics

Define one clear owner for the application torrent Destination/SAM session.

Required behavior:

- normal tracker/peer connection loss does not destroy the torrent identity;
- failure of the underlying SAM session marks the transport unavailable and
  fails/tears down dependent stream operations;
- restoration creates or reattaches only according to valid SAM semantics;
- reconnect attempts are bounded and cancellable;
- no reconnect storm after router unavailability;
- torrent desired running/stopped state remains storage/service authority;
- M004 may later decide persistence of destination key material, but C001 must
  make the key/session injection boundary explicit.

If current Java I2P/i2pd SAM behavior differs materially, record the supported
intersection instead of adding router-specific hidden fallbacks.

## 7. Tracker HTTP fingerprint policy

Remove the versioned `i2pr-tc/0.1` tracker User-Agent.

Research/qualify the smallest interoperable policy in this order:

1. omit `User-Agent` if current I2P trackers accept the request;
2. otherwise use one stable, non-versioned compatibility value justified by
   interoperability evidence.

Do not dynamically expose crate/package/router versions or per-host details.

Add a golden HTTP request test so a future version bump cannot silently change
the tracker fingerprint.

This corrective does not claim that HTTP header normalization eliminates all
BitTorrent implementation fingerprinting; it only removes unnecessary
version-specific entropy.

## 8. Runtime synchronization and blocking storage

No filesystem read/write/hash/recheck path may execute while holding the global
torrent-state map lock.

Refactor state ownership so unrelated torrents do not serialize behind one
torrent's disk operation. Preferred shape:

- global catalog/map lock only locates an `Arc`/owned per-torrent state;
- each torrent has its own bounded state owner/lock;
- piece assembly transitions to an explicit persistence-in-progress
  reservation/generation;
- the assembled piece buffer is moved out of the state owner;
- storage hash/write occurs after releasing torrent-state locks;
- completion/failure reacquires the torrent state and commits only if the
  reservation/generation is still authoritative.

Exact synchronization primitives may differ, but one lock may not span
blocking filesystem I/O.

On async peer paths, synchronous filesystem work must not run directly on a
Tokio worker. Use a bounded blocking executor/storage worker or an equivalent
design with explicit backpressure. Do not hold an async mutex guard across
`await`.

## 9. Race and cancellation semantics

The refactor must define/test:

- peer disconnect after piece assembly but before persistence;
- choke while persistence is in flight;
- torrent stop/remove while persistence is in flight;
- duplicate block arrival during persistence;
- cancellation before storage begins;
- cancellation during storage work;
- storage/hash failure;
- successful storage after a stale generation/removed torrent;
- two torrents persisting concurrently without global head-of-line blocking.

A completed write may update service progress only if its reservation remains
valid. A stale completion must not resurrect removed/stopped state or double
count verified bytes.

## 10. Live interoperability qualification

Add an explicit interoperability harness separate from the production managed
profile.

Minimum desired evidence:

- current Java I2P SAM: HELLO, session create, naming lookup, STREAM CONNECT,
  STREAM ACCEPT;
- a real I2P HTTP tracker announce with compact 32-byte peer records where a
  suitable test tracker is available;
- outbound piece transfer against a real peer/client fixture;
- inbound peer transfer;
- complete magnet `ut_metadata` acquisition and promotion;
- router/SAM interruption followed by bounded restoration.

Also exercise an alternate SAM implementation such as current i2pd for the
subset it actually supports. Do not fail the architecture merely because an
alternate router lacks a newer SAM feature; record exact version/capability
limits.

External-network evidence may be operationally environment-dependent, but the
local harness and protocol transcript tests are required. If a particular live
case cannot be run, closure must state it explicitly and M004 may not claim
that case as qualified.

## 11. Static ownership guards

Extend the boundary guard to prove:

- production `i2pr-tc-i2p` still owns no host `TcpStream`/`TcpListener`
  connector;
- any direct local-SAM connector exists only under a named test/dev target or
  feature excluded from the managed production profile;
- no i2pr daemon/private gateway type becomes a dependency of core torrent
  protocol crates;
- tracker requests contain no semantic version/User-Agent regression;
- storage APIs are not called from known runtime critical sections where a
  torrent/global state guard is held.

Use static checks plus runtime concurrency tests; do not rely on comments.

## 12. Ordered work packages

### WP1 — SAM raw-stream contract

Add the bounded connection-factory contract, protocol parser/encoder, SAM
session owner, and fake raw-stream transcript tests.

### WP2 — I2pSession production adapter

Implement `I2pSession` over the SAM client, preserving current tracker/peer
callers. Add CONNECT/ACCEPT/lookup/session reuse/error fixtures.

### WP3 — Tracker fingerprint corrective

Remove versioned User-Agent behavior, qualify omission or a fixed compatibility
value, and freeze the request shape in tests.

### WP4 — Per-torrent state and persistence reservation

Eliminate global-state lock ownership across storage work and introduce
generation/reservation semantics for in-flight persistence.

### WP5 — Async storage execution

Move blocking piece persistence/recheck work off Tokio worker paths with bounded
backpressure and cancellation. Add multi-torrent concurrency tests.

### WP6 — Live/interop qualification

Run the explicit Java I2P and available alternate-router matrix, complete
magnet/inbound transfer cases, and interruption/recovery scenarios.

### WP7 — Closure and downstream audit

Write C001 closure evidence, update M002 current status, and re-evaluate M004
against current i2pr managed-runtime state.

## 13. Required verification

At minimum, record current equivalents of:

```text
cargo fmt --all -- --check
cargo test --workspace --all-targets --locked
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
cargo deny check
python3 scripts/check-foundation-boundaries.py --self-test
git diff --check
```

Retain the existing fuzz floor and add SAM parser fuzz/property coverage if the
parser accepts hostile application-visible protocol bytes.

## 14. Acceptance criteria

C001 closes when:

1. a real application-side SAM-v3 implementation exists over raw connection
   streams and implements the existing `I2pSession` contract;
2. production composition no longer depends on an unexplained typed
   lookup/connect/accept provider from i2pr;
3. the SAM client supports the M002-required lookup/CONNECT/ACCEPT/session
   lifecycle with strict bounds;
4. tracker announces no longer expose an i2pr-tc semantic version;
5. no filesystem I/O occurs while holding global torrent-state ownership;
6. async peer operation does not perform unbounded/blocking filesystem work on
   a Tokio worker;
7. persistence completion is race-safe against cancellation, stop/remove, and
   stale generations;
8. multi-torrent tests demonstrate no global disk-induced state-lock
   head-of-line blocking;
9. full magnet and inbound-transfer deterministic tests exist;
10. live Java I2P qualification is recorded where the environment permits, and
    any unavailable evidence remains explicit;
11. network-boundary/static guards and full verification pass.

## 15. Stop conditions

Stop and reconcile if:

- i2pr changes the managed-app SAM service from exact raw protocol streams to a
  different contract;
- correct SAM operation requires moving torrent-specific semantics into the
  router gateway;
- session identity persistence requires an unscoped host filesystem secret;
- lock-free/storage refactoring would permit two authorities to commit the same
  piece;
- live interoperability requires clearnet fallback or direct networking in the
  production managed profile.

## 16. Closure evidence

Create
`plans/closure/torrent-client/006-m002-c001-status.md` containing:

- implementation SHAs;
- exact i2pr upstream contract SHA recheck;
- raw-SAM factory/client ownership diagram;
- SAM version/session/CONNECT/ACCEPT/lookup matrix;
- tracker HTTP fingerprint before/after fixture;
- state-lock/storage ownership diagram;
- persistence race/cancellation matrix;
- multi-torrent concurrency evidence;
- magnet/inbound/live-router interoperability matrix;
- static guard/fuzz/full verification results;
- residual findings with severity;
- explicit M004 unblock/continue-block decision.
