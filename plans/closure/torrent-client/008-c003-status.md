# Torrent Client C003 — Closure Record

Status: **conditionally closed**

Date: 2026-10-07

Plan: `plans/implementation/torrent-client/008-c003-sam33-primary-dht-transport-corrective.md`

Implementation commit: `a94a7e8` — the transport, its tests, the fuzz target,
and this closure record. The follow-up commits on the branch are documentation
only.

Closure recommendation: **conditionally closed.** Criteria 1–7 and 9 are met
with evidence below. Criterion 8 is *not* met and cannot be met from this
repository: it requires upstream i2pr Plan 368 to close, and Plan 368 is
registered upstream but not implemented. M004 therefore stays blocked, and M006
stays deferred behind the same single gate. The transport C003 was asked to
build is built, qualified against a real router, and is the substrate both of
those milestones depend on.

---

## 1. What changed

The STREAM-only, per-operation session model is gone. `SamClient` now owns one
long-lived shared-Destination session and its child channels:

```
TorrentI2pTransport
  |
  +-- one persistent primary control connection          (SamClient supervisor)
  |     HELLO VERSION MIN=3.1 MAX=3.3
  |     SESSION CREATE STYLE=PRIMARY ID=<id> DESTINATION=<keys|TRANSIENT>
  |     SESSION ADD / SESSION REMOVE run only here
  |
  +-- STREAM child   -> peer CONNECT/ACCEPT, tracker HTTP   (protocol 6)
  +-- DATAGRAM child -> repliable transport, reserved for DHT (protocol 17)
  +-- RAW child      -> raw transport, reserved for DHT       (protocol 18)
```

- `SamClient::establish_primary` is the single explicit restoration transition.
  It runs at most once per generation, sends one `SESSION CREATE` per
  *connection*, and never retries on its own.
- `stream_connect`, `stream_accept`, `datagram_send/receive`, `raw_send/receive`
  and `nam_lookup` open a fresh raw connection, issue `HELLO` and their own
  command, and never issue `SESSION CREATE`.
- A supervisor task owns the control connection. Its loss, an I/O failure, or an
  unparseable reply clears the primary, which makes every child of that
  generation stale (`TransportError::Stale`).
- The control connection applies explicit backpressure: a saturated command
  queue reports rather than buffering without limit.
- `SamIdentity` no longer carries a hash. `I2pSession::local_peer_hash()` now
  returns `Result<[u8; 32], TransportError>` and yields
  `TransportError::IdentityNotReady` until the router has confirmed a
  Destination. The placeholder hash is deleted, not deprecated.
- PEX self-filtering and the peer self-connection paths consume the real hash or
  fail; the `eprintln!("DBG got remote handshake")` debug print is removed.

## 2. Pinned reference revisions

| Reference | Pinned revision | How it was obtained |
|---|---|---|
| SAM v3 specification | `i2p.net/en/docs/api/samv3/`, page marked "Updated: 2026-07, Accurate for: 0.9.70" | fetched during WP1 |
| I2P common structures (Destination layout) | `i2p.net/en/docs/specs/common-structures/` | fetched during WP6 correction |
| i2pd under test | 2.61.0 (0.9.70), `/usr/local/bin/i2pd`, bridge at `127.0.0.1:7656` | live |
| i2pd source consulted | tag `2.61.0`, `libi2pd_client/SAM.cpp`, `libi2pd/Identity.h`, `libi2pd/Identity.cpp`, `libi2pd/Base.cpp` | read during WP1/WP6 |
| i2pr upstream | `origin/main` = `579725eec85f8a38233a06f86c23cf6efab3f21b`, Plan 368 registered `ready`, not implemented | fetched during WP7 |
| Java I2P | **not available in this environment** (no Java I2P installation; only an OpenJDK runtime) | see §5 |

## 3. PRIMARY/MASTER compatibility disposition

The current SAM 3.3 specification and Java I2P spell the shared-Destination
session `STYLE=PRIMARY`. i2pd 2.61.0 answers `STYLE=PRIMARY` with
`SESSION STATUS RESULT=I2P_ERROR MESSAGE="Unknown STYLE"` and requires the older
`STYLE=MASTER`.

Disposition: both spellings are first-class. `SamPrimaryStyle::OFFER_ORDER` is
`[Primary, Master]`; the normative spelling is offered first **on its own fresh
connection**, and only an explicit rejection triggers the second offer. A
client never sends two create commands on one connection, and the accepted
spelling is recorded as negotiated state (`SamNegotiated::primary_style`).
This is a spelling difference over one protocol model, not two models, so no
emulation and no application-specific gateway is involved.

Live evidence (i2pd 2.61.0, this repository's client):

```
live shared-Destination profile: version=3.3; accepted style=Master; raw connections opened=2
```

Two connections, one attempt each: `PRIMARY` rejected on the first,
`MASTER` accepted on the second.

## 4. STREAM/DATAGRAM/RAW same-Destination evidence

`crates/i2pr-tc-i2p/tests/sam33_shared_destination.rs` is an in-memory SAM 3.3
service written against the specification, not against this client's encoder. It
enforces the shared-Destination rules (children attached only on the control
connection, children inheriting the primary's Destination, children dropped
when the primary goes away). The test `sam33_one_destination_is_shared_by_stream_datagram_and_raw`
asserts:

- one primary, three child channels, one generation, one identity;
- a real byte round trip on the STREAM child;
- a bounded DATAGRAM send/receive whose authenticated sender hash equals the
  client's own local Destination hash;
- a bounded RAW send/receive on protocol 18;
- the local identity hash unchanged across all three transports;
- exactly one `SESSION CREATE` for the whole session, with sibling channels
  still usable after one is removed.

Two further cases pin the lifecycle: `sam33_primary_loss_invalidates_every_child_transport`
and `sam33_unavailable_client_does_not_reconnect_by_itself`.

## 5. Interoperability matrix

| Row | Java I2P | i2pd 2.61.0 (live) | i2pr managed-app |
|---|---|---|---|
| `HELLO VERSION MIN=3.1 MAX=3.3` | not exercised | **PASS** `HELLO REPLY RESULT=OK VERSION=3.3` | blocked on Plan 368 |
| shared-Destination create, normative spelling | not exercised | **rejected**: `I2P_ERROR MESSAGE="Unknown STYLE"` | blocked |
| shared-Destination create, legacy spelling | not exercised | **PASS**, returns `RESULT=OK DESTINATION=<663 bytes>` | blocked |
| local identity from `NAMING LOOKUP NAME=ME` | not exercised | **PASS** 387-byte Destination, SHA-256 hash | blocked |
| `SESSION ADD STYLE=STREAM` + `SESSION REMOVE` | not exercised | **PASS** | blocked |
| `SESSION ADD STYLE=DATAGRAM` | not exercised | **FAIL** `RESULT=I2P_ERROR` | blocked |
| `SESSION ADD STYLE=RAW` | not exercised | **FAIL** connection ended without a reply | blocked |
| `STREAM CONNECT`/`ACCEPT` | not exercised | not executed (needs a second peer Destination) | blocked |
| `DATAGRAM SEND`/`RECEIVE`, `RAW DATA SEND`/`RECEIVE` | not exercised | not executed (needs a router that accepts datagram children) | blocked |

A skip is evidence of missing infrastructure, never a pass. Two rows were not
executable in this environment and are recorded as such:

- **Java I2P is not installed here.** Only an OpenJDK runtime exists. Every Java
  I2P row is unexercised; the normative `PRIMARY` spelling, `DESTINATION=`
  handling, and Java's datagram-child behaviour are qualified from the
  specification text and i2pd's source, not from a Java router.
- **No second peer and no datagram-capable router.** `STREAM CONNECT` needs
  `I2PR_TC_LIVE_DESTINATION`; datagram children need a router that accepts
  non-stream subsession styles. The i2pd under test rejects both by construction.

### Residual incompatibilities

| Severity | Finding |
|---|---|
| medium | i2pd 2.61.0 accepts only `STYLE=MASTER` for the shared-Destination session. Handled by a negotiated one-per-connection fallback; no workaround is needed elsewhere. |
| medium | i2pd 2.61.0 answers `SESSION ADD` with `STYLE=DATAGRAM`/`STYLE=RAW` with `I2P_ERROR`, and terminates the control connection after the RAW attempt. The client survives this exactly as the model requires — the primary dies, its children become stale, and restoration is explicit — but a production deployment on i2pd cannot use the datagram transports until the router implements them. |
| low | Both routers' datagram models (`sam.udp.host`/`sam.udp.port`) imply a host UDP socket. This client never requests one: SAM 3.2+ frames datagrams on the bridge socket itself (`DATAGRAM SEND`/`RAW DATA SEND`), which is what keeps the managed profile free of host UDP authority. |
| low | Java I2P rows are unexercised; see §5. |

## 6. Real local Destination/hash evidence

- The identity is the router's answer, taken from `NAMING LOOKUP NAME=ME` or
  from a `SESSION CREATE` reply that carries a structurally valid Destination.
- Live: `live local identity ok: 387-byte Destination,
  hash=347a2e968d40f5dee0866221ad791ca20a770cb8ad1885b92acb5ce92416085f`.
- A transient `SESSION CREATE` reply carries 663 bytes — the 387-byte
  Destination followed by private key material. A structural check rejects it,
  so a key blob can never be cached as the local identity; the client falls back
  to `NAME=ME`. Covered by
  `sam_local_identity_falls_back_to_naming_me`.

### A defect this milestone found and fixed

The first structural check read a Destination as certificate-first. The real
layout is **keys first**: 256 bytes of public key, then 128 bytes of signing
key, then a certificate of at least three bytes whose two length bytes must
account for the value's length exactly (confirmed in the common-structures
specification, in i2pd's `struct Identity`, and against live i2pd bytes: the
387-byte reply ends `00 00 00`, a null certificate). The certificate-first
reading rejected every real i2pd Destination while passing synthetic fixtures —
it only failed once run against the router. `decode_destination` now implements
the real layout, and `sam_accepts_the_keys_first_destination_layout_a_real_router_sends`
plus the 663-byte key-blob case pin it.

## 7. Lifecycle/restart/cancellation matrix

| Property | Evidence |
|---|---|
| one create per generation | `sam_primary_is_created_once_per_generation_on_one_connection` |
| one create per connection on fallback | `sam_primary_falls_back_to_the_legacy_style_on_its_own_connection` |
| child ops never create a session | `sam_stream_connect_and_accept_attach_without_creating_a_session`, `sam_datagram_and_raw_children_frame_on_the_bridge_socket`, transcript assertions in the 3.3 fixture |
| attach and detach on the control connection | `sam_child_channels_are_attached_on_the_control_connection` |
| primary loss invalidates all children | `sam_primary_loss_makes_every_child_of_that_generation_stale` |
| stale child cannot attach to a replacement identity | `sam_stale_child_cannot_attach_to_a_replacement_identity` |
| no reconnect storm | `sam33_unavailable_client_does_not_reconnect_by_itself`, `sam_reply_timeout_and_eof_are_bounded_failures` |
| cancellation | `sam_pending_operations_are_cancellable`, `sam_close_cancels_in_flight_work_and_fails_dependent_operations` |
| backpressure | `sam_control_queue_applies_explicit_backpressure` |
| bounded payloads | `sam_datagram_payload_bounds_are_enforced_without_io` |

## 8. PEX self-filter regression evidence

`I2pSession::local_peer_hash()` returns `Result<[u8; 32], TransportError>`;
`connect_peer` and `serve_incoming` propagate `?`. The self-filter path in
`peer.rs` therefore receives the SHA-256 of the router-confirmed Destination, or
fails, and can no longer filter itself with a hash derived from a session
identifier. Covered by the existing `pex::tests::pex_uses_only_bounded_hashes_and_source_deduplication`
and the peer suite, all of which pass unchanged.

## 9. Verification results

```
cargo fmt --all -- --check                                    OK
cargo test --workspace --all-targets --locked                129 passed, 0 failed
cargo check --workspace --all-targets --locked               OK
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings   clean
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked             clean
cargo deny check                                              advisories/bans/licenses/sources ok
python3 scripts/check-foundation-boundaries.py --self-test     passed
(cd fuzz && cargo check --bins --locked)                      OK
git diff --check                                              clean
```

Test breakdown: 46 SAM unit tests (scripted-service transcripts), 3 shared-
Destination 3.3 fixture tests, 8 live-interoperability tests, 27 core, 38
storage/transmission, and the remaining targets. `cargo deny` emits the same
pre-existing `license-not-encountered` and duplicate-`syn` warnings before and
after this change; no new advisories were introduced.

The SAM fuzz target was extended for the 3.3 surface: `DATAGRAM RECEIVED` and
`RAW DATA SEND`/`RAW DATA RECEIVED` reply forms with no `RESULT`, the delivery
vs. status distinction, range-checked numeric options, and full Destination
verification round-trips under two limit profiles.

## 10. Criterion-by-criterion

| # | Criterion | Result |
|---|---|---|
| 1 | one long-lived primary session owns the torrent Destination | **met** — §1, §4 |
| 2 | STREAM operations share that identity without their own `SESSION CREATE` | **met** — §4, §7 |
| 3 | real local Destination/hash exposed, placeholder hashes gone | **met** — §6 |
| 4 | PEX self-filtering uses the real local hash | **met** — §8 |
| 5 | bounded DATAGRAM and RAW share exactly the same local Destination | **met** — §4 (in-memory SAM 3.3 service) |
| 6 | no DHT algorithm leaked into the SAM codec | **met** — the codec has no KRPC/DHT vocabulary; only SAM commands |
| 7 | Java I2P and i2pd 3.3 behaviour qualified or residual incompatibilities recorded | **partially met** — i2pd qualified live with residuals recorded (§5); Java I2P unavailable in this environment and recorded as an unexercised row |
| 8 | i2pr Plan 368 closed and the same matrix passing through the managed-app private SAM seam before M004 is promoted | **not met** — Plan 368 is registered upstream and `ready`, not implemented. Outside this repository's control. M004 stays blocked. |
| 9 | primary/child failure, cancellation, restart, stale-generation tests pass | **met** — §7 |

## 11. M004 and M006 readiness decisions

**M004 — stays blocked.** The blocker that changed is gone: M004 no longer has
to compose a STREAM-only SAM 3.1 client, because there is now a correct
SAM 3.3 shared-Destination transport to compose. What remains is unchanged and
was re-confirmed against upstream `main` at `579725ee`:

1. upstream i2pr Plan 368 must close so the private SAM seam can carry a
   PRIMARY/subsession profile;
2. AppManager/package/process lifecycle and process authentication are still
   not registered/implemented upstream;
3. OS sandbox/resource containment remains future work;
4. host-owned local RPC ingress and private persistent-data/key semantics are
   still not available to this consumer.

M004 additionally now owns one decision this milestone deliberately left open:
where injected destination key material is persisted. C003 provides the
injection and observation seams (`SamSessionDestination::PrivateKeys`,
`SamLocalIdentity`) and chooses no host path.

**M006 — stays deferred, now with a single named gate.** The handoff condition
"a qualified same-Destination STREAM + DATAGRAM + RAW contract" is satisfied in
this repository against a SAM 3.3 service and against a real router's
STREAM child. The only remaining gate is a router that actually serves
protocol 17/18 children — upstream Plan 368 today, or an i2pd release later.
Authoring the M006 plan can start against the shipped transport API; live
qualification of a DHT cannot.

## 12. What this record does not claim

- It does not claim Java I2P interoperability. No Java router was available.
- It does not claim live peer-to-peer traffic. No second peer Destination was
  configured, and the i2pd under test has no working transit, so no LeaseSet is
  published and no peer is reachable.
- It does not claim live datagram traffic. The router under test rejects
  datagram children.
- It does not claim upstream Plan 368 progress. Nothing in this repository
  implements or substitutes for it.