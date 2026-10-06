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
build is built, qualified against two real routers — i2pd 2.61.0 and Java I2P
2.13.0 — and is the substrate both of those milestones depend on.

A corrective pass after the first closure installed Java I2P and re-ran the
matrix; it discharged criterion 7 completely and fixed three defects. See §5a
and §13. It did not change the disposition, because the only open criterion is
external.

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
| Java I2P | 2.13.0, installed at `/usr/local/Cellar/i2p/2.13.0`, SAM bridge on `127.0.0.1:17656` | live, second pass — see §5a |
| Java SAM bridge source | `libexec/lib/sam.jar`, `net/i2p/sam/{SAMv1Handler,PrimarySession}.class` | read in the second pass for reply spellings and the datagram `PORT` rule |

**Reading note.** This record was written in two passes. §2–§4 and the first
half of §5 describe the state at the implementation commit `a94a7e8`, when
Java I2P was not installed in this environment. §5a and everything that cites it
are a later corrective pass that installed Java I2P and qualified it. The
earlier "not exercised" cells are left standing as written and are superseded,
not rewritten.

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
  **(Superseded by §5a — Java I2P 2.13.0 was installed in a later pass and
  qualified live. This statement stood at `a94a7e8`.)**
- **No second peer and no datagram-capable router.** `STREAM CONNECT` needs
  `I2PR_TC_LIVE_DESTINATION`; datagram children need a router that accepts
  non-stream subsession styles. The i2pd under test rejects both by construction.
  **(The datagram-child half of this is superseded by §5a: Java I2P does accept
  them. The no-second-peer half still stands — see §5a.1.)**

### Residual incompatibilities

| Severity | Finding |
|---|---|
| medium | i2pd 2.61.0 accepts only `STYLE=MASTER` for the shared-Destination session. Handled by a negotiated one-per-connection fallback; no workaround is needed elsewhere. The Java row in §5a shows the normative `PRIMARY` spelling is accepted unchanged, so the fallback is a compatibility path, not the primary path. |
| medium | i2pd 2.61.0 answers `SESSION ADD` with `STYLE=DATAGRAM`/`STYLE=RAW` with `I2P_ERROR`, and terminates the control connection after the RAW attempt. The client survives this exactly as the model requires — the primary dies, its children become stale, and restoration is explicit — but a production deployment on i2pd cannot use the datagram transports until the router implements them. |
| low | Both routers' datagram models (`sam.udp.host`/`sam.udp.port`) imply a host UDP socket. This client never requests one: SAM 3.2+ frames datagrams on the bridge socket itself (`DATAGRAM SEND`/`RAW DATA SEND`), which is what keeps the managed profile free of host UDP authority. |
| low | Java I2P rows are unexercised; see §5. Superseded by §5a, which qualifies Java I2P live. |

## 5a. Java I2P qualification (second pass)

Java I2P 2.13.0 was installed in this environment and qualified live through the
same harness that produced the i2pd rows above. The SAM bridge runs on
`127.0.0.1:17656`, started via `runplain.sh` with
`~/Library/Application Support/i2p/clients.config.d/01-net.i2p.sam.SAMBridge-clients.config`
set to `startOnLoad=true`, `delay=5`, `args=sam.keys 127.0.0.1 17656 i2cp.tcp.port=7654`.

All eight tests in `crates/i2pr-tc-i2p/tests/sam_live_qualification.rs` ran
against it with no skip of the shared-Destination profile:

| Row | Java I2P 2.13.0 (live) |
|---|---|
| `HELLO VERSION MIN=3.1 MAX=3.3` | **PASS** `HELLO REPLY RESULT=OK VERSION=3.3` |
| shared-Destination create, normative spelling | **PASS** `STYLE=PRIMARY` accepted on the **first** connection |
| `DESTINATION=` handling | **PASS** transient Destination returned and accepted |
| local identity from `NAMING LOOKUP NAME=ME` | **PASS** 387-byte Destination, ends `00 00 00`, SHA-256 hash |
| `SESSION ADD STYLE=STREAM` + `SESSION REMOVE` | **PASS** protocol 6 |
| `SESSION ADD STYLE=DATAGRAM` + `SESSION REMOVE` | **PASS** protocol 17 (after the `PORT` correction below) |
| `SESSION ADD STYLE=RAW` + `SESSION REMOVE` | **PASS** protocol 18 (after the `PORT` correction below) |
| `STREAM CONNECT`/`ACCEPT` with real traffic | not executed — needs a reachable peer Destination (§5a.1) |
| `DATAGRAM SEND`/`RECEIVE`, `RAW DATA SEND`/`RECEIVE` | not executed — needs a router with transit (§5a.1) |

Verbatim harness output:

```
live HELLO + shared-Destination SESSION CREATE ok; version=3.3; style=Primary; DESTINATION=Transient; raw connections opened=1
live shared-Destination profile: version=3.3; accepted style=Primary; raw connections opened=1
live SESSION ADD ok for Stream: id=…-stream-1 protocol=6
live SESSION ADD ok for RepliableDatagram: id=…-datagram-1 protocol=17
live SESSION ADD ok for RawDatagram: id=…-raw-1 protocol=18
live local identity ok: 387-byte Destination, hash=872c75a11eef301f4635d3222888fb18d5257304e82b9459e15ba6865cea832a
```

`raw connections opened=1` is the decisive result: against Java I2P the
normative `PRIMARY` spelling is accepted on the connection that offers it, so the
`PRIMARY` → `MASTER` fallback in §3 is never taken. The fallback exists for
i2pd, not because the protocol requires it.

### A second defect this milestone found and fixed

**Java I2P refuses to attach a datagram child that names no `PORT`.** Both
`SESSION ADD STYLE=DATAGRAM` and `SESSION ADD STYLE=RAW` were answered
`I2P_ERROR MESSAGE="DATAGRAM subsession must specify PORT"`, and
`"RAW subsession must specify PORT"` respectively. The strings are in Java's
own shipped bridge code (`net/i2p/sam/PrimarySession.class` in
`libexec/lib/sam.jar`), so this is a rule a conforming router enforces, not a
quirk of one version.

`encode_session_add` now emits `PORT=<from_port>` on datagram and RAW children,
and still emits no port options at all on a STREAM child. What this does **not**
do is acquire host UDP authority:

- `HOST`, `sam.udp.host` and `sam.udp.port` are never emitted, on any channel,
  and `sam_commands_are_exact_octets_for_the_shared_destination_profile` asserts
  their absence in the exact octets.
- `PORT` names the datagram server a *host-side* bridge would forward to. This
  client does not run a host datagram server: inbound datagrams are read on the
  child's own SAM connection with `DATAGRAM RECEIVE`/`RAW DATA RECEIVE`, which is
  the SAM 3.2+ framing. The value is therefore inert here.
- The managed-profile invariant of Plan 368 criterion 10 (no host UDP) is
  preserved. The parameter is a forwarding target, not a local bind.

The same rule was then mirrored in the in-memory SAM 3.3 service in
`sam33_shared_destination.rs`, which previously accepted a datagram child
without `PORT`. A fixture that accepts commands a real router refuses would have
let this defect survive the cross-transport test that exists to catch exactly
this class of bug.

### A third correction: the delivery-reply spelling

Java's bridge emits the two-word delivery form `RAW RECEIVED SIZE=<n>`, not
`RAW DATA RECEIVED` (both appear in `net/i2p/sam/SAMv1Handler.class`, alongside
`DATAGRAM RECEIVED DESTINATION=` and `RAW SEND `). The reply parser accepts
`RAW SEND`/`RAW RECEIVE`/`RAW RECEIVED` and `RAW DATA SEND`/`RAW DATA RECEIVE`/
`RAW DATA RECEIVED` as one arm rather than guessing which spelling a given router
emits. Covered by
`sam_reply_parsing_accepts_every_status_line_shape`.

### 5a.1 What Java I2P still could not exercise

Java I2P ran with **no network database**. Its reseed failed
(`EepGet failed on https://reseed-pl.i2pd.xyz/i2pseeds.su3?netid=2 :
java.net.SocketTimeoutException: Connect timed out`), and the alternate source
does not resolve from this host. Independently confirmed from the shell:

```
curl -m 20 https://reseed-pl.i2pd.xyz/i2pseeds.su3?netid=2   -> connection timed out after 20006 ms
curl -m 20 https://reseed.i2p-propagation.eu/netDb/netDb-2.su3 -> Could not resolve host
```

With no netDb there are no tunnels, so no LeaseSet is published and no peer is
reachable. The same is true of the i2pd under test, which builds SSU2 and 777
tunnels but passes **0** tunnel tests and never publishes a LeaseSet (clock
checked and accurate, so this is not a clock-skew artifact).

`crates/i2pr-tc-i2p/tests/sam_live_two_peer.rs` is the harness that would close
this row: it runs this client on two routers, gives each one shared-Destination
primary session, and requires one to `STREAM CONNECT` to the other's
`NAME=ME` Destination and echo bytes. It is committed, compiles, and skips with
the reason printed:

```
live two-peer SKIPPED: set I2PR_TC_LIVE_PEER_SAM_ADDR to a second SAM bridge for the peer's router, and I2PR_TC_SAM_ADDR for the dialer's router. Pointing them at two different routers (i2pd and Java I2P) also qualifies cross-router interoperability.
```

It is a skip, and it is recorded as a skip. Executing it needs a router position
with working transit — a VPS or a clean UDP path — not more code.

### 5a.2 A guard defect found while recording this evidence

`scripts/check-foundation-boundaries.py` printed a hard-coded list of declared
test-only connectors instead of the list it had actually collected, so adding a
second declared connector (`sam_live_two_peer.rs`) did not appear in the output,
and a pre-existing third one (`crates/i2pr-tc-transmission/tests/transmission_remote.rs`)
had been concealed since it was written. `check()` now returns the collected
connectors and `main()` prints the real set; the self-test asserts that a declared
connector is reported, not merely tolerated. A guard whose evidence line is a
literal is not evidence.

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
cargo test --workspace --all-targets --locked                130 passed, 0 failed
cargo check --workspace --all-targets --locked               OK
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings   clean
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked             clean
cargo deny check                                              advisories/bans/licenses/sources ok
python3 scripts/check-foundation-boundaries.py --self-test     passed
(cd fuzz && cargo check --bins --locked)                      OK
git diff --check                                              clean
```

Test breakdown: 46 SAM unit tests (scripted-service transcripts), 3 shared-
Destination 3.3 fixture tests, 8 live-interoperability tests, 1 two-peer live
test (skips in this environment), 27 core, 38 storage/transmission, and the
remaining targets. `cargo deny` emits the same pre-existing
`license-not-encountered` and duplicate-`syn` warnings before and after this
change; no new advisories were introduced.

The guard's own report is now part of the evidence:

```
declared test-only host connectors (absent from the managed production profile):
  crates/i2pr-tc-i2p/tests/sam_live_qualification.rs
  crates/i2pr-tc-i2p/tests/sam_live_two_peer.rs
  crates/i2pr-tc-transmission/tests/transmission_remote.rs
```

Live re-verification after the second pass, against Java I2P 2.13.0 on
`127.0.0.1:17656`:

```
cargo test -p i2pr-tc-i2p --lib                                  46 passed, 0 failed
cargo test -p i2pr-tc-i2p --test sam33_shared_destination        3 passed, 0 failed
I2PR_TC_SAM_ADDR=127.0.0.1:17656 cargo test --test sam_live_qualification   8 passed, 0 failed
cargo test -p i2pr-tc-i2p --test sam_live_two_peer               1 skipped with a reason, 0 failed
```

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
| 7 | Java I2P and i2pd 3.3 behaviour qualified or residual incompatibilities recorded | **met** — i2pd qualified live (§5), Java I2P 2.13.0 qualified live in the second pass (§5a), including the normative `PRIMARY` spelling and all three child styles. Residual incompatibilities recorded for both. |
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
this repository against a SAM 3.3 service and against two real routers' child
channels. Correcting the first pass: the remaining gate is *not* "a router that
serves protocol 17/18 children" — Java I2P 2.13.0 does, and C003 attaches
protocol 17 and 18 children on it live (§5a). The gate is a reachable I2P peer:
upstream Plan 368 today, or any router position with working transit, which
neither router under test had (§5a.1). Authoring the M006 plan can start against
the shipped transport API; live qualification of a DHT cannot.

## 12. What this record does not claim

- It does not claim Java I2P interoperability beyond what §5a shows. Java I2P
  2.13.0 accepted the negotiation and all three child styles, but with no
  network database it carried no traffic. The first pass's statement that no
  Java router was available stood at commit `a94a7e8` and is superseded by
  §5a, not retroactively edited.
- It does not claim live peer-to-peer traffic. No second peer Destination was
  configured, and neither router under test has working transit — the i2pd has
  no viable tunnels and the Java router has no netDb at all — so no LeaseSet is
  published and no peer is reachable. §5a.1.
- It does not claim live datagram traffic. The children attach and detach on a
  real router, but no datagram was ever sent or received over I2P, because no
  peer is reachable. i2pd additionally refuses datagram children outright.
- It does not claim that `PORT=` on a datagram child grants this client a host
  UDP socket. It does the opposite; see §5a.
- It does not claim upstream Plan 368 progress. Nothing in this repository
  implements or substitutes for it, and criterion 8 remains unmet.

## 13. Corrective pass summary

After the first closure, Java I2P 2.13.0 was installed and the matrix re-run.
The pass produced three corrections and no regressions:

1. `SESSION ADD` for DATAGRAM/RAW now emits `PORT=`, which Java I2P requires and
   i2pd never sees. Verified live against Java for protocols 17 and 18.
2. The RAW delivery-reply parser accepts both the `RAW RECEIVED` and
   `RAW DATA RECEIVED` spellings, confirmed against the strings in Java's own
   bridge classes.
3. The in-memory SAM 3.3 service now enforces the same `PORT` rule as a real
   router, so the cross-transport fixture would catch a regression of (1).

A fourth, unrelated correction: the boundary guard's connector report is now
derived from what it scanned instead of a hard-coded literal (§5a.2).

Criterion 8 remains unmet and remains outside this repository. The disposition
is unchanged: **conditionally closed.**