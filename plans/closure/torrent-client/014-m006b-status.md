# Torrent Client M006-B — I2P DHT Transport and Integration Closure

Status: **conditionally closed**

Date: 2026-10-10

Plan: `plans/implementation/torrent-client/014-m006-i2p-dht-transport-integration.md`

Implementation commits:

- `9bc471fdeb7c511cd785b77bd2b0a8bef117b20d` — I2P DHT transport, peer-source
  integration, persistence, and deterministic two-node fixture.
- `27bc76f96dd8de3162d3c26204dea6aa4d177a75` — verified compact-hash
  Destination cache with bounded capacity, expiry, SAM-generation ownership,
  cancellation, and an end-to-end request deadline.

Closure recommendation: **conditionally closed**. All implementation and
deterministic acceptance criteria are met. Live Java DHT traffic could not be
qualified because no independent reachable DHT peer was available/configured;
the two-bridge live harness reports that it skipped for lack of a second SAM
bridge. Managed-product DHT remains unclaimed until the private gateway and
M004-B qualification are available.

## 1. Delivered

- Bound signed protocol-17 queries to the SAM DATAGRAM child and raw replies,
  errors, and announce queries to the protocol-18 RAW child under the existing
  primary Destination. `DhtResponder::run` checks the core's local Destination
  hash and query port against the router-confirmed SAM identity.
- Resolved compact Destination hashes only through canonical b32.i2p lookup
  and SHA-256 verification. Positive results are capped at 256 entries,
  expire after ten minutes, and are scoped to the SAM session generation.
- Added bounded pending reply correlation (256), traversal (32 nodes),
  datagram size (32 KiB), timeout (at most 120 seconds), token-cache (256), and
  shared peer-source/backoff (2,000) limits. Lookup, send, and reply wait share
  one request deadline; cancellation releases pending state.
- Integrated DHT provenance with tracker and PEX sources, preserving shared
  hashes, filtering self, retaining cross-source peers, and withdrawing DHT
  provenance independently.
- Added caller-driven bounded `get_peers`, iterative traversal, and
  token-authorized announce operations. Torrent lifecycle timing remains with
  the networked torrent owner; this repository's `TorrentRuntime` owns no
  network session or per-torrent async actor. Stop withdraws local DHT source
  provenance; remote announcements expire because the deployed profile has no
  unannounce operation.
- Added three owned responder workers for signed ingress, raw ingress/reply
  dispatch, and bounded maintenance/persistence. Shutdown joins workers and
  flushes only the bounded bootstrap snapshot.
- Added a deterministic two-node fixture covering ping, find_node, iterative
  get_peers, peer-source merge, and token-authorized raw announce. Bootstrap
  restore and final flush are also covered.

No new dependencies or host-network connectors were added. M006-B does not
rely on i2pd as an oracle for current SAM or DHT acceptance. The retained Java
I2P 2.13.0 SAM evidence in Plan 008 proves child attachment, not live DHT
reachability.

## 2. Requirement-to-evidence matrix

| Requirement | Result and evidence |
|---|---|
| Same-Destination protocol-17/18 transport | **Met.** `DhtResponder::run` verifies local hash/port against `I2pSession`; `SamClient` reuses the same-generation DATAGRAM and RAW children owned by the C003 primary session. The shared-Destination SAM fixture covers all three children. |
| No host UDP or alternate network connector | **Met.** Production DHT I/O is only through `DhtDatagramIo` implemented by `SamClient`; `scripts/check-foundation-boundaries.py --self-test` passes and reports host connectors only in declared test targets. |
| Verified compact-hash resolution | **Met.** `resolve_peer` uses canonical b32.i2p naming and verifies the resulting Destination hash. `destination_cache_is_positive_bounded_by_expiry_and_session_generation` covers positive reuse, 256-entry capacity, generation invalidation, expiry, and re-resolution. |
| DHT peer-source semantics | **Met.** `dht_source_deduplicates_with_tracker_and_pex_and_withdraws_independently`, `peer_source_set_stops_at_its_shared_capacity`, and the two-node fixture cover deduplication, independent withdrawal, capacity, and source merge. |
| Tracker/PEX remain usable when DHT is empty or unavailable | **Met at the boundary.** DHT operations return bounded errors to their caller and do not own or gate tracker/PEX work; the tracker suite passes independently and the peer-source set requires no DHT state. |
| Bounded and cancellable async work | **Met.** Named ceilings are in §1; requests share one deadline and cancellation race; the responder has a fixed three-worker JoinSet and bounded pending tables. Query correlation, responder cancellation through fixture teardown, SAM operation cancellation, and bounded storage restart state pass. |
| Deterministic multi-node protocol behavior | **Met.** `two_node_fixture_runs_get_peers_then_raw_token_authorized_announce` exercises ping, find_node, iterative get_peers, peer merge, and announce; `responder_correlates_raw_reply_with_bounded_pending_query` covers raw reply correlation. |
| Live compatible-router DHT traffic | **Operationally blocked; not passed.** The SAM live qualification tests ran, but no independent reachable DHT peer was configured. `sam_live_two_peer` printed `SKIPPED` because `I2PR_TC_LIVE_PEER_SAM_ADDR` was absent. No live DHT KRPC request, response, peer lookup, or Streaming connection is claimed. |
| i2pr private-gateway qualification | **Not yet eligible.** It remains a prerequisite before any managed-product DHT claim and depends on upstream SAM/368 integration plus M004-B. This closure does not promote that claim. |
| Routine verification | **Met.** Commands and results are listed in §3. |

## 3. Verification

All local checks below passed on the implementation tree:

```text
cargo fmt --all --check
cargo check --locked --workspace --all-targets
cargo test --locked --workspace --all-targets -- --test-threads=1
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps
cargo test --locked --workspace --doc
cargo deny check
python3 scripts/check-foundation-boundaries.py --self-test
git diff --check
```

The serial workspace test run reported 40 core, 56 I2P library, 3 shared-
Destination SAM fixture, 8 live SAM qualification, 39 storage, 6 Transmission,
and 1 Transmission remote-adapter tests passing. The separate two-peer test
returned successfully after printing its explicit `SKIPPED` reason; it is not
interop evidence. `cargo deny check` passed with the existing unmatched-license
allowance and duplicate-`syn` warnings. No dependencies changed.

## 4. Compatibility, lifecycle, and security disposition

- The KRPC wire profile remains the M006-A I2P profile: signed queries on
  protocol 17; raw replies/errors and announce queries on protocol 18; compact
  peers are Destination hashes, not IP endpoints.
- Secure node IDs validate the source Destination hash and query port before
  signed-query state is learned. Raw announce requester identity is recovered
  from the valid requester-bound token, never from the query's `id` field.
- Outbound reply matching is bounded and correlated by transaction and expected
  node; an available raw sender identity is also checked. Transaction IDs are
  caller supplied and documented as fresh/unpredictable.
- Compact hash resolution cannot update routing state unless the returned
  Destination hash matches. Cached entries are positive only and are invalid
  after expiry or session-generation change.
- Persistence retains validated bootstrap nodes only. Tokens, transactions,
  and peer-source runtime state are not persisted. Shutdown flushes the bounded
  snapshot.
- No client/version string, host IP, UDP socket, DNS resolver, clearnet
  bootstrap, direct peer socket, or second DHT Destination was introduced.
- Empty DHT state does not disable tracker or PEX paths. A managed runtime may
  not claim the DHT feature until SAM/368 is on upstream main and M004-B is
  qualified. Live Java DHT traffic remains an operational qualification gap.

No unresolved implementation security finding remains. The operational gaps
are explicit and do not weaken deterministic acceptance.

## 5. Unblock audit

| Successor | Result | Reason |
|---|---|---|
| Further M006 plan | **None registered or promoted** | Plan 014 completes the currently registered M006 sequence. Live Java DHT traffic is an operational qualification row, not a coding prerequisite for a new plan. |
| M004-A / Plan 009 | **Still blocked** | Upstream Plan 388 external SDK/package builder remains outstanding. M006-B does not satisfy it. |
| M004-B / Plan 010 | **Still blocked** | Requires M004-A and Plan 388; upstream SAM/368 must also be integrated to main before private-gateway qualification. |
| M004-C / Plan 011 | **Still blocked** | Requires M004-A/B and upstream Plans 385/386/388. |
| M004-D / Plan 012 | **Still blocked** | Requires M004-A and upstream Plans 387/388. |
| M005 / Plan 005 | **Still blocked** | Router-owned release target, private invocation, and artifact staging/export contracts remain absent; M006-B does not unblock them. |

No blocked future plan became eligible, so no readiness status was changed.
The subsystem roadmap remains active for M004/M005 work.

## 6. Closure record

Closure recommendation: **conditionally closed**. The transport implementation
and deterministic behavior are complete. The only M006-B evidence left open is
live Java DHT traffic with a reachable independent peer. The managed-product
claim remains gated on upstream SAM/368 integration and M004-B qualification.
