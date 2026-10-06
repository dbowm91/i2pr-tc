# M002 status report — I2P streaming, trackers, magnet metadata, and PEX

Status: **conditionally closed**

Implementation commit: `cbb65d03635652896fa41132c06bf60324c14147` (`feat: add I2P torrent transport and discovery`).

## Landed scope and evidence

| Requirement area | Evidence | Status |
|---|---|---|
| Router-owned I2P session boundary | `i2pr-tc-i2p::I2pSession` accepts an injected app-scoped session and byte streams. It has no SAM socket, IP socket, router process, or session lifecycle implementation. | Implemented at the M002 adapter seam; M004 owns production composition. |
| I2P identity and naming | Destination bytes hash to 32-byte SHA-256 identities; compact peers resolve through `<hash>.b32.i2p`; no IP socket address is retained. | Implemented with hostname/IP-literal and Destination-hash mismatch rejection. |
| HTTP tracker client | Bounded HTTP GET over injected streams, binary query encoding, I2P-only URL policy, response/header/body limits, hash-only 32-byte compact records, tier failover, capped exponential backoff with deterministic per-infohash jitter, operation timeouts, and cancellation. | Implemented and scripted against a fake injected session. Redirects are rejected because only HTTP 200 is accepted. |
| Peer streams | Outbound connect and bounded inbound accept run through the injected session; peer count, handshake, frame, idle, request-window, and write bounds are enforced. All terminal peer paths release runtime ownership. | Implemented. Scripted outgoing transfer downloads, verifies, stores, and completes a piece; wrong-infohash handshake is rejected. |
| Magnet metadata | `ut_metadata` uses bounded 16 KiB blocks and in-flight requests, exact SHA-1 verification, metainfo validation, durable promotion under the original torrent ID, and live peer-session metadata refresh. | Assembler mismatch/oversize/unsolicited cases and runtime metadata promotion/session retention are covered. The full exchange against a real remote implementation was not run. |
| I2P PEX | PEX accepts only 32-byte hashes; tracker and PEX sources share deduplication and failed-peer suppression. Shared peer sources feed bounded outgoing PEX updates and discovered peers are reported to the caller. | Codec/source behavior is covered. Live multi-peer propagation was not run. |
| Tracker tiers and magnet recovery | Metainfo tiers remain ordered and bounded; magnet tracker URLs survive metadata promotion and restart. Tracker announce lookup uses stored tiers and verifies the requested infohash. | Implemented with parser, persistence, and announce fixtures. |
| Static networking boundary | `scripts/check-foundation-boundaries.py --self-test` now includes `i2pr-tc-i2p`, allows only its declared async/protocol dependencies, and rejects host networking/router process APIs. | Passed. |

The rechecked protocol baseline is I2P SAM v3, I2P BitTorrent application guidance, and BEP 9/10/11. I2P compact peers carry 32-byte Destination hashes without ports; metadata exchange uses 16 KiB blocks and verifies against the infohash; extension IDs are peer-local. See [SAM v3](https://geti2p.net/en/docs/api/samv3), [I2P BitTorrent guidance](https://geti2p.net/en/docs/applications/bittorrent), [BEP 9](https://www.bittorrent.org/beps/bep_0009.html), [BEP 10](https://www.bittorrent.org/beps/bep_0010.html), and [BEP 11](https://www.bittorrent.org/beps/bep_0011.html).

## Verification

Passed on the repository workspace:

- `rtk proxy cargo fmt --all -- --check`
- `rtk proxy cargo test --workspace --all-targets --locked` — 68 tests (27 core, 12 I2P transport, 29 storage)
- `rtk proxy cargo check --workspace --all-targets --locked`
- `rtk proxy cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `RUSTDOCFLAGS='-D warnings' rtk proxy cargo doc --workspace --no-deps --locked`
- `rtk proxy cargo deny check` — passed with existing duplicate-`syn` and unused license-allowance warnings
- `rtk proxy python3 scripts/check-foundation-boundaries.py --self-test`
- `rtk proxy git diff --check`

The deterministic fake-session tests cover tracker request/response parsing, URL rejection, compact peer bounds, retry delay/cancellation, peer infohash rejection, a verified outgoing piece transfer, metadata size/hash/membership checks, PEX bounds/deduplication, and metadata promotion. No Java I2P or alternate-router process was available for a live SAM interoperability run.

## Unresolved findings

| Finding | Severity | Treatment |
|---|---|---|
| Live Java I2P and alternate SAM implementation interoperability has not been exercised. | Operational qualification | Carry into M004 integration qualification; do not claim a live-router pass. |
| SAM reconnect and coordinated swarm restoration are owned by the app/router session lifecycle, which is not part of this injected adapter crate. | Interface dependency | Recheck against the current i2pr AppManager successor before M004 implementation; do not add a direct host SAM fallback. |
| The tests do not yet exercise a complete magnet `ut_metadata` exchange or inbound peer transfer end to end. | Test coverage | Keep as M004/M002 interoperability qualification work; component and session-promotion behavior are covered. |

No clearnet fallback, direct host SAM connection, IP peer state, router identity reuse, DHT, or datagram dependency was introduced.

## Upstream and unblock audit

The upstream was fetched again on 2026-10-06. `main` remains `144c54da2eaaa46497955e0e371f06ab6efcd1b1`; `codex/plan-345-native-app-runtime` remains `ea7b5ccef9bacbddf826f074cc59d891849a1424`. Plans 354–355 provide private injected SAM/I2CP streams and the router principal gateway. AppManager/package/process work remains eligible but unregistered, and no torrent-facing `ReleaseTarget` or artifact staging/export contract exists.

- M003 can proceed: it depends on M001's frozen `TorrentService`, not M002.
- M004 remains **blocked**: it requires M002 qualification and the unregistered managed-app/AppManager, local-ingress, and private persistent-data contracts.
- M005 remains **blocked**: it requires M002/M004 and router-owned release-target plus staging/export interfaces.
- M006 remains deferred pending a stable production datagram/PRIMARY contract and a later spec recheck.

## Closure recommendation

**Conditionally close M002's I2P-only adapter implementation.** The local contract, bounds, persistence handoff, and scripted peer transfer are implemented and verified. Keep live-router interoperability, end-to-end magnet metadata exchange, inbound transfer, and router-disconnect recovery as explicit operational qualification before promoting M004. M003 may proceed independently from M001; M004 and M005 stay blocked.
