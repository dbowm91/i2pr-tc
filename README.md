# i2pr-tc

A Rust BitTorrent client purpose-built for the I2P network and intended to run
as a managed native application alongside i2pr.

The project prioritizes:

- I2P-only peer and tracker operation;
- a small, auditable torrent backend before frontend work;
- resilience to long latency, disconnects, and intermittent peers;
- Transmission RPC compatibility for existing client tooling;
- a strict separation between torrent transport and software-update trust;
- eventual integration with i2pr through its managed-app service-stream
  capability boundary rather than unrestricted host networking.

## Current status

The backend foundation is implemented. The client is not yet runnable as a
product: there is no frontend, no process entry point, and no managed i2pr
integration.

| Layer | State | Notes |
| --- | --- | --- |
| Core protocol | implemented | bencode, metainfo, magnet, BitTorrent v1 wire, extension protocol |
| Storage | implemented | verified piece writes, per-torrent state ownership, generation-checked persistence, bounded blocking offload |
| I2P transport | implemented, conditionally closed | C003 replaced the STREAM-only lifecycle with one long-lived SAM 3.3 shared-Destination primary session plus STREAM/DATAGRAM/RAW children, and replaced the placeholder local hash with the router-confirmed Destination hash |
| Tracker announces | implemented | I2P-only tracker URLs, deterministic bounded retry |
| Magnet metadata | implemented | `ut_metadata` acquisition, verification, and promotion |
| PEX | implemented | bounded peer discovery and source deduplication |
| Transmission RPC | implemented | request/response adapter over the local RPC surface |
| Managed i2pr integration | **decomposed / blocked** | upstream appd/apphost/package/policy foundations are now closed; M004-A–D wait on Plans 385–388 interfaces rather than missing generic AppManager ownership |
| I2P DHT | **M006-B active** | bounded KRPC/routing/token/tracker core is closed; protocol-17/18 transport and peer-source integration are underway |
| Update transport | **planned, not implemented** | blocked on upstream router-owned release, invocation, and artifact contracts |

Planning status lives in `plans/registry.md` and
`plans/subsystems/torrent-client-roadmap.md` on `main`. M004 is split into Plans 009–012; M006 DHT is split into Plans 013–014.

## Architecture

- `crates/i2pr-tc-core` — protocol primitives: bencode, metainfo, magnet,
  BitTorrent v1 wire framing, the extension protocol, and the service and
  scheduler types.
- `crates/i2pr-tc-storage` — verified storage, the durable torrent catalog,
  per-torrent runtime state, and the bounded blocking-work executor. This crate
  deliberately has no async runtime dependency.
- `crates/i2pr-tc-i2p` — the I2P transport: the SAM 3.3 shared-Destination
  client, tracker announces, peer sessions, magnet metadata, and PEX. One
  long-lived primary session owns the Destination; STREAM, DATAGRAM, and RAW
  child channels share it.
- `crates/i2pr-tc-transmission` — a Transmission RPC compatibility adapter.

There is no host-network connector in the production library surface. The I2P
transport asks a caller-supplied factory for raw SAM protocol byte streams and
speaks SAM itself; the router side is reached only through that seam. The
transport holds one long-lived primary/control stream plus child attachment
streams, all sharing one Destination. A direct local connector exists solely in
a declared test target for live qualification.

## Qualification boundaries

- Protocol behaviour is covered by deterministic tests, including full magnet
  acquisition to completion and inbound peer transfer.
- SAM protocol behaviour is covered by exact-octet transcript tests.
- Live-router qualification against i2pd 2.61.0 proved HELLO/session/naming wire
  details and exposed four self-consistent transcript defects, all retained as
  fixes. It is still **not final transport qualification**: the shared
  Destination is qualified over an in-memory SAM 3.3 service and, for STREAM,
  against the live bridge. Java I2P rows, live peer rows, and live datagram rows
  were not executable here and are recorded as unexercised with reasons in
  `plans/closure/torrent-client/008-c003-status.md`. The SAM/368 private seam
  implementation and closeout are prepared in the i2pr workspace and await
  integration to upstream main before managed-seam qualification proceeds.
- Tracker announces carry no `User-Agent`, and the peer extension handshake
  advertises no client version.

## Building

Rust 1.89, edition 2024, pinned in `rust-toolchain.toml`.

```
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo deny check
python3 scripts/check-foundation-boundaries.py --self-test
```

## License

MIT OR Apache-2.0.
