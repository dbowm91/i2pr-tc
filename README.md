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
| I2P transport | corrective active | C001 SAM code retained; C003 is replacing the STREAM-only lifecycle with SAM 3.3 PRIMARY/subsessions and a real shared Destination identity |
| Tracker announces | implemented | I2P-only tracker URLs, deterministic bounded retry |
| Magnet metadata | implemented | `ut_metadata` acquisition, verification, and promotion |
| PEX | implemented | bounded peer discovery and source deduplication |
| Transmission RPC | implemented | request/response adapter over the local RPC surface |
| Managed i2pr integration | **blocked** | blocked on C003, upstream i2pr Plan 368 SAM 3.3 PRIMARY/subsessions, AppManager/process ownership, sandbox containment, host-owned ingress, and private persistent-data/key semantics |
| Update transport | **planned, not implemented** | blocked on upstream router-owned release, invocation, and artifact contracts |

Planning status lives in `plans/registry.md` and
`plans/subsystems/torrent-client-roadmap.md` on `main`.

## Architecture

- `crates/i2pr-tc-core` — protocol primitives: bencode, metainfo, magnet,
  BitTorrent v1 wire framing, the extension protocol, and the service and
  scheduler types.
- `crates/i2pr-tc-storage` — verified storage, the durable torrent catalog,
  per-torrent runtime state, and the bounded blocking-work executor. This crate
  deliberately has no async runtime dependency.
- `crates/i2pr-tc-i2p` — the I2P transport: retained bounded SAM client code,
  tracker announces, peer sessions, magnet metadata, and PEX. C003 is the
  current authority for converting the transport to SAM 3.3 PRIMARY/subsessions
  with one shared Destination across STREAM and future DATAGRAM/RAW use.
- `crates/i2pr-tc-transmission` — a Transmission RPC compatibility adapter.

There is no host-network connector in the production library surface. The I2P
transport asks a caller-supplied factory for raw SAM protocol byte streams and
speaks SAM itself; the router side is reached only through that seam. C003
requires one long-lived primary/control stream plus child attachment streams,
all sharing one Destination. A direct local connector exists solely in a
declared test target for live qualification.

## Qualification boundaries

- Protocol behaviour is covered by deterministic tests, including full magnet
  acquisition to completion and inbound peer transfer.
- SAM protocol behaviour is covered by exact-octet transcript tests.
- Live-router qualification against i2pd 2.61.0 was useful but is **not final
  transport qualification**. It proved HELLO/session/naming wire details and
  exposed four self-consistent transcript defects, all retained as fixes.
  Subsequent SAM/I2CP research found a deeper lifecycle issue: the final torrent
  transport must hold one long-lived SAM 3.3 primary Destination and share it
  across STREAM plus future DATAGRAM/RAW DHT channels. C003 and upstream i2pr
  Plan 368 now own that qualification.
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