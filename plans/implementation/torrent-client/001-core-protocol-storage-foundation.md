# Torrent Client M001 — Core Protocol and Storage Foundation

Status: ready for handoff

Repository baseline: `f9f88bf1ad906f3ce3c0e444d76407eed1a31b66`

Source roadmap:
`plans/subsystems/torrent-client-roadmap.md#M001--core-protocol-and-storage-foundation`

Relevant ADRs: 0001, 0002, 0003

Primary class: infrastructure + protocol/storage invariants

## Objective

Create a Rust workspace containing a deterministic, bounded BitTorrent v1 core and durable storage layer that require no router or network access.

M001 freezes enough native TorrentService/state vocabulary for M002 and M003 to build without freezing SAM, HTTP listener, managed-app, UI, or update-specific behavior.

## Non-goals

No SAM/network sockets, tracker HTTP, live peer dialing/listening, DHT/datagram trackers, Transmission wire handling, managed-app packaging, update fetching, frontend, or BitTorrent v2/hybrid torrents.

## Required architecture

Recommended initial split:

- `crates/i2pr-tc-core`: bounded bencode framing/metainfo/magnet, infohash, peer-wire/extension codecs, torrent state, piece scheduling, native service DTO/traits;
- `crates/i2pr-tc-storage`: authorized-root filesystem layout, random-access piece/file mapping, resume/recheck persistence.

A root binary may be added only as a thin non-network scaffold. Do not create placeholder transport/RPC crates.

Core must not depend on Tokio networking, HTTP clients, SAM, i2pr, or Transmission-specific types.

## Metainfo and magnet contract

Support v1 single-file and multi-file torrents.

Requirements:

- bounded top-level bencode parsing;
- exact raw `info` dictionary byte-span capture;
- SHA-1 InfoHashV1 from those exact bytes;
- documented fail-closed duplicate/ambiguous key policy;
- checked integer/total-length arithmetic;
- bounded file/path/piece/announce/metainfo counts and lengths;
- reject absolute, empty, dot/dotdot, NUL, drive/prefix, and separator-ambiguous paths;
- v1 `xt=urn:btih` magnet parsing plus bounded tracker/name metadata;
- unsupported hash schemes explicit.

A reviewed permissive bencode crate may provide semantic decode. If it cannot preserve the exact `info` span, add a small bounded framing/span scanner. Never hash reserialization.

## Peer-wire and extension codecs

Implement bounded incremental codecs/state for v1 handshake, keepalive, choke/unchoke, interested/not interested, have/bitfield, request/piece/cancel, extension negotiation, `ut_metadata` framing, and `i2p_pex` framing/types using I2P identity bytes.

M001 performs no network behavior. Unknown extensions/messages have explicit ignore/reject policy. Length prefixes are checked before allocation.

## Torrent state and piece scheduling

Provide one-owner state primitives for stopped/starting/running/checking/error/completed states, peer availability, bounded in-flight block requests, piece completion/hash verification, invalid-piece reset, cancellation, disconnect cleanup, and a simple deterministic availability-aware selection policy.

Optimize only after correctness fixtures exist.

## Storage owner

All paths resolve below one authorized root.

Required: single/multi-file piece-to-file mapping, checked offsets/lengths, random-access writes/reads, no symlink/path escape, partial final piece, size mismatch detection, recheck by piece hash, deletion separated from torrent-record removal, and bounded disk concurrency.

## Resume persistence

Define a versioned strict resume format containing bounded local state: torrent identity/metainfo reference, desired run state, verified-piece checkpoint data, reporting counters if needed, and storage schema/mapping version.

Resume is advisory. On incompatible schema, file mismatch, impossible bitmap, or inconsistency, recheck rather than mark verified. Writes use atomic temp+rename or equivalent crash-safe replacement.

## TorrentService contract

Freeze a native API for add metainfo/magnet intent; list/query snapshots; start/stop; verify; remove with explicit data-delete flag; supported priorities/limits; reannounce intent; and bounded state/progress events.

Do not copy Transmission naming into core. Unsupported/unavailable operations are typed.

## Failure/restart/cancellation

Malformed input never partially installs a torrent record. Failed writes never mark blocks/pieces complete. Verified state advances only after hash verification. Cancellation stops new work and drains/invalidates in-flight ownership. Crash during resume write leaves old or new valid state. Startup validates persistence/storage before trusting resume bits.

## Ordered work packages

WP1 workspace/toolchain/crate boundaries/dependency review.

WP2 strict bencode/metainfo/magnet + exact infohash fixtures.

WP3 peer-wire/extension codecs with fuzz-ready decoders.

WP4 piece/block state and deterministic scheduler.

WP5 path-safe storage, mapping, verification/recheck.

WP6 versioned resume persistence/restart recovery.

WP7 native TorrentService/events and dependency guards.

## Required tests

Include canonical/noncanonical valid v1 infohash fixtures, exact-info-span proof, duplicate/invalid bencode, integer overflow, cross-platform path traversal corpus, piece/file/count boundaries, peer frame truncation/oversize/unknown messages, extension bounds, i2p_pex codec rejecting IP-shaped assumptions, cross-file pieces, corrupt/short/long files, corrupt piece redownload, resume restart/crash/stale state, cancellation during check/write, remove with/without data, deterministic scheduler behavior.

Add fuzz targets for bencode/metainfo, peer-wire, extension messages, and resume decode.

## Static/dependency guards

Prove foundational crates do not acquire production dependencies/use of HTTP clients, `tokio::net`/`std::net`, i2pr crates, Transmission RPC, or process execution.

## Verification

At closure record current equivalents of:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
cargo deny check
git diff --check
```

plus focused fuzz smoke.

## Acceptance criteria

M001 closes only when exact raw-info hashing is proven; hostile metainfo/wire/resume inputs are bounded; storage cannot escape its root; piece verification is authoritative over resume state; restart recovery is deterministic; native TorrentService exists without Transmission/network coupling; no production network owner exists in foundational crates; fuzz/full verification pass; and architecture docs match the landed graph.

## Stop conditions

Stop and register design/corrective work if the bencode choice cannot support exact-info hashing without ambiguity, storage requires unbounded state, TorrentService cannot remain Transmission-independent, network/SAM is required to prove core correctness, or a general-purpose engine must be imported wholesale.

## Closure evidence

Create `plans/closure/torrent-client/001-status.md` with implementation SHAs, crate/dependency graph, parser limits, infohash fixture matrix, path/security matrix, storage/restart matrix, fuzz results, verification commands, findings, and unblock audit for M002/M003.
