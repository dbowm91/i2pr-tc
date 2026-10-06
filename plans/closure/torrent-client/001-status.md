# M001 status report — core protocol and storage foundation

Status: **active; not closed**

This record captures the implementation checkpoint on `codex/planning-foundation`. It is not a closure claim. The milestone acceptance contract remains in force.

## Implementation commits

- `36ef50a` — initial Rust workspace, core/storage primitives, parser and storage fixtures, fuzz targets, dependency guard, architecture snapshot, and registry activation.
- `66ef59d` — recorded the current i2pr upstream contract state and confirmed M004/M005 remain blocked.
- `d5b4539` — recorded the initial implementation checkpoint and closure gaps.
- `edfc7d6` — corrected peer availability replacement and multi-block in-flight ownership.
- `a1ad537` — added bounded incremental peer framing and hash-checked storage writes.
- `15c1002` — added durable service state, bounded events/priorities, resume-root safety, cancellation, metadata bounds, boundary guards, and fuzz corpora.
- `73cbc38` — made persistent progress depend on hash-verified storage writes and recovery-derived piece verification.
- `a2d32af` — composed `PieceMap`, bounded block assembly, storage writes, startup recovery, and runtime fixtures; retained fuzz discoveries.
- `762887a` — connected peer-wire session state to runtime availability, cancellation, download, and verified upload paths; bounded catalog reads and resume bitmaps.

## Landed scope

| Requirement area | Current evidence | Status |
|---|---|---|
| Workspace/dependency boundary | `i2pr-tc-core` and `i2pr-tc-storage`; no networking/HTTP/SAM/Transmission dependencies in core | Implemented |
| Bencode/metainfo | Bounded strict parser, duplicate/unsorted-key rejection, exact raw `info` span SHA-1, v1 single/multi-file validation, announce tiers, portable path and arithmetic limits | Core implemented; broader protocol corpus remains |
| Magnet | Bounded v1 `btih` hex/base32, display name and tracker parsing | Basic implementation |
| Peer wire/extensions | Handshake/frame decoding, I2P 32-byte PEX entries, bounded `ut_metadata` message parsing and extension map | Partial codec; no complete peer state machine |
| Piece selection | Deterministic availability-aware scheduling primitive and bounded in-flight ownership | Primitive only; no transfer engine |
| Storage | Rooted file mapping, cross-file reads, hash-checked exact-size piece writes, piece recheck, serialized/cancellable disk access, per-torrent deletion, and rooted versioned atomic resume | Partial; no race-free platform-specific descriptor-relative API or automatic service/recheck composition |
| TorrentService | Native typed trait, bounded cursor-based event ring, priorities/limits, deterministic in-memory adapter, durable `PersistentTorrentService` catalog, magnet metadata promotion, restart intent restoration, and a `TorrentRuntime` composing `PieceMap`, bounded block assembly, storage, and progress | Runtime ownership implemented for metainfo-backed torrents; protocol state machine and broader fault matrix remain |
| Static/dependency boundary | `scripts/check-foundation-boundaries.py --self-test` checks manifests and source references; self-test injects forbidden dependency/socket controls | Implemented locally; not wired into CI yet |
| Fuzzing | Five cargo-fuzz targets with retained seed and generated corpora | 100-run smoke passed for bencode, metainfo, peer_wire, extension, and resume; sustained fuzzing remains |

## Verification run

Passed:

- `rtk proxy cargo fmt --all -- --check`
- `rtk proxy cargo fmt --manifest-path fuzz/Cargo.toml -- --check`
- `rtk proxy cargo test --workspace --all-targets --locked` — 44 tests passed (25 core, 19 storage)
- `rtk proxy cargo check --workspace --all-targets --locked`
- `rtk proxy cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `RUSTDOCFLAGS='-D warnings' rtk proxy cargo doc --workspace --no-deps --locked`
- `rtk proxy cargo check --manifest-path fuzz/Cargo.toml --bins --locked` — five targets
- `rtk proxy python3 scripts/check-foundation-boundaries.py --self-test`
- `rtk proxy cargo deny check` — all categories pass with duplicate `syn` and unused license allowance warnings
- `rtk proxy git diff --check`

Latest core protocol/storage checkpoint adds typed peer-wire state and frame encoding, strict known-message framing with decoder reset after malformed frames, compact bounded resume bitmaps with legacy decoding, large catalog bitmap coverage, short/long file-size rejection, and peer-wire/runtime integration for handshakes, bitfields, requests, pieces, uploads, choke cancellation, and storage progress. Catalog metainfo reads are also byte-bounded. The runtime adds lifecycle invalidation for outstanding blocks. Final verification after these changes: 47 tests pass (26 core, 21 storage); Rust 1.81 workspace/all-targets check succeeds with that toolchain's existing `cfg(test)` warnings; all five fuzz targets completed 100 runs; `cargo deny` passed with existing duplicate-`syn` and unused license-allowance warnings.

Fuzz smoke:

- All five targets completed 100 libFuzzer runs with nightly 2026-09-10 on the local arm64 macOS host. Seed corpora and coverage-discovered inputs are retained under `fuzz/corpus/`; no crash artifacts were produced.
- Example invocation through the configured RTK wrapper: `rtk proxy bash -c 'export RUSTUP_TOOLCHAIN=nightly-2026-09-10; export RUSTC=/Users/davidbowman/.rustup/toolchains/nightly-2026-09-10-aarch64-apple-darwin/bin/rustc; cargo fuzz run bencode -- -runs=100'` (same invocation for `metainfo`, `peer_wire`, `extension`, and `resume`).

## Unresolved acceptance work

- Compose `PieceMap`, verified storage, service snapshots, and completion callbacks under one authoritative torrent runtime owner.
- Integrate `PeerWireSession` lifecycle and choke/request/piece events directly with `TorrentRuntime` and add transport-fake end-to-end state transition fixtures.
- Complete crash interruption, cancellation during active multi-file writes/recheck, stable-ID collision, broader malformed parser/peer-wire matrices, and invalid-piece redownload/peer ownership coverage.
- Complete crash interruption, cancellation during active multi-file writes/recheck, stable-ID collision, corrupt/short/long file, and broad peer-wire fixture matrices.
- Wire the dependency guard into CI and qualify platform-specific filesystem guarantees; ordinary path checks still have TOCTOU limits.
- Run longer fuzz campaigns and retain their evidence.

## Security and compatibility evidence

The implemented parser and storage checks reject duplicate/unsorted bencode keys, duplicate/colliding file paths, unsafe and cross-platform-reserved path components, mismatched piece counts, oversized values, malformed peer frames, non-32-byte PEX records, invalid magnet hash schemes, and stale resume identity. Wrong-hash writes fail before touching storage; durable service progress advances only after exact-length piece bytes pass the metainfo hash and storage write, and resume verification claims reset on restart. No host networking or Transmission-specific type exists in core. This is bounded foundational evidence only; storage symlink checks use ordinary filesystem path operations and are not a race-free sandbox boundary.

## Unblock audit

M002 remains blocked on full M001 closure. M003 remains blocked because the TorrentService still lacks magnet metadata promotion and integrated torrent runtime ownership. No later handoff is promoted.

The initial upstream check recorded below was superseded by a fresh 2026-10-06 inspection of `main` at `2f82c7998fc9f43c6f94843b58faa0b0fdc9c2e4` and the runtime branch at `ea7b5ccef9bacbddf826f074cc59d891849a1424`. Plans 349 and 352–355 close corrected v1 policy and the private SAM/I2CP gateway. That removes the prior gateway-contract blocker, but upstream explicitly has no AppManager/package/process plan; process authentication, package lifecycle, and sandbox remain unimplemented. The upstream tree also contains no router-owned ReleaseTarget or artifact staging/export contract. M004 remains blocked on M002 and the missing app-runtime contract; M005 remains blocked on M002/M004 and update handoff interfaces.

Since commit `d5b4539`, M001 scheduler work replaced additive peer availability with peer-keyed replacement/withdrawal and keeps a piece in-flight until all outstanding blocks complete. The bounded peer-wire decoder handles fragmented/coalesced frames, rejects oversized announced lengths before payload accumulation, resets after malformed known frames, and bounds messages per feed; typed peer-wire state validates infohash handshakes, bitfield size/padding, choke-gated requests, and matching piece responses, and frame encoding is bounded. `TorrentRuntime` binds that state to I2P-sized peer identity keys, updates scheduler availability, cancels requests on choke/disconnect, routes piece blocks through bounded assembly and verified storage, and serves upload blocks only from verified pieces. Storage verifies exact-size piece bytes before writes; serialized disk access can be cancelled between files/pieces. Resume files stay under the authorized root, are byte-bounded on read and write, use compact bitmaps with legacy decoding, and use unique atomic temporary files. Metainfo rejects duplicate and file/directory-colliding paths, retains bounded announce tiers, and rejects cross-platform reserved names. The service exposes bounded events, priorities and limits; its durable catalog restores desired-running torrents as `Starting`, returns completed torrents to `Checking`, and clears verified-piece claims. Magnet metadata promotion preserves the torrent ID and exposes resolved metainfo. Persistent service recovery rechecks stored files and rebuilds the progress bitmap. Existing short/long payload files are rejected; large piece bitmaps no longer bloat catalog records. The current workspace suite passes (47 total), Clippy with warnings denied, rustdoc, fuzz-target compilation and 100-run smoke for each of five targets, Rust 1.81 check, the mutation-tested foundation boundary guard, and `git diff --check`. Crash/cancellation edge qualification, longer fuzz campaigns, and race-free descriptor-relative filesystem guarantees remain; M001 stays active.

## Closure recommendation

**Do not close M001.** Continue implementation against the registered plan. Do not promote M002–M005 until their hard dependencies and interface evidence are met.
