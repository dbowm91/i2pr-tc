# M001 status report — core protocol and storage foundation

Status: **closed**

This record captures the implementation and closure evidence on `codex/planning-foundation`.

## Implementation commits

- `36ef50a5b16ad85f0c4edcb03cffbf8f51d867d9` — initial Rust workspace, core/storage primitives, parser and storage fixtures, fuzz targets, dependency guard, architecture snapshot, and registry activation.
- `66ef59d261c51df2394e42cf4cbf97515edd3ad8` — recorded the current i2pr upstream contract state and confirmed M004/M005 remain blocked.
- `d5b45397ae04820d92f52dd4727dff7d296f2670` — recorded the initial implementation checkpoint and closure gaps.
- `edfc7d65fcbe9474c25fc82effb8119708f7f724` — corrected peer availability replacement and multi-block in-flight ownership.
- `a1ad537a3e8b0f11451c68bddc4a5ce4eb189039` — added bounded incremental peer framing and hash-checked storage writes.
- `15c100297e4e4bc432f8f59cffe45a73058e049b` — added durable service state, bounded events/priorities, resume-root safety, cancellation, metadata bounds, boundary guards, and fuzz corpora.
- `73cbc38c91c13cda59715e7f3921b1af3a32b3dd` — made persistent progress depend on hash-verified storage writes and recovery-derived piece verification.
- `a2d32af3165f4f20ca8b58e74b23fda61f47983e` — composed `PieceMap`, bounded block assembly, storage writes, startup recovery, and runtime fixtures; retained fuzz discoveries.
- `762887a87821bd6bb76184b35430095a0091fd39` — connected peer-wire session state to runtime availability, cancellation, download, and verified upload paths; bounded catalog reads and resume bitmaps.
- `61ace427d7157e8f168d14a3bc8cc3501fd6492f` — added CI gates, expanded platform path and peer ownership fixtures, updated architecture and rechecked upstream handoff evidence.

## Landed scope

| Requirement area | Current evidence | Status |
|---|---|---|
| Workspace/dependency boundary | `i2pr-tc-core` and `i2pr-tc-storage`; no networking/HTTP/SAM/Transmission dependencies in core | Implemented |
| Bencode/metainfo | Bounded strict parser, duplicate/unsorted-key rejection, exact raw `info` span SHA-1, v1 single/multi-file validation, announce tiers, portable path and arithmetic limits | Implemented with positive and hostile-input fixtures |
| Magnet | Bounded v1 `btih` hex/base32, display name and tracker parsing | Basic implementation |
| Peer wire/extensions | Handshake/frame decoding, I2P 32-byte PEX entries, bounded `ut_metadata` message parsing and extension map | Bounded codecs/state integrated with runtime request and response ownership |
| Piece selection | Deterministic availability-aware scheduling primitive and bounded in-flight ownership | Composed with runtime block assembly, peer choke/disconnect cleanup, cancellation, and retry |
| Storage | Rooted file mapping, cross-file reads, hash-checked exact-size piece writes, piece recheck, serialized/cancellable disk access, per-torrent deletion, and rooted versioned atomic resume | Implemented; assumes private authorized root against concurrent local mutation |
| TorrentService | Native typed trait, bounded cursor-based event ring, priorities/limits, deterministic in-memory adapter, durable `PersistentTorrentService` catalog, magnet metadata promotion, restart intent restoration, and a `TorrentRuntime` composing `PieceMap`, bounded block assembly, storage, and progress | Implemented for metainfo-backed torrents; magnets become schedulable after metadata promotion |
| Static/dependency boundary | `scripts/check-foundation-boundaries.py --self-test` checks manifests and source references; self-test injects forbidden dependency/socket controls | Implemented and wired into `.github/workflows/ci.yml` |
| Fuzzing | Five cargo-fuzz targets with retained seed corpora | 10,000-run smoke passed for bencode, metainfo, peer_wire, extension, and resume; no crash artifacts |

## Verification run

Passed:

- `rtk proxy cargo fmt --all -- --check`
- `rtk proxy cargo fmt --manifest-path fuzz/Cargo.toml -- --check`
- `rtk proxy cargo test --workspace --all-targets --locked` — latest run: 55 tests passed (27 core, 28 storage)
- `rtk proxy cargo check --workspace --all-targets --locked`
- `rtk proxy cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `RUSTDOCFLAGS='-D warnings' rtk proxy cargo doc --workspace --no-deps --locked`
- `rtk proxy cargo check --manifest-path fuzz/Cargo.toml --bins --locked` — five targets
- `rtk proxy python3 scripts/check-foundation-boundaries.py --self-test`
- `rtk proxy cargo deny check` — all categories pass with duplicate `syn` and unused license allowance warnings
- `rtk proxy git diff --check`

Latest core protocol/storage checkpoint adds a pinned canonical infohash vector, numeric and parser resource limit cases, typed peer-wire state and frame encoding, strict known-message framing with decoder reset after malformed frames, compact bounded resume bitmaps with legacy decoding, large catalog bitmap coverage, short/long file-size rejection, stable-ID collision rejection, and peer-wire/runtime integration for handshakes, bitfields, requests, pieces, uploads, choke cancellation, and storage progress. Catalog metainfo reads are also byte-bounded. Deterministic injected-boundary fixtures cover cancellation between file writes and during recheck, cancellable runtime piece acceptance, plus atomic replacement interruption before rename. Final verification after these changes: 53 tests pass (27 core, 26 storage); Rust 1.81 workspace/all-targets check succeeds with that toolchain's existing `cfg(test)` warnings; all five fuzz targets completed 100 runs; `cargo deny` passed with existing duplicate-`syn` and unused license-allowance warnings.

Fuzz smoke:

- All five targets completed 10,000 libFuzzer runs per target with nightly 2026-09-10 on the local arm64 macOS host. Retained seed corpora remain under `fuzz/corpus/`; no crash artifacts were produced.
- Invocation through the configured RTK wrapper: `RUSTUP_TOOLCHAIN=nightly-2026-09-10 RUSTC=/Users/davidbowman/.rustup/toolchains/nightly-2026-09-10-aarch64-apple-darwin/bin/rustc rtk proxy cargo fuzz run <target> -- -runs=10000` for `bencode`, `metainfo`, `peer_wire`, `extension`, and `resume`.

## Unresolved acceptance work

No acceptance gaps remain. The residual local-filesystem concurrency limitation is recorded below as a security finding and does not change the authorized-private-root trust assumption.

## Security and compatibility evidence

The implemented parser and storage checks reject duplicate/unsorted bencode keys, duplicate/colliding file paths, unsafe and cross-platform-reserved path components (including drive, UNC, separator, reserved-device, trailing-dot/space, NUL, and dot traversal cases), mismatched piece counts, oversized values, malformed peer frames, non-32-byte PEX records, invalid magnet hash schemes, and stale resume identity. Wrong-hash writes fail before touching storage; durable service progress advances only after exact-length piece bytes pass the metainfo hash and storage write, and resume verification claims reset on restart. Peer ownership tests cover choke cancellation, disconnect cleanup, cancellation before persistence, and valid retry. No host networking or Transmission-specific type exists in core. The local data root must remain private to the application; standard path APIs and symlink checks do not prevent a concurrent local mutator from racing a check and operation.

## Unblock audit

M002 and M003 are promoted to `ready`: M001 provides the frozen service contract, event/progress semantics, peer/session state owner, recovery-backed piece map, and CI/fuzz evidence. M002 is the next implementation plan; M003 remains ready by its independent M001 dependency. M004 and M005 remain blocked on current upstream contract gaps recorded below; no transport or update behavior was added to M001 to bypass those blockers.

Upstream was rechecked on 2026-10-06: `main` is `144c54da2eaaa46497955e0e371f06ab6efcd1b1`, and the managed-runtime branch is `ea7b5ccef9bacbddf826f074cc59d891849a1424`. Plans 349 and 352–355 close corrected v1 policy and the private SAM/I2CP gateway. The current upstream registry says AppManager/package/process work is eligible but not registered; process authentication/runtime, package lifecycle, and sandbox remain unimplemented. The upstream tree also contains no torrent-facing ReleaseTarget or artifact staging/export contract. M004 remains blocked on M002 and the missing managed-app contract; M005 remains blocked on M002/M004 and update handoff interfaces.

Since commit `d5b4539`, M001 scheduler work replaced additive peer availability with peer-keyed replacement/withdrawal and keeps a piece in-flight until all outstanding blocks complete. The bounded peer-wire decoder handles fragmented/coalesced frames, rejects oversized announced lengths before payload accumulation, resets after malformed known frames, and bounds messages per feed; typed peer-wire state validates infohash handshakes, bitfield size/padding, choke-gated requests, and matching piece responses, and frame encoding is bounded. `TorrentRuntime` binds that state to I2P-sized peer identity keys, updates scheduler availability, cancels requests on choke/disconnect, routes piece blocks through bounded assembly and verified storage, and serves upload blocks only from verified pieces. Runtime piece acceptance and recovery propagate cancellation into storage, release request ownership, and update status only after successful writes. Storage verifies exact-size piece bytes before writes; cancellation is deterministically tested between file writes and rechecked pieces. Resume files stay under the authorized root, are byte-bounded on read and write, use compact bitmaps with legacy decoding, and use unique atomic temporary files; a simulated interruption before rename proves the prior file remains valid. Metainfo has a pinned canonical infohash vector, rejects duplicate and file/directory-colliding paths, retains bounded announce tiers, and rejects cross-platform reserved names. The service exposes bounded events, priorities and limits; its durable catalog restores desired-running torrents as `Starting`, returns completed torrents to `Checking`, and clears verified-piece claims. Magnet metadata promotion preserves the torrent ID and exposes resolved metainfo. Persistent service recovery rechecks stored files and rebuilds the progress bitmap. Existing short/long payload files are rejected; large piece bitmaps no longer bloat catalog records; stable TorrentId prefix collisions fail closed. Final local verification passed: 55 workspace tests, check, Clippy with warnings denied, rustdoc, fuzz-target compilation, 10,000-run smoke for each of five fuzz targets, Rust 1.81 check, mutation-tested foundation boundary guard, dependency checks, formatting, and `git diff --check`. CI now runs formatting, checks, tests, Clippy, docs, dependency policy, and the boundary self-test. M001 closure is recommended with the local filesystem race limitation retained as a low-severity finding; the private application data root remains the trust assumption.

## Closure recommendation

**Close M001.** The implementation and required verification gates now satisfy its bounded core/storage objective. Record the non-race-free standard-path limitation as a low-severity local-root finding, and promote only M002 and M003 to `ready`. Keep M004/M005 blocked on upstream-owned contracts.
