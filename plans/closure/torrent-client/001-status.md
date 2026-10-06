# M001 status report — core protocol and storage foundation

Status: **active; not closed**

This record captures the implementation checkpoint on `codex/planning-foundation`. It is not a closure claim. The milestone acceptance contract remains in force.

## Implementation commits

- `36ef50a` — initial Rust workspace, core/storage primitives, parser and storage fixtures, fuzz targets, dependency guard, architecture snapshot, and registry activation.
- `66ef59d` — recorded the current i2pr upstream contract state and confirmed M004/M005 remain blocked.
- This checkpoint report is recorded in its own follow-up commit.

## Landed scope

| Requirement area | Current evidence | Status |
|---|---|---|
| Workspace/dependency boundary | `i2pr-tc-core` and `i2pr-tc-storage`; no networking/HTTP/SAM/Transmission dependencies in core | Implemented |
| Bencode/metainfo | Bounded strict parser, duplicate/unsorted-key rejection, exact raw `info` span SHA-1, v1 single/multi-file validation, path and arithmetic limits | Partial fixtures |
| Magnet | Bounded v1 `btih` hex/base32, display name and tracker parsing | Basic implementation |
| Peer wire/extensions | Handshake/frame decoding, I2P 32-byte PEX entries, bounded `ut_metadata` message parsing and extension map | Partial codec; no complete peer state machine |
| Piece selection | Deterministic availability-aware scheduling primitive and bounded in-flight ownership | Primitive only; no transfer engine |
| Storage | Rooted file mapping, cross-file piece read/write, piece recheck, deletion, versioned atomic resume replacement | Partial; no race-free platform-specific descriptor-relative API or full restart integration |
| TorrentService | Native typed trait and deterministic in-memory catalog; Transmission-independent naming | Contract prototype; no durable service/runtime or event stream |
| Fuzzing | Four cargo-fuzz targets compile | Execution not qualified |

## Verification run

Passed:

- `rtk proxy cargo fmt --all -- --check`
- `rtk proxy cargo fmt --manifest-path fuzz/Cargo.toml -- --check`
- `rtk proxy cargo test --workspace --all-targets --locked` — 11 tests passed
- `rtk proxy cargo check --workspace --all-targets --locked`
- `rtk proxy cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `RUSTDOCFLAGS='-D warnings' rtk proxy cargo doc --workspace --no-deps --locked`
- `rtk proxy cargo check --manifest-path fuzz/Cargo.toml --bins --locked`
- `rtk proxy cargo deny check` — all categories pass with duplicate `syn` and unused license allowance warnings
- `rtk proxy git diff --check`

Fuzz run attempt:

- `rtk proxy cargo fuzz run bencode -- -runs=100` — blocked because cargo-fuzz requires nightly sanitizer flags and only stable Rust is installed. The other three fuzz targets were not run for the same reason.

## Unresolved acceptance work

- Build full peer-wire and extension codecs, including property/fuzz-ready decoder behavior and complete unknown-message policy.
- Implement a real authoritative torrent state machine, peer availability accounting, cancellation/drain ownership, verification reset, events, and deterministic restart behavior.
- Integrate durable torrent records, metainfo/magnet intent, storage lifecycle, deletion semantics, and bounded disk concurrency behind TorrentService.
- Complete the hostile-input, cross-platform path, multi-file, corruption, cancellation, resume crash/restart, stable ID, and scheduler fixture matrices in M001.
- Execute fuzz smoke under nightly and retain seed corpus/results.
- Reconcile crate graph and architecture docs after the remaining M001 work.

## Security and compatibility evidence

The implemented parser and storage checks reject duplicate/unsorted bencode keys, unsafe path components, mismatched piece counts, oversized values, malformed peer frames, non-32-byte PEX records, invalid magnet hash schemes, and stale resume identity. No host networking or Transmission-specific type exists in core. This is bounded foundational evidence only; storage symlink checks use ordinary filesystem path operations and are not a race-free sandbox boundary.

## Unblock audit

M002 remains blocked on full M001 closure. M003 remains blocked because the TorrentService is only a contract prototype and M001 has not closed. No later handoff is promoted.

The initial upstream check recorded below was superseded by a fresh 2026-10-06 inspection of `main` at `2f82c7998fc9f43c6f94843b58faa0b0fdc9c2e4` and the runtime branch at `ea7b5ccef9bacbddf826f074cc59d891849a1424`. Plans 349 and 352–355 close corrected v1 policy and the private SAM/I2CP gateway. That removes the prior gateway-contract blocker, but upstream explicitly has no AppManager/package/process plan; process authentication, package lifecycle, and sandbox remain unimplemented. The upstream tree also contains no router-owned ReleaseTarget or artifact staging/export contract. M004 remains blocked on M002 and the missing app-runtime contract; M005 remains blocked on M002/M004 and update handoff interfaces.

Since commit `d5b4539`, M001 scheduler work replaced additive peer availability with peer-keyed replacement/withdrawal and keeps a piece in-flight until all its outstanding blocks complete. The bounded peer-wire decoder now handles fragmented/coalesced frames and rejects oversized announced lengths before payload accumulation. Storage offers a verified-write operation that hashes before touching files, and resume loading enforces its byte cap during the read. Regression tests cover these changes. Workspace tests pass (16 total), Clippy with warnings denied passes, fuzz targets compile, and `git diff --check` passes. This is implementation progress only; it does not satisfy M001 closure criteria.

## Closure recommendation

**Do not close M001.** Continue implementation against the registered plan. Do not promote M002–M005 until their hard dependencies and interface evidence are met.
