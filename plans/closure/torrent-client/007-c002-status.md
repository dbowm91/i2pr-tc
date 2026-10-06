# C002 — Planning, documentation, MSRV, and branch integration reconciliation: closure record

- Plan: `plans/implementation/torrent-client/007-c002-foundation-branch-reconciliation.md`
- Closed: 2026-10-06
- Result: **closed**. The foundational work line is integrated into `main`.

## 1. Branches and SHAs

| Item | Value |
| --- | --- |
| Source branch | `codex/planning-foundation` |
| Merge base with `main` | `f9f88bf1ad906f3ce3c0e444d76407eed1a31b66` |
| Integration head (pre-merge) | `624f11d3d400cc71a0243b821260433ac9bb8859` |
| Commits integrated | 45 |
| Diff from merge base | 227 files changed, 18387 insertions, 4 deletions |
| Integration mode | fast-forward; `main` had gained no work of its own |
| Post-integration `main` SHA | `624f11d3d400cc71a0243b821260433ac9bb8859` |
| This closure record | committed on `main` on top of that SHA, as the only commit after the fast-forward |

`main` at the merge base was bootstrap only: no crate source, no CI workflow,
no `rust-toolchain.toml`. The whole implementation line therefore lands as one
fast-forward with no reconciliation needed. The only commit on `main` after the
fast-forward is this closure record.

## 2. C001 closure reference

`plans/closure/torrent-client/006-m002-c001-status.md`, implementation
`852a8efd81308d51da24ce039c0b926fa52b21ec`. Historical M001/M002/M003 closure
records were not rewritten; M002 remains recorded as conditionally closed and
C001 is registered as separate, additional evidence.

## 3. i2pr upstream state

- Upstream: `dbowm91/i2pr`, branch `main`, SHA
  **`144c54da2eaaa46497955e0e371f06ab6efcd1b1`**, fetched 2026-10-06.
- Managed-app milestones: Plans 345, 349, 352, 353, 354, and 355 are closed.
  354 and 355 provide the listener-independent private SAM/I2CP protocol
  connection and the router app-principal/capability gateway.
- Remaining M004 upstream owners, none of which exist today: AppManager
  package and process ownership and process authentication, OS sandbox and
  resource containment, host-owned local RPC ingress, and private
  persistent-data semantics.
- M005 remains blocked independently: no torrent-facing router `ReleaseTarget`,
  private update invocation, or artifact staging/export contract exists.

## 4. Reconciliation evidence

| Surface | Change |
| --- | --- |
| `README.md` | rewritten from planning/bootstrap language into the implemented backend state, with a per-layer status table, a qualification-boundaries section stating that live-router interoperability has **not** been run, and an explicit "no frontend, no entry point, managed integration pending" |
| `plans/registry.md` | C001 registered as closed with its closure path and implementation SHA; C002 moved from blocked to ready and then closed; M004's blocker narrowed to the owners that actually remain; roadmap status line updated |
| `plans/subsystems/torrent-client-roadmap.md` | header status updated; C001 marked closed with its closure reference and exit condition recorded as reached, including the live-qualification residual |
| `docs/integration/i2pr-managed-app-requirements.md` | the "pre-runtime" framing and the `codex/plan-345-native-app-runtime` reference were stale; replaced with the current `main` SHA, an explicit statement that Plans 354/355 are closed, a per-section implemented/pending split, and the narrowed M004/M005 blockers |
| `docs/architecture/overview.md` | the storage layer no longer serializes through one global mutex and the runtime no longer dials; the SAM transport section and the per-torrent reservation/generation model were added; "Planned layers" replaced by "Not yet implemented" so M004/M005 are not advertised as present |

No capability is claimed before its closure. M006 datagram/DHT work was not
promoted: streaming SAM readiness alone does not establish a stable production
SAM datagram/PRIMARY/subsession contract.

## 5. Toolchain decision — Option A, align

Chosen: **align with the router's toolchain.**

| Item | Before | After |
| --- | --- | --- |
| Workspace `rust-version` | `1.80` | `1.89` |
| Workspace `edition` | `2021` | `2024` |
| Toolchain pinning | none | `rust-toolchain.toml` pinning `1.89` |
| CI toolchain | `dtolnay/rust-toolchain@stable` | pinned `1.89`, plus an assertion that the resolved workspace floor is the declared one |

Rationale: i2pr-tc is built by the same line as the router, and there is no
downstream consumer that cannot also build the router. An independent lower
floor would buy nothing and, being undeclared, only hid drift — which is
exactly what it had done.

Qualification: the migration is verified, not assumed. Edition 2024 changes
rustfmt's import-ordering style and enables let-chains, so applying it surfaced
real lint failures under the now-enforced floor: two `unnecessary_map_or` and
five `collapsible_if` sites, all fixed in the same style language rather than
suppressed with `allow`. The full floor passes at the integration head.

The fuzz crate is excluded from the workspace, so it states `edition = "2024"`
and `rust-version = "1.89"` explicitly rather than inheriting them.

## 6. Repository hygiene

| Finding | Action |
| --- | --- |
| `crates/i2pr-tc-core/.DS_Store` was tracked | removed and `.DS_Store` added to `.gitignore` |
| M001 closure record contains a machine-specific absolute toolchain path | annotated, **not rewritten**: an editorial note records that the path is not reproducible and that the reproducible part is `cargo fuzz run <target> -- -runs=10000`. The recorded finding is unchanged, which is what "do not rewrite historical closure evidence" requires |
| fuzz crash artifacts / local build output | none tracked; `target/`, `fuzz/target/`, `fuzz/artifacts/` are ignored, and `fuzz/corpus/` seed corpora are intentionally tracked |
| secrets, keys, tokens | none found in tracked files |
| license metadata | `MIT OR Apache-2.0` declared at the workspace level; `cargo deny` licenses check passes |
| CI | retains format, test, check, clippy `-D warnings`, rustdoc `-D warnings`, dependency policy, and the foundation boundary self-test, now on the pinned toolchain |

## 7. Exact verification at the integration head

Run at `624f11d3d400cc71a0243b821260433ac9bb8859`, before and again after the
merge:

```
cargo fmt --all -- --check                                     # clean
cargo test --workspace --all-targets --locked                  # 123 passed, 0 failed
cargo check --workspace --all-targets --locked                 # clean
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
cargo deny check                                               # advisories/bans/licenses/sources ok
python3 scripts/check-foundation-boundaries.py --self-test     # passed
(cd fuzz && cargo check --bins --locked)                       # clean
```

Test counts by crate: core 27, storage 38, i2p 46, i2p live harness 4,
transmission 6, transmission integration 1 — 122 plus the fuzz crate's build.

## 8. Hosted CI evidence

GitHub Actions run **37507965941**, workflow `Rust checks`, head
`c94af33254068b192462abedf1c8805be1a8fa5c`, result **success**.

Per-step results on that exact integrated head:

| Step | Result |
| --- | --- |
| Formatting | success |
| Workspace tests | success |
| Workspace check | success |
| Clippy (`-D warnings`) | success |
| Rustdoc (`-D warnings`) | success |
| Dependency policy (`cargo deny check`) | success |
| Foundation boundary guard (`--self-test`) | success |
| Declared toolchain floor | success |

CI builds the pinned 1.89 toolchain rather than whichever `stable` is current,
which is what makes the "declared floor" step meaningful rather than decorative.
The local floor in §7 and this hosted run agree.

## 9. Post-integration `main` contents

- 26 Rust source files across four crates, plus two integration-test targets.
- `.github/workflows/ci.yml` and `rust-toolchain.toml` present.
- `plans/closure/torrent-client/` holds 001, 002, 003, 006, and 007 records,
  with 001/002/003 unchanged apart from the appended M001 annotation.

## 10. Next-work audit

**M004 is still blocked, but on a narrower and more honest set of owners than
before.** What changed:

- the application side of the managed-app SAM contract is implemented and
  transcript-tested, so the transport is no longer a blocker;
- M004 now owns composing `SamConnectionFactory` from the managed runtime;
- M004's first act is running the live interoperability matrix in the C001
  closure against a real Java I2P or i2pd SAM bridge, which is the medium
  residual finding carried forward.

What still blocks M004, none of it owned by this repository:

1. AppManager package and process ownership, and process authentication.
2. OS sandbox and resource containment.
3. Host-owned local RPC ingress (the Transmission adapter owns no listener).
4. Private persistent-data semantics, including where the SAM Destination key
   material lives across restarts.

**M005 remains blocked** on router-owned `ReleaseTarget`, private invocation,
and artifact staging/export contracts, none of which exist upstream.

**M006 remains deferred.** Streaming SAM readiness does not establish a stable
production SAM datagram/PRIMARY/subsession contract, which is what M006 would
need.

The eligible next plan is therefore **M004**, and its first work package should
be live-router qualification rather than any new implementation.