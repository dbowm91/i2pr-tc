# Torrent Client C002 — Planning, Documentation, MSRV, and Branch Integration Reconciliation

Status: blocked on M002 C001 closure

Source roadmap:
`plans/subsystems/torrent-client-roadmap.md`

Primary class: corrective planning/integration hygiene

Hard dependency:
`plans/implementation/torrent-client/006-m002-c001-sam-runtime-hardening.md` closed.

## 1. Objective

Reconcile the repository's public/default-branch state after the foundational
M001-M003 implementation and C001 transport hardening so that `main`,
README/documentation, planning registry, toolchain policy, and closure evidence
tell one truthful story.

This is intentionally separate from C001. It must not hide transport/runtime
defects inside a documentation-only merge pass.

## 2. Current debt

At registration time:

- `main` contains only the initial repository bootstrap while the active
  implementation lives on `codex/planning-foundation`;
- README still describes the repository as planning/bootstrap;
- the local registry's upstream summary is stale: current i2pr main has closed
  Plans 354 and 355, establishing private SAM/I2CP transport seams and the
  app-principal gateway;
- M004 remains blocked, but now on the narrower remaining AppManager/process,
  sandbox, host-owned ingress, and persistent-data owners plus C001
  qualification;
- i2pr-tc declares Rust 1.80 / edition 2021 while current i2pr and the broader
  project line use Rust 1.89 / edition 2024; that divergence is not documented
  as an intentional compatibility policy.

## 3. Planning reconciliation

After C001 closes:

- preserve historical M001/M002/M003 closure records;
- add C001 closure as new evidence rather than rewriting M002 history;
- update the subsystem roadmap to describe current M002+C001 status;
- update `plans/registry.md` with exact then-current i2pr refs and remaining
  M004/M005 blockers;
- remove statements that Plans 354/355 are pending if they remain closed;
- keep M005 blocked until router-owned ReleaseTarget/private invocation/artifact
  staging contracts actually exist;
- do not promote M006 datagram/DHT work merely because streaming SAM is ready.

## 4. README and architecture status

Rewrite README from planning/bootstrap language into a concise current project
status:

- Rust I2P-only torrent backend;
- implemented core/storage/I2P/Transmission layers;
- current support/qualification boundaries;
- no frontend yet;
- managed i2pr application integration still pending;
- update distribution is planned but not yet implemented.

Update architecture/integration docs if C001 changed the raw-SAM ownership
diagram or state/storage concurrency model.

Do not advertise M004/M005 capabilities before closure.

## 5. Explicit Rust toolchain/MSRV decision

Make the toolchain policy deliberate.

Preferred default for this project line is to align with current i2pr/eggstack
at Rust 1.89 and edition 2024 unless there is a concrete downstream consumer
requiring the lower floor.

Choose exactly one:

### Option A — align

- set workspace `rust-version = "1.89"`;
- migrate to edition 2024;
- qualify workspace and fuzz manifests on the declared floor;
- document the floor.

### Option B — retain lower independent floor

Retain Rust 1.80/edition 2021 only with explicit rationale and CI proving that
floor. State that i2pr-tc is intentionally buildable below the router's toolchain
floor and identify the compatibility benefit.

Do not leave the current mismatch undocumented.

## 6. Branch integration gate

Before merging/fast-forwarding the active work line to `main`:

1. fetch current `main` and active branch;
2. require no unexpected divergence or reconcile it normally;
3. require M001/M003 closed, M002 historical conditional closure plus C001
   positive closure;
4. require registry/roadmap/README/toolchain reconciliation;
5. run the full repository verification floor on the exact integration head;
6. confirm no generated fuzz crash artifacts, local build output, secrets, or
   machine-specific paths are tracked;
7. inspect the complete diff from merge base, including planning-only commits;
8. integrate without rewriting historical closure evidence;
9. verify the default branch contains the expected implementation and CI
   workflow;
10. delete/archive the work branch only after the integrated default branch is
    verified.

A squash is acceptable only if repository policy explicitly prefers it and the
closure records retain source implementation SHAs needed for provenance.
Otherwise preserve the existing history.

## 7. CI/default-branch qualification

The integrated `main` must run/retain at least:

- format;
- workspace test/check;
- Clippy warnings denied;
- rustdoc warnings denied;
- dependency policy;
- foundation boundary self-test.

Where GitHub Actions evidence is available, record the exact run ID against the
integrated head. If the connector cannot observe push-triggered run state,
record that limitation instead of claiming hosted CI passed.

## 8. Ordered work packages

### WP1 — current-state recheck

Re-read i2pr-tc branch/default state and current i2pr managed-app registry.

### WP2 — planning/doc reconciliation

Update registry, subsystem roadmap, README, architecture/integration docs, and
support statements.

### WP3 — MSRV/edition decision

Apply Option A or document/qualify Option B.

### WP4 — repository hygiene

Review tracked files, dependency policy, fuzz corpus/crash artifacts, CI,
license metadata, and machine-specific state.

### WP5 — exact integration-head verification

Run full local checks and record exact results.

### WP6 — default-branch integration and post-merge verification

Integrate the work line, verify `main`, update final closure evidence, and
audit what is actually unblocked next.

## 9. Acceptance criteria

C002 closes when:

1. `main` contains the reviewed foundational implementation;
2. README no longer calls the implemented repository planning/bootstrap;
3. registry and roadmap reflect the current i2pr 354/355 closure state and
   truthful remaining M004/M005 blockers;
4. M002 C001 closure is registered without rewriting historical M002 closure;
5. Rust version/edition policy is explicit and tested;
6. full checks pass on the exact integrated head;
7. branch/default-branch divergence is reconciled;
8. no capability is overclaimed;
9. an explicit next-work audit states whether M004 is still blocked and by
   which upstream owners.

## 10. Stop conditions

Stop rather than merge if:

- C001 has unresolved medium/high correctness or anonymity findings;
- default branch gained unrelated conflicting work;
- planning says M004 is ready while AppManager/process/sandbox/ingress/data
  ownership is still absent;
- toolchain migration causes unexplained behavior/compatibility regression;
- full verification does not pass on the exact integration head.

## 11. Closure evidence

Create
`plans/closure/torrent-client/007-c002-status.md` containing:

- source branch and merge-base/head SHAs;
- C001 closure reference;
- i2pr upstream SHA and current managed-app milestone state;
- README/registry/roadmap reconciliation evidence;
- chosen MSRV/edition decision and qualification;
- exact verification commands/results;
- hosted CI evidence or explicit visibility limitation;
- integration/merge commit;
- post-integration `main` SHA;
- remaining blockers and next eligible milestone.
