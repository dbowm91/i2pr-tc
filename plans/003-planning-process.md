# i2pr-tc Planning and Agent-Handoff Process

Status: normative planning governance

This repository adopts the CodeGG planning model: stable canonical direction, adaptable subsystem roadmaps, bounded milestone handoffs, and evidence-based closure.

## Document classes

Canonical long-term documents are `plans/000-003`. Ordinary implementation must not rewrite them to match transient code. Material product/ownership changes require an explicit canonical revision and, where durable across milestones, an ADR.

ADRs under `docs/adr/` record durable choices and preserve history when superseded.

Subsystem roadmaps translate canonical requirements into dependency-ordered milestones.

Milestone implementation plans under `plans/implementation/` are coding-agent handoffs. Each plan must include objective, non-goals, baseline, dependencies, invariants, expected production changes, ordered work packages, failure/restart/cancellation behavior, tests, guards, docs, acceptance criteria, stop conditions, and closure evidence.

Closure records under `plans/closure/` state what actually landed and what was proven. Compilation alone is not closure.

## Work classes

Each milestone identifies a primary class: invariant, capability, infrastructure, or polish. Infrastructure is not a completed user capability until a real consumer path exists.

## Dependencies

Use hard, interface, soft, and operational dependencies. `plans/registry.md` is authoritative for current readiness.

## Handoff rules

Before editing code, inspect current repository state and re-check external interface dependencies, especially i2pr managed-app contracts. Preserve unrelated work. Do not bypass blocked interfaces with direct host networking, loopback binding by the managed app, clearnet fallback, or torrent-owned update installation.

Material plan deviations must be recorded. Durable architecture changes require ADR/canonical reconciliation.

## Corrective passes

A post-closure defect gets a new corrective plan referencing original plan and closure evidence. Do not rewrite historical closure to hide defects.

## Required planning review

Before a plan becomes `ready`, confirm ownership/trust boundaries, dependency readiness, bounded scope, protocol/storage effects, cancellation/restart/retry/contention, security/input bounds, verifiable acceptance, and stop conditions.

## Registry vocabulary

proposed, ready, active, blocked, closing, closed, conditionally closed, superseded, archived.

## Closure requirements

Every closure record includes implementation commits, requirement-to-evidence matrix, exact tests/guards, compatibility/interoperability evidence, restart/fault/cancellation evidence, security findings, docs evidence, unresolved findings with severity, closure recommendation, and unblock audit.
