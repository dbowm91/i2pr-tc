# Torrent Client M004-C — Private Data, Persistent Destination, and Secured Profile

Status: blocked on upstream Plans 385, 386, and 388

Date: 2026-10-09

Parent milestone:
`plans/implementation/torrent-client/004-i2pr-managed-app-integration.md`

Primary class: security invariant + persistence/lifecycle integration.

Hard dependencies:

- M004-A closed.
- upstream Plan 385 closed — private application data/cache/runtime roots.
- upstream Plan 386 closed — qualified Linux Secured sandbox/resource backend.
- upstream Plan 388 closed.
- M004-B closed for final secured I2P-network qualification.

## 1. Objective

Run i2pr-tc under the real `Secured` profile, place all mutable application
state beneath the authorized private root, persist the torrent I2P Destination
identity across restarts, and prove supported operation while direct host
networking and unrelated filesystem access are denied.

## 2. Data layout

Within the Plan-385 persistent app data root, define stable application-owned
subdirectories, conceptually:

```text
data/
  identity/       SAM private Destination/key material
  torrents/       torrent catalog/resume metadata
  payload/        first managed-profile download root
  incomplete/     transient piece staging if needed
cache/
  derived/discardable state
run/
  instance-scoped transient files only
```

Exact names may differ after implementation review.

Do not store secrets in package resources, logs, policy files, command
arguments, or the manager state root.

Arbitrary user-selected host download paths remain out of scope until a scoped
filesystem capability exists.

## 3. Destination identity

On first production launch:

- create or obtain one SAM private Destination through the C003 transport;
- atomically persist its canonical private representation beneath
  `data/identity`;
- apply owner-private permissions;
- fsync/replace according to the storage policy;
- never log the bytes.

On restart:

- validate/decode the stored identity strictly;
- inject it into C003 primary creation;
- prove `local_peer_hash()` is unchanged.

Corrupt identity fails closed with an actionable typed error. Do not silently
mint a new identity and thereby change peer/DHT addressability.

Key rotation/import/export is future work.

## 4. Storage adaptation

The existing rooted storage owner must be rebased on the authorized app data
root without weakening its symlink/path protections.

Required:

- all mutable torrent metadata below the app root;
- payload roots below the allowed managed root;
- no cwd/home/temp fallback on permission or space failure;
- cache may be rebuilt;
- runtime directory contains no durable truth;
- quota/filesystem exhaustion is surfaced, not redirected.

## 5. Secured-profile qualification

Run through upstream Plan-386's real Linux backend.

Prove that the packaged app can:

- complete managed-app protocol;
- open private SAM through M004-B;
- read/write its own allowed roots;
- download/seed deterministic torrent traffic in the controlled environment;
- restart and retain torrent + Destination identity.

At the same time prove it cannot:

- create direct host TCP/UDP sockets;
- connect/bind loopback;
- read sibling app data;
- read router policy/package-admin state;
- read arbitrary home files;
- leave descendants after manager termination;
- exceed enforced open-file/memory limits without the sandbox reacting as
  specified.

## 6. Cancellation/crash/recovery

Test cancellation or process death during:

- identity atomic write;
- resume/catalog write;
- piece persistence;
- recheck;
- manager shutdown.

Restart must restore only committed truth. It may recheck payload, but it may not
trust incomplete resume state or replace the Destination identity.

## 7. Work packages

WP1 — freeze app-private data layout and identity file contract.

WP2 — rebase torrent/storage paths on Plan-385 roots.

WP3 — atomic persistent Destination key lifecycle.

WP4 — consume Plan-386 Secured profile and resource policy.

WP5 — restart/crash/recovery and adversarial sandbox qualification.

WP6 — docs/guards/closure.

## 8. Acceptance criteria

M004-C closes when:

1. no mutable production state exists outside Plan-385 authorized roots;
2. the same real I2P Destination survives router/app restart;
3. corruption never silently rotates identity;
4. the app downloads/seeds over private SAM while host/loopback networking is
   denied;
5. filesystem/process/resource containment is evidenced by real Plan-386
   black-box tests;
6. restart/recheck preserves only verified torrent truth;
7. no arbitrary host path is required;
8. routine verification passes.

## 9. Stop conditions

Stop if Secured requires direct loopback, if dynamic/runtime dependencies force
an unbounded host filesystem grant, if identity persistence cannot be atomic
within the authorized root, or if payload storage requires arbitrary host paths.

## 10. Closure evidence

Create `plans/closure/torrent-client/011-m004c-status.md` with exact data
layout, identity hash before/after restart, corruption behavior, sandbox
attestation, host-network/filesystem negatives, resource evidence,
crash/recovery matrix, and M004 final-readiness audit.
