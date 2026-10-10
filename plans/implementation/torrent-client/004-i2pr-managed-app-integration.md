# Torrent Client M004 — i2pr Managed-App Integration

Status: blocked umbrella; execution decomposed into M004-A through M004-D

## 2026-10-09 upstream reconciliation and decomposition

The previous blocker description is stale. Upstream managed runtime/process and
package-policy work has advanced:

- Managed native app runtime/369–371 are closed;
- Plans 382–383 are closed;
- Plan 385 private app data is ready;
- Plan 386 Linux Secured sandbox is blocked on 385;
- Plan 387 host-owned local-service ingress is ready;
- Plan 388 external Rust app SDK/package builder is ready;
- the separate colliding SAM/368 remains ready and unimplemented.

This umbrella no longer serves as a single coding-agent handoff. Execute:

- M004-A / Plan 009 — package + lifecycle bootstrap, blocked on 388;
- M004-B / Plan 010 — private SAM 3.3, blocked on A + 388; SAM/368 closeout awaits integration to upstream main;
- M004-C / Plan 011 — private data + persistent Destination + Secured, blocked
  on A/B + 385/386/388;
- M004-D / Plan 012 — host-owned Transmission ingress, blocked on A + 387/388.

The historical C003 dependency amendment below remains useful background, but
its broad statement that AppManager/package/process ownership is absent is
superseded by this section.

## Corrective dependency amendment (2026-10-06)

Post-C001 SAM/I2CP research invalidated the assumption that M004 could compose
the existing STREAM-only `SamClient` directly.

The production torrent transport must preserve one I2P Destination across
peer/tracker Streaming and future I2P DHT datagrams. The current client instead
has an incorrect SAM session lifetime model and exposes a placeholder
session-ID-derived local hash. M004 must not encode those defects into the
managed-app runtime.

New hard dependencies:

- i2pr-tc C003:
  `plans/implementation/torrent-client/008-c003-sam33-primary-dht-transport-corrective.md`;
- upstream i2pr Plan 368:
  `plans/implementation/sam/368-sam33-primary-subsession-shared-destination-profile.md`.

The required transport topology after those close is one long-lived SAM 3.3
PRIMARY/shared session with STREAM plus DATAGRAM/RAW children sharing the same
Destination. C003 owns the application client and real local Destination/hash;
Plan 368 owns router-side PRIMARY/subsession semantics over the existing
Streaming and protocol-17/protocol-18 data planes.

Historical C001/C002 closure evidence is retained and not rewritten.

Source roadmap:
`plans/subsystems/torrent-client-roadmap.md#M004--i2pr-managed-app-integration`

Primary class: integration capability + security invariant

Hard dependencies: M002 historical work retained; C003 closed; upstream i2pr Plan 368 closed.

Interface dependencies:

- SAM 3.3 PRIMARY/subsession shared-Destination profile from upstream Plan 368;
- managed app manager/process authentication, package lifecycle, and OS sandbox contract;
- live app-scoped SAM gateway implementing the Plan-368 profile;
- host-owned PublishedLocalService or equivalent for RPC;
- private persistent app-data root semantics.

M003 is a soft dependency for the RPC publication portion.

## Objective

Run i2pr-tc as a secured supervised i2pr native application with no direct host networking while preserving full M002 I2P torrent behavior.

## Upstream contract revalidation

Before implementation, inspect then-current i2pr branch/main and reconcile against managed-app protocol version/directional messages, effective capabilities, SAM service semantics, sandbox attestation, lifecycle/health, package lifecycle, local-service ingress, and persistent-data ownership.

Do not code against stale Plan 345 enum spelling if corrective work changed it.

## Application manifest/capabilities

Request only capabilities actually needed. Expected minimum is SAM/I2P service, health/lifecycle reporting, private persistent data, and local-service publication only when RPC is enabled.

Do not request general brokered clearnet TCP or UnsafeDirect as a normal profile. First-party status does not justify bypassing the capability boundary.

## SAM adaptation

Compose the C003 transport, not the historical C001 STREAM-only shape.

The managed runtime supplies raw `service=sam` logical streams. The app-side
transport owns one long-lived primary session and opens additional raw SAM
connections for its STREAM/DATAGRAM/RAW children according to the Plan-368
contract.

M004 must prove:

- the primary and all children remain inside one app principal;
- the same real Destination/hash is observed by STREAM, DATAGRAM, and RAW;
- no direct host SAM/UDP/TCP socket exists in production;
- no administrator/Proposal-170 credential is required;
- no access to another app's primary/children/resources is possible;
- capability loss tears down the primary and children deterministically;
- restart restores persistent Destination key material through the authorized
  app-private data owner and therefore preserves the node identity when policy
  requests persistence;
- reconnect is bounded and generation-safe.

## Persistent data

Use only the authorized app-private root for the first managed profile. Separate torrent metadata/resume, downloaded payload, temporary/incomplete data, and logs if any. Respect host quota/limits and surface quota exhaustion as typed errors.

Arbitrary user host paths remain unsupported until a scoped filesystem capability exists.

## Transmission ingress

If M003 is closed and PublishedLocalService exists:

- declare a local service descriptor;
- host binds local-only endpoint;
- app receives forwarded streams;
- bound concurrent RPC streams;
- stop/revocation tears down ingress.

The app must not bind loopback itself.

If upstream ingress is unavailable, do not quietly use direct loopback. Split the remaining RPC publication into a successor plan if needed.

## Process lifecycle

Define clean start, crash restart, host stop, router shutdown, update/relaunch, degraded health when SAM is unavailable, restoration of persisted desired state, and bounded graceful flush/stopped announce behavior.

No peer may keep the process alive past host termination authority.

## Security/negative qualification

Closure must prove supported operation with direct network denied, loopback denied, sanitized environment, private filesystem boundary, contained process tree, resource ceilings, and only inherited capability channels.

Attempted direct socket use should fail without breaking supported operation.

## Ordered work packages

WP1 C003 + Plan-368 closure recheck and ADR correction if needed.
WP2 app package/manifest + protocol client.
WP3 SAM 3.3 primary/subsession capability-channel composition.
WP4 persistent data/resource adaptation.
WP5 lifecycle/health/restart.
WP6 host-published Transmission ingress if dependency exists.
WP7 secured-profile negative qualification.

## Acceptance criteria

M004 closes when the managed profile downloads/seeds through the corrected SAM 3.3 shared-Destination capability while host/loopback networking remain denied, the real Destination identity persists/restarts under AppManager policy, STREAM/DATAGRAM/RAW identity parity is demonstrated, and RPC is exposed only through host-owned ingress when that feature is claimed.

## Stop conditions

Stop rather than bypass if C003 or Plan 368 is not positively closed; upstream SAM requires administrator credentials; the shared Destination cannot span STREAM/DATAGRAM/RAW; RPC requires direct loopback; normal operation requires UnsafeDirect; persistence requires arbitrary host filesystem access; or the current managed-app protocol is too incomplete for ownership/cancellation semantics.

Register the missing upstream interface requirement instead.

## Closure evidence

Create `plans/closure/torrent-client/004-status.md` with exact i2pr commit/contract version, manifest grants, sandbox attestation, direct-network negative test, SAM capability evidence, lifecycle/restart matrix, persistent-data evidence, RPC publication evidence if claimed, and unblock audit for M005.
