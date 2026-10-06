# Torrent Client M004 — i2pr Managed-App Integration

Status: blocked on M002 and missing upstream app-runtime interfaces

Source roadmap:
`plans/subsystems/torrent-client-roadmap.md#M004--i2pr-managed-app-integration`

Primary class: integration capability + security invariant

Hard dependency: M002 closed.

Interface dependencies:

- managed app manager/process authentication, package lifecycle, and OS sandbox contract (the corrected protocol and router-side SAM/I2CP gateway are now closed upstream);
- live app-scoped SAM gateway;
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

Bind M002 SamTransport to i2pr's app-scoped service stream.

Prove no direct host SAM socket, no administrator/Proposal 170 credential, no access to another app's resources, deterministic capability loss/revocation behavior, and bounded reconnect following host lifecycle policy.

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

WP1 upstream contract recheck and ADR correction if needed.
WP2 app package/manifest + protocol client.
WP3 SAM capability-channel adapter.
WP4 persistent data/resource adaptation.
WP5 lifecycle/health/restart.
WP6 host-published Transmission ingress if dependency exists.
WP7 secured-profile negative qualification.

## Acceptance criteria

M004 closes when the managed profile downloads/seeds via i2pr SAM capability while host/loopback networking remain denied, persists/restarts under AppManager, and exposes RPC only through host-owned ingress when that feature is claimed.

## Stop conditions

Stop rather than bypass if upstream SAM requires administrator credentials; RPC requires direct loopback; normal operation requires UnsafeDirect; persistence requires arbitrary host filesystem access; or the current managed-app protocol is too incomplete for ownership/cancellation semantics.

Register the missing upstream interface requirement instead.

## Closure evidence

Create `plans/closure/torrent-client/004-status.md` with exact i2pr commit/contract version, manifest grants, sandbox attestation, direct-network negative test, SAM capability evidence, lifecycle/restart matrix, persistent-data evidence, RPC publication evidence if claimed, and unblock audit for M005.
