# Torrent Client M005 — Router Update Artifact Transport

Status: blocked on M002/M004 and router update interfaces

Source roadmap:
`plans/subsystems/torrent-client-roadmap.md#M005--router-update-artifact-transport`

Relevant ADR: 0003

Primary class: integration capability + supply-chain trust invariant

Hard dependencies: M002 and M004 closed.

Interface dependencies:

- i2pr router-owned normalized ReleaseTarget contract;
- private host-to-app request/reply/service invocation;
- capability-mediated artifact staging/export;
- router-side final verification/install owner.

## Objective

Let i2pr use i2pr-tc as one in-network transport for authenticated router release artifacts without granting the torrent app release trust or installation authority.

## ReleaseTarget input

The router, after authenticating its own release metadata, supplies a bounded immutable target description equivalent to target/release identifier, version, platform/architecture if relevant, expected byte length, authoritative cryptographic target digest, torrent source, optional allowed tracker/source set, request deadline/resource limits, and optional post-download seeding policy.

Exact wire type belongs to the integration contract.

The torrent app does not fetch unsigned "latest version" metadata and decide which router version should run.

## Fetch behavior

For an update request:

1. validate target/request bounds;
2. add/resolve the requested torrent without broadening network policy;
3. require one expected artifact or explicitly defined path within signed target set;
4. enforce maximum length before/through download;
5. perform normal torrent piece verification;
6. compute the caller-requested target digest after completion;
7. expose completed bytes through authorized staging/export;
8. return typed observed length/digest/torrent identity;
9. never install, unpack into router directories, restart, or promote version state.

Digest mismatch is a hard failure and cannot be seed-promoted as an accepted release.

## Router-side verification boundary

The router independently verifies its update security requirements after receiving the artifact.

Tests must show a torrent-complete file with wrong target digest, wrong version metadata, untrusted signing metadata, or rollback/freeze-invalid version cannot become installable merely because i2pr-tc returned bytes. Router policy may use a fake authority fixture if updater code is still evolving; i2pr-tc itself must not implement those policies.

## Artifact handoff

Do not exchange arbitrary filesystem paths as authority.

Use current managed-runtime staging/export with request/app binding, bounded size/quota, random-access support or safe final export, cleanup on failure/cancel, deterministic crash behavior, and router read/verify ability without app install-dir writes.

## Seeding policy

After router acceptance it may request bounded seeding. Support current + optional previous release retention, storage ceiling, upload bandwidth/priority, retention age/ratio, explicit stop/revoke, and safe cleanup.

Before router acceptance, ordinary BitTorrent uploading behavior must be explicitly documented. Torrent piece verification must never be described as trusted router release verification.

## Availability/recovery

Torrent is an alternate distribution transport, not the sole update path. Router startup/update recovery must not depend on this app being installed and healthy.

Multiple first-party I2P seeders/trackers and PEX may improve availability, but their identity is not a replacement for signed metadata.

## Failure/cancellation/restart

Duplicate request IDs are idempotent or explicitly rejected. Cancellation stops update-owned intent without deleting unrelated user ownership of the same torrent. User and update ownership of one infohash are separate leases. Restart correlation follows the host contract or is safely retried. Deadline expiry cannot leave privileged staged artifacts indefinitely. Export failure cannot mark update complete. Seeding persists only by explicit host policy.

## Ordered work packages

WP1 freeze ReleaseTarget + invocation + artifact handoff.
WP2 update-owned torrent lease/reference semantics.
WP3 bounded fetch, length/digest verification, staged result.
WP4 cancellation/restart/idempotency.
WP5 accepted-release seeding retention.
WP6 malicious/mismatch/recovery qualification.

## Required tests

Valid target; wrong infohash/metainfo; target too large/length mismatch; final digest mismatch despite torrent completion; cancellation at metadata/piece/finalize/export; app crash/retry; user+update ownership; concurrent duplicate target requests; staging quota exhaustion; router rejection after transport success; no app install path; seed acceptance/retention/bandwidth/cleanup; and independent router recovery path evidence.

## Acceptance criteria

M005 closes only when a router-owned authenticated target can be fetched and returned through the managed capability boundary, router independently decides trust/installability, and no app credential/filesystem/API can install or restart the router.

## Stop conditions

Stop if implementation requires administrator/update signing credentials in the app, app-selected trusted latest version, arbitrary install-directory writes, public Transmission RPC as privileged update control plane, BitTorrent verification as release authentication, or torrent as sole recovery route.

## Closure evidence

Create `plans/closure/torrent-client/005-status.md` with exact i2pr updater contract commit, ReleaseTarget schema, request/artifact ownership matrix, mismatch/security tests, crash/cancel/idempotency evidence, seed-retention evidence, proof of no install authority, and final foundational-workstream closure audit.
