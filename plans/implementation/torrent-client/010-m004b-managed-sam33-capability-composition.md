# Torrent Client M004-B — Managed SAM 3.3 Capability Composition

Status: blocked on upstream SAM/368 and Plan 388

Date: 2026-10-09

Parent milestone:
`plans/implementation/torrent-client/004-i2pr-managed-app-integration.md`

Primary class: integration capability + isolation invariant.

Hard dependencies:

- C003 conditionally closed with application transport implemented.
- M004-A closed.
- upstream SAM/368 closed.
- upstream Plan 388 closed.

## 1. Objective

Compose C003's `SamConnectionFactory` with the real managed-app SDK so every
raw SAM connection comes from an authorized app logical `sam` stream, then
execute the C003 SAM 3.3 matrix through i2pr's private managed-app gateway.

This is the criterion-8 completion pass for C003 and the I2P-networking portion
of M004.

## 2. Ownership

The application owns:

- SAM 3.3 client state;
- one primary/shared Destination;
- STREAM/DATAGRAM/RAW child semantics;
- torrent-specific lookup/connect/accept/DHT use.

The host owns:

- app principal authentication;
- effective Sam grant;
- logical-stream multiplexing;
- one private router SAM connection per opened logical stream;
- revocation/teardown.

No torrent operation is added to i2pr's gateway.

## 3. Connection factory

Implement a production `SamConnectionFactory` adapter over the Plan-388 SDK:

```text
SamClient::open_sam_connection()
       |
       v
ManagedAppSession::open_service(Sam)
       |
       v
one logical managed-app stream
       |
       v
one router private SAM connection
```

The adapter must preserve exact ordered bytes and independent close/reset.

It must not know listener addresses or create TCP/Unix sockets.

## 4. Shared-Destination qualification through i2pr

Repeat the C003 matrix through the private managed path:

- HELLO 3.3;
- normative PRIMARY creation;
- real `NAME=ME` Destination/hash;
- STREAM child add/remove;
- DATAGRAM protocol-17 child add/remove;
- RAW protocol-18 child add/remove;
- STREAM CONNECT/ACCEPT where a reachable peer exists;
- protocol-17/18 payload exchange where a reachable peer exists;
- sibling child survives removal of another child;
- primary loss invalidates all children.

Same-Destination identity equality across all children is mandatory.

## 5. App-principal isolation

Using two independently launched fixture/app instances, prove:

- one app cannot attach to the other's primary/session id;
- same textual SAM ids may be reused safely in isolated principal domains;
- revoking/stopping one app does not damage the sibling;
- its open private SAM streams are all closed on revocation;
- DATAGRAM/RAW does not grant host UDP;
- STREAM FORWARD remains unavailable in the secured/private policy.

## 6. Destination key policy seam

M004-B consumes key bytes from an injected application storage abstraction. It
does not choose the host path; M004-C/Plan 385 owns that.

Tests may use ephemeral/in-memory keys. Production activation of persistent
identity remains blocked until M004-C.

No router identity or admin secret is used.

## 7. Failure and restart

Cover:

- grant missing/revoked before open;
- grant revoked with primary active;
- apphost/manager restart;
- router SAM service unavailable;
- primary control EOF;
- stale child after restored generation;
- logical-stream backpressure;
- SDK session cancellation.

No failure may trigger direct-host fallback.

## 8. Work packages

WP1 — implement SDK-backed `SamConnectionFactory`.

WP2 — run deterministic private-gateway SAM 3.3 matrix.

WP3 — cross-app isolation/revocation qualification.

WP4 — live peer/datagram traffic when the execution environment supplies
working I2P transit; retain explicit skip otherwise.

WP5 — close C003 criterion 8 forward and reconcile planning/docs.

## 9. Acceptance criteria

M004-B closes when:

1. all production SAM connections come through managed logical service streams;
2. the shared Destination and STREAM/DATAGRAM/RAW children work through i2pr;
3. cross-app SAM state is isolated;
4. revocation deterministically invalidates the transport;
5. no host SAM/TCP/UDP connector exists in production;
6. C003 acceptance criterion 8 is discharged;
7. routine and private-gateway verification pass.

Live public-network traffic is recorded honestly: required only for any
interoperability claim actually made, never converted from a skip to a pass.

## 10. Stop conditions

Stop if SAM/368 does not preserve the private principal boundary, if protocol
17/18 requires host UDP, if the SDK rewrites SAM payloads, or if persistent
identity would require an arbitrary host path.

## 11. Closure evidence

Create `plans/closure/torrent-client/010-m004b-status.md` with upstream
SAM/368 SHA, SDK version, private-gateway transcript matrix, same-Destination
evidence, cross-app/revocation tests, live/skipped network rows, C003
criterion-8 disposition, and M004 unblock audit.
