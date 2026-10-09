# Torrent Client M004-A — Managed Package, SDK, and Lifecycle Bootstrap

Status: blocked on upstream i2pr Plan 388

Date: 2026-10-09

Parent milestone:
`plans/implementation/torrent-client/004-i2pr-managed-app-integration.md`

Primary class: infrastructure + integration capability.

Hard dependencies:

- M001/M003 closed.
- C003 application-side transport implementation retained/conditionally closed.
- upstream Managed native app runtime/369–371 closed.
- upstream Plans 382–383 closed.
- upstream Plan 388 external Rust managed-app SDK/package builder closed.

Soft/interface dependencies:

- SAM/368 is not required to build/package/launch the process, but is required
  by M004-B before the app can use its final SAM 3.3 production transport.
- Plan 385 private data is not required for bootstrap-only qualification.

## 1. Objective

Turn the existing library backend into an actual managed application package
that can be installed, selected, granted, autostarted, launched, stopped, and
restarted by i2pr's Plan-383 runtime without copying the managed-app protocol
into this repository.

M004-A proves process/package/lifecycle composition only. It does not claim the
Secured profile, final SAM networking, Transmission publication, or update
transport.

## 2. Baseline

The repository currently contains library crates only and no product process
entrypoint. Upstream now has:

- real `i2pr-appd` / `i2pr-apphost` lifecycle;
- signed immutable `.i2prapp` packages;
- persistent publisher trust, exact package selection, Sam/I2cp grants,
  autostart, and restart-safe launch catalog;
- offline `i2pr-appctl`.

The missing cross-repository seam is upstream Plan 388: a supported external
application SDK and package builder. Do not solve that by vendoring
`i2pr-app-proto`, copying its JSON/framing types, or pinning a private path
crate.

## 3. Application composition root

Add a thin application binary/crate, preferably `i2pr-tc-app`, that owns:

- managed-app SDK session bootstrap over inherited stdin/stdout;
- configuration parsing limited to app-owned settings;
- construction of the existing TorrentService/storage/I2P/RPC components;
- lifecycle cancellation and graceful shutdown;
- health/degraded-state projection;
- no direct router/admin protocol implementation.

Library crates stay usable independently.

The binary must not introduce a production host-network connector. Existing
declared live-test connectors remain test-only.

## 4. Package manifest

Build a signed `.i2prapp` using the Plan-388 builder.

Initial requested capabilities:

- `sam`;
- lifecycle/health semantics only if the SDK contract models them as explicit
  requested capabilities.

Do not request:

- I2CP merely because it exists;
- `brokered_tcp`;
- `control_scoped`;
- UI;
- future local-service ingress until M004-D.

The manifest contains one exact platform entrypoint per supported packaged
target and no installer hook or localhost URL.

## 5. Launch behavior before M004-B

The process must be able to start and remain well-defined when final SAM 3.3
service capability is not yet available.

Allowed behavior:

- initialize local state and protocol session;
- report I2P transport unavailable/degraded;
- accept lifecycle stop;
- avoid reconnect storms.

It must not silently fall back to a host SAM socket.

For black-box upstream Plan-383 lifecycle qualification, use the existing
explicitly uncontained test/UnsafeDirect profile only to exercise process
composition. That evidence is **not** a production security claim and the app
must still contain no direct networking path.

## 6. Configuration and logs

- stdin/stdout belong exclusively to managed-app protocol framing.
- diagnostics go to stderr through bounded, privacy-aware logging.
- no Destination/private key, tracker payload, peer payload, or package signing
  key is logged.
- configuration never contains router administrator credentials.
- unknown configuration keys fail closed.

Remove the residual production debug print
`eprintln!("DBG got remote handshake")` discovered after C003 closure and add a
guard preventing equivalent ad-hoc stdout/stderr protocol-path prints.

## 7. Lifecycle semantics

Define and test:

- first launch;
- operator stop;
- router/appd shutdown;
- app process crash;
- appd restart/autostart;
- package version replacement followed by explicit selection;
- rejected/tampered package;
- missing SAM service while process remains manageable;
- cancellation while local storage work is in flight.

Plan 383 does not auto-restart an app after the app itself exits; M004-A must not
invent a private crash-loop policy.

## 8. Work packages

WP1 — consume upstream Plan-388 SDK/package APIs at a pinned compatible version.

WP2 — add `i2pr-tc-app` composition root and managed lifecycle.

WP3 — add package manifest and deterministic package build/release fixture.

WP4 — install/trust/select/grant/autostart through real `i2pr-appctl` and
Plan-383 runtime.

WP5 — lifecycle/restart/degraded-SAM tests and cleanup of the residual debug
print.

WP6 — docs, guards, and closure record.

## 9. Verification

In addition to the repository floor:

- package builds and verifies with the upstream canonical verifier;
- an installed package launches through real appd/apphost;
- hello/accept and stop complete through the SDK;
- restart/autostart produces a fresh app instance id;
- no direct host network call exists in production targets;
- stdout contains only protocol bytes;
- negative package/trust/grant cases fail without partial launch.

## 10. Acceptance criteria

M004-A closes when:

1. i2pr-tc ships a real managed-app process entrypoint;
2. a signed package is built by the supported upstream developer surface;
3. real Plan-383 install/trust/select/grant/autostart launches it;
4. the process completes managed-app handshake and lifecycle cleanly;
5. restart relaunches approved state with a fresh launch identity;
6. unavailable SAM degrades without host-network fallback or reconnect storm;
7. the residual debug print is removed and guarded;
8. full verification passes.

## 11. Stop conditions

Stop if Plan 388 still requires private workspace/path dependencies, if package
construction requires copied signing-format code, if process startup requires a
host SAM socket, or if stdout cannot remain exclusively managed-app protocol.

## 12. Closure evidence

Create `plans/closure/torrent-client/009-m004a-status.md` with upstream crate
versions/SHAs, package identity, manifest grants, real appd/apphost lifecycle
matrix, restart evidence, no-host-network guard results, logging cleanup, and
unblock audit for M004-B/C/D.
