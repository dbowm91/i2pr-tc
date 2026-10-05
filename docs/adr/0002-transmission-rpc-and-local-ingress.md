# ADR 0002: Transmission compatibility is an adapter; local ingress is host-owned

Status: accepted

## Context

Transmission RPC provides compatibility with mature CLI and third-party control tooling. A normal Transmission daemon exposes HTTP on loopback. An i2pr secured managed app is intentionally denied loopback and host networking, so having the torrent process bind 127.0.0.1 would defeat the sandbox model.

## Decision

- i2pr-tc defines a native typed TorrentService independent of Transmission.
- A TransmissionAdapter translates current and supported legacy RPC shapes into TorrentService operations.
- HTTP parsing/serialization is allowed in the adapter, but production endpoint publication is not owned by the app process.
- i2pr/AppManager must own a PublishedLocalService capability: it binds the local endpoint under administrator policy and forwards accepted streams to the app.
- The managed profile exposes RPC only after this capability exists. Direct loopback binding is not a fallback.
- Unsupported Transmission behavior returns truthful errors or omitted capability fields rather than successful no-ops.

## Consequences

RPC development/testing can proceed in-process before the runtime has local ingress. A test-only loopback harness may be used for interoperability but cannot become the production launch path.

The host can enforce bind scope, authentication/exposure policy, connection limits, and lifecycle independently from torrent code.
