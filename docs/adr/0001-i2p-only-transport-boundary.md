# ADR 0001: Production torrent networking is I2P-only and capability-mediated

Status: accepted

## Context

A general BitTorrent stack commonly assumes IP addresses, direct TCP/UDP sockets, DNS, DHT, LPD, UPnP/NAT-PMP, and fallback paths. Retaining those capabilities in an anonymity-network-specific client creates accidental clearnet leakage and makes security depend on configuration.

The i2pr managed-app design intends to deny direct host networking and provide router services through scoped capability channels.

## Decision

- Canonical peer addresses are I2P Destination/Destination-hash values.
- Production tracker, peer, naming, metadata, PEX, and later DHT traffic uses I2P only.
- Managed production code does not directly create host network sockets, including loopback.
- SAM is consumed through an injected transport; i2pr's production adapter supplies that transport through the app-scoped capability channel.
- Standalone direct-to-local-router connectors, if useful for development/interoperability tests, are explicit non-production adapters and cannot be selected silently by the managed profile.
- Clearnet trackers/peers, IP DHT, LPD, UPnP/NAT-PMP, and automatic fallback are outside the production feature set.

## Consequences

Many general-purpose torrent engines cannot be used wholesale without carrying the wrong address/network abstractions. The project prefers a smaller native engine and reusable low-level components.

Network containment is necessary but does not make arbitrary app traffic anonymous; protocol/state minimization and stable identity policy remain security concerns.
