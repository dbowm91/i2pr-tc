# ADR 0003: Torrent update distribution is transport, not update authority

Status: accepted

## Context

I2P demonstrates the usefulness of distributing router updates through I2PSnark. BitTorrent provides efficient in-network replication and lets routers seed releases after download.

A torrent infohash and piece hashes are not a software supply-chain trust root. Giving the torrent app install/update authority would move signing, rollback, filesystem, and restart privileges into a larger network-facing process.

## Decision

i2pr owns UpdateAuthority. i2pr-tc owns only UpdateTransport.

The router supplies an authenticated immutable ReleaseTarget. i2pr-tc may download it, enforce caller-supplied size/hash/source bounds, stage/export it through authorized resources, and report completion. The router independently verifies trusted release metadata/signatures/hashes before installation.

The torrent process receives no update signing keys, install-directory write authority, general router administrator credential, or restart authority.

Post-verification seeding may be requested under explicit storage, bandwidth, and retention limits. Torrent distribution is not the sole router recovery/update path.

## Consequences

The torrent update service remains usable if i2pr later chooses a native signed manifest, SU3-compatible metadata, TUF-style metadata, or another release-security format. Only the normalized ReleaseTarget boundary needs to remain stable.

A managed-runtime artifact staging/export capability and private bounded host-to-app invocation are integration dependencies, not permissions to invent inside i2pr-tc.
