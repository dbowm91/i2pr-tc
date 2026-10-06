# Torrent Client M003 — Transmission RPC Compatibility

Status: closed

## Contract recheck (2026-10-06)

- Current official RPC contract: [`transmission/docs/rpc-spec.md`](https://github.com/transmission/transmission/blob/main/docs/rpc-spec.md) at upstream `main` commit `48835c6660a7a3730b5a122bb7b88909997addbe`.
- Target current wire mode: Transmission 4.1.0+ JSON-RPC 2.0 with snake_case methods/fields; the current contract identifies `rpc_version_semver` 6.0.0 as the semver API version and documents HTTP 409 session-token retry behavior.
- Legacy compatibility reference: the official 4.0.6 RPC specification at commit `38c164933e9f77c110b48fe745861c3b98e3d83e`, covering the older bespoke envelope and mixed kebab/camel naming.
- Local interoperability tools are installed as Transmission 4.1.3 (`transmission-remote` and `transmission-daemon`, build `838877323f`).

Implementation will expose one canonical request model for both wire modes. The method and field matrix will state which current/legacy aliases are accepted and which settings remain explicitly unsupported.

Source roadmap:
`plans/subsystems/torrent-client-roadmap.md#M003--transmission-rpc-compatibility`

Primary class: compatibility capability

Hard dependency: M001 closed with stable native TorrentService.

## Objective

Provide truthful Transmission-compatible RPC without making Transmission naming or HTTP endpoint ownership part of the torrent core.

At implementation start, re-read current upstream Transmission RPC specification and record the exact target/version in closure.

## Wire modes

Support one internal typed request model with adapters for current JSON-RPC/snake_case and the legacy Transmission envelope/naming needed by commonly deployed clients.

Do not maintain two mutation implementations.

Bound JSON depth/members/strings/arrays/torrent IDs/requested fields/response size.

## Initial supported methods

Required baseline: session_get, session_stats, torrent_get, torrent_add, torrent_set for explicitly supported properties, torrent_start, torrent_start_now where truthful, torrent_stop, torrent_verify, torrent_reannounce, torrent_remove, and free_space where storage can answer truthfully.

Do not fake queueing, labels, scripts, alternate-speed schedules, blocklists, port forwarding, encryption, IP bind configuration, DHT/uTP/LPD, or clearnet-specific settings.

## Torrent identity mapping

Transmission integer IDs are compatibility identifiers. Maintain stable local mapping from TorrentId across restart when required. Never use array position as durable identity. Infohash queries map through validated InfoHashV1.

## HTTP/session semantics

HTTP is a stream/service adapter, not a listener owner. Reproduce the Transmission session-token/409 behavior expected by clients. Authentication/exposure policy belongs to M004/i2pr.

A test-only loopback harness is permitted solely for transmission-remote interoperability.

## Field translation

Create an explicit matrix mapping each exposed Transmission field to native source, units, support/derivation/constant/unavailable status. Do not parse human text to recover machine state. Counters/rates/timestamps use checked conversions and documented restart semantics.

## Failure/cancellation/contention

Malformed RPC never mutates state. Batch sizes are bounded if supported. Long verify operations reflect asynchronous native state instead of blocking HTTP indefinitely. Concurrent mutations serialize through TorrentService. Remove/query races return stable not-found semantics. Client disconnect does not undo an already committed native mutation unless explicitly specified.

## Ordered work packages

WP1 freeze upstream matrix/golden corpus.
WP2 current/legacy decoders -> canonical request model.
WP3 handlers over TorrentService.
WP4 field projection + stable RPC IDs.
WP5 stream/HTTP adapter + session-token behavior.
WP6 transmission-remote/third-party harness/docs.

## Required tests

Golden current/legacy requests, duplicate/unknown/oversized JSON, 409 token negotiation, stable ID restart, duplicate add, mutation races, remove data semantics, unsupported settings, field subsets, large-list bounds, and transmission-remote smoke against test-only listener.

## Acceptance criteria

M003 closes when the documented method/field matrix interoperates with qualified Transmission tooling, all mutations go through TorrentService, unsupported features are truthful, and no production listener/host-network requirement was added.

## Stop conditions

Stop if compatibility requires changing canonical torrent state to Transmission-specific semantics, binding a production host socket, or claiming unsupported behavior.

## Closure evidence

Closure record: `plans/closure/torrent-client/003-status.md`.
