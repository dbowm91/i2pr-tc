# M003 status report — Transmission RPC compatibility

Status: **closed**

Implementation commits:

- `8c111cf59eaa54366caed8f72fd84347fd6bddba` (`feat: add Transmission RPC compatibility adapter`)
- `85d95002ff01a82333f93ffe7178b007dd8c2689` (`fix: enforce Transmission RPC JSON depth bound`)

## Compatibility target

- Current contract: Transmission RPC 4.1.0+ JSON-RPC 2.0, snake_case, `rpc_version_semver` 6.0.0; upstream spec at commit `48835c6660a7a3730b5a122bb7b88909997addbe`.
- Legacy contract: Transmission 4.0.6 envelope/mixed naming; reference spec commit `38c164933e9f77c110b48fe745861c3b98e3d83e`.
- Interop tool: installed `transmission-remote` 4.1.3, build `838877323f`.
- Both decoders feed the same native adapter and `TorrentRuntime`; no Transmission-specific state was added to the torrent domain.

## Implemented method matrix

| Method | Result/behavior | Limits and truthful boundary |
|---|---|---|
| `session_get` | `version`, `rpc_version_semver` | Other session settings are unsupported. |
| `session_stats` | Torrent/active/paused counts and cumulative downloaded/uploaded bytes | Counts are snapshots; byte totals saturate at `u64::MAX`. No rates/current-vs-cumulative reset model is claimed. |
| `torrent_get` | IDs, v1 hash, name, native-derived status, total/verified/left bytes, completion, metadata completeness, limits, files, wanted size, upload ratio | No synthetic rates, peer counts, ETA, error strings, dates, or tracker state. File list requires resolved metainfo. Ratio is omitted when downloaded bytes are zero. |
| `torrent_add` | Base64 metainfo or magnet URI; duplicate response for an existing infohash; optional paused state | Other filename forms and add options are unsupported. Infohash is parsed/validated before insertion. |
| `torrent_set` | Download/upload limits and file wanted/unwanted/priority updates | Every target and update is validated before mutation; rates convert checked KiB/s to bytes/s. Other settings are rejected. |
| `torrent_start`, `torrent_start_now` | Native start command | No Transmission queue policy exists; start-now has the same truthful native start behavior. |
| `torrent_stop` | Native stop command | Removes active runtime ownership through the native service. |
| `torrent_verify` | Returns after setting native `Checking` state and scheduling bounded blocking verification | At most 16 verification jobs; each request is capped at 16 torrents. A Tokio runtime must own request handling. |
| `torrent_reannounce` | Emits the native reannounce request | Tracker I/O remains with the I2P transport owner; this adapter does not perform a host-network request. |
| `torrent_remove` | Removes selected torrent; optional local payload deletion | Delete-data behavior is delegated to the native storage command. |
| `free_space` | Available bytes from configured canonical data root | Arbitrary path queries are unsupported. |

Current snake_case and supported legacy kebab/camel aliases are covered by inline golden current/legacy request fixtures. The legacy response envelope and key projection use the same results and mutation path.

## Fields and unsupported surface

Projected fields map to native snapshots or validated metainfo. `status` maps native stopped/error/checking/starting/running/completed states to Transmission numeric states. `have_valid`, `percent_done`, and `left_until_done` derive only from verified bytes. `size_when_done` derives from non-low-priority metainfo files. Limits project native bytes/s back to integer KiB/s. `files` exposes native file lengths, verified bytes, and native priorities.

Known fields `added_date`, `error`, `error_string`, `eta`, `peers_getting_from_us`, `peers_sending_to_us`, `rate_download`, and `rate_upload` are accepted by the field-name decoder but omitted because the service does not supply truthful values. Unknown fields and unsupported settings fail closed. Queue position, labels, scripts, alternate-speed schedules, blocklists, port forwarding, encryption policy, bind-address settings, DHT/uTP/LPD, and clearnet settings are not implemented.

## Bounds, persistence, and security evidence

- JSON request and encoded response: 1 MiB each; HTTP headers: 32 KiB and at most 64 headers.
- JSON: at most 64 levels deep, 100,000 nodes, 1,024 object members, 4,096 array entries, 64 KiB strings; duplicate object keys reject.
- Torrent selection: 4,096 IDs; requested fields: 64; stable ID catalog: 100,000 entries / 4 MiB.
- HTTP body length and headers are checked; duplicate content-length/session-token headers and transfer-encoding reject. Session-token mismatch returns HTTP 409 and the host-supplied token. The adapter requires the runtime owner to supply a fresh unpredictable token.
- Transmission integer IDs are monotonically assigned and persisted by atomic catalog replacement. The reopen test proves stable mapping and no ID reuse. They are never array positions.
- Mutating RPC calls serialize in the adapter and then pass through `TorrentRuntime`/`TorrentService`. Native add and remove semantics remain authoritative.
- Production `src` owns no listener or socket. Its stream handler operates only on a caller-supplied async stream. The sole loopback listener is in the test-only `transmission_remote` harness.
- `scripts/check-foundation-boundaries.py --self-test` passes with the Transmission domain name allowed only inside its compatibility crate and host networking/process APIs still rejected.

## Verification

Passed:

- `rtk proxy cargo fmt --all -- --check`
- `rtk proxy cargo test --workspace --all-targets --locked` — 75 tests total: 27 core, 12 I2P transport, 29 storage, 6 RPC unit, and 1 `transmission-remote` integration test.
- `rtk proxy cargo check --workspace --all-targets --locked`
- `rtk proxy cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `rtk proxy cargo doc --workspace --no-deps --locked`
- `rtk proxy cargo deny check` — passed with existing duplicate-`syn` and unused license-allowance warnings.
- `rtk proxy python3 scripts/check-foundation-boundaries.py --self-test`
- `rtk proxy git diff --check`
- Test-only loopback smoke: `transmission-remote http://127.0.0.1:<ephemeral>/transmission --list` succeeded against a seeded native torrent after 409 session-token negotiation and JSON-RPC retry.

The interop smoke verifies the common list/token flow, not complete conformance with every Transmission release or optional RPC method. No production listener or public RPC exposure policy is included; those belong to the host integration.

## Unresolved findings

| Finding | Treatment |
|---|---|
| Transmission fields without native state are omitted; consumers requiring rates, peer counts, ETA, errors, dates, or tracker arrays need a later capability-backed extension. | Explicit unsupported boundary; no synthetic data. |
| `torrent_reannounce` only raises a native request for the I2P transport owner. | Transport-owned asynchronous behavior, no direct host network path. |
| Session token generation and RPC exposure policy are host responsibilities. | M004 integration must provide a fresh token and host-owned ingress. |
| M002 live SAM/router, full metadata exchange, inbound transfer, and reconnect qualification remain outstanding. | M002 remains conditionally closed; this independent M003 closure does not waive it. |

No Transmission compatibility requirement caused a change to canonical TorrentService state or production host networking.

## Unblock audit

Fresh upstream refs checked on 2026-10-06: i2pr `main` `144c54da2eaaa46497955e0e371f06ab6efcd1b1` and `codex/plan-345-native-app-runtime` `ea7b5ccef9bacbddf826f074cc59d891849a1424`. The current main registry has Plan 354 ready and Plan 355 blocked on 354. AppManager/package/process ownership remains unregistered; host-owned app ingress and private persistent-data contracts are absent. No torrent-facing router `ReleaseTarget`, private invocation, or artifact staging/export contract was found.

- M004 remains **blocked** on M002 operational qualification and the missing upstream SAM gateway successor, app runtime/AppManager, ingress, and private-data contracts. Closing M003 removes its soft RPC dependency only.
- M005 remains **blocked** on M002/M004 and router-owned release-target, invocation, and artifact staging/export contracts.
- M006 remains deferred; M003 exposes no datagram/DHT capability.

## Closure recommendation

**Close M003.** The bounded current/legacy adapter interoperates with the qualified local Transmission client for the common list/token path, keeps all mutations in the native runtime, and reports its supported and omitted behavior explicitly. Keep M004 and M005 blocked in the registry; no later implementation plan became unblocked.
