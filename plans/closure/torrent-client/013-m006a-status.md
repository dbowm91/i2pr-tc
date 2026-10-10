# Torrent Client M006-A — I2P DHT KRPC Core Closure

Status: **closed**

Date: 2026-10-10

Plan: `plans/implementation/torrent-client/013-m006-i2p-dht-krpc-core.md`

Implementation commit: `abe4fbb967aeefef160b249a564dfe0a40ae7eb6` — bounded
runtime-neutral DHT core, storage snapshot, fuzz target/corpus, protocol freeze,
and implementation documentation.

Closure recommendation: **closed**. All M006-A acceptance criteria are met.
M006-B is promoted to ready after the unblock audit below. This closure makes no
claim of live DHT network interoperability; that belongs to M006-B.

## 1. Delivered

- Strict bounded KRPC query/reply/error codec for the frozen I2P profile,
  including exact compact-peer and compact-node encodings.
- Secure node IDs, bounded XOR routing buckets and failure state, bounded
  transaction tracking/expiry, and deterministic caller-supplied entropy.
- Seed-aware bounded local peer tracker and HMAC announce-token issue/verify
  with requester binding, expiry, key rotation, constant-time tag comparison,
  and zeroized key material.
- Query transitions that require externally authenticated signed-query identity;
  raw announce identity is recovered only from a valid token, never from the
  untrusted announce `id` field.
- Versioned bounded bootstrap snapshot serialization and rooted atomic storage.
  Transaction and token state is intentionally not persisted.
- DHT KRPC fuzz target with curated seeds; boundary allowlist recognizes only
  the added cryptographic dependencies.

The independent protocol freeze records I2PSnark revision
`37039e6594f1372368f0c1cf8258cdaf805a7fb4` and accepted BEP 5 revision
`aa944d9e2faf989cbb4b1bad5ec130b9f22631d9` in Plan 013. I2PSnark was consulted
as behavioral evidence; no GPL implementation code was copied.

## 2. Requirement-to-evidence matrix

| Requirement | Evidence |
|---|---|
| Exact bounded I2P KRPC forms | `crates/i2pr-tc-core/src/dht.rs` codec tests: strict query/reply/error forms, unknown fields, lengths, and bencode limits. |
| 32-byte peers, 54-byte nodes, secure IDs | DHT compact-format and deterministic secure-ID vector tests. |
| Bounded routing, closest order, and failure state | Routing capacity, replacement/eviction, failure, and XOR ordering tests. |
| Bounded transactions and expiry | Transaction book capacity, duplicate handling, lookup/removal/expiry, and exact-capacity tests. |
| Token authenticity, requester/torrent binding, expiry, and key rotation | Announce-token and DhtCore transition tests, including malformed/cross-requester/cross-infohash rejection and no tracker mutation on invalid authorization. |
| Seed-aware bounded local tracker | Tracker tests cover filtering, seed selection, deduplication, per-torrent and global caps. |
| Bounded bootstrap encoding and persistence | Snapshot codec bounds plus `dht_bootstrap_snapshot_is_bounded_rooted_and_atomically_replaced`; simulated interrupted write retains the previous snapshot. |
| Hostile-input robustness | `dht_krpc` libFuzzer target; two recorded runs completed 3,223,864 and 1,157,942 executions without crash or finding. |
| No I/O/runtime ownership in core | Core crate boundary check and `scripts/check-foundation-boundaries.py --self-test`. |

## 3. Verification

All commands below passed on the implementation commit unless otherwise stated:

```text
cargo fmt --all --check
cargo check --locked --workspace --all-targets
cargo test --locked --workspace --all-targets -- --test-threads=1
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps
cargo test --locked --workspace --doc
cargo deny check advisories bans licenses sources
python3 scripts/check-foundation-boundaries.py --self-test
cargo check --locked --manifest-path fuzz/Cargo.toml --bins
cargo +nightly fuzz run dht_krpc -- -max_total_time=30
```

The two fuzz runs used the target's curated seed corpus and found no issue.
`cargo deny` exited successfully with pre-existing warnings for unmatched
license allowances and duplicate `syn`; no dependency-policy failure was
reported. `git diff --check` passed before commit.

## 4. Compatibility, lifecycle, and security disposition

- Wire differences from generic BEP 5 and the current I2PSnark profile are
  recorded in Plan 013. I2PSnark's lack of infohash binding in its outbound
  token cache is not copied: this core binds issued authorization to requester
  and infohash.
- KRPC uses I2P Destination hashes and ports; no host IP endpoint, UDP socket,
  clearnet bootstrap, DNS, or network connector was added.
- Core time and entropy are caller supplied; the core owns no runtime, task,
  socket, or ambient randomness.
- Token secrets are caller supplied, key generations are zeroized on drop, and
  tag comparison is constant time. Raw announce sender authority comes from the
  token, not its query `id`.
- Persistence is bounded, rooted, symlink checked, and atomically replaced.
  Restart state is limited to validated routing bootstrap nodes; ephemeral
  transaction and token state does not survive restart.
- Fault evidence covers invalid tokens, bounded-state rejection, expiry, and
  interruption of snapshot replacement. Async cancellation is not applicable
  to this runtime-neutral core; transport task lifecycle is M006-B scope.

No unresolved M006-A security or correctness finding remains. Live router
interoperability, iterative network behavior, and async cancellation remain
explicit M006-B work rather than implied by this core closure.

## 5. Unblock audit

| Successor | Result | Reason |
|---|---|---|
| M006-B / Plan 014 | **unblocked; ready** | M006-A is closed and C003 remains conditionally closed with the shared-Destination STREAM/DATAGRAM/RAW transport implementation retained. Its outstanding upstream managed-gateway criterion is not needed to develop against Java I2P. Live transit remains an operational dependency for final live evidence, not a coding prerequisite. |
| M004-B / Plan 010 | **still blocked** | M004-A and upstream Plan 388 remain hard dependencies. SAM/368 implementation and closeout exist in the separate i2pr worktree but are not integrated to upstream `main`; no readiness promotion is justified. |
| M004-A / Plan 009 | **still blocked** | Upstream Plan 388 external SDK/package builder remains outstanding. |
| M004-C / Plan 011 | **still blocked** | Depends on M004-A/B and upstream Plans 385/386/388. |
| M004-D / Plan 012 | **still blocked** | Depends on M004-A and upstream Plans 387/388. |
| M005 / Plan 005 | **still blocked** | Router-owned release target, private invocation, and artifact staging/export contracts remain absent. |

## 6. Closure record

Closure recommendation: **closed**. The successor handoff is Plan 014, promoted
to `ready`. The Java I2P live network database/transit outage recorded in C003
remains an operational constraint for M006-B's live evidence; it does not
weaken deterministic acceptance or permit any host-network fallback.
