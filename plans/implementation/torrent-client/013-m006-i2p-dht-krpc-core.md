# Torrent Client M006-A — I2P DHT KRPC Core

Status: active

Date: 2026-10-10

Parent roadmap phase: future M006 advanced I2P discovery.

Primary class: protocol capability foundation + hostile-input invariant.

Hard dependencies:

- M001 core bencode/infohash foundation closed.
- C003 transport model implemented/conditionally closed.

Operational dependency: none. This plan is deterministic and does not require a
live router.

Behavioral reference:

- current I2PSnark KRPC/DHT implementation, used as an interoperability oracle
  only; do not copy GPL source;
- BEP 5 semantics adapted to the I2P wire profile.

## 1. Objective

Implement the runtime-neutral I2P BitTorrent DHT protocol/state core without
performing network I/O.

M006-A owns:

- bounded KRPC bencode codec;
- I2P compact peer/node forms;
- secure node-id construction/validation;
- transaction/reply tracking model;
- routing buckets / closest-node selection;
- token generation/validation;
- local peer tracker;
- ping/find_node/get_peers/announce_peer request/response state;
- bounded persistence representation for known nodes.

M006-B later binds this core to C003 DATAGRAM/RAW transport.

## 2. I2P BEP-5 profile

Freeze these I2P-specific differences as wire invariants:

- infohash and DHT node id are 20 bytes;
- compact peer = 32-byte SHA-256 I2P Destination hash, no IP/peer port;
- compact node = 54 bytes:
  `20-byte node id || 32-byte Destination hash || 2-byte big-endian query port`;
- node response port is query port + 1 and query port must therefore leave that
  value representable;
- trackerless metainfo `nodes` values are Destination hashes rather than
  clearnet host/port tuples;
- queries use signed/repliable I2P datagrams;
- replies/errors and announce queries use the raw response channel in the
  integration profile;
- no IP SocketAddr appears in canonical DHT state.

Confirm exact current I2PSnark behavior during WP1 and record any divergence
rather than assuming standard BEP-5 UDP encodings.

### WP1 protocol freeze — 2026-10-10

The I2PSnark source was read at repository revision
`37039e6594f1372368f0c1cf8258cdaf805a7fb4` (source paths under
`apps/i2psnark/java/src/org/klomp/snark/dht/`; inspection only, no source is
copied). BEP 5 was read at accepted revision
`aa944d9e2faf989cbb4b1bad5ec130b9f22631d9`.

- I2P peers are exactly 32-byte Destination hashes. Compact nodes are exactly
  54 bytes: 20-byte NID, 32-byte Destination hash, and a big-endian query port
  in `1..=65534`.
- Secure NIDs bind the first four bytes to the Destination hash. Bytes 4 and 5
  XOR the corresponding Destination-hash bytes with the query port's high and
  low bytes. The remaining 14 bytes come from caller-supplied CSPRNG output.
- The I2PSnark query client caches a received get_peers token by remote NID
  alone; its source explicitly leaves one-infohash binding unresolved. The
  outbound cache belongs to M006-B. The server-side core binds authorization to
  both Destination hash and infohash, rejects cross-torrent replay, and expires
  grants. `announce_peer` travels over the raw response channel without a
  trusted signed sender identity, so the token carries the requester hash and
  lets the server recover it. The `id` query field on announce is not identity
  authority.
- The current I2PSnark server issues random eight-byte tokens and stores the
  token-to-requester mapping; it accepts opaque tokens up to 64 bytes and
  expires issued tokens after ten minutes. The plan's rotating-secret token
  design is a deliberate strengthening behind that opaque wire field. The
  implementation uses a 64-byte token containing a four-byte issue time, the
  32-byte requester hash, and a 28-byte truncated HMAC-SHA256. It accepts
  tokens for at most eight minutes and uses current/previous rotating secrets.
  HMAC uses `hmac` with existing `sha2`; tag comparison uses
  `subtle::ConstantTimeEq`; the current and previous key bytes are zeroized on
  drop with `zeroize`. Secret bytes are supplied by the caller.
- I2PSnark sends optional `noseed=1` on get_peers and `seed=0|1` on
  announce_peer; its server accepts seed as optional. The announce `port` is
  explicitly ignored for I2P peers. The codec preserves seed filtering and
  tolerates a missing seed flag; it validates the ignored port only as u16.
- Queries use the signed/repliable channel. Responses and errors, plus
  announce_peer, use the raw response channel. find_node returns at most K=8
  compact nodes. I2PSnark accepts transaction IDs up to 16 bytes.
- I2PSnark reference ceilings include 799 known nodes, 800 tracked torrents,
  150 normally retained peers per torrent (300 temporary insertion ceiling),
  2,000 total peers, 5,000 outgoing tokens, ten-minute issued-token expiry,
  and eight-minute inbound-token use. M006-A will keep explicit equal-or-
  stricter local caps and must not import its GPL implementation.

These differences from generic BEP 5 are recorded as the I2P profile. The
reference implementation is behavioral evidence only; the Rust codec and
state machines are independently authored.

## 3. Secure node id

Independently implement and test the deployed secure-node-id relationship used
by the I2P DHT.

The generated 20-byte id is bound to local Destination hash + query port, with
the remaining entropy generated from a cryptographically secure RNG. Validation
must reject a compact node whose claimed node id does not match its Destination
hash/port relationship.

Do not derive DHT identity from torrent peer id or SAM session id.

## 4. Codec

Add a strict KRPC message model for:

- ping;
- find_node;
- get_peers;
- announce_peer;
- response forms: pong, nodes, peers/token;
- error.

Requirements:

- exact/canonical dictionary keys where protocol requires them;
- bounded input bytes/depth/items/string sizes before secondary allocation;
- transaction-id bound;
- exact node/peer record multiples;
- bounded nodes/peers per response;
- strict 20/32/54-byte field lengths;
- valid port range;
- duplicate/unknown critical-field policy;
- no panic on malformed bencode.

Reuse the existing core bencode parser/encoder primitives where suitable rather
than introduce a second generic bencode implementation.

## 5. Routing table

Implement a bounded Kademlia-style routing structure parameterized by explicit
constants.

At minimum:

- deployed bucket size compatible with I2P DHT behavior (research baseline uses
  K=8);
- XOR distance over 20-byte node ids;
- bounded buckets/nodes;
- deterministic closest-node selection;
- last-seen/failure state;
- eviction only by explicit tested policy;
- local node never inserted as a remote;
- no unbounded retry/blacklist set.

Time is injected for deterministic testing.

## 6. Tokens

Announce authorization tokens must be:

- unpredictable;
- bound to the requesting node/Destination identity and torrent context needed
  by the chosen interoperable profile;
- time-bounded;
- constant-time compared where secret material is involved;
- generated from rotating local secret state;
- bounded in retained state.

WP1 must confirm the deployed I2PSnark token semantics sufficiently for
interoperability without copying implementation code.

Invalid/expired/wrong-node tokens refuse announce and do not mutate peer state.

## 7. Local DHT tracker

Maintain bounded peers per infohash with:

- Destination hash;
- seed/leech state when present in the deployed profile;
- last-seen/expiry;
- self-filtering using C003 real local Destination hash.

No peer Destination private data is stored.

## 8. Persistence

Persist only restart-useful routing bootstrap state, not live transaction/token
authority.

The format must be:

- versioned;
- byte/count bounded;
- atomic;
- tolerant of stale/unreachable nodes;
- strict on malformed structural data;
- rooted below a caller-provided path so M004-C can later place it in app data.

Tokens, in-flight queries, and temporary blacklist state do not survive restart.

## 9. DoS/resource policy

Name ceilings for:

- datagram/KRPC bytes;
- decoded items;
- nodes/peers per message;
- routing nodes/buckets;
- in-flight transactions;
- tokens;
- tracked infohashes;
- peers per infohash;
- blacklist/failure entries;
- persisted bytes.

Parsing and lookup must be linear or bounded by those ceilings.

One unauthenticated query must not force expensive Destination lookup or
unbounded response amplification in the core.

Implementation ceilings (all protocol time values are injected seconds):

| State/input | Bound |
|---|---:|
| KRPC input/output | 65,536 bytes |
| decoded bencode values / nesting | 256 values / depth 8 |
| transaction ID | 1–16 bytes |
| compact nodes per reply / peers per reply | 8 / 200 |
| routing buckets / known nodes | 160 / 799 |
| pending transactions | 256 |
| announce token / retained secret generations | 64 bytes / 2 |
| tracked infohashes / peers per infohash / total peers | 800 / 150 / 2,000 |
| failure state | one saturating counter per retained routing node; 799 max |
| blacklist | none retained by the core |
| persisted bootstrap representation | 43,156 bytes |

## 10. Work packages

WP1 — freeze independent protocol dossier from current I2PSnark + BEP 5.

WP2 — wire types and strict codec.

WP3 — secure node id + compact peer/node representation.

WP4 — routing table and transaction state.

WP5 — token/local tracker state.

WP6 — restart persistence.

WP7 — property/fuzz/DoS tests, docs, closure.

## 11. Acceptance criteria

M006-A closes when:

1. all four KRPC methods and response/error forms have strict bounded codecs;
2. 32-byte peers and 54-byte nodes round-trip exactly;
3. secure node-id generation/validation has independent vectors/properties;
4. routing/token/tracker state is bounded and deterministic under injected time;
5. malformed/adversarial inputs cannot grow state outside named ceilings;
6. persistent bootstrap state round-trips atomically without persisting
   transaction/token authority;
7. no network/router/Tokio dependency is required by the core owner;
8. fuzz/property/routine verification passes.

## 12. Stop conditions

Stop if current deployed I2P DHT behavior contradicts the assumed compact
formats/query-response channel roles, if interoperability requires a
license-derived implementation detail that cannot be independently specified,
or if the core requires router-specific types.

## 13. Closure evidence

Create `plans/closure/torrent-client/013-m006a-status.md` with protocol dossier
pins, format vectors, resource-ceiling table, routing/token properties,
persistence crash tests, fuzz results, and M006-B readiness audit.
