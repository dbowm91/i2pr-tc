//! Runtime-neutral, bounded I2P DHT wire identities and compact records.
//!
//! This module intentionally contains no router, socket, or random-number
//! generator ownership. Callers supply secure entropy when creating identities.
use crate::bencode::{self, Value};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};
use subtle::ConstantTimeEq;
use thiserror::Error;
use zeroize::Zeroize;

pub const INFO_HASH_LEN: usize = 20;
pub const NODE_ID_LEN: usize = 20;
pub const DEST_HASH_LEN: usize = 32;
pub const COMPACT_NODE_LEN: usize = 54;
pub const COMPACT_PEER_LEN: usize = 32;
pub const K: usize = 8;
pub const MAX_KRPC_BYTES: usize = 64 * 1024;
pub const MAX_TRANSACTION_ID: usize = 16;
pub const MAX_TOKEN_BYTES: usize = 64;
pub const MAX_PEERS_PER_REPLY: usize = 200;
pub const MAX_NODES_PER_REPLY: usize = K;
pub const MAX_ROUTING_NODES: usize = 799;
pub const MAX_ROUTING_BUCKETS: usize = NODE_ID_LEN * 8;
pub const MAX_IN_FLIGHT_TRANSACTIONS: usize = 256;
pub const MAX_NODE_FAILURE_ENTRIES: usize = MAX_ROUTING_NODES;
pub const MAX_TRACKED_TORRENTS: usize = 800;
pub const MAX_PEERS_PER_TORRENT: usize = 150;
pub const MAX_TRACKED_PEERS: usize = 2_000;
pub const TOKEN_ISSUE_LIFETIME_SECS: u64 = 10 * 60;
pub const TOKEN_ACCEPT_LIFETIME_SECS: u64 = 8 * 60;
pub const TOKEN_LEN: usize = MAX_TOKEN_BYTES;
const TOKEN_TAG_LEN: usize = 28;
const PERSIST_MAGIC: &[u8; 8] = b"I2PDHT01";
pub const MAX_PERSISTED_BYTES: usize = 8 + 2 + MAX_ROUTING_NODES * COMPACT_NODE_LEN;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub [u8; NODE_ID_LEN]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DestinationHash(pub [u8; DEST_HASH_LEN]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InfoHash(pub [u8; INFO_HASH_LEN]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactNode {
    pub id: NodeId,
    pub destination: DestinationHash,
    /// SAM query port. The corresponding raw response port is query_port + 1.
    pub query_port: u16,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DhtError {
    #[error("invalid DHT compact record")]
    Invalid,
    #[error("DHT resource limit exceeded")]
    Limit,
    #[error("invalid bounded KRPC message")]
    Krpc,
    #[error("bencode decode failed: {0}")]
    Bencode(#[from] bencode::Error),
    #[error("DHT state capacity reached")]
    Capacity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Query {
    Ping {
        id: NodeId,
    },
    FindNode {
        id: NodeId,
        target: NodeId,
    },
    GetPeers {
        id: NodeId,
        info_hash: InfoHash,
        noseed: bool,
    },
    AnnouncePeer {
        id: NodeId,
        info_hash: InfoHash,
        token: Vec<u8>,
        port: u16,
        seed: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    Pong {
        id: NodeId,
    },
    Nodes {
        id: NodeId,
        nodes: Vec<CompactNode>,
    },
    Peers {
        id: NodeId,
        token: Vec<u8>,
        peers: Vec<DestinationHash>,
        nodes: Vec<CompactNode>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KrpcMessage {
    Query {
        transaction: Vec<u8>,
        query: Query,
    },
    Reply {
        transaction: Vec<u8>,
        reply: Reply,
    },
    Error {
        transaction: Vec<u8>,
        code: i64,
        message: Vec<u8>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoutingEntry {
    pub node: CompactNode,
    pub last_seen: u64,
    pub failures: u8,
}

/// Fixed-capacity Kademlia routing table. Full buckets reject new nodes until
/// the caller explicitly removes an expired or failed entry.
pub struct RoutingTable {
    local: NodeId,
    buckets: Vec<Vec<RoutingEntry>>,
    len: usize,
}
impl RoutingTable {
    pub fn new(local: NodeId) -> Self {
        Self {
            local,
            buckets: (0..MAX_ROUTING_BUCKETS)
                .map(|_| Vec::with_capacity(K))
                .collect(),
            len: 0,
        }
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn insert_or_refresh(&mut self, node: CompactNode, now: u64) -> Result<(), DhtError> {
        node.validate()?;
        if node.id == self.local {
            return Err(DhtError::Invalid);
        }
        let bucket = bucket_index(self.local, node.id).ok_or(DhtError::Invalid)?;
        if let Some(entry) = self.buckets[bucket]
            .iter_mut()
            .find(|entry| entry.node.id == node.id)
        {
            entry.node = node;
            entry.last_seen = now;
            entry.failures = 0;
            return Ok(());
        }
        if self.buckets[bucket].len() >= K || self.len >= MAX_ROUTING_NODES {
            return Err(DhtError::Capacity);
        }
        self.buckets[bucket].push(RoutingEntry {
            node,
            last_seen: now,
            failures: 0,
        });
        self.len += 1;
        Ok(())
    }
    pub fn mark_failure(&mut self, id: NodeId) -> bool {
        let Some(index) = bucket_index(self.local, id) else {
            return false;
        };
        let bucket = &mut self.buckets[index];
        if let Some(pos) = bucket.iter().position(|entry| entry.node.id == id) {
            bucket[pos].failures = bucket[pos].failures.saturating_add(1);
            if bucket[pos].failures >= 3 {
                bucket.remove(pos);
                self.len -= 1;
            }
            true
        } else {
            false
        }
    }
    pub fn remove_expired(&mut self, now: u64, max_age: u64) -> usize {
        let mut removed = 0;
        for bucket in &mut self.buckets {
            let before = bucket.len();
            bucket.retain(|entry| now.saturating_sub(entry.last_seen) <= max_age);
            removed += before - bucket.len();
        }
        self.len -= removed;
        removed
    }
    pub fn closest(&self, target: NodeId, limit: usize) -> Vec<CompactNode> {
        let mut nodes = self
            .buckets
            .iter()
            .flat_map(|bucket| bucket.iter())
            .map(|entry| entry.node)
            .collect::<Vec<_>>();
        nodes.sort_by_key(|node| xor_distance(node.id, target));
        nodes.truncate(limit.min(K));
        nodes
    }
    pub fn entries(&self) -> impl Iterator<Item = RoutingEntry> + '_ {
        self.buckets
            .iter()
            .flat_map(|bucket| bucket.iter().copied())
    }
}

fn bucket_index(local: NodeId, remote: NodeId) -> Option<usize> {
    for (byte_index, (&a, &b)) in local.0.iter().zip(&remote.0).enumerate() {
        let different = a ^ b;
        if different != 0 {
            return Some(byte_index * 8 + different.leading_zeros() as usize);
        }
    }
    None
}
fn xor_distance(left: NodeId, right: NodeId) -> [u8; NODE_ID_LEN] {
    std::array::from_fn(|index| left.0[index] ^ right.0[index])
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingTransaction {
    pub expires_at: u64,
}
pub struct TransactionBook {
    pending: BTreeMap<Vec<u8>, PendingTransaction>,
    capacity: usize,
}
impl TransactionBook {
    pub fn new(capacity: usize) -> Result<Self, DhtError> {
        if capacity == 0 || capacity > MAX_IN_FLIGHT_TRANSACTIONS {
            return Err(DhtError::Limit);
        }
        Ok(Self {
            pending: BTreeMap::new(),
            capacity,
        })
    }
    pub fn begin(&mut self, transaction: Vec<u8>, now: u64, timeout: u64) -> Result<(), DhtError> {
        if transaction.is_empty() || transaction.len() > MAX_TRANSACTION_ID || timeout == 0 {
            return Err(DhtError::Invalid);
        }
        self.expire(now);
        if self.pending.contains_key(&transaction) {
            return Err(DhtError::Invalid);
        }
        if self.pending.len() >= self.capacity {
            return Err(DhtError::Capacity);
        }
        self.pending.insert(
            transaction,
            PendingTransaction {
                expires_at: now.saturating_add(timeout),
            },
        );
        Ok(())
    }
    pub fn complete(&mut self, transaction: &[u8], now: u64) -> bool {
        self.expire(now);
        self.pending.remove(transaction).is_some()
    }
    pub fn expire(&mut self, now: u64) -> usize {
        let before = self.pending.len();
        self.pending.retain(|_, pending| pending.expires_at > now);
        before - self.pending.len()
    }
    pub fn len(&self) -> usize {
        self.pending.len()
    }
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrackedPeer {
    pub destination: DestinationHash,
    pub seed: bool,
    pub last_seen: u64,
}
pub struct LocalTracker {
    self_destination: DestinationHash,
    torrents: BTreeMap<InfoHash, BTreeMap<DestinationHash, TrackedPeer>>,
    peers: usize,
}
impl LocalTracker {
    pub fn new(self_destination: DestinationHash) -> Self {
        Self {
            self_destination,
            torrents: BTreeMap::new(),
            peers: 0,
        }
    }
    pub fn announce(
        &mut self,
        info_hash: InfoHash,
        destination: DestinationHash,
        seed: bool,
        now: u64,
    ) -> Result<bool, DhtError> {
        if destination == self.self_destination {
            return Ok(false);
        }
        self.expire(now, 3 * 60 * 60);
        if let Some(existing) = self
            .torrents
            .get_mut(&info_hash)
            .and_then(|peers| peers.get_mut(&destination))
        {
            *existing = TrackedPeer {
                destination,
                seed,
                last_seen: now,
            };
            return Ok(true);
        }
        if !self.torrents.contains_key(&info_hash) && self.torrents.len() >= MAX_TRACKED_TORRENTS {
            return Err(DhtError::Capacity);
        }
        if self
            .torrents
            .get(&info_hash)
            .is_some_and(|peers| peers.len() >= MAX_PEERS_PER_TORRENT)
            || self.peers >= MAX_TRACKED_PEERS
        {
            return Err(DhtError::Capacity);
        }
        self.torrents.entry(info_hash).or_default().insert(
            destination,
            TrackedPeer {
                destination,
                seed,
                last_seen: now,
            },
        );
        self.peers += 1;
        Ok(true)
    }
    pub fn peers(&self, info_hash: InfoHash, noseed: bool, max: usize) -> Vec<TrackedPeer> {
        self.torrents
            .get(&info_hash)
            .into_iter()
            .flat_map(|peers| peers.values())
            .filter(|peer| !noseed || !peer.seed)
            .take(max.min(MAX_PEERS_PER_TORRENT))
            .copied()
            .collect()
    }
    pub fn expire(&mut self, now: u64, max_age: u64) -> usize {
        let mut removed = 0;
        self.torrents.retain(|_, peers| {
            let before = peers.len();
            peers.retain(|_, peer| now.saturating_sub(peer.last_seen) <= max_age);
            removed += before - peers.len();
            !peers.is_empty()
        });
        self.peers -= removed;
        removed
    }
    pub fn counts(&self) -> (usize, usize) {
        (self.torrents.len(), self.peers)
    }
}

type HmacSha256 = Hmac<Sha256>;
/// Rotating-secret announce authority. Secure secret creation/rotation bytes
/// are supplied by the process boundary, never generated by this crate.
pub struct TokenAuthority {
    current: [u8; 32],
    current_since: u64,
    previous: Option<([u8; 32], u64)>,
}
impl TokenAuthority {
    pub fn new(initial_secret: [u8; 32], now: u64) -> Self {
        Self {
            current: initial_secret,
            current_since: now,
            previous: None,
        }
    }
    pub fn rotate(&mut self, next_secret: [u8; 32], now: u64) -> Result<(), DhtError> {
        if now <= self.current_since || now - self.current_since < TOKEN_ISSUE_LIFETIME_SECS {
            return Err(DhtError::Invalid);
        }
        self.previous = Some((self.current, now.saturating_add(TOKEN_ACCEPT_LIFETIME_SECS)));
        self.current = next_secret;
        self.current_since = now;
        Ok(())
    }
    pub fn issue(
        &self,
        destination: DestinationHash,
        info_hash: InfoHash,
        now: u64,
    ) -> Result<[u8; TOKEN_LEN], DhtError> {
        if now < self.current_since || now - self.current_since >= TOKEN_ISSUE_LIFETIME_SECS {
            return Err(DhtError::Invalid);
        }
        let issued = u32::try_from(now).map_err(|_| DhtError::Limit)?;
        let mut token = [0; TOKEN_LEN];
        token[..4].copy_from_slice(&issued.to_be_bytes());
        token[4..36].copy_from_slice(&destination.0);
        let mut mac = HmacSha256::new_from_slice(&self.current).map_err(|_| DhtError::Invalid)?;
        mac.update(&token[4..36]);
        mac.update(&info_hash.0);
        mac.update(&token[..4]);
        token[36..].copy_from_slice(&mac.finalize().into_bytes()[..TOKEN_TAG_LEN]);
        Ok(token)
    }
    pub fn validate(&self, token: &[u8], info_hash: InfoHash, now: u64) -> Option<DestinationHash> {
        if token.len() != TOKEN_LEN {
            return None;
        }
        let issued = u32::from_be_bytes(match token[..4].try_into() {
            Ok(value) => value,
            Err(_) => return None,
        });
        let issued = u64::from(issued);
        if issued > now || now - issued > TOKEN_ACCEPT_LIFETIME_SECS {
            return None;
        }
        let secret = if issued >= self.current_since {
            &self.current
        } else if let Some((previous, valid_until)) = self.previous.as_ref() {
            if now > *valid_until {
                return None;
            }
            previous
        } else {
            return None;
        };
        let destination = DestinationHash(match token[4..36].try_into() {
            Ok(value) => value,
            Err(_) => return None,
        });
        let mut mac = match HmacSha256::new_from_slice(secret) {
            Ok(mac) => mac,
            Err(_) => return None,
        };
        mac.update(&destination.0);
        mac.update(&info_hash.0);
        mac.update(&token[..4]);
        let expected = mac.finalize().into_bytes();
        if bool::from(expected[..TOKEN_TAG_LEN].ct_eq(&token[36..])) {
            Some(destination)
        } else {
            None
        }
    }
}
impl Drop for TokenAuthority {
    fn drop(&mut self) {
        self.current.zeroize();
        if let Some((secret, _)) = self.previous.as_mut() {
            secret.zeroize();
        }
    }
}

/// Deterministic KRPC state owner. Transport supplies the authenticated sender
/// node for signed/repliable queries; announce_peer identity comes only from
/// its validated opaque token.
pub struct DhtCore {
    local: CompactNode,
    routing: RoutingTable,
    tracker: LocalTracker,
    tokens: TokenAuthority,
}
impl DhtCore {
    pub fn new(local: CompactNode, token_secret: [u8; 32], now: u64) -> Result<Self, DhtError> {
        local.validate()?;
        Ok(Self {
            local,
            routing: RoutingTable::new(local.id),
            tracker: LocalTracker::new(local.destination),
            tokens: TokenAuthority::new(token_secret, now),
        })
    }
    pub fn rotate_token_secret(&mut self, next_secret: [u8; 32], now: u64) -> Result<(), DhtError> {
        self.tokens.rotate(next_secret, now)
    }
    pub fn handle_query(
        &mut self,
        query: Query,
        authenticated_sender: Option<CompactNode>,
        now: u64,
    ) -> Result<Reply, DhtError> {
        match query {
            Query::AnnouncePeer {
                info_hash,
                token,
                seed,
                ..
            } => {
                let destination = self
                    .tokens
                    .validate(&token, info_hash, now)
                    .ok_or(DhtError::Invalid)?;
                self.tracker.announce(info_hash, destination, seed, now)?;
                Ok(Reply::Pong { id: self.local.id })
            }
            query => {
                let sender = authenticated_sender.ok_or(DhtError::Invalid)?;
                sender.validate()?;
                let (claimed_id, reply) = match query {
                    Query::Ping { id } => (id, Reply::Pong { id: self.local.id }),
                    Query::FindNode { id, target } => (
                        id,
                        Reply::Nodes {
                            id: self.local.id,
                            nodes: self.routing.closest(target, K),
                        },
                    ),
                    Query::GetPeers {
                        id,
                        info_hash,
                        noseed,
                    } => {
                        let token = self
                            .tokens
                            .issue(sender.destination, info_hash, now)?
                            .to_vec();
                        let peers = self
                            .tracker
                            .peers(info_hash, noseed, MAX_PEERS_PER_REPLY)
                            .into_iter()
                            .map(|peer| peer.destination)
                            .collect::<Vec<_>>();
                        let nodes = if peers.is_empty() {
                            self.routing.closest(NodeId(info_hash.0), K)
                        } else {
                            Vec::new()
                        };
                        (
                            id,
                            Reply::Peers {
                                id: self.local.id,
                                token,
                                peers,
                                nodes,
                            },
                        )
                    }
                    Query::AnnouncePeer { .. } => return Err(DhtError::Invalid),
                };
                if sender.id != claimed_id {
                    return Err(DhtError::Invalid);
                }
                match self.routing.insert_or_refresh(sender, now) {
                    Ok(()) | Err(DhtError::Capacity) => {}
                    Err(error) => return Err(error),
                }
                Ok(reply)
            }
        }
    }
    pub fn routing(&self) -> &RoutingTable {
        &self.routing
    }
    pub fn tracker(&self) -> &LocalTracker {
        &self.tracker
    }
    pub fn bootstrap_snapshot(&self) -> BootstrapSnapshot {
        BootstrapSnapshot {
            nodes: self.routing.entries().map(|entry| entry.node).collect(),
        }
    }
}

/// Restart-useful routing bootstrap data only. Transactions, tokens and
/// temporary failures deliberately have no representation here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapSnapshot {
    pub nodes: Vec<CompactNode>,
}
impl BootstrapSnapshot {
    pub fn encode(&self) -> Result<Vec<u8>, DhtError> {
        if self.nodes.len() > MAX_ROUTING_NODES {
            return Err(DhtError::Limit);
        }
        let mut seen = BTreeSet::new();
        let mut out = Vec::with_capacity(10 + self.nodes.len() * COMPACT_NODE_LEN);
        out.extend_from_slice(PERSIST_MAGIC);
        out.extend_from_slice(&(self.nodes.len() as u16).to_be_bytes());
        let mut ordered = self.nodes.clone();
        ordered.sort_by_key(|node| node.id);
        for node in &ordered {
            if !seen.insert(node.id) {
                return Err(DhtError::Invalid);
            }
            out.extend_from_slice(&node.encode()?);
        }
        if out.len() > MAX_PERSISTED_BYTES {
            return Err(DhtError::Limit);
        }
        Ok(out)
    }
    pub fn decode(input: &[u8]) -> Result<Self, DhtError> {
        if input.len() > MAX_PERSISTED_BYTES || input.len() < 10 || &input[..8] != PERSIST_MAGIC {
            return Err(DhtError::Invalid);
        }
        let count = u16::from_be_bytes([input[8], input[9]]) as usize;
        if count > MAX_ROUTING_NODES || input.len() != 10 + count * COMPACT_NODE_LEN {
            return Err(DhtError::Invalid);
        }
        let mut seen = BTreeSet::new();
        let mut nodes = Vec::with_capacity(count);
        for record in input[10..].chunks_exact(COMPACT_NODE_LEN) {
            let node = CompactNode::decode(record)?;
            if !seen.insert(node.id) {
                return Err(DhtError::Invalid);
            }
            nodes.push(node);
        }
        Ok(Self { nodes })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct KrpcLimits {
    pub message_bytes: usize,
    pub transaction_bytes: usize,
    pub token_bytes: usize,
    pub peers: usize,
    pub nodes: usize,
}
impl KrpcLimits {
    fn bounded(self) -> Self {
        Self {
            message_bytes: self.message_bytes.min(MAX_KRPC_BYTES),
            transaction_bytes: self.transaction_bytes.min(MAX_TRANSACTION_ID),
            token_bytes: self.token_bytes.min(MAX_TOKEN_BYTES),
            peers: self.peers.min(MAX_PEERS_PER_REPLY),
            nodes: self.nodes.min(MAX_NODES_PER_REPLY),
        }
    }
}
impl Default for KrpcLimits {
    fn default() -> Self {
        Self {
            message_bytes: MAX_KRPC_BYTES,
            transaction_bytes: MAX_TRANSACTION_ID,
            token_bytes: MAX_TOKEN_BYTES,
            peers: MAX_PEERS_PER_REPLY,
            nodes: MAX_NODES_PER_REPLY,
        }
    }
}

impl KrpcMessage {
    pub fn decode(input: &[u8], limits: KrpcLimits) -> Result<Self, DhtError> {
        let limits = limits.bounded();
        if input.len() > limits.message_bytes || limits.transaction_bytes == 0 {
            return Err(DhtError::Limit);
        }
        let root = bencode::parse(
            input,
            bencode::Limits {
                input: limits.message_bytes,
                depth: 8,
                items: 256,
                string: limits.message_bytes,
            },
        )?;
        let fields = dict(&root)?;
        let transaction = bytes_field(&root, input, b"t")?;
        if transaction.is_empty() || transaction.len() > limits.transaction_bytes {
            return Err(DhtError::Limit);
        }
        let kind = bytes_field(&root, input, b"y")?;
        match kind {
            b"q" => {
                require_keys(fields, input, &[b"a", b"q", b"t", b"y"])?;
                let name = bytes_field(&root, input, b"q")?;
                let args = bencode::dict_get(&root, input, b"a").ok_or(DhtError::Krpc)?;
                let args_fields = dict(args)?;
                let id = node_id_field(args, input, b"id")?;
                let query = match name {
                    b"ping" => {
                        require_keys(args_fields, input, &[b"id"])?;
                        Query::Ping { id }
                    }
                    b"find_node" => {
                        require_keys(args_fields, input, &[b"id", b"target"])?;
                        Query::FindNode {
                            id,
                            target: NodeId(fixed_field(args, input, b"target")?),
                        }
                    }
                    b"get_peers" => {
                        if args_fields.len() != 2 && args_fields.len() != 3 {
                            return Err(DhtError::Krpc);
                        }
                        for (key, _) in args_fields {
                            if ![b"id".as_slice(), b"info_hash", b"noseed"]
                                .contains(&&input[key.clone()])
                            {
                                return Err(DhtError::Krpc);
                            }
                        }
                        let noseed = match bencode::dict_get(args, input, b"noseed") {
                            None | Some(Value::Integer(0)) => false,
                            Some(Value::Integer(1)) => true,
                            _ => return Err(DhtError::Krpc),
                        };
                        Query::GetPeers {
                            id,
                            info_hash: InfoHash(fixed_field(args, input, b"info_hash")?),
                            noseed,
                        }
                    }
                    b"announce_peer" => {
                        if args_fields.len() != 4 && args_fields.len() != 5 {
                            return Err(DhtError::Krpc);
                        }
                        for (key, _) in args_fields {
                            let key = &input[key.clone()];
                            if ![b"id".as_slice(), b"info_hash", b"port", b"token", b"seed"]
                                .contains(&key)
                            {
                                return Err(DhtError::Krpc);
                            }
                        }
                        for required in [b"id".as_slice(), b"info_hash", b"port", b"token"] {
                            if bencode::dict_get(args, input, required).is_none() {
                                return Err(DhtError::Krpc);
                            }
                        }
                        let token = bytes_field(args, input, b"token")?.to_vec();
                        // This field is retained for BEP-5 wire compatibility;
                        // I2PSnark explicitly ignores it on the I2P profile.
                        let port = int_field(args, input, b"port")?;
                        let seed = match bencode::dict_get(args, input, b"seed") {
                            None => false,
                            Some(Value::Integer(1)) => true,
                            Some(Value::Integer(0)) => false,
                            _ => return Err(DhtError::Krpc),
                        };
                        if token.is_empty()
                            || token.len() > limits.token_bytes
                            || !(0..=u16::MAX as i64).contains(&port)
                        {
                            return Err(DhtError::Krpc);
                        }
                        Query::AnnouncePeer {
                            id,
                            info_hash: InfoHash(fixed_field(args, input, b"info_hash")?),
                            token,
                            port: port as u16,
                            seed,
                        }
                    }
                    _ => return Err(DhtError::Krpc),
                };
                Ok(Self::Query {
                    transaction: transaction.to_vec(),
                    query,
                })
            }
            b"r" => {
                require_keys(fields, input, &[b"r", b"t", b"y"])?;
                let body = bencode::dict_get(&root, input, b"r").ok_or(DhtError::Krpc)?;
                let body_fields = dict(body)?;
                let id = node_id_field(body, input, b"id")?;
                let reply = if body_fields.len() == 1 {
                    Reply::Pong { id }
                } else if body_fields.len() == 2
                    && bencode::dict_get(body, input, b"nodes").is_some()
                {
                    let bytes = bytes_field(body, input, b"nodes")?;
                    Reply::Nodes {
                        id,
                        nodes: decode_compact_nodes(bytes, limits.nodes)?,
                    }
                } else if bencode::dict_get(body, input, b"token").is_some() {
                    let token = bytes_field(body, input, b"token")?.to_vec();
                    if token.is_empty() || token.len() > limits.token_bytes {
                        return Err(DhtError::Krpc);
                    }
                    let peers = match bencode::dict_get(body, input, b"values") {
                        Some(Value::List(values)) if values.len() <= limits.peers => values
                            .iter()
                            .map(|value| {
                                let peer: [u8; DEST_HASH_LEN] = bytes(value, input)?
                                    .try_into()
                                    .map_err(|_| DhtError::Krpc)?;
                                Ok(DestinationHash(peer))
                            })
                            .collect::<Result<Vec<_>, DhtError>>()?,
                        None => Vec::new(),
                        _ => return Err(DhtError::Krpc),
                    };
                    let nodes = match bencode::dict_get(body, input, b"nodes") {
                        Some(value) => decode_compact_nodes(bytes(value, input)?, limits.nodes)?,
                        None => Vec::new(),
                    };
                    if body_fields.iter().any(|(key, _)| {
                        let key = &input[key.clone()];
                        ![b"id".as_slice(), b"token", b"values", b"nodes"].contains(&key)
                    }) {
                        return Err(DhtError::Krpc);
                    }
                    Reply::Peers {
                        id,
                        token,
                        peers,
                        nodes,
                    }
                } else {
                    return Err(DhtError::Krpc);
                };
                Ok(Self::Reply {
                    transaction: transaction.to_vec(),
                    reply,
                })
            }
            b"e" => {
                require_keys(fields, input, &[b"e", b"t", b"y"])?;
                let Value::List(items) =
                    bencode::dict_get(&root, input, b"e").ok_or(DhtError::Krpc)?
                else {
                    return Err(DhtError::Krpc);
                };
                if items.len() != 2 {
                    return Err(DhtError::Krpc);
                }
                let Value::Integer(code) = items[0] else {
                    return Err(DhtError::Krpc);
                };
                let message = bytes(&items[1], input)?.to_vec();
                if message.len() > 256 {
                    return Err(DhtError::Limit);
                }
                Ok(Self::Error {
                    transaction: transaction.to_vec(),
                    code,
                    message,
                })
            }
            _ => Err(DhtError::Krpc),
        }
    }

    pub fn encode(&self, limits: KrpcLimits) -> Result<Vec<u8>, DhtError> {
        let limits = limits.bounded();
        let mut out = Vec::new();
        match self {
            Self::Query { transaction, query } => {
                check_transaction(transaction, limits)?;
                let (name, id, extra): QueryEncoding<'_> = match query {
                    Query::Ping { id } => (b"ping", id, vec![]),
                    Query::FindNode { id, target } => (
                        b"find_node",
                        id,
                        vec![(b"target", BValue::Bytes(target.0.to_vec()))],
                    ),
                    Query::GetPeers {
                        id,
                        info_hash,
                        noseed,
                    } => {
                        let mut entries =
                            vec![(b"info_hash".as_slice(), BValue::Bytes(info_hash.0.to_vec()))];
                        if *noseed {
                            entries.push((b"noseed", BValue::Int(1)));
                        }
                        (b"get_peers", id, entries)
                    }
                    Query::AnnouncePeer {
                        id,
                        info_hash,
                        token,
                        port,
                        seed,
                    } => {
                        if token.is_empty() || token.len() > limits.token_bytes {
                            return Err(DhtError::Krpc);
                        }
                        let e = vec![
                            (b"info_hash".as_slice(), BValue::Bytes(info_hash.0.to_vec())),
                            (b"port", BValue::Int(*port as i64)),
                            (b"seed", BValue::Int(i64::from(*seed))),
                            (b"token", BValue::Bytes(token.clone())),
                        ];
                        (b"announce_peer", id, e)
                    }
                };
                start_dict(&mut out);
                key_dict(&mut out, b"a", |out| {
                    // Bencode keys must be emitted in lexical order.
                    let mut entries = extra;
                    entries.push((b"id", BValue::Bytes(id.0.to_vec())));
                    entries.sort_by(|a, b| a.0.cmp(b.0));
                    encode_entries(out, &entries);
                });
                key_bytes(&mut out, b"q", name);
                key_bytes(&mut out, b"t", transaction);
                key_bytes(&mut out, b"y", b"q");
                end_dict(&mut out);
            }
            Self::Reply { transaction, reply } => {
                check_transaction(transaction, limits)?;
                match reply {
                    Reply::Nodes { nodes, .. } => {
                        encode_compact_nodes(nodes, limits.nodes)?;
                    }
                    Reply::Peers {
                        token,
                        peers,
                        nodes,
                        ..
                    } => {
                        if token.is_empty() || token.len() > limits.token_bytes {
                            return Err(DhtError::Limit);
                        }
                        encode_compact_peers(peers, limits.peers)?;
                        encode_compact_nodes(nodes, limits.nodes)?;
                    }
                    Reply::Pong { .. } => {}
                }
                start_dict(&mut out);
                key_dict(&mut out, b"r", |out| match reply {
                    Reply::Pong { id } => {
                        key_bytes(out, b"id", &id.0);
                    }
                    Reply::Nodes { id, nodes } => {
                        let packed = encode_compact_nodes(nodes, limits.nodes)
                            .expect("validated reply nodes");
                        key_bytes(out, b"id", &id.0);
                        key_bytes(out, b"nodes", &packed);
                    }
                    Reply::Peers {
                        id,
                        token,
                        peers,
                        nodes,
                    } => {
                        let packed_nodes = encode_compact_nodes(nodes, limits.nodes)
                            .expect("validated reply nodes");
                        key_bytes(out, b"id", &id.0);
                        if !packed_nodes.is_empty() {
                            key_bytes(out, b"nodes", &packed_nodes);
                        }
                        key_bytes(out, b"token", token);
                        if !peers.is_empty() {
                            bytes_value(out, b"values");
                            start_list(out);
                            for peer in peers {
                                bytes_value(out, &peer.0);
                            }
                            end_list(out);
                        }
                    }
                });
                key_bytes(&mut out, b"t", transaction);
                key_bytes(&mut out, b"y", b"r");
                end_dict(&mut out);
            }
            Self::Error {
                transaction,
                code,
                message,
            } => {
                check_transaction(transaction, limits)?;
                if message.len() > 256 {
                    return Err(DhtError::Limit);
                }
                start_dict(&mut out);
                key_list(&mut out, b"e", |out| {
                    integer(out, *code);
                    bytes_value(out, message);
                });
                key_bytes(&mut out, b"t", transaction);
                key_bytes(&mut out, b"y", b"e");
                end_dict(&mut out);
            }
        }
        if out.len() > limits.message_bytes {
            return Err(DhtError::Limit);
        }
        Ok(out)
    }
}

#[derive(Clone)]
enum BValue {
    Bytes(Vec<u8>),
    Int(i64),
}
type QueryEncoding<'a> = (&'static [u8], &'a NodeId, Vec<(&'static [u8], BValue)>);

fn dict(value: &Value) -> Result<&[(std::ops::Range<usize>, Value)], DhtError> {
    if let Value::Dict(items) = value {
        Ok(items)
    } else {
        Err(DhtError::Krpc)
    }
}
fn require_keys(
    fields: &[(std::ops::Range<usize>, Value)],
    input: &[u8],
    expected: &[&[u8]],
) -> Result<(), DhtError> {
    if fields.len() != expected.len() {
        return Err(DhtError::Krpc);
    }
    let mut names = expected.to_vec();
    names.sort();
    if fields
        .iter()
        .zip(names)
        .all(|((range, _), name)| &input[range.clone()] == name)
    {
        Ok(())
    } else {
        Err(DhtError::Krpc)
    }
}
fn bytes<'a>(value: &'a Value, input: &'a [u8]) -> Result<&'a [u8], DhtError> {
    bencode::bytes(value, input).ok_or(DhtError::Krpc)
}
fn bytes_field<'a>(value: &'a Value, input: &'a [u8], key: &[u8]) -> Result<&'a [u8], DhtError> {
    bytes(
        bencode::dict_get(value, input, key).ok_or(DhtError::Krpc)?,
        input,
    )
}
fn int_field(value: &Value, input: &[u8], key: &[u8]) -> Result<i64, DhtError> {
    match bencode::dict_get(value, input, key) {
        Some(Value::Integer(n)) => Ok(*n),
        _ => Err(DhtError::Krpc),
    }
}
fn fixed_field<const N: usize>(
    value: &Value,
    input: &[u8],
    key: &[u8],
) -> Result<[u8; N], DhtError> {
    bytes_field(value, input, key)?
        .try_into()
        .map_err(|_| DhtError::Krpc)
}
fn node_id_field(value: &Value, input: &[u8], key: &[u8]) -> Result<NodeId, DhtError> {
    Ok(NodeId(fixed_field(value, input, key)?))
}
fn check_transaction(tx: &[u8], limits: KrpcLimits) -> Result<(), DhtError> {
    if tx.is_empty() || tx.len() > limits.transaction_bytes || tx.len() > MAX_TRANSACTION_ID {
        Err(DhtError::Limit)
    } else {
        Ok(())
    }
}
fn start_dict(out: &mut Vec<u8>) {
    out.push(b'd');
}
fn end_dict(out: &mut Vec<u8>) {
    out.push(b'e');
}
fn start_list(out: &mut Vec<u8>) {
    out.push(b'l');
}
fn end_list(out: &mut Vec<u8>) {
    out.push(b'e');
}
fn bytes_value(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(value.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(value);
}
fn key_bytes(out: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    bytes_value(out, key);
    bytes_value(out, value);
}
fn integer(out: &mut Vec<u8>, value: i64) {
    out.push(b'i');
    out.extend_from_slice(value.to_string().as_bytes());
    out.push(b'e');
}
fn key_dict(out: &mut Vec<u8>, key: &[u8], f: impl FnOnce(&mut Vec<u8>)) {
    bytes_value(out, key);
    start_dict(out);
    f(out);
    end_dict(out);
}
fn key_list(out: &mut Vec<u8>, key: &[u8], f: impl FnOnce(&mut Vec<u8>)) {
    bytes_value(out, key);
    start_list(out);
    f(out);
    end_list(out);
}
fn encode_entries(out: &mut Vec<u8>, entries: &[(&[u8], BValue)]) {
    for (key, value) in entries {
        bytes_value(out, key);
        match value {
            BValue::Bytes(b) => bytes_value(out, b),
            BValue::Int(n) => integer(out, *n),
        }
    }
}

impl NodeId {
    /// Construct the deployed secure node ID from a caller-supplied 14-byte
    /// CSPRNG value. Entropy acquisition is kept outside this core crate.
    pub fn from_destination(
        destination: DestinationHash,
        query_port: u16,
        random_tail: [u8; 14],
    ) -> Result<Self, DhtError> {
        validate_query_port(query_port)?;
        let mut id = [0; NODE_ID_LEN];
        id[..4].copy_from_slice(&destination.0[..4]);
        id[4] = destination.0[4] ^ (query_port >> 8) as u8;
        id[5] = destination.0[5] ^ query_port as u8;
        id[6..].copy_from_slice(&random_tail);
        Ok(Self(id))
    }

    pub fn validates(self, destination: DestinationHash, query_port: u16) -> bool {
        validate_query_port(query_port).is_ok()
            && self.0[..4] == destination.0[..4]
            && self.0[4] == destination.0[4] ^ (query_port >> 8) as u8
            && self.0[5] == destination.0[5] ^ query_port as u8
    }
}

impl CompactNode {
    pub fn validate(self) -> Result<Self, DhtError> {
        validate_query_port(self.query_port)?;
        if !self.id.validates(self.destination, self.query_port) {
            return Err(DhtError::Invalid);
        }
        Ok(self)
    }

    pub fn encode(self) -> Result<[u8; COMPACT_NODE_LEN], DhtError> {
        self.validate()?;
        let mut out = [0; COMPACT_NODE_LEN];
        out[..NODE_ID_LEN].copy_from_slice(&self.id.0);
        out[NODE_ID_LEN..NODE_ID_LEN + DEST_HASH_LEN].copy_from_slice(&self.destination.0);
        out[NODE_ID_LEN + DEST_HASH_LEN..].copy_from_slice(&self.query_port.to_be_bytes());
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DhtError> {
        if bytes.len() != COMPACT_NODE_LEN {
            return Err(DhtError::Invalid);
        }
        let node = Self {
            id: NodeId(
                bytes[..NODE_ID_LEN]
                    .try_into()
                    .map_err(|_| DhtError::Invalid)?,
            ),
            destination: DestinationHash(
                bytes[NODE_ID_LEN..NODE_ID_LEN + DEST_HASH_LEN]
                    .try_into()
                    .map_err(|_| DhtError::Invalid)?,
            ),
            query_port: u16::from_be_bytes(
                bytes[NODE_ID_LEN + DEST_HASH_LEN..]
                    .try_into()
                    .map_err(|_| DhtError::Invalid)?,
            ),
        };
        node.validate()
    }
}

pub fn encode_compact_peers(peers: &[DestinationHash], max: usize) -> Result<Vec<u8>, DhtError> {
    if peers.len() > max {
        return Err(DhtError::Limit);
    }
    let size = peers
        .len()
        .checked_mul(COMPACT_PEER_LEN)
        .ok_or(DhtError::Limit)?;
    let mut out = Vec::with_capacity(size);
    for peer in peers {
        out.extend_from_slice(&peer.0);
    }
    Ok(out)
}

pub fn decode_compact_peers(bytes: &[u8], max: usize) -> Result<Vec<DestinationHash>, DhtError> {
    if bytes.len() % COMPACT_PEER_LEN != 0 || bytes.len() / COMPACT_PEER_LEN > max {
        return Err(DhtError::Invalid);
    }
    bytes
        .chunks_exact(COMPACT_PEER_LEN)
        .map(|chunk| {
            chunk
                .try_into()
                .map(DestinationHash)
                .map_err(|_| DhtError::Invalid)
        })
        .collect::<Result<Vec<_>, DhtError>>()
}

pub fn encode_compact_nodes(nodes: &[CompactNode], max: usize) -> Result<Vec<u8>, DhtError> {
    if nodes.len() > max {
        return Err(DhtError::Limit);
    }
    let size = nodes
        .len()
        .checked_mul(COMPACT_NODE_LEN)
        .ok_or(DhtError::Limit)?;
    let mut out = Vec::with_capacity(size);
    for node in nodes {
        out.extend_from_slice(&node.encode()?);
    }
    Ok(out)
}

pub fn decode_compact_nodes(bytes: &[u8], max: usize) -> Result<Vec<CompactNode>, DhtError> {
    if bytes.len() % COMPACT_NODE_LEN != 0 || bytes.len() / COMPACT_NODE_LEN > max {
        return Err(DhtError::Invalid);
    }
    bytes
        .chunks_exact(COMPACT_NODE_LEN)
        .map(CompactNode::decode)
        .collect()
}

fn validate_query_port(port: u16) -> Result<(), DhtError> {
    if (1..=65_534).contains(&port) {
        Ok(())
    } else {
        Err(DhtError::Invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(port: u16, seed: u8) -> CompactNode {
        let destination = DestinationHash([seed; DEST_HASH_LEN]);
        CompactNode {
            id: NodeId::from_destination(destination, port, [seed.wrapping_add(1); 14]).unwrap(),
            destination,
            query_port: port,
        }
    }

    #[test]
    fn secure_id_binds_destination_and_query_port() {
        let good = node(12_345, 4);
        assert!(good.id.validates(good.destination, good.query_port));
        assert!(!good.id.validates(DestinationHash([5; 32]), good.query_port));
        assert!(!good.id.validates(good.destination, good.query_port + 1));
        assert!(NodeId::from_destination(good.destination, 0, [0; 14]).is_err());
        assert!(NodeId::from_destination(good.destination, u16::MAX, [0; 14]).is_err());
        let destination = DestinationHash(std::array::from_fn(|i| i as u8));
        let vector = NodeId::from_destination(destination, 0x1234, [0xaa; 14]).unwrap();
        assert_eq!(&vector.0[..6], &[0, 1, 2, 3, 0x16, 0x31]);
        assert_eq!(&vector.0[6..], &[0xaa; 14]);
    }

    #[test]
    fn compact_nodes_require_exact_secure_54_byte_records() {
        let nodes = [node(1, 1), node(65_534, 2)];
        let encoded = encode_compact_nodes(&nodes, 8).unwrap();
        assert_eq!(encoded.len(), 108);
        assert_eq!(decode_compact_nodes(&encoded, 8).unwrap(), nodes);
        assert!(decode_compact_nodes(&encoded[..107], 8).is_err());
        assert!(encode_compact_nodes(&nodes, 1).is_err());
        let mut invalid = nodes[0].encode().unwrap();
        invalid[20 + 32..].copy_from_slice(&0u16.to_be_bytes());
        assert!(CompactNode::decode(&invalid).is_err());
    }

    #[test]
    fn compact_peer_records_are_bounded_and_exact() {
        let peers = [DestinationHash([3; 32]), DestinationHash([9; 32])];
        let bytes = encode_compact_peers(&peers, 2).unwrap();
        assert_eq!(decode_compact_peers(&bytes, 2).unwrap(), peers);
        assert!(decode_compact_peers(&bytes[..63], 2).is_err());
        assert!(encode_compact_peers(&peers, 1).is_err());
    }

    #[test]
    fn krpc_query_and_reply_forms_round_trip_as_canonical_bencode() {
        let id = NodeId([1; 20]);
        let tx = b"tx01".to_vec();
        let queries = [
            Query::Ping { id },
            Query::FindNode {
                id,
                target: NodeId([2; 20]),
            },
            Query::GetPeers {
                id,
                info_hash: InfoHash([3; 20]),
                noseed: true,
            },
            Query::AnnouncePeer {
                id,
                info_hash: InfoHash([4; 20]),
                token: vec![5; 8],
                port: 6881,
                seed: true,
            },
        ];
        for query in queries {
            let message = KrpcMessage::Query {
                transaction: tx.clone(),
                query,
            };
            let encoded = message.encode(KrpcLimits::default()).unwrap();
            assert_eq!(
                KrpcMessage::decode(&encoded, KrpcLimits::default()).unwrap(),
                message
            );
        }
        let peers = vec![DestinationHash([7; 32]), DestinationHash([8; 32])];
        let reply = KrpcMessage::Reply {
            transaction: tx,
            reply: Reply::Peers {
                id,
                token: vec![9; 8],
                peers,
                nodes: vec![node(1234, 5)],
            },
        };
        let encoded = reply.encode(KrpcLimits::default()).unwrap();
        assert_eq!(
            KrpcMessage::decode(&encoded, KrpcLimits::default()).unwrap(),
            reply
        );
        for reply in [
            Reply::Pong { id },
            Reply::Nodes {
                id,
                nodes: vec![node(2345, 6)],
            },
        ] {
            let message = KrpcMessage::Reply {
                transaction: b"pong".to_vec(),
                reply,
            };
            let encoded = message.encode(KrpcLimits::default()).unwrap();
            assert_eq!(
                KrpcMessage::decode(&encoded, KrpcLimits::default()).unwrap(),
                message
            );
        }
        let error = KrpcMessage::Error {
            transaction: b"err".to_vec(),
            code: 203,
            message: b"invalid token".to_vec(),
        };
        let encoded = error.encode(KrpcLimits::default()).unwrap();
        assert_eq!(
            KrpcMessage::decode(&encoded, KrpcLimits::default()).unwrap(),
            error
        );
    }

    #[test]
    fn krpc_rejects_unknown_fields_wrong_lengths_and_oversized_transactions() {
        let valid = KrpcMessage::Query {
            transaction: b"t".to_vec(),
            query: Query::Ping {
                id: NodeId([1; 20]),
            },
        }
        .encode(KrpcLimits::default())
        .unwrap();
        assert!(KrpcMessage::decode(b"d1:ai1e1:t1:x1:y1:qe", KrpcLimits::default()).is_err());
        let mut oversized_tx = valid.clone();
        // Canonically replace transaction `1:t1:t` with a 17-byte value.
        oversized_tx.splice(
            oversized_tx
                .windows(6)
                .position(|w| w == b"1:t1:t")
                .unwrap()
                ..oversized_tx
                    .windows(6)
                    .position(|w| w == b"1:t1:t")
                    .unwrap()
                    + 6,
            b"1:t17:12345678901234567".iter().copied(),
        );
        assert!(KrpcMessage::decode(&oversized_tx, KrpcLimits::default()).is_err());
        assert!(
            KrpcMessage::decode(
                &valid,
                KrpcLimits {
                    message_bytes: 3,
                    ..KrpcLimits::default()
                }
            )
            .is_err()
        );
    }

    #[test]
    fn routing_table_is_bounded_deterministic_and_requires_explicit_eviction() {
        let local = NodeId([0; 20]);
        let mut table = RoutingTable::new(local);
        for seed in 1..=K as u8 {
            table
                .insert_or_refresh(node_for_id(0x80, seed, 1000 + seed as u16), seed as u64)
                .unwrap();
        }
        let overflow = node_for_id(0x80, 99, 2000);
        assert_eq!(
            table.insert_or_refresh(overflow, 99),
            Err(DhtError::Capacity)
        );
        assert_eq!(table.len(), K);
        let refreshed = table.entries().next().unwrap().node;
        table.insert_or_refresh(refreshed, 200).unwrap();
        assert_eq!(
            table
                .entries()
                .find(|e| e.node.id == refreshed.id)
                .unwrap()
                .last_seen,
            200
        );
        let closest = table.closest(NodeId([0x80; 20]), 8);
        let mut expected = closest.clone();
        expected.sort_by_key(|node| xor_distance(node.id, NodeId([0x80; 20])));
        assert_eq!(closest, expected);
        assert!(
            table
                .insert_or_refresh(
                    CompactNode {
                        id: local,
                        destination: DestinationHash([0; 32]),
                        query_port: 1
                    },
                    0
                )
                .is_err()
        );
        let failed_id = table.entries().next().unwrap().node.id;
        assert!(table.mark_failure(failed_id));
        assert!(table.mark_failure(failed_id));
        assert!(table.mark_failure(failed_id));
        assert_eq!(table.len(), K - 1);
        assert_eq!(table.remove_expired(500, 100), K - 1);
        assert!(table.is_empty());
    }

    fn node_for_id(prefix: u8, tail: u8, port: u16) -> CompactNode {
        let mut dest = [tail; 32];
        dest[0] = prefix;
        let destination = DestinationHash(dest);
        CompactNode {
            id: NodeId::from_destination(destination, port, [tail; 14]).unwrap(),
            destination,
            query_port: port,
        }
    }

    #[test]
    fn transactions_expire_and_do_not_overrun_capacity() {
        let mut book = TransactionBook::new(1).unwrap();
        book.begin(b"a".to_vec(), 10, 5).unwrap();
        assert_eq!(book.begin(b"b".to_vec(), 10, 5), Err(DhtError::Capacity));
        assert!(!book.complete(b"a", 15));
        assert_eq!(book.len(), 0);
        assert!(TransactionBook::new(MAX_IN_FLIGHT_TRANSACTIONS + 1).is_err());
        let mut full = TransactionBook::new(MAX_IN_FLIGHT_TRANSACTIONS).unwrap();
        for id in 0..MAX_IN_FLIGHT_TRANSACTIONS {
            full.begin((id as u16).to_be_bytes().to_vec(), 1, 60)
                .unwrap();
        }
        assert_eq!(full.len(), MAX_IN_FLIGHT_TRANSACTIONS);
        assert_eq!(
            full.begin(b"overflow".to_vec(), 1, 60),
            Err(DhtError::Capacity)
        );
        book.begin(b"b".to_vec(), 20, 10).unwrap();
        assert!(book.complete(b"b", 21));
        assert_eq!(book.len(), 0);
    }

    #[test]
    fn announce_tokens_bind_identity_torrent_and_lifetime_across_rotation() {
        let destination = DestinationHash([6; 32]);
        let info_hash = InfoHash([7; 20]);
        let mut authority = TokenAuthority::new([1; 32], 100);
        let token = authority.issue(destination, info_hash, 300).unwrap();
        assert_eq!(&token[..4], &[0, 0, 1, 44]);
        assert_eq!(
            &token[36..],
            &[
                0x1c, 0x7a, 0x19, 0x97, 0x20, 0x29, 0xfe, 0xc7, 0xb8, 0x76, 0x01, 0xfd, 0x23, 0x50,
                0xcf, 0xcf, 0x03, 0xc9, 0x83, 0xfb, 0x2e, 0x52, 0x6c, 0xee, 0x37, 0xc2, 0x4a, 0xe4,
            ]
        );
        assert_eq!(
            authority.validate(&token, info_hash, 400),
            Some(destination)
        );
        assert_eq!(authority.validate(&token, InfoHash([8; 20]), 400), None);
        let mut corrupt = token;
        corrupt[TOKEN_LEN - 1] ^= 1;
        assert_eq!(authority.validate(&corrupt, info_hash, 400), None);
        authority.rotate([2; 32], 700).unwrap();
        assert_eq!(
            authority.validate(&token, info_hash, 750),
            Some(destination)
        );
        let new_token = authority.issue(destination, info_hash, 701).unwrap();
        assert_eq!(
            authority.validate(&new_token, info_hash, 701),
            Some(destination)
        );
        assert_eq!(authority.validate(&token, info_hash, 800), None);
        assert!(authority.issue(destination, info_hash, 1300).is_err());
    }

    #[test]
    fn local_tracker_self_filters_and_expires_with_seed_filtering() {
        let local = DestinationHash([0; 32]);
        let mut tracker = LocalTracker::new(local);
        let hash = InfoHash([1; 20]);
        assert!(!tracker.announce(hash, local, false, 0).unwrap());
        assert!(
            tracker
                .announce(hash, DestinationHash([2; 32]), true, 1)
                .unwrap()
        );
        assert!(
            tracker
                .announce(hash, DestinationHash([3; 32]), false, 2)
                .unwrap()
        );
        assert_eq!(tracker.peers(hash, true, 20).len(), 1);
        assert_eq!(tracker.peers(hash, false, 20).len(), 2);
        assert_eq!(tracker.expire(3 * 60 * 60 + 3, 3 * 60 * 60), 2);
        assert_eq!(tracker.counts(), (0, 0));
    }

    #[test]
    fn local_tracker_enforces_peer_and_torrent_ceilings() {
        let local = DestinationHash([0xff; 32]);
        let mut tracker = LocalTracker::new(local);
        let hash = InfoHash([1; 20]);
        for index in 1..=MAX_PEERS_PER_TORRENT {
            let mut bytes = [0; 32];
            bytes[..2].copy_from_slice(&(index as u16).to_be_bytes());
            assert!(
                tracker
                    .announce(hash, DestinationHash(bytes), false, index as u64)
                    .unwrap()
            );
        }
        let mut overflow = [0; 32];
        overflow[..2].copy_from_slice(&((MAX_PEERS_PER_TORRENT + 1) as u16).to_be_bytes());
        assert_eq!(
            tracker.announce(hash, DestinationHash(overflow), false, 1000),
            Err(DhtError::Capacity)
        );
        assert_eq!(tracker.counts(), (1, MAX_PEERS_PER_TORRENT));
    }

    #[test]
    fn local_tracker_enforces_total_peer_and_infohash_ceilings() {
        let local = DestinationHash([0xff; 32]);
        let mut tracker = LocalTracker::new(local);
        for index in 0..MAX_TRACKED_PEERS {
            let mut hash = [0; 20];
            hash[..4].copy_from_slice(&((index / MAX_PEERS_PER_TORRENT) as u32).to_be_bytes());
            let mut destination = [0; 32];
            destination[..4].copy_from_slice(&((index + 1) as u32).to_be_bytes());
            assert!(
                tracker
                    .announce(InfoHash(hash), DestinationHash(destination), false, 1)
                    .unwrap()
            );
        }
        assert_eq!(tracker.counts().1, MAX_TRACKED_PEERS);
        assert_eq!(
            tracker.announce(InfoHash([0; 20]), DestinationHash([1; 32]), false, 1),
            Err(DhtError::Capacity)
        );
        let mut torrents = LocalTracker::new(local);
        for index in 0..MAX_TRACKED_TORRENTS {
            let mut hash = [0; 20];
            hash[..4].copy_from_slice(&(index as u32).to_be_bytes());
            let mut destination = [0; 32];
            destination[..4].copy_from_slice(&((index + 1) as u32).to_be_bytes());
            torrents
                .announce(InfoHash(hash), DestinationHash(destination), false, 1)
                .unwrap();
        }
        let mut extra_hash = [0; 20];
        extra_hash[..4].copy_from_slice(&(MAX_TRACKED_TORRENTS as u32).to_be_bytes());
        assert_eq!(
            torrents.announce(InfoHash(extra_hash), DestinationHash([99; 32]), false, 1),
            Err(DhtError::Capacity)
        );
    }

    #[test]
    fn bootstrap_snapshot_round_trips_only_validated_node_state() {
        let snapshot = BootstrapSnapshot {
            nodes: vec![node(321, 4), node(654, 5)],
        };
        let encoded = snapshot.encode().unwrap();
        assert_eq!(BootstrapSnapshot::decode(&encoded).unwrap(), snapshot);
        assert!(BootstrapSnapshot::decode(&encoded[..encoded.len() - 1]).is_err());
        assert!(BootstrapSnapshot::decode(b"wrongver!").is_err());
        assert!(
            BootstrapSnapshot {
                nodes: vec![snapshot.nodes[0], snapshot.nodes[0]]
            }
            .encode()
            .is_err()
        );
    }

    #[test]
    fn core_get_peers_token_announce_recovers_authenticated_destination() {
        let local = node(1111, 10);
        let sender = node(2222, 20);
        let mut core = DhtCore::new(local, [42; 32], 100).unwrap();
        let info_hash = InfoHash([17; 20]);
        let response = core
            .handle_query(
                Query::GetPeers {
                    id: sender.id,
                    info_hash,
                    noseed: false,
                },
                Some(sender),
                101,
            )
            .unwrap();
        let Reply::Peers { token, .. } = response else {
            panic!("get_peers must return token and peer lookup")
        };
        let announce = Query::AnnouncePeer {
            id: NodeId([99; 20]), // Untrusted on the raw announce channel.
            info_hash,
            token: token.clone(),
            port: 0,
            seed: true,
        };
        assert_eq!(
            core.handle_query(announce, None, 102).unwrap(),
            Reply::Pong { id: local.id }
        );
        assert_eq!(core.tracker().counts(), (1, 1));

        let mut bad_token = token;
        bad_token[63] ^= 1;
        assert_eq!(
            core.handle_query(
                Query::AnnouncePeer {
                    id: sender.id,
                    info_hash,
                    token: bad_token,
                    port: 0,
                    seed: false,
                },
                None,
                103,
            ),
            Err(DhtError::Invalid)
        );
        assert_eq!(core.tracker().counts(), (1, 1));

        let response = core
            .handle_query(
                Query::GetPeers {
                    id: sender.id,
                    info_hash,
                    noseed: false,
                },
                Some(sender),
                104,
            )
            .unwrap();
        assert!(
            matches!(response, Reply::Peers { peers, nodes, .. } if peers == vec![sender.destination] && nodes.is_empty())
        );
    }
}
