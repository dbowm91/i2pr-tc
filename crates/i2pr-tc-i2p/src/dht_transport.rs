//! SAM DATAGRAM/RAW access for the I2P DHT profile.
//!
//! The signed query channel uses protocol 17 on the local query port. Replies,
//! errors, and announce queries use protocol 18 on the adjacent raw response
//! port. This module adds no host-network connector.
use crate::{
    TransportError,
    identity::{Destination, DestinationHash},
    sam,
    sam::SamClient,
};
use async_trait::async_trait;
use i2pr_tc_core::dht::{
    CompactNode, DhtCore, DhtError, InfoHash, KrpcLimits, KrpcMessage, NodeId, Query,
    QueryResponse, Reply,
};
use i2pr_tc_storage::Cancellation;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{sync::oneshot, task::JoinSet};

pub const MAX_DHT_DATAGRAM_BYTES: usize = 32 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DhtPorts {
    pub query: u16,
    pub response: u16,
}

#[derive(Clone, Debug)]
pub struct DhtRpcOptions {
    pub transaction: Vec<u8>,
    pub timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct DhtTraversalOptions {
    pub transactions: Vec<Vec<u8>>,
    pub max_queries: usize,
    pub timeout_per_query: Duration,
}

impl DhtPorts {
    pub fn new(query: u16) -> Result<Self, TransportError> {
        if !(1..=u16::MAX - 1).contains(&query) {
            return Err(TransportError::Address);
        }
        Ok(Self {
            query,
            response: query + 1,
        })
    }

    fn query_channel_config(self) -> sam::SamChannelConfig {
        sam::SamChannelConfig {
            from_port: self.query,
            to_port: self.query,
            listen_port: self.query,
        }
    }

    fn response_channel_config(self) -> sam::SamChannelConfig {
        sam::SamChannelConfig {
            from_port: self.response,
            to_port: self.response,
            listen_port: self.response,
        }
    }

    fn query_send_config(self, remote_port: u16) -> sam::SamChannelConfig {
        sam::SamChannelConfig {
            to_port: remote_port,
            ..self.query_channel_config()
        }
    }

    fn response_send_config(self, remote_port: u16) -> sam::SamChannelConfig {
        sam::SamChannelConfig {
            to_port: remote_port,
            ..self.response_channel_config()
        }
    }
}

#[async_trait]
pub trait DhtDatagramIo: Send + Sync {
    async fn send_signed(
        &self,
        destination: &Destination,
        ports: DhtPorts,
        remote_query_port: u16,
        payload: &[u8],
        cancellation: &Cancellation,
    ) -> Result<(), TransportError>;

    async fn receive_signed(
        &self,
        ports: DhtPorts,
        cancellation: &Cancellation,
    ) -> Result<sam::SamDatagram, TransportError>;

    async fn send_raw(
        &self,
        destination: &Destination,
        ports: DhtPorts,
        remote_response_port: u16,
        payload: &[u8],
        cancellation: &Cancellation,
    ) -> Result<(), TransportError>;

    async fn receive_raw(
        &self,
        ports: DhtPorts,
        cancellation: &Cancellation,
    ) -> Result<sam::SamRawDatagram, TransportError>;
}

#[async_trait]
impl<F: sam::SamConnectionFactory> DhtDatagramIo for SamClient<F> {
    async fn send_signed(
        &self,
        destination: &Destination,
        ports: DhtPorts,
        remote_query_port: u16,
        payload: &[u8],
        cancellation: &Cancellation,
    ) -> Result<(), TransportError> {
        let channel = self
            .datagram_channel(ports.query_channel_config(), cancellation)
            .await?;
        self.datagram_send(
            &channel,
            destination,
            ports.query_send_config(remote_query_port),
            payload,
            cancellation,
        )
        .await
    }

    async fn receive_signed(
        &self,
        ports: DhtPorts,
        cancellation: &Cancellation,
    ) -> Result<sam::SamDatagram, TransportError> {
        let channel = self
            .datagram_channel(ports.query_channel_config(), cancellation)
            .await?;
        self.datagram_receive(&channel, cancellation).await
    }

    async fn send_raw(
        &self,
        destination: &Destination,
        ports: DhtPorts,
        remote_response_port: u16,
        payload: &[u8],
        cancellation: &Cancellation,
    ) -> Result<(), TransportError> {
        let channel = self
            .raw_channel(ports.response_channel_config(), cancellation)
            .await?;
        self.raw_send(
            &channel,
            destination,
            ports.response_send_config(remote_response_port),
            payload,
            cancellation,
        )
        .await
    }

    async fn receive_raw(
        &self,
        ports: DhtPorts,
        cancellation: &Cancellation,
    ) -> Result<sam::SamRawDatagram, TransportError> {
        let channel = self
            .raw_channel(ports.response_channel_config(), cancellation)
            .await?;
        self.raw_receive(&channel, cancellation).await
    }
}

/// Authenticated ingress from protocol 17. The query port is learned from the
/// signed datagram header and checked by validating the secure node ID.
pub fn answer_signed_query(
    core: &mut DhtCore,
    payload: &[u8],
    sender: &Destination,
    query_port: u16,
    now: u64,
) -> Result<(u16, Vec<u8>, QueryResponse), DhtError> {
    if payload.len() > MAX_DHT_DATAGRAM_BYTES {
        return Err(DhtError::Limit);
    }
    let KrpcMessage::Query { transaction, query } =
        KrpcMessage::decode(payload, KrpcLimits::default())?
    else {
        return Err(DhtError::Krpc);
    };
    if matches!(query, Query::AnnouncePeer { .. }) {
        return Err(DhtError::Invalid);
    }
    let claimed_id = query_id(&query);
    let remote = CompactNode {
        id: claimed_id,
        destination: i2pr_tc_core::dht::DestinationHash(sender.hash()),
        query_port,
    }
    .validate()?;
    let response = core.handle_query_with_destination(query, Some(remote), now)?;
    let wire = KrpcMessage::Reply {
        transaction,
        reply: response.reply.clone(),
    }
    .encode(KrpcLimits::default())?;
    let response_port = query_port.checked_add(1).ok_or(DhtError::Invalid)?;
    Ok((response_port, wire, response))
}

/// Raw announce ingress has no authenticated SAM sender. The token authorizes
/// the Destination hash; the packet's source port only supplies the return
/// route and is never identity authority.
pub fn answer_raw_announce(
    core: &mut DhtCore,
    payload: &[u8],
    response_port: u16,
    now: u64,
) -> Result<(DestinationHash, Vec<u8>, QueryResponse), DhtError> {
    if payload.len() > MAX_DHT_DATAGRAM_BYTES {
        return Err(DhtError::Limit);
    }
    if response_port == 0 {
        return Err(DhtError::Invalid);
    }
    let KrpcMessage::Query { transaction, query } =
        KrpcMessage::decode(payload, KrpcLimits::default())?
    else {
        return Err(DhtError::Krpc);
    };
    if !matches!(query, Query::AnnouncePeer { .. }) {
        return Err(DhtError::Invalid);
    }
    let response = core.handle_query_with_destination(query, None, now)?;
    let wire = KrpcMessage::Reply {
        transaction,
        reply: response.reply.clone(),
    }
    .encode(KrpcLimits::default())?;
    Ok((DestinationHash(response.destination.0), wire, response))
}

fn query_id(query: &Query) -> NodeId {
    match query {
        Query::Ping { id }
        | Query::FindNode { id, .. }
        | Query::GetPeers { id, .. }
        | Query::AnnouncePeer { id, .. } => *id,
    }
}

fn xor_distance(left: NodeId, right: NodeId) -> [u8; 20] {
    std::array::from_fn(|index| left.0[index] ^ right.0[index])
}

/// Owns the two bounded ingress workers for a primary Destination's DHT
/// channels. Torrent-specific lookups and announces are driven by later
/// methods on this owner; these workers only answer valid inbound queries.
pub struct DhtResponder<S> {
    session: Arc<S>,
    core: Arc<Mutex<DhtCore>>,
    ports: DhtPorts,
    cancellation: Cancellation,
    started: AtomicBool,
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
    pending: Arc<Mutex<BTreeMap<Vec<u8>, PendingRpc>>>,
    tokens: Mutex<BTreeMap<(i2pr_tc_core::dht::DestinationHash, InfoHash), CachedToken>>,
    storage: Option<Arc<i2pr_tc_storage::Storage>>,
}

struct PendingRpc {
    expected: CompactNode,
    expires_at: Instant,
    response: oneshot::Sender<KrpcMessage>,
}

struct CachedToken {
    value: Vec<u8>,
    expires_at: u64,
}

impl<S> DhtResponder<S>
where
    S: crate::I2pSession + DhtDatagramIo + 'static,
{
    pub fn new(
        session: Arc<S>,
        core: DhtCore,
        ports: DhtPorts,
        now: impl Fn() -> u64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            session,
            core: Arc::new(Mutex::new(core)),
            ports,
            cancellation: Cancellation::default(),
            started: AtomicBool::new(false),
            now: Arc::new(now),
            pending: Arc::new(Mutex::new(BTreeMap::new())),
            tokens: Mutex::new(BTreeMap::new()),
            storage: None,
        }
    }

    pub fn with_storage(mut self, storage: Arc<i2pr_tc_storage::Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Sends one signed query and waits for its correlated raw reply/error.
    /// Call `run` concurrently so one raw receiver dispatches replies and
    /// answers inbound announces without competing receive operations.
    /// The transaction ID must be fresh and unpredictable; its entropy belongs
    /// to the application boundary and the pending table rejects duplicates.
    pub async fn query(
        &self,
        node: CompactNode,
        transaction: Vec<u8>,
        query: Query,
        timeout: Duration,
        cancellation: &Cancellation,
    ) -> Result<KrpcMessage, TransportError> {
        if !self.started.load(Ordering::Acquire) || self.cancellation.is_cancelled() {
            return Err(TransportError::Session(
                "DHT responder is not running".into(),
            ));
        }
        node.validate().map_err(|_| TransportError::Protocol)?;
        let local_id = self
            .core
            .lock()
            .map_err(|_| TransportError::Session("DHT state lock poisoned".into()))?
            .local_node()
            .id;
        let is_announce = matches!(query, Query::AnnouncePeer { .. });
        if query_id(&query) != local_id {
            return Err(TransportError::Protocol);
        }
        if timeout.is_zero() || timeout > Duration::from_secs(120) {
            return Err(TransportError::Timeout);
        }
        let wire = KrpcMessage::Query {
            transaction: transaction.clone(),
            query,
        }
        .encode(KrpcLimits::default())
        .map_err(|_| TransportError::Protocol)?;
        if wire.len() > MAX_DHT_DATAGRAM_BYTES {
            return Err(TransportError::Protocol);
        }
        let destination = crate::identity::resolve_peer(
            self.session.as_ref(),
            &crate::identity::I2pPeer::from_hash(node.destination.0),
        )
        .await?;
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| TransportError::Session("DHT transaction lock poisoned".into()))?;
            pending.retain(|_, request| request.expires_at > Instant::now());
            if pending.len() >= 256 || pending.contains_key(&transaction) {
                return Err(TransportError::Protocol);
            }
            pending.insert(
                transaction.clone(),
                PendingRpc {
                    expected: node,
                    expires_at: Instant::now() + timeout,
                    response: sender,
                },
            );
        }
        let sent = if is_announce {
            self.session
                .send_raw(
                    &destination,
                    self.ports,
                    node.query_port.saturating_add(1),
                    &wire,
                    cancellation,
                )
                .await
        } else {
            self.session
                .send_signed(
                    &destination,
                    self.ports,
                    node.query_port,
                    &wire,
                    cancellation,
                )
                .await
        };
        if let Err(error) = sent {
            if let Ok(mut pending) = self.pending.lock() {
                pending.remove(&transaction);
            }
            return Err(error);
        }
        let received =
            crate::tracker::race_cancel(tokio::time::timeout(timeout, receiver), cancellation)
                .await;
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(&transaction);
        }
        match received? {
            Ok(Ok(message)) => {
                let nodes = match &message {
                    KrpcMessage::Reply {
                        reply: Reply::Nodes { nodes, .. } | Reply::Peers { nodes, .. },
                        ..
                    } => nodes.clone(),
                    _ => Vec::new(),
                };
                if let Ok(mut core) = self.core.lock() {
                    core.learn_nodes(nodes, (self.now)());
                }
                Ok(message)
            }
            Ok(Err(_)) => Err(TransportError::Session("DHT reply receiver closed".into())),
            Err(_) => {
                if let Ok(mut core) = self.core.lock() {
                    core.mark_node_failure(node.id);
                }
                Err(TransportError::Timeout)
            }
        }
    }

    /// Performs get_peers and merges bounded results into the shared
    /// tracker/PEX/DHT source set. Only newly retained peers are returned.
    pub async fn get_peers(
        &self,
        node: CompactNode,
        info_hash: InfoHash,
        noseed: bool,
        request: DhtRpcOptions,
        sources: &Arc<Mutex<crate::pex::PeerSourceSet>>,
        cancellation: &Cancellation,
    ) -> Result<Vec<crate::identity::I2pPeer>, TransportError> {
        let local = self
            .core
            .lock()
            .map_err(|_| TransportError::Session("DHT state lock poisoned".into()))?
            .local_node();
        let response = self
            .query(
                node,
                request.transaction,
                Query::GetPeers {
                    id: local.id,
                    info_hash,
                    noseed,
                },
                request.timeout,
                cancellation,
            )
            .await?;
        let KrpcMessage::Reply {
            reply: Reply::Peers { token, peers, .. },
            ..
        } = response
        else {
            return Err(TransportError::Protocol);
        };
        if token.is_empty() || token.len() > i2pr_tc_core::dht::MAX_TOKEN_BYTES {
            return Err(TransportError::Protocol);
        }
        let key = (node.destination, info_hash);
        {
            let mut tokens = self
                .tokens
                .lock()
                .map_err(|_| TransportError::Session("DHT token cache lock poisoned".into()))?;
            let now = (self.now)();
            tokens.retain(|_, token| token.expires_at > now);
            if !tokens.contains_key(&key)
                && tokens.len() >= 256
                && let Some(oldest) = tokens
                    .iter()
                    .min_by_key(|(_, token)| token.expires_at)
                    .map(|(key, _)| *key)
            {
                tokens.remove(&oldest);
            }
            tokens.insert(
                key,
                CachedToken {
                    value: token,
                    expires_at: now.saturating_add(
                        i2pr_tc_core::dht::TOKEN_ACCEPT_LIFETIME_SECS.saturating_sub(120),
                    ),
                },
            );
        }
        let local_hash = DestinationHash(self.session.local_peer_hash()?);
        let additions = sources
            .lock()
            .map_err(|_| TransportError::Session("peer source lock poisoned".into()))?
            .add_dht_peers(
                peers
                    .into_iter()
                    .map(|peer| crate::identity::I2pPeer::from_hash(peer.0)),
                local_hash,
                i2pr_tc_core::dht::MAX_PEERS_PER_REPLY,
            );
        Ok(additions)
    }

    /// Announces to a node only with that node's unexpired token for the same
    /// infohash. Announce queries are sent over protocol 18 RAW.
    pub async fn announce_peer(
        &self,
        node: CompactNode,
        info_hash: InfoHash,
        seed: bool,
        request: DhtRpcOptions,
        cancellation: &Cancellation,
    ) -> Result<KrpcMessage, TransportError> {
        let key = (node.destination, info_hash);
        let token = {
            let mut tokens = self
                .tokens
                .lock()
                .map_err(|_| TransportError::Session("DHT token cache lock poisoned".into()))?;
            let now = (self.now)();
            tokens.retain(|_, token| token.expires_at > now);
            tokens.get(&key).map(|token| token.value.clone())
        }
        .ok_or(TransportError::Protocol)?;
        let local = self
            .core
            .lock()
            .map_err(|_| TransportError::Session("DHT state lock poisoned".into()))?
            .local_node();
        self.query(
            node,
            request.transaction,
            Query::AnnouncePeer {
                id: local.id,
                info_hash,
                token,
                port: 0,
                seed,
            },
            request.timeout,
            cancellation,
        )
        .await
    }

    /// Bounded iterative get_peers traversal. At most 32 distinct nodes are
    /// queried; candidates come from supplied seeds or validated node records
    /// learned from matching replies.
    pub async fn iterative_get_peers(
        &self,
        info_hash: InfoHash,
        seeds: impl IntoIterator<Item = CompactNode>,
        traversal: DhtTraversalOptions,
        sources: &Arc<Mutex<crate::pex::PeerSourceSet>>,
        cancellation: &Cancellation,
    ) -> Result<Vec<crate::identity::I2pPeer>, TransportError> {
        if traversal.max_queries == 0
            || traversal.max_queries > 32
            || traversal.transactions.len() > 32
        {
            return Err(TransportError::Protocol);
        }
        let target = NodeId(info_hash.0);
        let mut candidates = seeds.into_iter().take(799).collect::<Vec<_>>();
        candidates.sort_by_key(|node| xor_distance(node.id, target));
        candidates.dedup_by_key(|node| node.id);
        let mut queued = candidates
            .iter()
            .map(|node| node.id)
            .collect::<std::collections::BTreeSet<_>>();
        let mut visited = std::collections::BTreeSet::new();
        let mut transactions = traversal.transactions.into_iter();
        let mut discovered = Vec::new();
        let mut iterations = 0;

        while !candidates.is_empty() && iterations < traversal.max_queries {
            if cancellation.is_cancelled() {
                return Err(TransportError::Cancelled);
            }
            candidates.sort_by_key(|node| xor_distance(node.id, target));
            let node = candidates.remove(0);
            if !visited.insert(node.id) {
                continue;
            }
            let Some(transaction) = transactions.next() else {
                break;
            };
            iterations += 1;
            match self
                .get_peers(
                    node,
                    info_hash,
                    false,
                    DhtRpcOptions {
                        transaction,
                        timeout: traversal.timeout_per_query,
                    },
                    sources,
                    cancellation,
                )
                .await
            {
                Ok(peers) => discovered.extend(peers),
                Err(TransportError::Cancelled) => return Err(TransportError::Cancelled),
                Err(_) => {}
            }
            let closest = self
                .core
                .lock()
                .map_err(|_| TransportError::Session("DHT state lock poisoned".into()))?
                .closest_nodes(target, i2pr_tc_core::dht::K);
            for candidate in closest {
                if queued.insert(candidate.id) && candidates.len() < 799 {
                    candidates.push(candidate);
                }
            }
        }
        Ok(discovered)
    }

    pub async fn run(&self) -> Result<(), TransportError> {
        let local = self
            .core
            .lock()
            .map_err(|_| TransportError::Session("DHT state lock poisoned".into()))?
            .local_node();
        if local.destination.0 != self.session.local_peer_hash()?
            || local.query_port != self.ports.query
        {
            return Err(TransportError::Address);
        }
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(TransportError::Session(
                "DHT responder may only be started once".to_owned(),
            ));
        }
        if let Some(storage) = &self.storage {
            let snapshot = storage.load_dht_bootstrap().map_err(|error| {
                TransportError::Session(format!("DHT bootstrap load failed: {error}"))
            })?;
            if let Some(snapshot) = snapshot {
                self.core
                    .lock()
                    .map_err(|_| TransportError::Session("DHT state lock poisoned".into()))?
                    .learn_nodes(snapshot.nodes, (self.now)());
            }
        }
        let mut workers = JoinSet::new();
        let session = self.session.clone();
        let core = self.core.clone();
        let cancel = self.cancellation.clone();
        let ports = self.ports;
        let now = self.now.clone();
        workers.spawn(async move { signed_query_loop(session, core, ports, cancel, now).await });

        let session = self.session.clone();
        let core = self.core.clone();
        let cancel = self.cancellation.clone();
        let ports = self.ports;
        let now = self.now.clone();
        let pending = self.pending.clone();
        workers.spawn(async move {
            raw_announce_loop(session, core, ports, cancel, now, pending).await
        });

        let core = self.core.clone();
        let cancel = self.cancellation.clone();
        let storage = self.storage.clone();
        let now = self.now.clone();
        workers.spawn(async move { maintain_dht_loop(core, storage, cancel, now).await });

        let mut result = Ok(());
        while let Some(joined) = workers.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(TransportError::Cancelled)) if self.cancellation.is_cancelled() => {}
                Ok(Err(error)) => {
                    result = Err(error);
                    self.cancellation.cancel();
                    break;
                }
                Err(error) => {
                    result = Err(TransportError::Session(format!(
                        "DHT receive worker failed: {error}"
                    )));
                    self.cancellation.cancel();
                    break;
                }
            }
        }
        while workers.join_next().await.is_some() {}
        result
    }
}

async fn maintain_dht_loop(
    core: Arc<Mutex<DhtCore>>,
    storage: Option<Arc<i2pr_tc_storage::Storage>>,
    cancellation: Cancellation,
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
) -> Result<(), TransportError> {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                persist_snapshot(&core, storage.as_deref(), now())?;
            }
            _ = tokio::time::sleep(Duration::from_millis(20)) => {
                if cancellation.is_cancelled() {
                    persist_snapshot(&core, storage.as_deref(), now())?;
                    return Ok(());
                }
            }
        }
    }
}

fn persist_snapshot(
    core: &Mutex<DhtCore>,
    storage: Option<&i2pr_tc_storage::Storage>,
    now: u64,
) -> Result<(), TransportError> {
    let snapshot = {
        let mut core = core
            .lock()
            .map_err(|_| TransportError::Session("DHT state lock poisoned".into()))?;
        core.maintain(now, 30 * 60);
        core.bootstrap_snapshot()
    };
    if let Some(storage) = storage {
        storage.save_dht_bootstrap(&snapshot).map_err(|error| {
            TransportError::Session(format!("DHT bootstrap save failed: {error}"))
        })?;
    }
    Ok(())
}

impl<S> Drop for DhtResponder<S> {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

async fn signed_query_loop<S>(
    session: Arc<S>,
    core: Arc<Mutex<DhtCore>>,
    ports: DhtPorts,
    cancellation: Cancellation,
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
) -> Result<(), TransportError>
where
    S: crate::I2pSession + DhtDatagramIo + 'static,
{
    while !cancellation.is_cancelled() {
        let datagram = match session.receive_signed(ports, &cancellation).await {
            Ok(datagram) => datagram,
            Err(TransportError::Cancelled) => return Err(TransportError::Cancelled),
            Err(TransportError::Timeout) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        let Some(sender) = datagram.sender else {
            continue;
        };
        let handled = {
            let Ok(mut core) = core.lock() else {
                return Err(TransportError::Session("DHT state lock poisoned".into()));
            };
            answer_signed_query(
                &mut core,
                &datagram.payload,
                &sender,
                datagram.from_port,
                now(),
            )
        };
        let Ok((remote_response_port, response, authorized)) = handled else {
            continue;
        };
        let destination = crate::identity::resolve_peer(
            session.as_ref(),
            &crate::identity::I2pPeer::from_hash(authorized.destination.0),
        )
        .await?;
        session
            .send_raw(
                &destination,
                ports,
                remote_response_port,
                &response,
                &cancellation,
            )
            .await?;
    }
    Err(TransportError::Cancelled)
}

async fn raw_announce_loop<S>(
    session: Arc<S>,
    core: Arc<Mutex<DhtCore>>,
    ports: DhtPorts,
    cancellation: Cancellation,
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
    pending: Arc<Mutex<BTreeMap<Vec<u8>, PendingRpc>>>,
) -> Result<(), TransportError>
where
    S: crate::I2pSession + DhtDatagramIo + 'static,
{
    while !cancellation.is_cancelled() {
        let datagram = match session.receive_raw(ports, &cancellation).await {
            Ok(datagram) if datagram.protocol == sam::I2P_PROTOCOL_RAW => datagram,
            Ok(_) => continue,
            Err(TransportError::Cancelled) => return Err(TransportError::Cancelled),
            Err(TransportError::Timeout) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        let Ok(message) = KrpcMessage::decode(&datagram.payload, KrpcLimits::default()) else {
            continue;
        };
        match &message {
            KrpcMessage::Reply { transaction, reply } => {
                let Some(node_id) = reply_node_id(reply) else {
                    continue;
                };
                let pending_rpc = {
                    let Ok(mut pending) = pending.lock() else {
                        return Err(TransportError::Session(
                            "DHT transaction lock poisoned".into(),
                        ));
                    };
                    let matches = pending.get(transaction).is_some_and(|request| {
                        request.expires_at > Instant::now()
                            && request.expected.id == node_id
                            && datagram.sender.as_ref().is_none_or(|sender| {
                                sender.hash() == request.expected.destination.0
                            })
                    });
                    if matches {
                        pending.remove(transaction)
                    } else {
                        None
                    }
                };
                if let Some(request) = pending_rpc {
                    let _ = request.response.send(message);
                }
                continue;
            }
            KrpcMessage::Error { transaction, .. } => {
                let request = {
                    let Ok(mut pending) = pending.lock() else {
                        return Err(TransportError::Session(
                            "DHT transaction lock poisoned".into(),
                        ));
                    };
                    pending
                        .get(transaction)
                        .is_some_and(|request| {
                            request.expires_at > Instant::now()
                                && datagram.sender.as_ref().is_none_or(|sender| {
                                    sender.hash() == request.expected.destination.0
                                })
                        })
                        .then(|| pending.remove(transaction))
                        .flatten()
                };
                if let Some(request) = request {
                    let _ = request.response.send(message);
                }
                continue;
            }
            KrpcMessage::Query { query, .. } if !matches!(query, Query::AnnouncePeer { .. }) => {
                continue;
            }
            KrpcMessage::Query { .. } => {}
        }
        let handled = {
            let Ok(mut core) = core.lock() else {
                return Err(TransportError::Session("DHT state lock poisoned".into()));
            };
            answer_raw_announce(&mut core, &datagram.payload, datagram.from_port, now())
        };
        let Ok((destination_hash, response, _)) = handled else {
            continue;
        };
        let destination = crate::identity::resolve_peer(
            session.as_ref(),
            &crate::identity::I2pPeer::from_hash(destination_hash.0),
        )
        .await?;
        session
            .send_raw(
                &destination,
                ports,
                datagram.from_port,
                &response,
                &cancellation,
            )
            .await?;
    }
    Err(TransportError::Cancelled)
}

fn reply_node_id(reply: &i2pr_tc_core::dht::Reply) -> Option<NodeId> {
    match reply {
        i2pr_tc_core::dht::Reply::Pong { id }
        | i2pr_tc_core::dht::Reply::Nodes { id, .. }
        | i2pr_tc_core::dht::Reply::Peers { id, .. } => Some(*id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{I2pSession, I2pStream};
    use async_trait::async_trait;
    use i2pr_tc_core::dht::{DestinationHash as CoreHash, InfoHash, NodeId, Reply};
    use std::sync::Mutex as StdMutex;
    use tokio::sync::{Mutex as AsyncMutex, mpsc};

    fn destination(byte: u8) -> Destination {
        Destination::from_bytes(vec![byte; 387]).unwrap()
    }

    fn node(destination: &Destination, query_port: u16, tail: [u8; 14]) -> CompactNode {
        let hash = CoreHash(destination.hash());
        CompactNode {
            id: NodeId::from_destination(hash, query_port, tail).unwrap(),
            destination: hash,
            query_port,
        }
    }

    fn core(local: &Destination) -> DhtCore {
        DhtCore::new(node(local, 12_000, [0x44; 14]), [0x55; 32], 1_000).unwrap()
    }

    fn temp_root() -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("i2pr-tc-dht-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    struct FakeDhtSession {
        local: Destination,
        remote: Destination,
        signed_sent: StdMutex<Vec<Vec<u8>>>,
        raw_sent: StdMutex<Vec<Vec<u8>>>,
        out_signed: Option<mpsc::Sender<sam::SamDatagram>>,
        out_raw: Option<mpsc::Sender<sam::SamRawDatagram>>,
        signed_rx: AsyncMutex<mpsc::Receiver<sam::SamDatagram>>,
        raw_rx: AsyncMutex<mpsc::Receiver<sam::SamRawDatagram>>,
    }

    #[async_trait]
    impl I2pSession for FakeDhtSession {
        fn local_peer_hash(&self) -> Result<[u8; 32], TransportError> {
            Ok(self.local.hash())
        }

        async fn lookup(&self, name: &str) -> Result<Destination, TransportError> {
            let expected = format!(
                "{}.b32.i2p",
                crate::identity::encode_base32(&self.remote.hash())
            );
            if name == expected {
                Ok(self.remote.clone())
            } else {
                Err(TransportError::Address)
            }
        }

        async fn connect(
            &self,
            _destination: &Destination,
            _port: u16,
        ) -> Result<I2pStream, TransportError> {
            Err(TransportError::Protocol)
        }

        async fn accept(&self) -> Result<(Destination, I2pStream), TransportError> {
            Err(TransportError::Protocol)
        }
    }

    #[async_trait]
    impl DhtDatagramIo for FakeDhtSession {
        async fn send_signed(
            &self,
            _destination: &Destination,
            ports: DhtPorts,
            remote_query_port: u16,
            payload: &[u8],
            _cancellation: &Cancellation,
        ) -> Result<(), TransportError> {
            self.signed_sent.lock().unwrap().push(payload.to_vec());
            if let Some(sender) = &self.out_signed {
                sender
                    .send(sam::SamDatagram {
                        sender: Some(self.local.clone()),
                        payload: payload.to_vec(),
                        from_port: ports.query,
                        to_port: remote_query_port,
                    })
                    .await
                    .map_err(|_| TransportError::Cancelled)?;
            }
            Ok(())
        }

        async fn receive_signed(
            &self,
            _ports: DhtPorts,
            cancellation: &Cancellation,
        ) -> Result<sam::SamDatagram, TransportError> {
            let mut receiver = self.signed_rx.lock().await;
            loop {
                if cancellation.is_cancelled() {
                    return Err(TransportError::Cancelled);
                }
                tokio::select! {
                    message = receiver.recv() => return message.ok_or(TransportError::Cancelled),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                }
            }
        }

        async fn send_raw(
            &self,
            _destination: &Destination,
            ports: DhtPorts,
            remote_response_port: u16,
            payload: &[u8],
            _cancellation: &Cancellation,
        ) -> Result<(), TransportError> {
            self.raw_sent.lock().unwrap().push(payload.to_vec());
            if let Some(sender) = &self.out_raw {
                sender
                    .send(sam::SamRawDatagram {
                        payload: payload.to_vec(),
                        protocol: sam::I2P_PROTOCOL_RAW,
                        from_port: ports.response,
                        to_port: remote_response_port,
                        sender: Some(self.local.clone()),
                    })
                    .await
                    .map_err(|_| TransportError::Cancelled)?;
            }
            Ok(())
        }

        async fn receive_raw(
            &self,
            _ports: DhtPorts,
            cancellation: &Cancellation,
        ) -> Result<sam::SamRawDatagram, TransportError> {
            let mut receiver = self.raw_rx.lock().await;
            loop {
                if cancellation.is_cancelled() {
                    return Err(TransportError::Cancelled);
                }
                tokio::select! {
                    message = receiver.recv() => return message.ok_or(TransportError::Cancelled),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                }
            }
        }
    }

    #[test]
    fn query_and_response_ports_are_adjacent_and_bounded() {
        assert_eq!(
            DhtPorts::new(1234).unwrap(),
            DhtPorts {
                query: 1234,
                response: 1235
            }
        );
        assert!(DhtPorts::new(0).is_err());
        assert!(DhtPorts::new(u16::MAX).is_err());
        assert_eq!(DhtPorts::new(u16::MAX - 1).unwrap().response, u16::MAX);
    }

    #[test]
    fn signed_query_reply_uses_authenticated_destination_and_adjacent_raw_port() {
        let local = destination(1);
        let remote = destination(2);
        let remote_node = node(&remote, 22_000, [0x33; 14]);
        let request = KrpcMessage::Query {
            transaction: b"tx1".to_vec(),
            query: Query::Ping { id: remote_node.id },
        }
        .encode(KrpcLimits::default())
        .unwrap();

        let (port, response, authorized) = answer_signed_query(
            &mut core(&local),
            &request,
            &remote,
            remote_node.query_port,
            1_001,
        )
        .unwrap();
        assert_eq!(port, remote_node.query_port + 1);
        assert_eq!(authorized.destination.0, remote.hash());
        assert!(matches!(authorized.reply, Reply::Pong { .. }));
        assert!(matches!(
            KrpcMessage::decode(&response, KrpcLimits::default()).unwrap(),
            KrpcMessage::Reply { transaction, reply: Reply::Pong { .. } } if transaction == b"tx1"
        ));
    }

    #[test]
    fn raw_announce_uses_token_identity_and_never_query_id() {
        let local = destination(1);
        let remote = destination(2);
        let remote_node = node(&remote, 22_000, [0x33; 14]);
        let info_hash = InfoHash([0x77; 20]);
        let mut core = core(&local);
        let get_peers = core
            .handle_query(
                Query::GetPeers {
                    id: remote_node.id,
                    info_hash,
                    noseed: false,
                },
                Some(remote_node),
                1_001,
            )
            .unwrap();
        let Reply::Peers { token, .. } = get_peers else {
            panic!("get_peers reply expected")
        };
        let announce = KrpcMessage::Query {
            transaction: b"tx2".to_vec(),
            query: Query::AnnouncePeer {
                id: NodeId([0; 20]),
                info_hash,
                token,
                port: 0,
                seed: true,
            },
        }
        .encode(KrpcLimits::default())
        .unwrap();

        let (authorized_hash, response, handled) =
            answer_raw_announce(&mut core, &announce, 22_001, 1_002).unwrap();
        assert_eq!(authorized_hash, DestinationHash(remote.hash()));
        assert_eq!(handled.destination.0, remote.hash());
        assert!(matches!(
            KrpcMessage::decode(&response, KrpcLimits::default()).unwrap(),
            KrpcMessage::Reply {
                reply: Reply::Pong { .. },
                ..
            }
        ));
        assert_eq!(
            core.tracker().peers(info_hash, false, 4)[0].destination.0,
            remote.hash()
        );
    }

    #[tokio::test]
    async fn responder_correlates_raw_reply_with_bounded_pending_query() {
        let local = destination(1);
        let remote = destination(2);
        let remote_node = node(&remote, 22_000, [0x33; 14]);
        let (signed_tx, signed_rx) = mpsc::channel(4);
        let (raw_tx, raw_rx) = mpsc::channel(4);
        let session = Arc::new(FakeDhtSession {
            local: local.clone(),
            remote: remote.clone(),
            signed_sent: StdMutex::new(Vec::new()),
            raw_sent: StdMutex::new(Vec::new()),
            out_signed: None,
            out_raw: None,
            signed_rx: AsyncMutex::new(signed_rx),
            raw_rx: AsyncMutex::new(raw_rx),
        });
        let responder = Arc::new(DhtResponder::new(
            session.clone(),
            core(&local),
            DhtPorts::new(12_000).unwrap(),
            || 1_001,
        ));
        let local_id = node(&local, 12_000, [0x44; 14]).id;
        let service = {
            let responder = responder.clone();
            tokio::spawn(async move { responder.run().await })
        };
        let transaction = b"rpc-1".to_vec();
        let request = {
            let responder = responder.clone();
            let cancel = Cancellation::default();
            let tx = transaction.clone();
            tokio::spawn(async move {
                responder
                    .query(
                        remote_node,
                        tx,
                        Query::Ping { id: local_id },
                        Duration::from_secs(2),
                        &cancel,
                    )
                    .await
            })
        };
        tokio::task::yield_now().await;
        let sent = session.signed_sent.lock().unwrap().clone();
        assert_eq!(sent.len(), 1);
        let sent_message = KrpcMessage::decode(&sent[0], KrpcLimits::default()).unwrap();
        let KrpcMessage::Query {
            query: Query::Ping { id: sent_id },
            ..
        } = sent_message
        else {
            panic!("signed ping query expected");
        };
        // Rebuild the expected node with the ID required by the signed query.
        let valid = KrpcMessage::Reply {
            transaction: transaction.clone(),
            reply: Reply::Pong {
                id: node(&remote, 22_000, [0x33; 14]).id,
            },
        }
        .encode(KrpcLimits::default())
        .unwrap();
        assert_eq!(sent_id, local_id);
        raw_tx
            .send(sam::SamRawDatagram {
                payload: valid,
                protocol: sam::I2P_PROTOCOL_RAW,
                from_port: 22_001,
                to_port: 12_001,
                sender: Some(remote.clone()),
            })
            .await
            .unwrap();
        assert!(matches!(
            request.await.unwrap().unwrap(),
            KrpcMessage::Reply { .. }
        ));
        responder.cancel();
        assert!(service.await.unwrap().is_ok());
        drop(signed_tx);
    }

    #[tokio::test]
    async fn two_node_fixture_runs_get_peers_then_raw_token_authorized_announce() {
        let destination_a = destination(0x61);
        let destination_b = destination(0x62);
        let destination_c = destination(0x63);
        let node_a = node(&destination_a, 12_000, [0x41; 14]);
        let node_b = node(&destination_b, 12_000, [0x42; 14]);
        let node_c = node(&destination_c, 12_000, [0x43; 14]);
        let info_hash = InfoHash([0x31; 20]);
        let mut core_b = DhtCore::new(node_b, [0xb1; 32], 1_000).unwrap();
        let c_token = match core_b
            .handle_query(
                Query::GetPeers {
                    id: node_c.id,
                    info_hash,
                    noseed: false,
                },
                Some(node_c),
                1_001,
            )
            .unwrap()
        {
            Reply::Peers { token, .. } => token,
            _ => panic!("get_peers reply expected"),
        };
        core_b
            .handle_query(
                Query::AnnouncePeer {
                    id: node_c.id,
                    info_hash,
                    token: c_token,
                    port: 0,
                    seed: false,
                },
                None,
                1_002,
            )
            .unwrap();
        let (a_signed_tx, a_signed_rx) = mpsc::channel(8);
        let (b_signed_tx, b_signed_rx) = mpsc::channel(8);
        let (a_raw_tx, a_raw_rx) = mpsc::channel(8);
        let (b_raw_tx, b_raw_rx) = mpsc::channel(8);
        let session_a = Arc::new(FakeDhtSession {
            local: destination_a.clone(),
            remote: destination_b.clone(),
            signed_sent: StdMutex::new(Vec::new()),
            raw_sent: StdMutex::new(Vec::new()),
            out_signed: Some(b_signed_tx),
            out_raw: Some(b_raw_tx),
            signed_rx: AsyncMutex::new(a_signed_rx),
            raw_rx: AsyncMutex::new(a_raw_rx),
        });
        let session_b = Arc::new(FakeDhtSession {
            local: destination_b.clone(),
            remote: destination_a.clone(),
            signed_sent: StdMutex::new(Vec::new()),
            raw_sent: StdMutex::new(Vec::new()),
            out_signed: Some(a_signed_tx),
            out_raw: Some(a_raw_tx),
            signed_rx: AsyncMutex::new(b_signed_rx),
            raw_rx: AsyncMutex::new(b_raw_rx),
        });
        let responder_a = Arc::new(DhtResponder::new(
            session_a.clone(),
            DhtCore::new(node_a, [0xa1; 32], 1_000).unwrap(),
            DhtPorts::new(12_000).unwrap(),
            || 1_001,
        ));
        let responder_b = Arc::new(DhtResponder::new(
            session_b.clone(),
            core_b,
            DhtPorts::new(12_000).unwrap(),
            || 1_001,
        ));
        let task_a = {
            let responder = responder_a.clone();
            tokio::spawn(async move { responder.run().await })
        };
        let task_b = {
            let responder = responder_b.clone();
            tokio::spawn(async move { responder.run().await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while !responder_a.started.load(Ordering::Acquire)
                || !responder_b.started.load(Ordering::Acquire)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both DHT responders started");
        let sources = Arc::new(Mutex::new(crate::pex::PeerSourceSet::default()));
        let local_id = node_a.id;
        let ping = responder_a
            .query(
                node_b,
                b"ping-1".to_vec(),
                Query::Ping { id: local_id },
                Duration::from_secs(2),
                &Cancellation::default(),
            )
            .await
            .unwrap();
        assert!(matches!(
            ping,
            KrpcMessage::Reply {
                reply: Reply::Pong { .. },
                ..
            }
        ));
        let find = responder_a
            .query(
                node_b,
                b"find-1".to_vec(),
                Query::FindNode {
                    id: local_id,
                    target: node_a.id,
                },
                Duration::from_secs(2),
                &Cancellation::default(),
            )
            .await
            .unwrap();
        assert!(matches!(
            find,
            KrpcMessage::Reply { reply: Reply::Nodes { nodes, .. }, .. }
                if nodes.iter().any(|node| node.destination.0 == destination_a.hash())
        ));
        let peers = responder_a
            .iterative_get_peers(
                info_hash,
                [node_b],
                DhtTraversalOptions {
                    transactions: vec![b"get-1".to_vec()],
                    max_queries: 1,
                    timeout_per_query: Duration::from_secs(2),
                },
                &sources,
                &Cancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            peers,
            vec![crate::identity::I2pPeer::from_hash(destination_c.hash())]
        );
        assert_eq!(
            sources
                .lock()
                .unwrap()
                .sources(DestinationHash(destination_c.hash())),
            Some(crate::pex::PeerSources {
                tracker: false,
                pex: false,
                dht: true
            })
        );
        assert_eq!(session_a.signed_sent.lock().unwrap().len(), 3);
        assert_eq!(session_b.raw_sent.lock().unwrap().len(), 3);

        let announced = responder_a
            .announce_peer(
                node_b,
                info_hash,
                true,
                DhtRpcOptions {
                    transaction: b"announce-1".to_vec(),
                    timeout: Duration::from_secs(2),
                },
                &Cancellation::default(),
            )
            .await
            .unwrap();
        assert!(matches!(
            announced,
            KrpcMessage::Reply {
                reply: Reply::Pong { .. },
                ..
            }
        ));
        assert_eq!(session_a.raw_sent.lock().unwrap().len(), 1);
        assert_eq!(session_b.raw_sent.lock().unwrap().len(), 4);
        assert_eq!(
            responder_b
                .core
                .lock()
                .unwrap()
                .tracker()
                .peers(info_hash, false, 4)[0]
                .destination
                .0,
            destination_a.hash()
        );

        responder_a.cancel();
        responder_b.cancel();
        assert!(task_a.await.unwrap().is_ok());
        assert!(task_b.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn responder_restores_and_flushes_only_bounded_bootstrap_nodes() {
        let local = destination(0x71);
        let remote = destination(0x72);
        let bootstrap_destination = destination(0x73);
        let bootstrap_node = node(&bootstrap_destination, 12_001, [0x43; 14]);
        let root = temp_root();
        let storage = Arc::new(i2pr_tc_storage::Storage::open(&root).unwrap());
        storage
            .save_dht_bootstrap(&i2pr_tc_core::dht::BootstrapSnapshot {
                nodes: vec![bootstrap_node],
            })
            .unwrap();
        let (_signed_tx, signed_rx) = mpsc::channel(4);
        let (_raw_tx, raw_rx) = mpsc::channel(4);
        let session = Arc::new(FakeDhtSession {
            local: local.clone(),
            remote,
            signed_sent: StdMutex::new(Vec::new()),
            raw_sent: StdMutex::new(Vec::new()),
            out_signed: None,
            out_raw: None,
            signed_rx: AsyncMutex::new(signed_rx),
            raw_rx: AsyncMutex::new(raw_rx),
        });
        let responder = Arc::new(
            DhtResponder::new(
                session,
                core(&local),
                DhtPorts::new(12_000).unwrap(),
                || 1_002,
            )
            .with_storage(storage.clone()),
        );
        let task = {
            let responder = responder.clone();
            tokio::spawn(async move { responder.run().await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while responder.core.lock().unwrap().routing().len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("responder loaded its bootstrap state");
        assert_eq!(responder.core.lock().unwrap().routing().len(), 1);
        responder.cancel();
        assert!(task.await.unwrap().is_ok());
        assert_eq!(
            storage.load_dht_bootstrap().unwrap().unwrap().nodes,
            vec![bootstrap_node]
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
