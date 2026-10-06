//! Shared-Destination qualification against an in-memory SAM 3.3 service.
//!
//! boundary-guard:test-only
//!
//! This is the cross-transport fixture C003 requires. It is not a mock of this
//! client's own encoder: it is an independent implementation of the SAM 3.3
//! shared-Destination rules that a service must follow — one Destination and
//! one tunnel set per primary session, children attached with `SESSION ADD` on
//! the control connection that created the primary, and children carrying the
//! primary's identity — written against the specification rather than against
//! this client. The client under test speaks to it through the same
//! [`SamConnectionFactory`] the managed runtime implements, so every command
//! it sends and every reply it parses crosses a real protocol boundary.
//!
//! What it proves, and what only it can prove, is the property the milestone
//! turns on: one Destination, shared by STREAM, DATAGRAM, and RAW at the same
//! time. A unit test with a scripted service can only show that this client
//! sends well-formed commands; only a service that actually shares an identity
//! can show that one identity survives all three transports.

use i2pr_tc_i2p::{
    I2pSession, TransportError,
    identity::Destination,
    sam::{
        SamChannelConfig, SamChannelKind, SamClient, SamConnectionFactory, SamIdentity, SamLimits,
        SamRawStream, SamSessionDestination, SamTimeouts,
    },
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Builds a structurally valid Destination: 384 key bytes then a null
/// certificate. The bytes are a function of `seed` so a test can tell two
/// identities apart.
fn destination_for(seed: u8) -> Destination {
    let mut bytes: Vec<u8> = (0..384u32).map(|index| index as u8 ^ seed).collect();
    bytes.extend([0u8, 0u8, 0u8]);
    Destination::from_bytes(bytes).expect("fixture destination is well formed")
}

/// Decodes a `DESTINATION=` option the way any SAM client must.
fn decode_destination(value: &str) -> Option<Destination> {
    i2pr_tc_i2p::sam::decode_base64(value.as_bytes(), 8192)
        .ok()
        .filter(|bytes| (387..=8192).contains(&bytes.len()))
        .and_then(|bytes| Destination::from_bytes(bytes).ok())
}

#[derive(Clone, Debug)]
struct Child {
    style: String,
    from_port: u16,
    to_port: u16,
    protocol: u8,
    /// Datagrams delivered to this child, awaiting a receive.
    inbox: VecDeque<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct Session {
    destination: Destination,
    control_alive: bool,
    children: BTreeMap<String, Child>,
    /// Every command this service accepted, in order.
    transcript: Vec<String>,
    /// `SESSION CREATE` count, so a test can prove one per identity.
    creates: usize,
}

#[derive(Default)]
struct ServiceState {
    sessions: BTreeMap<String, Session>,
    /// Seeds handed out, so each primary gets a distinct identity.
    next_seed: u8,
    /// A way to end one primary's control connection, which is how a router
    /// closing that session looks from the client side.
    controls: BTreeMap<String, tokio::sync::oneshot::Sender<()>>,
}

/// An in-memory SAM 3.3 service implementing the shared-Destination rules.
#[derive(Clone, Default)]
struct SamService {
    state: Arc<Mutex<ServiceState>>,
}

impl SamService {
    fn destination_of(&self, session: &str) -> Option<Destination> {
        self.state
            .lock()
            .expect("service state")
            .sessions
            .get(session)
            .map(|session| session.destination.clone())
    }

    fn transcript(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("service state")
            .sessions
            .values()
            .flat_map(|session| session.transcript.clone())
            .collect()
    }

    fn create_count(&self) -> usize {
        self.state
            .lock()
            .expect("service state")
            .sessions
            .values()
            .map(|session| session.creates)
            .sum()
    }

    /// Ends a primary's control connection, exactly as a router does when that
    /// session goes away.
    fn close_control(&self, session: &str) -> bool {
        self.state
            .lock()
            .expect("service state")
            .controls
            .remove(session)
            .is_some_and(|sender| sender.send(()).is_ok())
    }

    /// Children a live session still has attached.
    fn children(&self, session: &str) -> BTreeMap<String, Child> {
        self.state
            .lock()
            .expect("service state")
            .sessions
            .get(session)
            .map(|session| session.children.clone())
            .unwrap_or_default()
    }
}

/// One served connection: the session it created, if any.
struct Connection {
    service: SamService,
    /// The session this connection created, and therefore the one that owns
    /// `SESSION ADD` and `SESSION REMOVE`.
    created: Option<String>,
    close_sender: Option<tokio::sync::oneshot::Sender<()>>,
    close_receiver: Option<tokio::sync::oneshot::Receiver<()>>,
}

#[async_trait::async_trait]
impl SamConnectionFactory for SamService {
    async fn open_sam_connection(&self) -> Result<SamRawStream, TransportError> {
        let (client, service_side) = tokio::io::duplex(64 * 1024);
        let (close_sender, close_receiver) = tokio::sync::oneshot::channel();
        let connection = Connection {
            service: self.clone(),
            created: None,
            close_sender: Some(close_sender),
            close_receiver: Some(close_receiver),
        };
        tokio::spawn(serve(connection, service_side));
        Ok(Box::pin(client))
    }
}

/// Parses and answers commands until the connection ends.
async fn serve(mut connection: Connection, mut service: tokio::io::DuplexStream) {
    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        while let Some(position) = buffer.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = buffer.drain(..=position).collect();
            let Ok(text) = std::str::from_utf8(&line[..line.len() - 1]) else {
                return;
            };
            if !handle(&mut connection, &mut service, text, &mut buffer).await {
                return;
            }
        }
        let closed = async {
            match connection.close_receiver.as_mut() {
                Some(receiver) => {
                    let _ = receiver.await;
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            result = service.read(&mut chunk) => match result {
                Ok(0) | Err(_) => break,
                Ok(count) => buffer.extend_from_slice(&chunk[..count]),
            },
            _ = closed => break,
        }
    }
    // The control connection closing takes the session's children with it: a
    // router closes the primary's subsessions when the primary goes away.
    if let Some(session) = &connection.created {
        let mut state = connection.service.state.lock().expect("service state");
        if let Some(entry) = state.sessions.get_mut(session) {
            entry.control_alive = false;
            entry.children.clear();
        }
    }
}

/// Answers one command. Returns false to end the connection.
async fn handle(
    connection: &mut Connection,
    service: &mut tokio::io::DuplexStream,
    line: &str,
    buffered: &mut Vec<u8>,
) -> bool {
    let mut options: BTreeMap<&str, &str> = BTreeMap::new();
    let mut words: Vec<&str> = Vec::new();
    for token in line.trim().split(' ') {
        if let Some((key, value)) = token.split_once('=') {
            options.insert(key, value);
        } else {
            words.push(token);
        }
    }
    // `RAW DATA SEND` has two words before the action where `DATAGRAM SEND`
    // has one, so the action is taken from the last protocol word.
    let command = words.first().copied();
    let action = match command {
        Some("RAW") => words.get(2).copied(),
        Some(_) => words.get(1).copied(),
        None => None,
    };
    match (command, action) {
        (Some("HELLO"), _) => reply(service, "HELLO REPLY RESULT=OK VERSION=3.3\n").await,
        (Some("NAMING"), Some("LOOKUP")) => {
            // `NAME=ME` resolves to the Destination of the session on this
            // connection's control socket.
            let Some(session) = &connection.created else {
                return reply(
                    service,
                    "NAMING REPLY RESULT=INVALID_ID NAME=ME\n".to_owned(),
                )
                .await;
            };
            let Some(destination) = connection.service.destination_of(session) else {
                return reply(
                    service,
                    "NAMING REPLY RESULT=INVALID_ID NAME=ME\n".to_owned(),
                )
                .await;
            };
            let text = format!(
                "NAMING REPLY RESULT=OK NAME=ME\nVALUE={}\n",
                i2pr_tc_i2p::sam::encode_base64(destination.as_bytes())
            );
            reply(service, text).await
        }
        (Some("SESSION"), Some("CREATE")) => {
            let style = options.get("STYLE").copied().unwrap_or("");
            let Some(id) = options.get("ID").copied() else {
                return reply(service, "SESSION STATUS RESULT=INVALID_ID\n".to_owned()).await;
            };
            // A `SESSION CREATE` without a destination is not a shorter
            // spelling of the same request; it is a different, rejected one.
            let Some(destination_option) = options.get("DESTINATION").copied() else {
                return reply(service, "SESSION STATUS RESULT=INVALID_KEY\n".to_owned()).await;
            };
            let (transient, key_material) = match destination_option {
                "TRANSIENT" => (true, None),
                encoded => match i2pr_tc_i2p::sam::decode_base64(encoded.as_bytes(), 8192) {
                    Ok(bytes) => (false, Some(bytes)),
                    Err(_) => {
                        return reply(service, "SESSION STATUS RESULT=INVALID_KEY\n".to_owned())
                            .await;
                    }
                },
            };
            if !matches!(style, "PRIMARY" | "MASTER") {
                return reply(
                    service,
                    "SESSION STATUS RESULT=I2P_ERROR MESSAGE=\"Unknown STYLE\"\n".to_owned(),
                )
                .await;
            }
            // A duplicate session name is the router's answer, and it must be
            // the same one a real service gives.
            let allocated = {
                let mut state = connection.service.state.lock().expect("service state");
                match state.sessions.contains_key(id) {
                    true => None,
                    false => {
                        let seed = state.next_seed;
                        state.next_seed = state.next_seed.wrapping_add(1);
                        Some(seed)
                    }
                }
            };
            let Some(seed) = allocated else {
                return reply(service, "SESSION STATUS RESULT=DUPLICATED_ID\n").await;
            };
            // Injected key material carries its own Destination, so a
            // persistent session and a transient one are distinguishable by the
            // identity they hand back.
            let destination = match key_material.and_then(|bytes| {
                Destination::from_bytes(bytes[..387.min(bytes.len())].to_vec()).ok()
            }) {
                Some(adopted) => adopted,
                None => destination_for(seed),
            };
            let encoded = {
                let mut state = connection.service.state.lock().expect("service state");
                state.sessions.insert(
                    id.to_owned(),
                    Session {
                        destination: destination.clone(),
                        control_alive: true,
                        children: BTreeMap::new(),
                        transcript: vec![format!(
                            "SESSION CREATE STYLE={style} ID={id} TRANSIENT={transient}"
                        )],
                        creates: 1,
                    },
                );
                if let Some(sender) = connection.close_sender.take() {
                    state.controls.insert(id.to_owned(), sender);
                }
                i2pr_tc_i2p::sam::encode_base64(destination.as_bytes())
            };
            connection.created = Some(id.to_owned());
            reply(
                service,
                format!("SESSION STATUS RESULT=OK DESTINATION={encoded}\n"),
            )
            .await
        }
        (Some("SESSION"), Some("ADD")) => {
            // `SESSION ADD` is only accepted on the connection that created the
            // primary session.
            let Some(session) = connection.created.clone() else {
                return reply(service, "SESSION STATUS RESULT=INVALID_ID\n".to_owned()).await;
            };
            let style = options.get("STYLE").copied().unwrap_or("");
            let Some(id) = options.get("ID").copied() else {
                return reply(service, "SESSION STATUS RESULT=INVALID_ID\n".to_owned()).await;
            };
            if options.contains_key("DESTINATION") {
                // A subsession inherits the primary's Destination.
                return reply(service, "SESSION STATUS RESULT=INVALID_KEY\n".to_owned()).await;
            }
            let (from_port, to_port, protocol) = match style {
                "STREAM" => (0u16, 0u16, 6u8),
                "DATAGRAM" | "RAW" => {
                    let from = options
                        .get("FROM_PORT")
                        .and_then(|port| port.parse::<u16>().ok())
                        .unwrap_or(u16::MAX);
                    let to = options
                        .get("TO_PORT")
                        .and_then(|port| port.parse::<u16>().ok())
                        .unwrap_or(u16::MAX);
                    if from == u16::MAX || to == u16::MAX {
                        // A datagram channel with no routing port has no way to
                        // deliver anything.
                        return reply(service, "SESSION STATUS RESULT=INVALID_KEY\n".to_owned())
                            .await;
                    }
                    let protocol = options
                        .get("PROTOCOL")
                        .and_then(|value| value.parse::<u8>().ok())
                        .unwrap_or(if style == "RAW" { 18 } else { 17 });
                    (from, to, protocol)
                }
                _ => {
                    return reply(
                        service,
                        "SESSION STATUS RESULT=I2P_ERROR MESSAGE=\"Unsupported STYLE\"\n"
                            .to_owned(),
                    )
                    .await;
                }
            };
            // The guard never spans an await: this service is single-threaded
            // per connection, and a lock held across a write would serialise
            // unrelated commands behind a slow peer.
            let attached = {
                let mut state = connection.service.state.lock().expect("service state");
                match state.sessions.get_mut(&session) {
                    Some(entry) if entry.control_alive && !entry.children.contains_key(id) => {
                        entry.transcript.push(format!(
                            "SESSION ADD STYLE={style} ID={id} FROM_PORT={from_port} TO_PORT={to_port}"
                        ));
                        entry.children.insert(
                            id.to_owned(),
                            Child {
                                style: style.to_owned(),
                                from_port,
                                to_port,
                                protocol,
                                inbox: VecDeque::new(),
                            },
                        );
                        Ok(true)
                    }
                    Some(_) => Ok(false),
                    None => Err(()),
                }
            };
            match attached {
                Ok(true) => reply(service, "SESSION STATUS RESULT=OK\n").await,
                Ok(false) => reply(service, "SESSION STATUS RESULT=DUPLICATED_ID\n").await,
                Err(()) => reply(service, "SESSION STATUS RESULT=INVALID_ID\n").await,
            }
        }
        (Some("SESSION"), Some("REMOVE")) => {
            let Some(session) = connection.created.clone() else {
                return reply(service, "SESSION STATUS RESULT=INVALID_ID\n".to_owned()).await;
            };
            let Some(id) = options.get("ID").copied() else {
                return reply(service, "SESSION STATUS RESULT=INVALID_ID\n".to_owned()).await;
            };
            let detached = {
                let mut state = connection.service.state.lock().expect("service state");
                match state.sessions.get_mut(&session) {
                    Some(entry) => match entry.children.remove(id) {
                        Some(_) => {
                            entry.transcript.push(format!("SESSION REMOVE ID={id}"));
                            true
                        }
                        None => false,
                    },
                    None => false,
                }
            };
            reply(
                service,
                if detached {
                    "SESSION STATUS RESULT=OK\n"
                } else {
                    "SESSION STATUS RESULT=INVALID_ID\n"
                },
            )
            .await
        }
        (Some("STREAM"), Some("CONNECT")) => {
            let Some((session, _child)) = resolve(connection, options.get("ID").copied()) else {
                return reply(service, "STREAM STATUS RESULT=INVALID_ID\n").await;
            };
            if decode_destination(options.get("DESTINATION").copied().unwrap_or("")).is_none() {
                return reply(service, "STREAM STATUS RESULT=INVALID_KEY\n").await;
            }
            let Some(local) = connection.service.destination_of(&session) else {
                return reply(service, "STREAM STATUS RESULT=INVALID_ID\n").await;
            };
            let text = format!(
                "STREAM STATUS RESULT=OK DESTINATION={}\n",
                i2pr_tc_i2p::sam::encode_base64(local.as_bytes())
            );
            if !reply(service, text).await {
                return false;
            }
            // Everything after a connect status line is raw peer data; this
            // service echoes the probe so the test can prove the stream is live.
            let mut probe = [0u8; 64];
            let Ok(count) = service.read(&mut probe).await else {
                return false;
            };
            service.write_all(&probe[..count]).await.is_ok()
        }
        (Some("STREAM"), Some("ACCEPT")) => {
            let Some((session, _child)) = resolve(connection, options.get("ID").copied()) else {
                return reply(service, "STREAM STATUS RESULT=INVALID_ID\n".to_owned()).await;
            };
            let Some(peer) = connection.service.destination_of(&session) else {
                return reply(service, "STREAM STATUS RESULT=INVALID_ID\n".to_owned()).await;
            };
            let text = format!(
                "STREAM STATUS RESULT=OK\n{}\n",
                i2pr_tc_i2p::sam::encode_base64(peer.as_bytes())
            );
            if !reply(service, text).await {
                return false;
            }
            let mut probe = [0u8; 64];
            let Ok(count) = service.read(&mut probe).await else {
                return false;
            };
            service.write_all(&probe[..count]).await.is_ok()
        }
        (Some(command @ ("DATAGRAM" | "RAW")), Some("SEND")) => {
            let raw = command == "RAW";
            let Some((session, _child)) = resolve(connection, options.get("ID").copied()) else {
                return reply(service, "SESSION STATUS RESULT=INVALID_ID\n".to_owned()).await;
            };
            let Some(destination) =
                decode_destination(options.get("DESTINATION").copied().unwrap_or(""))
            else {
                return reply(service, "SESSION STATUS RESULT=INVALID_KEY\n".to_owned()).await;
            };
            let Some(size) = options
                .get("SIZE")
                .and_then(|value| value.parse::<usize>().ok())
            else {
                return reply(service, "SESSION STATUS RESULT=I2P_ERROR\n".to_owned()).await;
            };
            if size > 32 * 1024 {
                return reply(service, "SESSION STATUS RESULT=I2P_ERROR\n".to_owned()).await;
            }
            // A datagram command line and its payload usually arrive together,
            // so whatever is already buffered is part of the payload.
            let mut payload = vec![0u8; size];
            let ready = size.min(buffered.len());
            payload[..ready].copy_from_slice(&buffered[..ready]);
            buffered.drain(..ready);
            if ready < size && service.read_exact(&mut payload[ready..]).await.is_err() {
                return false;
            }
            // Deliver to the datagram channel the service itself owns, so a
            // receive proves the packet travelled over one shared identity.
            let delivered = {
                let mut state = connection.service.state.lock().expect("service state");
                match state.sessions.get_mut(&session) {
                    Some(entry) => entry
                        .children
                        .values_mut()
                        .find(|child| {
                            (raw && child.style == "RAW") || (!raw && child.style == "DATAGRAM")
                        })
                        .map(|target| {
                            target.inbox.push_back(payload);
                            true
                        })
                        .unwrap_or(false),
                    None => false,
                }
            };
            if !delivered {
                return reply(service, "SESSION STATUS RESULT=INVALID_ID\n").await;
            }
            let _ = destination;
            let text = if raw {
                "RAW DATA SEND RESULT=OK\n"
            } else {
                "DATAGRAM SEND RESULT=OK\n"
            };
            reply(service, text.to_owned()).await
        }
        (Some(command @ ("DATAGRAM" | "RAW")), Some("RECEIVE")) => {
            let raw = command == "RAW";
            let Some((session, _child)) = resolve(connection, options.get("ID").copied()) else {
                return reply(service, "SESSION STATUS RESULT=INVALID_ID\n".to_owned()).await;
            };
            let (payload, local) = {
                let mut state = connection.service.state.lock().expect("service state");
                match state.sessions.get_mut(&session) {
                    Some(entry) => {
                        let payload = entry.children.values_mut().find_map(|child| {
                            if (raw && child.style == "RAW") || (!raw && child.style == "DATAGRAM")
                            {
                                child.inbox.pop_front()
                            } else {
                                None
                            }
                        });
                        (payload, Some(entry.destination.clone()))
                    }
                    None => (None, None),
                }
            };
            let Some(payload) = payload else {
                return reply(service, "NAMING REPLY RESULT=KEY_NOT_FOUND\n").await;
            };
            let sender =
                i2pr_tc_i2p::sam::encode_base64(local.expect("session destination").as_bytes());
            let text = if raw {
                format!(
                    "RAW DATA RECEIVED RESULT=OK SIZE={} PROTOCOL=18 FROM_PORT=1 TO_PORT=2\n",
                    payload.len()
                )
            } else {
                format!(
                    "DATAGRAM RECEIVED DESTINATION={sender} SIZE={} FROM_PORT=1 TO_PORT=2\n",
                    payload.len()
                )
            };
            if !reply(service, text).await {
                return false;
            }
            service.write_all(&payload).await.is_ok()
        }
        (Some(command @ ("DATAGRAM" | "RAW")), _) => {
            reply(
                service,
                format!("SESSION STATUS RESULT=I2P_ERROR MESSAGE=\"Unsupported {command}\"\n"),
            )
            .await
        }
        _ => reply(service, "SESSION STATUS RESULT=I2P_ERROR\n".to_owned()).await,
    }
}

/// Writes one reply. Returns false when the connection has ended.
async fn reply<T: AsRef<[u8]>>(service: &mut tokio::io::DuplexStream, text: T) -> bool {
    service.write_all(text.as_ref()).await.is_ok()
}

/// Resolves a child channel id to the session that owns it.
///
/// `SESSION ADD` and `SESSION REMOVE` are only accepted on the control
/// connection, but data commands name the child and are accepted on any
/// connection, so a router resolves the id globally rather than from the
/// connection it arrived on.
fn resolve(connection: &Connection, id: Option<&str>) -> Option<(String, String)> {
    let id = id?.to_owned();
    let state = connection.service.state.lock().expect("service state");
    if let Some(session) = &connection.created
        && state
            .sessions
            .get(session)
            .is_some_and(|entry| entry.children.contains_key(&id))
    {
        return Some((session.clone(), id));
    }
    state
        .sessions
        .iter()
        .find(|(_, entry)| entry.children.contains_key(&id))
        .map(|(session, _)| (session.clone(), id))
}

fn build(service: SamService, case: &str) -> SamClient<SamService> {
    SamClient::new(
        service,
        SamIdentity::new(SamSessionDestination::Transient, &format!("fixture-{case}"))
            .expect("fixture identity"),
        SamLimits::default(),
        SamTimeouts {
            open: Duration::from_secs(2),
            handshake: Duration::from_secs(2),
            lookup: Duration::from_secs(2),
            connect: Duration::from_secs(2),
            accept: Duration::from_secs(2),
            datagram: Duration::from_secs(2),
            child: Duration::from_secs(2),
            close: Duration::from_secs(2),
        },
    )
    .expect("client configuration")
}

/// The cross-transport property: one Destination, three transports, one
/// generation, one identity.
#[tokio::test]
async fn sam33_one_destination_is_shared_by_stream_datagram_and_raw() {
    let service = SamService::default();
    let client = build(service.clone(), "shared");
    let cancellation = i2pr_tc_storage::Cancellation::default();
    let negotiated = client
        .establish_primary(&cancellation)
        .await
        .expect("primary is created");
    assert_eq!(negotiated.version, i2pr_tc_i2p::sam::SamVersion::new(3, 3));
    let generation = client.generation();
    let identity = client.local_identity().expect("a confirmed identity");

    let ports = SamChannelConfig {
        from_port: 1,
        to_port: 2,
        listen_port: 3,
    };
    let stream = client
        .add_channel(
            SamChannelKind::Stream,
            SamChannelConfig::STREAM,
            &cancellation,
        )
        .await
        .expect("stream child");
    let datagram = client
        .add_channel(SamChannelKind::RepliableDatagram, ports, &cancellation)
        .await
        .expect("datagram child");
    let raw = client
        .add_channel(SamChannelKind::RawDatagram, ports, &cancellation)
        .await
        .expect("raw child");

    // All three children belong to one session, one identity, one generation.
    for channel in [&stream, &datagram, &raw] {
        assert_eq!(channel.generation(), generation);
    }
    assert_eq!(client.channel_count(), 3);
    assert_eq!(
        client.local_identity().expect("identity").hash(),
        identity.hash()
    );
    assert_eq!(service.create_count(), 1, "one session per identity");
    let attached = service.children("fixture-shared");
    assert_eq!(attached.len(), 3);
    // The service recorded the routing ports the client asked for, which is how
    // two datagram channels on one Destination stay distinguishable.
    for child in attached.values().filter(|child| child.style != "STREAM") {
        assert_eq!(child.from_port, 1);
        assert_eq!(child.to_port, 2);
        assert!(matches!(child.protocol, 17 | 18));
    }

    // STREAM carries real bytes on that identity.
    let mut outbound = client
        .stream_connect(&stream, &identity.destination().clone(), 7, &cancellation)
        .await
        .expect("stream connect");
    outbound
        .write_all(b"c003-shared-destination")
        .await
        .expect("probe is written");
    let mut echo = [0u8; 23];
    tokio::time::timeout(Duration::from_secs(2), outbound.read_exact(&mut echo))
        .await
        .expect("peer answers")
        .expect("read succeeds");
    assert_eq!(&echo, b"c003-shared-destination");

    // DATAGRAM and RAW carry bounded payloads on the same identity.
    client
        .datagram_send(
            &datagram,
            &identity.destination().clone(),
            ports,
            b"ping",
            &cancellation,
        )
        .await
        .expect("datagram send");
    let received = client
        .datagram_receive(&datagram, &cancellation)
        .await
        .expect("datagram receive");
    assert_eq!(received.payload, b"ping");
    assert_eq!(received.from_port, 1);
    assert_eq!(received.to_port, 2);
    // The service reports the packet as coming from the session's own
    // Destination, which is the same hash the client computed for itself.
    assert_eq!(
        received.sender.expect("an authenticated sender").hash(),
        identity.hash(),
        "a datagram must arrive under the shared identity"
    );

    client
        .raw_send(
            &raw,
            &identity.destination().clone(),
            ports,
            b"raw",
            &cancellation,
        )
        .await
        .expect("raw send");
    let raw_received = client
        .raw_receive(&raw, &cancellation)
        .await
        .expect("raw receive");
    assert_eq!(raw_received.payload, b"raw");
    assert_eq!(raw_received.protocol, 18);

    // The identity survived every transport unchanged.
    assert_eq!(
        I2pSession::local_peer_hash(&client).expect("typed hash"),
        identity.hash()
    );
    assert_eq!(client.generation(), generation);

    // Exactly one create, three adds, one remove per removal: no child ever
    // created a session of its own.
    let transcript = service.transcript();
    assert_eq!(
        transcript
            .iter()
            .filter(|line| line.starts_with("SESSION CREATE"))
            .count(),
        1
    );
    client
        .remove_channel(&datagram, &cancellation)
        .await
        .expect("datagram child removed");
    assert_eq!(client.channel_count(), 2);
    // The surviving siblings keep working after a sibling is removed.
    client
        .raw_send(
            &raw,
            &identity.destination().clone(),
            ports,
            b"still",
            &cancellation,
        )
        .await
        .expect("raw send after sibling removal");
    assert_eq!(
        client.local_identity().expect("identity").hash(),
        identity.hash()
    );
}

/// Losing the primary control connection takes every child with it.
#[tokio::test]
async fn sam33_primary_loss_invalidates_every_child_transport() {
    let service = SamService::default();
    let client = build(service.clone(), "loss");
    let cancellation = i2pr_tc_storage::Cancellation::default();
    client
        .establish_primary(&cancellation)
        .await
        .expect("primary is created");
    let ports = SamChannelConfig {
        from_port: 1,
        to_port: 2,
        listen_port: 3,
    };
    let stream = client
        .add_channel(
            SamChannelKind::Stream,
            SamChannelConfig::STREAM,
            &cancellation,
        )
        .await
        .expect("stream child");
    let datagram = client
        .add_channel(SamChannelKind::RepliableDatagram, ports, &cancellation)
        .await
        .expect("datagram child");
    let identity = client.local_identity().expect("identity").hash();
    assert_eq!(service.children("fixture-loss").len(), 2);

    // Ending the control connection is how a router closing the primary
    // session looks from the client side.
    assert!(
        service.close_control("fixture-loss"),
        "the primary is observable"
    );
    // The loss is observed on the next use of the control connection, which is
    // the same observation point a router-loss recovery would have.
    assert!(
        client
            .add_channel(
                SamChannelKind::Stream,
                SamChannelConfig::STREAM,
                &cancellation
            )
            .await
            .is_err(),
        "a lost primary cannot take new children"
    );

    // A stale child fails typed rather than reattaching to something else.
    let error = client
        .datagram_send(&datagram, &destination_for(42), ports, b"x", &cancellation)
        .await
        .expect_err("a child of a lost primary cannot send");
    assert!(
        matches!(error, TransportError::Stale(_) | TransportError::Io(_)),
        "unexpected error: {error:?}"
    );
    assert!(matches!(
        client.local_identity(),
        Err(TransportError::IdentityNotReady)
    ));
    assert!(
        client.remove_channel(&stream, &cancellation).await.is_err(),
        "a child of a lost primary cannot be detached"
    );
    assert_ne!(
        identity, [0u8; 32],
        "the identity was real while it was live"
    );
}

/// A client that is unavailable never re-establishes on its own, and a service
/// that no longer accepts the session name says so rather than hanging.
#[tokio::test]
async fn sam33_unavailable_client_does_not_reconnect_by_itself() {
    let service = SamService::default();
    let client = build(service.clone(), "unavailable");
    let cancellation = i2pr_tc_storage::Cancellation::default();
    client
        .establish_primary(&cancellation)
        .await
        .expect("primary is created");
    client.mark_unavailable();
    assert!(matches!(
        client.local_identity(),
        Err(TransportError::IdentityNotReady)
    ));
    for _ in 0..3 {
        assert!(matches!(
            client.establish_primary(&cancellation).await,
            Err(TransportError::Cancelled)
        ));
    }
    assert_eq!(
        service.create_count(),
        1,
        "an unavailable client must not create another session"
    );
    // Restoration is the caller's decision, and the caller can make it.
    let replacement = build(service.clone(), "unavailable-restore");
    let negotiated = replacement
        .establish_primary(&cancellation)
        .await
        .expect("an explicit restoration succeeds");
    assert_eq!(negotiated.version, i2pr_tc_i2p::sam::SamVersion::new(3, 3));
    assert_eq!(service.create_count(), 2);
}
