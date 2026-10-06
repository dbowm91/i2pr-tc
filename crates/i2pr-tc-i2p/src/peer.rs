//! Peer-wire over I2P streams, with bounded framing and service ownership.
use crate::{
    I2pSession, TransportError,
    identity::{DestinationHash, I2pPeer, resolve_peer},
    metadata::{MAX_METADATA_BYTES, METADATA_BLOCK_BYTES, MetadataAssembler, MetadataError},
    pex::{PeerSourceSet, decode_i2p_pex, encode_i2p_pex},
    tracker::race_cancel,
};
use i2pr_tc_core::{
    bencode,
    extension::{MetadataLimits, UtMetadata, parse_extension_map, parse_ut_metadata},
    service::{ServiceError, TorrentId, TorrentService},
    wire::{FrameDecoder, Handshake, Message, PeerEvent, encode_frame, encode_handshake},
};
use i2pr_tc_storage::{Cancellation, TorrentRuntime};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    task::JoinSet,
};

const MAX_PEER_FRAME: usize = 2 * 1024 * 1024;
const MAX_READ_CHUNK: usize = 16 * 1024;
const LOCAL_UT_PEX_ID: u8 = 1;
const LOCAL_UT_METADATA_ID: u8 = 2;

#[derive(Debug, Error)]
pub enum PeerError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("peer protocol failed")]
    Protocol,
    #[error("peer request or service transition failed: {0:?}")]
    Service(ServiceError),
    #[error("metadata exchange failed: {0}")]
    Metadata(#[from] MetadataError),
}

impl From<ServiceError> for PeerError {
    fn from(error: ServiceError) -> Self {
        Self::Service(error)
    }
}

impl From<std::io::Error> for PeerError {
    fn from(error: std::io::Error) -> Self {
        Self::Transport(TransportError::Io(error))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PeerLimits {
    pub handshake_timeout: Duration,
    pub idle_timeout: Duration,
    pub request_window: usize,
    pub max_pex_peers: usize,
    pub max_metadata_size: usize,
}

impl Default for PeerLimits {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(120),
            idle_timeout: Duration::from_secs(180),
            request_window: 4,
            max_pex_peers: 128,
            max_metadata_size: MAX_METADATA_BYTES,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerRunReport {
    pub peer_id: Option<[u8; 20]>,
    pub downloaded_pieces: u64,
    pub discovered_peers: Vec<I2pPeer>,
    pub metadata_promoted: bool,
}

struct SessionState {
    ext: BTreeMapExtensions,
    metadata: Option<MetadataAssembler>,
    pex_seen: BTreeSet<[u8; 32]>,
    sources: Arc<Mutex<PeerSourceSet>>,
    active_piece: Option<u32>,
    next_begin: u32,
    in_flight: usize,
    pending_availability: Vec<Message>,
    report: PeerRunReport,
}

struct PeerContext<'a> {
    runtime: &'a TorrentRuntime,
    id: TorrentId,
    peer: DestinationHash,
    local_hash: DestinationHash,
    cancellation: &'a Cancellation,
    limits: PeerLimits,
    sources: Arc<Mutex<PeerSourceSet>>,
}

/// Per-torrent inputs shared by outbound and inbound peer sessions.
pub struct PeerClient<'a> {
    pub runtime: &'a TorrentRuntime,
    pub torrent: TorrentId,
    pub local_peer_id: [u8; 20],
    pub cancellation: &'a Cancellation,
    pub limits: PeerLimits,
    pub sources: Arc<Mutex<PeerSourceSet>>,
}

#[derive(Default)]
struct BTreeMapExtensions(std::collections::BTreeMap<String, u8>);

pub async fn connect_peer<S: I2pSession + ?Sized>(
    session: &S,
    client: &PeerClient<'_>,
    peer: &I2pPeer,
) -> Result<PeerRunReport, PeerError> {
    validate_limits(client.limits)?;
    let destination = tokio::time::timeout(
        client.limits.handshake_timeout,
        race_cancel(resolve_peer(session, peer), client.cancellation),
    )
    .await
    .map_err(|_| PeerError::Transport(TransportError::Timeout))??
    .map_err(PeerError::Transport)?;
    let stream = tokio::time::timeout(
        client.limits.handshake_timeout,
        race_cancel(session.connect(&destination, 0), client.cancellation),
    )
    .await
    .map_err(|_| PeerError::Transport(TransportError::Timeout))??
    .map_err(PeerError::Transport)?;
    let local_hash = DestinationHash(session.local_peer_hash());
    let context = PeerContext {
        runtime: client.runtime,
        id: client.torrent,
        peer: peer.hash,
        local_hash,
        cancellation: client.cancellation,
        limits: client.limits,
        sources: client.sources.clone(),
    };
    run_outbound_stream(&context, stream, client.local_peer_id).await
}

async fn run_outbound_stream(
    context: &PeerContext<'_>,
    mut stream: crate::I2pStream,
    local_peer_id: [u8; 20],
) -> Result<PeerRunReport, PeerError> {
    validate_limits(context.limits)?;
    let snapshot = context.runtime.service().get(context.id)?;
    let meta = context.runtime.service().get_metainfo(context.id)?;
    let mut local = local_handshake(snapshot.info_hash, local_peer_id);
    local.reserved[5] |= 0x10;
    write_handshake(
        &mut stream,
        &local,
        context.cancellation,
        context.limits.handshake_timeout,
    )
    .await?;
    let remote = read_handshake(
        &mut stream,
        context.cancellation,
        context.limits.handshake_timeout,
    )
    .await?;
    eprintln!("DBG got remote handshake");
    if remote.info_hash != snapshot.info_hash {
        return Err(PeerError::Protocol);
    }
    context
        .runtime
        .accept_peer_handshake(context.id, context.peer.0, &encode_handshake(&remote))
        .map_err(PeerError::Service)?;
    if let Err(error) = write_extended_handshake(
        &mut stream,
        meta.as_ref().map(|meta| meta.info_bytes.len()),
        context.cancellation,
    )
    .await
    {
        let _ = context.runtime.disconnect_peer(context.id, context.peer.0);
        return Err(error);
    }
    run_messages(context, stream, remote.peer_id).await
}

async fn run_inbound_stream(
    context: &PeerContext<'_>,
    mut stream: crate::I2pStream,
    remote: Handshake,
    local_peer_id: [u8; 20],
) -> Result<PeerRunReport, PeerError> {
    validate_limits(context.limits)?;
    let snapshot = context.runtime.service().get(context.id)?;
    let meta = context.runtime.service().get_metainfo(context.id)?;
    if remote.info_hash != snapshot.info_hash {
        return Err(PeerError::Protocol);
    }
    context
        .runtime
        .accept_peer_handshake(context.id, context.peer.0, &encode_handshake(&remote))
        .map_err(PeerError::Service)?;
    let mut local = local_handshake(snapshot.info_hash, local_peer_id);
    local.reserved[5] |= 0x10;
    if let Err(error) = write_handshake(
        &mut stream,
        &local,
        context.cancellation,
        context.limits.handshake_timeout,
    )
    .await
    {
        let _ = context.runtime.disconnect_peer(context.id, context.peer.0);
        return Err(error);
    }
    if let Err(error) = write_extended_handshake(
        &mut stream,
        meta.as_ref().map(|meta| meta.info_bytes.len()),
        context.cancellation,
    )
    .await
    {
        let _ = context.runtime.disconnect_peer(context.id, context.peer.0);
        return Err(error);
    }
    run_messages(context, stream, remote.peer_id).await
}

/// Accept streams from the router-owned I2P session with a hard connection
/// ceiling. Every child task is joined or aborted when cancellation wins.
pub async fn serve_incoming<S: I2pSession + ?Sized + 'static>(
    session: Arc<S>,
    runtime: Arc<TorrentRuntime>,
    local_peer_id: [u8; 20],
    cancellation: Cancellation,
    limits: PeerLimits,
    max_peers: usize,
    sources: Arc<Mutex<PeerSourceSet>>,
) -> Result<(), PeerError> {
    validate_limits(limits)?;
    if max_peers == 0 || max_peers > 1024 {
        return Err(PeerError::Protocol);
    }
    let mut children = JoinSet::new();
    let local_hash = DestinationHash(session.local_peer_hash());
    loop {
        if cancellation.is_cancelled() {
            children.abort_all();
            while children.join_next().await.is_some() {}
            return Ok(());
        }
        tokio::select! {
            biased;
            _ = cancellation_tick(&cancellation) => {
                children.abort_all();
                while children.join_next().await.is_some() {}
                return Ok(());
            }
            Some(_) = children.join_next(), if !children.is_empty() => {}
            accepted = session.accept(), if children.len() < max_peers => {
                let (destination, stream) = accepted.map_err(PeerError::Transport)?;
                let runtime = runtime.clone();
                let token = cancellation.clone();
                let peer_sources = sources.clone();
                children.spawn(async move {
                    let mut stream = stream;
                    if let Ok(remote) =
                        read_handshake(&mut stream, &token, limits.handshake_timeout).await
                        && let Ok(Some(id)) = find_torrent(runtime.service(), remote.info_hash)
                    {
                        let context = PeerContext {
                            runtime: &runtime,
                            id,
                            peer: DestinationHash(destination.hash()),
                            local_hash,
                            cancellation: &token,
                            limits,
                            sources: peer_sources,
                        };
                        let _ =
                            run_inbound_stream(&context, stream, remote, local_peer_id).await;
                    }
                });
            }
        }
    }
}

fn find_torrent(
    service: &i2pr_tc_storage::PersistentTorrentService,
    info_hash: [u8; 20],
) -> Result<Option<TorrentId>, ServiceError> {
    for snapshot in service.list()? {
        if snapshot.info_hash == info_hash {
            return Ok(Some(snapshot.id));
        }
    }
    Ok(None)
}

fn validate_limits(limits: PeerLimits) -> Result<(), PeerError> {
    if limits.handshake_timeout.is_zero()
        || limits.idle_timeout.is_zero()
        || limits.request_window == 0
        || limits.request_window > 64
        || limits.max_pex_peers > 4096
        || limits.max_metadata_size == 0
        || limits.max_metadata_size > MAX_METADATA_BYTES
    {
        return Err(PeerError::Protocol);
    }
    Ok(())
}

fn local_handshake(info_hash: [u8; 20], peer_id: [u8; 20]) -> Handshake {
    Handshake {
        reserved: [0; 8],
        info_hash,
        peer_id,
    }
}

async fn write_handshake<W: AsyncWrite + Unpin>(
    stream: &mut W,
    handshake: &Handshake,
    cancellation: &Cancellation,
    timeout: Duration,
) -> Result<(), PeerError> {
    let bytes = encode_handshake(handshake);
    let write = tokio::time::timeout(timeout, stream.write_all(&bytes));
    race_cancel(write, cancellation)
        .await?
        .map_err(|_| PeerError::Transport(TransportError::Timeout))??;
    Ok(())
}

async fn read_handshake<R: AsyncRead + Unpin>(
    stream: &mut R,
    cancellation: &Cancellation,
    timeout: Duration,
) -> Result<Handshake, PeerError> {
    let mut bytes = [0; 68];
    let read = tokio::time::timeout(timeout, stream.read_exact(&mut bytes));
    race_cancel(read, cancellation)
        .await?
        .map_err(|_| PeerError::Transport(TransportError::Timeout))??;
    i2pr_tc_core::wire::parse_handshake(&bytes).map_err(|_| PeerError::Protocol)
}

async fn write_extended_handshake<W: AsyncWrite + Unpin>(
    stream: &mut W,
    metadata_size: Option<usize>,
    cancellation: &Cancellation,
) -> Result<(), PeerError> {
    let payload = encode_extension_handshake(metadata_size);
    write_message(stream, &Message::Extension { id: 0, payload }, cancellation).await
}

async fn run_messages(
    context: &PeerContext<'_>,
    stream: crate::I2pStream,
    remote_peer_id: [u8; 20],
) -> Result<PeerRunReport, PeerError> {
    let result = run_messages_inner(context, stream, remote_peer_id).await;
    // Release peer-owned requests and availability on every terminal path.
    let _ = context.runtime.disconnect_peer(context.id, context.peer.0);
    result
}

async fn run_messages_inner(
    context: &PeerContext<'_>,
    mut stream: crate::I2pStream,
    remote_peer_id: [u8; 20],
) -> Result<PeerRunReport, PeerError> {
    let snapshot = context.runtime.service().get(context.id)?;
    let meta = context.runtime.service().get_metainfo(context.id)?;
    let mut state = SessionState {
        ext: BTreeMapExtensions::default(),
        metadata: if meta.is_none() {
            Some(MetadataAssembler::new(
                i2pr_tc_core::InfoHashV1(snapshot.info_hash),
                context.limits.max_metadata_size,
            )?)
        } else {
            None
        },
        pex_seen: BTreeSet::new(),
        sources: context.sources.clone(),
        active_piece: None,
        next_begin: 0,
        in_flight: 0,
        pending_availability: Vec::new(),
        report: PeerRunReport {
            peer_id: Some(remote_peer_id),
            ..PeerRunReport::default()
        },
    };
    let mut decoder = FrameDecoder::new(MAX_PEER_FRAME).map_err(|_| PeerError::Protocol)?;
    let mut buffer = vec![0; MAX_READ_CHUNK];
    loop {
        if context.cancellation.is_cancelled() {
            return Err(PeerError::Transport(TransportError::Cancelled));
        }
        let read = tokio::time::timeout(context.limits.idle_timeout, stream.read(&mut buffer));
        let count = race_cancel(read, context.cancellation)
            .await?
            .map_err(|_| PeerError::Transport(TransportError::Timeout))??;
        if count == 0 {
            return Ok(state.report);
        }
        let messages = decoder
            .feed(&buffer[..count])
            .map_err(|_| PeerError::Protocol)?;
        for message in messages {
            handle_message(context, &mut stream, &mut state, message).await?;
        }
    }
}

async fn handle_message<W: AsyncWrite + Unpin>(
    context: &PeerContext<'_>,
    stream: &mut W,
    state: &mut SessionState,
    message: Message,
) -> Result<(), PeerError> {
    if let Message::Extension { id: 0, payload } = &message {
        let map = parse_extension_map(payload, 64).map_err(|_| PeerError::Protocol)?;
        let mut used = BTreeSet::new();
        if map.values().any(|id| *id == 0 || !used.insert(*id)) {
            return Err(PeerError::Protocol);
        }
        state.ext.0 = map;
        if state.metadata.is_some()
            && state.ext.0.contains_key("ut_metadata")
            && let Some(size) = parse_metadata_size(payload, context.limits.max_metadata_size)?
        {
            state.metadata.as_mut().unwrap().set_size(size)?;
            send_metadata_requests(stream, state, context.cancellation).await?;
        }
    }
    if let Message::Extension {
        id: extension_id,
        payload,
    } = &message
    {
        if state.ext.0.get("i2p_pex") == Some(extension_id) {
            let pex = decode_i2p_pex(payload, context.limits.max_pex_peers)
                .map_err(|_| PeerError::Protocol)?;
            let additions = state
                .sources
                .lock()
                .map_err(|_| PeerError::Protocol)?
                .apply_pex(&pex, context.local_hash, context.limits.max_pex_peers)
                .map_err(|_| PeerError::Protocol)?;
            for addition in additions {
                if state.pex_seen.insert(addition.hash.0) {
                    state.report.discovered_peers.push(addition);
                }
            }
        }
        if state.ext.0.get("ut_metadata") == Some(extension_id) {
            handle_metadata(context, stream, state, payload).await?;
        }
    }
    if let Message::Extension { id: 0, .. } = &message
        && state.ext.0.contains_key("i2p_pex")
    {
        let added = state
            .sources
            .lock()
            .map_err(|_| PeerError::Protocol)?
            .advertisable(context.peer, context.limits.max_pex_peers);
        if !added.is_empty() {
            let payload = encode_i2p_pex(&i2pr_tc_core::wire::I2pPex {
                added,
                dropped: Vec::new(),
            });
            write_message(
                stream,
                &Message::Extension {
                    id: LOCAL_UT_PEX_ID,
                    payload,
                },
                context.cancellation,
            )
            .await?;
        }
    }
    if state.metadata.is_some() && matches!(&message, Message::Bitfield(_) | Message::Have(_)) {
        if state.pending_availability.len() >= 4096
            || (matches!(&message, Message::Bitfield(_))
                && state
                    .pending_availability
                    .iter()
                    .any(|m| matches!(m, Message::Bitfield(_))))
        {
            return Err(PeerError::Protocol);
        }
        state.pending_availability.push(message);
        return Ok(());
    }
    if state.metadata.is_none() && !state.pending_availability.is_empty() {
        for pending in state.pending_availability.drain(..) {
            context
                .runtime
                .process_peer_message(context.id, context.peer.0, pending)
                .map_err(PeerError::Service)?;
        }
        schedule_requests(
            context.runtime,
            context.id,
            context.peer,
            stream,
            state,
            context.cancellation,
            context.limits,
        )
        .await?;
    }
    let outcome = context
        .runtime
        .process_peer_message(context.id, context.peer.0, message)
        .map_err(PeerError::Service)?;
    for cancel in outcome.cancellations {
        write_message(stream, &cancel, context.cancellation).await?;
    }
    match outcome.event {
        PeerEvent::UploadRequest {
            index,
            begin,
            length,
        } => {
            if let Ok(block) = context
                .runtime
                .read_verified_block(context.id, index, begin, length)
            {
                write_message(
                    stream,
                    &Message::Piece {
                        index,
                        begin,
                        block,
                    },
                    context.cancellation,
                )
                .await?;
            }
        }
        PeerEvent::Choked => {
            state.active_piece = None;
            state.in_flight = 0;
        }
        PeerEvent::Unchoked | PeerEvent::Have(_) | PeerEvent::Bitfield(_) => {
            schedule_requests(
                context.runtime,
                context.id,
                context.peer,
                stream,
                state,
                context.cancellation,
                context.limits,
            )
            .await?;
        }
        PeerEvent::DownloadPiece { .. } => {
            if outcome.completed_piece == Some(true) {
                state.report.downloaded_pieces = state.report.downloaded_pieces.saturating_add(1);
                state.active_piece = None;
                state.next_begin = 0;
            } else {
                state.in_flight = state.in_flight.saturating_sub(1);
            }
            schedule_requests(
                context.runtime,
                context.id,
                context.peer,
                stream,
                state,
                context.cancellation,
                context.limits,
            )
            .await?;
        }
        _ => {}
    }
    Ok(())
}

async fn schedule_requests<W: AsyncWrite + Unpin>(
    runtime: &TorrentRuntime,
    id: TorrentId,
    peer: DestinationHash,
    stream: &mut W,
    state: &mut SessionState,
    cancellation: &Cancellation,
    limits: PeerLimits,
) -> Result<(), PeerError> {
    if state.active_piece.is_none() {
        let Some(piece) = runtime.choose_piece(id, peer.0)? else {
            return Ok(());
        };
        state.active_piece = Some(piece);
        state.next_begin = 0;
    }
    let meta = runtime
        .service()
        .get_metainfo(id)?
        .ok_or(PeerError::Protocol)?;
    let piece = state.active_piece.ok_or(PeerError::Protocol)?;
    let offset = piece as u64 * meta.piece_length as u64;
    let piece_size = (meta.total_length - offset).min(meta.piece_length as u64) as u32;
    while state.in_flight < limits.request_window && state.next_begin < piece_size {
        let length = (piece_size - state.next_begin).min(METADATA_BLOCK_BYTES as u32);
        let request = match runtime.request_block(id, peer.0, piece, state.next_begin, length) {
            Ok(request) => request,
            Err(ServiceError::Conflict) => break,
            Err(error) => return Err(PeerError::Service(error)),
        };
        write_message(
            stream,
            &Message::Request {
                index: request.piece,
                begin: request.begin,
                length: request.length,
            },
            cancellation,
        )
        .await?;
        state.next_begin += length;
        state.in_flight += 1;
    }
    Ok(())
}

async fn handle_metadata<W: AsyncWrite + Unpin>(
    context: &PeerContext<'_>,
    stream: &mut W,
    state: &mut SessionState,
    payload: &[u8],
) -> Result<(), PeerError> {
    match parse_ut_metadata(
        payload,
        MetadataLimits {
            total_size: MAX_METADATA_BYTES as u32,
            block_size: METADATA_BLOCK_BYTES,
            encoded_header: 1024,
        },
    )
    .map_err(|_| PeerError::Protocol)?
    {
        UtMetadata::Request { piece } => {
            if state.metadata.is_some() {
                return send_metadata_reject(stream, piece, context.cancellation).await;
            }
            let bytes = context
                .runtime
                .service()
                .get_metainfo_bytes(context.id)?
                .ok_or(PeerError::Protocol)?;
            let info = i2pr_tc_core::metainfo::parse(&bytes, Default::default())
                .map_err(|_| PeerError::Protocol)?
                .info_bytes;
            let start = piece as usize * METADATA_BLOCK_BYTES;
            if start >= info.len() {
                return send_metadata_reject(stream, piece, context.cancellation).await;
            }
            let end = info.len().min(start + METADATA_BLOCK_BYTES);
            let mut response = format!(
                "d8:msg_typei1e5:piecei{piece}e10:total_sizei{}ee",
                info.len()
            )
            .into_bytes();
            response.extend_from_slice(&info[start..end]);
            write_message(
                stream,
                &Message::Extension {
                    id: LOCAL_UT_METADATA_ID,
                    payload: response,
                },
                context.cancellation,
            )
            .await?;
        }
        data @ (UtMetadata::Data { .. } | UtMetadata::Reject { .. }) => {
            let Some(assembler) = state.metadata.as_mut() else {
                return Err(PeerError::Protocol);
            };
            let mut encoded = match data {
                UtMetadata::Data {
                    piece,
                    total_size,
                    block,
                } => {
                    let mut encoded =
                        format!("d8:msg_typei1e5:piecei{piece}e10:total_sizei{total_size}ee")
                            .into_bytes();
                    encoded.extend_from_slice(&block);
                    encoded
                }
                UtMetadata::Reject { piece } => {
                    format!("d8:msg_typei2e5:piecei{piece}ee").into_bytes()
                }
                UtMetadata::Request { .. } => unreachable!(),
            };
            if let Some(metainfo) = assembler.accept_payload(&encoded)? {
                let promoted = context.runtime.service().add_metainfo(&metainfo)?;
                if promoted != context.id {
                    return Err(PeerError::Protocol);
                }
                context.runtime.refresh_metainfo(context.id)?;
                state.metadata = None;
                state.report.metadata_promoted = true;
                return Ok(());
            }
            encoded.clear();
            send_metadata_requests(stream, state, context.cancellation).await?;
        }
    }
    Ok(())
}

async fn send_metadata_reject<W: AsyncWrite + Unpin>(
    stream: &mut W,
    piece: u32,
    cancellation: &Cancellation,
) -> Result<(), PeerError> {
    let payload = format!("d8:msg_typei2e5:piecei{piece}ee").into_bytes();
    write_message(
        stream,
        &Message::Extension {
            id: LOCAL_UT_METADATA_ID,
            payload,
        },
        cancellation,
    )
    .await
}

async fn send_metadata_requests<W: AsyncWrite + Unpin>(
    stream: &mut W,
    state: &mut SessionState,
    cancellation: &Cancellation,
) -> Result<(), PeerError> {
    let remote_id = state
        .ext
        .0
        .get("ut_metadata")
        .copied()
        .ok_or(PeerError::Protocol)?;
    let Some(assembler) = state.metadata.as_mut() else {
        return Ok(());
    };
    for piece in assembler.next_requests(4) {
        let payload = format!("d8:msg_typei0e5:piecei{piece}ee").into_bytes();
        write_message(
            stream,
            &Message::Extension {
                id: remote_id,
                payload,
            },
            cancellation,
        )
        .await?;
    }
    Ok(())
}

fn parse_metadata_size(payload: &[u8], max: usize) -> Result<Option<usize>, PeerError> {
    let root = bencode::parse(
        payload,
        bencode::Limits {
            input: 4096,
            string: 2048,
            items: 256,
            ..Default::default()
        },
    )
    .map_err(|_| PeerError::Protocol)?;
    let Some(value) = bencode::dict_get(&root, payload, b"metadata_size") else {
        return Ok(None);
    };
    let bencode::Value::Integer(size) = value else {
        return Err(PeerError::Protocol);
    };
    if *size <= 0 || *size as usize > max {
        return Err(PeerError::Protocol);
    }
    Ok(Some(*size as usize))
}

fn encode_extension_handshake(metadata_size: Option<usize>) -> Vec<u8> {
    // `i2p_pex` is seven bytes. Declaring eight produced a dictionary that no
    // conformant parser could read, which silently disabled every extension
    // exchange with a real peer.
    let mut out = b"d1:md7:i2p_pex".to_vec();
    out.extend_from_slice(
        format!("i{LOCAL_UT_PEX_ID}e11:ut_metadatai{LOCAL_UT_METADATA_ID}e").as_bytes(),
    );
    // Closes the extension map. `metadata_size` is a sibling of `m` in the
    // handshake dictionary, so it can only be written after this point.
    out.push(b'e');
    if let Some(size) = metadata_size {
        out.extend_from_slice(format!("13:metadata_sizei{size}e").as_bytes());
    }
    // Closes the handshake dictionary.
    out.push(b'e');
    // The BEP 10 `v` key is deliberately omitted. It is a peer-visible client
    // version fingerprint, and qBittorrent's Anonymous Mode already ships the
    // client with no `v` value in its extension handshake.
    out
}

async fn write_message<W: AsyncWrite + Unpin>(
    stream: &mut W,
    message: &Message,
    cancellation: &Cancellation,
) -> Result<(), PeerError> {
    let frame = encode_frame(message, MAX_PEER_FRAME).map_err(|_| PeerError::Protocol)?;
    let write = tokio::time::timeout(Duration::from_secs(30), async {
        stream.write_all(&frame).await?;
        stream.flush().await
    });
    race_cancel(write, cancellation)
        .await?
        .map_err(|_| PeerError::Transport(TransportError::Timeout))??;
    Ok(())
}

async fn cancellation_tick(cancellation: &Cancellation) {
    while !cancellation.is_cancelled() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use i2pr_tc_core::{
        service::{TorrentCommand, TorrentStatus},
        wire::{encode_frame, encode_handshake},
    };
    use sha1::{Digest, Sha1};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    fn root() -> std::path::PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "i2pr-tc-i2p-peer-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn metainfo() -> Vec<u8> {
        let mut bytes = b"d4:infod6:lengthi4e4:name1:x12:piece lengthi4e6:pieces20:".to_vec();
        bytes.extend(Sha1::digest(b"data"));
        bytes.extend_from_slice(b"ee");
        bytes
    }

    #[tokio::test]
    async fn outgoing_peer_rejects_wrong_infohash_before_registering_session() {
        let root = root();
        let runtime = TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap();
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let (client, mut remote) = duplex(4096);
        let server = tokio::spawn(async move {
            let mut request = [0; 68];
            remote.read_exact(&mut request).await.unwrap();
            let wrong = Handshake {
                reserved: [0; 8],
                info_hash: [42; 20],
                peer_id: [3; 20],
            };
            remote.write_all(&encode_handshake(&wrong)).await.unwrap();
        });
        let cancellation = Cancellation::default();
        let context = PeerContext {
            runtime: &runtime,
            id,
            peer: DestinationHash([7; 32]),
            local_hash: DestinationHash([2; 32]),
            cancellation: &cancellation,
            limits: PeerLimits::default(),
            sources: Arc::new(Mutex::new(PeerSourceSet::default())),
        };
        let result = run_outbound_stream(&context, Box::pin(client), [1; 20]).await;
        assert!(matches!(result, Err(PeerError::Protocol)));
        server.await.unwrap();
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    async fn read_frame<R: tokio::io::AsyncRead + Unpin>(reader: &mut R) -> Message {
        let mut header = [0; 4];
        reader.read_exact(&mut header).await.unwrap();
        let length = u32::from_be_bytes(header) as usize;
        let mut frame = header.to_vec();
        frame.resize(length + 4, 0);
        reader.read_exact(&mut frame[4..]).await.unwrap();
        let mut decoder = FrameDecoder::new(MAX_PEER_FRAME).unwrap();
        decoder.feed(&frame).unwrap().pop().unwrap()
    }

    async fn write_frame<W: tokio::io::AsyncWrite + Unpin>(writer: &mut W, message: &Message) {
        writer
            .write_all(&encode_frame(message, MAX_PEER_FRAME).unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn outgoing_peer_downloads_and_verifies_a_piece_over_injected_stream() {
        assert!(parse_extension_map(b"d1:mdee", 64).unwrap().is_empty());
        let root = root();
        let runtime = TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap();
        let bytes = metainfo();
        let hash = i2pr_tc_core::metainfo::parse(&bytes, Default::default())
            .unwrap()
            .info_hash
            .0;
        let id = runtime.add_metainfo(&bytes).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let (client, mut remote) = duplex(64 * 1024);
        let server = tokio::spawn(async move {
            let mut request = [0; 68];
            remote.read_exact(&mut request).await.unwrap();
            let handshake = Handshake {
                reserved: [0; 8],
                info_hash: hash,
                peer_id: [3; 20],
            };
            remote
                .write_all(&encode_handshake(&handshake))
                .await
                .unwrap();
            assert!(matches!(
                read_frame(&mut remote).await,
                Message::Extension { id: 0, .. }
            ));
            write_frame(
                &mut remote,
                &Message::Extension {
                    id: 0,
                    payload: b"d1:mdee".to_vec(),
                },
            )
            .await;
            write_frame(&mut remote, &Message::Unchoke).await;
            write_frame(&mut remote, &Message::Bitfield(vec![0x80])).await;
            let request = read_frame(&mut remote).await;
            let Message::Request {
                index,
                begin,
                length,
            } = request
            else {
                panic!("expected a piece request, got {request:?}");
            };
            assert_eq!((index, begin, length), (0, 0, 4));
            write_frame(
                &mut remote,
                &Message::Piece {
                    index,
                    begin,
                    block: b"data".to_vec(),
                },
            )
            .await;
        });
        let cancellation = Cancellation::default();
        let context = PeerContext {
            runtime: &runtime,
            id,
            peer: DestinationHash([7; 32]),
            local_hash: DestinationHash([2; 32]),
            cancellation: &cancellation,
            limits: PeerLimits::default(),
            sources: Arc::new(Mutex::new(PeerSourceSet::default())),
        };
        let result = run_outbound_stream(&context, Box::pin(client), [1; 20]).await;
        if result.is_err() {
            server.abort();
            panic!("peer run failed: {result:?}");
        }
        server.await.unwrap();
        let report = result.unwrap();
        assert_eq!(report.downloaded_pieces, 1);
        assert_eq!(
            runtime.service().get(id).unwrap().status,
            TorrentStatus::Completed
        );
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn extension_handshake_advertises_support_without_a_version_fingerprint() {
        let without_metadata = String::from_utf8(encode_extension_handshake(None)).unwrap();
        assert_eq!(
            without_metadata,
            format!("d1:md7:i2p_pexi{LOCAL_UT_PEX_ID}e11:ut_metadatai{LOCAL_UT_METADATA_ID}eee")
        );
        // `metadata_size` is a sibling of `m`, not a member of it.
        let with_size = String::from_utf8(encode_extension_handshake(Some(4096))).unwrap();
        // `metadata_size` sits between the extension map and the terminator of
        // the handshake dictionary that contains it.
        assert_eq!(
            with_size,
            concat!(
                "d1:md7:i2p_pexi1e11:ut_metadatai2ee",
                "13:metadata_sizei4096ee"
            )
        );
        // Both shapes must round-trip through the same parsers this client uses
        // on a peer handshake, otherwise the client cannot read what it writes.
        for (payload, expected_size) in [(&without_metadata, None), (&with_size, Some(4096))] {
            let map = parse_extension_map(payload.as_bytes(), 64)
                .unwrap_or_else(|e| panic!("map parse failed for {payload}: {e:?}"));
            assert_eq!(map.get("ut_metadata"), Some(&LOCAL_UT_METADATA_ID));
            assert_eq!(map.get("i2p_pex"), Some(&LOCAL_UT_PEX_ID));
            assert!(
                matches!(
                    parse_metadata_size(payload.as_bytes(), MAX_METADATA_BYTES),
                    Ok(size) if size == expected_size
                ),
                "size parse mismatch for {payload}"
            );
        }
        for payload in [&without_metadata, &with_size] {
            assert!(
                !payload.contains("1:v"),
                "extension handshake advertised a client version: {payload}"
            );
            assert!(
                !payload.contains("i2pr-tc"),
                "extension handshake advertised the product name: {payload}"
            );
        }
    }

    /// A session that yields one scripted inbound stream and then parks, so
    /// `serve_incoming` stays alive until cancellation releases its children.
    struct AcceptOnceSession {
        hash: [u8; 32],
        peer: crate::identity::Destination,
        pending: std::sync::Mutex<Option<crate::I2pStream>>,
    }

    #[async_trait::async_trait]
    impl crate::I2pSession for AcceptOnceSession {
        fn local_peer_hash(&self) -> [u8; 32] {
            self.hash
        }

        async fn lookup(
            &self,
            _name: &str,
        ) -> Result<crate::identity::Destination, crate::TransportError> {
            Err(crate::TransportError::Session(
                "no lookup in this fixture".into(),
            ))
        }

        async fn connect(
            &self,
            _destination: &crate::identity::Destination,
            _port: u16,
        ) -> Result<crate::I2pStream, crate::TransportError> {
            Err(crate::TransportError::Session(
                "no connect in this fixture".into(),
            ))
        }

        async fn accept(
            &self,
        ) -> Result<(crate::identity::Destination, crate::I2pStream), crate::TransportError>
        {
            // The guard is released before parking, so the future stays `Send`.
            let next = self.pending.lock().unwrap().take();
            match next {
                Some(stream) => Ok((self.peer.clone(), stream)),
                None => std::future::pending().await,
            }
        }
    }

    fn magnet_uri(hash: [u8; 20]) -> String {
        let hex: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
        format!("magnet:?xt=urn:btih:{hex}&dn=x")
    }

    #[tokio::test]
    async fn magnet_metadata_is_promoted_and_then_downloads_a_verified_piece() {
        let root = root();
        let runtime = TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap();
        let bytes = metainfo();
        let parsed = i2pr_tc_core::metainfo::parse(&bytes, Default::default()).unwrap();
        let hash = parsed.info_hash.0;
        let info = parsed.info_bytes;
        let id = runtime.add_magnet(&magnet_uri(hash)).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        assert!(
            runtime.service().get_metainfo(id).unwrap().is_none(),
            "magnet registration must not synthesise metainfo"
        );
        let (client, mut remote) = duplex(64 * 1024);
        let server = tokio::spawn(async move {
            let mut request = [0; 68];
            remote.read_exact(&mut request).await.unwrap();
            let handshake = Handshake {
                reserved: [0; 8],
                info_hash: hash,
                peer_id: [9; 20],
            };
            remote
                .write_all(&encode_handshake(&handshake))
                .await
                .unwrap();
            // A magnet client has no metainfo, so its own extension handshake
            // advertises support without a metadata size.
            assert!(matches!(
                read_frame(&mut remote).await,
                Message::Extension { id: 0, .. }
            ));
            write_frame(
                &mut remote,
                &Message::Extension {
                    id: 0,
                    payload: format!("d1:md11:ut_metadatai2ee13:metadata_sizei{}ee", info.len())
                        .into_bytes(),
                },
            )
            .await;
            let Message::Extension { id, payload } = read_frame(&mut remote).await else {
                panic!("expected a ut_metadata request");
            };
            assert_eq!(id, LOCAL_UT_METADATA_ID);
            assert_eq!(payload, b"d8:msg_typei0e5:piecei0ee".to_vec());
            write_frame(
                &mut remote,
                &Message::Extension {
                    id: LOCAL_UT_METADATA_ID,
                    payload: [
                        format!("d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len())
                            .into_bytes(),
                        info.clone(),
                    ]
                    .concat(),
                },
            )
            .await;
            write_frame(&mut remote, &Message::Unchoke).await;
            write_frame(&mut remote, &Message::Bitfield(vec![0x80])).await;
            let Message::Request {
                index,
                begin,
                length,
            } = read_frame(&mut remote).await
            else {
                panic!("expected a piece request after metadata promotion");
            };
            assert_eq!((index, begin, length), (0, 0, 4));
            write_frame(
                &mut remote,
                &Message::Piece {
                    index,
                    begin,
                    block: b"data".to_vec(),
                },
            )
            .await;
            // Closing here is the seeder's end-of-transfer signal.
        });
        let cancellation = Cancellation::default();
        let context = PeerContext {
            runtime: &runtime,
            id,
            peer: DestinationHash([7; 32]),
            local_hash: DestinationHash([2; 32]),
            cancellation: &cancellation,
            limits: PeerLimits::default(),
            sources: Arc::new(Mutex::new(PeerSourceSet::default())),
        };
        let result = run_outbound_stream(&context, Box::pin(client), [1; 20]).await;
        let server = server.await;
        if result.is_err() {
            panic!("magnet peer run failed: {result:?} (seeder: {server:?})");
        }
        server.unwrap();
        let report = result.unwrap();
        assert!(report.metadata_promoted, "metadata was never promoted");
        assert_eq!(report.downloaded_pieces, 1);
        assert_eq!(
            runtime.service().get(id).unwrap().status,
            TorrentStatus::Completed
        );
        assert!(runtime.service().get_metainfo(id).unwrap().is_some());
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn inbound_peer_delivers_a_verified_piece_to_the_accepting_client() {
        let root = root();
        let runtime =
            Arc::new(TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap());
        let bytes = metainfo();
        let hash = i2pr_tc_core::metainfo::parse(&bytes, Default::default())
            .unwrap()
            .info_hash
            .0;
        let id = runtime.add_metainfo(&bytes).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let (client, mut remote) = duplex(64 * 1024);
        let server = tokio::spawn(async move {
            // The accepting side reads the connecting peer's handshake first.
            let handshake = Handshake {
                reserved: [0; 8],
                info_hash: hash,
                peer_id: [8; 20],
            };
            remote
                .write_all(&encode_handshake(&handshake))
                .await
                .unwrap();
            let mut local = [0; 68];
            remote.read_exact(&mut local).await.unwrap();
            assert!(matches!(
                read_frame(&mut remote).await,
                Message::Extension { id: 0, .. }
            ));
            write_frame(
                &mut remote,
                &Message::Extension {
                    id: 0,
                    payload: b"d1:mdee".to_vec(),
                },
            )
            .await;
            write_frame(&mut remote, &Message::Unchoke).await;
            write_frame(&mut remote, &Message::Bitfield(vec![0x80])).await;
            let Message::Request { index, begin, .. } = read_frame(&mut remote).await else {
                panic!("inbound peer received no piece request");
            };
            write_frame(
                &mut remote,
                &Message::Piece {
                    index,
                    begin,
                    block: b"data".to_vec(),
                },
            )
            .await;
            let mut sink = [0; 8];
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), remote.read(&mut sink))
                .await;
        });
        let peer = crate::identity::Destination::from_bytes(vec![3; 387]).unwrap();
        let session = Arc::new(AcceptOnceSession {
            hash: [2; 32],
            peer,
            pending: std::sync::Mutex::new(Some(Box::pin(client))),
        });
        let cancellation = Cancellation::default();
        let serving = tokio::spawn(serve_incoming(
            session,
            runtime.clone(),
            [1; 20],
            cancellation.clone(),
            PeerLimits::default(),
            4,
            Arc::new(Mutex::new(PeerSourceSet::default())),
        ));
        let completed = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if runtime.service().get(id).unwrap().status == TorrentStatus::Completed {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        if completed.is_err() {
            serving.abort();
            server.abort();
            panic!("inbound transfer never completed");
        }
        server.await.unwrap();
        cancellation.cancel();
        serving.await.unwrap().unwrap();
        assert_eq!(
            runtime.service().get(id).unwrap().status,
            TorrentStatus::Completed
        );
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }
}
